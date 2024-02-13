use crate::modules::blocks::FinalBlockInfo;
use crate::utils::{gigabytes_to_bytes, update_final_block_height_regularly};
use jsonrpc_v2::{Data, Server};

#[macro_use]
extern crate lazy_static;

mod cache;
mod config;
mod errors;
mod metrics;
mod modules;
mod utils;

// Categories for logging
pub(crate) const RPC_SERVER: &str = "read_rpc_server";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    configuration::init_tracing(RPC_SERVER).await?;
    let rpc_server_config =
        configuration::read_configuration::<configuration::RpcServerConfig>().await?;

    let near_rpc_client = utils::JsonRpcClient::new(
        rpc_server_config.general.near_rpc_url.clone(),
        rpc_server_config.general.near_archival_rpc_url.clone(),
    );
    // We want to set a custom referer to let NEAR JSON RPC nodes know that we are a read-rpc instance
    let near_rpc_client = near_rpc_client.header(
        "Referer".to_string(),
        rpc_server_config.general.referer_header_value.clone(),
    )?;

    let limit_memory_cache_in_bytes =
        if let Some(limit_memory_cache) = rpc_server_config.general.limit_memory_cache {
            Some(gigabytes_to_bytes(limit_memory_cache).await)
        } else {
            None
        };
    let reserved_memory_in_bytes =
        gigabytes_to_bytes(rpc_server_config.general.reserved_memory).await;
    let block_cache_size_in_bytes =
        gigabytes_to_bytes(rpc_server_config.general.block_cache_size).await;

    let contract_code_cache_size = utils::calculate_contract_code_cache_sizes(
        reserved_memory_in_bytes,
        block_cache_size_in_bytes,
        limit_memory_cache_in_bytes,
    )
    .await;

    let blocks_cache = std::sync::Arc::new(futures_locks::RwLock::new(cache::LruMemoryCache::new(
        block_cache_size_in_bytes,
    )));

    let finale_block_info = std::sync::Arc::new(futures_locks::RwLock::new(
        FinalBlockInfo::new(&near_rpc_client, &blocks_cache).await,
    ));

    let compiled_contract_code_cache = std::sync::Arc::new(config::CompiledCodeCache {
        local_cache: std::sync::Arc::new(futures_locks::RwLock::new(cache::LruMemoryCache::new(
            contract_code_cache_size,
        ))),
    });
    let contract_code_cache = std::sync::Arc::new(futures_locks::RwLock::new(
        cache::LruMemoryCache::new(contract_code_cache_size),
    ));

    tracing::info!("Get genesis config...");
    let genesis_config = near_rpc_client
        .call(near_jsonrpc_client::methods::EXPERIMENTAL_genesis_config::RpcGenesisConfigRequest)
        .await?;
    let lake_config = rpc_server_config
        .lake_config
        .lake_config(
            finale_block_info
                .read()
                .await
                .final_block_cache
                .block_height,
        )
        .await?;
    let lake_s3_client = rpc_server_config.lake_config.lake_s3_client().await;

    #[cfg(feature = "scylla_db")]
    let db_manager =
        database::prepare_db_manager::<database::scylladb::rpc_server::ScyllaDBManager>(
            &rpc_server_config.database,
        )
        .await?;

    #[cfg(all(feature = "postgres_db", not(feature = "scylla_db")))]
    let db_manager = database::prepare_db_manager::<
        database::postgres::rpc_server::PostgresDBManager,
    >(&rpc_server_config.database)
    .await?;

    let state = config::ServerContext::new(
        lake_s3_client,
        db_manager,
        near_rpc_client.clone(),
        rpc_server_config.lake_config.aws_bucket_name.clone(),
        genesis_config,
        std::sync::Arc::clone(&blocks_cache),
        std::sync::Arc::clone(&finale_block_info),
        compiled_contract_code_cache,
        contract_code_cache,
        rpc_server_config.general.max_gas_burnt,
        rpc_server_config.general.shadow_data_consistency_rate,
    );

    tokio::spawn(async move {
        update_final_block_height_regularly(
            blocks_cache,
            finale_block_info,
            lake_config,
            near_rpc_client,
        )
        .await
    });

    let rpc = Server::new()
        .with_data(Data::new(state))
        .with_method("query", modules::queries::methods::query)
        .with_method(
            "view_state_paginated",
            modules::state::methods::view_state_paginated,
        )
        .with_method("block", modules::blocks::methods::block)
        .with_method(
            "EXPERIMENTAL_changes",
            modules::blocks::methods::changes_in_block_by_type,
        )
        .with_method(
            "EXPERIMENTAL_changes_in_block",
            modules::blocks::methods::changes_in_block,
        )
        .with_method("chunk", modules::blocks::methods::chunk)
        .with_method("tx", modules::transactions::methods::tx)
        .with_method(
            "EXPERIMENTAL_tx_status",
            modules::transactions::methods::tx_status,
        )
        .with_method(
            "broadcast_tx_async",
            modules::transactions::methods::send_tx_async,
        )
        .with_method(
            "broadcast_tx_commit",
            modules::transactions::methods::send_tx_commit,
        )
        .with_method("gas_price", modules::gas::methods::gas_price)
        .with_method("status", modules::network::methods::status)
        .with_method(
            "light_client_proof",
            modules::clients::methods::light_client_proof,
        )
        .with_method(
            "next_light_client_block",
            modules::clients::methods::next_light_client_block,
        )
        .with_method("network_info", modules::network::methods::network_info)
        .with_method("validators", modules::network::methods::validators)
        .with_method(
            "EXPERIMENTAL_validators_ordered",
            modules::network::methods::validators_ordered,
        )
        .with_method(
            "EXPERIMENTAL_genesis_config",
            modules::network::methods::genesis_config,
        )
        .with_method(
            "EXPERIMENTAL_protocol_config",
            modules::network::methods::protocol_config,
        )
        .with_method("EXPERIMENTAL_receipt", modules::receipts::methods::receipt)
        .finish();

    actix_web::HttpServer::new(move || {
        let rpc = rpc.clone();

        // Configure CORS
        let cors = actix_cors::Cors::permissive();

        actix_web::App::new()
            .wrap(cors)
            .wrap(tracing_actix_web::TracingLogger::default())
            .service(
                actix_web::web::service("/")
                    .guard(actix_web::guard::Post())
                    .finish(rpc.into_web_service()),
            )
            .service(metrics::get_metrics)
    })
    .bind(format!(
        "0.0.0.0:{:0>5}",
        rpc_server_config.general.server_port
    ))?
    .run()
    .await?;

    Ok(())
}
