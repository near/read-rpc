use actix_web::dev::Service;
use jsonrpc_v2::{Data, Server};

#[macro_use]
extern crate lazy_static;

mod cache;
mod config;
mod errors;
mod health;
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
    let redis_client = redis::Client::open(rpc_server_config.general.redis_url.clone())?;

    let server_port = rpc_server_config.general.server_port;

    let server_context =
        config::ServerContext::init(rpc_server_config.clone(), near_rpc_client.clone()).await?;

    let blocks_cache = std::sync::Arc::clone(&server_context.blocks_cache);
    let final_block_info = std::sync::Arc::clone(&server_context.final_block_info);

    #[cfg(not(feature = "near_state_indexer"))]
    tokio::spawn(async move {
        utils::update_final_block_regularly(
            blocks_cache,
            final_block_info,
            rpc_server_config,
            near_rpc_client,
        )
        .await
    });

    #[cfg(feature = "near_state_indexer")]
    {
        let redis_client_clone = redis_client.clone();
        tokio::spawn(async move {
            utils::update_final_block_regularly(
                blocks_cache,
                final_block_info,
                redis_client_clone,
                near_rpc_client,
            )
            .await
        });
        let final_block_info = std::sync::Arc::clone(&server_context.final_block_info);
        tokio::spawn(async move {
            utils::update_optimistic_block_regularly(final_block_info, redis_client.clone()).await
        });
    }

    let rpc = Server::new()
        .with_data(Data::new(server_context.clone()))
        // custom requests methods
        .with_method(
            "view_state_paginated",
            modules::state::methods::view_state_paginated,
        )
        // requests methods
        .with_method("query", modules::queries::methods::query)
        // basic requests methods
        .with_method("block", modules::blocks::methods::block)
        .with_method(
            "broadcast_tx_async",
            modules::transactions::methods::broadcast_tx_async,
        )
        .with_method(
            "broadcast_tx_commit",
            modules::transactions::methods::broadcast_tx_commit,
        )
        .with_method("chunk", modules::blocks::methods::chunk)
        .with_method("gas_price", modules::gas::methods::gas_price)
        .with_method("health", modules::network::methods::health)
        .with_method(
            "light_client_proof",
            modules::clients::methods::light_client_proof,
        )
        .with_method(
            "next_light_client_block",
            modules::clients::methods::next_light_client_block,
        )
        .with_method("network_info", modules::network::methods::network_info)
        .with_method("send_tx", modules::transactions::methods::send_tx)
        .with_method("status", modules::network::methods::status)
        .with_method("tx", modules::transactions::methods::tx)
        .with_method("validators", modules::network::methods::validators)
        .with_method("client_config", modules::network::methods::client_config)
        .with_method(
            "EXPERIMENTAL_changes",
            modules::blocks::methods::changes_in_block_by_type,
        )
        .with_method(
            "EXPERIMENTAL_changes_in_block",
            modules::blocks::methods::changes_in_block,
        )
        .with_method(
            "EXPERIMENTAL_genesis_config",
            modules::network::methods::genesis_config,
        )
        .with_method(
            "EXPERIMENTAL_light_client_proof",
            modules::clients::methods::light_client_proof,
        )
        .with_method(
            "EXPERIMENTAL_protocol_config",
            modules::network::methods::protocol_config,
        )
        .with_method("EXPERIMENTAL_receipt", modules::receipts::methods::receipt)
        .with_method(
            "EXPERIMENTAL_tx_status",
            modules::transactions::methods::tx_status,
        )
        .with_method(
            "EXPERIMENTAL_validators_ordered",
            modules::network::methods::validators_ordered,
        )
        .with_method(
            "EXPERIMENTAL_maintenance_windows",
            modules::network::methods::maintenance_windows,
        )
        .with_method(
            "EXPERIMENTAL_split_storage_info",
            modules::network::methods::split_storage_info,
        )
        .finish();

    actix_web::HttpServer::new(move || {
        let rpc = rpc.clone();

        // Configure CORS
        let cors = actix_cors::Cors::permissive();

        actix_web::App::new()
            .wrap(cors)
            .wrap(tracing_actix_web::TracingLogger::default())
            // wrapper to count rpc total requests
            .wrap_fn(|request, service| {
                if request.path() == "/" {
                    metrics::TOTAL_REQUESTS_COUNTER.inc();
                };
                service.call(request)
            })
            .app_data(actix_web::web::Data::new(server_context.clone()))
            .service(
                actix_web::web::service("/")
                    .guard(actix_web::guard::Post())
                    .finish(rpc.into_web_service()),
            )
            .service(metrics::get_metrics)
            .service(health::get_health_status)
    })
    .bind(format!("0.0.0.0:{:0>5}", server_port))?
    .run()
    .await?;

    Ok(())
}
