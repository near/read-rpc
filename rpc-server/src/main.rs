use crate::modules::blocks::FinalBlockInfo;
use crate::utils::{gigabytes_to_bytes, update_final_block_height_regularly};
use jsonrpc_v2::{Data, Server};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[macro_use]
extern crate lazy_static;

mod cache;
mod config;
mod errors;
mod metrics;
mod modules;
mod utils;

fn init_logging(use_tracer: bool) -> anyhow::Result<()> {
    // Filter based on level - trace, debug, info, warn, error
    // Tunable via `RUST_LOG` env variable
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or(tracing_subscriber::EnvFilter::new("info"));

    // Combined them all together in a `tracing` subscriber
    let subscriber = tracing_subscriber::Registry::default().with(env_filter);

    if use_tracer {
        let app_name = "read_rpc_server";
        // Start a new Jaeger trace pipeline.
        // Spans are exported in batch - recommended setup for a production application.
        opentelemetry::global::set_text_map_propagator(
            opentelemetry::sdk::propagation::TraceContextPropagator::new(),
        );
        let tracer = opentelemetry_jaeger::new_pipeline()
            .with_service_name(app_name)
            .install_simple()?;
        // Create a `tracing` layer using the Jaeger tracer
        let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

        if std::env::var("ENABLE_JSON_LOGS").is_ok() {
            subscriber
                .with(telemetry)
                .with(tracing_subscriber::fmt::Layer::default().json())
                .try_init()?;
        } else {
            subscriber
                .with(telemetry)
                .with(tracing_subscriber::fmt::Layer::default().compact())
                .try_init()?;
        };
    } else if std::env::var("ENABLE_JSON_LOGS").is_ok() {
        subscriber.with(tracing_stackdriver::layer()).try_init()?;
    } else {
        subscriber
            .with(tracing_subscriber::fmt::Layer::default().compact())
            .try_init()?;
    };

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let rpc_server_config = configuration::read_configuration().await?;

    #[cfg(feature = "tracing-instrumentation")]
    init_logging(true)?;

    #[cfg(not(feature = "tracing-instrumentation"))]
    init_logging(false)?;

    let near_rpc_client = utils::JsonRpcClient::new(
        rpc_server_config.general.near_rpc_url.clone(),
        rpc_server_config.general.near_archival_rpc_url.clone(),
    );
    // We want to set a custom referer to let NEAR JSON RPC nodes know that we are a read-rpc instance
    let near_rpc_client = near_rpc_client.header(
        "Referer".to_string(),
        rpc_server_config
            .general
            .rpc_server
            .referer_header_value
            .clone(),
    )?;

    let limit_memory_cache_in_bytes =
        if let Some(limit_memory_cache) = rpc_server_config.general.rpc_server.limit_memory_cache {
            Some(gigabytes_to_bytes(limit_memory_cache).await)
        } else {
            None
        };
    let reserved_memory_in_bytes =
        gigabytes_to_bytes(rpc_server_config.general.rpc_server.reserved_memory).await;
    let block_cache_size_in_bytes =
        gigabytes_to_bytes(rpc_server_config.general.rpc_server.block_cache_size).await;

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
        .to_lake_config(
            finale_block_info
                .read()
                .await
                .final_block_cache
                .block_height,
        )
        .await?;
    let lake_s3_client = rpc_server_config.to_s3_client().await;

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
        rpc_server_config.general.rpc_server.max_gas_burnt,
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
        rpc_server_config.general.rpc_server.server_port
    ))?
    .run()
    .await?;

    Ok(())
}
