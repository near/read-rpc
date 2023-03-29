use crate::config::CompiledCodeCache;
use crate::utils::get_final_cache_block;
use clap::Parser;
use config::{Opts, ServerContext};
use database::ScyllaStorageManager;
use dotenv::dotenv;
use jsonrpc_v2::{Data, Server};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use utils::{prepare_s3_client, update_final_block_height_regularly};

mod config;
mod errors;
mod modules;
mod utils;

fn init_logging(use_tracer: bool) {
    // Filter based on level - trace, debug, info, warn, error
    // Tunable via `RUST_LOG` env variable
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or(tracing_subscriber::EnvFilter::new("info"));

    // Combined them all together in a `tracing` subscriber
    let subscriber = tracing_subscriber::Registry::default()
        .with(env_filter)
        .with(tracing_subscriber::fmt::Layer::default());

    if use_tracer {
        let app_name = "json-rpc-100x";
        // Start a new Jaeger trace pipeline.
        // Spans are exported in batch - recommended setup for a production application.
        opentelemetry::global::set_text_map_propagator(
            opentelemetry::sdk::propagation::TraceContextPropagator::new(),
        );
        let tracer = opentelemetry_jaeger::new_pipeline()
            .with_service_name(app_name)
            .install_simple()
            .unwrap();
        // Create a `tracing` layer using the Jaeger tracer
        let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
        subscriber
            .with(telemetry)
            .try_init()
            .expect("Failed to install `tracing` subscriber.");
    } else {
        subscriber
            .try_init()
            .expect("Failed to install `tracing` subscriber.");
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();

    let opts: Opts = Opts::parse();

    #[cfg(feature = "tracing-instrumentation")]
    init_logging(true);

    #[cfg(not(feature = "tracing-instrumentation"))]
    init_logging(false);

    let near_rpc_client = near_jsonrpc_client::JsonRpcClient::connect(opts.rpc_url.to_string());
    let blocks_cache = std::sync::Arc::new(std::sync::RwLock::new(lru::LruCache::new(
        std::num::NonZeroUsize::new(100000).unwrap(),
    )));

    let final_block = get_final_cache_block(&near_rpc_client)
        .await
        .expect("Error to get final block");
    let final_block_height =
        std::sync::Arc::new(std::sync::atomic::AtomicU64::new(final_block.block_height));
    blocks_cache
        .write()
        .unwrap()
        .put(final_block.block_height, final_block);

    let compiled_contract_code_cache = std::sync::Arc::new(CompiledCodeCache {
        local_cache: std::sync::Arc::new(std::sync::RwLock::new(lru::LruCache::new(
            std::num::NonZeroUsize::new(128).unwrap(),
        ))),
    });
    let contract_code_cache = std::sync::Arc::new(std::sync::RwLock::new(lru::LruCache::new(
        std::num::NonZeroUsize::new(128).unwrap(),
    )));

    let scylla_db_manager = std::sync::Arc::new(
        *config::ScyllaDBManager::new(
            &opts.scylla_url,
            opts.scylla_user.as_deref(),
            opts.scylla_password.as_deref(),
            Some(opts.scylla_keepalive_interval),
        )
        .await?,
    );

    tracing::info!("Get genesis config...");
    let genesis_config = near_rpc_client
        .call(near_jsonrpc_client::methods::EXPERIMENTAL_genesis_config::RpcGenesisConfigRequest)
        .await?;

    let state = ServerContext {
        s3_client: prepare_s3_client(
            &opts.access_key_id,
            &opts.secret_access_key,
            opts.region.clone(),
        )
        .await,
        scylla_db_manager,
        near_rpc_client: near_rpc_client.clone(),
        s3_bucket_name: opts.s3_bucket_name,
        genesis_config,
        blocks_cache: std::sync::Arc::clone(&blocks_cache),
        final_block_height: std::sync::Arc::clone(&final_block_height),
        compiled_contract_code_cache,
        contract_code_cache,
    };

    let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let shutdown_task = std::sync::Arc::clone(&shutdown);

    tokio::spawn(async move {
        update_final_block_height_regularly(
            final_block_height.clone(),
            blocks_cache,
            near_rpc_client,
            shutdown_task,
        )
        .await
    });

    let rpc = Server::new()
        .with_data(Data::new(state))
        .with_method("query", modules::queries::methods::query)
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
        .with_method("network_info", modules::network::methods::network_info)
        .with_method("validators", modules::network::methods::validators)
        .with_method(
            "EXPERIMENTAL_genesis_config",
            modules::network::methods::genesis_config,
        )
        .with_method(
            "EXPERIMENTAL_protocol_config",
            modules::network::methods::protocol_config,
        )
        .finish();

    actix_web::HttpServer::new(move || {
        let rpc = rpc.clone();
        actix_web::App::new()
            .wrap(tracing_actix_web::TracingLogger::default())
            .service(
                actix_web::web::service("/")
                    .guard(actix_web::guard::Post())
                    .finish(rpc.into_web_service()),
            )
    })
    .bind(format!("0.0.0.0:{:0>5}", opts.server_port))?
    .run()
    .await?;

    shutdown.store(true, std::sync::atomic::Ordering::Relaxed);

    Ok(())
}
