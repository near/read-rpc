use crate::utils::get_final_cache_block;
use clap::Parser;
use config::{Opts, ServerContext};
use dotenv::dotenv;
use jsonrpc_v2::{Data, Server};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use utils::{
    prepare_db_client, prepare_redis_client, prepare_s3_client, update_final_block_height_regularly,
};

mod config;
mod errors;
mod modules;
mod utils;

fn init_logging() {
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

    // Filter based on level - trace, debug, info, warn, error
    // Tunable via `RUST_LOG` env variable
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or(tracing_subscriber::EnvFilter::new("info"));
    // Create a `tracing` layer using the Jaeger tracer
    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

    // Combined them all together in a `tracing` subscriber
    // let subscriber = tracing_subscriber::Registry::default()
    tracing_subscriber::Registry::default()
        .with(env_filter)
        .with(telemetry)
        .with(tracing_subscriber::fmt::Layer::default())
        .try_init()
        .expect("Failed to install `tracing` subscriber.");
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok();
    init_logging();
    let opts: Opts = Opts::parse();

    let near_rpc_client = near_jsonrpc_client::JsonRpcClient::connect(opts.rpc_url.to_string());

    let final_block = get_final_cache_block(&near_rpc_client)
        .await
        .expect("Error to get final block");
    let final_block_height =
        std::sync::Arc::new(std::sync::atomic::AtomicU64::new(final_block.block_height));
    let shared_cache = shared_lru::SharedLru::with_byte_limit(1024 * 1024 + 512);
    let blocks_cache = std::sync::Arc::new(shared_cache.make_cache());
    blocks_cache.insert(final_block.block_height, final_block);

    let state = ServerContext {
        s3_client: prepare_s3_client(
            &opts.access_key_id,
            &opts.secret_access_key,
            opts.region.clone(),
        )
        .await,
        db_client: prepare_db_client(&opts.database_url).await,
        redis_client: prepare_redis_client(&opts.redis_url).await,
        near_rpc_client: near_rpc_client.clone(),
        s3_bucket_name: opts.s3_bucket_name,
        blocks_cache: std::sync::Arc::clone(&blocks_cache),
        final_block_height: std::sync::Arc::clone(&final_block_height),
    };

    tokio::spawn(async move {
        update_final_block_height_regularly(
            final_block_height.clone(),
            std::sync::Arc::clone(&blocks_cache),
            near_rpc_client,
        )
        .await
    });

    let rpc = Server::new()
        .with_data(Data::new(state))
        .with_method("query", modules::queries::methods::query)
        .with_method("block", modules::blocks::methods::block)
        .with_method("chunk", modules::blocks::methods::chunk)
        .with_method("tx", modules::transactions::methods::tx_status_common)
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
    .await
}
