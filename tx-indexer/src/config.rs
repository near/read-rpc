pub use clap::{Parser, Subcommand};
use near_indexer_primitives::types::{BlockReference, Finality};
use near_jsonrpc_client::{methods, JsonRpcClient};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// NEAR Indexer for Explorer
/// Watches for stream of blocks from the chain
#[derive(Parser, Debug)]
#[clap(
    version,
    author,
    about,
    setting(clap::AppSettings::DisableHelpSubcommand),
    setting(clap::AppSettings::PropagateVersion),
    setting(clap::AppSettings::NextLineHelp)
)]
pub(crate) struct Opts {
    #[clap(subcommand)]
    pub start_options: StartOptions,
}

#[allow(clippy::enum_variant_names)]
#[derive(Subcommand, Debug, Clone)]
pub enum StartOptions {
    FromBlock {
        height: u64,
    },
    FromInterruption {
        /// Fallback start block height if interruption is not found
        height: Option<u64>,
    },
    FromLatest,
}

pub(crate) async fn get_start_block_height(
    rpc_client: &JsonRpcClient,
    db_manager: &std::sync::Arc<Box<dyn database::TxIndexerDbManager + Sync + Send + 'static>>,
    start_options: &StartOptions,
    indexer_id: &str,
) -> anyhow::Result<u64> {
    match start_options {
        StartOptions::FromBlock { height } => Ok(*height),
        StartOptions::FromInterruption { height } => {
            if let Ok(block_height) = db_manager.get_last_processed_block_height(indexer_id).await {
                Ok(block_height)
            } else {
                if let Some(height) = height {
                    return Ok(*height);
                }
                Ok(final_block_height(rpc_client).await?)
            }
        }
        StartOptions::FromLatest => Ok(final_block_height(rpc_client).await?),
    }
}

pub async fn final_block_height(rpc_client: &JsonRpcClient) -> anyhow::Result<u64> {
    let request = methods::block::RpcBlockRequest {
        block_reference: BlockReference::Finality(Finality::Final),
    };

    let latest_block = rpc_client.call(request).await?;

    Ok(latest_block.header.height)
}

pub fn init_tracing() -> anyhow::Result<()> {
    let mut env_filter = tracing_subscriber::EnvFilter::new("tx_indexer=info");

    if let Ok(rust_log) = std::env::var("RUST_LOG") {
        if !rust_log.is_empty() {
            for directive in rust_log.split(',').filter_map(|s| match s.parse() {
                Ok(directive) => Some(directive),
                Err(err) => {
                    eprintln!("Ignoring directive `{}`: {}", s, err);
                    None
                }
            }) {
                env_filter = env_filter.add_directive(directive);
            }
        }
    }

    opentelemetry::global::shutdown_tracer_provider();

    opentelemetry::global::set_text_map_propagator(
        opentelemetry::sdk::propagation::TraceContextPropagator::new(),
    );

    #[cfg(feature = "tracing-instrumentation")]
    let subscriber = {
        let tracer = opentelemetry_jaeger::new_collector_pipeline()
            .with_service_name("tx_indexer")
            .with_endpoint(std::env::var("OTEL_EXPORTER_JAEGER_ENDPOINT").unwrap_or_default())
            .with_isahc()
            .with_batch_processor_config(
                opentelemetry::sdk::trace::BatchConfig::default()
                    .with_max_queue_size(10_000)
                    .with_max_export_batch_size(10_000)
                    .with_max_concurrent_exports(100),
            )
            .install_batch(opentelemetry::runtime::TokioCurrentThread)?;
        let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

        tracing_subscriber::Registry::default()
            .with(env_filter)
            .with(telemetry)
    };

    #[cfg(not(feature = "tracing-instrumentation"))]
    let subscriber = tracing_subscriber::Registry::default().with(env_filter);

    if std::env::var("ENABLE_JSON_LOGS").is_ok() {
        subscriber.with(tracing_stackdriver::layer()).try_init()?;
    } else {
        subscriber
            .with(tracing_subscriber::fmt::Layer::default().compact())
            .try_init()?;
    }

    Ok(())
}
