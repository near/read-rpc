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
    /// Indexer ID to handle meta data about the instance
    #[clap(long, env)]
    pub indexer_id: String,
    /// Port for metrics server
    #[clap(long, default_value = "8080", env)]
    pub port: u16,
    /// DB connection string
    #[clap(long, default_value = "127.0.0.1:9042", env)]
    pub database_url: String,
    /// DB user(login)
    #[clap(long, env)]
    pub database_user: Option<String>,
    /// DB password
    #[clap(long, env)]
    pub database_password: Option<String>,
    /// Chain ID: testnet or mainnet
    #[clap(subcommand)]
    pub chain_id: ChainId,
    /// To restore cache from db we use smart range blocks
    /// Regular transaction takes some blocks to be finalized
    /// We don't need to restore too old transactions for the indexer because we will probably never be able to reassemble them.
    /// We use a range of 1000 blocks for our peace of mind. We also leave the option to increase or decrease this range
    #[clap(long, default_value = "1000", env)]
    pub cache_restore_blocks_range: u64,
    /// Parallel queries = (nodes in cluster) ✕ (cores in node) ✕ 3
    /// Current we have 6 - nodes with 8 - cpus
    /// 6 ✕ 8 ✕ 3 = 144
    #[clap(long, env, default_value = "144")]
    pub max_db_parallel_queries: i64,
}

#[derive(Subcommand, Debug, Clone)]
pub enum ChainId {
    #[clap(subcommand)]
    Mainnet(StartOptions),
    #[clap(subcommand)]
    Testnet(StartOptions),
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

impl Opts {
    /// Returns [StartOptions] for current [Opts]
    pub fn start_options(&self) -> &StartOptions {
        match &self.chain_id {
            ChainId::Mainnet(start_options) | ChainId::Testnet(start_options) => start_options,
        }
    }

    pub fn rpc_url(&self) -> &str {
        match self.chain_id {
            ChainId::Mainnet(_) => "https://rpc.mainnet.near.org",
            ChainId::Testnet(_) => "https://rpc.testnet.near.org",
        }
    }
}

impl Opts {
    pub async fn to_lake_config(
        &self,
        start_block_height: u64,
    ) -> anyhow::Result<near_lake_framework::LakeConfig> {
        let config_builder = near_lake_framework::LakeConfigBuilder::default();

        Ok(match &self.chain_id {
            ChainId::Mainnet(_) => config_builder
                .mainnet()
                .start_block_height(start_block_height),
            ChainId::Testnet(_) => config_builder
                .testnet()
                .start_block_height(start_block_height),
        }
        .build()
        .expect("Failed to build LakeConfig"))
    }
}

pub(crate) async fn get_start_block_height(
    opts: &Opts,
    db_manager: &std::sync::Arc<Box<dyn database::TxIndexerDbManager + Sync + Send + 'static>>,
) -> anyhow::Result<u64> {
    match opts.start_options() {
        StartOptions::FromBlock { height } => Ok(*height),
        StartOptions::FromInterruption { height } => {
            if let Ok(block_height) = db_manager
                .get_last_processed_block_height(opts.indexer_id.as_str())
                .await
            {
                Ok(block_height)
            } else {
                if let Some(height) = height {
                    return Ok(*height);
                }
                Ok(final_block_height(opts.rpc_url()).await?)
            }
        }
        StartOptions::FromLatest => Ok(final_block_height(opts.rpc_url()).await?),
    }
}

pub async fn final_block_height(rpc_url: &str) -> anyhow::Result<u64> {
    let client = JsonRpcClient::connect(rpc_url.to_string());
    let request = methods::block::RpcBlockRequest {
        block_reference: BlockReference::Finality(Finality::Final),
    };

    let latest_block = client.call(request).await?;

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
