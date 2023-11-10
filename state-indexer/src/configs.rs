pub use clap::{Parser, Subcommand};
use near_jsonrpc_client::{methods, JsonRpcClient};
use near_lake_framework::near_indexer_primitives::types::{BlockReference, Finality};

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
    /// DB connection string
    #[clap(long, default_value = "127.0.0.1:9042", env)]
    pub database_url: String,
    /// DB user(login)
    #[clap(long, env)]
    pub database_user: Option<String>,
    /// DB password
    #[clap(long, env)]
    pub database_password: Option<String>,
    /// Metrics HTTP server port
    #[clap(long, default_value = "8080", env)]
    pub port: u16,
    #[clap(long, default_value = "1", env)]
    pub concurrency: usize,
    /// Chain ID: testnet or mainnet
    #[clap(subcommand)]
    pub chain_id: ChainId,

    /// ScyllaDB preferred DataCenter
    /// Accepts the DC name of the ScyllaDB to filter the connection to that DC only (preferrably).
    /// If you connect to multi-DC cluter, you might experience big latencies while working with the DB. This is due to the fact that ScyllaDB driver tries to connect to any of the nodes in the cluster disregarding of the location of the DC. This option allows to filter the connection to the DC you need. Example: "DC1" where DC1 is located in the same region as the application.
    #[cfg(feature = "scylla_db")]
    #[clap(long, env)]
    pub preferred_dc: Option<String>,

    /// Max retry count for ScyllaDB if `strict_mode` is `false`
    #[cfg(feature = "scylla_db")]
    #[clap(long, env, default_value_t = 5)]
    pub max_retry: u8,

    /// Attempts to store data in the database should be infinite to ensure no data is missing.
    /// Disable it to perform a limited write attempts (`max_retry`)
    /// before skipping giving up and moving to the next piece of data
    #[cfg(feature = "scylla_db")]
    #[clap(long, env, default_value_t = true)]
    pub strict_mode: bool,

    /// Postgres database name
    #[cfg(feature = "postgres_db")]
    #[clap(long, env)]
    pub database_name: Option<String>,
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
        /// Fallback start block height if interruption block is not found
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
        match &self.chain_id {
            ChainId::Mainnet(_) => "https://rpc.mainnet.near.org",
            ChainId::Testnet(_) => "https://rpc.testnet.near.org",
        }
    }
}

impl Opts {
    pub async fn to_additional_database_options(&self) -> database::AdditionalDatabaseOptions {
        #[cfg(feature = "scylla_db")]
        let database_options = database::AdditionalDatabaseOptions {
            preferred_dc: self.preferred_dc.clone(),
            keepalive_interval: None,
            max_retry: self.max_retry,
            strict_mode: self.strict_mode,
        };

        #[cfg(feature = "postgres_db")]
        let database_options = database::AdditionalDatabaseOptions {
            database_name: self.database_name.clone(),
        };

        database_options
    }

    pub async fn to_lake_config(
        &self,
        db_manager: &(impl database::StateIndexerDbManager + Sync + Send + 'static),
    ) -> anyhow::Result<near_lake_framework::LakeConfig> {
        let config_builder = near_lake_framework::LakeConfigBuilder::default();

        Ok(match &self.chain_id {
            ChainId::Mainnet(_) => config_builder
                .mainnet()
                .start_block_height(get_start_block_height(self, db_manager).await?),
            ChainId::Testnet(_) => config_builder
                .testnet()
                .start_block_height(get_start_block_height(self, db_manager).await?),
        }
        .build()
        .expect("Failed to build LakeConfig"))
    }
}

async fn get_start_block_height(
    opts: &Opts,
    db_manager: &(impl database::StateIndexerDbManager + Sync + Send + 'static),
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

pub(crate) async fn final_block_height(rpc_url: &str) -> anyhow::Result<u64> {
    tracing::debug!(target: crate::INDEXER, "Fetching final block from NEAR RPC",);
    let client = JsonRpcClient::connect(rpc_url);
    let request = methods::block::RpcBlockRequest {
        block_reference: BlockReference::Finality(Finality::Final),
    };

    let latest_block = client.call(request).await?;

    Ok(latest_block.header.height)
}

pub(crate) fn init_tracing() -> anyhow::Result<()> {
    let mut env_filter = tracing_subscriber::EnvFilter::new("state_indexer=info");

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

    opentelemetry::global::set_text_map_propagator(opentelemetry::sdk::propagation::TraceContextPropagator::new());

    #[cfg(feature = "tracing-instrumentation")]
    let subscriber = {
        let tracer = opentelemetry_jaeger::new_collector_pipeline()
            .with_service_name("state_indexer")
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

        tracing_subscriber::Registry::default().with(env_filter).with(telemetry)
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
