use std::io::Write;
use std::path::PathBuf;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use validator::Validate;

mod configs;
mod default_env_configs;

pub use crate::configs::database::DatabaseConfig;
pub use crate::configs::general::ChainId;
pub use crate::configs::{
    IndexerConfig, RightsizingConfig, RpcServerConfig, StateIndexerConfig, TxIndexerConfig,
};

/// Two new shard layouts included in nearcore 2.5.0. New protocol version is 74.
/// The network will undergo two resharding events and the number of shards will increase from 6 to 8 shards.
/// We should freeze the shard layout for read_rpc. The shard layout will be updated in the future.
pub const SHARD_LAYOUT_PROTOCOL_VERSION:
    near_lake_framework::near_indexer_primitives::types::ProtocolVersion = 73;

// Read the configuration file and parse it into a struct
// Validate the struct and return it
// If the config file does not exist, create a default one
// If the config file is invalid, panic
pub async fn read_configuration<T>() -> anyhow::Result<T>
where
    T: configs::Config + Send + Sync + 'static,
{
    let path_root = find_configs_root()?;
    load_env(path_root.clone()).await?;
    let common_config = read_toml_file(path_root)?;

    if let Err(validation_errors) = common_config.validate() {
        panic!("Failed to validate config: {validation_errors}");
    }

    Ok(T::from_common_config(common_config))
}

// Initialize the tracing subscriber with the given service name
// and the environment variables
pub async fn init_tracing(service_name: &str) -> anyhow::Result<()> {
    let path_root = find_configs_root()?;
    load_env(path_root.clone()).await?;

    let mut env_filter = tracing_subscriber::EnvFilter::new(format!("{}=info,info", service_name));

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
            .with_service_name(service_name)
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

// Load the environment variables from the .env file
async fn load_env(mut path_root: PathBuf) -> anyhow::Result<()> {
    path_root.push(".env");
    if path_root.exists() {
        dotenv::from_path(path_root.as_path()).ok();
    } else {
        dotenv::dotenv().ok();
    }
    Ok(())
}

// Read the config file and parse it into a struct
fn read_toml_file(mut path_root: PathBuf) -> anyhow::Result<configs::CommonConfig> {
    path_root.push("config.toml");
    match std::fs::read_to_string(path_root.as_path()) {
        Ok(content) => match toml::from_str::<configs::CommonConfig>(&content) {
            Ok(config) => Ok(config),
            Err(err) => {
                anyhow::bail!(
                    "Unable to load data from: {:?}.\n Error: {}",
                    path_root.to_str(),
                    err
                );
            }
        },
        Err(err) => {
            anyhow::bail!(
                "Could not read file: {:?}.\n Error: {}",
                path_root.to_str(),
                err
            );
        }
    }
}

// Create default config file if it does not exist
fn create_default_config_file(mut path_config: PathBuf) -> anyhow::Result<()> {
    path_config.push("config.toml");
    let mut file = std::fs::File::create(path_config.clone())?;
    file.write_all(default_env_configs::DEFAULT_CONFIG.as_bytes())?;
    tracing::info!("Config file created at: {:?}", path_config);
    Ok(())
}

// Find the root of the project where the config file is located
// If the config file does not exist, create a default one
fn find_configs_root() -> anyhow::Result<PathBuf> {
    let current_path = std::env::current_dir()?;

    for path_config in current_path.as_path().ancestors() {
        let has_config = std::fs::read_dir(path_config)?.any(|path| {
            path.unwrap().file_name() == std::ffi::OsString::from(String::from("config.toml"))
        });
        if has_config {
            return Ok(PathBuf::from(path_config));
        }
    }

    tracing::warn!("Config file does not exist. Creating new default...");
    create_default_config_file(current_path.clone())?;

    Ok(current_path)
}

// Helper function to get shard layout for the `SHARD_LAYOUT_PROTOCOL_VERSION` protocol version
pub fn shard_layout() -> anyhow::Result<near_primitives::shard_layout::ShardLayout> {
    let genesis_config = read_genesis_config_from_root()?;
    let default_epoch_config = near_primitives::epoch_manager::EpochConfig::from(&genesis_config);
    let all_epoch_config = near_primitives::epoch_manager::AllEpochConfig::new(
        true,
        genesis_config.protocol_version,
        default_epoch_config,
        &genesis_config.chain_id,
    );
    let epoch_config = all_epoch_config.for_protocol_version(SHARD_LAYOUT_PROTOCOL_VERSION);
    Ok(epoch_config.shard_layout)
}

// Private helper to read genesis_config.json from the project root
pub fn read_genesis_config_from_root() -> anyhow::Result<near_chain_configs::GenesisConfig> {
    let mut path_root = find_configs_root()?;
    path_root.push("genesis_config.json");

    if !path_root.exists() {
        tracing::warn!("genesis_config.json does not exist. Downloading from RPC...");
        download_genesis_config(&path_root)?;
    }

    let content = std::fs::read_to_string(&path_root)
        .map_err(|err| anyhow::anyhow!("Could not read file: {:?}.\n Error: {}", path_root, err))?;
    serde_json::from_str::<near_chain_configs::GenesisConfig>(&content).map_err(|err| {
        anyhow::anyhow!(
            "Unable to load data from: {:?}.\n Error: {}",
            path_root,
            err
        )
    })
}

// Private helper to download genesis_config.json from RPC
fn download_genesis_config(path: &std::path::Path) -> anyhow::Result<()> {
    let rpc_url = std::env::var("NEAR_RPC_URL")
        .unwrap_or_else(|_| "https://rpc.mainnet.fastnear.com".to_string());

    let body = r#"{"jsonrpc":"2.0","id":"dontcare","method":"EXPERIMENTAL_genesis_config"}"#;
    let response: serde_json::Value = {
        let resp = ureq::post(&rpc_url)
            .set("Content-Type", "application/json")
            .send_string(body)
            .map_err(|e| anyhow::anyhow!("Failed to fetch genesis config: {}", e))?;
        serde_json::from_reader(resp.into_reader())
            .map_err(|e| anyhow::anyhow!("Failed to parse RPC response: {}", e))?
    };

    let result = response
        .get("result")
        .ok_or_else(|| anyhow::anyhow!("No 'result' field in RPC response"))?;

    std::fs::write(path, serde_json::to_vec_pretty(result)?)
        .map_err(|e| anyhow::anyhow!("Failed to write genesis_config.json: {}", e))?;

    tracing::info!("Downloaded genesis_config.json to {:?}", path);
    Ok(())
}
