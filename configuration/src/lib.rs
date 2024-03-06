use std::path::PathBuf;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

mod configs;

pub use crate::configs::database::DatabaseConfig;
pub use crate::configs::general::ChainId;
pub use crate::configs::{
    EpochIndexerConfig, NearStateIndexerConfig, RpcServerConfig, StateIndexerConfig,
    TxIndexerConfig,
};

pub async fn read_configuration<T>() -> anyhow::Result<T>
where
    T: configs::Config + Send + Sync + 'static,
{
    let path_root = find_configs_root().await?;
    load_env(path_root.clone()).await?;
    let common_config = read_toml_file(path_root).await?;
    Ok(T::from_common_config(common_config))
}

pub async fn init_tracing(service_name: &str) -> anyhow::Result<()> {
    let path_root = find_configs_root().await?;
    load_env(path_root.clone()).await?;

    let mut env_filter = tracing_subscriber::EnvFilter::new(format!("{}=info", service_name));

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

async fn load_env(mut path_root: PathBuf) -> anyhow::Result<()> {
    path_root.push(".env");
    if path_root.exists() {
        dotenv::from_path(path_root.as_path()).ok();
    } else {
        dotenv::dotenv().ok();
    }
    Ok(())
}

async fn read_toml_file(mut path_root: PathBuf) -> anyhow::Result<configs::CommonConfig> {
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

async fn find_configs_root() -> anyhow::Result<PathBuf> {
    let current_path = std::env::current_dir()?;

    for path_config in current_path.as_path().ancestors() {
        let has_config = std::fs::read_dir(path_config)?.any(|path| {
            path.unwrap().file_name() == std::ffi::OsString::from(String::from("config.toml"))
        });
        if has_config {
            return Ok(PathBuf::from(path_config));
        }
    }
    anyhow::bail!("Ran out of places to find config.toml")
}
