use actix_web::{get, App, HttpServer, Responder};
use near_jsonrpc_client::JsonRpcClient;
use prometheus::{Encoder, IntCounter, IntGauge, Opts};

type Result<T, E> = std::result::Result<T, E>;

fn try_create_int_counter(name: &str, help: &str) -> Result<IntCounter, prometheus::Error> {
    let opts = Opts::new(name, help);
    let counter = IntCounter::with_opts(opts)?;
    prometheus::register(Box::new(counter.clone()))?;
    Ok(counter)
}

fn try_create_int_gauge(name: &str, help: &str) -> Result<IntGauge, prometheus::Error> {
    let opts = Opts::new(name, help);
    let gauge = IntGauge::with_opts(opts)?;
    prometheus::register(Box::new(gauge.clone()))?;
    Ok(gauge)
}

lazy_static! {
    pub(crate) static ref BLOCK_PROCESSED_TOTAL: IntCounter = try_create_int_counter(
        "total_blocks_processed",
        "Total number of blocks processed by indexer regardless of restarts. Used to calculate Block Processing Rate(BPS)"
    )
    .unwrap();
    pub(crate) static ref LATEST_BLOCK_HEIGHT: IntGauge = try_create_int_gauge(
        "latest_block_height",
        "Last seen block height by indexer"
    )
    .unwrap();
    pub(crate) static ref TX_IN_MEMORY_CACHE: IntGauge = try_create_int_gauge(
        "tx_in_memory_cache",
        "Number of transactions in memory cache"
    )
    .unwrap();
    pub(crate) static ref RECEIPTS_IN_MEMORY_CACHE: IntGauge = try_create_int_gauge(
        "receipts_in_memory_cache",
        "Number of receipts in memory cache"
    )
    .unwrap();
    pub(crate) static ref TX_STORE_ERRORS_TOTAL: IntCounter = try_create_int_counter(
        "total_tx_store_errors",
        "Total number of errors while storing transactions"
    )
    .unwrap();
}

#[get("/metrics")]
async fn get_metrics() -> impl Responder {
    let encoder = prometheus::TextEncoder::new();

    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&prometheus::gather(), &mut buffer) {
        tracing::error!(target: crate::INDEXER, "could not encode metrics: {}", e);
    };

    match String::from_utf8(buffer.clone()) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(
                target: crate::INDEXER,
                "custom metrics could not be from_utf8'd: {}",
                e
            );
            String::default()
        }
    }
}

pub(crate) fn init_server(port: u16) -> anyhow::Result<actix_web::dev::Server> {
    tracing::info!(
        target: crate::INDEXER,
        "Starting metrics server on http://0.0.0.0:{port}/metrics"
    );

    Ok(HttpServer::new(|| App::new().service(get_metrics))
        .bind(("0.0.0.0", port))?
        .disable_signals()
        .run())
}

#[derive(Debug, Clone)]
pub struct Stats {
    pub block_heights_processing: std::collections::BTreeSet<u64>,
    pub blocks_processed_count: u64,
    pub last_processed_block_height: u64,
}

impl Stats {
    pub fn new() -> Self {
        Self {
            block_heights_processing: std::collections::BTreeSet::new(),
            blocks_processed_count: 0,
            last_processed_block_height: 0,
        }
    }
}

pub async fn state_logger(
    stats: std::sync::Arc<tokio::sync::RwLock<Stats>>,
    rpc_client: JsonRpcClient,
) {
    let interval_secs = 10;
    let mut prev_blocks_processed_count: u64 = 0;

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(interval_secs)).await;
        let stats_lock = stats.read().await;

        let block_processing_speed: f64 = ((stats_lock.blocks_processed_count
            - prev_blocks_processed_count) as f64)
            / (interval_secs as f64);

        let time_to_catch_the_tip_duration = if block_processing_speed > 0.0 {
            if let Ok(block_height) = crate::config::final_block_height(&rpc_client).await {
                Some(std::time::Duration::from_millis(
                    (((block_height - stats_lock.last_processed_block_height) as f64
                        / block_processing_speed)
                        * 1000f64) as u64,
                ))
            } else {
                None
            }
        } else {
            None
        };

        tracing::info!(
            target: crate::INDEXER,
            "# {} | Blocks processing: {}| Blocks done: {}. Bps {:.2} b/s {}",
            stats_lock.last_processed_block_height,
            stats_lock.block_heights_processing.len(),
            stats_lock.blocks_processed_count,
            block_processing_speed,
            if let Some(duration) = time_to_catch_the_tip_duration {
                format!(
                    " | {} to catch up the tip",
                    humantime::format_duration(duration)
                )
            } else {
                "".to_string()
            }
        );
        prev_blocks_processed_count = stats_lock.blocks_processed_count;
    }
}
