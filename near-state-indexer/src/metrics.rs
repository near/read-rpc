use near_o11y::metrics::*;
use once_cell::sync::Lazy;

pub static BLOCKS_DONE: Lazy<IntCounter> = Lazy::new(|| {
    try_create_int_counter(
        "near_lake_block_done_total",
        "Total number of indexed blocks",
    )
    .unwrap()
});

// This metric is present in the near_o11y crate but it's not public
// so we can't use it directly. We have to redefine it here.
pub static NODE_BUILD_INFO: Lazy<IntCounterVec> = Lazy::new(|| {
    try_create_int_counter_vec(
        "near_lake_build_info",
        "Metric whose labels indicate node’s version; see \
             <https://www.robustperception.io/exposing-the-software-version-to-prometheus>.",
        &["release", "build", "rustc_version"],
    )
    .unwrap()
});

#[derive(Debug, Clone)]
pub struct Stats {
    pub block_heights_processing: std::collections::BTreeSet<u64>,
    pub blocks_processed_count: u64,
    pub last_processed_block_height: u64,
    pub current_epoch_id: Option<near_indexer::near_primitives::hash::CryptoHash>,
    pub current_epoch_height: u64,
}

impl Stats {
    pub fn new() -> Self {
        Self {
            block_heights_processing: std::collections::BTreeSet::new(),
            blocks_processed_count: 0,
            last_processed_block_height: 0,
            current_epoch_id: None,
            current_epoch_height: 0,
        }
    }
}

pub async fn state_logger(
    stats: std::sync::Arc<tokio::sync::RwLock<Stats>>,
    view_client: actix::Addr<near_client::ViewClientActor>,
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
            if let Ok(block_height) = crate::utils::fetch_latest_block(&view_client).await {
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
                format!(" | {} to catch up the tip", humantime::format_duration(duration))
            } else {
                "".to_string()
            }
        );
        prev_blocks_processed_count = stats_lock.blocks_processed_count;
    }
}
