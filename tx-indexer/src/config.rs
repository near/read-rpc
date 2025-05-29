pub use clap::{Parser, Subcommand};
use tx_details_storage::TxDetailsStorage;

/// NEAR Indexer for Explorer
/// Watches for stream of blocks from the chain
#[derive(Parser, Debug)]
#[command(version = concat!(env!("CARGO_PKG_VERSION"), "\nnearcore ", env!("NEARCORE_VERSION")))]
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
    fastnear_client: &near_lake_framework::FastNearClient,
    tx_details_storage: &std::sync::Arc<TxDetailsStorage>,
    start_options: &StartOptions,
    indexer_id: &str,
) -> anyhow::Result<u64> {
    let start_block_height = match start_options {
        StartOptions::FromBlock { height } => *height,
        StartOptions::FromInterruption { height } => {
            if let Ok(block_height) = tx_details_storage
                .get_last_processed_block_height(indexer_id)
                .await
            {
                block_height
            } else if let Some(height) = height {
                *height
            } else {
                final_block_height(fastnear_client).await?
            }
        }
        StartOptions::FromLatest => final_block_height(fastnear_client).await?,
    };
    Ok(start_block_height - 100) // Start just a bit earlier to overlap indexed blocks to ensure we don't miss anything in-between
}

pub async fn final_block_height(
    fastnear_client: &near_lake_framework::FastNearClient,
) -> anyhow::Result<u64> {
    let latest_block = near_lake_framework::fastnear::fetchers::fetch_block_by_finality(
        fastnear_client,
        near_indexer_primitives::types::Finality::Final,
    )
    .await;
    Ok(latest_block.header.height)
}
