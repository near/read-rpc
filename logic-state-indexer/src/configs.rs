pub use clap::{Parser, Subcommand};
use near_chain_configs::GenesisConfig;
use near_primitives::epoch_manager::{AllEpochConfig, EpochConfig};

/// NEAR Indexer for Explorer
/// Watches for stream of blocks from the chain
#[derive(Parser, Debug)]
#[command(version = concat!(env!("CARGO_PKG_VERSION"), "\nnearcore ", env!("NEARCORE_VERSION")))]
pub struct Opts {
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
        /// Fallback start block height if interruption block is not found
        height: Option<u64>,
    },
    FromLatest,
}

pub async fn get_start_block_height(
    near_client: &impl crate::NearClient,
    db_manager: &(impl database::StateIndexerDbManager + Sync + Send + 'static),
    start_options: &StartOptions,
    indexer_id: &str,
) -> anyhow::Result<u64> {
    let start_block_height = match start_options {
        StartOptions::FromBlock { height } => *height,
        StartOptions::FromInterruption { height } => {
            if let Ok(block_height) = db_manager.get_last_processed_block_height(indexer_id).await {
                block_height
            } else if let Some(height) = height {
                *height
            } else {
                final_block_height(near_client).await?
            }
        }
        StartOptions::FromLatest => final_block_height(near_client).await?,
    };
    Ok(start_block_height - 100) // Start just a bit earlier to overlap indexed blocks to ensure we don't miss anything in-between
}

pub(crate) async fn final_block_height(
    near_client: &impl crate::NearClient,
) -> anyhow::Result<u64> {
    tracing::debug!(target: crate::INDEXER, "Fetching final block from NEAR RPC",);
    near_client.final_block_height().await
}

pub async fn shard_layout(
    genesis_config: GenesisConfig,
) -> anyhow::Result<near_primitives::shard_layout::ShardLayout> {
    let default_epoch_config = EpochConfig::from(&genesis_config);
    let all_epoch_config = AllEpochConfig::new(
        true,
        genesis_config.protocol_version,
        default_epoch_config,
        &genesis_config.chain_id,
    );
    let epoch_config =
        all_epoch_config.for_protocol_version(near_primitives::version::PROTOCOL_VERSION);
    Ok(epoch_config.shard_layout)
}
