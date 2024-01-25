pub use clap::{Parser, Subcommand};
use near_jsonrpc_client::{methods, JsonRpcClient};
use near_lake_framework::near_indexer_primitives::types::{BlockReference, Finality};

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
        /// Fallback start block height if interruption block is not found
        height: Option<u64>,
    },
    FromLatest,
}

pub async fn get_start_block_height(
    rpc_client: &JsonRpcClient,
    db_manager: &(impl database::StateIndexerDbManager + Sync + Send + 'static),
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

pub(crate) async fn final_block_height(rpc_client: &JsonRpcClient) -> anyhow::Result<u64> {
    tracing::debug!(target: crate::INDEXER, "Fetching final block from NEAR RPC",);
    let request = methods::block::RpcBlockRequest {
        block_reference: BlockReference::Finality(Finality::Final),
    };

    let latest_block = rpc_client.call(request).await?;

    Ok(latest_block.header.height)
}
