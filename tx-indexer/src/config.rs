pub use clap::{Parser, Subcommand};
use near_indexer_primitives::types::{BlockReference, Finality};
use near_jsonrpc_client::{methods, JsonRpcClient};

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
    rpc_client: &JsonRpcClient,
    db_manager: &std::sync::Arc<Box<dyn database::TxIndexerDbManager + Sync + Send + 'static>>,
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
                final_block_height(rpc_client).await?
            }
        }
        StartOptions::FromLatest => final_block_height(rpc_client).await?,
    };
    Ok(start_block_height - 100) // Start just a bit earlier to avoid missing transactions
}

pub async fn final_block_height(rpc_client: &JsonRpcClient) -> anyhow::Result<u64> {
    let request = methods::block::RpcBlockRequest {
        block_reference: BlockReference::Finality(Finality::Final),
    };

    let latest_block = rpc_client.call(request).await?;

    Ok(latest_block.header.height)
}
