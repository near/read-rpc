use crate::config::ServerContext;
use crate::errors::RPCError;
use crate::modules::blocks::utils::{fetch_block_from_s3, fetch_block_height_from_scylla_db};
use crate::utils::proxy_rpc_call;
use jsonrpc_v2::{Data, Params};

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn fetch_block(
    data: &Data<ServerContext>,
    block_reference: near_primitives::types::BlockReference,
) -> anyhow::Result<near_primitives::views::BlockView> {
    tracing::debug!("`fetch_block` call");
    let block_height = match block_reference {
        near_primitives::types::BlockReference::BlockId(block_id) => match block_id {
            near_primitives::types::BlockId::Height(block_height) => block_height,
            near_primitives::types::BlockId::Hash(block_hash) => {
                fetch_block_height_from_scylla_db(&data.scylla_db_client, block_hash).await?
            }
        },
        near_primitives::types::BlockReference::Finality(finality) => match finality {
            near_primitives::types::Finality::Final => data
                .final_block_height
                .load(std::sync::atomic::Ordering::SeqCst),
            _ => anyhow::bail!("Finality other than final is not supported"),
        },
        near_primitives::types::BlockReference::SyncCheckpoint(_) => {
            anyhow::bail!("SyncCheckpoint is not supported")
        }
    };

    Ok(fetch_block_from_s3(&data.s3_client, &data.s3_bucket_name, block_height).await?)
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn block(
    data: Data<ServerContext>,
    Params(params): Params<near_jsonrpc_primitives::types::blocks::RpcBlockRequest>,
) -> Result<near_jsonrpc_primitives::types::blocks::RpcBlockResponse, RPCError> {
    tracing::debug!("`block` called with parameters: {:?}", params);
    match fetch_block(&data, params.block_reference.clone()).await {
        Ok(block_view) => {
            Ok(near_jsonrpc_primitives::types::blocks::RpcBlockResponse { block_view })
        }
        Err(err) => {
            tracing::warn!("`block` error: {:?}", err);
            let block_view = proxy_rpc_call(&data.near_rpc_client, params).await?;
            Ok(near_jsonrpc_primitives::types::blocks::RpcBlockResponse { block_view })
        }
    }
}

pub async fn chunk(
    Params(_params): Params<near_jsonrpc_primitives::types::chunks::RpcChunkRequest>,
) -> Result<near_jsonrpc_primitives::types::chunks::RpcChunkResponse, RPCError> {
    unimplemented!("chunk - Unimplemented")
}
