use crate::config::ServerContext;
use crate::errors::RPCError;
use crate::modules::blocks::utils::{
    fetch_block_from_s3, fetch_block_height_from_db, fetch_latest_block_height_from_db,
    fetch_latest_block_height_from_redis,
};
use jsonrpc_v2::{Data, Params};

pub async fn fetch_block(
    data: &Data<ServerContext>,
    block_reference: near_primitives::types::BlockReference,
) -> anyhow::Result<near_primitives::views::BlockView> {
    tracing::debug!(target: "jsonrpc - block - fetch_block", "call fetch_block");
    let block_height = match block_reference {
        near_primitives::types::BlockReference::BlockId(block_id) => match block_id {
            near_primitives::types::BlockId::Height(block_height) => block_height,
            near_primitives::types::BlockId::Hash(block_hash) => {
                fetch_block_height_from_db(&data.db_client, block_hash).await?
            }
        },
        near_primitives::types::BlockReference::Finality(finality) => match finality {
            near_primitives::types::Finality::Final => {
                match fetch_latest_block_height_from_redis(data.redis_client.clone()).await {
                    Ok(height) => height,
                    Err(_) => fetch_latest_block_height_from_db(&data.db_client).await?,
                }
            }
            _ => anyhow::bail!("Finality other than final is not supported"),
        },
        near_primitives::types::BlockReference::SyncCheckpoint(_) => {
            anyhow::bail!("SyncCheckpoint is not supported")
        }
    };

    Ok(fetch_block_from_s3(&data.s3_client, &data.s3_bucket_name, block_height).await?)
}

pub async fn block(
    data: Data<ServerContext>,
    Params(params): Params<near_jsonrpc_primitives::types::blocks::RpcBlockRequest>,
) -> Result<near_jsonrpc_primitives::types::blocks::RpcBlockResponse, RPCError> {
    tracing::debug!(target: "jsonrpc - block", "Params: {:?}", params);
    match fetch_block(&data, params.block_reference.clone()).await {
        Ok(block_view) => {
            Ok(near_jsonrpc_primitives::types::blocks::RpcBlockResponse { block_view })
        }
        Err(_) => {
            tracing::debug!(target: "jsonrpc - block", "Block not found. Proxy to near rpc");
            let block_view = data.near_rpc_client.call(params).await?;
            Ok(near_jsonrpc_primitives::types::blocks::RpcBlockResponse { block_view })
        }
    }
}

pub async fn chunk(
    Params(_params): Params<near_jsonrpc_primitives::types::chunks::RpcChunkRequest>,
) -> Result<near_jsonrpc_primitives::types::chunks::RpcChunkResponse, RPCError> {
    unimplemented!("chunk - Unimplemented")
}
