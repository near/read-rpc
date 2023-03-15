use crate::config::ServerContext;
use crate::errors::RPCError;
use crate::modules::blocks::utils::{
    fetch_block_from_s3, fetch_chunk_from_s3, scylla_db_convert_block_hash_to_block_height,
    scylla_db_convert_chunk_hash_to_block_height_and_shard_id,
};
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
                scylla_db_convert_block_hash_to_block_height(&data.scylla_db_manager, block_hash)
                    .await?
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
pub async fn fetch_chunk(
    data: &Data<ServerContext>,
    chunk_reference: near_jsonrpc_primitives::types::chunks::ChunkReference,
) -> anyhow::Result<near_primitives::views::ChunkView> {
    let (block_height, shard_id) = match chunk_reference {
        near_jsonrpc_primitives::types::chunks::ChunkReference::BlockShardId {
            block_id,
            shard_id,
        } => match block_id {
            near_primitives::types::BlockId::Height(block_height) => (block_height, shard_id),
            near_primitives::types::BlockId::Hash(block_hash) => {
                let block_height = scylla_db_convert_block_hash_to_block_height(
                    &data.scylla_db_manager,
                    block_hash,
                )
                .await?;
                (block_height, shard_id)
            }
        },
        near_jsonrpc_primitives::types::chunks::ChunkReference::ChunkHash { chunk_id } => {
            scylla_db_convert_chunk_hash_to_block_height_and_shard_id(
                &data.scylla_db_manager,
                chunk_id,
            )
            .await?
        }
    };
    Ok(fetch_chunk_from_s3(
        &data.s3_client,
        &data.s3_bucket_name,
        block_height,
        shard_id,
    )
    .await?)
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

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn chunk(
    data: Data<ServerContext>,
    Params(params): Params<near_jsonrpc_primitives::types::chunks::RpcChunkRequest>,
) -> Result<near_jsonrpc_primitives::types::chunks::RpcChunkResponse, RPCError> {
    tracing::debug!("`chunk` called with parameters: {:?}", params);

    match fetch_chunk(&data, params.chunk_reference.clone()).await {
        Ok(chunk_view) => {
            Ok(near_jsonrpc_primitives::types::chunks::RpcChunkResponse { chunk_view })
        }
        Err(err) => {
            tracing::warn!("`chunk` error: {:?}", err);
            let chunk_view = proxy_rpc_call(&data.near_rpc_client, params).await?;
            Ok(near_jsonrpc_primitives::types::chunks::RpcChunkResponse { chunk_view })
        }
    }
}
