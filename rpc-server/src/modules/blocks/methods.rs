use crate::config::ServerContext;
use crate::errors::RPCError;
use crate::modules::blocks::utils::{
    fetch_block_from_s3, fetch_chunk_from_s3, fetch_shard_from_s3, is_matching_change,
    scylla_db_convert_block_hash_to_block_height,
    scylla_db_convert_chunk_hash_to_block_height_and_shard_id,
};
use crate::utils::proxy_rpc_call;
use jsonrpc_v2::{Data, Params};
use near_primitives::views::StateChangeValueView;

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
async fn fetch_shards(
    data: &Data<ServerContext>,
    block_view: &near_primitives::views::BlockView,
) -> anyhow::Result<Vec<near_indexer_primitives::IndexerShard>> {
    let fetch_shards_futures = (0..block_view.chunks.len() as u64)
        .collect::<Vec<u64>>()
        .into_iter()
        .map(|shard_id| {
            fetch_shard_from_s3(
                &data.s3_client,
                &data.s3_bucket_name,
                block_view.header.height,
                shard_id,
            )
        });
    Ok(futures::future::try_join_all(fetch_shards_futures).await?)
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn fetch_changes_in_block(
    data: &Data<ServerContext>,
    block_reference: near_primitives::types::BlockReference,
) -> anyhow::Result<near_jsonrpc_primitives::types::changes::RpcStateChangesInBlockByTypeResponse> {
    let block_view = fetch_block(&data, block_reference).await?;
    let shards = fetch_shards(&data, &block_view).await?;
    let changes = shards
        .into_iter()
        .flat_map(|shard| {
            shard
                .state_changes
                .into_iter()
                .map(|change| match change.value {
                    StateChangeValueView::AccountUpdate { account_id, .. }
                    | StateChangeValueView::AccountDeletion { account_id } => {
                        near_primitives::views::StateChangeKindView::AccountTouched { account_id }
                    }
                    StateChangeValueView::AccessKeyUpdate { account_id, .. }
                    | StateChangeValueView::AccessKeyDeletion { account_id, .. } => {
                        near_primitives::views::StateChangeKindView::AccessKeyTouched { account_id }
                    }
                    StateChangeValueView::DataUpdate { account_id, .. }
                    | StateChangeValueView::DataDeletion { account_id, .. } => {
                        near_primitives::views::StateChangeKindView::DataTouched { account_id }
                    }
                    StateChangeValueView::ContractCodeUpdate { account_id, .. }
                    | StateChangeValueView::ContractCodeDeletion { account_id } => {
                        near_primitives::views::StateChangeKindView::ContractCodeTouched {
                            account_id,
                        }
                    }
                })
        })
        .collect();
    Ok(
        near_jsonrpc_primitives::types::changes::RpcStateChangesInBlockByTypeResponse {
            block_hash: block_view.header.hash,
            changes,
        },
    )
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn fetch_changes_in_block_by_type(
    data: &Data<ServerContext>,
    block_reference: near_primitives::types::BlockReference,
    state_changes_request: &near_primitives::views::StateChangesRequestView,
) -> anyhow::Result<near_jsonrpc_primitives::types::changes::RpcStateChangesInBlockResponse> {
    let block_view = fetch_block(&data, block_reference).await?;
    let shards = fetch_shards(&data, &block_view).await?;
    let changes = shards
        .into_iter()
        .flat_map(|shard| shard.state_changes)
        .filter(|change| is_matching_change(change, &state_changes_request))
        .collect();
    Ok(
        near_jsonrpc_primitives::types::changes::RpcStateChangesInBlockResponse {
            block_hash: block_view.header.hash,
            changes,
        },
    )
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn block(
    data: Data<ServerContext>,
    Params(params): Params<near_jsonrpc_primitives::types::blocks::RpcBlockRequest>,
) -> Result<near_jsonrpc_primitives::types::blocks::RpcBlockResponse, RPCError> {
    tracing::debug!("`block` called with parameters: {:?}", params);
    let block_view = match fetch_block(&data, params.block_reference.clone()).await {
        Ok(block_view) => block_view,
        Err(err) => {
            tracing::warn!("`block` error: {:?}", err);
            proxy_rpc_call(&data.near_rpc_client, params).await?
        }
    };
    Ok(near_jsonrpc_primitives::types::blocks::RpcBlockResponse { block_view })
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn changes_in_block(
    data: Data<ServerContext>,
    Params(params): Params<near_jsonrpc_primitives::types::changes::RpcStateChangesInBlockRequest>,
) -> Result<near_jsonrpc_primitives::types::changes::RpcStateChangesInBlockByTypeResponse, RPCError>
{
    match fetch_changes_in_block(&data, params.block_reference.clone()).await {
        Ok(changes) => Ok(changes),
        Err(err) => {
            tracing::warn!("`changes_in_block` error: {:?}", err);
            let response = proxy_rpc_call(&data.near_rpc_client, params).await?;
            Ok(response)
        }
    }
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn changes_in_block_by_type(
    data: Data<ServerContext>,
    Params(params): Params<
        near_jsonrpc_primitives::types::changes::RpcStateChangesInBlockByTypeRequest,
    >,
) -> Result<near_jsonrpc_primitives::types::changes::RpcStateChangesInBlockResponse, RPCError> {
    match fetch_changes_in_block_by_type(
        &data,
        params.block_reference.clone(),
        &params.state_changes_request,
    )
    .await
    {
        Ok(changes) => Ok(changes),
        Err(err) => {
            tracing::warn!("`changes_in_block` error: {:?}", err);
            Ok(proxy_rpc_call(&data.near_rpc_client, params).await?)
        }
    }
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn chunk(
    data: Data<ServerContext>,
    Params(params): Params<near_jsonrpc_primitives::types::chunks::RpcChunkRequest>,
) -> Result<near_jsonrpc_primitives::types::chunks::RpcChunkResponse, RPCError> {
    tracing::debug!("`chunk` called with parameters: {:?}", params);

    let chunk_view = match fetch_chunk(&data, params.chunk_reference.clone()).await {
        Ok(chunk_view) => chunk_view,
        Err(err) => {
            tracing::warn!("`chunk` error: {:?}", err);
            proxy_rpc_call(&data.near_rpc_client, params).await?
        }
    };
    Ok(near_jsonrpc_primitives::types::chunks::RpcChunkResponse { chunk_view })
}
