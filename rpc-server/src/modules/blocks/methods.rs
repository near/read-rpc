use crate::config::ServerContext;
use crate::errors::RPCError;
use crate::modules::blocks::utils::{
    fetch_block_from_cache_or_get, fetch_block_from_s3, fetch_chunk_from_s3, fetch_shard_from_s3,
    is_matching_change, scylla_db_convert_block_hash_to_block_height,
    scylla_db_convert_chunk_hash_to_block_height_and_shard_id,
};
#[cfg(feature = "shadow_data_consistency")]
use crate::utils::shadow_compare_results;
use jsonrpc_v2::{Data, Params};

use near_primitives::trie_key::TrieKey;
use near_primitives::views::StateChangeValueView;

#[allow(unused)]
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn block(
    data: Data<ServerContext>,
    Params(mut params): Params<near_jsonrpc_primitives::types::blocks::RpcBlockRequest>,
) -> Result<near_jsonrpc_primitives::types::blocks::RpcBlockResponse, RPCError> {
    tracing::debug!("`block` called with parameters: {:?}", params);

    // Increase the OPTIMISTIC_REQUESTS_TOTAL metric if the request has optimistic finality.
    if let near_primitives::types::BlockReference::Finality(finality) = &params.block_reference {
        // Finality::None stands for optimistic finality.
        if finality == &near_primitives::types::Finality::None {
            crate::metrics::OPTIMISTIC_REQUESTS_TOTAL.inc();
        }
    }
    crate::metrics::BLOCK_REQUESTS_TOTAL.inc();

    #[cfg(not(feature = "shadow_data_consistency"))]
    let result = fetch_block(&data, params.block_reference.clone()).await;

    #[cfg(feature = "shadow_data_consistency")]
    let result = {
        /// For the shadow comparison we need to be sure that the block height is the same for all requests
        let block = fetch_block_from_cache_or_get(&data, params.block_reference.clone()).await;
        if let near_primitives::types::BlockReference::Finality(_) = params.block_reference {
            params.block_reference = near_primitives::types::BlockReference::from(
                near_primitives::types::BlockId::Height(block.block_height),
            )
        };

        let block_response = fetch_block(&data, params.block_reference.clone()).await;
        let near_rpc_client = data.near_rpc_client.clone();
        let error_meta = format!("BLOCK: {:?}", params);
        let read_rpc_response_json = match &block_response {
            Ok(res) => serde_json::to_value(&res.block_view),
            Err(err) => serde_json::to_value(err),
        };
        let comparison_result =
            shadow_compare_results(read_rpc_response_json, near_rpc_client, params).await;

        match comparison_result {
            Ok(_) => {
                tracing::info!(target: "shadow_data_consistency", "Shadow data check: CORRECT\n{}", error_meta);
            }
            Err(err) => {
                tracing::warn!(target: "shadow_data_consistency", "Shadow data check: ERROR\n{}\n{:?}", error_meta, err);
                crate::metrics::BLOCK_PROXIES_TOTAL.inc()
            }
        };
        block_response
    };

    Ok(result.map_err(near_jsonrpc_primitives::errors::RpcError::from)?)
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn chunk(
    data: Data<ServerContext>,
    Params(params): Params<near_jsonrpc_primitives::types::chunks::RpcChunkRequest>,
) -> Result<near_jsonrpc_primitives::types::chunks::RpcChunkResponse, RPCError> {
    tracing::debug!("`chunk` called with parameters: {:?}", params);
    crate::metrics::CHUNK_REQUESTS_TOTAL.inc();
    let chunk_response = fetch_chunk(&data, params.chunk_reference.clone()).await;
    #[cfg(feature = "shadow_data_consistency")]
    {
        let near_rpc_client = data.near_rpc_client.clone();
        let error_meta = format!("CHUNK: {:?}", params);
        let read_rpc_response_json = match &chunk_response {
            Ok(res) => serde_json::to_value(&res.chunk_view),
            Err(err) => serde_json::to_value(err),
        };
        let comparison_result =
            shadow_compare_results(read_rpc_response_json, near_rpc_client, params).await;

        match comparison_result {
            Ok(_) => {
                tracing::info!(target: "shadow_data_consistency", "Shadow data check: CORRECT\n{}", error_meta);
            }
            Err(err) => {
                tracing::warn!(target: "shadow_data_consistency", "Shadow data check: ERROR\n{}\n{:?}", error_meta, err);
                crate::metrics::CHUNK_PROXIES_TOTAL.inc()
            }
        }
    }
    Ok(chunk_response.map_err(near_jsonrpc_primitives::errors::RpcError::from)?)
}

#[allow(unused_mut)]
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn changes_in_block(
    data: Data<ServerContext>,
    Params(mut params): Params<
        near_jsonrpc_primitives::types::changes::RpcStateChangesInBlockRequest,
    >,
) -> Result<near_jsonrpc_primitives::types::changes::RpcStateChangesInBlockByTypeResponse, RPCError>
{
    // Increase the OPTIMISTIC_REQUESTS_TOTAL metric if the request has optimistic finality.
    if let near_primitives::types::BlockReference::Finality(finality) = &params.block_reference {
        // Finality::None stands for optimistic finality.
        if finality == &near_primitives::types::Finality::None {
            crate::metrics::OPTIMISTIC_REQUESTS_TOTAL.inc();
        }
    }
    crate::metrics::CHNGES_IN_BLOCK_REQUESTS_TOTAL.inc();
    let block = fetch_block_from_cache_or_get(&data, params.block_reference.clone()).await;
    let changes_response = fetch_changes_in_block(&data, block).await;
    #[cfg(feature = "shadow_data_consistency")]
    {
        let near_rpc_client = data.near_rpc_client.clone();
        if let near_primitives::types::BlockReference::Finality(_) = params.block_reference {
            params.block_reference = near_primitives::types::BlockReference::from(
                near_primitives::types::BlockId::Height(block.block_height),
            )
        }
        let error_meta = format!("CHANGES_IN_BLOCK: {:?}", params);
        let read_rpc_response_json = match &changes_response {
            Ok(res) => serde_json::to_value(res),
            Err(err) => serde_json::to_value(err),
        };
        let comparison_result =
            shadow_compare_results(read_rpc_response_json, near_rpc_client, params).await;

        match comparison_result {
            Ok(_) => {
                tracing::info!(target: "shadow_data_consistency", "Shadow data check: CORRECT\n{}", error_meta);
            }
            Err(err) => {
                tracing::warn!(target: "shadow_data_consistency", "Shadow data check: ERROR\n{}\n{:?}", error_meta, err);
                crate::metrics::CHNGES_IN_BLOCK_PROXIES_TOTAL.inc()
            }
        }
    }

    Ok(changes_response.map_err(near_jsonrpc_primitives::errors::RpcError::from)?)
}

#[allow(unused_mut)]
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn changes_in_block_by_type(
    data: Data<ServerContext>,
    Params(mut params): Params<
        near_jsonrpc_primitives::types::changes::RpcStateChangesInBlockByTypeRequest,
    >,
) -> Result<near_jsonrpc_primitives::types::changes::RpcStateChangesInBlockResponse, RPCError> {
    // Increase the OPTIMISTIC_REQUESTS_TOTAL metric if the request has optimistic finality.
    if let near_primitives::types::BlockReference::Finality(finality) = &params.block_reference {
        // Finality::None stands for optimistic finality.
        if finality == &near_primitives::types::Finality::None {
            crate::metrics::OPTIMISTIC_REQUESTS_TOTAL.inc();
        }
    }
    crate::metrics::CHNGES_IN_BLOCK_BY_TYPE_REQUESTS_TOTAL.inc();
    let block = fetch_block_from_cache_or_get(&data, params.block_reference.clone()).await;
    let changes_response =
        fetch_changes_in_block_by_type(&data, block, &params.state_changes_request).await;

    #[cfg(feature = "shadow_data_consistency")]
    {
        let near_rpc_client = data.near_rpc_client.clone();
        if let near_primitives::types::BlockReference::Finality(_) = params.block_reference {
            params.block_reference = near_primitives::types::BlockReference::from(
                near_primitives::types::BlockId::Height(block.block_height),
            )
        }
        let error_meta = format!("CHANGES_IN_BLOCK_BY_TYPE: {:?}", params);
        let read_rpc_response_json = match &changes_response {
            Ok(res) => serde_json::to_value(res),
            Err(err) => serde_json::to_value(err),
        };
        let comparison_result =
            shadow_compare_results(read_rpc_response_json, near_rpc_client, params).await;

        match comparison_result {
            Ok(_) => {
                tracing::info!(target: "shadow_data_consistency", "Shadow data check: CORRECT\n{}", error_meta);
            }
            Err(err) => {
                tracing::warn!(target: "shadow_data_consistency", "Shadow data check: ERROR\n{}\n{:?}", error_meta, err);
                crate::metrics::CHNGES_IN_BLOCK_BY_TYPE_PROXIES_TOTAL.inc()
            }
        }
    }

    Ok(changes_response.map_err(near_jsonrpc_primitives::errors::RpcError::from)?)
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn fetch_block(
    data: &Data<ServerContext>,
    block_reference: near_primitives::types::BlockReference,
) -> Result<
    near_jsonrpc_primitives::types::blocks::RpcBlockResponse,
    near_jsonrpc_primitives::types::blocks::RpcBlockError,
> {
    tracing::debug!("`fetch_block` call");
    let block_height = match block_reference {
        near_primitives::types::BlockReference::BlockId(block_id) => match block_id {
            near_primitives::types::BlockId::Height(block_height) => Ok(block_height),
            near_primitives::types::BlockId::Hash(block_hash) => {
                scylla_db_convert_block_hash_to_block_height(&data.scylla_db_manager, block_hash)
                    .await
            }
        },
        near_primitives::types::BlockReference::Finality(finality) => match finality {
            near_primitives::types::Finality::Final => Ok(data
                .final_block_height
                .load(std::sync::atomic::Ordering::SeqCst)),
            _ => Err(
                near_jsonrpc_primitives::types::blocks::RpcBlockError::UnknownBlock {
                    error_message: "Finality other than final is not supported".to_string(),
                },
            ),
        },
        near_primitives::types::BlockReference::SyncCheckpoint(_) => Err(
            near_jsonrpc_primitives::types::blocks::RpcBlockError::UnknownBlock {
                error_message: "SyncCheckpoint is not supported".to_string(),
            },
        ),
    };
    let block_view =
        fetch_block_from_s3(&data.s3_client, &data.s3_bucket_name, block_height?).await?;
    Ok(near_jsonrpc_primitives::types::blocks::RpcBlockResponse { block_view })
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn fetch_chunk(
    data: &Data<ServerContext>,
    chunk_reference: near_jsonrpc_primitives::types::chunks::ChunkReference,
) -> Result<
    near_jsonrpc_primitives::types::chunks::RpcChunkResponse,
    near_jsonrpc_primitives::types::chunks::RpcChunkError,
> {
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
                .await
                .map_err(|err| {
                    near_jsonrpc_primitives::types::chunks::RpcChunkError::UnknownBlock {
                        error_message: err.to_string(),
                    }
                })?;
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
    let chunk_view = fetch_chunk_from_s3(
        &data.s3_client,
        &data.s3_bucket_name,
        block_height,
        shard_id,
    )
    .await?;
    Ok(near_jsonrpc_primitives::types::chunks::RpcChunkResponse { chunk_view })
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn fetch_changes_in_block(
    data: &Data<ServerContext>,
    block: crate::modules::blocks::CacheBlock,
) -> Result<
    near_jsonrpc_primitives::types::changes::RpcStateChangesInBlockByTypeResponse,
    near_jsonrpc_primitives::types::changes::RpcStateChangesError,
> {
    let shards = fetch_shards(data, block).await.map_err(|err| {
        near_jsonrpc_primitives::types::changes::RpcStateChangesError::InternalError {
            error_message: err.to_string(),
        }
    })?;

    let trie_keys = shards
        .into_iter()
        .flat_map(|shard| shard.state_changes)
        .map(
            |state_change_with_cause| match state_change_with_cause.value {
                StateChangeValueView::AccountUpdate { account_id, .. }
                | StateChangeValueView::AccountDeletion { account_id } => {
                    TrieKey::Account { account_id }
                }
                StateChangeValueView::DataUpdate {
                    account_id, key, ..
                }
                | StateChangeValueView::DataDeletion { account_id, key } => {
                    let key: Vec<u8> = key.into();
                    TrieKey::ContractData { account_id, key }
                }
                StateChangeValueView::ContractCodeUpdate { account_id, .. }
                | StateChangeValueView::ContractCodeDeletion { account_id } => {
                    TrieKey::ContractCode { account_id }
                }
                StateChangeValueView::AccessKeyUpdate {
                    account_id,
                    public_key,
                    ..
                }
                | StateChangeValueView::AccessKeyDeletion {
                    account_id,
                    public_key,
                } => TrieKey::AccessKey {
                    account_id,
                    public_key,
                },
            },
        );

    let mut unique_trie_keys = vec![];
    for trie_key in trie_keys {
        if let Some(prev_trie_key) = unique_trie_keys.last() {
            if prev_trie_key == &trie_key {
                continue;
            }
        }

        unique_trie_keys.push(trie_key);
    }

    let changes = unique_trie_keys
        .into_iter()
        .filter_map(|trie_key| match trie_key {
            TrieKey::Account { account_id } => {
                Some(near_primitives::views::StateChangeKindView::AccountTouched { account_id })
            }
            TrieKey::ContractData { account_id, .. } => {
                Some(near_primitives::views::StateChangeKindView::DataTouched { account_id })
            }
            TrieKey::ContractCode { account_id } => Some(
                near_primitives::views::StateChangeKindView::ContractCodeTouched { account_id },
            ),
            TrieKey::AccessKey { account_id, .. } => {
                Some(near_primitives::views::StateChangeKindView::AccessKeyTouched { account_id })
            }
            _ => None,
        })
        .collect();

    Ok(
        near_jsonrpc_primitives::types::changes::RpcStateChangesInBlockByTypeResponse {
            block_hash: block.block_hash,
            changes,
        },
    )
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn fetch_changes_in_block_by_type(
    data: &Data<ServerContext>,
    block: crate::modules::blocks::CacheBlock,
    state_changes_request: &near_primitives::views::StateChangesRequestView,
) -> Result<
    near_jsonrpc_primitives::types::changes::RpcStateChangesInBlockResponse,
    near_jsonrpc_primitives::types::changes::RpcStateChangesError,
> {
    let shards = fetch_shards(data, block).await.map_err(|err| {
        near_jsonrpc_primitives::types::changes::RpcStateChangesError::InternalError {
            error_message: err.to_string(),
        }
    })?;
    let changes = shards
        .into_iter()
        .flat_map(|shard| shard.state_changes)
        .filter(|change| is_matching_change(change, state_changes_request))
        .collect();
    Ok(
        near_jsonrpc_primitives::types::changes::RpcStateChangesInBlockResponse {
            block_hash: block.block_hash,
            changes,
        },
    )
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn fetch_shards(
    data: &Data<ServerContext>,
    block: crate::modules::blocks::CacheBlock,
) -> anyhow::Result<Vec<near_indexer_primitives::IndexerShard>> {
    let fetch_shards_futures = (0..block.chunks_count)
        .collect::<Vec<u64>>()
        .into_iter()
        .map(|shard_id| {
            fetch_shard_from_s3(
                &data.s3_client,
                &data.s3_bucket_name,
                block.block_height,
                shard_id,
            )
        });
    futures::future::try_join_all(fetch_shards_futures).await
}
