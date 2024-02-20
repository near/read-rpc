use crate::config::ServerContext;
use crate::errors::RPCError;
use crate::modules::blocks::utils::{
    fetch_block_from_cache_or_get, fetch_chunk_from_s3, is_matching_change,
};
use jsonrpc_v2::{Data, Params};
use near_primitives::trie_key::TrieKey;
use near_primitives::views::StateChangeValueView;

/// `block` rpc method implementation
/// calls proxy_rpc_call to get `block` from near-rpc if request parameters not supported by read-rpc
/// as example: block_id by Finality::None is not supported by read-rpc
/// another way to get `block` from read-rpc using `block_call`
#[allow(unused_mut)]
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn block(
    data: Data<ServerContext>,
    Params(mut params): Params<near_jsonrpc_primitives::types::blocks::RpcBlockRequest>,
) -> Result<near_jsonrpc_primitives::types::blocks::RpcBlockResponse, RPCError> {
    if let near_primitives::types::BlockReference::Finality(finality) = &params.block_reference {
        if finality != &near_primitives::types::Finality::Final {
            // Increase the OPTIMISTIC_REQUESTS_TOTAL metric if the request has
            // optimistic finality or doom_slug finality
            // and proxy to near-rpc
            crate::metrics::OPTIMISTIC_REQUESTS_TOTAL.inc();
            let block_view = data.near_rpc_client.call(params).await?;
            return Ok(near_jsonrpc_primitives::types::blocks::RpcBlockResponse { block_view });
        }
    };
    block_call(data, Params(params)).await
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn chunk(
    data: Data<ServerContext>,
    Params(params): Params<near_jsonrpc_primitives::types::chunks::RpcChunkRequest>,
) -> Result<near_jsonrpc_primitives::types::chunks::RpcChunkResponse, RPCError> {
    tracing::debug!("`chunk` called with parameters: {:?}", params);
    crate::metrics::CHUNK_REQUESTS_TOTAL.inc();
    let result = fetch_chunk(&data, params.chunk_reference.clone()).await;
    #[cfg(feature = "shadow_data_consistency")]
    {
        if let Some(err_code) = crate::utils::shadow_compare_results_handler(
            crate::metrics::CHUNK_REQUESTS_TOTAL.get(),
            data.shadow_data_consistency_rate,
            &result,
            data.near_rpc_client.clone(),
            params,
            "CHUNK",
        )
        .await
        {
            crate::utils::capture_shadow_consistency_error!(err_code, "CHUNK")
        };
    }
    Ok(result.map_err(near_jsonrpc_primitives::errors::RpcError::from)?)
}

/// `EXPERIMENTAL_changes` rpc method implementation
/// calls proxy_rpc_call to get `EXPERIMENTAL_changes` from near-rpc if request parameters not supported by read-rpc
/// as example: BlockReference for Finality::None is not supported by read-rpc
/// another way to get `EXPERIMENTAL_changes` from read-rpc using `changes_in_block_by_type_call`
#[allow(unused_mut)]
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn changes_in_block_by_type(
    data: Data<ServerContext>,
    Params(mut params): Params<
        near_jsonrpc_primitives::types::changes::RpcStateChangesInBlockByTypeRequest,
    >,
) -> Result<near_jsonrpc_primitives::types::changes::RpcStateChangesInBlockResponse, RPCError> {
    if let near_primitives::types::BlockReference::Finality(finality) = &params.block_reference {
        if finality != &near_primitives::types::Finality::Final {
            // Increase the OPTIMISTIC_REQUESTS_TOTAL metric if the request has
            // optimistic finality or doom_slug finality
            // and proxy to near-rpc
            crate::metrics::OPTIMISTIC_REQUESTS_TOTAL.inc();
            return Ok(data.near_rpc_client.call(params).await?);
        }
    };
    changes_in_block_by_type_call(data, Params(params)).await
}

/// `EXPERIMENTAL_changes_in_block` rpc method implementation
/// calls proxy_rpc_call to get `EXPERIMENTAL_changes_in_block` from near-rpc if request parameters not supported by read-rpc
/// as example: BlockReference for Finality::None is not supported by read-rpc
/// another way to get `EXPERIMENTAL_changes_in_block` from read-rpc using `changes_in_block_call`
#[allow(unused_mut)]
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn changes_in_block(
    data: Data<ServerContext>,
    Params(mut params): Params<
        near_jsonrpc_primitives::types::changes::RpcStateChangesInBlockRequest,
    >,
) -> Result<near_jsonrpc_primitives::types::changes::RpcStateChangesInBlockByTypeResponse, RPCError>
{
    if let near_primitives::types::BlockReference::Finality(finality) = &params.block_reference {
        if finality != &near_primitives::types::Finality::Final {
            // Increase the OPTIMISTIC_REQUESTS_TOTAL metric if the request has
            // optimistic finality or doom_slug finality
            // and proxy to near-rpc
            crate::metrics::OPTIMISTIC_REQUESTS_TOTAL.inc();
            return Ok(data.near_rpc_client.call(params).await?);
        }
    };
    changes_in_block_call(data, Params(params)).await
}

/// fetch block from read-rpc
#[allow(unused_mut)]
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn block_call(
    data: Data<ServerContext>,
    Params(mut params): Params<near_jsonrpc_primitives::types::blocks::RpcBlockRequest>,
) -> Result<near_jsonrpc_primitives::types::blocks::RpcBlockResponse, RPCError> {
    tracing::debug!("`block` called with parameters: {:?}", params);
    crate::metrics::BLOCK_REQUESTS_TOTAL.inc();
    let result = fetch_block(&data, params.block_reference.clone()).await;

    #[cfg(feature = "shadow_data_consistency")]
    {
        if let Ok(res) = &result {
            if let near_primitives::types::BlockReference::Finality(_) = params.block_reference {
                params.block_reference = near_primitives::types::BlockReference::from(
                    near_primitives::types::BlockId::Height(res.block_view.header.height),
                )
            }
        };

        if let Some(err_code) = crate::utils::shadow_compare_results_handler(
            crate::metrics::BLOCK_REQUESTS_TOTAL.get(),
            data.shadow_data_consistency_rate,
            &result,
            data.near_rpc_client.clone(),
            params,
            "BLOCK",
        )
        .await
        {
            crate::utils::capture_shadow_consistency_error!(err_code, "BLOCK")
        };
    };

    Ok(result.map_err(near_jsonrpc_primitives::errors::RpcError::from)?)
}

/// fetch changes_in_block from read-rpc
#[allow(unused_mut)]
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn changes_in_block_call(
    data: Data<ServerContext>,
    Params(mut params): Params<
        near_jsonrpc_primitives::types::changes::RpcStateChangesInBlockRequest,
    >,
) -> Result<near_jsonrpc_primitives::types::changes::RpcStateChangesInBlockByTypeResponse, RPCError>
{
    crate::metrics::CHNGES_IN_BLOCK_REQUESTS_TOTAL.inc();
    let block = fetch_block_from_cache_or_get(&data, params.block_reference.clone())
        .await
        .map_err(near_jsonrpc_primitives::errors::RpcError::from)?;
    let result = fetch_changes_in_block(&data, block).await;
    #[cfg(feature = "shadow_data_consistency")]
    {
        if let near_primitives::types::BlockReference::Finality(_) = params.block_reference {
            params.block_reference = near_primitives::types::BlockReference::from(
                near_primitives::types::BlockId::Height(block.block_height),
            )
        }
        if let Some(err_code) = crate::utils::shadow_compare_results_handler(
            crate::metrics::CHNGES_IN_BLOCK_REQUESTS_TOTAL.get(),
            data.shadow_data_consistency_rate,
            &result,
            data.near_rpc_client.clone(),
            params,
            "CHANGES_IN_BLOCK",
        )
        .await
        {
            crate::utils::capture_shadow_consistency_error!(err_code, "CHANGES_IN_BLOCK")
        };
    }

    Ok(result.map_err(near_jsonrpc_primitives::errors::RpcError::from)?)
}

/// fetch changes_in_block_by_type from read-rpc
#[allow(unused_mut)]
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn changes_in_block_by_type_call(
    data: Data<ServerContext>,
    Params(mut params): Params<
        near_jsonrpc_primitives::types::changes::RpcStateChangesInBlockByTypeRequest,
    >,
) -> Result<near_jsonrpc_primitives::types::changes::RpcStateChangesInBlockResponse, RPCError> {
    crate::metrics::CHNGES_IN_BLOCK_BY_TYPE_REQUESTS_TOTAL.inc();
    let block = fetch_block_from_cache_or_get(&data, params.block_reference.clone())
        .await
        .map_err(near_jsonrpc_primitives::errors::RpcError::from)?;
    let result = fetch_changes_in_block_by_type(&data, block, &params.state_changes_request).await;

    #[cfg(feature = "shadow_data_consistency")]
    {
        if let near_primitives::types::BlockReference::Finality(_) = params.block_reference {
            params.block_reference = near_primitives::types::BlockReference::from(
                near_primitives::types::BlockId::Height(block.block_height),
            )
        }
        if let Some(err_code) = crate::utils::shadow_compare_results_handler(
            crate::metrics::CHNGES_IN_BLOCK_BY_TYPE_REQUESTS_TOTAL.get(),
            data.shadow_data_consistency_rate,
            &result,
            data.near_rpc_client.clone(),
            params,
            "CHANGES_IN_BLOCK_BY_TYPE",
        )
        .await
        {
            crate::utils::capture_shadow_consistency_error!(err_code, "CHANGES_IN_BLOCK_BY_TYPE")
        };
    }

    Ok(result.map_err(near_jsonrpc_primitives::errors::RpcError::from)?)
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
                match data.db_manager.get_block_by_hash(block_hash).await {
                    Ok(block_height) => Ok(block_height),
                    Err(err) => Err(
                        near_jsonrpc_primitives::types::blocks::RpcBlockError::UnknownBlock {
                            error_message: err.to_string(),
                        },
                    ),
                }
            }
        },
        near_primitives::types::BlockReference::Finality(finality) => match finality {
            near_primitives::types::Finality::Final => Ok(data
                .final_block_info
                .read()
                .await
                .final_block_cache
                .block_height),
            _ => Err(
                near_jsonrpc_primitives::types::blocks::RpcBlockError::InternalError {
                    error_message: "Finality other than final is not supported".to_string(),
                },
            ),
        },
        // for archive node both SyncCheckpoint(Genesis and EarliestAvailable)
        // are returning the genesis block height
        near_primitives::types::BlockReference::SyncCheckpoint(_) => {
            Ok(data.genesis_info.genesis_block_cache.block_height)
        }
    };
    let block_view = near_lake_framework::s3_fetchers::fetch_block(
        &data.s3_client,
        &data.s3_bucket_name,
        block_height?,
    )
    .await
    .map_err(
        |err| near_jsonrpc_primitives::types::blocks::RpcBlockError::UnknownBlock {
            error_message: err.to_string(),
        },
    )?;
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
            near_primitives::types::BlockId::Height(block_height) => data
                .db_manager
                .get_block_by_height_and_shard_id(block_height, shard_id)
                .await
                .map_err(|_err| {
                    near_jsonrpc_primitives::types::chunks::RpcChunkError::InvalidShardId {
                        shard_id,
                    }
                })
                .map(|block_height_shard_id| (block_height_shard_id.0, block_height_shard_id.1))?,
            near_primitives::types::BlockId::Hash(block_hash) => {
                let block_height = data
                    .db_manager
                    .get_block_by_hash(block_hash)
                    .await
                    .map_err(|err| {
                        near_jsonrpc_primitives::types::chunks::RpcChunkError::UnknownBlock {
                            error_message: err.to_string(),
                        }
                    })?;
                (block_height, shard_id)
            }
        },
        near_jsonrpc_primitives::types::chunks::ChunkReference::ChunkHash { chunk_id } => data
            .db_manager
            .get_block_by_chunk_hash(chunk_id)
            .await
            .map_err(
                |_err| near_jsonrpc_primitives::types::chunks::RpcChunkError::UnknownChunk {
                    chunk_hash: chunk_id.into(),
                },
            )
            .map(|block_height_shard_id| (block_height_shard_id.0, block_height_shard_id.1))?,
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
        near_jsonrpc_primitives::types::changes::RpcStateChangesError::UnknownBlock {
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
        near_jsonrpc_primitives::types::changes::RpcStateChangesError::UnknownBlock {
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
    let fetch_shards_futures = (0..block.chunks_included)
        .collect::<Vec<u64>>()
        .into_iter()
        .map(|shard_id| {
            near_lake_framework::s3_fetchers::fetch_shard(
                &data.s3_client,
                &data.s3_bucket_name,
                block.block_height,
                shard_id,
            )
        });
    futures::future::try_join_all(fetch_shards_futures)
        .await
        .map_err(|err| {
            anyhow::anyhow!(
                "Failed to fetch shards for block {} with error: {}",
                block.block_height,
                err
            )
        })
}
