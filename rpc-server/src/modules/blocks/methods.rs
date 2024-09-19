use actix_web::web::Data;
use near_primitives::trie_key::TrieKey;
use near_primitives::views::StateChangeValueView;

use crate::config::ServerContext;
use crate::modules::blocks::utils::{
    fetch_block_from_cache_or_get, fetch_chunk_from_s3, is_matching_change,
};

/// `block` rpc method implementation
/// calls proxy_rpc_call to get `block` from near-rpc if request parameters not supported by read-rpc
/// as example: block_id by Finality::None is not supported by read-rpc
/// another way to get `block` from read-rpc using `block_call`
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn block(
    data: Data<ServerContext>,
    request_data: near_jsonrpc::primitives::types::blocks::RpcBlockRequest,
) -> Result<
    near_jsonrpc::primitives::types::blocks::RpcBlockResponse,
    near_jsonrpc::primitives::types::blocks::RpcBlockError,
> {
    if let near_primitives::types::BlockReference::Finality(
        near_primitives::types::Finality::None,
    ) = &request_data.block_reference
    {
        if crate::metrics::OPTIMISTIC_UPDATING.is_not_working() {
            // Proxy if the optimistic updating is not working
            let block_view = data
                .near_rpc_client
                .call(request_data, Some("block"))
                .await
                .map_err(|err| {
                    err.handler_error().cloned().unwrap_or(
                        near_jsonrpc::primitives::types::blocks::RpcBlockError::InternalError {
                            error_message: err.to_string(),
                        },
                    )
                })?;
            return Ok(near_jsonrpc::primitives::types::blocks::RpcBlockResponse { block_view });
        }
    };

    block_call(data, request_data).await
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn chunk(
    data: Data<ServerContext>,
    request_data: near_jsonrpc::primitives::types::chunks::RpcChunkRequest,
) -> Result<
    near_jsonrpc::primitives::types::chunks::RpcChunkResponse,
    near_jsonrpc::primitives::types::chunks::RpcChunkError,
> {
    tracing::debug!("`chunk` called with parameters: {:?}", request_data);
    let chunk_result = fetch_chunk(&data, request_data.chunk_reference.clone()).await;
    #[cfg(feature = "shadow-data-consistency")]
    {
        crate::utils::shadow_compare_results_handler(
            data.shadow_data_consistency_rate,
            &chunk_result,
            data.near_rpc_client.clone(),
            request_data,
            "chunk",
        )
        .await;
    }
    chunk_result
}

/// `EXPERIMENTAL_changes` rpc method implementation
/// calls proxy_rpc_call to get `EXPERIMENTAL_changes` from near-rpc if request parameters not supported by read-rpc
/// as example: BlockReference for Finality::None is not supported by read-rpc
/// another way to get `EXPERIMENTAL_changes` from read-rpc using `changes_in_block_by_type_call`
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn changes_in_block_by_type(
    data: Data<ServerContext>,
    request_data: near_jsonrpc::primitives::types::changes::RpcStateChangesInBlockByTypeRequest,
) -> Result<
    near_jsonrpc::primitives::types::changes::RpcStateChangesInBlockResponse,
    near_jsonrpc::primitives::types::changes::RpcStateChangesError,
> {
    if let near_primitives::types::BlockReference::Finality(
        near_primitives::types::Finality::None,
    ) = &request_data.block_reference
    {
        if crate::metrics::OPTIMISTIC_UPDATING.is_not_working() {
            // Proxy if the optimistic updating is not working
            return Ok(data
                .near_rpc_client
                .call(request_data, Some("EXPERIMENTAL_changes"))
                .await.map_err(|err| {
                    err.handler_error().cloned().unwrap_or(
                        near_jsonrpc::primitives::types::changes::RpcStateChangesError::InternalError {
                            error_message: err.to_string(),
                        },
                    )
                })?);
        }
    };

    changes_in_block_by_type_call(data, request_data).await
}

/// `EXPERIMENTAL_changes_in_block` rpc method implementation
/// calls proxy_rpc_call to get `EXPERIMENTAL_changes_in_block` from near-rpc if request parameters not supported by read-rpc
/// as example: BlockReference for Finality::None is not supported by read-rpc
/// another way to get `EXPERIMENTAL_changes_in_block` from read-rpc using `changes_in_block_call`
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn changes_in_block(
    data: Data<ServerContext>,
    request_data: near_jsonrpc::primitives::types::changes::RpcStateChangesInBlockRequest,
) -> Result<
    near_jsonrpc::primitives::types::changes::RpcStateChangesInBlockByTypeResponse,
    near_jsonrpc::primitives::types::changes::RpcStateChangesError,
> {
    if let near_primitives::types::BlockReference::Finality(
        near_primitives::types::Finality::None,
    ) = &request_data.block_reference
    {
        if crate::metrics::OPTIMISTIC_UPDATING.is_not_working() {
            // Proxy if the optimistic updating is not working
            return Ok(data
                .near_rpc_client
                .call(request_data, Some("EXPERIMENTAL_changes_in_block"))
                .await.map_err(|err| {
                    err.handler_error().cloned().unwrap_or(
                        near_jsonrpc::primitives::types::changes::RpcStateChangesError::InternalError {
                            error_message: err.to_string(),
                        },
                    )
                })?);
        }
    };

    changes_in_block_call(data, request_data).await
}

/// fetch block from read-rpc
#[allow(unused_mut)]
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn block_call(
    data: Data<ServerContext>,
    mut block_request: near_jsonrpc::primitives::types::blocks::RpcBlockRequest,
) -> Result<
    near_jsonrpc::primitives::types::blocks::RpcBlockResponse,
    near_jsonrpc::primitives::types::blocks::RpcBlockError,
> {
    tracing::debug!("`block` called with parameters: {:?}", block_request);
    let block_result = match fetch_block(&data, &block_request.block_reference, "block").await {
        Ok(block) => {
            // increase block category metrics
            crate::metrics::increase_request_category_metrics(
                &data,
                &block_request.block_reference,
                "block",
                Some(block.block_view.header.height),
            )
            .await;
            Ok(block)
        }
        Err(err) => Err(err),
    };

    #[cfg(feature = "shadow-data-consistency")]
    {
        if let Ok(res) = &block_result {
            if let near_primitives::types::BlockReference::Finality(_) =
                block_request.block_reference
            {
                block_request.block_reference = near_primitives::types::BlockReference::from(
                    near_primitives::types::BlockId::Height(res.block_view.header.height),
                )
            }
        };

        crate::utils::shadow_compare_results_handler(
            data.shadow_data_consistency_rate,
            &block_result,
            data.near_rpc_client.clone(),
            block_request,
            "block",
        )
        .await;
    };

    block_result
}

/// fetch changes_in_block from read-rpc
#[allow(unused_mut)]
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn changes_in_block_call(
    data: Data<ServerContext>,
    mut params: near_jsonrpc::primitives::types::changes::RpcStateChangesInBlockRequest,
) -> Result<
    near_jsonrpc::primitives::types::changes::RpcStateChangesInBlockByTypeResponse,
    near_jsonrpc::primitives::types::changes::RpcStateChangesError,
> {
    let cache_block = fetch_block_from_cache_or_get(
        &data,
        &params.block_reference,
        "EXPERIMENTAL_changes_in_block",
    )
    .await
    .map_err(|err| {
        near_jsonrpc::primitives::types::changes::RpcStateChangesError::UnknownBlock {
            error_message: err.to_string(),
        }
    })?;

    let result = fetch_changes_in_block(&data, cache_block, &params.block_reference).await;
    #[cfg(feature = "shadow-data-consistency")]
    {
        if let near_primitives::types::BlockReference::Finality(_) = params.block_reference {
            params.block_reference = near_primitives::types::BlockReference::from(
                near_primitives::types::BlockId::Height(cache_block.block_height),
            )
        }
        crate::utils::shadow_compare_results_handler(
            data.shadow_data_consistency_rate,
            &result,
            data.near_rpc_client.clone(),
            params,
            "EXPERIMENTAL_changes_in_block",
        )
        .await;
    }

    result
}

/// fetch changes_in_block_by_type from read-rpc
#[allow(unused_mut)]
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn changes_in_block_by_type_call(
    data: Data<ServerContext>,
    mut params: near_jsonrpc::primitives::types::changes::RpcStateChangesInBlockByTypeRequest,
) -> Result<
    near_jsonrpc::primitives::types::changes::RpcStateChangesInBlockResponse,
    near_jsonrpc::primitives::types::changes::RpcStateChangesError,
> {
    let cache_block =
        fetch_block_from_cache_or_get(&data, &params.block_reference, "EXPERIMENTAL_changes")
            .await
            .map_err(|err| {
                near_jsonrpc::primitives::types::changes::RpcStateChangesError::UnknownBlock {
                    error_message: err.to_string(),
                }
            })?;

    let result = fetch_changes_in_block_by_type(
        &data,
        cache_block,
        &params.state_changes_request,
        &params.block_reference,
    )
    .await;

    #[cfg(feature = "shadow-data-consistency")]
    {
        if let near_primitives::types::BlockReference::Finality(_) = params.block_reference {
            params.block_reference = near_primitives::types::BlockReference::from(
                near_primitives::types::BlockId::Height(cache_block.block_height),
            )
        }
        crate::utils::shadow_compare_results_handler(
            data.shadow_data_consistency_rate,
            &result,
            data.near_rpc_client.clone(),
            params,
            "EXPERIMENTAL_changes",
        )
        .await;
    }

    result
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn fetch_block(
    data: &Data<ServerContext>,
    block_reference: &near_primitives::types::BlockReference,
    method_name: &str,
) -> Result<
    near_jsonrpc::primitives::types::blocks::RpcBlockResponse,
    near_jsonrpc::primitives::types::blocks::RpcBlockError,
> {
    tracing::debug!("`fetch_block` call");
    let block_height = match block_reference {
        near_primitives::types::BlockReference::BlockId(block_id) => match block_id {
            near_primitives::types::BlockId::Height(block_height) => Ok(*block_height),
            near_primitives::types::BlockId::Hash(block_hash) => {
                match data
                    .db_manager
                    .get_block_height_by_hash(*block_hash, method_name)
                    .await
                {
                    Ok(block_height) => Ok(block_height),
                    Err(err) => {
                        tracing::error!("Failed to fetch block by hash: {}", err);
                        Err(
                            near_jsonrpc::primitives::types::blocks::RpcBlockError::UnknownBlock {
                                error_message: format!("BLOCK: {:?}", block_hash),
                            },
                        )
                    }
                }
            }
        },
        near_primitives::types::BlockReference::Finality(finality) => {
            return match finality {
                near_primitives::types::Finality::Final
                | near_primitives::types::Finality::DoomSlug => {
                    let block_view = data.blocks_info_by_finality.final_block_view().await;
                    Ok(near_jsonrpc::primitives::types::blocks::RpcBlockResponse { block_view })
                }
                near_primitives::types::Finality::None => {
                    if crate::metrics::OPTIMISTIC_UPDATING.is_not_working() {
                        Err(
                            near_jsonrpc::primitives::types::blocks::RpcBlockError::UnknownBlock {
                                error_message: "Finality::None is not supported by read-rpc"
                                    .to_string(),
                            },
                        )
                    } else {
                        let block_view = data.blocks_info_by_finality.optimistic_block_view().await;
                        Ok(
                            near_jsonrpc::primitives::types::blocks::RpcBlockResponse {
                                block_view,
                            },
                        )
                    }
                }
            }
        }
        // for archive node both SyncCheckpoint(Genesis and EarliestAvailable)
        // are returning the genesis block height
        near_primitives::types::BlockReference::SyncCheckpoint(_) => {
            Ok(data.genesis_info.genesis_block_cache.block_height)
        }
    }?;
    let block_view = near_lake_framework::s3_fetchers::fetch_block(
        &data.s3_client,
        &data.s3_bucket_name,
        block_height,
    )
    .await
    .map_err(|err| {
        tracing::error!("Failed to fetch block from S3: {}", err);
        near_jsonrpc::primitives::types::blocks::RpcBlockError::UnknownBlock {
            error_message: format!("BLOCK HEIGHT: {:?}", block_height),
        }
    })?;
    Ok(near_jsonrpc::primitives::types::blocks::RpcBlockResponse { block_view })
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn fetch_chunk(
    data: &Data<ServerContext>,
    chunk_reference: near_jsonrpc::primitives::types::chunks::ChunkReference,
) -> Result<
    near_jsonrpc::primitives::types::chunks::RpcChunkResponse,
    near_jsonrpc::primitives::types::chunks::RpcChunkError,
> {
    let (block_height, shard_id) = match chunk_reference {
        near_jsonrpc::primitives::types::chunks::ChunkReference::BlockShardId {
            block_id,
            shard_id,
        } => match block_id {
            near_primitives::types::BlockId::Height(block_height) => data
                .db_manager
                .get_block_by_height_and_shard_id(block_height, shard_id, "chunk")
                .await
                .map_err(|_err| {
                    near_jsonrpc::primitives::types::chunks::RpcChunkError::InvalidShardId {
                        shard_id,
                    }
                })
                .map(|block_height_shard_id| (block_height_shard_id.0, block_height_shard_id.1))?,
            near_primitives::types::BlockId::Hash(block_hash) => {
                let block_height = data
                    .db_manager
                    .get_block_height_by_hash(block_hash, "chunk")
                    .await
                    .map_err(|err| {
                        tracing::error!("Failed to fetch block by hash: {}", err);
                        near_jsonrpc::primitives::types::chunks::RpcChunkError::UnknownBlock {
                            error_message: format!("BLOCK: {:?}", block_hash),
                        }
                    })?;
                (block_height, shard_id)
            }
        },
        near_jsonrpc::primitives::types::chunks::ChunkReference::ChunkHash { chunk_id } => data
            .db_manager
            .get_block_by_chunk_hash(chunk_id, "chunk")
            .await
            .map_err(
                |_err| near_jsonrpc::primitives::types::chunks::RpcChunkError::UnknownChunk {
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
    // increase block category metrics
    crate::metrics::increase_request_category_metrics(
        data,
        &near_primitives::types::BlockReference::BlockId(near_primitives::types::BlockId::Height(
            block_height,
        )),
        "chunk",
        Some(block_height),
    )
    .await;

    Ok(near_jsonrpc::primitives::types::chunks::RpcChunkResponse { chunk_view })
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn fetch_changes_in_block(
    data: &Data<ServerContext>,
    cache_block: crate::modules::blocks::CacheBlock,
    block_reference: &near_primitives::types::BlockReference,
) -> Result<
    near_jsonrpc::primitives::types::changes::RpcStateChangesInBlockByTypeResponse,
    near_jsonrpc::primitives::types::changes::RpcStateChangesError,
> {
    let trie_keys = fetch_state_changes(data, cache_block, block_reference)
        .await
        .map_err(|err| {
            near_jsonrpc::primitives::types::changes::RpcStateChangesError::UnknownBlock {
                error_message: err.to_string(),
            }
        })?
        .into_iter()
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
        near_jsonrpc::primitives::types::changes::RpcStateChangesInBlockByTypeResponse {
            block_hash: cache_block.block_hash,
            changes,
        },
    )
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn fetch_changes_in_block_by_type(
    data: &Data<ServerContext>,
    cache_block: crate::modules::blocks::CacheBlock,
    state_changes_request: &near_primitives::views::StateChangesRequestView,
    block_reference: &near_primitives::types::BlockReference,
) -> Result<
    near_jsonrpc::primitives::types::changes::RpcStateChangesInBlockResponse,
    near_jsonrpc::primitives::types::changes::RpcStateChangesError,
> {
    let changes = fetch_state_changes(data, cache_block, block_reference)
        .await
        .map_err(|err| {
            near_jsonrpc::primitives::types::changes::RpcStateChangesError::UnknownBlock {
                error_message: err.to_string(),
            }
        })?
        .into_iter()
        .filter(|change| is_matching_change(change, state_changes_request))
        .collect();
    Ok(
        near_jsonrpc::primitives::types::changes::RpcStateChangesInBlockResponse {
            block_hash: cache_block.block_hash,
            changes,
        },
    )
}

// Helper method to fetch state changes from the cache or from the S3
// depending on the block reference
// If the block reference is Finality, then the state changes are returned from cached optimistic or final blocks
// Otherwise, the state changes are fetched from the S3
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn fetch_state_changes(
    data: &Data<ServerContext>,
    cache_block: crate::modules::blocks::CacheBlock,
    block_reference: &near_primitives::types::BlockReference,
) -> anyhow::Result<near_primitives::views::StateChangesView> {
    if let near_primitives::types::BlockReference::Finality(finality) = block_reference {
        match finality {
            near_primitives::types::Finality::None => {
                if crate::metrics::OPTIMISTIC_UPDATING.is_not_working() {
                    Err(anyhow::anyhow!(
                        "Failed to fetch shards! Finality::None is not supported by rpc_server",
                    ))
                } else {
                    Ok(data
                        .blocks_info_by_finality
                        .optimistic_block_changes()
                        .await)
                }
            }
            near_primitives::types::Finality::DoomSlug
            | near_primitives::types::Finality::Final => {
                Ok(data.blocks_info_by_finality.final_block_changes().await)
            }
        }
    } else {
        Ok(fetch_shards_by_cache_block(data, cache_block)
            .await?
            .into_iter()
            .flat_map(|shard| shard.state_changes)
            .collect())
    }
}

// Helper method to fetch block shards from the S3 by the cache block
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn fetch_shards_by_cache_block(
    data: &Data<ServerContext>,
    cache_block: crate::modules::blocks::CacheBlock,
) -> anyhow::Result<Vec<near_indexer_primitives::IndexerShard>> {
    let fetch_shards_futures = (0..cache_block.chunks_included)
        .collect::<Vec<u64>>()
        .into_iter()
        .map(|shard_id| {
            near_lake_framework::s3_fetchers::fetch_shard(
                &data.s3_client,
                &data.s3_bucket_name,
                cache_block.block_height,
                shard_id,
            )
        });

    futures::future::join_all(fetch_shards_futures)
        .await
        .into_iter()
        .collect::<Result<_, _>>()
        .map_err(|err| {
            anyhow::anyhow!(
                "Failed to fetch shards for block {} with error: {}",
                cache_block.block_height,
                err
            )
        })
}
