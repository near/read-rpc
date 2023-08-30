use crate::config::ServerContext;
use crate::modules::blocks::methods::fetch_block;
use crate::modules::blocks::CacheBlock;
use crate::storage::ScyllaDBManager;
use near_primitives::views::{StateChangeValueView, StateChangesRequestView};

/// Fetch object from s3 with retry
/// Try to get 3 times if we have some problems with network
async fn fetch_object_from_s3_with_retry<T>(
    s3_client: &aws_sdk_s3::Client,
    s3_bucket_name: &str,
    key: String,
) -> anyhow::Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let mut retry_count: u8 = 0;
    let s3_response = loop {
        match s3_client
            .get_object()
            .bucket(s3_bucket_name)
            .key(&key)
            .send()
            .await
        {
            Ok(response) => break Ok(response),
            Err(err) => match err {
                // Do retry on network errors
                aws_sdk_s3::types::SdkError::ConstructionFailure(_)
                | aws_sdk_s3::types::SdkError::TimeoutError(_)
                | aws_sdk_s3::types::SdkError::DispatchFailure(_)
                | aws_sdk_s3::types::SdkError::ResponseError(_) => {
                    tracing::debug!("Error to get object from s3, Retry");
                    if retry_count < crate::config::DEFAULT_RETRY_COUNT {
                        retry_count += 1;
                        continue;
                    } else {
                        // Stop retrying after 3 times
                        break Err(err);
                    }
                }
                // Stop retrying if we get response from s3 with error related to object
                _ => break Err(err),
            },
        }
    }?;
    let body_bytes = s3_response.body.collect().await?.into_bytes();

    Ok(serde_json::from_slice::<T>(body_bytes.as_ref())?)
}

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(s3_client))
)]
pub async fn fetch_block_from_s3(
    s3_client: &aws_sdk_s3::Client,
    s3_bucket_name: &str,
    block_height: near_primitives::types::BlockHeight,
) -> Result<near_primitives::views::BlockView, near_jsonrpc_primitives::types::blocks::RpcBlockError>
{
    tracing::debug!("`fetch_block_from_s3` call");

    match fetch_object_from_s3_with_retry::<near_primitives::views::BlockView>(
        s3_client,
        s3_bucket_name,
        format!("{:0>12}/block.json", block_height), // path to the block in the bucket
    )
    .await
    {
        Ok(block) => Ok(block),
        Err(err) => Err(
            near_jsonrpc_primitives::types::blocks::RpcBlockError::UnknownBlock {
                error_message: err.to_string(),
            },
        ),
    }
}

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(s3_client))
)]
pub async fn fetch_shard_from_s3(
    s3_client: &aws_sdk_s3::Client,
    s3_bucket_name: &str,
    block_height: near_primitives::types::BlockHeight,
    shard_id: near_primitives::types::ShardId,
) -> anyhow::Result<near_indexer_primitives::IndexerShard> {
    tracing::debug!("`fetch_shard_from_s3` call");

    fetch_object_from_s3_with_retry::<near_indexer_primitives::IndexerShard>(
        s3_client,
        s3_bucket_name,
        format!("{:0>12}/shard_{shard_id}.json", block_height), // path to the shard in the bucket
    )
    .await
}

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(s3_client))
)]
pub async fn fetch_chunk_from_s3(
    s3_client: &aws_sdk_s3::Client,
    s3_bucket_name: &str,
    block_height: near_primitives::types::BlockHeight,
    shard_id: near_primitives::types::ShardId,
) -> Result<near_primitives::views::ChunkView, near_jsonrpc_primitives::types::chunks::RpcChunkError>
{
    tracing::debug!(
        "`fetch_chunk_from_s3` call: block_height {}, shard_id {}",
        block_height,
        shard_id
    );
    match fetch_shard_from_s3(s3_client, s3_bucket_name, block_height, shard_id).await {
        Ok(shard) => match shard.chunk {
            Some(chunk) => {
                // We collect a list of local receipt ids to filter out local receipts from the chunk
                let local_receipt_ids: Vec<near_indexer_primitives::CryptoHash> = chunk
                    .transactions
                    .iter()
                    .filter(|indexer_tx| {
                        indexer_tx.transaction.signer_id == indexer_tx.transaction.receiver_id
                    })
                    .map(|indexer_tx| {
                        *indexer_tx
                            .outcome
                            .execution_outcome
                            .outcome
                            .receipt_ids
                            .first()
                            .expect("Conversion receipt_id must be present in transaction outcome")
                    })
                    .collect();
                Ok(near_primitives::views::ChunkView {
                    author: chunk.author,
                    header: chunk.header,
                    transactions: chunk
                        .transactions
                        .into_iter()
                        .map(|indexer_transaction| indexer_transaction.transaction)
                        .collect(),
                    receipts: chunk
                        .receipts
                        .into_iter()
                        .filter(|receipt| !local_receipt_ids.contains(&receipt.receipt_id))
                        .collect(),
                })
            }
            None => Err(
                near_jsonrpc_primitives::types::chunks::RpcChunkError::InternalError {
                    error_message: "Unavailable chunk".to_string(),
                },
            ),
        },
        Err(err) => Err(
            near_jsonrpc_primitives::types::chunks::RpcChunkError::InternalError {
                error_message: err.to_string(),
            },
        ),
    }
}

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(scylla_db_manager))
)]
pub async fn scylla_db_convert_block_hash_to_block_height(
    scylla_db_manager: &std::sync::Arc<ScyllaDBManager>,
    block_hash: near_primitives::hash::CryptoHash,
) -> Result<u64, near_jsonrpc_primitives::types::blocks::RpcBlockError> {
    tracing::debug!("`scylla_db_convert_block_hash_to_block_height` call");
    match scylla_db_manager.get_block_by_hash(block_hash).await {
        Ok(block_height) => Ok(block_height),
        Err(err) => Err(
            near_jsonrpc_primitives::types::blocks::RpcBlockError::UnknownBlock {
                error_message: err.to_string(),
            },
        ),
    }
}

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(scylla_db_manager))
)]
pub async fn scylla_db_convert_chunk_hash_to_block_height_and_shard_id(
    scylla_db_manager: &std::sync::Arc<ScyllaDBManager>,
    chunk_hash: near_primitives::hash::CryptoHash,
) -> Result<
    (
        near_primitives::types::BlockHeight,
        near_primitives::types::ShardId,
    ),
    near_jsonrpc_primitives::types::chunks::RpcChunkError,
> {
    tracing::debug!("`scylla_db_convert_chunk_hash_to_block_height_and_shard_id` call");
    match scylla_db_manager.get_block_by_chunk_hash(chunk_hash).await {
        Ok(block_id_shard_id) => Ok((block_id_shard_id.0, block_id_shard_id.1)),
        Err(_err) => Err(
            near_jsonrpc_primitives::types::chunks::RpcChunkError::InternalError {
                error_message: format!("Unknown chunk hash: {chunk_hash}"),
            },
        ),
    }
}

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(scylla_db_manager))
)]
pub async fn scylla_db_convert_block_height_and_shard_id_to_height_included_and_shard_id(
    scylla_db_manager: &std::sync::Arc<ScyllaDBManager>,
    block_height: near_primitives::types::BlockHeight,
    shard_id: near_primitives::types::ShardId,
) -> Result<
    (
        near_primitives::types::BlockHeight,
        near_primitives::types::ShardId,
    ),
    near_jsonrpc_primitives::types::chunks::RpcChunkError,
> {
    tracing::debug!(
        "`scylla_db_convert_block_height_and_shard_id_to_height_included_and_shard_id` call"
    );
    Ok(scylla_db_manager
        .get_block_by_height_and_shard_id(block_height, shard_id)
        .await
        .map_err(
            |_err| near_jsonrpc_primitives::types::chunks::RpcChunkError::InternalError {
                error_message: format!(
                    "Unknown block height: {} and shard_id: {}",
                    block_height, shard_id
                ),
            },
        )
        .map(|block_height_shard_id| (block_height_shard_id.0, block_height_shard_id.1)))?
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn fetch_block_from_cache_or_get(
    data: &jsonrpc_v2::Data<ServerContext>,
    block_reference: near_primitives::types::BlockReference,
) -> Result<CacheBlock, near_jsonrpc_primitives::types::blocks::RpcBlockError> {
    let block_height = match block_reference.clone() {
        near_primitives::types::BlockReference::BlockId(block_id) => match block_id {
            near_primitives::types::BlockId::Height(block_height) => Ok(block_height),
            near_primitives::types::BlockId::Hash(hash) => {
                scylla_db_convert_block_hash_to_block_height(&data.scylla_db_manager, hash).await
            }
        },
        near_primitives::types::BlockReference::Finality(_) => {
            // Returns the final_block_height for all the finalities.
            Ok(data
                .final_block_height
                .load(std::sync::atomic::Ordering::SeqCst))
        }
        // TODO: return the height of the first block height from S3 (cache it once on the start)
        near_primitives::types::BlockReference::SyncCheckpoint(_) => Err(
            near_jsonrpc_primitives::types::blocks::RpcBlockError::InternalError {
                error_message: "Finality other than final is not supported".to_string(),
            },
        ),
    };
    let block = data
        .blocks_cache
        .write()
        .unwrap()
        .get(&block_height?)
        .cloned();
    match block {
        Some(block) => Ok(block),
        None => {
            let block_from_s3 = fetch_block(data, block_reference).await?;
            let block = CacheBlock {
                block_hash: block_from_s3.block_view.header.hash,
                block_height: block_from_s3.block_view.header.height,
                block_timestamp: block_from_s3.block_view.header.timestamp,
                latest_protocol_version: block_from_s3.block_view.header.latest_protocol_version,
                chunks_included: block_from_s3.block_view.header.chunks_included,
            };

            data.blocks_cache
                .write()
                .unwrap()
                .put(block_from_s3.block_view.header.height, block);
            Ok(block)
        }
    }
}

/// Determines whether a given `StateChangeWithCauseView` object matches a set of criteria
/// specified by the `state_changes_request` parameter.
/// Using for filtering state changes.
pub(crate) fn is_matching_change(
    change: &near_primitives::views::StateChangeWithCauseView,
    state_changes_request: &StateChangesRequestView,
) -> bool {
    match state_changes_request {
        StateChangesRequestView::AccountChanges { account_ids }
        | StateChangesRequestView::AllAccessKeyChanges { account_ids }
        | StateChangesRequestView::ContractCodeChanges { account_ids } => {
            if let StateChangeValueView::AccountUpdate { account_id, .. }
            | StateChangeValueView::AccountDeletion { account_id }
            | StateChangeValueView::AccessKeyUpdate { account_id, .. }
            | StateChangeValueView::AccessKeyDeletion { account_id, .. }
            | StateChangeValueView::ContractCodeUpdate { account_id, .. }
            | StateChangeValueView::ContractCodeDeletion { account_id } = &change.value
            {
                account_ids.contains(account_id)
            } else {
                false
            }
        }

        StateChangesRequestView::SingleAccessKeyChanges { keys } => {
            if let StateChangeValueView::AccessKeyUpdate {
                account_id,
                public_key,
                ..
            }
            | StateChangeValueView::AccessKeyDeletion {
                account_id,
                public_key,
            } = &change.value
            {
                let account_id_match = keys
                    .iter()
                    .any(|account_public_key| account_public_key.account_id == *account_id);

                let public_key_match = keys
                    .iter()
                    .any(|account_public_key| account_public_key.public_key == *public_key);

                account_id_match && public_key_match
            } else {
                false
            }
        }

        StateChangesRequestView::DataChanges {
            account_ids,
            key_prefix,
        } => {
            if let StateChangeValueView::DataUpdate {
                account_id, key, ..
            }
            | StateChangeValueView::DataDeletion { account_id, key } = &change.value
            {
                let key: Vec<u8> = key.clone().into();
                let key_prefix: Vec<u8> = key_prefix.clone().into();
                account_ids.contains(account_id)
                    && hex::encode(key).starts_with(&hex::encode(key_prefix))
            } else {
                false
            }
        }
    }
}
