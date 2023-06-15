use crate::config::ServerContext;
use crate::modules::blocks::methods::fetch_block;
use crate::modules::blocks::CacheBlock;
use crate::storage::ScyllaDBManager;
use anyhow::Context;
use near_primitives::views::{StateChangeValueView, StateChangesRequestView};
use num_traits::ToPrimitive;

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
    match s3_client
        .get_object()
        .bucket(s3_bucket_name)
        .key(format!("{:0>12}/block.json", block_height))
        .send()
        .await
    {
        Ok(response) => {
            let body_bytes = response.body.collect().await.unwrap().into_bytes();
            match serde_json::from_slice::<near_primitives::views::BlockView>(body_bytes.as_ref()) {
                Ok(block) => Ok(block),
                Err(err) => Err(
                    near_jsonrpc_primitives::types::blocks::RpcBlockError::UnknownBlock {
                        error_message: err.to_string(),
                    },
                ),
            }
        }
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
    let response = s3_client
        .get_object()
        .bucket(s3_bucket_name)
        .key(format!("{:0>12}/shard_{shard_id}.json", block_height))
        .send()
        .await
        .with_context(|| "Error to get object from s3")?;
    let body_bytes = response
        .body
        .collect()
        .await
        .with_context(|| "Invalid data from s3")?
        .into_bytes();

    serde_json::from_slice::<near_indexer_primitives::IndexerShard>(body_bytes.as_ref())
        .with_context(|| "Invalid serialised data")
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
    tracing::debug!("`fetch_chunk_from_s3` call");
    match fetch_shard_from_s3(s3_client, s3_bucket_name, block_height, shard_id).await {
        Ok(shard) => match shard.chunk {
            Some(chunk) => Ok(near_primitives::views::ChunkView {
                author: chunk.author,
                header: chunk.header,
                transactions: chunk
                    .transactions
                    .into_iter()
                    .map(|indexer_transaction| indexer_transaction.transaction)
                    .collect(),
                receipts: chunk.receipts,
            }),
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
    let result = scylla_db_manager.get_block_by_hash(block_hash).await;
    if let Ok(row) = result {
        let (block_height,): (num_bigint::BigInt,) =
            row.into_typed::<(num_bigint::BigInt,)>().map_err(|err| {
                near_jsonrpc_primitives::types::blocks::RpcBlockError::InternalError {
                    error_message: err.to_string(),
                }
            })?;
        match block_height.to_u64() {
            Some(block_height) => Ok(block_height),
            None => Err(
                near_jsonrpc_primitives::types::blocks::RpcBlockError::InternalError {
                    error_message: "Invalid block height".to_string(),
                },
            ),
        }
    } else {
        Err(
            near_jsonrpc_primitives::types::blocks::RpcBlockError::UnknownBlock {
                error_message: "Unknown block hash".to_string(),
            },
        )
    }
}

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(scylla_db_manager))
)]
pub async fn scylla_db_convert_chunk_hash_to_block_height_and_shard_id(
    scylla_db_manager: &std::sync::Arc<ScyllaDBManager>,
    chunk_id: near_primitives::hash::CryptoHash,
) -> Result<
    (
        near_primitives::types::BlockHeight,
        near_primitives::types::ShardId,
    ),
    near_jsonrpc_primitives::types::chunks::RpcChunkError,
> {
    tracing::debug!("`scylla_db_convert_chunk_hash_to_block_height_and_shard_id` call");
    let result = scylla_db_manager.get_block_by_chunk_id(chunk_id).await;
    if let Ok(row) = result {
        let (block_height, shard_id): (num_bigint::BigInt, num_bigint::BigInt) = row
            .into_typed::<(num_bigint::BigInt, num_bigint::BigInt)>()
            .expect("Invalid value from db");
        let block_height = block_height
            .to_u64()
            .expect("Error to convert BigInt into u64");
        let shard_id = shard_id.to_u64().expect("Error to convert BigInt into u64");
        Ok((block_height, shard_id))
    } else {
        Err(
            near_jsonrpc_primitives::types::chunks::RpcChunkError::InternalError {
                error_message: format!("Unknown chunk hash: {chunk_id}"),
            },
        )
    }
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
