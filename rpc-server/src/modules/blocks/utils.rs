use crate::config::ServerContext;
use crate::modules::blocks::methods::fetch_block;
use crate::modules::blocks::CacheBlock;
use near_primitives::views::{StateChangeValueView, StateChangesRequestView};

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(s3_client))
)]
pub async fn fetch_chunk_from_s3(
    s3_client: &near_lake_framework::s3_fetchers::LakeS3Client,
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
    match near_lake_framework::s3_fetchers::fetch_shard(
        s3_client,
        s3_bucket_name,
        block_height,
        shard_id,
    )
    .await
    {
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

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn fetch_block_from_cache_or_get(
    data: &jsonrpc_v2::Data<ServerContext>,
    block_reference: near_primitives::types::BlockReference,
) -> Result<CacheBlock, near_jsonrpc_primitives::types::blocks::RpcBlockError> {
    let block = match block_reference.clone() {
        near_primitives::types::BlockReference::BlockId(block_id) => {
            let block_height = match block_id {
                near_primitives::types::BlockId::Height(block_height) => block_height,
                near_primitives::types::BlockId::Hash(hash) => data
                    .db_manager
                    .get_block_by_hash(hash)
                    .await
                    .map_err(|err| {
                        near_jsonrpc_primitives::types::blocks::RpcBlockError::UnknownBlock {
                            error_message: err.to_string(),
                        }
                    })?,
            };
            data.blocks_cache.write().await.get(&block_height).cloned()
        }
        near_primitives::types::BlockReference::Finality(_) => {
            // Returns the final_block_height for all the finalities.
            Some(data.final_block_info.read().await.final_block_cache)
        }
        near_primitives::types::BlockReference::SyncCheckpoint(_) => {
            // Return genesis_block_cache for all SyncCheckpoint
            Some(data.genesis_info.genesis_block_cache)
        }
    };
    match block {
        Some(block) => Ok(block),
        None => {
            let block_from_s3 = fetch_block(data, block_reference).await?;
            let block: CacheBlock = block_from_s3.block_view.into();

            data.blocks_cache
                .write()
                .await
                .put(block.block_height, block);
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
