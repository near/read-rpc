use near_primitives::views::{StateChangeValueView, StateChangesRequestView};

use crate::config::ServerContext;
use crate::modules::blocks::methods::fetch_block;

// Helper function to check if the requested block height is within the range of the available blocks
// If block height is lower than genesis block height, return an error block height is too low
// If block height is higher than optimistic block height, return an error block height is too high
// Otherwise return Ok(())
pub async fn check_block_height(
    data: &actix_web::web::Data<ServerContext>,
    block_height: near_primitives::types::BlockHeight,
) -> Result<(), near_jsonrpc::primitives::types::blocks::RpcBlockError> {
    let optimistic_block_height = data
        .blocks_info_by_finality
        .optimistic_block_view()
        .await
        .header
        .height;
    let genesis_block_height = data.genesis_info.genesis_block.header.height;
    if block_height < genesis_block_height {
        return Err(
            near_jsonrpc::primitives::types::blocks::RpcBlockError::UnknownBlock {
                error_message: format!(
                    "Requested block height {} is to low, genesis block height is {}",
                    block_height, genesis_block_height
                ),
            },
        );
    }
    if block_height > optimistic_block_height {
        return Err(
            near_jsonrpc::primitives::types::blocks::RpcBlockError::UnknownBlock {
                error_message: format!(
                    "Requested block height {} is to high, optimistic block height is {}",
                    block_height, optimistic_block_height
                ),
            },
        );
    }
    Ok(())
}

pub fn from_indexer_chunk_to_chunk_view(
    indexer_chunk: near_indexer_primitives::IndexerChunkView,
) -> near_primitives::views::ChunkView {
    // We collect a list of local receipt ids to filter out local receipts from the chunk
    let local_receipt_ids: Vec<near_indexer_primitives::CryptoHash> = indexer_chunk
        .transactions
        .iter()
        .filter(|indexer_tx| indexer_tx.transaction.signer_id == indexer_tx.transaction.receiver_id)
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
    near_primitives::views::ChunkView {
        author: indexer_chunk.author,
        header: indexer_chunk.header,
        transactions: indexer_chunk
            .transactions
            .into_iter()
            .map(|indexer_transaction| indexer_transaction.transaction)
            .collect(),
        receipts: indexer_chunk
            .receipts
            .into_iter()
            .filter(|receipt| !local_receipt_ids.contains(&receipt.receipt_id))
            .collect(),
    }
}

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(fastnear_client))
)]
pub async fn fetch_chunk_from_fastnear(
    fastnear_client: &near_lake_framework::FastNearClient,
    block_height: near_primitives::types::BlockHeight,
    shard_id: near_primitives::types::ShardId,
) -> Result<near_primitives::views::ChunkView, near_jsonrpc::primitives::types::chunks::RpcChunkError>
{
    tracing::debug!(
        "`fetch_chunk_from_fastnear` call: block_height {}, shard_id {}",
        block_height,
        shard_id
    );
    let indexer_chunk = near_lake_framework::fastnear::fetchers::fetch_chunk_or_retry(
        fastnear_client,
        block_height,
        shard_id.into(),
    )
    .await
    .map_err(
        |err| near_jsonrpc::primitives::types::chunks::RpcChunkError::InternalError {
            error_message: err.to_string(),
        },
    )?;
    Ok(from_indexer_chunk_to_chunk_view(indexer_chunk))
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn fetch_block_from_cache_or_get(
    data: &actix_web::web::Data<ServerContext>,
    block_reference: &near_primitives::types::BlockReference,
    method_name: &str,
) -> Result<near_primitives::views::BlockView, near_jsonrpc::primitives::types::blocks::RpcBlockError>
{
    let block = match block_reference {
        near_primitives::types::BlockReference::BlockId(block_id) => {
            let block_height = match block_id {
                near_primitives::types::BlockId::Height(block_height) => {
                    check_block_height(data, *block_height).await?;
                    *block_height
                }
                near_primitives::types::BlockId::Hash(hash) => data
                    .db_manager
                    .get_block_height_by_hash(*hash, method_name)
                    .await
                    .map_err(|err| {
                        near_jsonrpc::primitives::types::blocks::RpcBlockError::UnknownBlock {
                            error_message: err.to_string(),
                        }
                    })?,
            };
            data.blocks_cache.get(&block_height).await
        }
        near_primitives::types::BlockReference::Finality(finality) => {
            match finality {
                near_primitives::types::Finality::None => {
                    if crate::metrics::OPTIMISTIC_UPDATING.is_not_working() {
                        // Returns the final_block for None.
                        Some(data.blocks_info_by_finality.final_block_view().await)
                    } else {
                        // Returns the optimistic_block for None.
                        Some(data.blocks_info_by_finality.optimistic_block_view().await)
                    }
                }
                near_primitives::types::Finality::DoomSlug
                | near_primitives::types::Finality::Final => {
                    // Returns the final_block for DoomSlug and Final.
                    Some(data.blocks_info_by_finality.final_block_view().await)
                }
            }
        }
        near_primitives::types::BlockReference::SyncCheckpoint(_) => {
            // Return genesis_block_cache for all SyncCheckpoint
            // for archive node both Genesis and EarliestAvailable
            // are returning the genesis block
            Some(data.genesis_info.genesis_block.clone())
        }
    };
    let cache_block = match block {
        Some(block) => {
            crate::metrics::REQUESTS_BLOCKS_COUNTERS
                .with_label_values(&[method_name, "cache"])
                .inc();
            block
        }
        None => {
            let block_from_s3 = fetch_block(data, block_reference, method_name).await?;
            let block = block_from_s3.block_view;

            data.blocks_cache
                .put(block.header.height, block.clone())
                .await;
            block
        }
    };
    // increase block category metrics
    crate::metrics::increase_request_category_metrics(
        data,
        block_reference,
        method_name,
        Some(cache_block.header.height),
    )
    .await;
    Ok(cache_block)
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
