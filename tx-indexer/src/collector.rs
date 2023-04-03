use std::future::ready;
use futures::{
    future::{join_all, try_join_all},
    StreamExt,
};

use near_indexer_primitives::views::ExecutionStatusView;
use near_indexer_primitives::IndexerTransactionWithOutcome;

use crate::{config, storage};

// TODO: Handle known TX hash collision case for mainnet
// ref: https://github.com/near/near-indexer-for-explorer/issues/84
pub(crate) async fn index_transactions(
    streamer_message: &near_indexer_primitives::StreamerMessage,
    scylla_db_client: &std::sync::Arc<config::ScyllaDBManager>,
    hash_storage: &std::sync::Arc<futures_locks::RwLock<storage::HashStorage>>,
) -> anyhow::Result<()> {
    extract_transactions_to_collect(streamer_message, scylla_db_client, hash_storage).await?;
    collect_receipts_and_outcomes(streamer_message, scylla_db_client, hash_storage).await?;

    let finished_transaction_details = hash_storage.with_write(
        |mut hash_storage| {
            ready(hash_storage.transactions_to_save())
        }
    ).await?;

    if !finished_transaction_details.is_empty() {
        let scylla_db_client = scylla_db_client.clone();
        let block_height = streamer_message.block.header.height;
        tokio::spawn(async move {
            let send_finished_transaction_details_futures =
                finished_transaction_details.into_iter().map(|tx_details| {
                    save_transaction_details(&scylla_db_client, tx_details, block_height)
                });

            join_all(send_finished_transaction_details_futures).await;
        });
    }

    Ok(())
}

// Extracts all Transactions from the given `StreamerMessage` and pushes them to the memory storage
// by calling the function `new_transaction_details_to_collecting_pool`.
async fn extract_transactions_to_collect(
    streamer_message: &near_indexer_primitives::StreamerMessage,
    scylla_db_client: &std::sync::Arc<config::ScyllaDBManager>,
    hash_storage: &std::sync::Arc<futures_locks::RwLock<storage::HashStorage>>,
) -> anyhow::Result<()> {
    let block_height = streamer_message.block.header.height;

    let futures = streamer_message
        .shards
        .iter()
        .filter_map(|shard| shard.chunk.as_ref())
        .map(|chunk| (chunk.header.shard_id, chunk.transactions.iter()))
        .flat_map(|(shard_id, transactions)| {
            transactions.map(move |tx| {
                new_transaction_details_to_collecting_pool(
                    tx,
                    block_height,
                    shard_id,
                    scylla_db_client,
                    hash_storage,
                )
            })
        });
    try_join_all(futures).await.map(|_| ())
}

// Converts Transaction into CollectingTransactionDetails and puts it into memory storage.
// Also, adds the Receipt produced by ExecutionOutcome of the given Transaction to the watching list
// in memory storage
async fn new_transaction_details_to_collecting_pool(
    transaction: &IndexerTransactionWithOutcome,
    block_height: u64,
    shard_id: u64,
    scylla_db_client: &std::sync::Arc<config::ScyllaDBManager>,
    hash_storage: &std::sync::Arc<futures_locks::RwLock<storage::HashStorage>>,
) -> anyhow::Result<()> {
    let transaction_hash_string = transaction.transaction.hash.to_string();
    let converted_into_receipt_id = transaction
        .outcome
        .execution_outcome
        .outcome
        .receipt_ids
        .first()
        .expect("`receipt_ids` must contain one Receipt ID")
        .to_string();

    // Save the Receipt produced by the Transaction to the ScyllaDB Map
    save_receipt(
        scylla_db_client,
        &converted_into_receipt_id,
        &transaction_hash_string,
        block_height,
        shard_id,
    )
    .await?;

    let transaction_details =
        readnode_primitives::CollectingTransactionDetails::from_indexer_tx(transaction.clone());
    match hash_storage.with_write(
        |mut hash_storage| {
        ready(hash_storage.set_tx(transaction_details))
    }).await
    {
        Ok(_) => {
            hash_storage.with_write(|mut hash_storage| {
                ready(hash_storage.push_receipt_to_watching_list(converted_into_receipt_id, transaction_hash_string))
            }).await?
        }
        Err(e) => tracing::error!(
            target: crate::INDEXER,
            "Failed to add TransactionDetails to memory storage\n{:#?}",
            e
        ),
    }

    Ok(())
}

async fn collect_receipts_and_outcomes(
    streamer_message: &near_indexer_primitives::StreamerMessage,
    scylla_db_client: &std::sync::Arc<config::ScyllaDBManager>,
    hash_storage: &std::sync::Arc<futures_locks::RwLock<storage::HashStorage>>,
) -> anyhow::Result<()> {
    let block_height = streamer_message.block.header.height;

    let shard_futures = streamer_message
        .shards
        .iter()
        .map(|shard| process_shard(scylla_db_client, hash_storage, block_height, shard));

    futures::future::try_join_all(shard_futures).await?;

    Ok(())
}

async fn process_shard(
    scylla_db_client: &std::sync::Arc<config::ScyllaDBManager>,
    hash_storage: &std::sync::Arc<futures_locks::RwLock<storage::HashStorage>>,
    block_height: u64,
    shard: &near_indexer_primitives::IndexerShard,
) -> anyhow::Result<()> {
    let process_receipt_execution_outcome_futures =
        shard
            .receipt_execution_outcomes
            .iter()
            .map(|receipt_execution_outcome| {
                process_receipt_execution_outcome(
                    scylla_db_client,
                    hash_storage,
                    block_height,
                    shard.shard_id,
                    receipt_execution_outcome,
                )
            });

    futures::future::try_join_all(process_receipt_execution_outcome_futures).await?;

    Ok(())
}

async fn push_receipt_to_watching_list(
    hash_storage: &std::sync::Arc<futures_locks::RwLock<storage::HashStorage>>,
    receipt_id: String,
    transaction_hash: String
) -> anyhow::Result<()> {
    hash_storage.with_write(|mut hash_storage| {
        ready(hash_storage.push_receipt_to_watching_list(receipt_id, transaction_hash))
    }).await
}

async fn process_receipt_execution_outcome(
    scylla_db_client: &std::sync::Arc<config::ScyllaDBManager>,
    hash_storage: &std::sync::Arc<futures_locks::RwLock<storage::HashStorage>>,
    block_height: u64,
    shard_id: u64,
    receipt_execution_outcome: &near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
) -> anyhow::Result<()> {
    let receipt_id = receipt_execution_outcome.receipt.receipt_id.clone().to_string();
    if let Ok(Some(transaction_hash)) = hash_storage.with_write(
        move |mut hash_storage| {
            ready(hash_storage.remove_receipt_from_watching_list(&receipt_id))
        }
    ).await {
        tracing::debug!(
            target: crate::INDEXER,
            "-R {}",
            &receipt_execution_outcome.receipt.receipt_id.to_string(),
        );

        tracing::debug!(
            target: crate::INDEXER,
            "Saving receipt {} to the `receipts_map` in ScyllaDB",
            &receipt_execution_outcome.receipt.receipt_id.to_string(),
        );

        save_receipt(
            scylla_db_client,
            &receipt_execution_outcome.receipt.receipt_id.to_string(),
            &transaction_hash,
            block_height,
            shard_id,
        )
        .await?;

        let mut tasks = futures::stream::FuturesUnordered::new();

        // Add the newly produced receipt_ids to the watching list
        tasks.extend(
            receipt_execution_outcome
                .execution_outcome
                .outcome
                .receipt_ids
                .iter()
                .map(|receipt_id| {
                    tracing::debug!(target: crate::INDEXER, "+R {}", &receipt_id.to_string(),);
                    push_receipt_to_watching_list(hash_storage, receipt_id.to_string(), transaction_hash.clone())
                }),
        );

        // Add the success receipt to the watching list
        if let ExecutionStatusView::SuccessReceiptId(receipt_id) =
            receipt_execution_outcome.execution_outcome.outcome.status
        {
            tracing::debug!(target: crate::INDEXER, "+R {}", &receipt_id.to_string(),);
            tasks.push(
                push_receipt_to_watching_list(hash_storage, receipt_id.to_string(), transaction_hash.clone())
            );
        }

        while let Some(result) = tasks.next().await {
            let _ = result.map_err(|e| {
                tracing::debug!(
                    target: crate::INDEXER,
                    "Task encountered an error: {:#?}",
                    e
                )
            });
        }
        let receipt_outcome = receipt_execution_outcome.clone();
        let _ = hash_storage.with_write( move |mut hash_storage| {
            ready(hash_storage.push_outcome_and_receipt(
                &transaction_hash,
                receipt_outcome
                )
            )
        }).await.map_err(|e| {
                tracing::error!(
                    target: crate::INDEXER,
                    "Failed to push_outcome_and_receipt\n{:#?}",
                    e
                )
            });
    }
    Ok(())
}

// Save transaction detail into the scylla db
async fn save_transaction_details(
    scylla_db_client: &std::sync::Arc<config::ScyllaDBManager>,
    tx_details: readnode_primitives::CollectingTransactionDetails,
    block_height: u64,
) -> bool {
    let transaction_details = match tx_details.to_final_transaction_result() {
        Ok(details) => details,
        Err(err) => {
            tracing::error!(
                target: crate::INDEXER,
                "Failed to get final transaction \n{:#?}",
                err
            );
            return false;
        }
    };

    let result = scylla_db_client
        .add_transaction(transaction_details, block_height)
        .await
        .map_err(|err| {
            tracing::error!(
                target: crate::INDEXER,
                "Failed to save transaction \n{:#?}",
                err
            );
        });

    result.is_ok()
}

// Save receipt_id, parent_transaction_hash, block_height and shard_id to the ScyllaDb
async fn save_receipt(
    scylla_db_client: &std::sync::Arc<config::ScyllaDBManager>,
    receipt_id: &str,
    parent_tx_hash: &str,
    block_height: u64,
    shard_id: u64,
) -> anyhow::Result<()> {
    scylla_db_client
        .add_receipt(receipt_id, parent_tx_hash, block_height, shard_id)
        .await
        .map_err(|err| {
            tracing::error!(
                target: crate::INDEXER,
                "Failed to save receipt \n{:#?}",
                err
            );
            err
        })?;
    Ok(())
}
