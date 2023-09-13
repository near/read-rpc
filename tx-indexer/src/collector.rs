use futures::{
    future::{join_all, try_join_all},
    FutureExt, StreamExt,
};
use near_indexer_primitives::IndexerTransactionWithOutcome;

use crate::base_storage::TxCollectingStorage;
use crate::config;

// TODO: Handle known TX hash collision case for mainnet
// ref: https://github.com/near/near-indexer-for-explorer/issues/84
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
pub(crate) async fn index_transactions(
    streamer_message: &near_indexer_primitives::StreamerMessage,
    scylla_db_client: &std::sync::Arc<config::ScyllaDBManager>,
    tx_collecting_storage: &std::sync::Arc<futures_locks::RwLock<impl TxCollectingStorage>>,
) -> anyhow::Result<()> {
    extract_transactions_to_collect(streamer_message, scylla_db_client, tx_collecting_storage)
        .await?;
    collect_receipts_and_outcomes(streamer_message, scylla_db_client, tx_collecting_storage)
        .await?;

    let finished_transaction_details = tx_collecting_storage
        .try_write()?
        .transactions_to_save()
        .await?;

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
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
async fn extract_transactions_to_collect(
    streamer_message: &near_indexer_primitives::StreamerMessage,
    scylla_db_client: &std::sync::Arc<config::ScyllaDBManager>,
    tx_collecting_storage: &std::sync::Arc<futures_locks::RwLock<impl TxCollectingStorage>>,
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
                    tx_collecting_storage,
                )
            })
        });
    try_join_all(futures).await.map(|_| ())
}

// Converts Transaction into CollectingTransactionDetails and puts it into memory storage.
// Also, adds the Receipt produced by ExecutionOutcome of the given Transaction to the watching list
// in memory storage
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
async fn new_transaction_details_to_collecting_pool(
    transaction: &IndexerTransactionWithOutcome,
    block_height: u64,
    shard_id: u64,
    scylla_db_client: &std::sync::Arc<config::ScyllaDBManager>,
    tx_collecting_storage: &std::sync::Arc<futures_locks::RwLock<impl TxCollectingStorage>>,
) -> anyhow::Result<()> {
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
        &transaction.transaction.hash.to_string(),
        block_height,
        shard_id,
    )
    .await?;

    let transaction_details =
        readnode_primitives::CollectingTransactionDetails::from_indexer_tx(transaction.clone());
    match tx_collecting_storage
        .try_write()?
        .set_tx(transaction_details)
        .await
    {
        Ok(_) => {
            tx_collecting_storage
                .try_write()?
                .push_receipt_to_watching_list(
                    converted_into_receipt_id,
                    transaction.transaction.hash.to_string(),
                )
                .await?
        }
        Err(e) => tracing::error!(
            target: crate::INDEXER,
            "Failed to add TransactionDetails to memory storage\n{:#?}",
            e
        ),
    }

    Ok(())
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
async fn collect_receipts_and_outcomes(
    streamer_message: &near_indexer_primitives::StreamerMessage,
    scylla_db_client: &std::sync::Arc<config::ScyllaDBManager>,
    tx_collecting_storage: &std::sync::Arc<futures_locks::RwLock<impl TxCollectingStorage>>,
) -> anyhow::Result<()> {
    let block_height = streamer_message.block.header.height;

    let shard_futures = streamer_message
        .shards
        .iter()
        .map(|shard| process_shard(scylla_db_client, tx_collecting_storage, block_height, shard));

    futures::future::try_join_all(shard_futures).await?;

    Ok(())
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
async fn process_shard(
    scylla_db_client: &std::sync::Arc<config::ScyllaDBManager>,
    tx_collecting_storage: &std::sync::Arc<futures_locks::RwLock<impl TxCollectingStorage>>,
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
                    tx_collecting_storage,
                    block_height,
                    shard.shard_id,
                    receipt_execution_outcome,
                )
            });

    futures::future::try_join_all(process_receipt_execution_outcome_futures).await?;

    Ok(())
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
async fn push_receipt_to_watching_list(
    tx_collecting_storage: &std::sync::Arc<futures_locks::RwLock<impl TxCollectingStorage>>,
    receipt_id: String,
    transaction_hash: String,
) -> anyhow::Result<()> {
    tx_collecting_storage
        .try_write()?
        .push_receipt_to_watching_list(receipt_id, transaction_hash)
        .await
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
async fn process_receipt_execution_outcome(
    scylla_db_client: &std::sync::Arc<config::ScyllaDBManager>,
    tx_collecting_storage: &std::sync::Arc<futures_locks::RwLock<impl TxCollectingStorage>>,
    block_height: u64,
    shard_id: u64,
    receipt_execution_outcome: &near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
) -> anyhow::Result<()> {
    if let Ok(Some(transaction_hash)) = tx_collecting_storage
        .try_write()?
        .get_transaction_hash_by_receipt_id(
            &receipt_execution_outcome.receipt.receipt_id.to_string(),
        )
        .await
    {
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
                    push_receipt_to_watching_list(
                        tx_collecting_storage,
                        receipt_id.to_string(),
                        transaction_hash.clone(),
                    )
                }),
        );
        while let Some(result) = tasks.next().await {
            let _ = result.map_err(|e| {
                tracing::debug!(
                    target: crate::INDEXER,
                    "Task encountered an error: {:#?}",
                    e
                )
            });
        }

        tx_collecting_storage
            .write()
            .map(move |mut tx_collecting_storage| {
                tx_collecting_storage
                    .push_outcome_and_receipt(&transaction_hash, receipt_execution_outcome.clone())
            })
            .await
            .map_err(|err| {
                tracing::error!(
                    target: crate::INDEXER,
                    "Failed to push_outcome_and_receipt\n{:#?}",
                    err
                );
                err
            })?;
    }
    Ok(())
}

// Save transaction detail into the scylla db
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
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
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
async fn save_receipt(
    scylla_db_client: &std::sync::Arc<config::ScyllaDBManager>,
    receipt_id: &str,
    parent_tx_hash: &str,
    block_height: u64,
    shard_id: u64,
) -> anyhow::Result<()> {
    tracing::debug!(
        target: crate::INDEXER,
        "Saving receipt_id: {} to `receipts_map` in ScyllaDB",
        receipt_id,
    );
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
