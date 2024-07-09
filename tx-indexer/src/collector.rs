#[cfg(feature = "validator")]
use borsh::BorshDeserialize;

use futures::{
    future::{join_all, try_join_all},
    StreamExt,
};
use near_indexer_primitives::IndexerTransactionWithOutcome;

use crate::metrics;
use crate::storage;

const TRANSACTION_SAVE_ATTEMPTS: usize = 20;

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
pub(crate) async fn index_transactions(
    streamer_message: &near_indexer_primitives::StreamerMessage,
    db_manager: &std::sync::Arc<Box<dyn database::TxIndexerDbManager + Sync + Send + 'static>>,
    tx_collecting_storage: &std::sync::Arc<crate::storage::CacheStorage>,
    tx_details_storage: &std::sync::Arc<crate::TxDetailsStorage>,
    indexer_config: &configuration::TxIndexerConfig,
) -> anyhow::Result<()> {
    extract_transactions_to_collect(
        streamer_message,
        db_manager,
        tx_collecting_storage,
        indexer_config,
    )
    .await?;
    collect_receipts_and_outcomes(streamer_message, db_manager, tx_collecting_storage).await?;

    let finished_transaction_details =
        tx_collecting_storage
            .transactions_to_save()
            .await
            .map_err(|err| {
                tracing::error!(
                    target: crate::INDEXER,
                    "Failed to get transactions to save\n{:#?}",
                    err
                );
                err
            })?;

    if !finished_transaction_details.is_empty() {
        let tx_collecting_storage = tx_collecting_storage.clone();
        let tx_details_storage = tx_details_storage.clone();
        tokio::spawn(async move {
            let send_finished_transaction_details_futures =
                finished_transaction_details.into_iter().map(|tx_details| {
                    save_transaction_details(
                        &tx_collecting_storage,
                        &tx_details_storage,
                        tx_details,
                    )
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
    db_manager: &std::sync::Arc<Box<dyn database::TxIndexerDbManager + Sync + Send + 'static>>,
    tx_collecting_storage: &std::sync::Arc<crate::storage::CacheStorage>,
    indexer_config: &configuration::TxIndexerConfig,
) -> anyhow::Result<()> {
    let block_height = streamer_message.block.header.height;
    let block_hash = streamer_message.block.header.hash;

    let txs_in_block = streamer_message
        .shards
        .iter()
        .map(|shard| {
            shard
                .chunk
                .as_ref()
                .map_or(0, |chunk| chunk.transactions.len())
        })
        .sum::<usize>();
    crate::metrics::TX_IN_BLOCK_TOTAL.set(txs_in_block as i64);

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
                    block_hash,
                    shard_id,
                    db_manager,
                    tx_collecting_storage,
                    indexer_config,
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
    block_hash: near_indexer_primitives::CryptoHash,
    shard_id: u64,
    db_manager: &std::sync::Arc<Box<dyn database::TxIndexerDbManager + Sync + Send + 'static>>,
    tx_collecting_storage: &std::sync::Arc<storage::CacheStorage>,
    indexer_config: &configuration::TxIndexerConfig,
) -> anyhow::Result<()> {
    if !indexer_config.tx_should_be_indexed(transaction) {
        return Ok(());
    };
    crate::metrics::TX_IN_MEMORY_CACHE.inc();
    let converted_into_receipt_id = transaction
        .outcome
        .execution_outcome
        .outcome
        .receipt_ids
        .first()
        .expect("`receipt_ids` must contain one Receipt ID");

    // Save the Receipt produced by the Transaction to the DB Map
    save_receipt(
        db_manager,
        converted_into_receipt_id,
        &transaction.transaction.hash,
        &transaction.transaction.receiver_id,
        block_height,
        block_hash,
        shard_id,
    )
    .await?;

    let transaction_details = readnode_primitives::CollectingTransactionDetails::from_indexer_tx(
        transaction.clone(),
        block_height,
    );
    let transaction_key = transaction_details.transaction_key();
    match tx_collecting_storage.set_tx(transaction_details).await {
        Ok(_) => {
            tx_collecting_storage
                .push_receipt_to_watching_list(
                    converted_into_receipt_id.to_string(),
                    transaction_key,
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
    db_manager: &std::sync::Arc<Box<dyn database::TxIndexerDbManager + Sync + Send + 'static>>,
    tx_collecting_storage: &std::sync::Arc<crate::storage::CacheStorage>,
) -> anyhow::Result<()> {
    let block_height = streamer_message.block.header.height;
    let block_hash = streamer_message.block.header.hash;

    let shard_futures = streamer_message.shards.iter().map(|shard| {
        process_shard(
            db_manager,
            tx_collecting_storage,
            block_height,
            block_hash,
            shard,
        )
    });

    futures::future::try_join_all(shard_futures).await?;

    Ok(())
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
async fn process_shard(
    db_manager: &std::sync::Arc<Box<dyn database::TxIndexerDbManager + Sync + Send + 'static>>,
    tx_collecting_storage: &std::sync::Arc<crate::storage::CacheStorage>,
    block_height: u64,
    block_hash: near_indexer_primitives::CryptoHash,
    shard: &near_indexer_primitives::IndexerShard,
) -> anyhow::Result<()> {
    let process_receipt_execution_outcome_futures =
        shard
            .receipt_execution_outcomes
            .iter()
            .map(|receipt_execution_outcome| {
                process_receipt_execution_outcome(
                    db_manager,
                    tx_collecting_storage,
                    block_height,
                    block_hash,
                    shard.shard_id,
                    receipt_execution_outcome,
                )
            });

    futures::future::try_join_all(process_receipt_execution_outcome_futures).await?;

    Ok(())
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
async fn process_receipt_execution_outcome(
    db_manager: &std::sync::Arc<Box<dyn database::TxIndexerDbManager + Sync + Send + 'static>>,
    tx_collecting_storage: &std::sync::Arc<storage::CacheStorage>,
    block_height: u64,
    block_hash: near_indexer_primitives::CryptoHash,
    shard_id: u64,
    receipt_execution_outcome: &near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
) -> anyhow::Result<()> {
    if let Ok(transaction_key) = tx_collecting_storage
        .get_transaction_hash_by_receipt_id(
            &receipt_execution_outcome.receipt.receipt_id.to_string(),
        )
        .await
    {
        save_receipt(
            db_manager,
            &receipt_execution_outcome.receipt.receipt_id,
            &transaction_key.transaction_hash,
            &receipt_execution_outcome.receipt.receiver_id,
            block_height,
            block_hash,
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
                    tx_collecting_storage.push_receipt_to_watching_list(
                        receipt_id.to_string(),
                        transaction_key.clone(),
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
            .push_outcome_and_receipt(&transaction_key, receipt_execution_outcome.clone())
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

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
async fn save_transaction_details(
    tx_collecting_storage: &std::sync::Arc<storage::CacheStorage>,
    tx_details_storage: &std::sync::Arc<crate::TxDetailsStorage>,
    tx_details: readnode_primitives::CollectingTransactionDetails,
) {
    let tx_key = tx_details.transaction_key();
    match save_transaction_details_to_storage(tx_details_storage, tx_details.clone()).await {
        Ok(_) => {
            // We assume that the transaction is saved correctly
            // We can remove the transaction from the cache storage
            if let Err(err) = tx_collecting_storage
                .remove_transaction_from_cache(tx_key.clone())
                .await
            {
                tracing::error!(
                    target: crate::INDEXER,
                    "Failed to remove transaction from cache {}: Error {}",
                    tx_key.transaction_hash,
                    err
                );
            }
        }
        Err(err) => {
            tracing::error!(
                target: crate::INDEXER,
                "Failed to save transaction {}: Back transaction to save. Error {}",
                tx_key.transaction_hash,
                err
            );
            // If the transaction wasn't saved correctly, we will move it back to the save queue
            if let Err(err) = tx_collecting_storage.move_tx_to_save(tx_details).await {
                tracing::error!(
                    target: crate::INDEXER,
                    "Failed to move transaction to save {}: Error {}",
                    tx_key.transaction_hash,
                    err
                );
            };
        }
    }
}

// Save transaction detail into the storage
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
async fn save_transaction_details_to_storage(
    tx_details_storage: &std::sync::Arc<crate::TxDetailsStorage>,
    tx_details: readnode_primitives::CollectingTransactionDetails,
) -> anyhow::Result<()> {
    let transaction_details = tx_details.to_final_transaction_result()?;
    let transaction_hash = transaction_details.transaction.hash.to_string();
    let tx_bytes = borsh::to_vec(&transaction_details)?;

    // We faced the issue when the transaction was saved to the storage but later
    // was failing to deserialize. To avoid this issue and monitor the situation
    // without runing the user experience, we will try to save the transaction
    // to the storage and validate that it is saved correctly for 3 attempts
    // before we throw an error.
    let mut save_attempts = 0;
    'retry: loop {
        save_attempts += 1;
        if save_attempts >= TRANSACTION_SAVE_ATTEMPTS {
            anyhow::bail!(
                "Failed to save transaction {} after {} attempts",
                transaction_hash,
                save_attempts,
            );
        }
        match tx_details_storage
            .store(&transaction_hash, tx_bytes.clone())
            .await
        {
            Ok(_) => {
                if save_attempts > 1 {
                    // If the transaction wasn't saved after first attempt we want to inform
                    // a log reader about it
                    tracing::info!(
                        target: crate::INDEXER,
                        "Transaction {} was saved after {} attempts",
                        transaction_hash,
                        save_attempts,
                    );
                }
                #[cfg(feature = "validator")]
                {
                    // At this moment transaction seems to be stored, and we want to validate the correctness of the stored data
                    // To validate we will try to retrieve the transaction from the storage and validate that it is deserializable
                    // If the transaction is not deserializable, we will try to save it again
                    let mut retrieve_attempts = 0;
                    'validator: loop {
                        retrieve_attempts += 1;
                        if retrieve_attempts >= TRANSACTION_SAVE_ATTEMPTS {
                            tracing::error!(
                                target: crate::INDEXER,
                                "Failed to retrieve transaction {} for validation after {} attempts",
                                transaction_hash,
                                retrieve_attempts,
                            );
                            break 'validator;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
                        let Ok(tx_details_bytes_from_storage) =
                            tx_details_storage.retrieve(&transaction_hash).await
                        else {
                            tracing::error!(
                                target: crate::INDEXER,
                                "Failed to retrieve transaction {} from storage",
                                transaction_hash,
                            );
                            continue 'validator;
                        };

                        match readnode_primitives::TransactionDetails::try_from_slice(
                            &tx_details_bytes_from_storage,
                        ) {
                            Ok(_) => {
                                // We assume that the transaction is saved correctly
                                // We can remove the transaction from the cache storage
                                metrics::TX_IN_MEMORY_CACHE.dec();
                                break 'retry Ok(());
                            }
                            Err(err) => {
                                tracing::warn!(
                                    target: crate::INDEXER,
                                    "Failed to validate transaction {} \n{:#?}",
                                    transaction_hash,
                                    err
                                );
                                // If the transaction is not deserializable, we will try to save it again
                                continue 'retry;
                            }
                        }
                    }
                }
                #[cfg(not(feature = "validator"))]
                {
                    metrics::TX_IN_MEMORY_CACHE.dec();
                    break 'retry Ok(());
                }
            }
            Err(err) => {
                crate::metrics::TX_STORE_ERRORS_TOTAL.inc();
                tracing::debug!(
                    target: crate::INDEXER,
                    "[{}] Failed to save transaction {} \n{:#?}",
                    save_attempts,
                    tx_details.transaction.hash,
                    err
                );
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                continue 'retry;
            }
        }
    }
}
// Save receipt_id, parent_transaction_hash, block_height and shard_id to the Db
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
async fn save_receipt(
    db_manager: &std::sync::Arc<Box<dyn database::TxIndexerDbManager + Sync + Send + 'static>>,
    receipt_id: &near_indexer_primitives::CryptoHash,
    parent_tx_hash: &near_indexer_primitives::CryptoHash,
    receiver_id: &near_indexer_primitives::types::AccountId,
    block_height: u64,
    block_hash: near_indexer_primitives::CryptoHash,
    shard_id: u64,
) -> anyhow::Result<()> {
    tracing::debug!(
        target: crate::INDEXER,
        "Saving receipt_id: {} to `receipts_map` in DB",
        receipt_id,
    );
    db_manager
        .save_receipt(
            receipt_id,
            parent_tx_hash,
            receiver_id,
            block_height,
            block_hash,
            shard_id,
        )
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
