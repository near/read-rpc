use itertools::EitherOrBoth::{Both, Left, Right};
use itertools::Itertools;

use futures::{FutureExt, StreamExt};
use tokio_retry::{strategy::FixedInterval, Retry};

use near_indexer_primitives::IndexerTransactionWithOutcome;

use crate::metrics;
use crate::storage;

const SAVE_ATTEMPTS: usize = 20;

#[allow(unused_variables)]
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
pub(crate) async fn index_transactions(
    streamer_message: &near_indexer_primitives::StreamerMessage,
    tx_collecting_storage: &std::sync::Arc<crate::storage::CacheStorage>,
    tx_details_storage: &std::sync::Arc<crate::TxDetailsStorage>,
    indexer_config: &configuration::TxIndexerConfig,
) -> anyhow::Result<()> {
    extract_transactions_to_collect(streamer_message, tx_collecting_storage, indexer_config)
        .await?;
    collect_receipts_and_outcomes(streamer_message, tx_collecting_storage).await?;

    let save_finished_tx_details_future =
        save_finished_transaction_details(tx_collecting_storage, tx_details_storage);

    let save_outcomes_and_receipts_future =
        save_outcomes_and_receipts(tx_details_storage, tx_collecting_storage);

    futures::future::join_all([
        save_finished_tx_details_future.boxed(),
        save_outcomes_and_receipts_future.boxed(),
    ])
    .await
    .into_iter()
    .collect::<anyhow::Result<_>>()
}

async fn save_finished_transaction_details(
    tx_collecting_storage: &std::sync::Arc<crate::storage::CacheStorage>,
    tx_details_storage: &std::sync::Arc<crate::TxDetailsStorage>,
) -> anyhow::Result<()> {
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

            futures::future::join_all(send_finished_transaction_details_futures).await;
        });
    }

    Ok(())
}

async fn save_outcomes_and_receipts(
    tx_details_storage: &std::sync::Arc<crate::TxDetailsStorage>,
    tx_collecting_storage: &std::sync::Arc<crate::storage::CacheStorage>,
) -> anyhow::Result<()> {
    let receipts_and_outcomes_to_save = tx_collecting_storage
        .outcomes_and_receipts_to_save()
        .await
        .map_err(|err| {
            tracing::error!(
                target: crate::INDEXER,
                "Failed to get receipts and outcomes to save\n{:#?}",
                err
            );
            err
        })?;

    let tx_collecting_storage = tx_collecting_storage.clone();
    let tx_details_storage = tx_details_storage.clone();
    tokio::spawn(async move {
        save_receipts_and_outcomes_details(
            &tx_details_storage,
            &tx_collecting_storage,
            receipts_and_outcomes_to_save.receipts,
            receipts_and_outcomes_to_save.outcomes,
        )
        .await;
    });

    Ok(())
}

/// Save receipts and outcomes to the DB
/// Split the receipts and outcomes into chunks of 1500 records and save them
/// It is necessary to split the records into chunks because Scylla has a limit per batch
/// We use 1500 records because we should be sure that the batch size is less than 50MB
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
async fn save_receipts_and_outcomes_details(
    tx_details_storage: &std::sync::Arc<crate::TxDetailsStorage>,
    tx_collecting_storage: &std::sync::Arc<crate::storage::CacheStorage>,
    receipts: Vec<readnode_primitives::ReceiptRecord>,
    outcomes: Vec<readnode_primitives::OutcomeRecord>,
) {
    let chunks: Vec<_> = receipts
        .chunks(1500)
        .zip_longest(outcomes.chunks(1500))
        .collect();

    let mut tasks = futures::stream::FuturesUnordered::new();

    for pair in chunks {
        let task = match pair {
            Both(receipts, outcomes) => save_chunks_receipts_and_outcomes_details(
                tx_details_storage,
                tx_collecting_storage,
                receipts.to_vec(),
                outcomes.to_vec(),
            ),
            Left(receipts) => save_chunks_receipts_and_outcomes_details(
                tx_details_storage,
                tx_collecting_storage,
                receipts.to_vec(),
                vec![],
            ),
            Right(outcomes) => save_chunks_receipts_and_outcomes_details(
                tx_details_storage,
                tx_collecting_storage,
                vec![],
                outcomes.to_vec(),
            ),
        };
        tasks.push(task);
    }

    while tasks.next().await.is_some() {}
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
async fn save_chunks_receipts_and_outcomes_details(
    tx_details_storage: &std::sync::Arc<crate::TxDetailsStorage>,
    tx_collecting_storage: &std::sync::Arc<crate::storage::CacheStorage>,
    receipts: Vec<readnode_primitives::ReceiptRecord>,
    outcomes: Vec<readnode_primitives::OutcomeRecord>,
) {
    match save_outcome_and_receipt_to_scylla(tx_details_storage, receipts.clone(), outcomes.clone())
        .await
    {
        Ok(_) => {
            tracing::debug!(
                target: crate::INDEXER,
                "Receipts and outcomes were saved",
            );
        }
        Err(err) => {
            tracing::error!(
                target: crate::INDEXER,
                "Failed to save receipts and outcomes: Error {}",
                err
            );

            tx_collecting_storage
                .return_outcomes_to_save(outcomes)
                .await;
            tx_collecting_storage
                .return_receipts_to_save(receipts)
                .await;
        }
    }
}

async fn save_outcome_and_receipt_to_scylla(
    tx_details_storage: &std::sync::Arc<crate::TxDetailsStorage>,
    receipts: Vec<readnode_primitives::ReceiptRecord>,
    outcomes: Vec<readnode_primitives::OutcomeRecord>,
) -> anyhow::Result<()> {
    let retry_strategy = FixedInterval::from_millis(500).take(SAVE_ATTEMPTS);

    let operation = || async {
        tx_details_storage
            .save_outcomes_and_receipts(receipts.clone(), outcomes.clone())
            .await
            .map_err(|e| {
                tracing::warn!(
                    target: crate::INDEXER,
                    "Failed to save receipts and outcomes: Error {}",
                    e
                );
                e
            })
    };

    Retry::spawn(retry_strategy, operation).await.map_err(|e| {
        anyhow::anyhow!(
            "Failed to save receipts and outcomes after {} attempts: {}",
            SAVE_ATTEMPTS,
            e
        )
    })?;

    tracing::debug!(
        target: crate::INDEXER,
        "Receipts and outcomes were saved successfully",
    );

    Ok(())
}

// Extracts all Transactions from the given `StreamerMessage` and pushes them to the memory storage
// by calling the function `new_transaction_details_to_collecting_pool`.
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
async fn extract_transactions_to_collect(
    streamer_message: &near_indexer_primitives::StreamerMessage,
    tx_collecting_storage: &std::sync::Arc<crate::storage::CacheStorage>,
    indexer_config: &configuration::TxIndexerConfig,
) -> anyhow::Result<()> {
    let block = readnode_primitives::BlockRecord {
        height: streamer_message.block.header.height,
        hash: streamer_message.block.header.hash,
    };

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
                    block,
                    shard_id.into(),
                    tx_collecting_storage,
                    indexer_config,
                )
            })
        });

    futures::future::join_all(futures)
        .await
        .into_iter()
        .collect::<anyhow::Result<_>>()
}

// Converts Transaction into CollectingTransactionDetails and puts it into memory storage.
// Also, adds the Receipt produced by ExecutionOutcome of the given Transaction to the watching list
// in memory storage
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
async fn new_transaction_details_to_collecting_pool(
    transaction: &IndexerTransactionWithOutcome,
    block: readnode_primitives::BlockRecord,
    shard_id: u64,
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
    add_outcome_and_receipt_to_save(
        tx_collecting_storage,
        &transaction.outcome.execution_outcome.id,
        converted_into_receipt_id,
        &transaction.transaction.hash,
        &transaction.transaction.receiver_id,
        block,
        shard_id,
    )
    .await?;

    let transaction_details = readnode_primitives::CollectingTransactionDetails::from_indexer_tx(
        transaction.clone(),
        block.height,
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
    tx_collecting_storage: &std::sync::Arc<crate::storage::CacheStorage>,
) -> anyhow::Result<()> {
    let block = readnode_primitives::BlockRecord {
        height: streamer_message.block.header.height,
        hash: streamer_message.block.header.hash,
    };
    let shard_futures = streamer_message
        .shards
        .iter()
        .map(|shard| process_shard(tx_collecting_storage, block, shard));

    futures::future::join_all(shard_futures)
        .await
        .into_iter()
        .collect::<anyhow::Result<_>>()
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
async fn process_shard(
    tx_collecting_storage: &std::sync::Arc<crate::storage::CacheStorage>,
    block: readnode_primitives::BlockRecord,
    shard: &near_indexer_primitives::IndexerShard,
) -> anyhow::Result<()> {
    let process_receipt_execution_outcome_futures =
        shard
            .receipt_execution_outcomes
            .iter()
            .map(|receipt_execution_outcome| {
                process_receipt_execution_outcome(
                    tx_collecting_storage,
                    block,
                    shard.shard_id.into(),
                    receipt_execution_outcome,
                )
            });

    futures::future::join_all(process_receipt_execution_outcome_futures)
        .await
        .into_iter()
        .collect::<anyhow::Result<_>>()
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
async fn process_receipt_execution_outcome(
    tx_collecting_storage: &std::sync::Arc<storage::CacheStorage>,
    block: readnode_primitives::BlockRecord,
    shard_id: u64,
    receipt_execution_outcome: &near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
) -> anyhow::Result<()> {
    if let Ok(transaction_key) = tx_collecting_storage
        .get_transaction_hash_by_receipt_id(
            &receipt_execution_outcome.receipt.receipt_id.to_string(),
        )
        .await
    {
        add_outcome_and_receipt_to_save(
            tx_collecting_storage,
            &receipt_execution_outcome.execution_outcome.id,
            &receipt_execution_outcome.receipt.receipt_id,
            &transaction_key.transaction_hash,
            &receipt_execution_outcome.receipt.receiver_id,
            block,
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
                "Failed to save transaction {}: Error {}",
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
    let tx_bytes = transaction_details.tx_serialize()?;

    let retry_strategy = FixedInterval::from_millis(500).take(SAVE_ATTEMPTS);

    let operation = || async {
        tx_details_storage
            .save_tx(&transaction_hash, tx_bytes.clone())
            .await
            .map_err(|e| {
                crate::metrics::TX_STORE_ERRORS_TOTAL.inc();
                tracing::warn!(
                    target: crate::INDEXER,
                    "Failed to save transaction {}: Error: {}",
                    transaction_hash,
                    e
                );
                e
            })
    };

    Retry::spawn(retry_strategy, operation).await.map_err(|e| {
        anyhow::anyhow!(
            "Failed to save transaction {} after {} attempts: {}",
            transaction_hash,
            SAVE_ATTEMPTS,
            e
        )
    })?;

    metrics::TX_IN_MEMORY_CACHE.dec();
    tracing::debug!(
        target: crate::INDEXER,
        "Transaction {} was saved successfully",
        transaction_hash,
    );

    Ok(())
}

// Save receipt_id, parent_transaction_hash, block_height and shard_id to the Db
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
async fn add_outcome_and_receipt_to_save(
    tx_collecting_storage: &std::sync::Arc<storage::CacheStorage>,
    outcome_id: &near_indexer_primitives::CryptoHash,
    receipt_id: &near_indexer_primitives::CryptoHash,
    parent_tx_hash: &near_indexer_primitives::CryptoHash,
    receiver_id: &near_indexer_primitives::types::AccountId,
    block: readnode_primitives::BlockRecord,
    shard_id: u64,
) -> anyhow::Result<()> {
    tracing::debug!(
        target: crate::INDEXER,
        "Push outcome: {}, and receipt: {} to save",
        outcome_id,
        receipt_id,
    );
    tx_collecting_storage
        .push_outcome_and_receipt_to_save(
            outcome_id,
            receipt_id,
            parent_tx_hash,
            receiver_id,
            block,
            shard_id,
        )
        .await
        .map_err(|err| {
            tracing::error!(
                target: crate::INDEXER,
                "Failed to push outcome_and_receipt to save \n{:#?}",
                err
            );
            err
        })?;
    Ok(())
}
