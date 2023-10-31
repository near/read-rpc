use futures::{
    future::{join_all, try_join_all},
    StreamExt,
};
use near_indexer_primitives::IndexerTransactionWithOutcome;

use crate::config;
use crate::storage::base::TxCollectingStorage;

/// Blocks #47317863 and #47317864 with restored receipts.
const PROBLEMATIC_BLOCKS: [near_indexer_primitives::CryptoHash; 2] = [
    near_indexer_primitives::CryptoHash(
        *b"\xcd\xde\x9a\x3f\x5d\xdf\xb4\x2c\xb9\x9b\xf4\x8c\x04\x95\x6f\x5b\
           \xa0\xb7\x29\xe2\xa5\x04\xf8\xbd\x9c\x86\x92\xd6\x16\x8c\xcf\x14",
    ),
    near_indexer_primitives::CryptoHash(
        *b"\x12\xa9\x5a\x1a\x3d\x14\xa7\x36\xb3\xce\xe6\xea\x07\x20\x8e\x75\
           \x4e\xb5\xc2\xd7\xf9\x11\xca\x29\x09\xe0\xb8\x85\xb5\x2b\x95\x6a",
    ),
];

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
pub(crate) async fn index_transactions(
    chain_id: config::ChainId,
    streamer_message: &near_indexer_primitives::StreamerMessage,
    scylla_db_client: &std::sync::Arc<config::ScyllaDBManager>,
    tx_collecting_storage: &std::sync::Arc<impl TxCollectingStorage>,
) -> anyhow::Result<()> {
    extract_transactions_to_collect(streamer_message, scylla_db_client, tx_collecting_storage)
        .await?;
    collect_receipts_and_outcomes(
        chain_id,
        streamer_message,
        scylla_db_client,
        tx_collecting_storage,
    )
    .await?;

    let finished_transaction_details = tx_collecting_storage.transactions_to_save().await?;

    if !finished_transaction_details.is_empty() {
        let scylla_db_client = scylla_db_client.clone();
        tokio::spawn(async move {
            let send_finished_transaction_details_futures = finished_transaction_details
                .into_iter()
                .map(|tx_details| save_transaction_details(&scylla_db_client, tx_details));

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
    tx_collecting_storage: &std::sync::Arc<impl TxCollectingStorage>,
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
    tx_collecting_storage: &std::sync::Arc<impl TxCollectingStorage>,
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

    let transaction_details = readnode_primitives::CollectingTransactionDetails::from_indexer_tx(
        transaction.clone(),
        block_height,
    );
    let transaction_key = transaction_details.transaction_key();
    match tx_collecting_storage.set_tx(transaction_details).await {
        Ok(_) => {
            tx_collecting_storage
                .push_receipt_to_watching_list(converted_into_receipt_id, transaction_key)
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
    chain_id: config::ChainId,
    streamer_message: &near_indexer_primitives::StreamerMessage,
    scylla_db_client: &std::sync::Arc<config::ScyllaDBManager>,
    tx_collecting_storage: &std::sync::Arc<impl TxCollectingStorage>,
) -> anyhow::Result<()> {
    let block_height = streamer_message.block.header.height;
    let block_hash = streamer_message.block.header.hash;

    let shard_futures = streamer_message.shards.iter().map(|shard| {
        process_shard(
            chain_id.clone(),
            scylla_db_client,
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
    chain_id: config::ChainId,
    scylla_db_client: &std::sync::Arc<config::ScyllaDBManager>,
    tx_collecting_storage: &std::sync::Arc<impl TxCollectingStorage>,
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
                    chain_id.clone(),
                    scylla_db_client,
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
    chain_id: config::ChainId,
    scylla_db_client: &std::sync::Arc<config::ScyllaDBManager>,
    tx_collecting_storage: &std::sync::Arc<impl TxCollectingStorage>,
    block_height: u64,
    block_hash: near_indexer_primitives::CryptoHash,
    shard_id: u64,
    receipt_execution_outcome: &near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
) -> anyhow::Result<()> {
    if PROBLEMATIC_BLOCKS.contains(&block_hash) {
        if let config::ChainId::Mainnet(_) = chain_id {
            tx_collecting_storage
                .restore_transaction_by_receipt_id(
                    &receipt_execution_outcome.receipt.receipt_id.to_string(),
                )
                .await?;
        }
    }

    if let Ok(transaction_key) = tx_collecting_storage
        .get_transaction_hash_by_receipt_id(
            &receipt_execution_outcome.receipt.receipt_id.to_string(),
        )
        .await
    {
        save_receipt(
            scylla_db_client,
            &receipt_execution_outcome.receipt.receipt_id.to_string(),
            &transaction_key.transaction_hash,
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

// Save transaction detail into the scylla db
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
async fn save_transaction_details(
    scylla_db_client: &std::sync::Arc<config::ScyllaDBManager>,
    tx_details: readnode_primitives::CollectingTransactionDetails,
) -> bool {
    let transaction_details = match tx_details.to_final_transaction_result() {
        Ok(details) => details,
        Err(err) => {
            tracing::error!(
                target: crate::INDEXER,
                "Failed to get final transaction {} \n{:#?}",
                tx_details.transaction.hash,
                err
            );
            return false;
        }
    };
    let transaction_hash = transaction_details.transaction.hash.to_string();
    match scylla_db_client
        .add_transaction(transaction_details, tx_details.block_height)
        .await
    {
        Ok(_) => {
            scylla_db_client
                .cache_delete_transaction(&transaction_hash, tx_details.block_height)
                .await
                .expect("Failed to delete transaction from memory storage");
            true
        }
        Err(err) => {
            tracing::error!(
                target: crate::INDEXER,
                "Failed to save transaction {} \n{:#?}",
                tx_details.transaction.hash,
                err
            );
            false
        }
    }
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
