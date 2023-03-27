use futures::{
    StreamExt,
    future::{join_all, try_join_all}
};

use near_indexer_primitives::views::ExecutionStatusView;
use near_indexer_primitives::IndexerTransactionWithOutcome;

use crate::{config, storage};

// TODO: Handle known TX hash collision case for mainnet
// ref: https://github.com/near/near-indexer-for-explorer/issues/84
pub(crate) async fn index_transactions(
    streamer_message: &near_indexer_primitives::StreamerMessage,
    scylla_db_client: &std::sync::Arc<config::ScyllaDBManager>,
    redis_connection_manager: &redis::aio::ConnectionManager,
) -> anyhow::Result<()> {
    extract_transactions_to_collect(streamer_message, scylla_db_client, redis_connection_manager)
        .await?;
    collect_receipts_and_outcomes(streamer_message, scylla_db_client, redis_connection_manager)
        .await?;

    let finished_transaction_details =
        storage::transactions_to_save(redis_connection_manager).await?;

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

// Extracts all Transactions from the given `StreamerMessage` and pushes them to the Redis
// by calling the function `new_transaction_details_to_collecting_pool`.
async fn extract_transactions_to_collect(
    streamer_message: &near_indexer_primitives::StreamerMessage,
    scylla_db_client: &std::sync::Arc<config::ScyllaDBManager>,
    redis_connection_manager: &redis::aio::ConnectionManager,
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
                    redis_connection_manager,
                )
            })
        });
    try_join_all(futures).await.map(|_| ())
}

// Converts Transaction into CollectingTransactionDetails and puts it into Redis.
// Also, adds the Receipt produced by ExecutionOutcome of the given Transaction to the watching list
// in Redis
async fn new_transaction_details_to_collecting_pool(
    transaction: &IndexerTransactionWithOutcome,
    block_height: u64,
    shard_id: u64,
    scylla_db_client: &std::sync::Arc<config::ScyllaDBManager>,
    redis_connection_manager: &redis::aio::ConnectionManager,
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
    match storage::set_tx(redis_connection_manager, transaction_details).await {
        Ok(_) => {
            storage::push_receipt_to_watching_list(
                redis_connection_manager,
                converted_into_receipt_id,
                &transaction_hash_string,
            )
            .await?;
        }
        Err(e) => tracing::error!(
            target: crate::INDEXER,
            "Failed to add TransactionDetails to Redis\n{:#?}",
            e
        ),
    }

    Ok(())
}

// async fn collect_receipts_and_outcomes(
//     streamer_message: &near_indexer_primitives::StreamerMessage,
//     scylla_db_client: &std::sync::Arc<config::ScyllaDBManager>,
//     redis_connection_manager: &redis::aio::ConnectionManager,
// ) -> anyhow::Result<()> {
//     let block_height = streamer_message.block.header.height;

//     for shard in &streamer_message.shards {
//         for receipt_execution_outcome in &shard.receipt_execution_outcomes {
//             if let Ok(Some(transaction_hash)) = storage::remove_receipt_from_watching_list(
//                 redis_connection_manager,
//                 &receipt_execution_outcome.receipt.receipt_id.to_string(),
//             )
//             .await
//             {
//                 tracing::debug!(
//                     target: crate::INDEXER,
//                     "-R {}",
//                     &receipt_execution_outcome.receipt.receipt_id.to_string(),
//                 );
//                 // Add the newly produced receipt_ids to the watching list
//                 for receipt_id in receipt_execution_outcome
//                     .execution_outcome
//                     .outcome
//                     .receipt_ids
//                     .iter()
//                 {
//                     tracing::debug!(target: crate::INDEXER, "+R {}", &receipt_id.to_string(),);
//                     storage::push_receipt_to_watching_list(
//                         redis_connection_manager,
//                         receipt_id.to_string(),
//                         &transaction_hash,
//                     )
//                     .await?;
//                     save_receipt(
//                         scylla_db_client,
//                         &receipt_id.to_string(),
//                         &transaction_hash,
//                         block_height,
//                         shard.shard_id,
//                     )
//                     .await?;
//                 }

//                 // Add the success receipt to the watching list
//                 if let ExecutionStatusView::SuccessReceiptId(receipt_id) =
//                     receipt_execution_outcome.execution_outcome.outcome.status
//                 {
//                     tracing::debug!(target: crate::INDEXER, "+R {}", &receipt_id.to_string(),);
//                     storage::push_receipt_to_watching_list(
//                         redis_connection_manager,
//                         receipt_id.to_string(),
//                         &transaction_hash,
//                     )
//                     .await?;
//                     save_receipt(
//                         scylla_db_client,
//                         &receipt_id.to_string(),
//                         &transaction_hash,
//                         block_height,
//                         shard.shard_id,
//                     )
//                     .await?;
//                 }

//                 let _ = storage::push_outcome_and_receipt(
//                     redis_connection_manager,
//                     &transaction_hash,
//                     receipt_execution_outcome.clone(),
//                 )
//                 .await
//                 .map_err(|e| {
//                     tracing::error!(
//                         target: crate::INDEXER,
//                         "Failed to push_outcome_and_receipt\n{:#?}",
//                         e
//                     )
//                 });

//             }
//         }
//     }
//     Ok(())
// }

async fn collect_receipts_and_outcomes(
    streamer_message: &near_indexer_primitives::StreamerMessage,
    scylla_db_client: &std::sync::Arc<config::ScyllaDBManager>,
    redis_connection_manager: &redis::aio::ConnectionManager,
) -> anyhow::Result<()> {
    let block_height = streamer_message.block.header.height;

    let shard_futures = streamer_message
        .shards
        .iter()
        .map(|shard| {
            process_shard(
                scylla_db_client,
                redis_connection_manager,
                block_height,
                shard,
            )
        });

    futures::future::try_join_all(shard_futures).await?;

    Ok(())
}

async fn process_shard(
    scylla_db_client: &std::sync::Arc<config::ScyllaDBManager>,
    redis_connection_manager: &redis::aio::ConnectionManager,
    block_height: u64,
    shard: &near_indexer_primitives::IndexerShard,
) -> anyhow::Result<()> {
    let process_receipt_execution_outcome_futures = shard
        .receipt_execution_outcomes
        .iter()
        .map(|receipt_execution_outcome| {
            process_receipt_execution_outcome(
                scylla_db_client,
                redis_connection_manager,
                block_height,
                shard.shard_id,
                receipt_execution_outcome,
            )
        });

    futures::future::try_join_all(process_receipt_execution_outcome_futures).await?;

    Ok(())
}

async fn process_receipt_execution_outcome(
    scylla_db_client: &std::sync::Arc<config::ScyllaDBManager>,
    redis_connection_manager: &redis::aio::ConnectionManager,
    block_height: u64,
    shard_id: u64,
    receipt_execution_outcome: &near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
) -> anyhow::Result<()> {
    if let Ok(Some(transaction_hash)) = storage::remove_receipt_from_watching_list(
        redis_connection_manager,
        &receipt_execution_outcome.receipt.receipt_id.to_string(),
    )
    .await
    {
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
                    storage::push_receipt_to_watching_list(
                        redis_connection_manager,
                        receipt_id.to_string(),
                        &transaction_hash,
                    )
                }),
        );

        // Add the success receipt to the watching list
        if let ExecutionStatusView::SuccessReceiptId(receipt_id) =
            receipt_execution_outcome.execution_outcome.outcome.status
        {
            tracing::debug!(target: crate::INDEXER, "+R {}", &receipt_id.to_string(),);
            tasks.push(storage::push_receipt_to_watching_list(
                redis_connection_manager,
                receipt_id.to_string(),
                &transaction_hash,
            ));
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


        let _ = storage::push_outcome_and_receipt(
            redis_connection_manager,
            &transaction_hash,
            receipt_execution_outcome.clone(),
        )
        .await
        .map_err(|e| {
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
            tracing::error!(target: crate::INDEXER, "Failed to save receipt \n{:#?}", err);
            err
        })?;
    Ok(())
}
