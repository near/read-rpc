use crate::{config, storage};
use futures::future::{join_all, try_join_all};
use near_indexer_primitives::views::ExecutionStatusView;
use near_indexer_primitives::IndexerTransactionWithOutcome;

pub(crate) async fn index_transactions(
    streamer_message: &near_indexer_primitives::StreamerMessage,
    scylla_db_client: &std::sync::Arc<config::ScyllaDBManager>,
    redis_connection_manager: &redis::aio::ConnectionManager,
) -> anyhow::Result<()> {
    extract_transactions_to_collect(streamer_message, redis_connection_manager).await?;
    collect_receipts_and_outcomes(streamer_message, redis_connection_manager).await?;

    let finished_transaction_details =
        storage::transactions_to_save(redis_connection_manager).await?;

    if !finished_transaction_details.is_empty() {
        let scylla_db_client = scylla_db_client.clone();
        let block_height = streamer_message.block.header.height.clone();
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

async fn extract_transactions_to_collect(
    streamer_message: &near_indexer_primitives::StreamerMessage,
    redis_connection_manager: &redis::aio::ConnectionManager,
) -> anyhow::Result<()> {
    let futures = streamer_message
        .shards
        .iter()
        .filter_map(|shard| shard.chunk.as_ref())
        .flat_map(|chunk| chunk.transactions.iter())
        .filter_map(|tx| {
            Some(new_transaction_details_to_collecting_pool(
                tx,
                redis_connection_manager,
            ))
        });
    try_join_all(futures).await.map(|_| ())
}

async fn new_transaction_details_to_collecting_pool(
    transaction: &IndexerTransactionWithOutcome,
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

    let transaction_details =
        readnode_primitives::TransactionDetails::from_indexer_tx(transaction.clone());
    match storage::set_tx(redis_connection_manager, transaction_details).await {
        Ok(_) => {
            storage::push_receipt_to_watching_list(
                redis_connection_manager,
                &converted_into_receipt_id,
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

async fn collect_receipts_and_outcomes(
    streamer_message: &near_indexer_primitives::StreamerMessage,
    redis_connection_manager: &redis::aio::ConnectionManager,
) -> anyhow::Result<()> {
    let receipt_execution_outcomes = streamer_message
        .shards
        .iter()
        .flat_map(|shard| shard.receipt_execution_outcomes.iter());

    for receipt_execution_outcome in receipt_execution_outcomes {
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
            // Add the newly produced receipt_ids to the watching list
            for receipt_id in receipt_execution_outcome
                .execution_outcome
                .outcome
                .receipt_ids
                .iter()
            {
                tracing::debug!(target: crate::INDEXER, "+R {}", &receipt_id.to_string(),);
                storage::push_receipt_to_watching_list(
                    redis_connection_manager,
                    &receipt_id.to_string(),
                    &transaction_hash,
                )
                .await?;
            }

            // Add the success receipt to the watching list
            if let ExecutionStatusView::SuccessReceiptId(receipt_id) =
                receipt_execution_outcome.execution_outcome.outcome.status
            {
                tracing::debug!(target: crate::INDEXER, "+R {}", &receipt_id.to_string(),);
                storage::push_receipt_to_watching_list(
                    redis_connection_manager,
                    &receipt_id.to_string(),
                    &transaction_hash,
                )
                .await?;
            }

            match storage::push_outcome_and_receipt(
                redis_connection_manager,
                &transaction_hash,
                receipt_execution_outcome.clone(),
            )
            .await
            {
                Ok(_) => {}
                Err(e) => tracing::error!(
                    target: crate::INDEXER,
                    "Failed to push_outcome_and_receipt\n{:#?}",
                    e
                ),
            };
        }
    }
    Ok(())
}

// Save transaction detail into the scylla db
async fn save_transaction_details(
    scylla_db_client: &std::sync::Arc<config::ScyllaDBManager>,
    transaction_details: readnode_primitives::TransactionDetails,
    block_height: u64,
) -> bool {
    match scylla_db_client
        .add_transaction(transaction_details, block_height)
        .await
    {
        Ok(_) => true,
        Err(e) => {
            tracing::error!(
                target: crate::INDEXER,
                "Failed to save transaction \n{:#?}",
                e
            );
            false
        }
    }
}
