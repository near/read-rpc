use futures::future::{join_all, try_join_all};

use crate::storage;
use crate::types::TransactionDetails;
use near_indexer_primitives::views::ExecutionStatusView;
use near_indexer_primitives::IndexerTransactionWithOutcome;

pub(crate) async fn transactions(
    streamer_message: &near_indexer_primitives::StreamerMessage,
    redis_connection_manager: &storage::ConnectionManager,
) -> anyhow::Result<()> {
    collecting_tx(streamer_message, redis_connection_manager).await?;
    outcomes_and_receipts(streamer_message, redis_connection_manager).await?;

    let finished_transaction_details =
        crate::storage::transactions_to_send(redis_connection_manager).await?;

    if !finished_transaction_details.is_empty() {
        tokio::spawn(async move {
            let send_finished_transaction_details_futures = finished_transaction_details
                .into_iter()
                .map(send_transaction_details);

            join_all(send_finished_transaction_details_futures).await;
        });
    }

    Ok(())
}

async fn collecting_tx(
    streamer_message: &near_indexer_primitives::StreamerMessage,
    redis_connection_manager: &storage::ConnectionManager,
) -> anyhow::Result<()> {
    let futures = streamer_message
        .shards
        .iter()
        .filter_map(|shard| shard.chunk.as_ref())
        .flat_map(|chunk| chunk.transactions.iter())
        .filter_map(|tx| Some(start_collecting_tx(tx, redis_connection_manager)));
    try_join_all(futures).await.map(|_| ())
}

async fn start_collecting_tx(
    transaction: &IndexerTransactionWithOutcome,
    redis_connection_manager: &storage::ConnectionManager,
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

    let transaction_details = TransactionDetails::from_indexer_tx(transaction.clone());
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

async fn outcomes_and_receipts(
    streamer_message: &near_indexer_primitives::StreamerMessage,
    redis_connection_manager: &storage::ConnectionManager,
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

async fn send_transaction_details(transaction_details: TransactionDetails) -> bool {
    let tx_json = serde_json::to_string(&transaction_details).unwrap();
    println!("Transaction json {}", tx_json);
    true
}
