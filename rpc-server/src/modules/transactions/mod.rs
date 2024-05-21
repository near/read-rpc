pub mod methods;


pub async fn get_transaction_by_hash(
    db_manager: &std::sync::Arc<Box<dyn database::ReaderDbManager + Sync + Send + 'static>>,
    transaction_hash: &str,
    method_name: &str,
) -> anyhow::Result<readnode_primitives::TransactionDetails> {
    crate::metrics::SCYLLA_QUERIES.with_label_values(&[method_name, "tx_indexer.transactions_details"]).inc();
    if let Ok(tx) = db_manager.get_transaction_by_hash(&transaction_hash.to_string()).await {
        Ok(tx)
    } else {
        crate::metrics::SCYLLA_QUERIES.with_label_values(&[method_name, "tx_indexer_cache.transactions"]).inc();
        let mut tx = db_manager.get_indexing_transaction_by_hash(&transaction_hash.to_string()).await?;
        let receipt = db_manager.get_receipts_outcomes(&transaction_hash.to_string(), tx.block_height).await?;
        for (receipt, outcome) in receipt {
            crate::metrics::SCYLLA_QUERIES.with_label_values(&[method_name, "tx_indexer_cache.receipts_outcomes"]).inc();
            tx.receipts.push(receipt);
            tx.execution_outcomes.push(outcome);
        }
        Ok(tx.into())
    }
}