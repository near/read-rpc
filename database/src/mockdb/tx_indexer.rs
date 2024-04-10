use std::collections::HashMap;
use futures::StreamExt;
use near_indexer_primitives::IndexerExecutionOutcomeWithReceipt;
use num_traits::ToPrimitive;
use readnode_primitives::{CollectingTransactionDetails, TransactionKey};

pub struct MockDBManager {}

#[async_trait::async_trait]
impl crate::BaseDbManager for MockDBManager {
    async fn new(_config: &configuration::DatabaseConfig) -> anyhow::Result<Box<Self>> {
        Ok(Box::new(Self {}))
    }
}

#[async_trait::async_trait]
impl crate::TxIndexerDbManager for MockDBManager {
    async fn add_transaction(&self, transaction_hash: &str, tx_bytes: Vec<u8>, block_height: u64, signer_id: &str) -> anyhow::Result<()> {
        todo!()
    }

    async fn validate_saved_transaction_deserializable(&self, transaction_hash: &str, tx_bytes: &[u8]) -> anyhow::Result<bool> {
        todo!()
    }

    async fn add_receipt(&self, receipt_id: &str, parent_tx_hash: &str, block_height: u64, shard_id: u64) -> anyhow::Result<()> {
        todo!()
    }

    async fn update_meta(&self, indexer_id: &str, block_height: u64) -> anyhow::Result<()> {
        todo!()
    }

    async fn cache_add_transaction(&self, transaction_details: CollectingTransactionDetails) -> anyhow::Result<()> {
        todo!()
    }

    async fn cache_add_receipt(&self, transaction_key: TransactionKey, indexer_execution_outcome_with_receipt: IndexerExecutionOutcomeWithReceipt) -> anyhow::Result<()> {
        todo!()
    }

    async fn get_transactions_to_cache(&self, start_block_height: u64, cache_restore_blocks_range: u64, max_db_parallel_queries: i64) -> anyhow::Result<HashMap<TransactionKey, CollectingTransactionDetails>> {
        todo!()
    }

    async fn get_transaction_by_receipt_id(&self, receipt_id: &str) -> anyhow::Result<CollectingTransactionDetails> {
        todo!()
    }

    async fn get_receipts_in_cache(&self, transaction_key: &TransactionKey) -> anyhow::Result<Vec<IndexerExecutionOutcomeWithReceipt>> {
        todo!()
    }

    async fn cache_delete_transaction(&self, transaction_hash: &str, block_height: u64) -> anyhow::Result<()> {
        todo!()
    }

    async fn get_last_processed_block_height(&self, indexer_id: &str) -> anyhow::Result<u64> {
        todo!()
    }
}
