use crate::AdditionalDatabaseOptions;
use near_indexer_primitives::IndexerExecutionOutcomeWithReceipt;
use readnode_primitives::{CollectingTransactionDetails, TransactionDetails, TransactionKey};
use std::collections::HashMap;

pub(crate) struct PostgresDBManager {
    // scylla_session: std::sync::Arc<scylla::Session>,
    // add_transaction: PreparedStatement,
    // add_receipt: PreparedStatement,
    // update_meta: PreparedStatement,
    // last_processed_block_height: PreparedStatement,
    //
    // cache_get_all_transactions: PreparedStatement,
    // cache_get_transaction: PreparedStatement,
    // cache_get_transaction_by_receipt_id: PreparedStatement,
    // cache_get_receipts: PreparedStatement,
    // cache_add_transaction: PreparedStatement,
    // cache_delete_transaction: PreparedStatement,
    // cache_add_receipt: PreparedStatement,
    // cache_delete_receipts: PreparedStatement,
}

#[async_trait::async_trait]
impl crate::BaseDbManager for PostgresDBManager {
    async fn new(
        database_url: &str,
        database_user: Option<&str>,
        database_password: Option<&str>,
        database_options: AdditionalDatabaseOptions,
    ) -> anyhow::Result<Box<Self>> {
        todo!()
    }
}

#[async_trait::async_trait]
impl crate::TxIndexerDbManager for PostgresDBManager {
    async fn add_transaction(
        &self,
        transaction: TransactionDetails,
        block_height: u64,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn add_receipt(
        &self,
        receipt_id: &str,
        parent_tx_hash: &str,
        block_height: u64,
        shard_id: u64,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn update_meta(&self, indexer_id: &str, block_height: u64) -> anyhow::Result<()> {
        todo!()
    }

    async fn cache_add_transaction(
        &self,
        transaction_details: CollectingTransactionDetails,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn cache_add_receipt(
        &self,
        transaction_key: TransactionKey,
        indexer_execution_outcome_with_receipt: IndexerExecutionOutcomeWithReceipt,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn get_transactions_to_cache(
        &self,
        start_block_height: u64,
        cache_restore_blocks_range: u64,
        max_db_parallel_queries: i64,
    ) -> anyhow::Result<HashMap<TransactionKey, CollectingTransactionDetails>> {
        todo!()
    }

    async fn get_transaction_by_receipt_id(
        &self,
        receipt_id: &str,
    ) -> anyhow::Result<CollectingTransactionDetails> {
        todo!()
    }

    async fn get_receipts_in_cache(
        &self,
        transaction_key: &TransactionKey,
    ) -> anyhow::Result<Vec<IndexerExecutionOutcomeWithReceipt>> {
        todo!()
    }

    async fn cache_delete_transaction(
        &self,
        transaction_hash: &str,
        block_height: u64,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn get_last_processed_block_height(&self, indexer_id: &str) -> anyhow::Result<u64> {
        todo!()
    }
}
