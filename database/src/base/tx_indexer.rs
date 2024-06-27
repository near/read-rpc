#[async_trait::async_trait]
pub trait TxIndexerDbManager {
    async fn add_transaction(
        &self,
        transaction_hash: &str,
        tx_bytes: Vec<u8>,
        block_height: u64,
        signer_id: &str,
    ) -> anyhow::Result<()>;

    // This function is used to validate that the transaction is saved correctly.
    // For some unknown reason, tx-indexer saves invalid data for transactions.
    // We want to avoid these problems and get more information.
    // That's why we added this method.
    async fn validate_saved_transaction_deserializable(
        &self,
        transaction_hash: &str,
        tx_bytes: &[u8],
    ) -> anyhow::Result<bool>;

    async fn save_receipt(
        &self,
        receipt_id: &near_indexer_primitives::CryptoHash,
        parent_tx_hash: &near_indexer_primitives::CryptoHash,
        receiver_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_indexer_primitives::CryptoHash,
        shard_id: crate::primitives::ShardId,
    ) -> anyhow::Result<()>;

    async fn update_meta(&self, indexer_id: &str, block_height: u64) -> anyhow::Result<()>;

    async fn cache_add_transaction(
        &self,
        transaction_details: readnode_primitives::CollectingTransactionDetails,
    ) -> anyhow::Result<()>;

    async fn cache_add_receipt(
        &self,
        transaction_key: readnode_primitives::TransactionKey,
        indexer_execution_outcome_with_receipt: near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
    ) -> anyhow::Result<()>;

    async fn get_transactions_to_cache(
        &self,
        start_block_height: u64,
        cache_restore_blocks_range: u64,
        max_db_parallel_queries: i64,
    ) -> anyhow::Result<
        std::collections::HashMap<
            readnode_primitives::TransactionKey,
            readnode_primitives::CollectingTransactionDetails,
        >,
    >;

    async fn get_transaction_by_receipt_id(
        &self,
        receipt_id: &str,
    ) -> anyhow::Result<readnode_primitives::CollectingTransactionDetails>;

    async fn get_receipts_in_cache(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
    ) -> anyhow::Result<Vec<near_indexer_primitives::IndexerExecutionOutcomeWithReceipt>>;

    async fn cache_delete_transaction(
        &self,
        transaction_hash: &str,
        block_height: u64,
    ) -> anyhow::Result<()>;

    async fn get_last_processed_block_height(&self, indexer_id: &str) -> anyhow::Result<u64>;
}
