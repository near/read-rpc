#[async_trait::async_trait]
pub trait ReaderDbManager {
    /// Searches the block height by the given block hash
    async fn get_block_by_hash(
        &self,
        block_hash: near_primitives::hash::CryptoHash,
    ) -> anyhow::Result<u64>;

    /// Searches the block height and shard id by the given chunk hash
    async fn get_block_by_chunk_hash(
        &self,
        chunk_hash: near_primitives::hash::CryptoHash,
    ) -> anyhow::Result<readnode_primitives::BlockHeightShardId>;

    /// Returns all state keys for the given account id
    async fn get_state_keys_all(
        &self,
        account_id: &near_primitives::types::AccountId,
    ) -> anyhow::Result<Vec<readnode_primitives::StateKey>>;

    /// Returns state keys for the given account id filtered by the given prefix
    async fn get_state_keys_by_prefix(
        &self,
        account_id: &near_primitives::types::AccountId,
        prefix: &[u8],
    ) -> anyhow::Result<Vec<readnode_primitives::StateKey>>;

    /// Returns the state value for the given key of the given account at the given block height
    async fn get_state_key_value(
        &self,
        account_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        key_data: readnode_primitives::StateKey,
    ) -> anyhow::Result<readnode_primitives::StateValue>;

    /// Returns the near_primitives::account::Account at the given block height
    async fn get_account(
        &self,
        account_id: &near_primitives::types::AccountId,
        request_block_height: near_primitives::types::BlockHeight,
    ) -> anyhow::Result<readnode_primitives::QueryData<near_primitives::account::Account>>;

    /// Returns the contract code at the given block height
    async fn get_contract_code(
        &self,
        account_id: &near_primitives::types::AccountId,
        request_block_height: near_primitives::types::BlockHeight,
    ) -> anyhow::Result<readnode_primitives::QueryData<Vec<u8>>>;

    /// Returns the near_primitives::account::AccessKey at the given block height
    async fn get_access_key(
        &self,
        account_id: &near_primitives::types::AccountId,
        request_block_height: near_primitives::types::BlockHeight,
        public_key: near_crypto::PublicKey,
    ) -> anyhow::Result<readnode_primitives::QueryData<near_primitives::account::AccessKey>>;

    #[cfg(feature = "account_access_keys")]
    async fn get_account_access_keys(
        &self,
        account_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
    ) -> anyhow::Result<std::collections::HashMap<String, Vec<u8>>>;

    /// Returns the near_primitives::views::ReceiptView at the given receipt_id
    async fn get_receipt_by_id(
        &self,
        receipt_id: near_primitives::hash::CryptoHash,
    ) -> anyhow::Result<readnode_primitives::ReceiptRecord>;

    /// Returns the readnode_primitives::TransactionDetails at the given transaction hash
    async fn get_transaction_by_hash(
        &self,
        transaction_hash: &str,
    ) -> anyhow::Result<readnode_primitives::TransactionDetails>;

    /// Returns the block height and shard id by the given block height
    async fn get_block_by_height_and_shard_id(
        &self,
        block_height: near_primitives::types::BlockHeight,
        shard_id: near_primitives::types::ShardId,
    ) -> anyhow::Result<readnode_primitives::BlockHeightShardId>;
}

#[async_trait::async_trait]
pub trait StateIndexerDbManager {
    async fn add_state_changes(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
        key: &[u8],
        value: &[u8],
    ) -> anyhow::Result<()>;

    async fn delete_state_changes(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
        key: &[u8],
    ) -> anyhow::Result<()>;

    async fn add_access_key(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
        public_key: &[u8],
        access_key: &[u8],
    ) -> anyhow::Result<()>;

    async fn delete_access_key(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
        public_key: &[u8],
    ) -> anyhow::Result<()>;

    #[cfg(feature = "account_access_keys")]
    async fn get_access_keys(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: u64,
    ) -> anyhow::Result<std::collections::HashMap<String, Vec<u8>>>;

    #[cfg(feature = "account_access_keys")]
    async fn add_account_access_keys(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: u64,
        public_key: &[u8],
        access_key: Option<&[u8]>,
    ) -> anyhow::Result<()>;

    #[cfg(feature = "account_access_keys")]
    async fn update_account_access_keys(
        &self,
        account_id: String,
        block_height: u64,
        account_keys: std::collections::HashMap<String, Vec<u8>>,
    ) -> anyhow::Result<()>;

    async fn add_contract_code(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
        code: &[u8],
    ) -> anyhow::Result<()>;

    async fn delete_contract_code(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()>;

    async fn add_account(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
        account: Vec<u8>,
    ) -> anyhow::Result<()>;

    async fn delete_account(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()>;

    async fn add_block(
        &self,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()>;

    async fn add_chunks(
        &self,
        block_height: u64,
        chunks: Vec<(
            crate::primitives::ChunkHash,
            crate::primitives::ShardId,
            crate::primitives::HeightIncluded,
        )>,
    ) -> anyhow::Result<()>;

    async fn update_meta(&self, indexer_id: &str, block_height: u64) -> anyhow::Result<()>;
    async fn get_last_processed_block_height(&self, indexer_id: &str) -> anyhow::Result<u64>;
}

#[async_trait::async_trait]
pub trait TxIndexerDbManager {
    async fn add_transaction(
        &self,
        transaction: readnode_primitives::TransactionDetails,
        block_height: u64,
    ) -> anyhow::Result<()>;

    async fn add_receipt(
        &self,
        receipt_id: &str,
        parent_tx_hash: &str,
        block_height: u64,
        shard_id: u64,
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
