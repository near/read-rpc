#[async_trait::async_trait]
pub trait RpcDbManager {
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
    async fn get_all_state_keys(
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
    ) -> anyhow::Result<scylla::frame::response::result::Row>;

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

    async fn update_meta(
        &self,
        indexer_id: &str,
        block_height: u64,
    ) -> anyhow::Result<()>;


}

#[async_trait::async_trait]
pub trait StateIndexerDbManager {

}