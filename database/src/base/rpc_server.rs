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

    /// Returns state keys for the given account id by page
    async fn get_state_keys_by_page(
        &self,
        account_id: &near_primitives::types::AccountId,
        page_token: crate::PageToken,
    ) -> anyhow::Result<(Vec<readnode_primitives::StateKey>, crate::PageToken)>;

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
    ) -> (
        readnode_primitives::StateKey,
        readnode_primitives::StateValue,
    );

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

    /// Returns epoch validators info by the given epoch id
    async fn get_validators_by_epoch_id(
        &self,
        epoch_id: near_primitives::hash::CryptoHash,
    ) -> anyhow::Result<readnode_primitives::EpochValidatorsInfo>;

    /// Return epoch validators info by the given epoch end block height
    async fn get_validators_by_end_block_height(
        &self,
        block_height: near_primitives::types::BlockHeight,
    ) -> anyhow::Result<readnode_primitives::EpochValidatorsInfo>;

    /// Return protocol config by the given epoch id
    async fn get_protocol_config_by_epoch_id(
        &self,
        epoch_id: near_primitives::hash::CryptoHash,
    ) -> anyhow::Result<near_chain_configs::ProtocolConfigView>;
}
