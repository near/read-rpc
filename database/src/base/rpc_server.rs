#[async_trait::async_trait]
pub trait ReaderDbManager {
    /// Searches the block height by the given block hash
    async fn get_block_height_by_hash(
        &self,
        block_hash: near_primitives::hash::CryptoHash,
        method_name: &str,
    ) -> anyhow::Result<u64>;

    /// Searches the block height and shard id by the given chunk hash
    async fn get_block_by_chunk_hash(
        &self,
        chunk_hash: near_primitives::hash::CryptoHash,
        method_name: &str,
    ) -> anyhow::Result<readnode_primitives::BlockHeightShardId>;

    /// Returns state for the given account id by page
    async fn get_state_by_page(
        &self,
        account_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        page_token: crate::PageToken,
        method_name: &str,
    ) -> anyhow::Result<(
        std::collections::HashMap<readnode_primitives::StateKey, readnode_primitives::StateValue>,
        crate::PageToken,
    )>;

    /// Returns state keys for the given account id filtered by the given prefix
    async fn get_state_by_key_prefix(
        &self,
        account_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        prefix: &[u8],
        method_name: &str,
    ) -> anyhow::Result<
        std::collections::HashMap<readnode_primitives::StateKey, readnode_primitives::StateValue>,
    >;

    /// Returns the state for the given account id at the given block height
    async fn get_state(
        &self,
        account_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        method_name: &str,
    ) -> anyhow::Result<
        std::collections::HashMap<readnode_primitives::StateKey, readnode_primitives::StateValue>,
    >;

    /// Returns the state for the given account id at the given block height
    async fn get_account_state(
        &self,
        account_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        prefix: &[u8],
        method_name: &str,
    ) -> anyhow::Result<
        std::collections::HashMap<readnode_primitives::StateKey, readnode_primitives::StateValue>,
    > {
        if prefix.is_empty() {
            self.get_state(account_id, block_height, method_name).await
        } else {
            self.get_state_by_key_prefix(account_id, block_height, prefix, method_name)
                .await
        }
    }

    /// Returns the state value for the given key of the given account at the given block height
    async fn get_state_key_value(
        &self,
        account_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        key_data: readnode_primitives::StateKey,
        method_name: &str,
    ) -> anyhow::Result<(
        readnode_primitives::StateKey,
        readnode_primitives::StateValue,
    )>;

    /// Returns the near_primitives::account::Account at the given block height
    async fn get_account(
        &self,
        account_id: &near_primitives::types::AccountId,
        request_block_height: near_primitives::types::BlockHeight,
        method_name: &str,
    ) -> anyhow::Result<readnode_primitives::QueryData<near_primitives::account::Account>>;

    /// Returns the contract code at the given block height
    async fn get_contract_code(
        &self,
        account_id: &near_primitives::types::AccountId,
        request_block_height: near_primitives::types::BlockHeight,
        method_name: &str,
    ) -> anyhow::Result<readnode_primitives::QueryData<Vec<u8>>>;

    /// Returns the near_primitives::account::AccessKey at the given block height
    async fn get_access_key(
        &self,
        account_id: &near_primitives::types::AccountId,
        request_block_height: near_primitives::types::BlockHeight,
        public_key: near_crypto::PublicKey,
        method_name: &str,
    ) -> anyhow::Result<readnode_primitives::QueryData<near_primitives::account::AccessKey>>;

    async fn get_account_access_keys(
        &self,
        account_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        method_name: &str,
    ) -> anyhow::Result<Vec<near_primitives::views::AccessKeyInfoView>>;

    /// Returns the block height and shard id by the given block height
    async fn get_block_by_height_and_shard_id(
        &self,
        block_height: near_primitives::types::BlockHeight,
        shard_id: near_primitives::types::ShardId,
        method_name: &str,
    ) -> anyhow::Result<readnode_primitives::BlockHeightShardId>;

    /// Returns epoch validators info by the given epoch id
    async fn get_validators_by_epoch_id(
        &self,
        epoch_id: near_primitives::hash::CryptoHash,
        method_name: &str,
    ) -> anyhow::Result<readnode_primitives::EpochValidatorsInfo>;

    /// Return epoch validators info by the given epoch end block height
    async fn get_validators_by_end_block_height(
        &self,
        block_height: near_primitives::types::BlockHeight,
        method_name: &str,
    ) -> anyhow::Result<readnode_primitives::EpochValidatorsInfo>;

    /// Converts an account ID to a shard ID
    fn get_shard_id_by_account_id(
        &self,
        account_id: &near_primitives::types::AccountId,
    ) -> near_primitives::types::ShardId;
}
