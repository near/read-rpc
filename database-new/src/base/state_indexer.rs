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

    async fn get_block_by_hash(
        &self,
        block_hash: near_primitives::hash::CryptoHash,
    ) -> anyhow::Result<u64>;

    async fn update_meta(&self, indexer_id: &str, block_height: u64) -> anyhow::Result<()>;
    async fn get_last_processed_block_height(&self, indexer_id: &str) -> anyhow::Result<u64>;
    async fn add_validators(
        &self,
        epoch_id: near_indexer_primitives::CryptoHash,
        epoch_height: u64,
        epoch_start_height: u64,
        validators_info: &near_primitives::views::EpochValidatorInfo,
    ) -> anyhow::Result<()>;

    async fn add_protocol_config(
        &self,
        epoch_id: near_indexer_primitives::CryptoHash,
        epoch_height: u64,
        epoch_start_height: u64,
        protocol_config: &near_chain_configs::ProtocolConfigView,
    ) -> anyhow::Result<()>;

    async fn update_epoch_end_height(
        &self,
        epoch_id: near_indexer_primitives::CryptoHash,
        epoch_end_block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()>;

    async fn save_block(
        &self,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
        chunks: Vec<(
            crate::primitives::ChunkHash,
            crate::primitives::ShardId,
            crate::primitives::HeightIncluded,
        )>,
    ) -> anyhow::Result<()> {
        let add_block_future = self.add_block(block_height, block_hash);
        let add_chunks_future = self.add_chunks(block_height, chunks);

        futures::try_join!(add_block_future, add_chunks_future)?;
        Ok(())
    }

    async fn save_state_change(
        &self,
        state_change: &near_primitives::views::StateChangeWithCauseView,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()> {
        match &state_change.value {
            near_primitives::views::StateChangeValueView::DataUpdate {
                account_id,
                key,
                value,
            } => {
                self.add_state_changes(
                    account_id.clone(),
                    block_height,
                    block_hash,
                    key.as_ref(),
                    value.as_ref(),
                )
                .await?
            }
            near_primitives::views::StateChangeValueView::DataDeletion { account_id, key } => {
                self.delete_state_changes(
                    account_id.clone(),
                    block_height,
                    block_hash,
                    key.as_ref(),
                )
                .await?
            }
            near_primitives::views::StateChangeValueView::AccessKeyUpdate {
                account_id,
                public_key,
                access_key,
            } => {
                let data_key = borsh::to_vec(&public_key)?;
                let data_value = borsh::to_vec(&access_key)?;
                let add_access_key_future = self.add_access_key(
                    account_id.clone(),
                    block_height,
                    block_hash,
                    &data_key,
                    &data_value,
                );

                #[cfg(feature = "account_access_keys")]
                {
                    let add_account_access_keys_future = self.add_account_access_keys(
                        account_id.clone(),
                        block_height,
                        &data_key,
                        Some(&data_value),
                    );
                    futures::try_join!(add_access_key_future, add_account_access_keys_future)?;
                }
                #[cfg(not(feature = "account_access_keys"))]
                add_access_key_future.await?;
            }
            near_primitives::views::StateChangeValueView::AccessKeyDeletion {
                account_id,
                public_key,
            } => {
                let data_key = borsh::to_vec(&public_key)?;
                let delete_access_key_future =
                    self.delete_access_key(account_id.clone(), block_height, block_hash, &data_key);

                #[cfg(feature = "account_access_keys")]
                {
                    let delete_account_access_keys_future = self.add_account_access_keys(
                        account_id.clone(),
                        block_height,
                        &data_key,
                        None,
                    );
                    futures::try_join!(
                        delete_access_key_future,
                        delete_account_access_keys_future
                    )?;
                }
                #[cfg(not(feature = "account_access_keys"))]
                delete_access_key_future.await?;
            }
            near_primitives::views::StateChangeValueView::ContractCodeUpdate {
                account_id,
                code,
            } => {
                self.add_contract_code(account_id.clone(), block_height, block_hash, code.as_ref())
                    .await?
            }
            near_primitives::views::StateChangeValueView::ContractCodeDeletion { account_id } => {
                self.delete_contract_code(account_id.clone(), block_height, block_hash)
                    .await?
            }
            near_primitives::views::StateChangeValueView::AccountUpdate {
                account_id,
                account,
            } => {
                let value = borsh::to_vec(&near_primitives::account::Account::from(account))?;
                self.add_account(account_id.clone(), block_height, block_hash, value)
                    .await?
            }
            near_primitives::views::StateChangeValueView::AccountDeletion { account_id } => {
                self.delete_account(account_id.clone(), block_height, block_hash)
                    .await?
            }
        }
        Ok(())
    }
}
