use crate::primitives::{ChunkHash, HeightIncluded, ShardId};
use near_indexer_primitives::CryptoHash;
use near_primitives::types::AccountId;
use near_primitives::views::{EpochValidatorInfo, StateChangeWithCauseView};
use std::collections::HashMap;

pub struct MockDBManager {}

#[async_trait::async_trait]
impl crate::BaseDbManager for MockDBManager {
    async fn new(_config: &configuration::DatabaseConfig) -> anyhow::Result<Box<Self>> {
        Ok(Box::new(Self {}))
    }
}

#[async_trait::async_trait]
impl crate::StateIndexerDbManager for MockDBManager {
    async fn add_state_changes(
        &self,
        account_id: AccountId,
        block_height: u64,
        block_hash: CryptoHash,
        key: &[u8],
        value: &[u8],
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn delete_state_changes(
        &self,
        account_id: AccountId,
        block_height: u64,
        block_hash: CryptoHash,
        key: &[u8],
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn add_access_key(
        &self,
        account_id: AccountId,
        block_height: u64,
        block_hash: CryptoHash,
        public_key: &[u8],
        access_key: &[u8],
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn delete_access_key(
        &self,
        account_id: AccountId,
        block_height: u64,
        block_hash: CryptoHash,
        public_key: &[u8],
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn get_access_keys(
        &self,
        account_id: AccountId,
        block_height: u64,
    ) -> anyhow::Result<HashMap<String, Vec<u8>>> {
        todo!()
    }

    async fn add_account_access_keys(
        &self,
        account_id: AccountId,
        block_height: u64,
        public_key: &[u8],
        access_key: Option<&[u8]>,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn update_account_access_keys(
        &self,
        account_id: String,
        block_height: u64,
        account_keys: HashMap<String, Vec<u8>>,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn add_contract_code(
        &self,
        account_id: AccountId,
        block_height: u64,
        block_hash: CryptoHash,
        code: &[u8],
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn delete_contract_code(
        &self,
        account_id: AccountId,
        block_height: u64,
        block_hash: CryptoHash,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn add_account(
        &self,
        account_id: AccountId,
        block_height: u64,
        block_hash: CryptoHash,
        account: Vec<u8>,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn delete_account(
        &self,
        account_id: AccountId,
        block_height: u64,
        block_hash: CryptoHash,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn add_block(&self, block_height: u64, block_hash: CryptoHash) -> anyhow::Result<()> {
        todo!()
    }

    async fn add_chunks(
        &self,
        block_height: u64,
        chunks: Vec<(ChunkHash, ShardId, HeightIncluded)>,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn get_block_by_hash(&self, block_hash: CryptoHash) -> anyhow::Result<u64> {
        todo!()
    }

    async fn update_meta(&self, indexer_id: &str, block_height: u64) -> anyhow::Result<()> {
        todo!()
    }

    async fn get_last_processed_block_height(&self, indexer_id: &str) -> anyhow::Result<u64> {
        todo!()
    }

    async fn add_validators(
        &self,
        epoch_id: CryptoHash,
        epoch_height: u64,
        epoch_start_height: u64,
        validators_info: &EpochValidatorInfo,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn update_epoch_end_height(
        &self,
        epoch_id: CryptoHash,
        epoch_end_block_hash: CryptoHash,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn save_block(
        &self,
        block_height: u64,
        block_hash: CryptoHash,
        chunks: Vec<(ChunkHash, ShardId, HeightIncluded)>,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn save_state_change(
        &self,
        state_change: &StateChangeWithCauseView,
        block_height: u64,
        block_hash: CryptoHash,
    ) -> anyhow::Result<()> {
        todo!()
    }
}
