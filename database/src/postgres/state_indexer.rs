use crate::primitives::{ChunkHash, HeightIncluded, ShardId};
use crate::AdditionalDatabaseOptions;
use near_indexer_primitives::CryptoHash;
use near_primitives::types::AccountId;

pub(crate) struct PostgresDBManager {
    // scylla_session: std::sync::Arc<scylla::Session>,
    //
    // add_state_changes: PreparedStatement,
    // delete_state_changes: PreparedStatement,
    //
    // add_access_key: PreparedStatement,
    // delete_access_key: PreparedStatement,
    //
    // #[cfg(feature = "account_access_keys")]
    // add_account_access_keys: PreparedStatement,
    // #[cfg(feature = "account_access_keys")]
    // get_account_access_keys: PreparedStatement,
    //
    // add_contract: PreparedStatement,
    // delete_contract: PreparedStatement,
    //
    // add_account: PreparedStatement,
    // delete_account: PreparedStatement,
    //
    // add_block: PreparedStatement,
    // add_chunk: PreparedStatement,
    // add_account_state: PreparedStatement,
    // update_meta: PreparedStatement,
    // last_processed_block_height: PreparedStatement,
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
impl crate::StateIndexerDbManager for PostgresDBManager {
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

    #[cfg(feature = "account_access_keys")]
    async fn get_access_keys(
        &self,
        account_id: AccountId,
        block_height: u64,
    ) -> anyhow::Result<std::collections::HashMap<String, Vec<u8>>> {
        todo!()
    }

    #[cfg(feature = "account_access_keys")]
    async fn add_account_access_keys(
        &self,
        account_id: AccountId,
        block_height: u64,
        public_key: &[u8],
        access_key: Option<&[u8]>,
    ) -> anyhow::Result<()> {
        todo!()
    }

    #[cfg(feature = "account_access_keys")]
    async fn update_account_access_keys(
        &self,
        account_id: String,
        block_height: u64,
        account_keys: std::collections::HashMap<String, Vec<u8>>,
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

    async fn update_meta(&self, indexer_id: &str, block_height: u64) -> anyhow::Result<()> {
        todo!()
    }

    async fn get_last_processed_block_height(&self, indexer_id: &str) -> anyhow::Result<u64> {
        todo!()
    }
}
