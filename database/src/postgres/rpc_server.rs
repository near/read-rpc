use crate::postgres::PostgresStorageManager;
use crate::AdditionalDatabaseOptions;
use near_crypto::PublicKey;
use near_indexer_primitives::CryptoHash;
use near_primitives::account::{AccessKey, Account};
use near_primitives::types::{AccountId, BlockHeight, ShardId};
use readnode_primitives::{
    BlockHeightShardId, QueryData, ReceiptRecord, StateKey, StateValue, TransactionDetails,
};

pub(crate) struct PostgresDBManager {
    pg_pool: diesel_async::pooled_connection::deadpool::Pool<diesel_async::AsyncPgConnection>,
    // get_block_by_hash: PreparedStatement,
    // get_block_by_chunk_id: PreparedStatement,
    // get_all_state_keys: PreparedStatement,
    // get_state_keys_by_prefix: PreparedStatement,
    // get_state_key_value: PreparedStatement,
    // get_account: PreparedStatement,
    // get_contract_code: PreparedStatement,
    // get_access_key: PreparedStatement,
    // #[cfg(feature = "account_access_keys")]
    // get_account_access_keys: PreparedStatement,
    // get_receipt: PreparedStatement,
    // get_transaction_by_hash: PreparedStatement,
    // get_stored_at_block_height_and_shard_id_by_block_height: PreparedStatement,
}

#[async_trait::async_trait]
impl crate::BaseDbManager for PostgresDBManager {
    async fn new(
        database_url: &str,
        database_user: Option<&str>,
        database_password: Option<&str>,
        database_options: AdditionalDatabaseOptions,
    ) -> anyhow::Result<Box<Self>> {
        let pg_pool = Self::create_pool(
            database_url,
            database_user,
            database_password,
            database_options,
        )
        .await?;
        Ok(Box::new(Self { pg_pool }))
    }
}

#[async_trait::async_trait]
impl PostgresStorageManager for PostgresDBManager {}

#[async_trait::async_trait]
impl crate::ReaderDbManager for PostgresDBManager {
    async fn get_block_by_hash(&self, block_hash: CryptoHash) -> anyhow::Result<u64> {
        todo!()
    }

    async fn get_block_by_chunk_hash(
        &self,
        chunk_hash: CryptoHash,
    ) -> anyhow::Result<BlockHeightShardId> {
        todo!()
    }

    async fn get_state_keys_all(&self, account_id: &AccountId) -> anyhow::Result<Vec<StateKey>> {
        todo!()
    }

    async fn get_state_keys_by_prefix(
        &self,
        account_id: &AccountId,
        prefix: &[u8],
    ) -> anyhow::Result<Vec<StateKey>> {
        todo!()
    }

    async fn get_state_key_value(
        &self,
        account_id: &AccountId,
        block_height: BlockHeight,
        key_data: StateKey,
    ) -> anyhow::Result<StateValue> {
        todo!()
    }

    async fn get_account(
        &self,
        account_id: &AccountId,
        request_block_height: BlockHeight,
    ) -> anyhow::Result<QueryData<Account>> {
        todo!()
    }

    async fn get_contract_code(
        &self,
        account_id: &AccountId,
        request_block_height: BlockHeight,
    ) -> anyhow::Result<QueryData<Vec<u8>>> {
        todo!()
    }

    async fn get_access_key(
        &self,
        account_id: &AccountId,
        request_block_height: BlockHeight,
        public_key: PublicKey,
    ) -> anyhow::Result<QueryData<AccessKey>> {
        todo!()
    }

    #[cfg(feature = "account_access_keys")]
    async fn get_account_access_keys(
        &self,
        account_id: &AccountId,
        block_height: BlockHeight,
    ) -> anyhow::Result<std::collections::HashMap<String, Vec<u8>>> {
        todo!()
    }

    async fn get_receipt_by_id(&self, receipt_id: CryptoHash) -> anyhow::Result<ReceiptRecord> {
        todo!()
    }

    async fn get_transaction_by_hash(
        &self,
        transaction_hash: &str,
    ) -> anyhow::Result<TransactionDetails> {
        todo!()
    }

    async fn get_block_by_height_and_shard_id(
        &self,
        block_height: BlockHeight,
        shard_id: ShardId,
    ) -> anyhow::Result<BlockHeightShardId> {
        todo!()
    }
}
