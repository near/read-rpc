use crate::postgres::PostgresStorageManager;
use crate::AdditionalDatabaseOptions;
use bigdecimal::ToPrimitive;

pub(crate) struct PostgresDBManager {
    pg_pool: crate::postgres::PgAsyncPool,
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
    async fn get_block_by_hash(
        &self,
        block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<u64> {
        let block_height = crate::models::Block::get_block_height_by_hash(
            Self::get_connection(&self.pg_pool).await?,
            block_hash,
        )
        .await?;
        block_height
            .to_u64()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse `block_height` to u64"))
    }

    async fn get_block_by_chunk_hash(
        &self,
        chunk_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<readnode_primitives::BlockHeightShardId> {
        let block_height_shard_id = crate::models::Chunk::get_block_height_by_chunk_hash(
            Self::get_connection(&self.pg_pool).await?,
            chunk_hash,
        )
        .await;
        block_height_shard_id
            .map(readnode_primitives::BlockHeightShardId::try_from)
            .unwrap_or_else(|err| {
                Err(anyhow::anyhow!(
                    "Block height and shard id not found for chunk hash {}\n{:?}",
                    chunk_hash,
                    err,
                ))
            })
    }

    async fn get_state_keys_all(
        &self,
        account_id: &near_primitives::types::AccountId,
    ) -> anyhow::Result<Vec<readnode_primitives::StateKey>> {
        todo!()
    }

    async fn get_state_keys_by_prefix(
        &self,
        account_id: &near_primitives::types::AccountId,
        prefix: &[u8],
    ) -> anyhow::Result<Vec<readnode_primitives::StateKey>> {
        todo!()
    }

    async fn get_state_key_value(
        &self,
        account_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        key_data: readnode_primitives::StateKey,
    ) -> anyhow::Result<readnode_primitives::StateValue> {
        todo!()
    }

    async fn get_account(
        &self,
        account_id: &near_primitives::types::AccountId,
        request_block_height: near_primitives::types::BlockHeight,
    ) -> anyhow::Result<readnode_primitives::QueryData<near_primitives::account::Account>> {
        todo!()
    }

    async fn get_contract_code(
        &self,
        account_id: &near_primitives::types::AccountId,
        request_block_height: near_primitives::types::BlockHeight,
    ) -> anyhow::Result<readnode_primitives::QueryData<Vec<u8>>> {
        todo!()
    }

    async fn get_access_key(
        &self,
        account_id: &near_primitives::types::AccountId,
        request_block_height: near_primitives::types::BlockHeight,
        public_key: near_crypto::PublicKey,
    ) -> anyhow::Result<readnode_primitives::QueryData<near_primitives::account::AccessKey>> {
        todo!()
    }

    #[cfg(feature = "account_access_keys")]
    async fn get_account_access_keys(
        &self,
        account_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
    ) -> anyhow::Result<std::collections::HashMap<String, Vec<u8>>> {
        todo!()
    }

    async fn get_receipt_by_id(
        &self,
        receipt_id: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<readnode_primitives::ReceiptRecord> {
        todo!()
    }

    async fn get_transaction_by_hash(
        &self,
        transaction_hash: &str,
    ) -> anyhow::Result<readnode_primitives::TransactionDetails> {
        todo!()
    }

    async fn get_block_by_height_and_shard_id(
        &self,
        block_height: near_primitives::types::BlockHeight,
        shard_id: near_primitives::types::ShardId,
    ) -> anyhow::Result<readnode_primitives::BlockHeightShardId> {
        todo!()
    }
}
