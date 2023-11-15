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
impl crate::StateIndexerDbManager for PostgresDBManager {
    async fn add_state_changes(
        &self,
        account_id: near_primitives::types::AccountId,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
        key: &[u8],
        value: &[u8],
    ) -> anyhow::Result<()> {
        crate::models::StateChangesData {
            account_id: account_id.to_string(),
            block_height: bigdecimal::BigDecimal::from(block_height),
            block_hash: block_hash.to_string(),
            data_key: hex::encode(key).to_string(),
            data_value: Some(value.to_vec()),
        }
        .save(Self::get_connection(&self.pg_pool).await?)
        .await?;
        crate::models::AccountState {
            account_id: account_id.to_string(),
            data_key: hex::encode(key).to_string(),
        }
        .save(Self::get_connection(&self.pg_pool).await?)
        .await
    }

    async fn delete_state_changes(
        &self,
        account_id: near_primitives::types::AccountId,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
        key: &[u8],
    ) -> anyhow::Result<()> {
        crate::models::StateChangesData {
            account_id: account_id.to_string(),
            block_height: bigdecimal::BigDecimal::from(block_height),
            block_hash: block_hash.to_string(),
            data_key: hex::encode(key).to_string(),
            data_value: None,
        }
        .save(Self::get_connection(&self.pg_pool).await?)
        .await
    }

    async fn add_access_key(
        &self,
        account_id: near_primitives::types::AccountId,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
        public_key: &[u8],
        access_key: &[u8],
    ) -> anyhow::Result<()> {
        crate::models::StateChangesAccessKey {
            account_id: account_id.to_string(),
            block_height: bigdecimal::BigDecimal::from(block_height),
            block_hash: block_hash.to_string(),
            data_key: hex::encode(public_key).to_string(),
            data_value: Some(access_key.to_vec()),
        }
        .save(Self::get_connection(&self.pg_pool).await?)
        .await
    }

    async fn delete_access_key(
        &self,
        account_id: near_primitives::types::AccountId,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
        public_key: &[u8],
    ) -> anyhow::Result<()> {
        crate::models::StateChangesAccessKey {
            account_id: account_id.to_string(),
            block_height: bigdecimal::BigDecimal::from(block_height),
            block_hash: block_hash.to_string(),
            data_key: hex::encode(public_key).to_string(),
            data_value: None,
        }
        .save(Self::get_connection(&self.pg_pool).await?)
        .await
    }

    #[cfg(feature = "account_access_keys")]
    async fn get_access_keys(
        &self,
        account_id: near_primitives::types::AccountId,
        block_height: u64,
    ) -> anyhow::Result<std::collections::HashMap<String, Vec<u8>>> {
        let active_access_keys = crate::models::StateChangesAccessKeys::get_active_access_keys(
            Self::get_connection(&self.pg_pool).await?,
            &account_id,
            block_height,
        )
        .await?;

        if let Some(active_access_keys_value) = active_access_keys {
            let active_access_keys: std::collections::HashMap<String, Vec<u8>> =
                serde_json::from_value(active_access_keys_value)?;
            Ok(active_access_keys)
        } else {
            Ok(std::collections::HashMap::new())
        }
    }

    #[cfg(feature = "account_access_keys")]
    async fn add_account_access_keys(
        &self,
        account_id: near_primitives::types::AccountId,
        block_height: u64,
        public_key: &[u8],
        access_key: Option<&[u8]>,
    ) -> anyhow::Result<()> {
        let public_key_hex = hex::encode(public_key).to_string();

        let mut account_keys = match self.get_access_keys(account_id.clone(), block_height).await {
            Ok(account_keys) => account_keys,
            Err(_) => std::collections::HashMap::new(),
        };
        match access_key {
            Some(access_key) => {
                account_keys.insert(public_key_hex, access_key.to_vec());
            }
            None => {
                account_keys.remove(&public_key_hex);
            }
        }
        self.update_account_access_keys(account_id.to_string(), block_height, account_keys)
            .await?;
        Ok(())
    }

    #[cfg(feature = "account_access_keys")]
    async fn update_account_access_keys(
        &self,
        account_id: String,
        block_height: u64,
        account_keys: std::collections::HashMap<String, Vec<u8>>,
    ) -> anyhow::Result<()> {
        crate::models::StateChangesAccessKeys {
            account_id: account_id.to_string(),
            block_height: bigdecimal::BigDecimal::from(block_height),
            block_hash: "".to_string(),
            active_access_keys: Some(serde_json::to_value(account_keys)?),
        }
        .save(Self::get_connection(&self.pg_pool).await?)
        .await
    }

    async fn add_contract_code(
        &self,
        account_id: near_primitives::types::AccountId,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
        code: &[u8],
    ) -> anyhow::Result<()> {
        crate::models::StateChangesContract {
            account_id: account_id.to_string(),
            block_height: bigdecimal::BigDecimal::from(block_height),
            block_hash: block_hash.to_string(),
            data_value: Some(code.to_vec()),
        }
        .save(Self::get_connection(&self.pg_pool).await?)
        .await
    }

    async fn delete_contract_code(
        &self,
        account_id: near_primitives::types::AccountId,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()> {
        crate::models::StateChangesContract {
            account_id: account_id.to_string(),
            block_height: bigdecimal::BigDecimal::from(block_height),
            block_hash: block_hash.to_string(),
            data_value: None,
        }
        .save(Self::get_connection(&self.pg_pool).await?)
        .await
    }

    async fn add_account(
        &self,
        account_id: near_primitives::types::AccountId,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
        account: Vec<u8>,
    ) -> anyhow::Result<()> {
        crate::models::StateChangesAccount {
            account_id: account_id.to_string(),
            block_height: bigdecimal::BigDecimal::from(block_height),
            block_hash: block_hash.to_string(),
            data_value: Some(account),
        }
        .save(Self::get_connection(&self.pg_pool).await?)
        .await
    }

    async fn delete_account(
        &self,
        account_id: near_primitives::types::AccountId,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()> {
        crate::models::StateChangesAccount {
            account_id: account_id.to_string(),
            block_height: bigdecimal::BigDecimal::from(block_height),
            block_hash: block_hash.to_string(),
            data_value: None,
        }
        .save(Self::get_connection(&self.pg_pool).await?)
        .await
    }

    async fn add_block(
        &self,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()> {
        crate::models::Block {
            block_height: bigdecimal::BigDecimal::from(block_height),
            block_hash: block_hash.to_string(),
        }
        .save(Self::get_connection(&self.pg_pool).await?)
        .await
    }

    async fn add_chunks(
        &self,
        block_height: u64,
        chunks: Vec<(
            crate::primitives::ChunkHash,
            crate::primitives::ShardId,
            crate::primitives::HeightIncluded,
        )>,
    ) -> anyhow::Result<()> {
        let new_chunks = chunks
            .iter()
            .map(
                |(chunk_hash, shard_id, height_included)| crate::models::Chunk {
                    chunk_hash: chunk_hash.to_string(),
                    block_height: bigdecimal::BigDecimal::from(block_height),
                    shard_id: bigdecimal::BigDecimal::from(*shard_id),
                    stored_at_block_height: bigdecimal::BigDecimal::from(*height_included),
                },
            )
            .collect();
        crate::models::Chunk::bulk_insert(new_chunks, Self::get_connection(&self.pg_pool).await?)
            .await
    }

    async fn update_meta(&self, indexer_id: &str, block_height: u64) -> anyhow::Result<()> {
        crate::models::Meta {
            indexer_id: indexer_id.to_string(),
            last_processed_block_height: bigdecimal::BigDecimal::from(block_height),
        }
        .save(Self::get_connection(&self.pg_pool).await?)
        .await
    }

    async fn get_last_processed_block_height(&self, indexer_id: &str) -> anyhow::Result<u64> {
        let block_height = crate::models::Meta::get_last_processed_block_height(
            Self::get_connection(&self.pg_pool).await?,
            indexer_id,
        )
        .await?;
        block_height
            .to_u64()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse `block_height` to u64"))
    }
}
