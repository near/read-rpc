use crate::postgres::PostgresStorageManager;
use bigdecimal::ToPrimitive;

pub struct PostgresDBManager {
    pg_pool: crate::postgres::PgAsyncPool,
}

#[async_trait::async_trait]
impl crate::BaseDbManager for PostgresDBManager {
    async fn new(config: &configuration::DatabaseConfig) -> anyhow::Result<Box<Self>> {
        let pg_pool = Self::create_pool(
            &config.database_url,
            config.database_user.as_deref(),
            config.database_password.as_deref(),
            config.database_name.as_deref(),
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
        .insert_or_ignore(Self::get_connection(&self.pg_pool).await?)
        .await?;
        crate::models::AccountState {
            account_id: account_id.to_string(),
            data_key: hex::encode(key).to_string(),
        }
        .insert_or_ignore(Self::get_connection(&self.pg_pool).await?)
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
        .insert_or_ignore(Self::get_connection(&self.pg_pool).await?)
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
        .insert_or_ignore(Self::get_connection(&self.pg_pool).await?)
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
        .insert_or_ignore(Self::get_connection(&self.pg_pool).await?)
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
            account_id.as_str(),
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
        .insert_or_ignore(Self::get_connection(&self.pg_pool).await?)
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
        .insert_or_ignore(Self::get_connection(&self.pg_pool).await?)
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
        .insert_or_ignore(Self::get_connection(&self.pg_pool).await?)
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
        .insert_or_ignore(Self::get_connection(&self.pg_pool).await?)
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
        .insert_or_ignore(Self::get_connection(&self.pg_pool).await?)
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
        .insert_or_ignore(Self::get_connection(&self.pg_pool).await?)
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

    async fn update_meta(&self, indexer_id: &str, block_height: u64) -> anyhow::Result<()> {
        crate::models::Meta {
            indexer_id: indexer_id.to_string(),
            last_processed_block_height: bigdecimal::BigDecimal::from(block_height),
        }
        .insert_or_update(Self::get_connection(&self.pg_pool).await?)
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
    async fn add_validators(
        &self,
        epoch_id: near_indexer_primitives::CryptoHash,
        epoch_height: u64,
        epoch_start_height: u64,
        validators_info: &near_primitives::views::EpochValidatorInfo,
    ) -> anyhow::Result<()> {
        crate::models::Validators {
            epoch_id: epoch_id.to_string(),
            epoch_height: bigdecimal::BigDecimal::from(epoch_height),
            epoch_start_height: bigdecimal::BigDecimal::from(epoch_start_height),
            epoch_end_height: None,
            validators_info: serde_json::to_value(validators_info)?,
        }
        .insert_or_ignore(Self::get_connection(&self.pg_pool).await?)
        .await?;
        Ok(())
    }

    async fn update_epoch_end_height(
        &self,
        epoch_id: near_indexer_primitives::CryptoHash,
        epoch_end_block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()> {
        let epoch_end_height = self.get_block_by_hash(epoch_end_block_hash).await?;
        crate::models::Validators::update_epoch_end_height(
            Self::get_connection(&self.pg_pool).await?,
            epoch_id,
            bigdecimal::BigDecimal::from(epoch_end_height),
        )
        .await?;
        Ok(())
    }
}
