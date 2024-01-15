use std::str::FromStr;

use crate::postgres::PostgresStorageManager;
use bigdecimal::ToPrimitive;
use borsh::{BorshDeserialize, BorshSerialize};

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
        let result = crate::models::AccountState::get_state_keys_all(
            Self::get_connection(&self.pg_pool).await?,
            account_id,
        )
        .await?
        .into_iter()
        .filter_map(|key| hex::decode(key).ok());
        Ok(result.collect())
    }

    async fn get_state_keys_by_prefix(
        &self,
        account_id: &near_primitives::types::AccountId,
        prefix: &[u8],
    ) -> anyhow::Result<Vec<readnode_primitives::StateKey>> {
        let hex_str_prefix = hex::encode(prefix);
        let result = crate::models::AccountState::get_state_keys_by_prefix(
            Self::get_connection(&self.pg_pool).await?,
            account_id,
            hex_str_prefix,
        )
        .await?
        .into_iter()
        .filter_map(|key| hex::decode(key).ok());
        Ok(result.collect())
    }

    async fn get_state_keys_by_page(
        &self,
        account_id: &near_primitives::types::AccountId,
        page_token: crate::PageToken,
    ) -> anyhow::Result<(Vec<readnode_primitives::StateKey>, crate::PageToken)> {
        let (state_keys, next_page_token) = crate::models::AccountState::get_state_keys_by_page(
            Self::get_connection(&self.pg_pool).await?,
            account_id,
            page_token,
        )
        .await?;

        let keys = state_keys
            .into_iter()
            .filter_map(|key| hex::decode(key).ok());
        Ok((keys.collect(), next_page_token))
    }

    async fn get_state_key_value(
        &self,
        account_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        key_data: readnode_primitives::StateKey,
    ) -> (
        readnode_primitives::StateKey,
        readnode_primitives::StateValue,
    ) {
        let connection = if let Ok(pg_connection) = Self::get_connection(&self.pg_pool).await {
            pg_connection
        } else {
            return (key_data, readnode_primitives::StateValue::default());
        };
        let result = if let Ok(result) = crate::models::StateChangesData::get_state_key_value(
            connection,
            account_id,
            block_height,
            hex::encode(key_data.clone()),
        )
        .await
        {
            result.unwrap_or_default()
        } else {
            readnode_primitives::StateValue::default()
        };
        (key_data, result)
    }

    async fn get_account(
        &self,
        account_id: &near_primitives::types::AccountId,
        request_block_height: near_primitives::types::BlockHeight,
    ) -> anyhow::Result<readnode_primitives::QueryData<near_primitives::account::Account>> {
        let account_data = crate::models::StateChangesAccount::get_account(
            Self::get_connection(&self.pg_pool).await?,
            account_id,
            request_block_height,
        )
        .await?;
        if let Some(data_value) = account_data.data_value {
            let block = readnode_primitives::BlockRecord::try_from((
                account_data.block_hash,
                account_data.block_height,
            ))?;
            readnode_primitives::QueryData::<near_primitives::account::Account>::try_from((
                data_value,
                block.height,
                block.hash,
            ))
        } else {
            anyhow::bail!(
                "Account `{}`not found! Block {}",
                account_id,
                request_block_height
            )
        }
    }

    async fn get_contract_code(
        &self,
        account_id: &near_primitives::types::AccountId,
        request_block_height: near_primitives::types::BlockHeight,
    ) -> anyhow::Result<readnode_primitives::QueryData<Vec<u8>>> {
        let contract_data = crate::models::StateChangesContract::get_contract(
            Self::get_connection(&self.pg_pool).await?,
            account_id,
            request_block_height,
        )
        .await?;
        if let Some(data_value) = contract_data.data_value {
            let block = readnode_primitives::BlockRecord::try_from((
                contract_data.block_hash,
                contract_data.block_height,
            ))?;
            Ok(readnode_primitives::QueryData {
                data: data_value,
                block_height: block.height,
                block_hash: block.hash,
            })
        } else {
            anyhow::bail!(
                "Contract code `{}`not found! Block {}",
                account_id,
                request_block_height
            )
        }
    }

    async fn get_access_key(
        &self,
        account_id: &near_primitives::types::AccountId,
        request_block_height: near_primitives::types::BlockHeight,
        public_key: near_crypto::PublicKey,
    ) -> anyhow::Result<readnode_primitives::QueryData<near_primitives::account::AccessKey>> {
        let key_data = public_key.try_to_vec()?;
        let access_key_data = crate::models::StateChangesAccessKey::get_access_key(
            Self::get_connection(&self.pg_pool).await?,
            account_id,
            request_block_height,
            hex::encode(key_data),
        )
        .await?;

        if let Some(data_value) = access_key_data.data_value {
            let block = readnode_primitives::BlockRecord::try_from((
                access_key_data.block_hash,
                access_key_data.block_height,
            ))?;
            readnode_primitives::QueryData::<near_primitives::account::AccessKey>::try_from((
                data_value,
                block.height,
                block.hash,
            ))
        } else {
            anyhow::bail!(
                "Access key `{}`not found! Block {}",
                account_id,
                request_block_height
            )
        }
    }

    #[cfg(feature = "account_access_keys")]
    async fn get_account_access_keys(
        &self,
        account_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
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

    async fn get_receipt_by_id(
        &self,
        receipt_id: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<readnode_primitives::ReceiptRecord> {
        let receipt = crate::models::ReceiptMap::get_receipt_by_id(
            Self::get_connection(&self.pg_pool).await?,
            receipt_id,
        )
        .await?;
        readnode_primitives::ReceiptRecord::try_from((
            receipt.receipt_id,
            receipt.parent_transaction_hash,
            receipt.block_height,
            receipt.shard_id,
        ))
    }

    async fn get_transaction_by_hash(
        &self,
        transaction_hash: &str,
    ) -> anyhow::Result<readnode_primitives::TransactionDetails> {
        let transaction_data = crate::models::TransactionDetail::get_transaction_by_hash(
            Self::get_connection(&self.pg_pool).await?,
            transaction_hash,
        )
        .await?;
        Ok(readnode_primitives::TransactionDetails::try_from_slice(
            &transaction_data,
        )?)
    }

    async fn get_block_by_height_and_shard_id(
        &self,
        block_height: near_primitives::types::BlockHeight,
        shard_id: near_primitives::types::ShardId,
    ) -> anyhow::Result<readnode_primitives::BlockHeightShardId> {
        let block_height_shard_id = crate::models::Chunk::get_stored_block_height(
            Self::get_connection(&self.pg_pool).await?,
            block_height,
            shard_id,
        )
        .await;
        block_height_shard_id
            .map(readnode_primitives::BlockHeightShardId::try_from)
            .unwrap_or_else(|err| {
                Err(anyhow::anyhow!(
                    "Block height and shard id not found for block height {} and shard id {}\n{:?}",
                    block_height,
                    shard_id,
                    err,
                ))
            })
    }

    async fn get_validators_by_epoch_id(
        &self,
        epoch_id: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<readnode_primitives::EpochValidatorsInfo> {
        let epoch = crate::models::Validators::get_validators(
            Self::get_connection(&self.pg_pool).await?,
            epoch_id,
        )
        .await?;
        let epoch_height = epoch
            .epoch_height
            .to_u64()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse `epoch_height` to u64"))?;
        let epoch_start_height = epoch
            .epoch_start_height
            .to_u64()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse `epoch_start_height` to u64"))?;
        let (validators_info,) = serde_json::from_value::<(
            near_indexer_primitives::views::EpochValidatorInfo,
        )>(epoch.validators_info)?;
        Ok(readnode_primitives::EpochValidatorsInfo {
            epoch_id,
            epoch_height,
            epoch_start_height,
            validators_info,
        })
    }

    async fn get_validators_by_end_block_height(
        &self,
        block_height: near_primitives::types::BlockHeight,
    ) -> anyhow::Result<readnode_primitives::EpochValidatorsInfo> {
        let epoch = crate::models::Validators::get_validators_epoch_end_height(
            Self::get_connection(&self.pg_pool).await?,
            bigdecimal::BigDecimal::from(block_height),
        )
        .await?;
        let epoch_id = near_indexer_primitives::CryptoHash::from_str(&epoch.epoch_id)
            .map_err(|err| anyhow::anyhow!("Failed to parse `epoch_id` to CryptoHash: {}", err))?;
        let epoch_height = epoch
            .epoch_height
            .to_u64()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse `epoch_height` to u64"))?;
        let epoch_start_height = epoch
            .epoch_start_height
            .to_u64()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse `epoch_start_height` to u64"))?;
        let (validators_info,) = serde_json::from_value::<(
            near_indexer_primitives::views::EpochValidatorInfo,
        )>(epoch.validators_info)?;
        Ok(readnode_primitives::EpochValidatorsInfo {
            epoch_id,
            epoch_height,
            epoch_start_height,
            validators_info,
        })
    }

    async fn get_protocol_config_by_epoch_id(
        &self,
        epoch_id: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<near_chain_configs::ProtocolConfigView> {
        let protocol_config = crate::models::ProtocolConfig::get_protocol_config(
            Self::get_connection(&self.pg_pool).await?,
            epoch_id,
        )
        .await?;
        let (protocol_config,) = serde_json::from_value::<(near_chain_configs::ProtocolConfigView,)>(
            protocol_config.protocol_config,
        )?;
        Ok(protocol_config)
    }
}
