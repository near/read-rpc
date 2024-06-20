use bigdecimal::ToPrimitive;
use futures::StreamExt;
use std::str::FromStr;

#[async_trait::async_trait]
impl crate::ReaderDbManager for crate::PostgresDBManager {
    async fn get_block_by_hash(
        &self,
        block_hash: near_indexer_primitives::CryptoHash,
        method_name: &str,
    ) -> anyhow::Result<u64> {
        crate::metrics::META_DATABASE_READ_QUERIES
            .with_label_values(&[method_name, "blocks"])
            .inc();
        let (block_height,): (bigdecimal::BigDecimal,) = sqlx::query_as(
            "
                SELECT block_height
                FROM blocks
                WHERE block_hash = ?
                LIMIT 1
                ",
        )
        .bind(block_hash.to_string())
        .fetch_one(&self.meta_db_pool)
        .await?;
        block_height
            .to_u64()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse `block_height` to u64"))
    }

    async fn get_block_by_chunk_hash(
        &self,
        chunk_hash: near_indexer_primitives::CryptoHash,
        method_name: &str,
    ) -> anyhow::Result<readnode_primitives::BlockHeightShardId> {
        crate::metrics::META_DATABASE_READ_QUERIES
            .with_label_values(&[method_name, "chunks"])
            .inc();
        let result: (bigdecimal::BigDecimal, bigdecimal::BigDecimal) = sqlx::query_as(
            "
                SELECT block_height, shard_id
                FROM chunks
                WHERE chunk_hash = ?
                LIMIT 1
                ",
        )
        .bind(chunk_hash.to_string())
        .fetch_one(&self.meta_db_pool)
        .await?;
        Ok(readnode_primitives::BlockHeightShardId::try_from(result)?)
    }

    async fn get_state_keys_all(
        &self,
        account_id: &near_primitives::types::AccountId,
        method_name: &str,
    ) -> anyhow::Result<Vec<readnode_primitives::StateKey>> {
        todo!()
    }

    async fn get_state_keys_by_page(
        &self,
        account_id: &near_primitives::types::AccountId,
        page_token: crate::PageToken,
        method_name: &str,
    ) -> anyhow::Result<(Vec<readnode_primitives::StateKey>, crate::PageToken)> {
        todo!()
    }

    async fn get_state_keys_by_prefix(
        &self,
        account_id: &near_primitives::types::AccountId,
        prefix: &[u8],
        method_name: &str,
    ) -> anyhow::Result<Vec<readnode_primitives::StateKey>> {
        todo!()
    }

    async fn get_state_key_value(
        &self,
        account_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        key_data: readnode_primitives::StateKey,
        method_name: &str,
    ) -> anyhow::Result<(
        readnode_primitives::StateKey,
        readnode_primitives::StateValue,
    )> {
        let shard_id_pool = self.get_shard_connection(account_id).await?;
        crate::metrics::SHARD_DATABASE_READ_QUERIES
            .with_label_values(&[
                &shard_id_pool.shard_id.to_string(),
                method_name,
                "state_changes_data",
            ])
            .inc();
        let (data_value,): (Vec<u8>,) = sqlx::query_as(
            "
                SELECT data_value 
                FROM state_changes_data
                WHERE account_id = ? AND key_data = ? AND block_height <= ?
                ORDER BY block_height DESC
                LIMIT 1
                ",
        )
        .bind(account_id.to_string())
        .bind(hex::encode(&key_data).to_string())
        .bind(bigdecimal::BigDecimal::from(block_height))
        .fetch_one(shard_id_pool.pool)
        .await?;
        Ok((key_data, data_value))
    }

    async fn get_account(
        &self,
        account_id: &near_primitives::types::AccountId,
        request_block_height: near_primitives::types::BlockHeight,
        method_name: &str,
    ) -> anyhow::Result<readnode_primitives::QueryData<near_primitives::account::Account>> {
        let shard_id_pool = self.get_shard_connection(account_id).await?;
        crate::metrics::SHARD_DATABASE_READ_QUERIES
            .with_label_values(&[
                &shard_id_pool.shard_id.to_string(),
                method_name,
                "state_changes_account",
            ])
            .inc();
        let (block_height, block_hash, data_value): (bigdecimal::BigDecimal, String, Vec<u8>) =
            sqlx::query_as(
                "
                SELECT block_height, block_hash, data_value 
                FROM state_changes_account
                WHERE account_id = ? AND block_height <= ?
                ORDER BY block_height DESC
                LIMIT 1
                ",
            )
            .bind(account_id.to_string())
            .bind(bigdecimal::BigDecimal::from(request_block_height))
            .fetch_one(shard_id_pool.pool)
            .await?;
        let block = readnode_primitives::BlockRecord::try_from((block_hash, block_height))?;
        readnode_primitives::QueryData::<near_primitives::account::Account>::try_from((
            data_value,
            block.height,
            block.hash,
        ))
    }

    async fn get_contract_code(
        &self,
        account_id: &near_primitives::types::AccountId,
        request_block_height: near_primitives::types::BlockHeight,
        method_name: &str,
    ) -> anyhow::Result<readnode_primitives::QueryData<Vec<u8>>> {
        let shard_id_pool = self.get_shard_connection(account_id).await?;
        crate::metrics::SHARD_DATABASE_READ_QUERIES
            .with_label_values(&[
                &shard_id_pool.shard_id.to_string(),
                method_name,
                "state_changes_contract",
            ])
            .inc();
        let (block_height, block_hash, contract_code): (bigdecimal::BigDecimal, String, Vec<u8>) =
            sqlx::query_as(
                "
                SELECT block_height, block_hash, data_value
                FROM state_changes_contract
                WHERE account_id = ? AND block_height <= ?
                ORDER BY block_height DESC
                LIMIT 1
                ",
            )
            .bind(account_id.to_string())
            .bind(bigdecimal::BigDecimal::from(request_block_height))
            .fetch_one(shard_id_pool.pool)
            .await?;
        let block = readnode_primitives::BlockRecord::try_from((block_hash, block_height))?;
        Ok(readnode_primitives::QueryData {
            data: contract_code,
            block_height: block.height,
            block_hash: block.hash,
        })
    }

    async fn get_access_key(
        &self,
        account_id: &near_primitives::types::AccountId,
        request_block_height: near_primitives::types::BlockHeight,
        public_key: near_crypto::PublicKey,
        method_name: &str,
    ) -> anyhow::Result<readnode_primitives::QueryData<near_primitives::account::AccessKey>> {
        let shard_id_pool = self.get_shard_connection(account_id).await?;
        crate::metrics::SHARD_DATABASE_READ_QUERIES
            .with_label_values(&[
                &shard_id_pool.shard_id.to_string(),
                method_name,
                "state_changes_access_key",
            ])
            .inc();
        let key_data = borsh::to_vec(&public_key)?;
        let (block_height, block_hash, data_value): (bigdecimal::BigDecimal, String, Vec<u8>) =
            sqlx::query_as(
                "
                SELECT block_height, block_hash, data_value
                FROM state_changes_access_key
                WHERE account_id = ? AND data_key = ? AND block_height <= ?
                ORDER BY block_height DESC
                LIMIT 1
                ",
            )
            .bind(account_id.to_string())
            .bind(hex::encode(&key_data).to_string())
            .bind(bigdecimal::BigDecimal::from(request_block_height))
            .fetch_one(shard_id_pool.pool)
            .await?;
        let block = readnode_primitives::BlockRecord::try_from((block_hash, block_height))?;
        readnode_primitives::QueryData::<near_primitives::account::AccessKey>::try_from((
            data_value,
            block.height,
            block.hash,
        ))
    }

    async fn get_receipt_by_id(
        &self,
        receipt_id: near_indexer_primitives::CryptoHash,
        method_name: &str,
    ) -> anyhow::Result<readnode_primitives::ReceiptRecord> {
        let futures = self.shards_pool.iter().map(|(shard_id, pool)| {
            crate::metrics::SHARD_DATABASE_READ_QUERIES
                .with_label_values(&[
                    &shard_id.to_string(),
                    method_name,
                    "receipts_map",
                ])
                .inc();
            sqlx::query_as::<_, (String, String, String, bigdecimal::BigDecimal, String, bigdecimal::BigDecimal)>(
                "
                SELECT receipt_id, parent_transaction_hash, receiver_id, block_height, block_hash, shard_id
                FROM receipts_map
                WHERE receipt_id = ?
                LIMIT 1
                ",
            )
                .bind(receipt_id.to_string())
                .fetch_one(pool)
        });
        let mut tasks = futures::stream::FuturesUnordered::from_iter(futures);
        while let Some(result) = tasks.next().await {
            if let Ok(row) = result {
                return readnode_primitives::ReceiptRecord::try_from(row);
            }
        }
        anyhow::bail!("Receipt not found")
    }

    async fn get_transaction_by_hash(
        &self,
        transaction_hash: &str,
        method_name: &str,
    ) -> anyhow::Result<(u64, readnode_primitives::TransactionDetails)> {
        todo!()
    }

    async fn get_indexed_transaction_by_hash(
        &self,
        transaction_hash: &str,
        method_name: &str,
    ) -> anyhow::Result<(u64, readnode_primitives::TransactionDetails)> {
        todo!()
    }

    async fn get_indexing_transaction_by_hash(
        &self,
        transaction_hash: &str,
        method_name: &str,
    ) -> anyhow::Result<(u64, readnode_primitives::TransactionDetails)> {
        todo!()
    }

    async fn get_block_by_height_and_shard_id(
        &self,
        block_height: near_primitives::types::BlockHeight,
        shard_id: near_primitives::types::ShardId,
        method_name: &str,
    ) -> anyhow::Result<readnode_primitives::BlockHeightShardId> {
        crate::metrics::META_DATABASE_READ_QUERIES
            .with_label_values(&[method_name, "chunks_duplicate"])
            .inc();
        let result: (bigdecimal::BigDecimal, bigdecimal::BigDecimal) = sqlx::query_as(
            "
                SELECT included_in_block_height, shard_id
                FROM chunks_duplicate
                WHERE block_height = ? AND shard_id = ?
                LIMIT 1
                ",
        )
        .bind(bigdecimal::BigDecimal::from(block_height))
        .bind(bigdecimal::BigDecimal::from(shard_id))
        .fetch_one(&self.meta_db_pool)
        .await?;
        readnode_primitives::BlockHeightShardId::try_from(result)
    }

    async fn get_validators_by_epoch_id(
        &self,
        epoch_id: near_indexer_primitives::CryptoHash,
        method_name: &str,
    ) -> anyhow::Result<readnode_primitives::EpochValidatorsInfo> {
        crate::metrics::META_DATABASE_READ_QUERIES
            .with_label_values(&[method_name, "validators"])
            .inc();
        let (epoch_height, validators_info): (bigdecimal::BigDecimal, String) = sqlx::query_as(
            "
                SELECT epoch_height, validators_info
                FROM validators
                WHERE epoch_id = ?
                LIMIT 1
                ",
        )
        .bind(epoch_id.to_string())
        .fetch_one(&self.meta_db_pool)
        .await?;
        let validators_info: near_primitives::views::EpochValidatorInfo =
            serde_json::from_str(&validators_info)?;
        Ok(readnode_primitives::EpochValidatorsInfo {
            epoch_id,
            epoch_height: epoch_height
                .to_u64()
                .ok_or_else(|| anyhow::anyhow!("Failed to parse `epoch_height` to u64"))?,
            epoch_start_height: validators_info.epoch_start_height,
            validators_info,
        })
    }

    async fn get_validators_by_end_block_height(
        &self,
        block_height: near_primitives::types::BlockHeight,
        method_name: &str,
    ) -> anyhow::Result<readnode_primitives::EpochValidatorsInfo> {
        crate::metrics::META_DATABASE_READ_QUERIES
            .with_label_values(&[method_name, "validators"])
            .inc();
        let (epoch_id, epoch_height, validators_info): (String, bigdecimal::BigDecimal, String) =
            sqlx::query_as(
                "
                SELECT epoch_id, epoch_height, validators_info
                FROM validators
                WHERE epoch_end_height = ?
                LIMIT 1
                ",
            )
            .bind(bigdecimal::BigDecimal::from(block_height))
            .fetch_one(&self.meta_db_pool)
            .await?;
        let epoch_id = near_indexer_primitives::CryptoHash::from_str(&epoch_id)
            .map_err(|err| anyhow::anyhow!("Failed to parse `epoch_id` to CryptoHash: {}", err))?;
        let validators_info: near_primitives::views::EpochValidatorInfo =
            serde_json::from_str(&validators_info)?;
        Ok(readnode_primitives::EpochValidatorsInfo {
            epoch_id,
            epoch_height: epoch_height
                .to_u64()
                .ok_or_else(|| anyhow::anyhow!("Failed to parse `epoch_height` to u64"))?,
            epoch_start_height: validators_info.epoch_start_height,
            validators_info,
        })
    }

    async fn get_protocol_config_by_epoch_id(
        &self,
        epoch_id: near_indexer_primitives::CryptoHash,
        method_name: &str,
    ) -> anyhow::Result<near_chain_configs::ProtocolConfigView> {
        crate::metrics::META_DATABASE_READ_QUERIES
            .with_label_values(&[method_name, "protocol_configs"])
            .inc();
        let (protocol_config,): (String,) = sqlx::query_as(
            "
                SELECT protocol_config
                FROM protocol_configs
                WHERE epoch_id = ?
                LIMIT 1
                ",
        )
        .bind(epoch_id.to_string())
        .fetch_one(&self.meta_db_pool)
        .await?;
        let protocol_config: near_chain_configs::ProtocolConfigView =
            serde_json::from_str(&protocol_config)?;

        Ok(protocol_config)
    }
}
