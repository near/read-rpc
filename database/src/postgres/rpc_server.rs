use std::str::FromStr;

use bigdecimal::ToPrimitive;
use futures::StreamExt;

#[async_trait::async_trait]
impl crate::ReaderDbManager for crate::PostgresDBManager {
    async fn get_block_height_by_hash(
        &self,
        block_hash: near_primitives::hash::CryptoHash,
        method_name: &str,
    ) -> anyhow::Result<u64> {
        crate::metrics::META_DATABASE_READ_QUERIES
            .with_label_values(&[method_name, "blocks"])
            .inc();
        let (block_height,): (bigdecimal::BigDecimal,) = sqlx::query_as(
            "
                SELECT block_height
                FROM blocks
                WHERE block_hash = $1
                LIMIT 1;
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
        chunk_hash: near_primitives::hash::CryptoHash,
        method_name: &str,
    ) -> anyhow::Result<readnode_primitives::BlockHeightShardId> {
        crate::metrics::META_DATABASE_READ_QUERIES
            .with_label_values(&[method_name, "chunks"])
            .inc();
        let result: (bigdecimal::BigDecimal, bigdecimal::BigDecimal) = sqlx::query_as(
            "
                SELECT block_height, shard_id
                FROM chunks
                WHERE chunk_hash = $1
                LIMIT 1;
                ",
        )
        .bind(chunk_hash.to_string())
        .fetch_one(&self.meta_db_pool)
        .await?;
        Ok(readnode_primitives::BlockHeightShardId::try_from(result)?)
    }

    async fn get_state_by_page(
        &self,
        account_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        page_token: crate::PageToken,
        method_name: &str,
    ) -> anyhow::Result<(
        std::collections::HashMap<readnode_primitives::StateKey, readnode_primitives::StateValue>,
        crate::PageToken,
    )> {
        let shard_id_pool = self.get_shard_connection(account_id).await?;
        crate::metrics::SHARD_DATABASE_READ_QUERIES
            .with_label_values(&[
                &shard_id_pool.shard_id.to_string(),
                method_name,
                "state_changes_data",
            ])
            .inc();
        let page_state = if let Some(page_state_token) = page_token {
            borsh::from_slice::<crate::postgres::PageState>(&hex::decode(page_state_token)?)?
        } else {
            crate::postgres::PageState::new(1000)
        };
        let mut stream = sqlx::query_as::<_, (String, Vec<u8>)>(
            "
                WITH latest_blocks AS (
                    SELECT 
                        data_key,
                        MAX(block_height) AS max_block_height
                    FROM 
                        state_changes_data
                    WHERE 
                        account_id = $1
                        AND block_height <= $2
                    GROUP BY 
                        data_key
                )
                SELECT 
                    sc.data_key,
                    sc.data_value
                FROM
                    state_changes_data sc
                INNER JOIN latest_blocks lb
                ON 
                    sc.data_key = lb.data_key 
                    AND sc.block_height = lb.max_block_height
                WHERE
                    sc.account_id = $1
                    AND sc.data_value IS NOT NULL
                ORDER BY 
                    sc.data_key
                LIMIT $3 OFFSET $4;
                ",
        )
        .bind(account_id.to_string())
        .bind(bigdecimal::BigDecimal::from(block_height))
        .bind(page_state.page_size)
        .bind(page_state.offset)
        .fetch(shard_id_pool.pool);
        let mut items = std::collections::HashMap::new();
        while let Some(row) = stream.next().await {
            let (key, value): (String, Vec<u8>) = row?;
            items.insert(hex::decode(key)?, value);
        }
        if items.len() < page_state.page_size as usize {
            Ok((items, None))
        } else {
            Ok((
                items,
                Some(hex::encode(borsh::to_vec(&page_state.next_page())?)),
            ))
        }
    }

    async fn get_state_by_key_prefix(
        &self,
        account_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        prefix: &[u8],
        method_name: &str,
    ) -> anyhow::Result<
        std::collections::HashMap<readnode_primitives::StateKey, readnode_primitives::StateValue>,
    > {
        let shard_id_pool = self.get_shard_connection(account_id).await?;
        crate::metrics::SHARD_DATABASE_READ_QUERIES
            .with_label_values(&[
                &shard_id_pool.shard_id.to_string(),
                method_name,
                "state_changes_data",
            ])
            .inc();
        let mut items = std::collections::HashMap::new();
        let mut stream = sqlx::query_as::<_, (String, Vec<u8>)>(
            "
                WITH latest_blocks AS (
                    SELECT 
                        data_key,
                        MAX(block_height) AS max_block_height
                    FROM 
                        state_changes_data
                    WHERE 
                        account_id = $1
                        AND data_key LIKE $2
                        AND block_height <= $3
                    GROUP BY 
                        data_key
                )
                SELECT 
                    sc.data_key,
                    sc.data_value
                FROM
                    state_changes_data sc
                INNER JOIN latest_blocks lb
                ON 
                    sc.data_key = lb.data_key 
                    AND sc.block_height = lb.max_block_height
                WHERE
                    sc.account_id = $1
                    AND sc.data_value IS NOT NULL;
                ",
        )
        .bind(account_id.to_string())
        .bind(format!("{}%", hex::encode(prefix)))
        .bind(bigdecimal::BigDecimal::from(block_height))
        .fetch(shard_id_pool.pool);
        while let Some(row) = stream.next().await {
            let (key, value): (String, Vec<u8>) = row?;
            items.insert(hex::decode(key)?, value);
        }
        Ok(items)
    }

    async fn get_state(
        &self,
        account_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        method_name: &str,
    ) -> anyhow::Result<
        std::collections::HashMap<readnode_primitives::StateKey, readnode_primitives::StateValue>,
    > {
        let shard_id_pool = self.get_shard_connection(account_id).await?;
        crate::metrics::SHARD_DATABASE_READ_QUERIES
            .with_label_values(&[
                &shard_id_pool.shard_id.to_string(),
                method_name,
                "state_changes_data",
            ])
            .inc();
        let mut items = std::collections::HashMap::new();
        let mut stream = sqlx::query_as::<_, (String, Vec<u8>)>(
            "
                WITH latest_blocks AS (
                    SELECT 
                        data_key,
                        MAX(block_height) AS max_block_height
                    FROM 
                        state_changes_data
                    WHERE 
                        account_id = $1
                        AND block_height <= $2
                    GROUP BY 
                        data_key
                )
                SELECT 
                    sc.data_key,
                    sc.data_value
                FROM
                    state_changes_data sc
                INNER JOIN latest_blocks lb
                ON 
                    sc.data_key = lb.data_key 
                    AND sc.block_height = lb.max_block_height
                WHERE
                    sc.account_id = $1
                    AND sc.data_value IS NOT NULL;
                ",
        )
        .bind(account_id.to_string())
        .bind(bigdecimal::BigDecimal::from(block_height))
        .fetch(shard_id_pool.pool);
        while let Some(row) = stream.next().await {
            let (key, value): (String, Vec<u8>) = row?;
            items.insert(hex::decode(key)?, value);
        }
        Ok(items)
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
                WHERE account_id = $1 
                    AND data_key = $2 
                    AND block_height <= $3
                ORDER BY block_height DESC
                LIMIT 1;
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
                WHERE account_id = $1 
                    AND block_height <= $2
                ORDER BY block_height DESC
                LIMIT 1;
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
                WHERE account_id = $1 
                    AND block_height <= $2
                ORDER BY block_height DESC
                LIMIT 1;
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
                WHERE account_id = $1 
                    AND data_key = $2 
                    AND block_height <= $3
                ORDER BY block_height DESC
                LIMIT 1;
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

    async fn get_account_access_keys(
        &self,
        account_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        method_name: &str,
    ) -> anyhow::Result<Vec<near_primitives::views::AccessKeyInfoView>> {
        let shard_id_pool = self.get_shard_connection(account_id).await?;
        crate::metrics::SHARD_DATABASE_READ_QUERIES
            .with_label_values(&[
                &shard_id_pool.shard_id.to_string(),
                method_name,
                "state_changes_access_key",
            ])
            .inc();
        let mut access_keys = vec![];
        let mut stream = sqlx::query_as::<_, (String, Vec<u8>, bigdecimal::BigDecimal)>(
            "
                WITH latest_blocks AS (
                    SELECT 
                        data_key,
                        account_id,
                        MAX(block_height) as max_block_height
                    FROM 
                        state_changes_access_key
                    WHERE 
                        account_id = $1
                        AND block_height <= $2
                    GROUP BY 
                        data_key,
                        account_id
                )
                SELECT 
                    sc.data_key,
                    sc.data_value,
                    sc.block_height 
                FROM
                    state_changes_access_key sc
                INNER JOIN latest_blocks lb
                ON 
                    sc.data_key = lb.data_key 
                    AND sc.block_height = lb.max_block_height
                    AND sc.account_id = lb.account_id
                WHERE
                    sc.data_value IS NOT NULL;
                ",
        )
        .bind(account_id.to_string())
        .bind(bigdecimal::BigDecimal::from(block_height))
        .fetch(shard_id_pool.pool);
        while let Some(row) = stream.next().await {
            let (public_key_hex, access_key, _): (String, Vec<u8>, _) = row?;
            let access_key_view = near_primitives::views::AccessKeyInfoView {
                public_key: borsh::from_slice::<near_crypto::PublicKey>(&hex::decode(
                    public_key_hex,
                )?)?,
                access_key: near_primitives::views::AccessKeyView::from(borsh::from_slice::<
                    near_primitives::account::AccessKey,
                >(
                    &access_key
                )?),
            };
            access_keys.push(access_key_view);
        }
        Ok(access_keys)
    }

    async fn get_receipt_by_id(
        &self,
        receipt_id: near_primitives::hash::CryptoHash,
        method_name: &str,
    ) -> anyhow::Result<readnode_primitives::ReceiptRecord> {
        // We need to query all shards because we don't know which shard the receipt is stored in
        // and we need to return the receipt as soon as we find it.
        // Query all shards in parallel and then we wait for the first result.
        let futures = self.shards_pool.iter().map(|(shard_id, pool)| {
            crate::metrics::SHARD_DATABASE_READ_QUERIES
                .with_label_values(&[&shard_id.to_string(), method_name, "receipts_map"])
                .inc();
            sqlx::query_as::<
                _,
                (
                    String,
                    String,
                    String,
                    bigdecimal::BigDecimal,
                    String,
                    bigdecimal::BigDecimal,
                ),
            >(
                "
                SELECT receipt_id, 
                    parent_transaction_hash, 
                    receiver_id, 
                    block_height, 
                    block_hash, 
                    shard_id
                FROM receipts_map
                WHERE receipt_id = $1
                LIMIT 1;
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
                WHERE block_height = $1 
                    AND shard_id = $2
                LIMIT 1;
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
        epoch_id: near_primitives::hash::CryptoHash,
        method_name: &str,
    ) -> anyhow::Result<readnode_primitives::EpochValidatorsInfo> {
        crate::metrics::META_DATABASE_READ_QUERIES
            .with_label_values(&[method_name, "validators"])
            .inc();
        let (epoch_height, validators_info): (bigdecimal::BigDecimal, serde_json::Value) =
            sqlx::query_as(
                "
                SELECT epoch_height, validators_info
                FROM validators
                WHERE epoch_id = $1
                LIMIT 1;
                ",
            )
            .bind(epoch_id.to_string())
            .fetch_one(&self.meta_db_pool)
            .await?;
        let validators_info: near_primitives::views::EpochValidatorInfo =
            serde_json::from_value(validators_info)?;
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
        let (epoch_id, epoch_height, validators_info): (
            String,
            bigdecimal::BigDecimal,
            serde_json::Value,
        ) = sqlx::query_as(
            "
                SELECT epoch_id, epoch_height, validators_info
                FROM validators
                WHERE epoch_end_height = $1
                LIMIT 1;
                ",
        )
        .bind(bigdecimal::BigDecimal::from(block_height))
        .fetch_one(&self.meta_db_pool)
        .await?;
        let epoch_id = near_primitives::hash::CryptoHash::from_str(&epoch_id)
            .map_err(|err| anyhow::anyhow!("Failed to parse `epoch_id` to CryptoHash: {}", err))?;
        let validators_info: near_primitives::views::EpochValidatorInfo =
            serde_json::from_value(validators_info)?;
        Ok(readnode_primitives::EpochValidatorsInfo {
            epoch_id,
            epoch_height: epoch_height
                .to_u64()
                .ok_or_else(|| anyhow::anyhow!("Failed to parse `epoch_height` to u64"))?,
            epoch_start_height: validators_info.epoch_start_height,
            validators_info,
        })
    }
}
