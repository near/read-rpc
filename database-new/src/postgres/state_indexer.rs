use bigdecimal::ToPrimitive;

#[async_trait::async_trait]
impl crate::StateIndexerDbManager for crate::PostgresDBManager {
    async fn add_state_changes(
        &self,
        account_id: near_primitives::types::AccountId,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
        key: &[u8],
        value: &[u8],
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn delete_state_changes(
        &self,
        account_id: near_primitives::types::AccountId,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
        key: &[u8],
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn add_access_key(
        &self,
        account_id: near_primitives::types::AccountId,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
        public_key: &[u8],
        access_key: &[u8],
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn delete_access_key(
        &self,
        account_id: near_primitives::types::AccountId,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
        public_key: &[u8],
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn get_access_keys(
        &self,
        account_id: near_primitives::types::AccountId,
        block_height: u64,
    ) -> anyhow::Result<std::collections::HashMap<String, Vec<u8>>> {
        todo!()
    }

    async fn add_account_access_keys(
        &self,
        account_id: near_primitives::types::AccountId,
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
        account_keys: std::collections::HashMap<String, Vec<u8>>,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn add_contract_code(
        &self,
        account_id: near_primitives::types::AccountId,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
        code: &[u8],
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn delete_contract_code(
        &self,
        account_id: near_primitives::types::AccountId,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn add_account(
        &self,
        account_id: near_primitives::types::AccountId,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
        account: Vec<u8>,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn delete_account(
        &self,
        account_id: near_primitives::types::AccountId,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn add_block(
        &self,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()> {
        sqlx::query(
            "
            INSERT INTO blocks (block_height, block_hash)
            VALUES (?, ?)
            ",
        )
        .bind(bigdecimal::BigDecimal::from(block_height))
        .bind(block_hash.to_string())
        .execute(&self.meta_db_pool)
        .await?;
        Ok(())
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
        let mut query = sqlx::query(
            "
            INSERT INTO chunks (block_height, chunk_hash, shard_id, height_included)
            VALUES
            ",
        );
        for (chunk_hash, shard_id, height_included) in chunks {
            query = query
                .bind(bigdecimal::BigDecimal::from(block_height))
                .bind(chunk_hash.to_string())
                .bind(bigdecimal::BigDecimal::from(shard_id))
                .bind(bigdecimal::BigDecimal::from(height_included))
                .bind(", ");
        }
        query = query.bind(";");
        query.execute(&self.meta_db_pool).await?;
        Ok(())
    }

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

    async fn update_meta(&self, indexer_id: &str, block_height: u64) -> anyhow::Result<()> {
        sqlx::query(
            "
            UPDATE meta
            SET last_processed_block_height = ?
            WHERE indexer_id = ?
            ",
        )
        .bind(bigdecimal::BigDecimal::from(block_height))
        .bind(indexer_id)
        .execute(&self.meta_db_pool)
        .await?;
        Ok(())
    }

    async fn get_last_processed_block_height(&self, indexer_id: &str) -> anyhow::Result<u64> {
        let (last_processed_block_height,): (bigdecimal::BigDecimal,) = sqlx::query_as(
            "
            SELECT last_processed_block_height
            FROM meta
            WHERE indexer_id = ?
            LIMIT 1
            ",
        )
        .bind(indexer_id)
        .fetch_one(&self.meta_db_pool)
        .await?;
        last_processed_block_height
            .to_u64()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse `last_processed_block_height` to u64"))
    }

    async fn add_validators(
        &self,
        epoch_id: near_indexer_primitives::CryptoHash,
        epoch_height: u64,
        epoch_start_height: u64,
        validators_info: &near_primitives::views::EpochValidatorInfo,
    ) -> anyhow::Result<()> {
        sqlx::query(
            "
            INSERT INTO validators (epoch_id, epoch_height, epoch_start_height, epoch_end_height, protocol_config)
            VALUES (?, ?, ?, NULL, ?)
            "
        )
            .bind(&epoch_id.to_string())
            .bind(bigdecimal::BigDecimal::from(epoch_height))
            .bind(bigdecimal::BigDecimal::from(epoch_start_height))
            .bind(&serde_json::to_string(validators_info)?)
            .execute(&self.meta_db_pool)
            .await?;
        Ok(())
    }

    async fn add_protocol_config(
        &self,
        epoch_id: near_indexer_primitives::CryptoHash,
        epoch_height: u64,
        epoch_start_height: u64,
        protocol_config: &near_chain_configs::ProtocolConfigView,
    ) -> anyhow::Result<()> {
        sqlx::query(
            "
            INSERT INTO protocol_configs (epoch_id, epoch_height, epoch_start_height, epoch_end_height, protocol_config)
            VALUES (?, ?, ?, NULL, ?)
            "
        )
            .bind(&epoch_id.to_string())
            .bind(bigdecimal::BigDecimal::from(epoch_height))
            .bind(bigdecimal::BigDecimal::from(epoch_start_height))
            .bind(&serde_json::to_string(protocol_config)?)
            .execute(&self.meta_db_pool)
            .await?;
        Ok(())
    }

    async fn update_epoch_end_height(
        &self,
        epoch_id: near_indexer_primitives::CryptoHash,
        epoch_end_block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()> {
        let block_height = self
            .get_block_by_hash(epoch_end_block_hash, "update_epoch_end_height")
            .await?;
        let epoch_id_str = epoch_id.to_string();
        let updated_validators_future = sqlx::query(
            "
            UPDATE validators
            SET epoch_end_height = ?
            WHERE epoch_id = ?
            ",
        )
        .bind(bigdecimal::BigDecimal::from(block_height))
        .bind(&epoch_id_str)
        .execute(&self.meta_db_pool);

        let updated_protocol_config_future = sqlx::query(
            "
            UPDATE protocol_configs
            SET epoch_end_height = ?
            WHERE epoch_id = ?
            ",
        )
        .bind(bigdecimal::BigDecimal::from(block_height))
        .bind(&epoch_id_str)
        .execute(&self.meta_db_pool);

        futures::try_join!(updated_validators_future, updated_protocol_config_future)?;
        Ok(())
    }
}
