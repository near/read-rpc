use bigdecimal::ToPrimitive;

#[async_trait::async_trait]
impl crate::TxIndexerDbManager for crate::PostgresDBManager {
    async fn save_receipt(
        &self,
        receipt_id: &near_indexer_primitives::CryptoHash,
        parent_tx_hash: &near_indexer_primitives::CryptoHash,
        receiver_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_indexer_primitives::CryptoHash,
        shard_id: crate::primitives::ShardId,
    ) -> anyhow::Result<()> {
        let shard_id_pool = self.get_shard_connection(receiver_id).await?;
        crate::metrics::SHARD_DATABASE_WRITE_QUERIES
            .with_label_values(&[
                &shard_id_pool.shard_id.to_string(),
                "save_receipt",
                "receipts_map",
            ])
            .inc();
        sqlx::query(
            "
            INSERT INTO receipts_map (receipt_id, parent_transaction_hash, receiver_id, block_height, block_hash, shard_id)
            VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING;
            ",
        )
            .bind(receipt_id.to_string())
            .bind(parent_tx_hash.to_string())
            .bind(receiver_id.to_string())
            .bind(bigdecimal::BigDecimal::from(block_height))
            .bind(block_hash.to_string())
            .bind(bigdecimal::BigDecimal::from(shard_id))
            .execute(shard_id_pool.pool)
            .await?;
        Ok(())
    }

    async fn update_meta(&self, indexer_id: &str, block_height: u64) -> anyhow::Result<()> {
        crate::metrics::META_DATABASE_WRITE_QUERIES
            .with_label_values(&["update_meta", "meta"])
            .inc();
        sqlx::query(
            "
            INSERT INTO meta (indexer_id, last_processed_block_height)
            VALUES ($1, $2)
            ON CONFLICT (indexer_id)
            DO UPDATE SET last_processed_block_height = $2;
            ",
        )
        .bind(indexer_id)
        .bind(bigdecimal::BigDecimal::from(block_height))
        .execute(&self.meta_db_pool)
        .await?;
        Ok(())
    }

    async fn get_last_processed_block_height(&self, indexer_id: &str) -> anyhow::Result<u64> {
        crate::metrics::META_DATABASE_READ_QUERIES
            .with_label_values(&["get_last_processed_block_height", "meta"])
            .inc();
        let (last_processed_block_height,): (bigdecimal::BigDecimal,) = sqlx::query_as(
            "
            SELECT last_processed_block_height
            FROM meta
            WHERE indexer_id = $1
            LIMIT 1;
            ",
        )
        .bind(indexer_id)
        .fetch_one(&self.meta_db_pool)
        .await?;
        last_processed_block_height
            .to_u64()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse `last_processed_block_height` to u64"))
    }
}
