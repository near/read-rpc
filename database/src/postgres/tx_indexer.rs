use bigdecimal::ToPrimitive;

#[async_trait::async_trait]
impl crate::TxIndexerDbManager for crate::PostgresDBManager {
    async fn save_receipts(
        &self,
        shard_id: crate::primitives::ShardId,
        receipts: Vec<readnode_primitives::ReceiptRecord>,
    ) -> anyhow::Result<()> {
        if receipts.is_empty() {
            return Ok(());
        }
        crate::metrics::SHARD_DATABASE_WRITE_QUERIES
            .with_label_values(&[&shard_id.to_string(), "save_receipts", "receipts_map"])
            .inc();
        let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
            "INSERT INTO receipts_map (receipt_id, parent_transaction_hash, receiver_id, block_height, block_hash, shard_id) ",
        );
        query_builder.push_values(receipts.iter(), |mut values, receipt| {
            values
                .push_bind(receipt.receipt_id.to_string())
                .push_bind(receipt.parent_transaction_hash.to_string())
                .push_bind(receipt.receiver_id.to_string())
                .push_bind(bigdecimal::BigDecimal::from(receipt.block_height))
                .push_bind(receipt.block_hash.to_string())
                .push_bind(bigdecimal::BigDecimal::from(receipt.shard_id));
        });
        query_builder.push(" ON CONFLICT DO NOTHING;");
        query_builder
            .build()
            .execute(self.shards_pool.get(&shard_id).ok_or(anyhow::anyhow!(
                "Database connection for Shard_{} not found",
                shard_id
            ))?)
            .await?;
        Ok(())
    }

    async fn save_outcomes(
        &self,
        shard_id: crate::primitives::ShardId,
        outcomes: Vec<readnode_primitives::OutcomeRecord>,
    ) -> anyhow::Result<()> {
        if outcomes.is_empty() {
            return Ok(());
        }
        crate::metrics::SHARD_DATABASE_WRITE_QUERIES
            .with_label_values(&[&shard_id.to_string(), "save_outcomes", "outcomes_map"])
            .inc();
        let mut query_builder: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
            "INSERT INTO outcomes_map (outcome_id, parent_transaction_hash, receiver_id, block_height, block_hash, shard_id) ",
        );
        query_builder.push_values(outcomes.iter(), |mut values, outcome| {
            values
                .push_bind(outcome.outcome_id.to_string())
                .push_bind(outcome.parent_transaction_hash.to_string())
                .push_bind(outcome.receiver_id.to_string())
                .push_bind(bigdecimal::BigDecimal::from(outcome.block_height))
                .push_bind(outcome.block_hash.to_string())
                .push_bind(bigdecimal::BigDecimal::from(outcome.shard_id));
        });
        query_builder.push(" ON CONFLICT DO NOTHING;");
        query_builder
            .build()
            .execute(self.shards_pool.get(&shard_id).ok_or(anyhow::anyhow!(
                "Database connection for Shard_{} not found",
                shard_id
            ))?)
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
