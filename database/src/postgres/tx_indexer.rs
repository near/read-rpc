use anyhow::Result;
use async_trait::async_trait;
use bigdecimal::num_traits::ToPrimitive;
use bigdecimal::BigDecimal;
use sqlx::QueryBuilder;

static META_RECEIPTS_AND_OUTCOMES_MIGRATOR: sqlx::migrate::Migrator =
    sqlx::migrate!("src/postgres/migrations/tx_details/receipts_and_outcomes");
static SHARDS_TRANSACTIONS_MIGRATOR: sqlx::migrate::Migrator =
    sqlx::migrate!("src/postgres/migrations/tx_details/transactions");

#[async_trait]
impl crate::base::tx_indexer::TxIndexerDbManager for crate::postgres::PostgresDBManager {
    async fn create_tx_tables(&self) -> Result<()> {
        // Transactions table and partitions on each shard
        for pool in self.shards_pool.values() {
            SHARDS_TRANSACTIONS_MIGRATOR.run(pool).await?;
        }

        // Receipts and outcomes tables and partitions in meta_db_pool only
        META_RECEIPTS_AND_OUTCOMES_MIGRATOR
            .run(&self.meta_db_pool)
            .await?;
        Ok(())
    }

    async fn save_transaction(
        &self,
        sender_id: &near_primitives::types::AccountId,
        key: &str,
        data: Vec<u8>,
        block_height: u64,
    ) -> Result<()> {
        let shard_conn = self.get_shard_connection(sender_id, &0).await?;
        sqlx::query(
            "
            INSERT INTO transactions (transaction_hash, sender_account_id, block_height, transaction_details)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (transaction_hash) DO NOTHING;
            ",
        )
        .bind(key)
        .bind(sender_id.as_str())
        .bind(BigDecimal::from(block_height))
        .bind(data)
        .execute(shard_conn.pool)
        .await?;
        Ok(())
    }

    async fn retrieve_transaction(
        &self,
        key: &str,
        shard_id: &near_primitives::types::ShardId,
    ) -> Result<Vec<u8>> {
        let shard_connection = self.get_shard_connection_by_id(shard_id).await?;
        let (data,): (Vec<u8>,) = sqlx::query_as(
            "
            SELECT transaction_details
            FROM transactions
            WHERE transaction_hash = $1
            LIMIT 1;
            ",
        )
        .bind(key)
        .fetch_one(shard_connection.pool)
        .await?;

        Ok(data)
    }

    async fn save_receipts(&self, receipts: Vec<readnode_primitives::ReceiptRecord>) -> Result<()> {
        if receipts.is_empty() {
            return Ok(());
        }

        let mut builder = QueryBuilder::new(
            "INSERT INTO receipts (receipt_id, parent_transaction_hash, receiver_id, block_height, block_hash, shard_id) "
        );
        builder.push_values(receipts.iter(), |mut b, receipt| {
            let shard_id: u64 = receipt.shard_id.into();
            b.push_bind(receipt.receipt_id.to_string())
                .push_bind(receipt.parent_transaction_hash.to_string())
                .push_bind(receipt.receiver_id.to_string())
                .push_bind(BigDecimal::from(receipt.block_height))
                .push_bind(receipt.block_hash.to_string())
                .push_bind(BigDecimal::from(shard_id));
        });
        builder.push(" ON CONFLICT DO NOTHING");

        builder.build().execute(&self.meta_db_pool).await?;
        Ok(())
    }

    async fn get_receipt_by_id(
        &self,
        receipt_id: &str,
    ) -> Result<readnode_primitives::ReceiptRecord> {
        let row: (String, String, String, BigDecimal, String, BigDecimal) = sqlx::query_as(
            "
            SELECT receipt_id, parent_transaction_hash, receiver_id, block_height, block_hash, shard_id
            FROM receipts
            WHERE receipt_id = $1
            LIMIT 1;
            ",
        )
        .bind(receipt_id)
        .fetch_one(&self.meta_db_pool)
        .await?;

        readnode_primitives::ReceiptRecord::try_from(row)
    }

    async fn save_outcomes(&self, outcomes: Vec<readnode_primitives::OutcomeRecord>) -> Result<()> {
        if outcomes.is_empty() {
            return Ok(());
        }

        let mut builder = QueryBuilder::new(
            "INSERT INTO outcomes (outcome_id, parent_transaction_hash, receiver_id, block_height, block_hash, shard_id) "
        );
        builder.push_values(outcomes.iter(), |mut b, outcome| {
            let shard_id: u64 = outcome.shard_id.into();
            b.push_bind(outcome.outcome_id.to_string())
                .push_bind(outcome.parent_transaction_hash.to_string())
                .push_bind(outcome.receiver_id.to_string())
                .push_bind(BigDecimal::from(outcome.block_height))
                .push_bind(outcome.block_hash.to_string())
                .push_bind(BigDecimal::from(shard_id));
        });
        builder.push(" ON CONFLICT (outcome_id) DO NOTHING;");

        builder.build().execute(&self.meta_db_pool).await?;
        Ok(())
    }

    async fn get_outcome_by_id(
        &self,
        outcome_id: &str,
    ) -> Result<readnode_primitives::OutcomeRecord> {
        let row: (String, String, String, BigDecimal, String, BigDecimal) = sqlx::query_as(
            "
            SELECT outcome_id, parent_transaction_hash, receiver_id, block_height, block_hash, shard_id
            FROM outcomes
            WHERE outcome_id = $1
            LIMIT 1;
            ",
        )
        .bind(outcome_id)
        .fetch_one(&self.meta_db_pool)
        .await?;

        readnode_primitives::OutcomeRecord::try_from(row)
    }

    async fn update_meta(&self, indexer_id: &str, last_processed_block_height: u64) -> Result<()> {
        sqlx::query(
            "
            INSERT INTO meta (indexer_id, last_processed_block_height)
            VALUES ($1, $2)
            ON CONFLICT (indexer_id)
            DO UPDATE SET last_processed_block_height = $2;
            ",
        )
        .bind(indexer_id)
        .bind(BigDecimal::from(last_processed_block_height))
        .execute(&self.meta_db_pool)
        .await?;
        Ok(())
    }

    async fn get_last_processed_block_height(&self, indexer_id: &str) -> Result<u64> {
        let (height,): (BigDecimal,) = sqlx::query_as(
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

        height
            .to_u64()
            .ok_or_else(|| anyhow::anyhow!("Failed to convert last_processed_block_height to u64"))
    }

    async fn save_outcomes_and_receipts(
        &self,
        receipts: Vec<readnode_primitives::ReceiptRecord>,
        outcomes: Vec<readnode_primitives::OutcomeRecord>,
    ) -> Result<()> {
        self.save_receipts(receipts).await?;
        self.save_outcomes(outcomes).await?;
        Ok(())
    }
}
