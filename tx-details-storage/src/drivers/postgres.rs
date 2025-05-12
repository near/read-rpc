use crate::traits::Storage;
use anyhow::Result;
use async_trait::async_trait;
use bigdecimal::BigDecimal;
use num_traits::cast::ToPrimitive;
use sqlx::{postgres::PgPool, Row};

pub struct PostgresTxDetailsStorage {
    pool: PgPool,
}

impl PostgresTxDetailsStorage {
    /// Creates a new instance of `PostgresTxDetailsStorage`.
    pub async fn new(pool: PgPool) -> Result<Self> {
        Self::create_tables(&pool).await?;
        Ok(Self { pool })
    }

    /// Creates the necessary tables if they don't exist.
    async fn create_tables(pool: &PgPool) -> Result<()> {
        sqlx::query(
            "
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_hash VARCHAR PRIMARY KEY,
                sender_account_id VARCHAR NOT NULL,
                block_height NUMERIC(20,0) NOT NULL,
                transaction_details BYTEA NOT NULL
            );
            ",
        )
        .execute(pool)
        .await?;

        sqlx::query(
            "
            CREATE TABLE IF NOT EXISTS receipts (
                receipt_id VARCHAR PRIMARY KEY,
                parent_transaction_hash VARCHAR NOT NULL,
                receiver_id VARCHAR NOT NULL,
                block_height NUMERIC(20,0) NOT NULL,
                block_hash VARCHAR NOT NULL,
                shard_id BIGINT NOT NULL
            );
            ",
        )
        .execute(pool)
        .await?;

        sqlx::query(
            "
            CREATE TABLE IF NOT EXISTS outcomes (
                outcome_id VARCHAR PRIMARY KEY,
                parent_transaction_hash VARCHAR NOT NULL,
                receiver_id VARCHAR NOT NULL,
                block_height NUMERIC(20,0) NOT NULL,
                block_hash VARCHAR NOT NULL,
                shard_id BIGINT NOT NULL
            );
            ",
        )
        .execute(pool)
        .await?;

        sqlx::query(
            "
            CREATE TABLE IF NOT EXISTS meta (
                indexer_id VARCHAR PRIMARY KEY,
                last_processed_block_height NUMERIC(20,0) NOT NULL
            );
            ",
        )
        .execute(pool)
        .await?;

        Ok(())
    }
}

#[async_trait]
impl Storage for PostgresTxDetailsStorage {
    async fn save_tx(&self, key: &str, data: Vec<u8>, block_height: u64) -> Result<()> {
        sqlx::query(
            "
            INSERT INTO transactions (transaction_hash, sender_account_id, block_height, transaction_details)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (transaction_hash) DO NOTHING;
            ",
        )
        .bind(key)
        .bind("default_sender") // TODO: use it to determine the shard id to store the data
        .bind(BigDecimal::from(block_height))
        .bind(data)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn retrieve_tx(&self, key: &str) -> Result<Vec<u8>> {
        let row = sqlx::query(
            "
            SELECT transaction_details
            FROM transactions
            WHERE transaction_hash = $1;
            ",
        )
        .bind(key)
        .fetch_one(&self.pool)
        .await?;

        let data: Vec<u8> = row.get("transaction_details");
        Ok(data)
    }

    async fn save_receipts(
        &self,
        receipts: Vec<readnode_primitives::ReceiptRecord>,
        block_height: u64,
    ) -> Result<()> {
        for receipt in receipts {
            let shard_id: u64 = receipt.shard_id.into();
            sqlx::query(
                "
                INSERT INTO receipts (receipt_id, parent_transaction_hash, receiver_id, block_height, block_hash, shard_id)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (receipt_id) DO NOTHING;
                ",
            )
            .bind(&receipt.receipt_id.to_string()) // Convert CryptoHash to String
            .bind(&receipt.parent_transaction_hash.to_string()) // Convert CryptoHash to String
            .bind(&receipt.receiver_id.to_string()) // Assuming receiver_id is already a String
            .bind(BigDecimal::from(block_height)) // Use block_height
            .bind(&receipt.block_hash.to_string()) // Convert CryptoHash to String
            .bind(BigDecimal::from(shard_id)) // Convert ShardId to BigDecimal
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    async fn get_receipt_by_id(
        &self,
        receipt_id: &str,
    ) -> Result<readnode_primitives::ReceiptRecord> {
        let row = sqlx::query(
            "
            SELECT receipt_id, parent_transaction_hash, receiver_id, block_height, block_hash, shard_id
            FROM receipts
            WHERE receipt_id = $1;
            ",
        )
        .bind(receipt_id)
        .fetch_one(&self.pool)
        .await?;

        // Extract fields from the row and construct a tuple
        let tuple = (
            row.get::<String, _>("receipt_id"),
            row.get::<String, _>("parent_transaction_hash"),
            row.get::<String, _>("receiver_id"),
            row.get::<BigDecimal, _>("block_height"),
            row.get::<String, _>("block_hash"),
            row.get::<BigDecimal, _>("shard_id"),
        );

        // Use the TryFrom implementation to convert the tuple into a ReceiptRecord
        readnode_primitives::ReceiptRecord::try_from(tuple)
    }

    async fn save_outcomes(
        &self,
        outcomes: Vec<readnode_primitives::OutcomeRecord>,
        block_height: u64,
    ) -> Result<()> {
        for outcome in outcomes {
            let shard_id: u64 = outcome.shard_id.into();
            sqlx::query(
                "
                INSERT INTO outcomes (outcome_id, parent_transaction_hash, receiver_id, block_height, block_hash, shard_id)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (outcome_id) DO NOTHING;
                ",
            )
            .bind(&outcome.outcome_id.to_string()) // Convert CryptoHash to String
            .bind(&outcome.parent_transaction_hash.to_string()) // Convert CryptoHash to String
            .bind(&outcome.receiver_id.to_string()) // Assuming receiver_id is already a String
            .bind(BigDecimal::from(block_height)) // Use block_height
            .bind(&outcome.block_hash.to_string()) // Convert CryptoHash to String
            .bind(BigDecimal::from(shard_id)) // Convert ShardId to BigDecimal
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    async fn get_outcome_by_id(
        &self,
        outcome_id: &str,
    ) -> Result<readnode_primitives::OutcomeRecord> {
        let row = sqlx::query(
            "
            SELECT outcome_id, parent_transaction_hash, receiver_id, block_height, block_hash, shard_id
            FROM outcomes
            WHERE outcome_id = $1;
            ",
        )
        .bind(outcome_id)
        .fetch_one(&self.pool)
        .await?;

        // Extract fields from the row and construct a tuple
        let tuple = (
            row.get::<String, _>("outcome_id"),
            row.get::<String, _>("parent_transaction_hash"),
            row.get::<String, _>("receiver_id"),
            row.get::<BigDecimal, _>("block_height"),
            row.get::<String, _>("block_hash"),
            row.get::<BigDecimal, _>("shard_id"),
        );

        // Use the TryFrom implementation to convert the tuple into an OutcomeRecord
        readnode_primitives::OutcomeRecord::try_from(tuple)
    }

    async fn update_meta(&self, indexer_id: &str, last_processed_block_height: u64) -> Result<()> {
        sqlx::query(
            "
            INSERT INTO meta (indexer_id, last_processed_block_height)
            VALUES ($1, $2)
            ON CONFLICT (indexer_id) DO UPDATE
            SET last_processed_block_height = $2;
            ",
        )
        .bind(indexer_id)
        .bind(BigDecimal::from(last_processed_block_height)) // Convert u64 to BigDecimal
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_last_processed_block_height(&self, indexer_id: &str) -> Result<u64> {
        let row = sqlx::query(
            "
            SELECT last_processed_block_height
            FROM meta
            WHERE indexer_id = $1;
            ",
        )
        .bind(indexer_id)
        .fetch_one(&self.pool)
        .await?;
        row.get::<BigDecimal, _>("last_processed_block_height")
            .to_u64()
            .ok_or_else(|| anyhow::anyhow!("Failed to convert last_processed_block_height to u64"))?
            .to_u64()
            .ok_or_else(|| anyhow::anyhow!("Failed to convert last_processed_block_height to u64"))
    }

    async fn save_outcomes_and_receipts(
        &self,
        receipts: Vec<readnode_primitives::ReceiptRecord>,
        outcomes: Vec<readnode_primitives::OutcomeRecord>,
        block_height: u64,
    ) -> Result<()> {
        self.save_receipts(receipts, block_height).await?;
        self.save_outcomes(outcomes, block_height).await?;
        Ok(())
    }
}
