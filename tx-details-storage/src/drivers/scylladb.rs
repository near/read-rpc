use futures::FutureExt;
use num_traits::cast::ToPrimitive;

use anyhow::Result;
use async_trait::async_trait;

use crate::traits::Storage;

pub struct ScyllaDbTxDetailsStorage {
    add_transaction: scylla::prepared_statement::PreparedStatement,
    get_transaction: scylla::prepared_statement::PreparedStatement,

    add_receipt: scylla::prepared_statement::PreparedStatement,
    get_receipt: scylla::prepared_statement::PreparedStatement,

    add_outcome: scylla::prepared_statement::PreparedStatement,
    get_outcome: scylla::prepared_statement::PreparedStatement,

    update_meta: scylla::prepared_statement::PreparedStatement,
    last_processed_block_height: scylla::prepared_statement::PreparedStatement,

    scylla_session: scylla::Session,
}

impl ScyllaDbTxDetailsStorage {
    pub async fn new(scylla_session: scylla::Session) -> anyhow::Result<Self> {
        Self::create_keyspace(&scylla_session).await?;
        Self::create_table(&scylla_session).await?;
        Ok(Self {
            add_transaction: Self::prepare_query(
                &scylla_session,
                "INSERT INTO tx_details.transactions
                (transaction_hash, transaction_details)
                VALUES(?, ?)",
                scylla::frame::types::Consistency::LocalQuorum,
            ).await?,
            get_transaction: Self::prepare_query(
                &scylla_session,
                "SELECT transaction_details FROM tx_details.transactions WHERE transaction_hash = ? LIMIT 1",
                scylla::frame::types::Consistency::LocalOne,
            ).await?,
            add_receipt: Self::prepare_query(
                &scylla_session,
                "INSERT INTO tx_details.receipts_map
                    (receipt_id, parent_transaction_hash, receiver_id, block_height, block_hash, shard_id)
                    VALUES(?, ?, ?, ?, ?, ?)",
                scylla::frame::types::Consistency::LocalQuorum,
            ).await?,
            get_receipt: Self::prepare_query(
                &scylla_session,
                "SELECT receipt_id, parent_transaction_hash, receiver_id, block_height, block_hash, shard_id FROM tx_details.receipts_map WHERE receipt_id = ? LIMIT 1",
                scylla::frame::types::Consistency::LocalOne,
            ).await?,
            add_outcome: Self::prepare_query(
                &scylla_session,
                "INSERT INTO tx_details.outcomes_map
                    (outcome_id, parent_transaction_hash, receiver_id, block_height, block_hash, shard_id)
                    VALUES(?, ?, ?, ?, ?, ?)",
                scylla::frame::types::Consistency::LocalQuorum,
            ).await?,
            get_outcome: Self::prepare_query(
                &scylla_session,
                "SELECT outcome_id, parent_transaction_hash, receiver_id, block_height, block_hash, shard_id FROM tx_details.outcomes_map WHERE outcome_id = ? LIMIT 1",
                scylla::frame::types::Consistency::LocalOne,
            ).await?,
            update_meta: Self::prepare_query(
                &scylla_session,
                "INSERT INTO tx_details.meta
                    (indexer_id, last_processed_block_height)
                    VALUES (?, ?)",
                scylla::frame::types::Consistency::LocalQuorum,
            ).await?,
            last_processed_block_height: Self::prepare_query(
                &scylla_session,
                "SELECT last_processed_block_height FROM tx_details.meta WHERE indexer_id = ? LIMIT 1",
                scylla::frame::types::Consistency::LocalOne,
            ).await?,
            scylla_session,
        })
    }

    async fn prepare_query(
        scylla_db_session: &scylla::Session,
        query_text: &str,
        consistency: scylla::frame::types::Consistency,
    ) -> anyhow::Result<scylla::prepared_statement::PreparedStatement> {
        let mut query = scylla::statement::query::Query::new(query_text);
        query.set_consistency(consistency);
        Ok(scylla_db_session.prepare(query).await?)
    }

    pub async fn create_keyspace(scylla_session: &scylla::Session) -> anyhow::Result<()> {
        scylla_session
            .query_unpaged(
                "CREATE KEYSPACE IF NOT EXISTS tx_details
                WITH REPLICATION = {
                    'class': 'NetworkTopologyStrategy',
                    'replication_factor': 3
                }",
                &[],
            )
            .await?;
        Ok(())
    }

    pub async fn create_table(scylla_session: &scylla::Session) -> anyhow::Result<()> {
        scylla_session
            .query_unpaged(
                "CREATE TABLE IF NOT EXISTS tx_details.transactions (
                    transaction_hash varchar PRIMARY KEY,
                    transaction_details BLOB
                )",
                &[],
            )
            .await?;
        scylla_session
            .query_unpaged(
                "CREATE TABLE IF NOT EXISTS tx_details.receipts_map (
                    receipt_id varchar PRIMARY KEY,
                    parent_transaction_hash varchar,
                    receiver_id varchar,
                    block_height varint,
                    block_hash varchar,
                    shard_id varint
                )",
                &[],
            )
            .await?;
        scylla_session
            .query_unpaged(
                "CREATE TABLE IF NOT EXISTS tx_details.outcomes_map (
                    outcome_id varchar PRIMARY KEY,
                    parent_transaction_hash varchar,
                    receiver_id varchar,
                    block_height varint,
                    block_hash varchar,
                    shard_id varint
                )",
                &[],
            )
            .await?;
        scylla_session
            .query_unpaged(
                "
                CREATE TABLE IF NOT EXISTS tx_details.meta (
                    indexer_id varchar PRIMARY KEY,
                    last_processed_block_height varint
                )
            ",
                &[],
            )
            .await?;
        Ok(())
    }
}

#[async_trait]
impl Storage for ScyllaDbTxDetailsStorage {
    async fn save_tx(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.scylla_session
            .execute_unpaged(&self.add_transaction, (key, data))
            .await?;
        Ok(())
    }

    async fn retrieve_tx(&self, key: &str) -> Result<Vec<u8>> {
        let (data,) = self
            .scylla_session
            .execute_unpaged(&self.get_transaction, (key.to_string(),))
            .await?
            .into_rows_result()?
            .single_row::<(Vec<u8>,)>()?;
        Ok(data)
    }

    async fn save_receipts(&self, receipts: Vec<readnode_primitives::ReceiptRecord>) -> Result<()> {
        if receipts.is_empty() {
            return Ok(());
        }
        let mut batch: scylla::batch::Batch = Default::default();
        let mut batch_values = Vec::new();
        for receipt in receipts {
            batch.append_statement(self.add_receipt.clone());
            let shard_id: u64 = receipt.shard_id.into();
            batch_values.push((
                receipt.receipt_id.to_string(),
                receipt.parent_transaction_hash.to_string(),
                receipt.receiver_id.to_string(),
                num_bigint::BigInt::from(receipt.block_height),
                receipt.block_hash.to_string(),
                num_bigint::BigInt::from(shard_id),
            ));
        }
        self.scylla_session.batch(&batch, batch_values).await?;
        Ok(())
    }

    async fn get_receipt_by_id(
        &self,
        receipt_id: &str,
    ) -> Result<readnode_primitives::ReceiptRecord> {
        readnode_primitives::ReceiptRecord::try_from(
            self.scylla_session
                .execute_unpaged(&self.get_receipt, (receipt_id.to_string(),))
                .await?
                .into_rows_result()?
                .single_row::<(
                    String,
                    String,
                    String,
                    num_bigint::BigInt,
                    String,
                    num_bigint::BigInt,
                )>()?,
        )
    }

    async fn save_outcomes(&self, outcomes: Vec<readnode_primitives::OutcomeRecord>) -> Result<()> {
        if outcomes.is_empty() {
            return Ok(());
        }
        let mut batch: scylla::batch::Batch = Default::default();
        let mut batch_values = Vec::new();
        for outcome in outcomes {
            batch.append_statement(self.add_outcome.clone());
            let shard_id: u64 = outcome.shard_id.into();
            batch_values.push((
                outcome.outcome_id.to_string(),
                outcome.parent_transaction_hash.to_string(),
                outcome.receiver_id.to_string(),
                num_bigint::BigInt::from(outcome.block_height),
                outcome.block_hash.to_string(),
                num_bigint::BigInt::from(shard_id),
            ));
        }
        self.scylla_session.batch(&batch, batch_values).await?;
        Ok(())
    }

    async fn get_outcome_by_id(
        &self,
        outcome_id: &str,
    ) -> Result<readnode_primitives::OutcomeRecord> {
        readnode_primitives::OutcomeRecord::try_from(
            self.scylla_session
                .execute_unpaged(&self.get_outcome, (outcome_id.to_string(),))
                .await?
                .into_rows_result()?
                .single_row::<(
                    String,
                    String,
                    String,
                    num_bigint::BigInt,
                    String,
                    num_bigint::BigInt,
                )>()?,
        )
    }

    async fn update_meta(&self, indexer_id: &str, last_processed_block_height: u64) -> Result<()> {
        self.scylla_session
            .execute_unpaged(
                &self.update_meta,
                (
                    indexer_id.to_string(),
                    num_bigint::BigInt::from(last_processed_block_height),
                ),
            )
            .await?;
        Ok(())
    }

    async fn get_last_processed_block_height(&self, indexer_id: &str) -> Result<u64> {
        let (last_processed_block_height,) = self
            .scylla_session
            .execute_unpaged(&self.last_processed_block_height, (indexer_id.to_string(),))
            .await?
            .into_rows_result()?
            .single_row::<(num_bigint::BigInt,)>()?;
        last_processed_block_height.to_u64().ok_or(anyhow::anyhow!(
            "Failed to convert last_processed_block_height to u64"
        ))
    }

    async fn save_outcomes_and_receipts(
        &self,
        receipts: Vec<readnode_primitives::ReceiptRecord>,
        outcomes: Vec<readnode_primitives::OutcomeRecord>,
    ) -> anyhow::Result<()> {
        let save_outcome_future = self.save_outcomes(outcomes);
        let save_receipt_future = self.save_receipts(receipts);

        futures::future::join_all([save_outcome_future.boxed(), save_receipt_future.boxed()])
            .await
            .into_iter()
            .collect::<anyhow::Result<()>>()
    }
}
