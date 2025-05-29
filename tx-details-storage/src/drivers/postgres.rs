use crate::traits::Storage;
use anyhow::Result;
use async_trait::async_trait;
use database::TxIndexerDbManager;
use std::sync::Arc;

pub struct PostgresTxDetailsStorage {
    db_manager: Arc<dyn TxIndexerDbManager + Send + Sync>,
}

impl PostgresTxDetailsStorage {
    /// Creates a new instance of `PostgresTxDetailsStorage`.
    pub async fn new(db_manager: Arc<dyn TxIndexerDbManager + Send + Sync>) -> Result<Self> {
        db_manager.create_tx_tables().await?;
        Ok(Self { db_manager })
    }
}

#[async_trait]
impl Storage for PostgresTxDetailsStorage {
    async fn save_tx(
        &self,
        sender_id: &near_primitives::types::AccountId,
        key: &str,
        data: Vec<u8>,
        block_height: u64,
    ) -> Result<()> {
        self.db_manager
            .save_transaction(sender_id, key, data, block_height)
            .await
    }

    async fn retrieve_tx(
        &self,
        key: &str,
        shard_id: &near_primitives::types::ShardId,
    ) -> Result<Vec<u8>> {
        self.db_manager.retrieve_transaction(key, shard_id).await
    }

    async fn save_receipts(&self, receipts: Vec<readnode_primitives::ReceiptRecord>) -> Result<()> {
        self.db_manager.save_receipts(receipts).await
    }

    async fn get_receipt_by_id(
        &self,
        receipt_id: &str,
    ) -> Result<readnode_primitives::ReceiptRecord> {
        self.db_manager.get_receipt_by_id(receipt_id).await
    }

    async fn save_outcomes(&self, outcomes: Vec<readnode_primitives::OutcomeRecord>) -> Result<()> {
        self.db_manager.save_outcomes(outcomes).await
    }

    async fn get_outcome_by_id(
        &self,
        outcome_id: &str,
    ) -> Result<readnode_primitives::OutcomeRecord> {
        self.db_manager.get_outcome_by_id(outcome_id).await
    }

    async fn update_meta(&self, indexer_id: &str, last_processed_block_height: u64) -> Result<()> {
        self.db_manager
            .update_meta(indexer_id, last_processed_block_height)
            .await
    }

    async fn get_last_processed_block_height(&self, indexer_id: &str) -> Result<u64> {
        self.db_manager
            .get_last_processed_block_height(indexer_id)
            .await
    }

    async fn save_outcomes_and_receipts(
        &self,
        receipts: Vec<readnode_primitives::ReceiptRecord>,
        outcomes: Vec<readnode_primitives::OutcomeRecord>,
    ) -> Result<()> {
        self.db_manager
            .save_outcomes_and_receipts(receipts, outcomes)
            .await
    }
}
