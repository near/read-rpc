use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub trait Storage {
    async fn save_tx(&self, key: &str, data: Vec<u8>, block_height: u64) -> Result<()>;
    async fn retrieve_tx(&self, key: &str) -> Result<Vec<u8>>;
    async fn save_receipts(
        &self,
        receipts: Vec<readnode_primitives::ReceiptRecord>,
        block_height: u64,
    ) -> Result<()>;
    async fn get_receipt_by_id(
        &self,
        receipt_id: &str,
    ) -> Result<readnode_primitives::ReceiptRecord>;
    async fn save_outcomes(
        &self,
        outcomes: Vec<readnode_primitives::OutcomeRecord>,
        block_height: u64,
    ) -> Result<()>;
    async fn save_outcomes_and_receipts(
        &self,
        receipts: Vec<readnode_primitives::ReceiptRecord>,
        outcomes: Vec<readnode_primitives::OutcomeRecord>,
        block_height: u64,
    ) -> Result<()>;
    async fn get_outcome_by_id(
        &self,
        outcome_id: &str,
    ) -> Result<readnode_primitives::OutcomeRecord>;
    async fn update_meta(&self, indexer_id: &str, last_processed_block_height: u64) -> Result<()>;
    async fn get_last_processed_block_height(&self, indexer_id: &str) -> Result<u64>;
}
