use anyhow::Result;

#[async_trait::async_trait]
pub trait TxIndexerDbManager {
    async fn create_tx_tables(&self) -> Result<()>;

    async fn save_transaction(
        &self,
        sender_id: &near_primitives::types::AccountId,
        key: &str,
        data: Vec<u8>,
        block_height: u64,
    ) -> Result<()>;

    async fn retrieve_transaction(
        &self,
        key: &str,
        shard_id: &near_primitives::types::ShardId,
    ) -> Result<Vec<u8>>;

    async fn save_receipts(&self, receipts: Vec<readnode_primitives::ReceiptRecord>) -> Result<()>;

    async fn get_receipt_by_id(
        &self,
        receipt_id: &str,
    ) -> Result<readnode_primitives::ReceiptRecord>;

    async fn save_outcomes(&self, outcomes: Vec<readnode_primitives::OutcomeRecord>) -> Result<()>;

    async fn get_outcome_by_id(
        &self,
        outcome_id: &str,
    ) -> Result<readnode_primitives::OutcomeRecord>;

    async fn update_meta(&self, indexer_id: &str, last_processed_block_height: u64) -> Result<()>;

    async fn get_last_processed_block_height(&self, indexer_id: &str) -> Result<u64>;

    async fn save_outcomes_and_receipts(
        &self,
        receipts: Vec<readnode_primitives::ReceiptRecord>,
        outcomes: Vec<readnode_primitives::OutcomeRecord>,
    ) -> Result<()>;
}
