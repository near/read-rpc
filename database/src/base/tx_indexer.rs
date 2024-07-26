#[async_trait::async_trait]
pub trait TxIndexerDbManager {
    async fn save_receipts(
        &self,
        shard_id: crate::primitives::ShardId,
        receipts: Vec<readnode_primitives::ReceiptRecord>,
    ) -> anyhow::Result<()>;

    async fn save_outcomes(
        &self,
        shard_id: crate::primitives::ShardId,
        outcomes: Vec<readnode_primitives::OutcomeRecord>,
    ) -> anyhow::Result<()>;

    async fn save_outcome_and_receipt(
        &self,
        shard_id: crate::primitives::ShardId,
        receipts: Vec<readnode_primitives::ReceiptRecord>,
        outcomes: Vec<readnode_primitives::OutcomeRecord>,
    ) -> anyhow::Result<()> {
        let save_outcome_future = self.save_outcomes(shard_id, outcomes);
        let save_receipt_future = self.save_receipts(shard_id, receipts);
        futures::try_join!(save_outcome_future, save_receipt_future)?;
        Ok(())
    }

    async fn update_meta(&self, indexer_id: &str, block_height: u64) -> anyhow::Result<()>;

    async fn get_last_processed_block_height(&self, indexer_id: &str) -> anyhow::Result<u64>;
}
