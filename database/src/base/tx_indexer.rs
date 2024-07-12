#[async_trait::async_trait]
pub trait TxIndexerDbManager {
    async fn save_receipt(
        &self,
        receipt_id: &near_primitives::hash::CryptoHash,
        parent_tx_hash: &near_primitives::hash::CryptoHash,
        receiver_id: &near_primitives::types::AccountId,
        block: readnode_primitives::BlockRecord,
        shard_id: crate::primitives::ShardId,
    ) -> anyhow::Result<()>;

    async fn save_outcome(
        &self,
        outcome_id: &near_primitives::hash::CryptoHash,
        parent_tx_hash: &near_primitives::hash::CryptoHash,
        receiver_id: &near_primitives::types::AccountId,
        block: readnode_primitives::BlockRecord,
        shard_id: crate::primitives::ShardId,
    ) -> anyhow::Result<()>;

    async fn save_outcome_and_receipt(
        &self,
        outcome_id: &near_primitives::hash::CryptoHash,
        receipt_id: &near_primitives::hash::CryptoHash,
        parent_tx_hash: &near_primitives::hash::CryptoHash,
        receiver_id: &near_primitives::types::AccountId,
        block: readnode_primitives::BlockRecord,
        shard_id: crate::primitives::ShardId,
    ) -> anyhow::Result<()> {
        let save_outcome_future =
            self.save_outcome(outcome_id, parent_tx_hash, receiver_id, block, shard_id);
        let save_receipt_future =
            self.save_receipt(receipt_id, parent_tx_hash, receiver_id, block, shard_id);
        futures::try_join!(save_outcome_future, save_receipt_future)?;
        Ok(())
    }

    async fn update_meta(&self, indexer_id: &str, block_height: u64) -> anyhow::Result<()>;

    async fn get_last_processed_block_height(&self, indexer_id: &str) -> anyhow::Result<u64>;
}
