#[async_trait::async_trait]
pub trait TxIndexerDbManager {
    async fn save_receipt(
        &self,
        receipt_id: &near_indexer_primitives::CryptoHash,
        parent_tx_hash: &near_indexer_primitives::CryptoHash,
        receiver_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        block_hash: near_indexer_primitives::CryptoHash,
        shard_id: crate::primitives::ShardId,
    ) -> anyhow::Result<()>;

    async fn update_meta(&self, indexer_id: &str, block_height: u64) -> anyhow::Result<()>;

    async fn get_last_processed_block_height(&self, indexer_id: &str) -> anyhow::Result<u64>;
}
