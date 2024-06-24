#[async_trait::async_trait]
pub trait StateIndexerDbManager {
    async fn add_block(
        &self,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()>;

    async fn add_chunks(
        &self,
        block_height: u64,
        chunks: Vec<(
            crate::primitives::ChunkHash,
            crate::primitives::ShardId,
            crate::primitives::HeightIncluded,
        )>,
    ) -> anyhow::Result<()>;

    async fn get_block_by_hash(
        &self,
        block_hash: near_primitives::hash::CryptoHash,
        method_name: &str,
    ) -> anyhow::Result<u64>;

    async fn update_meta(&self, indexer_id: &str, block_height: u64) -> anyhow::Result<()>;

    async fn get_last_processed_block_height(&self, indexer_id: &str) -> anyhow::Result<u64>;

    async fn add_validators(
        &self,
        epoch_id: near_indexer_primitives::CryptoHash,
        epoch_height: u64,
        epoch_start_height: u64,
        validators_info: &near_primitives::views::EpochValidatorInfo,
        epoch_end_block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()>;

    async fn save_block(
        &self,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
        chunks: Vec<(
            crate::primitives::ChunkHash,
            crate::primitives::ShardId,
            crate::primitives::HeightIncluded,
        )>,
    ) -> anyhow::Result<()> {
        let add_block_future = self.add_block(block_height, block_hash);
        let add_chunks_future = self.add_chunks(block_height, chunks);

        futures::try_join!(add_block_future, add_chunks_future)?;
        Ok(())
    }

    async fn save_state_changes_data(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()>;

    async fn save_state_changes_access_key(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()>;

    async fn save_state_changes_contract(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()>;

    async fn save_state_changes_account(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
        block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()>;
}
