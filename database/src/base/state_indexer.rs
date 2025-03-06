#[async_trait::async_trait]
pub trait StateIndexerDbManager {
    async fn save_block(
        &self,
        block_height: u64,
        block_hash: near_primitives::hash::CryptoHash,
    ) -> anyhow::Result<()>;

    async fn save_chunks(
        &self,
        block_height: u64,
        chunks: Vec<(
            crate::primitives::ChunkHash,
            crate::primitives::ShardId,
            crate::primitives::HeightIncluded,
        )>,
    ) -> anyhow::Result<()>;

    async fn get_block_height_by_hash(
        &self,
        block_hash: near_primitives::hash::CryptoHash,
        method_name: &str,
    ) -> anyhow::Result<u64>;

    async fn update_meta(&self, indexer_id: &str, block_height: u64) -> anyhow::Result<()>;

    async fn get_last_processed_block_height(&self, indexer_id: &str) -> anyhow::Result<u64>;

    async fn save_validators(
        &self,
        epoch_id: near_primitives::hash::CryptoHash,
        epoch_height: u64,
        epoch_start_height: u64,
        validators_info: &near_primitives::views::EpochValidatorInfo,
        epoch_end_block_hash: near_primitives::hash::CryptoHash,
    ) -> anyhow::Result<()>;

    async fn save_block_with_chunks(
        &self,
        block_height: u64,
        block_hash: near_primitives::hash::CryptoHash,
        chunks: Vec<(
            crate::primitives::ChunkHash,
            crate::primitives::ShardId,
            crate::primitives::HeightIncluded,
        )>,
    ) -> anyhow::Result<()> {
        let add_block_future = self.save_block(block_height, block_hash);
        let add_chunks_future = self.save_chunks(block_height, chunks);

        futures::future::join_all([add_block_future, add_chunks_future])
            .await
            .into_iter()
            .collect::<anyhow::Result<()>>()
    }

    async fn save_state_changes_data(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
    ) -> anyhow::Result<()> {
        self.insert_state_changes_data(shard_id, state_changes.clone(), block_height)
            .await?;
        self.update_state_changes_data(shard_id, state_changes, block_height)
            .await?;
        Ok(())
    }

    async fn insert_state_changes_data(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
    ) -> anyhow::Result<()>;

    async fn update_state_changes_data(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
    ) -> anyhow::Result<()>;

    async fn save_state_changes_access_key(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
    ) -> anyhow::Result<()> {
        self.insert_state_changes_access_key(shard_id, state_changes.clone(), block_height)
            .await?;
        self.update_state_changes_access_key(shard_id, state_changes, block_height)
            .await?;
        Ok(())
    }

    async fn insert_state_changes_access_key(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
    ) -> anyhow::Result<()>;

    async fn update_state_changes_access_key(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
    ) -> anyhow::Result<()>;

    async fn save_state_changes_contract(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
    ) -> anyhow::Result<()> {
        self.insert_state_changes_contract(shard_id, state_changes.clone(), block_height)
            .await?;
        self.update_state_changes_contract(shard_id, state_changes, block_height)
            .await?;
        Ok(())
    }

    async fn insert_state_changes_contract(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
    ) -> anyhow::Result<()>;

    async fn update_state_changes_contract(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
    ) -> anyhow::Result<()>;

    async fn save_state_changes_account(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
    ) -> anyhow::Result<()> {
        self.insert_state_changes_account(shard_id, state_changes.clone(), block_height)
            .await?;
        self.update_state_changes_account(shard_id, state_changes, block_height)
            .await?;
        Ok(())
    }

    async fn insert_state_changes_account(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
    ) -> anyhow::Result<()>;

    async fn update_state_changes_account(
        &self,
        shard_id: near_primitives::types::ShardId,
        state_changes: Vec<near_primitives::views::StateChangeWithCauseView>,
        block_height: u64,
    ) -> anyhow::Result<()>;
}
