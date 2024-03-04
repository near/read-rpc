pub mod methods;
pub mod utils;

#[derive(Clone, Copy, Debug)]
pub struct CacheBlock {
    pub block_hash: near_primitives::hash::CryptoHash,
    pub block_height: near_primitives::types::BlockHeight,
    pub block_timestamp: u64,
    pub gas_price: near_primitives::types::Balance,
    pub latest_protocol_version: near_primitives::types::ProtocolVersion,
    pub chunks_included: u64,
    pub state_root: near_primitives::hash::CryptoHash,
    pub epoch_id: near_primitives::hash::CryptoHash,
}

impl From<near_primitives::views::BlockView> for CacheBlock {
    fn from(block: near_primitives::views::BlockView) -> Self {
        Self {
            block_hash: block.header.hash,
            block_height: block.header.height,
            block_timestamp: block.header.timestamp,
            gas_price: block.header.gas_price,
            latest_protocol_version: block.header.latest_protocol_version,
            chunks_included: block.header.chunks_included,
            state_root: block.header.prev_state_root,
            epoch_id: block.header.epoch_id,
        }
    }
}

#[derive(Debug)]
pub struct FinalBlockInfo {
    pub final_block_cache: CacheBlock,
    pub current_validators: near_primitives::views::EpochValidatorInfo,
}

impl FinalBlockInfo {
    pub async fn new(
        near_rpc_client: &crate::utils::JsonRpcClient,
        blocks_cache: &std::sync::Arc<
            futures_locks::RwLock<crate::cache::LruMemoryCache<u64, CacheBlock>>,
        >,
    ) -> Self {
        let final_block = crate::utils::get_final_cache_block(near_rpc_client)
            .await
            .expect("Error to get final block");

        let validators = crate::utils::get_current_validators(near_rpc_client)
            .await
            .expect("Error to get protocol_config");

        blocks_cache
            .write()
            .await
            .put(final_block.block_height, final_block);

        Self {
            final_block_cache: final_block,
            current_validators: validators,
        }
    }
}
