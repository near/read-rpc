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

#[derive(Debug)]
pub struct FinalBlockInfo {
    pub final_block_cache: CacheBlock,
    pub current_protocol_config: near_chain_configs::ProtocolConfigView,
    pub current_validators: near_primitives::views::EpochValidatorInfo,
}

impl FinalBlockInfo {
    pub async fn new(
        near_rpc_client: &crate::utils::JsonRpcClient,
        blocks_cache: &std::sync::Arc<
            futures_locks::RwLock<crate::cache::LruMemoryCache<u64, CacheBlock>>,
        >,
    ) -> Self {
        let final_block = near_rpc_client.get_final_cache_block()
            .await
            .expect("Error to get final block");
        let protocol_config = near_rpc_client.get_current_protocol_config()
            .await
            .expect("Error to get protocol_config");

        let validators = near_rpc_client.get_current_validators()
            .await
            .expect("Error to get protocol_config");

        blocks_cache
            .write()
            .await
            .put(final_block.block_height, final_block);

        Self {
            final_block_cache: final_block,
            current_protocol_config: protocol_config,
            current_validators: validators,
        }
    }
}
