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
