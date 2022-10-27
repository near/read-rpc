pub mod methods;
pub mod utils;

#[derive(Clone, Copy, Debug)]
pub struct CacheBlock {
    pub block_hash: near_primitives::hash::CryptoHash,
    pub block_height: near_primitives::types::BlockHeight,
    pub block_timestamp: u64,
    pub latest_protocol_version: near_primitives::types::ProtocolVersion,
}

impl shared_lru::JustStack for CacheBlock {}
