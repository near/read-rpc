pub mod methods;
pub mod utils;

pub struct PageStateValues {
    pub values: Vec<near_primitives::views::StateItem>,
    pub next_page: Option<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct RpcViewStatePaginatedRequest {
    pub account_id: near_primitives::types::AccountId,
    pub block_id: near_primitives::types::BlockId,
    pub next_page: Option<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct RpcViewStatePaginatedResponse {
    pub values: Vec<near_primitives::views::StateItem>,
    pub block_height: near_primitives::types::BlockHeight,
    pub block_hash: near_primitives::hash::CryptoHash,
    pub next_page: Option<String>,
}
