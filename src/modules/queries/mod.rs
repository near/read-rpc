pub mod methods;
pub mod utils;

pub const ACCOUNT_SCOPE: &[u8] = b"a";
// const ACCESS_KEY_SCOPE: &str = "k";
// const DATA_SCOPE: &str = "d";
// const CODE_SCOPE: &str = "c";

fn build_redis_block_hash_key(
    scope: &[u8],
    account_id: &near_indexer_primitives::types::AccountId,
) -> Vec<u8> {
    [b"h:", scope, b":", account_id.as_bytes()].concat()
}

fn build_redis_data_key(
    scope: &[u8],
    account_id: &near_indexer_primitives::types::AccountId,
    block_hash: Vec<u8>,
) -> Vec<u8> {
    [
        b"d",
        scope,
        b":",
        account_id.as_bytes(),
        b":",
        block_hash.as_slice(),
    ]
    .concat()
}
