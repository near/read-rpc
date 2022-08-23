pub mod methods;
pub mod utils;

pub const ACCOUNT_SCOPE: &[u8] = b"a";
const CODE_SCOPE: &[u8] = b"c";
const ACCESS_KEY_SCOPE: &[u8] = b"k";
const DATA_SCOPE: &[u8] = b"d";

const MAX_LIMIT: u8 = 100;

fn build_redis_block_hash_key(
    scope: &[u8],
    account_id: &near_indexer_primitives::types::AccountId,
    key_data: Option<&[u8]>,
) -> Vec<u8> {
    match key_data {
        Some(data) => [b"h:", scope, b":", account_id.as_bytes(), b":", data].concat(),
        None => [b"h:", scope, b":", account_id.as_bytes()].concat(),
    }
}

fn build_redis_data_key(
    scope: &[u8],
    account_id: &near_indexer_primitives::types::AccountId,
    block_hash: Vec<u8>,
    key_data: Option<&[u8]>,
) -> Vec<u8> {
    match key_data {
        Some(data) => [
            b"d",
            scope,
            b":",
            account_id.as_bytes(),
            b":",
            data,
            b":",
            block_hash.as_slice(),
        ]
        .concat(),
        None => [
            b"d",
            scope,
            b":",
            account_id.as_bytes(),
            b":",
            block_hash.as_slice(),
        ]
        .concat(),
    }
}

fn build_redis_state_key(
    scope: &[u8],
    account_id: &near_indexer_primitives::types::AccountId,
) -> Vec<u8> {
    [b"k:", scope, b":", account_id.as_bytes()].concat()
}
