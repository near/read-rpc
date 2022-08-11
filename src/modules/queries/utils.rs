use crate::modules::queries::{build_redis_block_hash_key, build_redis_data_key, ACCOUNT_SCOPE};
use borsh::BorshDeserialize;

pub async fn fetch_latest_block_height_from_redis(
    redis_client: redis::aio::ConnectionManager,
) -> Option<near_indexer_primitives::types::BlockHeight> {
    if let Ok(block_height) = redis::cmd("GET")
        .arg("latest_block_height")
        .query_async(&mut redis_client.clone())
        .await
    {
        Some(block_height)
    } else {
        None
    }
}

pub async fn fetch_block_hash_from_redis(
    redis_client: redis::aio::ConnectionManager,
    account_id: &near_indexer_primitives::types::AccountId,
    block_height: near_indexer_primitives::types::BlockHeight,
) -> Option<Vec<u8>> {
    let block_redis_key = build_redis_block_hash_key(ACCOUNT_SCOPE, account_id);
    let blocks_hashes = if let Ok(blocks_hashes) = redis::cmd("ZREVRANGEBYSCORE")
        .arg(&block_redis_key)
        .arg(&[&block_height.to_string(), "-inf", "LIMIT", "0", "1"])
        .query_async(&mut redis_client.clone())
        .await
    {
        blocks_hashes
    } else {
        let result: Vec<Vec<u8>> = Vec::new();
        result
    };

    if blocks_hashes.is_empty() {
        None
    } else {
        Some(blocks_hashes[0].clone())
    }
}

pub async fn fetch_account_from_redis(
    redis_client: redis::aio::ConnectionManager,
    account_id: &near_indexer_primitives::types::AccountId,
    block_height: near_indexer_primitives::types::BlockHeight,
) -> Option<near_primitives_core::account::Account> {
    let account_from_redis = if let Some(block_hash) =
        fetch_block_hash_from_redis(redis_client.clone(), account_id, block_height).await
    {
        let data_redis_key = build_redis_data_key(ACCOUNT_SCOPE, account_id, block_hash);
        if let Ok(account_from_redis) = redis::cmd("GET")
            .arg(&data_redis_key)
            .query_async(&mut redis_client.clone())
            .await
        {
            account_from_redis
        } else {
            let result: Vec<u8> = Vec::new();
            result
        }
    } else {
        let result: Vec<u8> = Vec::new();
        result
    };

    return if account_from_redis.is_empty() {
        None
    } else {
        if let Ok(account) =
            near_primitives_core::account::Account::try_from_slice(&account_from_redis)
        {
            Some(account)
        } else {
            None
        }
    };
}
