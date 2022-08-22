use crate::modules::queries::{
    build_redis_block_hash_key, build_redis_data_key, ACCESS_KEY_SCOPE, ACCOUNT_SCOPE, CODE_SCOPE,
};
use borsh::BorshDeserialize;

pub async fn fetch_block_hash_from_redis(
    scope: &[u8],
    redis_client: redis::aio::ConnectionManager,
    account_id: &near_indexer_primitives::types::AccountId,
    key_data: Option<&[u8]>,
    block_height: near_indexer_primitives::types::BlockHeight,
) -> Option<Vec<u8>> {
    let block_redis_key = build_redis_block_hash_key(scope, account_id, key_data);
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

async fn fetch_data_from_redis(
    scope: &[u8],
    redis_client: redis::aio::ConnectionManager,
    account_id: &near_indexer_primitives::types::AccountId,
    block_height: near_indexer_primitives::types::BlockHeight,
    key_data: Option<&[u8]>,
) -> Vec<u8> {
    if let Some(block_hash) = fetch_block_hash_from_redis(
        scope,
        redis_client.clone(),
        account_id,
        key_data,
        block_height,
    )
    .await
    {
        let data_redis_key = build_redis_data_key(scope, account_id, block_hash, key_data);
        if let Ok(data_from_redis) = redis::cmd("GET")
            .arg(&data_redis_key)
            .query_async(&mut redis_client.clone())
            .await
        {
            data_from_redis
        } else {
            Vec::<u8>::new()
        }
    } else {
        Vec::<u8>::new()
    }
}

pub async fn fetch_access_key_from_redis(
    redis_client: redis::aio::ConnectionManager,
    account_id: &near_indexer_primitives::types::AccountId,
    block_height: near_indexer_primitives::types::BlockHeight,
    key_data: Vec<u8>,
) -> anyhow::Result<near_primitives_core::account::AccessKey> {
    let access_key_from_redis = fetch_data_from_redis(
        ACCESS_KEY_SCOPE,
        redis_client,
        account_id,
        block_height,
        Some(&key_data),
    )
    .await;
    Ok(near_primitives_core::account::AccessKey::try_from_slice(
        &access_key_from_redis,
    )?)
}

pub async fn fetch_code_from_redis(
    redis_client: redis::aio::ConnectionManager,
    account_id: &near_indexer_primitives::types::AccountId,
    block_height: near_indexer_primitives::types::BlockHeight,
) -> anyhow::Result<near_primitives_core::contract::ContractCode> {
    let code_data_from_redis =
        fetch_data_from_redis(CODE_SCOPE, redis_client, account_id, block_height, None).await;
    if code_data_from_redis.is_empty() {
        anyhow::bail!("Data not found in redis")
    } else {
        Ok(near_primitives_core::contract::ContractCode::new(
            code_data_from_redis,
            None,
        ))
    }
}

pub async fn fetch_account_from_redis(
    redis_client: redis::aio::ConnectionManager,
    account_id: &near_indexer_primitives::types::AccountId,
    block_height: near_indexer_primitives::types::BlockHeight,
) -> anyhow::Result<near_primitives_core::account::Account> {
    let account_from_redis =
        fetch_data_from_redis(ACCOUNT_SCOPE, redis_client, account_id, block_height, None).await;
    Ok(near_primitives_core::account::Account::try_from_slice(
        &account_from_redis,
    )?)
}
