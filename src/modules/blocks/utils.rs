use crate::config::ServerContext;
use crate::modules::blocks::methods::fetch_block;
use crate::modules::blocks::CacheBlock;
use bigdecimal::ToPrimitive;
use std::ops::Deref;

// #[tracing::instrument(skip(s3_client))]
pub async fn fetch_block_from_s3(
    s3_client: &aws_sdk_s3::Client,
    s3_bucket_name: &str,
    block_height: near_primitives::types::BlockHeight,
) -> Result<near_primitives::views::BlockView, near_jsonrpc_primitives::types::blocks::RpcBlockError>
{
    tracing::debug!(target: "jsonrpc - block", "call fetch_block_from_s3");
    match s3_client
        .get_object()
        .bucket(s3_bucket_name)
        .key(format!("{:0>12}/block.json", block_height))
        .send()
        .await
    {
        Ok(response) => {
            let body_bytes = response.body.collect().await.unwrap().into_bytes();
            match serde_json::from_slice::<near_primitives::views::BlockView>(body_bytes.as_ref()) {
                Ok(block) => Ok(block),
                Err(err) => Err(
                    near_jsonrpc_primitives::types::blocks::RpcBlockError::UnknownBlock {
                        error_message: err.to_string(),
                    },
                ),
            }
        }
        Err(err) => Err(
            near_jsonrpc_primitives::types::blocks::RpcBlockError::UnknownBlock {
                error_message: err.to_string(),
            },
        ),
    }
}

// #[tracing::instrument(skip(db_client))]
pub async fn fetch_block_height_from_db(
    db_client: &sqlx::PgPool,
    block_hash: near_primitives::hash::CryptoHash,
) -> Result<u64, near_jsonrpc_primitives::types::blocks::RpcBlockError> {
    tracing::debug!(target: "jsonrpc - block", "call fetch_block_height_from_db");
    match sqlx::query!(
        "SELECT block_height FROM blocks WHERE block_hash = $1 LIMIT 1",
        block_hash.to_string()
    )
    .fetch_one(db_client)
    .await
    {
        Ok(row) => Ok(row.block_height.to_u64().unwrap()),
        Err(err) => Err(
            near_jsonrpc_primitives::types::blocks::RpcBlockError::UnknownBlock {
                error_message: err.to_string(),
            },
        ),
    }
}

// #[tracing::instrument(skip(db_client))]
pub async fn fetch_latest_block_height_from_db(
    db_client: &sqlx::PgPool,
) -> Result<u64, near_jsonrpc_primitives::types::blocks::RpcBlockError> {
    tracing::debug!(target: "jsonrpc - block", "call fetch_latest_block_height_from_db");
    match sqlx::query!("SELECT block_height FROM blocks ORDER BY block_height DESC LIMIT 1",)
        .fetch_one(db_client)
        .await
    {
        Ok(row) => Ok(row.block_height.to_u64().unwrap()),
        Err(err) => Err(
            near_jsonrpc_primitives::types::blocks::RpcBlockError::UnknownBlock {
                error_message: err.to_string(),
            },
        ),
    }
}

// #[tracing::instrument(skip(redis_client))]
pub async fn fetch_latest_block_height_from_redis(
    redis_client: redis::aio::ConnectionManager,
) -> anyhow::Result<near_primitives::types::BlockHeight> {
    tracing::debug!(target: "jsonrpc - block", "call fetch_latest_block_height_from_redis");
    Ok(redis::cmd("GET")
        .arg("latest_block_height")
        .query_async(&mut redis_client.clone())
        .await?)
}

// #[tracing::instrument(skip(data))]
pub async fn fetch_block_from_cache_or_get(
    data: &jsonrpc_v2::Data<ServerContext>,
    block_reference: near_primitives::types::BlockReference,
) -> CacheBlock {
    let block = match block_reference.clone() {
        near_primitives::types::BlockReference::BlockId(block_id) => match block_id {
            near_primitives::types::BlockId::Height(block_height) => {
                data.blocks_cache.get(&block_height)
            }
            near_primitives::types::BlockId::Hash(_) => None,
        },
        near_primitives::types::BlockReference::Finality(_) => {
            // Returns the final_block_height for all the finalities.
            let block_height = &data
                .final_block_height
                .load(std::sync::atomic::Ordering::SeqCst);
            data.blocks_cache.get(block_height)
        }
        // TODO: return the height of the first block height from S3 (cache it once on the start)
        near_primitives::types::BlockReference::SyncCheckpoint(_) => None,
    };
    match block {
        Some(block) => *block.deref(),
        None => {
            let block_from_s3 = fetch_block(data, block_reference)
                .await
                .expect("Error to fetch block");
            let block = CacheBlock {
                block_hash: block_from_s3.header.hash,
                block_height: block_from_s3.header.height,
                block_timestamp: block_from_s3.header.timestamp,
                latest_protocol_version: block_from_s3.header.latest_protocol_version,
            };

            data.blocks_cache.insert(block_from_s3.header.height, block);
            block
        }
    }
}
