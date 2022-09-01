use bigdecimal::ToPrimitive;

pub async fn fetch_block_from_s3(
    s3_client: &aws_sdk_s3::Client,
    s3_bucket_name: &str,
    block_height: near_primitives::types::BlockHeight,
) -> Result<
    near_indexer_primitives::views::BlockView,
    near_jsonrpc_primitives::types::blocks::RpcBlockError,
> {
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
            match serde_json::from_slice::<near_indexer_primitives::views::BlockView>(
                body_bytes.as_ref(),
            ) {
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

pub async fn fetch_latest_block_height_from_redis(
    redis_client: redis::aio::ConnectionManager,
) -> anyhow::Result<near_indexer_primitives::types::BlockHeight> {
    tracing::debug!(target: "jsonrpc - block", "call fetch_latest_block_height_from_redis");
    Ok(redis::cmd("GET")
        .arg("latest_block_height")
        .query_async(&mut redis_client.clone())
        .await?)
}
