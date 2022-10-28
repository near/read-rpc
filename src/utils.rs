use crate::modules::blocks::CacheBlock;

pub async fn prepare_s3_client(
    access_key_id: &str,
    secret_access_key: &str,
    region: String,
) -> aws_sdk_s3::Client {
    let credentials = aws_types::Credentials::new(access_key_id, secret_access_key, None, None, "");
    let s3_config = aws_sdk_s3::Config::builder()
        .credentials_provider(credentials)
        .region(aws_sdk_s3::Region::new(region))
        .build();
    aws_sdk_s3::Client::from_conf(s3_config)
}

pub async fn prepare_db_client(database_url: &str) -> sqlx::PgPool {
    sqlx::PgPool::connect(database_url).await.unwrap()
}

pub async fn prepare_redis_client(redis_url: &str) -> redis::aio::ConnectionManager {
    redis::Client::open(redis_url)
        .expect("can create Redis client")
        .get_tokio_connection_manager()
        .await
        .unwrap()
}

async fn get_final_block(
    near_rpc_client: &near_jsonrpc_client::JsonRpcClient,
) -> anyhow::Result<near_jsonrpc_client::methods::block::RpcBlockResponse> {
    let block_request_method = near_jsonrpc_client::methods::block::RpcBlockRequest {
        block_reference: near_primitives::types::BlockReference::Finality(
            near_primitives::types::Finality::Final,
        ),
    };
    Ok(near_rpc_client.call(block_request_method).await?)
}

pub async fn get_final_cache_block(
    near_rpc_client: &near_jsonrpc_client::JsonRpcClient,
) -> Option<CacheBlock> {
    match get_final_block(near_rpc_client).await {
        Ok(block_view) => Some(CacheBlock {
            block_hash: block_view.header.hash,
            block_height: block_view.header.height,
            block_timestamp: block_view.header.timestamp,
            latest_protocol_version: block_view.header.latest_protocol_version,
        }),
        Err(_) => None,
    }
}

pub async fn update_final_block_height_regularly(
    final_block_height: std::sync::Arc<std::sync::atomic::AtomicU64>,
    blocks_cache: std::sync::Arc<shared_lru::LruCache<u64, CacheBlock>>,
    near_rpc_client: near_jsonrpc_client::JsonRpcClient,
    shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>,
) {
    tracing::info!("Task to get and store final block in the cache started");
    loop {
        match get_final_cache_block(&near_rpc_client).await {
            Some(block) => {
                final_block_height.store(block.block_height, std::sync::atomic::Ordering::SeqCst);
                blocks_cache.insert(block.block_height, block);
            }
            None => tracing::warn!("Error to get final block!"),
        };
        std::thread::sleep(std::time::Duration::from_secs(1));
        if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }
    }
}
