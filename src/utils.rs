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

pub async fn prepare_scylla_db_client(
    scylla_url: &str,
    scylla_keyspace: &str,
    scylla_user: Option<&str>,
    scylla_password: Option<&str>,
) -> anyhow::Result<scylla::Session> {
    let mut session: scylla::SessionBuilder = scylla::SessionBuilder::new().known_node(scylla_url);
    if let Some(user) = scylla_user {
        if let Some(password) = scylla_password {
            session = session.user(user, password);
        }
    }
    session = session.use_keyspace(scylla_keyspace, false);
    Ok(session.build().await?)
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(params)))]
pub async fn proxy_rpc_call<M>(
    client: &near_jsonrpc_client::JsonRpcClient,
    params: M,
) -> near_jsonrpc_client::MethodCallResult<M::Response, M::Error>
where
    M: near_jsonrpc_client::methods::RpcMethod + std::fmt::Debug,
{
    tracing::debug!("PROXY call. {:?}", params);
    client.call(params).await
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
    blocks_cache: std::sync::Arc<std::sync::RwLock<lru::LruCache<u64, CacheBlock>>>,
    near_rpc_client: near_jsonrpc_client::JsonRpcClient,
    shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>,
) {
    tracing::info!("Task to get and store final block in the cache started");
    loop {
        match get_final_cache_block(&near_rpc_client).await {
            Some(block) => {
                final_block_height.store(block.block_height, std::sync::atomic::Ordering::SeqCst);
                blocks_cache.write().unwrap().put(block.block_height, block);
            }
            None => tracing::warn!("Error to get final block!"),
        };
        std::thread::sleep(std::time::Duration::from_secs(1));
        if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }
    }
}
