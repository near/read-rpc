use crate::modules::blocks::CacheBlock;
#[cfg(feature = "shadow_data_consistency")]
use assert_json_diff::{assert_json_matches_no_panic, CompareMode, Config, NumericMode};

pub async fn prepare_s3_client(
    access_key_id: &str,
    secret_access_key: &str,
    region: String,
) -> aws_sdk_s3::Client {
    let credentials =
        aws_credential_types::Credentials::new(access_key_id, secret_access_key, None, None, "");
    let s3_config = aws_sdk_s3::Config::builder()
        .credentials_provider(credentials)
        .region(aws_sdk_s3::Region::new(region))
        .build();
    aws_sdk_s3::Client::from_conf(s3_config)
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

/// The `shadow_compare_results` is a function that compares
/// the results of a JSON-RPC call made using the `near_jsonrpc_client` library
/// with a given `readrpc_response_json` object representing the expected results.
/// This function is conditionally compiled using the `cfg` attribute
/// with the `shadow_data_consistency` feature.
///
/// The function takes three arguments:
///
/// `readrpc_response`: a `Result<serde_json::Value, serde_json::Error>` object representing the results from Read RPC.
/// `client`: `near_jsonrpc_client::JsonRpcClient`.
/// `params`: `near_jsonrpc_client::methods::RpcMethod` trait.
#[cfg(feature = "shadow_data_consistency")]
pub async fn shadow_compare_results<M>(
    readrpc_response: Result<serde_json::Value, serde_json::Error>,
    client: near_jsonrpc_client::JsonRpcClient,
    params: M,
) where
    M: near_jsonrpc_client::methods::RpcMethod + std::fmt::Debug,
    <M as near_jsonrpc_client::methods::RpcMethod>::Response: serde::ser::Serialize,
    <M as near_jsonrpc_client::methods::RpcMethod>::Error: std::fmt::Debug,
{
    tracing::debug!("Compare results. {:?}", params);

    let readrpc_response_json = match readrpc_response {
        Ok(readrpc_response_json) => readrpc_response_json,
        Err(err) => {
            tracing::error!(target: "is_not_consistency", "Parse hundredx response error: {:#?}", err);
            return;
        }
    };

    let near_rpc_response_json = match client.call(params).await {
        Ok(near_rpc_response) => match serde_json::to_value(&near_rpc_response) {
            Ok(near_rpc_response_json) => near_rpc_response_json,
            Err(err) => {
                tracing::error!(target: "is_not_consistency", "Parse PRC response error: {:#?}", err);
                return;
            }
        },
        Err(err) => {
            tracing::error!(target: "is_not_consistency", "RPC call error: {:#?}", err);
            return;
        }
    };

    let config = Config::new(CompareMode::Strict).numeric_mode(NumericMode::AssumeFloat);

    if let Err(err) =
        assert_json_matches_no_panic(&readrpc_response_json, &near_rpc_response_json, config)
    {
        tracing::error!(target: "is_not_consistency", "The results don't match: {:#?}", err);
    };
}
