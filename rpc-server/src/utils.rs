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

    let block_response = near_rpc_client.call(block_request_method).await?;

    // Updating the metric to expose the block height considered as final by the server
    // this metric can be used to calculate the lag between the server and the network
    // Prometheus Gauge Metric type do not support u64
    // https://github.com/tikv/rust-prometheus/issues/470
    crate::metrics::FINAL_BLOCK_HEIGHT.set(i64::try_from(block_response.header.height)?);
    Ok(block_response)
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
            chunks_included: block_view.header.chunks_included,
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
///
/// In case of a successful comparison, the function returns `Ok(())`.
/// Otherwise, it returns `Err(ShadowDataConsistencyError)`.
#[cfg(feature = "shadow_data_consistency")]
pub async fn shadow_compare_results<M>(
    read_rpc_response: Result<serde_json::Value, serde_json::Error>,
    client: near_jsonrpc_client::JsonRpcClient,
    params: M,
    read_rpc_response_is_ok: bool,
) -> Result<(), ShadowDataConsistencyError>
where
    M: near_jsonrpc_client::methods::RpcMethod + std::fmt::Debug,
    <M as near_jsonrpc_client::methods::RpcMethod>::Response: serde::ser::Serialize,
    <M as near_jsonrpc_client::methods::RpcMethod>::Error: std::fmt::Debug + serde::ser::Serialize,
{
    tracing::debug!(target: "shadow_data_consistency", "Compare results. {:?}", params);
    let read_rpc_response_json = match read_rpc_response {
        Ok(read_rpc_response_json) => read_rpc_response_json,
        Err(err) => {
            return Err(ShadowDataConsistencyError::ReadRpcResponseParseError(err));
        }
    };

    let mut near_rpc_response = client.call(&params).await;

    for _ in 0..crate::config::DEFAULT_RETRY_COUNT {
        if let Err(json_rpc_err) = &near_rpc_response {
            let retry = match json_rpc_err {
                near_jsonrpc_client::errors::JsonRpcError::TransportError(_) => true,
                near_jsonrpc_client::errors::JsonRpcError::ServerError(server_error) => {
                    match server_error {
                        near_jsonrpc_client::errors::JsonRpcServerError::NonContextualError(_)
                        | near_jsonrpc_client::errors::JsonRpcServerError::ResponseStatusError(_) => {
                            true
                        }
                        _ => false,
                    }
                }
            };
            if retry {
                near_rpc_response = client.call(&params).await;
            } else {
                break;
            }
        } else {
            break;
        }
    }

    let (near_rpc_response_json, near_rpc_response_is_ok) = match near_rpc_response {
        Ok(result) => match serde_json::to_value(&result) {
            Ok(near_rpc_response_json) => (near_rpc_response_json, true),
            Err(err) => {
                return Err(ShadowDataConsistencyError::NearRpcResponseParseError(err));
            }
        },
        Err(err) => {
            if let Some(e) = err.handler_error() {
                match serde_json::to_value(&e) {
                    Ok(near_rpc_response_json) => (near_rpc_response_json, false),
                    Err(err) => {
                        return Err(ShadowDataConsistencyError::NearRpcResponseParseError(err));
                    }
                }
            } else {
                return Err(ShadowDataConsistencyError::NearRpcCallError(format!(
                    "{:?}",
                    err
                )));
            }
        }
    };

    let config = Config::new(CompareMode::Strict).numeric_mode(NumericMode::AssumeFloat);

    // Sorts the values of the JSON objects before comparing them.
    let read_rpc_json = json_sort_value(read_rpc_response_json);
    let near_rpc_json = json_sort_value(near_rpc_response_json);

    if let Err(err) = assert_json_matches_no_panic(&read_rpc_json, &near_rpc_json, config) {
        // separate mismatching successful and failure responses into different targets
        // to make it easier to find reasons of the mismatching
        // let reason = String::new();
        // let description = String::new();
        let (reason, description) = if read_rpc_response_is_ok && near_rpc_response_is_ok {
            // Both services(read_rpc and near_rpc) have a successful response
            // if data mismatch we are logging into `shadow_data_consistency_successful_responses`
            // both response objects for future investigation.
            (
                "shadow_data_consistency_successful_responses",
                format!(
                    "Shadow data check: DATA MISMATCH\n READ_RPC_DATA: {:?}\n NEAR_RPC_DATA: {:?}\n",
                    read_rpc_json,
                    near_rpc_json,
                )
            )
        } else if !read_rpc_response_is_ok && near_rpc_response_is_ok {
            // read_rpc service has failure response
            // and near_rpc has successful response we are logging into
            // `shadow_data_consistency_read_rpc_failure_response` just message about mismatch
            // data without objects.
            // In the future we should be investigate why we got error in the read_rpc.
            (
                "shadow_data_consistency_read_rpc_failure_response",
                String::from(
                    "Shadow data check: read_rpc response failed, near_rpc response success",
                ),
            )
        } else if read_rpc_response_is_ok && !near_rpc_response_is_ok {
            // read_rpc service has successful response
            // and near_rpc has failure response we are logging into
            // `shadow_data_consistency_near_failure_response` just message about mismatch
            // data without objects. Expected that all error will be related with network issues.
            (
                "shadow_data_consistency_near_failure_response",
                String::from(
                    "Shadow data check: read_rpc response success, near_rpc response failed",
                ),
            )
        } else {
            // Both services(read_rpc and near_rpc) have a failure response
            // we are logging into `shadow_data_consistency_failure_responses`
            // both response objects for future investigation.
            // expected we will only have a difference in the error text.
            (
                "shadow_data_consistency_failure_responses",
                format!(
                    "Shadow data check: DATA MISMATCH\n READ_RPC_DATA: {:?}\n NEAR_RPC_DATA: {:?}\n",
                    read_rpc_json,
                    near_rpc_json,
                ),
            )
        };
        return Err(ShadowDataConsistencyError::ResultsDontMatch(format!(
            "{err}. Reason: {reason}. Description {description}"
        )));
    };
    Ok(())
}

/// Represents the error that can occur during the shadow data consistency check.
#[cfg(feature = "shadow_data_consistency")]
#[derive(thiserror::Error, Debug)]
pub enum ShadowDataConsistencyError {
    #[error("Failed to parse ReadRPC response: {0}")]
    ReadRpcResponseParseError(serde_json::Error),
    #[error("Failed to parse NEAR JSON RPC response: {0}")]
    NearRpcResponseParseError(serde_json::Error),
    #[error("NEAR RPC call error: {0}")]
    NearRpcCallError(String),
    #[error("Results don't match: {0}")]
    ResultsDontMatch(String),
}

/// Sort json value
///
/// 1. sort object key
/// 2. sort array
#[cfg(feature = "shadow_data_consistency")]
fn json_sort_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(array) => {
            let new_value: Vec<serde_json::Value> = array
                .into_iter()
                .enumerate()
                .fold(
                    std::collections::BTreeMap::new(),
                    |mut map, (index, val)| {
                        let sorted_value = json_sort_value(val);
                        let key = format!("{}_{}", generate_array_key(&sorted_value), index);
                        map.insert(key, sorted_value);
                        map
                    },
                )
                .into_iter()
                .map(|(_, v)| v)
                .collect();
            serde_json::Value::from(new_value)
        }
        serde_json::Value::Object(obj) => {
            let new_obj = obj
                .into_iter()
                .fold(serde_json::Map::new(), |mut map, (k, v)| {
                    map.insert(k, json_sort_value(v));
                    map
                });
            serde_json::Value::from(new_obj)
        }
        _ => value,
    }
}

/// Generate array key for sorting
#[cfg(feature = "shadow_data_consistency")]
fn generate_array_key(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "__null__".to_string(),
        serde_json::Value::Bool(bool_val) => {
            if *bool_val {
                "__true__".to_string()
            } else {
                "__false__".to_string()
            }
        }
        serde_json::Value::Number(num) => num.to_string(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Array(arr) => arr.iter().fold(String::new(), |str_key, arr_val| {
            str_key + &generate_array_key(arr_val)
        }),
        serde_json::Value::Object(obj) => obj.iter().fold(String::new(), |str_key, (key, val)| {
            format!("{}/{}:{}", str_key, key, val)
        }),
    }
}
