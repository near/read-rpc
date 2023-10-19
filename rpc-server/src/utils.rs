use crate::modules::blocks::CacheBlock;
#[cfg(feature = "shadow_data_consistency")]
use assert_json_diff::{assert_json_matches_no_panic, CompareMode, Config, NumericMode};
use futures::StreamExt;
use sysinfo::{System, SystemExt};

#[cfg(feature = "shadow_data_consistency")]
const DEFAULT_RETRY_COUNT: u8 = 3;

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

pub async fn get_final_cache_block(
    near_rpc_client: &near_jsonrpc_client::JsonRpcClient,
) -> Option<CacheBlock> {
    let block_request_method = near_jsonrpc_client::methods::block::RpcBlockRequest {
        block_reference: near_primitives::types::BlockReference::Finality(
            near_primitives::types::Finality::Final,
        ),
    };
    match near_rpc_client.call(block_request_method).await {
        Ok(block_view) => {
            // Updating the metric to expose the block height considered as final by the server
            // this metric can be used to calculate the lag between the server and the network
            // Prometheus Gauge Metric type do not support u64
            // https://github.com/tikv/rust-prometheus/issues/470
            crate::metrics::FINAL_BLOCK_HEIGHT
                .set(i64::try_from(block_view.header.height).unwrap());

            Some(CacheBlock {
                block_hash: block_view.header.hash,
                block_height: block_view.header.height,
                block_timestamp: block_view.header.timestamp,
                latest_protocol_version: block_view.header.latest_protocol_version,
                chunks_included: block_view.header.chunks_included,
                state_root: block_view.header.prev_state_root,
            })
        }
        Err(_) => None,
    }
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
async fn handle_streamer_message(
    streamer_message: near_indexer_primitives::StreamerMessage,
    blocks_cache: std::sync::Arc<std::sync::RwLock<crate::cache::LruMemoryCache<u64, CacheBlock>>>,
    final_block_height: std::sync::Arc<std::sync::atomic::AtomicU64>,
) -> anyhow::Result<()> {
    let block = CacheBlock {
        block_hash: streamer_message.block.header.hash,
        block_height: streamer_message.block.header.height,
        block_timestamp: streamer_message.block.header.timestamp,
        latest_protocol_version: streamer_message.block.header.latest_protocol_version,
        chunks_included: streamer_message.block.header.chunks_included,
        state_root: streamer_message.block.header.prev_state_root,
    };
    final_block_height.store(block.block_height, std::sync::atomic::Ordering::SeqCst);
    blocks_cache.write().unwrap().put(block.block_height, block);
    crate::metrics::FINAL_BLOCK_HEIGHT.set(i64::try_from(block.block_height)?);
    Ok(())
}

pub async fn update_final_block_height_regularly(
    final_block_height: std::sync::Arc<std::sync::atomic::AtomicU64>,
    blocks_cache: std::sync::Arc<std::sync::RwLock<crate::cache::LruMemoryCache<u64, CacheBlock>>>,
    lake_config: near_lake_framework::LakeConfig,
) -> anyhow::Result<()> {
    tracing::info!("Task to get and store final block in the cache started");
    let (sender, stream) = near_lake_framework::streamer(lake_config);
    let mut handlers = tokio_stream::wrappers::ReceiverStream::new(stream)
        .map(|streamer_message| {
            handle_streamer_message(
                streamer_message,
                std::sync::Arc::clone(&blocks_cache),
                std::sync::Arc::clone(&final_block_height),
            )
        })
        .buffer_unordered(1usize);

    while let Some(_handle_message) = handlers.next().await {
        if let Err(err) = _handle_message {
            tracing::warn!("{:?}", err);
        }
    }
    drop(handlers); // close the channel so the sender will stop

    // propagate errors from the sender
    match sender.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(e) => Err(anyhow::Error::from(e)), // JoinError
    }
}

/// Calculate the cache size based on the available memory.
/// For caching we use the limit or if it is not set then all available memory.
/// We divide the memory equally between the 3 caches: blocks, compiled_contracts, contract_code.
/// If the installed limit exceeds the size of the available memory, we get a panic.
pub(crate) async fn calculate_contract_code_cache_sizes(
    reserved_memory: usize,
    block_cache_size: usize,
    limit_memory_cache: Option<usize>,
) -> usize {
    let sys = System::new_all();
    let total_memory = sys.total_memory() as usize; // Total memory in bytes
    let used_memory = sys.used_memory() as usize; // Used memory in bytes
    let available_memory = total_memory - used_memory - reserved_memory; // Available memory in bytes

    let mem_cache_size = if let Some(limit) = limit_memory_cache {
        if limit >= available_memory {
            panic!(
                "Not enough memory to run the server. Available memory: {}, required memory: {}",
                crate::modules::network::friendly_memory_size_format(available_memory),
                crate::modules::network::friendly_memory_size_format(limit),
            );
        } else {
            limit
        }
    } else {
        available_memory
    };

    (mem_cache_size - block_cache_size) / 2 // divide on 2 because we have 2 caches: compiled_contracts and contract_code
}

/// Convert gigabytes to bytes
pub(crate) async fn gigabytes_to_bytes(gigabytes: f64) -> usize {
    (gigabytes * 1024.0 * 1024.0 * 1024.0) as usize
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

    for _ in 0..DEFAULT_RETRY_COUNT {
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
                    Ok(near_rpc_response_json) => {
                        if near_rpc_response_json["name"] == "TIMEOUT_ERROR" {
                            return Err(ShadowDataConsistencyError::NearRpcCallError(format!(
                                "{:?}",
                                err
                            )));
                        }
                        (near_rpc_response_json, false)
                    }
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

    if let Err(_err) = assert_json_matches_no_panic(&read_rpc_json, &near_rpc_json, config) {
        // separate mismatching successful and failure responses into different targets
        // to make it easier to find reasons of the mismatching
        let results_dont_match_error = if read_rpc_response_is_ok && near_rpc_response_is_ok {
            // Both services(read_rpc and near_rpc) have a successful response but the data mismatch
            // both response objects included for future investigation
            ShadowDataConsistencyError::ResultsDontMatch {
                error_message: format!("Success results don't match"),
                reason: DataMismatchReason::ReadRpcSuccessNearRpcSuccess,
                read_rpc_response: read_rpc_json,
                near_rpc_response: near_rpc_json,
            }
        } else if !read_rpc_response_is_ok && near_rpc_response_is_ok {
            // read_rpc service has error response and near_rpc has successful response
            ShadowDataConsistencyError::ResultsDontMatch {
                error_message: format!("ReadRPC failed, NearRPC success"),
                reason: DataMismatchReason::ReadRpcErrorNearRpcSuccess,
                read_rpc_response: read_rpc_json,
                near_rpc_response: near_rpc_json,
            }
        } else if read_rpc_response_is_ok && !near_rpc_response_is_ok {
            // read_rpc service has successful response and near_rpc has error response
            // Expected that all error will be related with network issues.
            ShadowDataConsistencyError::ResultsDontMatch {
                error_message: format!("ReadRPC success, NearRPC failed"),
                reason: DataMismatchReason::ReadRpcSuccessNearRpcError,
                read_rpc_response: read_rpc_json,
                near_rpc_response: near_rpc_json,
            }
        } else {
            // Both services(read_rpc and near_rpc) have an error response
            // both response objects included for future investigation.
            // Expected we will only have a difference in the error text.
            ShadowDataConsistencyError::ResultsDontMatch {
                error_message: format!("Both services failed, but results don't match"),
                reason: DataMismatchReason::ReadRpcErrorNearRpcError,
                read_rpc_response: read_rpc_json,
                near_rpc_response: near_rpc_json,
            }
        };
        return Err(results_dont_match_error);
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
    #[error("Results don't match: {error_message}")]
    ResultsDontMatch {
        error_message: String,
        reason: DataMismatchReason,
        read_rpc_response: serde_json::Value,
        near_rpc_response: serde_json::Value,
    },
}

/// This enum is used to track the mismatch between the data returned by the READ RPC server and
/// the data returned by the NEAR RPC server. The mismatch can be caused by a limited number of
/// reasons, and this enum is used to track them.
#[cfg(feature = "shadow_data_consistency")]
#[derive(Debug)]
pub enum DataMismatchReason {
    /// ReadRPC returns success result and NEAR RPC returns success result but the results mismatch
    ReadRpcSuccessNearRpcSuccess,
    /// ReadRPC returns success result and NEAR RPC returns error result
    ReadRpcSuccessNearRpcError,
    /// ReadRPC returns error result and NEAR RPC returns success result
    ReadRpcErrorNearRpcSuccess,
    /// ReadRPC returns error result and NEAR RPC returns error result but the results mismatch
    ReadRpcErrorNearRpcError,
}

#[cfg(feature = "shadow_data_consistency")]
impl DataMismatchReason {
    /// This method converts the reason into a number from 0 to 3. These numbers are used in the
    /// metrics like BLOCK_ERROR_0, BLOCK_ERROR_1, BLOCK_ERROR_2, BLOCK_ERROR_3 etc.
    pub fn code(&self) -> usize {
        match self {
            DataMismatchReason::ReadRpcSuccessNearRpcSuccess => 0,
            DataMismatchReason::ReadRpcSuccessNearRpcError => 1,
            DataMismatchReason::ReadRpcErrorNearRpcSuccess => 2,
            DataMismatchReason::ReadRpcErrorNearRpcError => 3,
        }
    }

    /// This method converts the reason into a string. These strings are used in the logs to include the
    /// human readable reason for the mismatch.
    pub fn reason(&self) -> &'static str {
        match self {
            DataMismatchReason::ReadRpcSuccessNearRpcSuccess => {
                "Read RPC success, and NEAR RPC success"
            }
            DataMismatchReason::ReadRpcSuccessNearRpcError => {
                "Read RPC success, but NEAR RPC error"
            }
            DataMismatchReason::ReadRpcErrorNearRpcSuccess => {
                "Read RPC error, but NEAR RPC success"
            }
            DataMismatchReason::ReadRpcErrorNearRpcError => "Read RPC error, and NEAR RPC error",
        }
    }
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
        serde_json::Value::Object(mut obj) => {
            // When comparing the response for methods `tx` and `EXPERIMENTAL_tx_status` the response
            // contains the field `receipts_outcome.outcome.metadata` which is not backward compatible
            // on the `nearcore` side. Different NEAR RPC nodes return different values for this field.
            // We don't want it to affect the comparison of the responses, so we remove it on the fly.
            // This field not expected to be present in other responses.
            obj.remove("metadata");
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

#[cfg(feature = "shadow_data_consistency")]
macro_rules! capture_shadow_consistency_error {
    ($err:ident, $meta_data:ident, $method_metric_name:expr) => {
        match $err {
            crate::utils::ShadowDataConsistencyError::ResultsDontMatch {
                reason,
                read_rpc_response,
                near_rpc_response,
                ..
            } => {
                tracing::warn!(
                    target: "shadow_data_consistency",
                    "Shadow data check: ERROR\n{}:{}: {}\n{}",
                    $method_metric_name,
                    reason.code(),
                    $meta_data,
                    format!("{}, ReadRPC: {:?}, NearRPC: {:?}", reason.reason(), read_rpc_response, near_rpc_response),
                );
                match reason.code() {
                    0 => {
                        paste::paste!{
                            crate::metrics::[<$method_metric_name _ERROR_0>].inc();
                        }
                    },
                    1 => {
                        paste::paste!{
                            crate::metrics::[<$method_metric_name _ERROR_1>].inc();
                        }
                    },
                    2 => {
                        paste::paste!{
                            crate::metrics::[<$method_metric_name _ERROR_2>].inc();
                        }
                    },
                    3 => {
                        paste::paste!{
                            crate::metrics::[<$method_metric_name _ERROR_3>].inc();
                        }
                    },
                    _ => panic!("Received unexpected reason code: {}", reason.code()),
                };
            },
            _ => {
                tracing::warn!(target: "shadow_data_consistency", "Shadow data check: ERROR\n{}: {}\n{:?}", $method_metric_name, $meta_data, $err);
                paste::paste!{
                    crate::metrics::[<$method_metric_name _ERROR_4>].inc();
                }
            }
        }
    };
}
#[cfg(feature = "shadow_data_consistency")]
pub(crate) use capture_shadow_consistency_error;
