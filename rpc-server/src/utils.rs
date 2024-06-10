use crate::modules::blocks::{BlockInfo, BlocksInfoByFinality, CacheBlock};
use crate::modules::network::epoch_config_from_protocol_config_view;
#[cfg(feature = "shadow_data_consistency")]
use assert_json_diff::{assert_json_matches_no_panic, CompareMode, Config, NumericMode};
use futures::StreamExt;

#[cfg(feature = "shadow_data_consistency")]
const DEFAULT_RETRY_COUNT: u8 = 3;

/// JsonRpcClient represents a client capable of interacting with NEAR JSON-RPC endpoints,
/// The client is capable of handling requests to both regular and archival nodes.
#[derive(Clone, Debug)]
pub struct JsonRpcClient {
    regular_client: near_jsonrpc_client::JsonRpcClient,
    archival_client: near_jsonrpc_client::JsonRpcClient,
}

impl JsonRpcClient {
    /// Creates a new JsonRpcClient.
    /// The client is capable of handling requests to both regular and archival nodes.
    /// If the `archival_rpc_url` is not provided, the client will use the regular endpoint for both
    pub fn new(rpc_url: String, archival_rpc_url: Option<String>) -> Self {
        let regular_client = near_jsonrpc_client::JsonRpcClient::connect(rpc_url);
        let archival_client = match archival_rpc_url {
            Some(archival_rpc_url) => near_jsonrpc_client::JsonRpcClient::connect(archival_rpc_url),
            None => regular_client.clone(),
        };
        Self {
            regular_client,
            archival_client,
        }
    }

    /// Adds a custom header to the RPC request.
    pub fn header(mut self, header_name: String, header_value: String) -> anyhow::Result<Self> {
        let header_name: &'static str = Box::leak(header_name.into_boxed_str());
        let header_value: &'static str = Box::leak(header_value.into_boxed_str());

        self.regular_client = self.regular_client.header((header_name, header_value))?;
        self.archival_client = self.archival_client.header((header_name, header_value))?;
        Ok(self)
    }

    /// Performs a RPC call to either the regular or archival endpoint.
    async fn rpc_call<M>(
        &self,
        params: M,
        is_archival: bool,
    ) -> near_jsonrpc_client::MethodCallResult<M::Response, M::Error>
    where
        M: near_jsonrpc_client::methods::RpcMethod + std::fmt::Debug,
    {
        if is_archival {
            self.archival_client.call(params).await
        } else {
            self.regular_client.call(params).await
        }
    }

    /// Performs a RPC call to the regular endpoint.
    pub async fn call<M>(
        &self,
        params: M,
        method_name: Option<&str>,
    ) -> near_jsonrpc_client::MethodCallResult<M::Response, M::Error>
    where
        M: near_jsonrpc_client::methods::RpcMethod + std::fmt::Debug,
    {
        tracing::debug!("PROXY call. {:?}", params);
        if let Some(method_name) = method_name {
            crate::metrics::TOTAL_REQUESTS_COUNTER
                .with_label_values(&[method_name, "proxy"])
                .inc();
        }
        self.rpc_call(params, false).await
    }

    /// Performs a RPC call to the archival endpoint.
    pub async fn archival_call<M>(
        &self,
        params: M,
        method_name: Option<&str>,
    ) -> near_jsonrpc_client::MethodCallResult<M::Response, M::Error>
    where
        M: near_jsonrpc_client::methods::RpcMethod + std::fmt::Debug,
    {
        tracing::debug!("ARCHIVAL PROXY call. {:?}", params);
        if let Some(method_name) = method_name {
            crate::metrics::TOTAL_REQUESTS_COUNTER
                .with_label_values(&[method_name, "archive_proxy"])
                .inc();
        }
        self.rpc_call(params, true).await
    }

    /// Performs a RPC call to the archival endpoint for shadow comparison results.
    #[cfg(feature = "shadow_data_consistency")]
    pub async fn shadow_comparison_call<M>(
        &self,
        params: M,
    ) -> near_jsonrpc_client::MethodCallResult<M::Response, M::Error>
    where
        M: near_jsonrpc_client::methods::RpcMethod + std::fmt::Debug,
    {
        tracing::debug!("SHADOW DATA CONSISTENCY PROXY call. {:?}", params);
        self.rpc_call(params, true).await
    }
}

pub async fn get_final_block(
    near_rpc_client: &JsonRpcClient,
    optimistic: bool,
) -> anyhow::Result<near_primitives::views::BlockView> {
    let block_request_method = near_jsonrpc_client::methods::block::RpcBlockRequest {
        block_reference: near_primitives::types::BlockReference::Finality(if optimistic {
            near_primitives::types::Finality::None
        } else {
            near_primitives::types::Finality::Final
        }),
    };
    let block_view = near_rpc_client.call(block_request_method, None).await?;

    // Updating the metric to expose the block height considered as final by the server
    // this metric can be used to calculate the lag between the server and the network
    // Prometheus Gauge Metric type do not support u64
    // https://github.com/tikv/rust-prometheus/issues/470
    if optimistic {
        // optimistic block height
        crate::metrics::LATEST_BLOCK_HEIGHT_BY_FINALITIY
            .with_label_values(&["optimistic"])
            .set(i64::try_from(block_view.header.height)?);
    } else {
        // final block height
        crate::metrics::LATEST_BLOCK_HEIGHT_BY_FINALITIY
            .with_label_values(&["final"])
            .set(i64::try_from(block_view.header.height)?);
    }
    Ok(block_view)
}

pub async fn get_current_validators(
    near_rpc_client: &JsonRpcClient,
) -> anyhow::Result<near_primitives::views::EpochValidatorInfo> {
    let params = near_jsonrpc_client::methods::validators::RpcValidatorRequest {
        epoch_reference: near_primitives::types::EpochReference::Latest,
    };
    Ok(near_rpc_client.call(params, None).await?)
}

pub async fn get_current_epoch_config(
    near_rpc_client: &JsonRpcClient,
) -> anyhow::Result<near_primitives::epoch_manager::EpochConfig> {
    let params =
        near_jsonrpc_client::methods::EXPERIMENTAL_protocol_config::RpcProtocolConfigRequest {
            block_reference: near_primitives::types::BlockReference::Finality(
                near_primitives::types::Finality::Final,
            ),
        };
    let protocol_config_view = near_rpc_client.call(params, None).await?;
    Ok(epoch_config_from_protocol_config_view(protocol_config_view).await)
}

async fn handle_streamer_message(
    streamer_message: near_indexer_primitives::StreamerMessage,
    blocks_cache: std::sync::Arc<crate::cache::RwLockLruMemoryCache<u64, CacheBlock>>,
    blocks_info_by_finality: std::sync::Arc<BlocksInfoByFinality>,
    near_rpc_client: &JsonRpcClient,
) -> anyhow::Result<()> {
    let block = BlockInfo::new_from_streamer_message(streamer_message).await;
    let block_cache = block.block_cache;

    if block_cache.block_height as i64
        > crate::metrics::LATEST_BLOCK_HEIGHT_BY_FINALITIY
            .with_label_values(&["final"])
            .get()
    {
        if blocks_info_by_finality.final_cache_block().await.epoch_id != block_cache.epoch_id {
            tracing::info!("New epoch started: {:?}", block_cache.epoch_id);
            blocks_info_by_finality
                .update_current_epoch_info(near_rpc_client)
                .await?;
        }

        blocks_info_by_finality.update_final_block(block).await;
        blocks_cache
            .put(block_cache.block_height, block_cache)
            .await;
        crate::metrics::LATEST_BLOCK_HEIGHT_BY_FINALITIY
            .with_label_values(&["final"])
            .set(i64::try_from(block_cache.block_height)?);
    }
    Ok(())
}

pub async fn update_final_block_regularly_from_lake(
    blocks_cache: std::sync::Arc<crate::cache::RwLockLruMemoryCache<u64, CacheBlock>>,
    blocks_info_by_finality: std::sync::Arc<BlocksInfoByFinality>,
    rpc_server_config: configuration::RpcServerConfig,
    near_rpc_client: JsonRpcClient,
) -> anyhow::Result<()> {
    tracing::info!("Task to get final block from lake and store in the cache started");
    let lake_config = rpc_server_config
        .lake_config
        .lake_config(
            blocks_info_by_finality
                .optimistic_cache_block()
                .await
                .block_height,
        )
        .await?;
    let (sender, stream) = near_lake_framework::streamer(lake_config);
    let mut handlers = tokio_stream::wrappers::ReceiverStream::new(stream)
        .map(|streamer_message| {
            handle_streamer_message(
                streamer_message,
                std::sync::Arc::clone(&blocks_cache),
                std::sync::Arc::clone(&blocks_info_by_finality),
                &near_rpc_client,
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

// Helper function to get the finality blocks streamer_message from the redis
pub(crate) async fn get_block_streamer_message(
    block_type: near_primitives::types::Finality,
    redis_client: redis::aio::ConnectionManager,
) -> anyhow::Result<near_indexer_primitives::StreamerMessage> {
    let block_type = serde_json::to_string(&block_type)?;
    let resp: String = redis::cmd("GET")
        .arg(block_type)
        .query_async(&mut redis_client.clone())
        .await?;
    Ok(serde_json::from_str(&resp)?)
}

// Task to get and store final block in the cache
// Subscribe to the redis channel and update the final block in the cache
pub async fn update_final_block_regularly_from_redis(
    blocks_cache: std::sync::Arc<crate::cache::RwLockLruMemoryCache<u64, CacheBlock>>,
    blocks_info_by_finality: std::sync::Arc<BlocksInfoByFinality>,
    redis_client: redis::aio::ConnectionManager,
    near_rpc_client: JsonRpcClient,
) {
    tracing::info!("Task to get and store final block in the cache started");
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        match get_block_streamer_message(
            near_primitives::types::Finality::Final,
            redis_client.clone(),
        )
        .await
        {
            Ok(streamer_message) => {
                if let Err(err) = handle_streamer_message(
                    streamer_message,
                    std::sync::Arc::clone(&blocks_cache),
                    std::sync::Arc::clone(&blocks_info_by_finality),
                    &near_rpc_client,
                )
                .await
                {
                    tracing::error!("Error to handle_streamer_message: {:?}", err);
                }
            }
            Err(err) => {
                tracing::warn!("Error to get final block from redis: {:?}", err);
            }
        }
    }
}

// Task to get and store optimistic block in the cache
// Subscribe to the redis channel and update the optimistic block in the cache
pub async fn update_optimistic_block_regularly(
    blocks_info_by_finality: std::sync::Arc<BlocksInfoByFinality>,
    redis_client: redis::aio::ConnectionManager,
) {
    tracing::info!("Task to get and store optimistic block in the cache started");
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        match get_block_streamer_message(
            near_primitives::types::Finality::None,
            redis_client.clone(),
        )
        .await
        {
            Ok(streamer_message) => {
                let optimistic_block = BlockInfo::new_from_streamer_message(streamer_message).await;
                let optimistic_block_cache = optimistic_block.block_cache;
                if optimistic_block_cache.block_height as i64
                    > crate::metrics::LATEST_BLOCK_HEIGHT_BY_FINALITIY
                        .with_label_values(&["optimistic"])
                        .get()
                {
                    blocks_info_by_finality
                        .update_optimistic_block(optimistic_block)
                        .await;
                    crate::metrics::LATEST_BLOCK_HEIGHT_BY_FINALITIY
                        .with_label_values(&["optimistic"])
                        .set(
                            i64::try_from(optimistic_block_cache.block_height)
                                .expect("Invalid optimistic block height"),
                        );
                }
            }
            Err(err) => {
                tracing::warn!("Error to get optimistic block from redis: {:?}", err);
            }
        }

        // When an optimistic block is not updated, or it is lower than the final block
        // we need to mark that optimistic updating is not working
        if crate::metrics::LATEST_BLOCK_HEIGHT_BY_FINALITIY
            .with_label_values(&["optimistic"])
            .get()
            <= crate::metrics::LATEST_BLOCK_HEIGHT_BY_FINALITIY
                .with_label_values(&["final"])
                .get()
            && !crate::metrics::OPTIMISTIC_UPDATING.is_not_working()
        {
            tracing::warn!(
                "Optimistic block in is not updated or optimistic block less than final block"
            );
            crate::metrics::OPTIMISTIC_UPDATING.set_not_working();
        };

        // When an optimistic block is updated, and it is greater than the final block
        // we need to mark that optimistic updating is working
        if crate::metrics::LATEST_BLOCK_HEIGHT_BY_FINALITIY
            .with_label_values(&["optimistic"])
            .get()
            > crate::metrics::LATEST_BLOCK_HEIGHT_BY_FINALITIY
                .with_label_values(&["final"])
                .get()
            && crate::metrics::OPTIMISTIC_UPDATING.is_not_working()
        {
            crate::metrics::OPTIMISTIC_UPDATING.set_working();
            tracing::info!("Optimistic block updating is resumed.");
        };
    }
}

/// Convert gigabytes to bytes
pub(crate) async fn gigabytes_to_bytes(gigabytes: f64) -> usize {
    (gigabytes * 1024.0 * 1024.0 * 1024.0) as usize
}

// Helper function to format memory size in a human-readable format
pub fn friendly_memory_size_format(memory_size_bytes: usize) -> String {
    if memory_size_bytes < 1024 {
        format!("{:.2} B", memory_size_bytes)
    } else if memory_size_bytes < 1024 * 1024 {
        format!("{:.2} KB", memory_size_bytes as f64 / 1024.0)
    } else if memory_size_bytes < 1024 * 1024 * 1024 {
        format!("{:.2} MB", memory_size_bytes as f64 / 1024.0 / 1024.0)
    } else {
        format!(
            "{:.2} GB",
            memory_size_bytes as f64 / 1024.0 / 1024.0 / 1024.0
        )
    }
}

#[cfg(feature = "shadow_data_consistency")]
pub async fn shadow_compare_results_handler<T, E, M>(
    shadow_rate: f64,
    read_rpc_result: &Result<T, E>,
    near_rpc_client: JsonRpcClient,
    params: M,
    method_name: &str,
) where
    M: near_jsonrpc_client::methods::RpcMethod + std::fmt::Debug,
    <M as near_jsonrpc_client::methods::RpcMethod>::Response: serde::ser::Serialize,
    <M as near_jsonrpc_client::methods::RpcMethod>::Error: std::fmt::Debug + serde::ser::Serialize,
    T: serde::ser::Serialize,
    E: std::fmt::Debug + serde::ser::Serialize,
{
    let method_total_requests = crate::metrics::METHOD_CALLS_COUNTER
        .with_label_values(&[method_name])
        .get();
    let err_code = if is_should_shadow_compare_results(method_total_requests, shadow_rate).await {
        let meta_data = format!("{:?}", params);
        let (read_rpc_response_json, is_response_ok) = match read_rpc_result {
            Ok(res) => (serde_json::to_value(res), true),
            Err(err) => (serde_json::to_value(err), false),
        };
        let read_rpc_response_meta_data = format!("{:?}", &read_rpc_response_json);
        let comparison_result = shadow_compare_results(
            read_rpc_response_json,
            near_rpc_client,
            params,
            is_response_ok,
        )
        .await;

        match comparison_result {
            Ok(_) => {
                tracing::info!(target: "shadow_data_consistency", "Shadow data check: CORRECT\n{}", meta_data);
                None
            }
            Err(err) => {
                if let ShadowDataConsistencyError::ResultsDontMatch {
                    reason,
                    read_rpc_response,
                    near_rpc_response,
                    ..
                } = &err
                {
                    tracing::warn!(
                        target: "shadow_data_consistency",
                        "Shadow data check: ERROR\n{}:{}: {}\n{}",
                        method_name,
                        reason.code(),
                        meta_data,
                        format!("{}, ReadRPC: {:?}, NearRPC: {:?}", reason.reason(), read_rpc_response, near_rpc_response),
                    );
                    Some(reason.code())
                } else {
                    tracing::warn!(
                        target: "shadow_data_consistency",
                        "Shadow data check: ERROR\n{}:4: {}\n{:?}",
                        method_name,
                        meta_data,
                        format!("NearRPC: {}, ReadRPC: {:?}", err, read_rpc_response_meta_data),
                    );
                    Some("4".to_string())
                }
            }
        }
    } else {
        None
    };
    if let Some(err_code) = &err_code {
        crate::metrics::REQUESTS_ERRORS
            .with_label_values(&[method_name, err_code])
            .inc();
    };
}

#[cfg(feature = "shadow_data_consistency")]
pub async fn is_should_shadow_compare_results(method_total_requests: u64, rate: f64) -> bool {
    let every_request = 100.0 / rate;
    method_total_requests % every_request as u64 == 0
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
/// `client`: `JsonRpcClient`.
/// `params`: `near_jsonrpc_client::methods::RpcMethod` trait.
///
/// In case of a successful comparison, the function returns `Ok(())`.
/// Otherwise, it returns `Err(ShadowDataConsistencyError)`.
#[cfg(feature = "shadow_data_consistency")]
pub async fn shadow_compare_results<M>(
    read_rpc_response: Result<serde_json::Value, serde_json::Error>,
    client: JsonRpcClient,
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

    let mut near_rpc_response = client.shadow_comparison_call(&params).await;

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
                near_rpc_response = client.shadow_comparison_call(&params).await;
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
    pub fn code(&self) -> String {
        match self {
            DataMismatchReason::ReadRpcSuccessNearRpcSuccess => "0".to_string(),
            DataMismatchReason::ReadRpcSuccessNearRpcError => "1".to_string(),
            DataMismatchReason::ReadRpcErrorNearRpcSuccess => "2".to_string(),
            DataMismatchReason::ReadRpcErrorNearRpcError => "3".to_string(),
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
            format!("{}{}", str_key, &generate_array_key(arr_val))
        }),
        serde_json::Value::Object(obj) => obj.iter().fold(String::new(), |str_key, (key, val)| {
            format!("{}/{}:{}", str_key, key, val)
        }),
    }
}
