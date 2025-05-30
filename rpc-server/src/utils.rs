use crate::modules::blocks::{BlockInfo, BlocksInfoByFinality, ChunkInfo};
#[cfg(feature = "shadow-data-consistency")]
use assert_json_diff::{assert_json_matches_no_panic, CompareMode, Config, NumericMode};

#[cfg(feature = "shadow-data-consistency")]
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

    /// add authorization header to the RPC request.
    pub fn authorization(mut self, token: &str) -> anyhow::Result<Self> {
        self.regular_client = self
            .regular_client
            .header(near_jsonrpc_client::auth::Authorization::bearer(token)?);
        self.archival_client = self
            .archival_client
            .header(near_jsonrpc_client::auth::Authorization::bearer(token)?);
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
    #[cfg(feature = "shadow-data-consistency")]
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

pub async fn get_current_validators(
    near_rpc_client: &JsonRpcClient,
) -> anyhow::Result<near_primitives::views::EpochValidatorInfo> {
    let params = near_jsonrpc_client::methods::validators::RpcValidatorRequest {
        epoch_reference: near_primitives::types::EpochReference::Latest,
    };
    Ok(near_rpc_client.call(params, None).await?)
}

pub async fn get_current_protocol_version(
    near_rpc_client: &JsonRpcClient,
) -> anyhow::Result<near_primitives::version::ProtocolVersion> {
    let params = near_jsonrpc_client::methods::status::RpcStatusRequest;
    let protocol_version = near_rpc_client.call(params, None).await?.protocol_version;
    Ok(protocol_version)
}

async fn handle_streamer_message(
    streamer_message: near_indexer_primitives::StreamerMessage,
    blocks_cache: std::sync::Arc<
        crate::cache::RwLockLruMemoryCache<u64, near_primitives::views::BlockView>,
    >,
    chunks_cache: std::sync::Arc<
        crate::cache::RwLockLruMemoryCache<u64, crate::modules::blocks::ChunksInfo>,
    >,
    blocks_info_by_finality: std::sync::Arc<BlocksInfoByFinality>,
    near_rpc_client: &JsonRpcClient,
) -> anyhow::Result<()> {
    if streamer_message.block.header.height as i64
        > crate::metrics::LATEST_BLOCK_HEIGHT_BY_FINALITIY
            .with_label_values(&["final"])
            .get()
    {
        let block = BlockInfo::new_from_streamer_message(&streamer_message).await;
        let block_view = block.block_view.clone();

        if blocks_info_by_finality
            .final_block_view()
            .await
            .header
            .epoch_id
            != block_view.header.epoch_id
        {
            tracing::info!("New epoch started: {:?}", block_view.header.epoch_id);
            blocks_info_by_finality
                .update_current_epoch_info(near_rpc_client)
                .await?;
        }

        blocks_info_by_finality.update_final_block(block).await;
        blocks_cache
            .put(block_view.header.height, block_view.clone())
            .await;
        chunks_cache
            .put(
                block_view.header.height,
                ChunkInfo::from_indexer_shards(streamer_message.shards),
            )
            .await;
        crate::metrics::LATEST_BLOCK_HEIGHT_BY_FINALITIY
            .with_label_values(&["final"])
            .set(i64::try_from(block_view.header.height)?);
    }
    Ok(())
}

// Task to get and store final block in the cache
async fn task_update_final_block_regularly(
    blocks_cache: std::sync::Arc<
        crate::cache::RwLockLruMemoryCache<u64, near_primitives::views::BlockView>,
    >,
    chunks_cache: std::sync::Arc<
        crate::cache::RwLockLruMemoryCache<u64, crate::modules::blocks::ChunksInfo>,
    >,
    blocks_info_by_finality: std::sync::Arc<BlocksInfoByFinality>,
    fastnear_client: near_lake_framework::FastNearClient,
    near_rpc_client: JsonRpcClient,
) {
    tracing::info!("Task to get and store final block in the cache started");
    let mut final_block_height = blocks_info_by_finality
        .final_block_view()
        .await
        .header
        .height;
    loop {
        final_block_height += 1;
        if let Some(streamer_message) =
            near_lake_framework::fastnear::fetchers::fetch_streamer_message(
                &fastnear_client,
                final_block_height,
            )
            .await
        {
            if let Err(err) = handle_streamer_message(
                streamer_message,
                std::sync::Arc::clone(&blocks_cache),
                std::sync::Arc::clone(&chunks_cache),
                std::sync::Arc::clone(&blocks_info_by_finality),
                &near_rpc_client,
            )
            .await
            {
                tracing::error!("Error in fn handle_streamer_message(): {:?}", err);
            };
        }
    }
}

// Task to get and store optimistic block in the cache
async fn task_update_optimistic_block_regularly(
    blocks_info_by_finality: std::sync::Arc<BlocksInfoByFinality>,
    fastnear_client: near_lake_framework::FastNearClient,
) {
    tracing::info!("Task to get and store optimistic block in the cache started");
    let mut optimistic_block_height = blocks_info_by_finality
        .optimistic_block_view()
        .await
        .header
        .height;
    loop {
        optimistic_block_height += 1;
        if let Some(streamer_message) =
            near_lake_framework::fastnear::fetchers::fetch_optimistic_streamer_message_by_height(
                &fastnear_client,
                optimistic_block_height,
            )
            .await
        {
            if streamer_message.block.header.height as i64
                > crate::metrics::LATEST_BLOCK_HEIGHT_BY_FINALITIY
                    .with_label_values(&["optimistic"])
                    .get()
            {
                let optimistic_block =
                    BlockInfo::new_from_streamer_message(&streamer_message).await;
                let optimistic_block_view = optimistic_block.block_view.clone();
                blocks_info_by_finality
                    .update_optimistic_block(optimistic_block)
                    .await;
                crate::metrics::LATEST_BLOCK_HEIGHT_BY_FINALITIY
                    .with_label_values(&["optimistic"])
                    .set(
                        i64::try_from(optimistic_block_view.header.height)
                            .expect("Invalid optimistic block height"),
                    );
            }
        }
    }
}

// Task to check optimistic block status
async fn task_optimistic_block_status() {
    tracing::info!("Task to check optimistic block status started");
    let mut last_optimistic = crate::metrics::LATEST_BLOCK_HEIGHT_BY_FINALITIY
        .with_label_values(&["optimistic"])
        .get();
    loop {
        // check every 2 seconds
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // get the current final block height
        let current_final = crate::metrics::LATEST_BLOCK_HEIGHT_BY_FINALITIY
            .with_label_values(&["final"])
            .get();

        // get the current optimistic block height
        let current_optimistic = crate::metrics::LATEST_BLOCK_HEIGHT_BY_FINALITIY
            .with_label_values(&["optimistic"])
            .get();

        // When an optimistic block is not updated, or it is lower than the final block
        // we need to mark that optimistic updating is not working
        if current_optimistic <= current_final
            || current_optimistic <= last_optimistic
                && !crate::metrics::OPTIMISTIC_UPDATING.is_not_working()
        {
            tracing::warn!(
                "Optimistic block in is not updated or optimistic block less than final block"
            );
            crate::metrics::OPTIMISTIC_UPDATING.set_not_working();
        };

        // When an optimistic block is updated, and it is greater than the final block
        // we need to mark that optimistic updating is working
        if current_optimistic > current_final
            && current_optimistic > last_optimistic
            && crate::metrics::OPTIMISTIC_UPDATING.is_not_working()
        {
            crate::metrics::OPTIMISTIC_UPDATING.set_working();
            tracing::info!("Optimistic block updating is resumed.");
        }

        last_optimistic = current_optimistic;
    }
}

pub async fn task_regularly_update_blocks_by_finality(
    blocks_info_by_finality: std::sync::Arc<BlocksInfoByFinality>,
    blocks_cache: std::sync::Arc<
        crate::cache::RwLockLruMemoryCache<u64, near_primitives::views::BlockView>,
    >,
    chunks_cache: std::sync::Arc<
        crate::cache::RwLockLruMemoryCache<u64, crate::modules::blocks::ChunksInfo>,
    >,
    fastnear_client: near_lake_framework::FastNearClient,
    near_rpc_client: JsonRpcClient,
) {
    // Task update final block regularly
    let blocks_info_by_finality_clone = std::sync::Arc::clone(&blocks_info_by_finality);
    let fastnear_client_clone = fastnear_client.clone();

    tokio::spawn(async move {
        task_update_final_block_regularly(
            blocks_cache,
            chunks_cache,
            blocks_info_by_finality_clone,
            fastnear_client_clone,
            near_rpc_client,
        )
        .await
    });

    // Task update optimistic block regularly
    tokio::spawn(async move {
        task_update_optimistic_block_regularly(blocks_info_by_finality, fastnear_client).await
    });

    // Task to check the optimistic block status
    tokio::spawn(async move { task_optimistic_block_status().await });
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

#[cfg(feature = "shadow-data-consistency")]
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

#[cfg(feature = "shadow-data-consistency")]
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
#[cfg(feature = "shadow-data-consistency")]
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
                    matches!(
                        server_error,
                        near_jsonrpc_client::errors::JsonRpcServerError::NonContextualError(_)
                            | near_jsonrpc_client::errors::JsonRpcServerError::ResponseStatusError(
                                _
                            )
                    )
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
                match serde_json::to_value(e) {
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
                error_message: "Success results don't match".to_string(),
                reason: DataMismatchReason::SuccessNearRpcSuccess,
                read_rpc_response: read_rpc_json,
                near_rpc_response: near_rpc_json,
            }
        } else if !read_rpc_response_is_ok && near_rpc_response_is_ok {
            // read_rpc service has error response and near_rpc has successful response
            ShadowDataConsistencyError::ResultsDontMatch {
                error_message: "ReadRPC failed, NearRPC success".to_string(),
                reason: DataMismatchReason::ErrorNearRpcSuccess,
                read_rpc_response: read_rpc_json,
                near_rpc_response: near_rpc_json,
            }
        } else if read_rpc_response_is_ok && !near_rpc_response_is_ok {
            // read_rpc service has successful response and near_rpc has error response
            // Expected that all error will be related with network issues.
            ShadowDataConsistencyError::ResultsDontMatch {
                error_message: "ReadRPC success, NearRPC failed".to_string(),
                reason: DataMismatchReason::SuccessNearRpcError,
                read_rpc_response: read_rpc_json,
                near_rpc_response: near_rpc_json,
            }
        } else {
            // Both services(read_rpc and near_rpc) have an error response
            // both response objects included for future investigation.
            // Expected we will only have a difference in the error text.
            ShadowDataConsistencyError::ResultsDontMatch {
                error_message: "Both services failed, but results don't match".to_string(),
                reason: DataMismatchReason::ErrorNearRpcError,
                read_rpc_response: read_rpc_json,
                near_rpc_response: near_rpc_json,
            }
        };
        return Err(results_dont_match_error);
    };
    Ok(())
}

/// Represents the error that can occur during the shadow data consistency check.
#[cfg(feature = "shadow-data-consistency")]
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
#[cfg(feature = "shadow-data-consistency")]
#[derive(Debug)]
pub enum DataMismatchReason {
    /// ReadRPC returns success result and NEAR RPC returns success result but the results mismatch
    SuccessNearRpcSuccess,
    /// ReadRPC returns success result and NEAR RPC returns error result
    SuccessNearRpcError,
    /// ReadRPC returns error result and NEAR RPC returns success result
    ErrorNearRpcSuccess,
    /// ReadRPC returns error result and NEAR RPC returns error result but the results mismatch
    ErrorNearRpcError,
}

#[cfg(feature = "shadow-data-consistency")]
impl DataMismatchReason {
    /// This method converts the reason into a number from 0 to 3. These numbers are used in the
    /// metrics like BLOCK_ERROR_0, BLOCK_ERROR_1, BLOCK_ERROR_2, BLOCK_ERROR_3 etc.
    pub fn code(&self) -> String {
        match self {
            DataMismatchReason::SuccessNearRpcSuccess => "0".to_string(),
            DataMismatchReason::SuccessNearRpcError => "1".to_string(),
            DataMismatchReason::ErrorNearRpcSuccess => "2".to_string(),
            DataMismatchReason::ErrorNearRpcError => "3".to_string(),
        }
    }

    /// This method converts the reason into a string. These strings are used in the logs to include the
    /// human readable reason for the mismatch.
    pub fn reason(&self) -> &'static str {
        match self {
            DataMismatchReason::SuccessNearRpcSuccess => "Read RPC success, and NEAR RPC success",
            DataMismatchReason::SuccessNearRpcError => "Read RPC success, but NEAR RPC error",
            DataMismatchReason::ErrorNearRpcSuccess => "Read RPC error, but NEAR RPC success",
            DataMismatchReason::ErrorNearRpcError => "Read RPC error, and NEAR RPC error",
        }
    }
}

/// Sort json value
///
/// 1. sort object key
/// 2. sort array
#[cfg(feature = "shadow-data-consistency")]
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
                .into_values()
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
#[cfg(feature = "shadow-data-consistency")]
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
