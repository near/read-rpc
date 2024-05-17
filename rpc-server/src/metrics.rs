use actix_web::dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::{get, Responder};
use futures::future::LocalBoxFuture;
use futures::StreamExt;
use prometheus::{Encoder, IntCounter, IntCounterVec, IntGauge, Opts};
use std::future::{ready, Ready};

type Result<T, E> = std::result::Result<T, E>;

fn try_create_int_counter(name: &str, help: &str) -> Result<IntCounter, prometheus::Error> {
    let opts = Opts::new(name, help);
    let counter = IntCounter::with_opts(opts)?;
    prometheus::register(Box::new(counter.clone()))?;
    Ok(counter)
}

fn try_create_int_gauge(name: &str, help: &str) -> Result<IntGauge, prometheus::Error> {
    let opts = Opts::new(name, help);
    let gauge = IntGauge::with_opts(opts)?;
    prometheus::register(Box::new(gauge.clone()))?;
    Ok(gauge)
}

fn register_int_counter_vec(
    name: &str,
    help: &str,
    label_names: &[&str],
) -> Result<IntCounterVec, prometheus::Error> {
    let opts = Opts::new(name, help);
    let counter = IntCounterVec::new(opts, label_names)?;
    prometheus::register(Box::new(counter.clone()))?;
    Ok(counter)
}

lazy_static! {
    pub(crate) static ref METHODS_CALLS_COUNTER: IntCounterVec = register_int_counter_vec(
        "methods_calls_counter",
        "Total number of calls to the method",
        &["method_name"] // This declares a label named `method name`
    ).unwrap();
}

// Struct to store the optimistic updating state
// This is used to track if the optimistic updating is working or not
// By default, it is set as working
pub struct OptimisticUpdating {
    is_not_working: std::sync::atomic::AtomicBool,
}

impl OptimisticUpdating {
    pub fn new() -> Self {
        Self {
            is_not_working: std::sync::atomic::AtomicBool::new(false),
        }
    }

    // Helper function to update AtomicBool value
    fn set(&self, val: bool) {
        self.is_not_working
            .store(val, std::sync::atomic::Ordering::Relaxed);
    }

    // Helper function to get AtomicBool value
    fn get(&self) -> bool {
        self.is_not_working
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    // return true if optimistic updating is not working
    pub fn is_not_working(&self) -> bool {
        self.get()
    }

    // Set optimistic updating as not working
    pub fn set_not_working(&self) {
        self.set(true);
    }

    // Set optimistic updating as working
    pub fn set_working(&self) {
        self.set(false);
    }
}

lazy_static! {
    pub(crate) static ref OPTIMISTIC_REQUESTS_TOTAL: IntCounter = try_create_int_counter(
        "total_optimistic_requests",
        "Total number of the request where finality was set to optimistic"
    )
    .unwrap();
    pub(crate) static ref FINAL_BLOCK_HEIGHT: IntGauge = try_create_int_gauge(
        "final_block_height",
        "The final block height from the perspective of the READ RPC server"
    )
    .unwrap();
    pub(crate) static ref OPTIMISTIC_BLOCK_HEIGHT: IntGauge = try_create_int_gauge(
        "optimistic_block_height",
        "The optimistic block height from the perspective of the READ RPC server"
    )
    .unwrap();

    pub(crate) static ref OPTIMISTIC_UPDATING: OptimisticUpdating = OptimisticUpdating::new();

    // REQUESTS TOTAL COUNTERS
    // total requests counter
    pub(crate) static ref TOTAL_REQUESTS_COUNTER: IntCounter = try_create_int_counter(
        "total_request_counter",
        "Total number requests"
    )
    .unwrap();
    pub(crate) static ref PROXY_OPTIMISTIC_REQUESTS_TOTAL: IntCounter = try_create_int_counter(
        "proxy_optimistic_requests",
        "Total number of the request where finality was set to optimistic and proxy to regular nodes"
    )
    .unwrap();
    pub(crate) static ref PROXY_REQUESTS_TO_REGULAR_NODES_COUNTER: IntCounter = try_create_int_counter(
        "proxy_request_to_regular_nodes_counter",
        "Total number requests proxy to regular nodes"
    )
    .unwrap();
    pub(crate) static ref PROXY_REQUESTS_TO_ARCHIVAL_NODES_COUNTER: IntCounter = try_create_int_counter(
        "proxy_request_to_archival_nodes_counter",
        "Total number requests proxy to archival nodes"
    )
    .unwrap();

    // // query requests counters
    // pub(crate) static ref QUERY_VIEW_ACCOUNT_REQUESTS_TOTAL: IntCounter = try_create_int_counter(
    //     "query_view_account_requests_counter",
    //     "Total number requests to the query view account endpoint"
    // )
    // .unwrap();
    // pub(crate) static ref QUERY_VIEW_CODE_REQUESTS_TOTAL: IntCounter = try_create_int_counter(
    //     "query_view_code_requests_counter",
    //     "Total number requests to the query view code endpoint"
    // )
    // .unwrap();
    // pub(crate) static ref QUERY_VIEW_ACCESS_KEY_REQUESTS_TOTAL: IntCounter = try_create_int_counter(
    //     "query_view_access_key_requests_counter",
    //     "Total number requests to the query view access key endpoint"
    // ).unwrap();
    // pub(crate) static ref QUERY_VIEW_STATE_REQUESTS_TOTAL: IntCounter = try_create_int_counter(
    //     "query_view_state_requests_counter",
    //     "Total number requests to the query view state endpoint"
    // ).unwrap();
    // pub(crate) static ref QUERY_FUNCTION_CALL_REQUESTS_TOTAL: IntCounter = try_create_int_counter(
    //     "query_function_call_requests_counter",
    //     "Total number requests to the query function call endpoint"
    // ).unwrap();
    // pub(crate) static ref QUERY_VIEW_ACCESS_KEYS_LIST_REQUESTS_TOTAL: IntCounter = try_create_int_counter(
    //     "query_access_keys_list_requests_counter",
    //     "Total number requests to the query access keys list endpoint"
    // ).unwrap();

    // // blocks requests counters
    // pub(crate) static ref BLOCK_REQUESTS_TOTAL: IntCounter = try_create_int_counter(
    //     "block_requests_counter",
    //     "Total number requests to the block endpoint"
    // ).unwrap();
    // pub(crate) static ref CHNGES_IN_BLOCK_BY_TYPE_REQUESTS_TOTAL: IntCounter = try_create_int_counter(
    //     "changes_in_block_by_type_requests_counter",
    //     "Total number requests to the changes in block by type endpoint"
    // ).unwrap();
    // pub(crate) static ref CHNGES_IN_BLOCK_REQUESTS_TOTAL: IntCounter = try_create_int_counter(
    //     "changes_in_block_requests_counter",
    //     "Total number requests to the changes in block endpoint"
    // ).unwrap();
    // pub(crate) static ref CHUNK_REQUESTS_TOTAL: IntCounter = try_create_int_counter(
    //     "chunk_requests_counter",
    //     "Total number requests to the chunk endpoint"
    // ).unwrap();

    // // gas_price requests counters
    // pub(crate) static ref GAS_PRICE_REQUESTS_TOTAL: IntCounter = try_create_int_counter(
    //     "gas_price_requests_counter",
    //     "Total number requests to the gas_price endpoint"
    // ).unwrap();
    //
    // // transactions requests counters
    // pub(crate) static ref TX_REQUESTS_TOTAL: IntCounter = try_create_int_counter(
    //     "tx_requests_counter",
    //     "Total number requests to the tx endpoint"
    // ).unwrap();
    // pub(crate) static ref TX_STATUS_REQUESTS_TOTAL: IntCounter = try_create_int_counter(
    //     "tx_status_requests_counter",
    //     "Total number requests to the tx status endpoint"
    // ).unwrap();
    // pub(crate) static ref RECEIPT_REQUESTS_TOTAL: IntCounter = try_create_int_counter(
    //     "receipt_requests_counter",
    //     "Total number requests to the receipt endpoint"
    // ).unwrap();
    //
    // // validators requests counters
    // pub(crate) static ref VALIDATORS_REQUESTS_TOTAL: IntCounter = try_create_int_counter(
    //     "validators_requests_counter",
    //     "Total number requests to the validators endpoint"
    // ).unwrap();

    // // protocol config requests counters
    // pub(crate) static ref PROTOCOL_CONFIG_REQUESTS_TOTAL: IntCounter = try_create_int_counter(
    //     "protocol_config_requests_counter",
    //     "Total number requests to the protocol_config endpoint"
    // ).unwrap();
}

// Error counters
// QUERY counters
lazy_static! {
    // QUERY.view_account
    pub(crate) static ref QUERY_VIEW_ACCOUNT_ERROR_0: IntCounter = try_create_int_counter(
        "query_view_account_error_0",
        "Query.view_account error 0: ReadRPC success, NEAR RPC success"
    )
    .unwrap();

    pub(crate) static ref QUERY_VIEW_ACCOUNT_ERROR_1: IntCounter = try_create_int_counter(
        "query_view_account_error_1",
        "Query.view_account error 1: ReadRPC success, NEAR RPC error"
    ).unwrap();

    pub(crate) static ref QUERY_VIEW_ACCOUNT_ERROR_2: IntCounter = try_create_int_counter(
        "query_view_account_error_2",
        "Query.view_account error 2: ReadRPC error, NEAR RPC success"
    ).unwrap();

    pub(crate) static ref QUERY_VIEW_ACCOUNT_ERROR_3: IntCounter = try_create_int_counter(
        "query_view_account_error_3",
        "Query.view_account error 3: ReadRPC error, NEAR RPC error"
    ).unwrap();

    pub(crate) static ref QUERY_VIEW_ACCOUNT_ERROR_4: IntCounter = try_create_int_counter(
        "query_view_account_error_4",
        "Query.view_account error 4: Failed to compare. Network or parsing error"
    ).unwrap();
    // end QUERY.view_account
}

lazy_static! {
    // QUERY.view_code
    pub(crate) static ref QUERY_VIEW_CODE_ERROR_0: IntCounter = try_create_int_counter(
        "query_view_code_error_0",
        "Query.view_code error 0: ReadRPC success, NEAR RPC success"
    )
    .unwrap();

    pub(crate) static ref QUERY_VIEW_CODE_ERROR_1: IntCounter = try_create_int_counter(
        "query_view_code_error_1",
        "Query.view_code error 1: ReadRPC success, NEAR RPC error"
    ).unwrap();

    pub(crate) static ref QUERY_VIEW_CODE_ERROR_2: IntCounter = try_create_int_counter(
        "query_view_code_error_2",
        "Query.view_code error 2: ReadRPC error, NEAR RPC success"
    ).unwrap();

    pub(crate) static ref QUERY_VIEW_CODE_ERROR_3: IntCounter = try_create_int_counter(
        "query_view_code_error_3",
        "Query.view_code error 3: ReadRPC error, NEAR RPC error"
    ).unwrap();

    pub(crate) static ref QUERY_VIEW_CODE_ERROR_4: IntCounter = try_create_int_counter(
        "query_view_code_error_4",
        "Query.view_code error 4: Failed to compare. Network or parsing error"
    ).unwrap();
    // end QUERY.view_code
}

lazy_static! {
    // QUERY.view_access_key
    pub(crate) static ref QUERY_VIEW_ACCESS_KEY_ERROR_0: IntCounter = try_create_int_counter(
        "query_view_access_key_error_0",
        "Query.view_access_key error 0: ReadRPC success, NEAR RPC success"
    ).unwrap();

    pub(crate) static ref QUERY_VIEW_ACCESS_KEY_ERROR_1: IntCounter = try_create_int_counter(
        "query_view_access_key_error_1",
        "Query.view_access_key error 1: ReadRPC success, NEAR RPC error"
    ).unwrap();

    pub(crate) static ref QUERY_VIEW_ACCESS_KEY_ERROR_2: IntCounter = try_create_int_counter(
        "query_view_access_key_error_2",
        "Query.view_access_key error 2: ReadRPC error, NEAR RPC success"
    ).unwrap();

    pub(crate) static ref QUERY_VIEW_ACCESS_KEY_ERROR_3: IntCounter = try_create_int_counter(
        "query_view_access_key_error_3",
        "Query.view_access_key error 3: ReadRPC error, NEAR RPC error"
    ).unwrap();

    pub(crate) static ref QUERY_VIEW_ACCESS_KEY_ERROR_4: IntCounter = try_create_int_counter(
        "query_view_access_key_error_4",
        "Query.view_access_key error 4: Failed to compare. Network or parsing error"
    ).unwrap();
    // end QUERY.view_access_key
}

lazy_static! {
    // QUERY.view_state
    pub(crate) static ref QUERY_VIEW_STATE_ERROR_0: IntCounter = try_create_int_counter(
        "query_view_state_error_0",
        "Query.view_state error 0: ReadRPC success, NEAR RPC success"
    ).unwrap();

    pub(crate) static ref QUERY_VIEW_STATE_ERROR_1: IntCounter = try_create_int_counter(
        "query_view_state_error_1",
        "Query.view_state error 1: ReadRPC success, NEAR RPC error"
    ).unwrap();

    pub(crate) static ref QUERY_VIEW_STATE_ERROR_2: IntCounter = try_create_int_counter(
        "query_view_state_error_2",
        "Query.view_state error 2: ReadRPC error, NEAR RPC success"
    ).unwrap();

    pub(crate) static ref QUERY_VIEW_STATE_ERROR_3: IntCounter = try_create_int_counter(
        "query_view_state_error_3",
        "Query.view_state error 3: ReadRPC error, NEAR RPC error"
    ).unwrap();

    pub(crate) static ref QUERY_VIEW_STATE_ERROR_4: IntCounter = try_create_int_counter(
        "query_view_state_error_4",
        "Query.view_state error 4: Failed to compare. Network or parsing error"
    ).unwrap();
    // end QUERY.view_state
}

lazy_static! {
    // QUERY.function_call
    pub(crate) static ref QUERY_FUNCTION_CALL_ERROR_0: IntCounter = try_create_int_counter(
        "query_function_call_error_0",
        "Query.function_call error 0: ReadRPC success, NEAR RPC success"
    ).unwrap();

    pub(crate) static ref QUERY_FUNCTION_CALL_ERROR_1: IntCounter = try_create_int_counter(
        "query_function_call_error_1",
        "Query.function_call error 1: ReadRPC success, NEAR RPC error"
    ).unwrap();

    pub(crate) static ref QUERY_FUNCTION_CALL_ERROR_2: IntCounter = try_create_int_counter(
        "query_function_call_error_2",
        "Query.function_call error 2: ReadRPC error, NEAR RPC success"
    ).unwrap();

    pub(crate) static ref QUERY_FUNCTION_CALL_ERROR_3: IntCounter = try_create_int_counter(
        "query_function_call_error_3",
        "Query.function_call error 3: ReadRPC error, NEAR RPC error"
    ).unwrap();

    pub(crate) static ref QUERY_FUNCTION_CALL_ERROR_4: IntCounter = try_create_int_counter(
        "query_function_call_error_4",
        "Query.function_call error 4: Failed to compare. Network or parsing error"
    ).unwrap();
    // end QUERY.function_call
}

lazy_static! {
    // QUERY.view_access_key_list
    pub(crate) static ref QUERY_VIEW_ACCESS_KEY_LIST_ERROR_0: IntCounter = try_create_int_counter(
        "query_view_access_key_list_error_0",
        "Query.view_access_key_list error 0: ReadRPC success, NEAR RPC success"
    ).unwrap();

    pub(crate) static ref QUERY_VIEW_ACCESS_KEY_LIST_ERROR_1: IntCounter = try_create_int_counter(
        "query_view_access_key_list_error_1",
        "Query.view_access_key_list error 1: ReadRPC success, NEAR RPC error"
    ).unwrap();

    pub(crate) static ref QUERY_VIEW_ACCESS_KEY_LIST_ERROR_2: IntCounter = try_create_int_counter(
        "query_view_access_key_list_error_2",
        "Query.view_access_key_list error 2: ReadRPC error, NEAR RPC success"
    ).unwrap();

    pub(crate) static ref QUERY_VIEW_ACCESS_KEY_LIST_ERROR_3: IntCounter = try_create_int_counter(
        "query_view_access_key_list_error_3",
        "Query.view_access_key_list error 3: ReadRPC error, NEAR RPC error"
    ).unwrap();

    pub(crate) static ref QUERY_VIEW_ACCESS_KEY_LIST_ERROR_4: IntCounter = try_create_int_counter(
        "query_view_access_key_list_error_4",
        "Query.view_access_key_list error 4: Failed to compare. Network or parsing error"
    ).unwrap();
    // end QUERY.view_access_key_list
}
// end QUERY

lazy_static! {
    // BLOCK
    pub(crate) static ref BLOCK_ERROR_0: IntCounter = try_create_int_counter(
        "block_error_0",
        "Block error 0: ReadRPC success, NEAR RPC success"
    ).unwrap();

    pub(crate) static ref BLOCK_ERROR_1: IntCounter = try_create_int_counter(
        "block_error_1",
        "Block error 1: ReadRPC success, NEAR RPC error"
    ).unwrap();

    pub(crate) static ref BLOCK_ERROR_2: IntCounter = try_create_int_counter(
        "block_error_2",
        "Block error 2: ReadRPC error, NEAR RPC success"
    ).unwrap();

    pub(crate) static ref BLOCK_ERROR_3: IntCounter = try_create_int_counter(
        "block_error_3",
        "Block error 3: ReadRPC error, NEAR RPC error"
    ).unwrap();

    pub(crate) static ref BLOCK_ERROR_4: IntCounter = try_create_int_counter(
        "block_error_4",
        "Block error 4: Failed to compare. Network or parsing error"
    ).unwrap();
    // end BLOCK
}

lazy_static! {
    // CHUNK
    pub(crate) static ref CHUNK_ERROR_0: IntCounter = try_create_int_counter(
        "chunk_error_0",
        "Chunk error 0: ReadRPC success, NEAR RPC success"
    ).unwrap();

    pub(crate) static ref CHUNK_ERROR_1: IntCounter = try_create_int_counter(
        "chunk_error_1",
        "Chunk error 1: ReadRPC success, NEAR RPC error"
    ).unwrap();

    pub(crate) static ref CHUNK_ERROR_2: IntCounter = try_create_int_counter(
        "chunk_error_2",
        "Chunk error 2: ReadRPC error, NEAR RPC success"
    ).unwrap();

    pub(crate) static ref CHUNK_ERROR_3: IntCounter = try_create_int_counter(
        "chunk_error_3",
        "Chunk error 3: ReadRPC error, NEAR RPC error"
    ).unwrap();

    pub(crate) static ref CHUNK_ERROR_4: IntCounter = try_create_int_counter(
        "chunk_error_4",
        "Chunk error 4: Failed to compare. Network or parsing error"
    ).unwrap();
    // end CHUNK
}

lazy_static! {
    // GAS_PRICE
    pub(crate) static ref GAS_PRICE_ERROR_0: IntCounter = try_create_int_counter(
        "gas_price_error_0",
        "Gas_price error 0: ReadRPC success, NEAR RPC success"
    ).unwrap();

    pub(crate) static ref GAS_PRICE_ERROR_1: IntCounter = try_create_int_counter(
        "gas_price_error_1",
        "Gas_price error 1: ReadRPC success, NEAR RPC error"
    ).unwrap();

    pub(crate) static ref GAS_PRICE_ERROR_2: IntCounter = try_create_int_counter(
        "gas_price_error_2",
        "Gas_price error 2: ReadRPC error, NEAR RPC success"
    ).unwrap();

    pub(crate) static ref GAS_PRICE_ERROR_3: IntCounter = try_create_int_counter(
        "gas_price_error_3",
        "Gas_price error 3: ReadRPC error, NEAR RPC error"
    ).unwrap();

    pub(crate) static ref GAS_PRICE_ERROR_4: IntCounter = try_create_int_counter(
        "gas_price_error_4",
        "Gas_price error 4: Failed to compare. Network or parsing error"
    ).unwrap();
    // end GAS_PRICE
}

lazy_static! {
    // VALIDATORS
    pub(crate) static ref VALIDATORS_ERROR_0: IntCounter = try_create_int_counter(
        "validators_error_0",
        "validators error 0: ReadRPC success, NEAR RPC success"
    ).unwrap();

    pub(crate) static ref VALIDATORS_ERROR_1: IntCounter = try_create_int_counter(
        "validators_error_1",
        "Validators error 1: ReadRPC success, NEAR RPC error"
    ).unwrap();

    pub(crate) static ref VALIDATORS_ERROR_2: IntCounter = try_create_int_counter(
        "validators_error_2",
        "Validators error 2: ReadRPC error, NEAR RPC success"
    ).unwrap();

    pub(crate) static ref VALIDATORS_ERROR_3: IntCounter = try_create_int_counter(
        "validators_error_3",
        "Validators error 3: ReadRPC error, NEAR RPC error"
    ).unwrap();

    pub(crate) static ref VALIDATORS_ERROR_4: IntCounter = try_create_int_counter(
        "validators_error_4",
        "Validators error 4: Failed to compare. Network or parsing error"
    ).unwrap();
    // end VALIDATORS
}

lazy_static! {
    // PROTOCOL_CONFIG
    pub(crate) static ref PROTOCOL_CONFIG_ERROR_0: IntCounter = try_create_int_counter(
        "protocol_config_error_0",
        "Protocol config error 0: ReadRPC success, NEAR RPC success"
    ).unwrap();

    pub(crate) static ref PROTOCOL_CONFIG_ERROR_1: IntCounter = try_create_int_counter(
        "protocol_config_error_1",
        "Protocol config error 1: ReadRPC success, NEAR RPC error"
    ).unwrap();

    pub(crate) static ref PROTOCOL_CONFIG_ERROR_2: IntCounter = try_create_int_counter(
        "protocol_config_error_2",
        "Protocol config error 2: ReadRPC error, NEAR RPC success"
    ).unwrap();

    pub(crate) static ref PROTOCOL_CONFIG_ERROR_3: IntCounter = try_create_int_counter(
        "protocol_config_error_3",
        "Protocol config error 3: ReadRPC error, NEAR RPC error"
    ).unwrap();

    pub(crate) static ref PROTOCOL_CONFIG_ERROR_4: IntCounter = try_create_int_counter(
        "protocol_config_error_4",
        "Protocol config error 4: Failed to compare. Network or parsing error"
    ).unwrap();
    // end PROTOCOL_CONFIG
}

lazy_static! {
    // TX
    pub(crate) static ref TX_ERROR_0: IntCounter = try_create_int_counter(
        "tx_error_0",
        "Tx error 0: ReadRPC success, NEAR RPC success"
    ).unwrap();

    pub(crate) static ref TX_ERROR_1: IntCounter = try_create_int_counter(
        "tx_error_1",
        "Tx error 1: ReadRPC success, NEAR RPC error"
    ).unwrap();

    pub(crate) static ref TX_ERROR_2: IntCounter = try_create_int_counter(
        "tx_error_2",
        "Tx error 2: ReadRPC error, NEAR RPC success"
    ).unwrap();

    pub(crate) static ref TX_ERROR_3: IntCounter = try_create_int_counter(
        "tx_error_3",
        "Tx error 3: ReadRPC error, NEAR RPC error"
    ).unwrap();

    pub(crate) static ref TX_ERROR_4: IntCounter = try_create_int_counter(
        "tx_error_4",
        "Tx error 4: Failed to compare. Network or parsing error"
    ).unwrap();
    // end TX
}

lazy_static! {
    // TX_STATUS
    pub(crate) static ref EXPERIMENTAL_TX_STATUS_ERROR_0: IntCounter = try_create_int_counter(
        "tx_status_error_0",
        "TxStatus error 0: ReadRPC success, NEAR RPC success"
    ).unwrap();

    pub(crate) static ref EXPERIMENTAL_TX_STATUS_ERROR_1: IntCounter = try_create_int_counter(
        "tx_status_error_1",
        "TxStatus error 1: ReadRPC success, NEAR RPC error"
    ).unwrap();

    pub(crate) static ref EXPERIMENTAL_TX_STATUS_ERROR_2: IntCounter = try_create_int_counter(
        "tx_status_error_2",
        "TxStatus error 2: ReadRPC error, NEAR RPC success"
    ).unwrap();

    pub(crate) static ref EXPERIMENTAL_TX_STATUS_ERROR_3: IntCounter = try_create_int_counter(
        "tx_status_error_3",
        "TxStatus error 3: ReadRPC error, NEAR RPC error"
    ).unwrap();

    pub(crate) static ref EXPERIMENTAL_TX_STATUS_ERROR_4: IntCounter = try_create_int_counter(
        "tx_status_error_4",
        "TxStatus error 4: Failed to compare. Network or parsing error"
    ).unwrap();
    // end TX_STATUS
}

lazy_static! {
    // CHANGES_IN_BLOCK_BY_TYPE
    pub(crate) static ref CHANGES_IN_BLOCK_BY_TYPE_ERROR_0: IntCounter = try_create_int_counter(
        "changes_in_block_by_type_error_0",
        "ChangesInBlockByType error 0: ReadRPC success, NEAR RPC success"
    ).unwrap();

    pub(crate) static ref CHANGES_IN_BLOCK_BY_TYPE_ERROR_1: IntCounter = try_create_int_counter(
        "changes_in_block_by_type_error_1",
        "ChangesInBlockByType error 1: ReadRPC success, NEAR RPC error"
    ).unwrap();

    pub(crate) static ref CHANGES_IN_BLOCK_BY_TYPE_ERROR_2: IntCounter = try_create_int_counter(
        "changes_in_block_by_type_error_2",
        "ChangesInBlockByType error 2: ReadRPC error, NEAR RPC success"
    ).unwrap();

    pub(crate) static ref CHANGES_IN_BLOCK_BY_TYPE_ERROR_3: IntCounter = try_create_int_counter(
        "changes_in_block_by_type_error_3",
        "ChangesInBlockByType error 3: ReadRPC error, NEAR RPC error"
    ).unwrap();

    pub(crate) static ref CHANGES_IN_BLOCK_BY_TYPE_ERROR_4: IntCounter = try_create_int_counter(
        "changes_in_block_by_type_error_4",
        "ChangesInBlockByType error 4: Failed to compare. Network or parsing error"
    ).unwrap();
    // end CHANGES_IN_BLOCK_BY_TYPE
}

lazy_static! {
    // CHANGES_IN_BLOCK
    pub(crate) static ref CHANGES_IN_BLOCK_ERROR_0: IntCounter = try_create_int_counter(
        "changes_in_block_error_0",
        "ChangesInBlock error 0: ReadRPC success, NEAR RPC success"
    ).unwrap();

    pub(crate) static ref CHANGES_IN_BLOCK_ERROR_1: IntCounter = try_create_int_counter(
        "changes_in_block_error_1",
        "ChangesInBlock error 1: ReadRPC success, NEAR RPC error"
    ).unwrap();

    pub(crate) static ref CHANGES_IN_BLOCK_ERROR_2: IntCounter = try_create_int_counter(
        "changes_in_block_error_2",
        "ChangesInBlock error 2: ReadRPC error, NEAR RPC success"
    ).unwrap();

    pub(crate) static ref CHANGES_IN_BLOCK_ERROR_3: IntCounter = try_create_int_counter(
        "changes_in_block_error_3",
        "ChangesInBlock error 3: ReadRPC error, NEAR RPC error"
    ).unwrap();

    pub(crate) static ref CHANGES_IN_BLOCK_ERROR_4: IntCounter = try_create_int_counter(
        "changes_in_block_error_4",
        "ChangesInBlock error 4: Failed to compare. Network or parsing error"
    ).unwrap();
    // end CHANGES_IN_BLOCK
}

lazy_static! {
    // RECEIPT
    pub(crate) static ref RECEIPT_ERROR_0: IntCounter = try_create_int_counter(
        "receipt_error_0",
        "Receipt error 0: ReadRPC success, NEAR RPC success"
    ).unwrap();

    pub(crate) static ref RECEIPT_ERROR_1: IntCounter = try_create_int_counter(
        "receipt_error_1",
        "Receipt error 1: ReadRPC success, NEAR RPC error"
    ).unwrap();

    pub(crate) static ref RECEIPT_ERROR_2: IntCounter = try_create_int_counter(
        "receipt_error_2",
        "Receipt error 2: ReadRPC error, NEAR RPC success"
    ).unwrap();

    pub(crate) static ref RECEIPT_ERROR_3: IntCounter = try_create_int_counter(
        "receipt_error_3",
        "Receipt error 3: ReadRPC error, NEAR RPC error"
    ).unwrap();

    pub(crate) static ref RECEIPT_ERROR_4: IntCounter = try_create_int_counter(
        "receipt_error_4",
        "Receipt error 4: Failed to compare. Network or parsing error"
    ).unwrap();
    // end RECEIPT
}

lazy_static! {
    // ARCHIVAL PROXY CALL COUNTERS
    pub(crate) static ref ARCHIVAL_PROXY_QUERY_VIEW_STATE_WITH_INCLUDE_PROOFS: IntCounter = try_create_int_counter(
        "archive_proxy_query_view_state_with_include_proofs",
        "Total number of the request to the archive nodes query_view_state with include_proofs"
    ).unwrap();
    pub(crate) static ref ARCHIVAL_PROXY_LIGHT_CLIENT_PROOF: IntCounter = try_create_int_counter(
        "archive_proxy_light_client_proof",
        "Total number of the request to the archive nodes light_client_proof"
    ).unwrap();
    pub(crate) static ref ARCHIVAL_PROXY_NEXT_LIGHT_CLIENT_BLOCK: IntCounter = try_create_int_counter(
        "archive_proxy_next_light_client_block",
        "Total number of the request to the archive nodes next_light_client_block"
    ).unwrap();
}

/// Exposes prometheus metrics
#[get("/metrics")]
pub(crate) async fn get_metrics() -> impl Responder {
    let encoder = prometheus::TextEncoder::new();

    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&prometheus::gather(), &mut buffer) {
        tracing::error!("could not encode metrics: {}", e);
    };

    String::from_utf8(buffer.clone()).unwrap_or_else(|err| {
        tracing::error!("custom metrics could not be from_utf8'd: {}", err);
        String::default()
    })
}
