use actix_web::{get, Responder};
use prometheus::{Encoder, IntCounterVec, IntGaugeVec, Opts};

type Result<T, E> = std::result::Result<T, E>;

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

fn register_int_gauge_vec(
    name: &str,
    help: &str,
    label_names: &[&str],
) -> Result<IntGaugeVec, prometheus::Error> {
    let opts = Opts::new(name, help);
    let counter = IntGaugeVec::new(opts, label_names)?;
    prometheus::register(Box::new(counter.clone()))?;
    Ok(counter)
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
    pub(crate) static ref FINALITIY_BLOCKS_HEIGHT: IntGaugeVec = register_int_gauge_vec(
        "finality_blocks_height",
        "Block height by finality",
        &["block_type"] // This declares a label named `block_type`
    ).unwrap();

    pub(crate) static ref METHODS_CALLS_COUNTER: IntCounterVec = register_int_counter_vec(
        "methods_calls_counter",
        "Total number of calls to the method",
        &["method_name"] // This declares a label named `method name`
    ).unwrap();

    pub(crate) static ref OPTIMISTIC_UPDATING: OptimisticUpdating = OptimisticUpdating::new();

    pub(crate) static ref REQUESTS_COUNTER: IntCounterVec = register_int_counter_vec(
        "requests_counter",
        "Total number of requests",
        &["request_type"] // This declares a label named `request_type`
    ).unwrap();

    // Error metrics
    // 0: ReadRPC success, NEAR RPC success"
    // 1: ReadRPC success, NEAR RPC error"
    // 2: ReadRPC error, NEAR RPC success"
    // 3: ReadRPC error, NEAR RPC error"
    // 4: Failed to compare. Network or parsing error"

    // QUERY
    pub(crate) static ref QUERY_VIEW_ACCOUNT_ERRORS: IntCounterVec = register_int_counter_vec(
        "query_view_account_errors",
        "Total number of errors in Query.view_account",
        &["error_type"]
    ).unwrap();
    pub(crate) static ref QUERY_VIEW_CODE_ERRORS: IntCounterVec = register_int_counter_vec(
        "query_view_code_errors",
        "Total number of errors in Query.view_code",
        &["error_type"]
    ).unwrap();
    pub(crate) static ref QUERY_VIEW_ACCESS_KEY_ERRORS: IntCounterVec = register_int_counter_vec(
        "query_view_access_key_errors",
        "Total number of errors in Query.view_access_key",
        &["error_type"]
    ).unwrap();
    pub(crate) static ref QUERY_VIEW_STATE_ERRORS: IntCounterVec = register_int_counter_vec(
        "query_view_state_errors",
        "Total number of errors in Query.view_state",
        &["error_type"]
    ).unwrap();
    pub(crate) static ref QUERY_FUNCTION_CALL_ERRORS: IntCounterVec = register_int_counter_vec(
        "query_function_call_errors",
        "Total number of errors in Query.function_call",
        &["error_type"]
    ).unwrap();
    pub(crate) static ref QUERY_VIEW_ACCESS_KEY_LIST_ERRORS: IntCounterVec = register_int_counter_vec(
        "query_view_access_key_list_errors",
        "Total number of errors in Query.view_access_key_list",
        &["error_type"]
    ).unwrap();

    // BLOCK
    pub(crate) static ref BLOCK_ERRORS: IntCounterVec = register_int_counter_vec(
        "block_errors",
        "Total number of errors in Block",
        &["error_type"]
    ).unwrap();

    // CHUNK
    pub(crate) static ref CHUNK_ERRORS: IntCounterVec = register_int_counter_vec(
        "chunk_errors",
        "Total number of errors in Chunk",
        &["error_type"]
    ).unwrap();

    // GAS_PRICE
    pub(crate) static ref GAS_PRICE_ERRORS: IntCounterVec = register_int_counter_vec(
        "gas_price_errors",
        "Total number of errors in GasPrice",
        &["error_type"]
    ).unwrap();

    // PROTOCOL_CONFIG
    pub(crate) static ref PROTOCOL_CONFIG_ERRORS: IntCounterVec = register_int_counter_vec(
        "protocol_config_errors",
        "Total number of errors in ProtocolConfig",
        &["error_type"]
    ).unwrap();

    // VALIDATORS
    pub(crate) static ref VALIDATORS_ERRORS: IntCounterVec = register_int_counter_vec(
        "validators_errors",
        "Total number of errors in Validators",
        &["error_type"]
    ).unwrap();

    // TX
    pub(crate) static ref TX_ERRORS: IntCounterVec = register_int_counter_vec(
        "tx_errors",
        "Total number of errors in Tx",
        &["error_type"]
    ).unwrap();

    // TX_STATUS
    pub(crate) static ref TX_STATUS_ERRORS: IntCounterVec = register_int_counter_vec(
        "tx_status_errors",
        "Total number of errors in TxStatus",
        &["error_type"]
    ).unwrap();

    // CHANGES_IN_BLOCK_BY_TYPE
    pub(crate) static ref CHANGES_IN_BLOCK_BY_TYPE_ERRORS: IntCounterVec = register_int_counter_vec(
        "changes_in_block_by_type_errors",
        "Total number of errors in ChangesInBlockByType",
        &["error_type"]
    ).unwrap();

    // CHANGES_IN_BLOCK
    pub(crate) static ref CHANGES_IN_BLOCK_ERRORS: IntCounterVec = register_int_counter_vec(
        "changes_in_block_errors",
        "Total number of errors in ChangesInBlock",
        &["error_type"]
    ).unwrap();

    // RECEIPT
    pub(crate) static ref RECEIPT_ERRORS: IntCounterVec = register_int_counter_vec(
        "receipt_errors",
        "Total number of errors in Receipt",
        &["error_type"]
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
