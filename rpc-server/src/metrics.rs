use actix_web::{get, Responder};
use prometheus::{Encoder, IntCounterVec, IntGauge, IntGaugeVec, Opts};

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

fn try_create_int_gauge(name: &str, help: &str) -> Result<IntGauge, prometheus::Error> {
    let opts = Opts::new(name, help);
    let gauge = IntGauge::with_opts(opts)?;
    prometheus::register(Box::new(gauge.clone()))?;
    Ok(gauge)
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
        OPTIMISTIC_STATUS.set(0);
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
        OPTIMISTIC_STATUS.set(1);
    }

    // Set optimistic updating as working
    pub fn set_working(&self) {
        self.set(false);
        OPTIMISTIC_STATUS.set(0);
    }
}

// Help struct to store the RPC methods for the server
// This is used to store the methods that are available for the server
// It should be present of METHOD_CALLS_COUNTER metrics
pub struct RpcMethods {
    methods: futures_locks::RwLock<std::collections::HashMap<String, String>>,
}

impl RpcMethods {
    pub fn new() -> Self {
        Self {
            methods: futures_locks::RwLock::new(std::collections::HashMap::new()),
        }
    }

    // Insert all rpc methods to the hashmap after init the server
    pub async fn insert(&self, methods_map: Vec<String>) {
        for method_name in methods_map {
            self.methods
                .write()
                .await
                .insert(method_name.clone(), method_name);
        }
    }

    // Method to get the method name from the hashmap
    pub async fn get(&self, method_name: &str) -> Option<String> {
        self.methods.read().await.get(method_name).cloned()
    }
}

// Is not a metric, but a global variable to track the optimistic updating status
lazy_static! {
    pub(crate) static ref OPTIMISTIC_UPDATING: OptimisticUpdating = OptimisticUpdating::new();
    // Initialize the RPC methods hashmap
    pub(crate) static ref RPC_METHODS: RpcMethods = RpcMethods::new();
}

// Metrics
lazy_static! {
    pub(crate) static ref LATEST_BLOCK_HEIGHT_BY_FINALITIY: IntGaugeVec = register_int_gauge_vec(
        "latest_block_height_by_finality",
        "Latest block height by finality",
        &["block_type"] // This declares a label named `block_type`
    ).unwrap();

    pub(crate) static ref METHOD_CALLS_COUNTER: IntCounterVec = register_int_counter_vec(
        "method_calls_counter",
        "Total number of calls to the method",
        &["method_name"] // This declares a label named `method name`
    ).unwrap();

    pub(crate) static ref METHOD_ERRORS_TOTAL: IntCounterVec = register_int_counter_vec(
        "method_errors_total",
        "Total number of errors for method",
        &["method_name", "error_type"] // This declares a label named `method_name` and `error_type`
    ).unwrap();

    pub(crate) static ref TOTAL_REQUESTS_COUNTER: IntCounterVec = register_int_counter_vec(
        "total_requests_counter",
        "Total number of method requests by type",
        &["method_name", "request_type"] // This declares a label named `method_name` and `request_type`
    ).unwrap();

    pub(crate) static ref OPTIMISTIC_STATUS: IntGauge = try_create_int_gauge(
        "optimistic_status",
        "Optimistic updating status. 0: working, 1: not working",
    ).unwrap();

    pub(crate) static ref LEGACY_DATABASE_TX_DETAILS: IntCounterVec = register_int_counter_vec(
        "legacy_database_tx_details",
        "Total number of calls to the legacy database for transaction details",
        // This declares a label named `lookup_type` to differentiate "finished" and "in_progress" transaction lookups
        &["lookup_type"]
    ).unwrap();

    // Error metrics
    // 0: ReadRPC success, NEAR RPC success"
    // 1: ReadRPC success, NEAR RPC error"
    // 2: ReadRPC error, NEAR RPC success"
    // 3: ReadRPC error, NEAR RPC error"
    // 4: Failed to compare. Network or parsing error"
    pub(crate) static ref REQUESTS_ERRORS: IntCounterVec = register_int_counter_vec(
        "requests_methods_errors",
        "Total number of errors for method with code",
        &["method", "error_type"]
    ).unwrap();

}

/// Help method to increment block category metrics
/// Main idea is to have a single place to increment metrics
/// It should help to analyze the most popular requests
/// And build s better caching strategy
pub async fn increase_request_category_metrics(
    data: &jsonrpc_v2::Data<crate::config::ServerContext>,
    block_reference: &near_primitives::types::BlockReference,
    method_name: &str,
    block_height: Option<u64>,
) {
    match block_reference {
        near_primitives::types::BlockReference::BlockId(_) => {
            let final_block = data.blocks_info_by_finality.final_cache_block().await;
            let expected_earliest_available_block =
                final_block.block_height - 5 * data.genesis_info.genesis_config.epoch_length;
            // By default, all requests should be historical, therefore
            // if block_height is None we use `genesis.block_height` by default
            if block_height.unwrap_or(data.genesis_info.genesis_block_cache.block_height)
                > expected_earliest_available_block
            {
                // This is request to regular nodes which includes 5 last epochs
                TOTAL_REQUESTS_COUNTER
                    .with_label_values(&[method_name, "regular"])
                    .inc();
            } else {
                // This is a request to archival nodes which include blocks from genesis (later than 5 epochs ago)
                TOTAL_REQUESTS_COUNTER
                    .with_label_values(&[method_name, "historical"])
                    .inc();
            }
        }
        near_primitives::types::BlockReference::Finality(finality) => {
            // All Finality is requests to regular nodes which includes 5 last epochs
            TOTAL_REQUESTS_COUNTER
                .with_label_values(&[method_name, "regular"])
                .inc();
            match finality {
                // Increase the TOTAL_REQUESTS_COUNTER `final` metric
                // if the request has final finality
                near_primitives::types::Finality::DoomSlug
                | near_primitives::types::Finality::Final => {
                    TOTAL_REQUESTS_COUNTER
                        .with_label_values(&[method_name, "final"])
                        .inc();
                }
                // Increase the TOTAL_REQUESTS_COUNTER `optimistic` metric
                // if the request has optimistic finality
                near_primitives::types::Finality::None => {
                    TOTAL_REQUESTS_COUNTER
                        .with_label_values(&[method_name, "optimistic"])
                        .inc();
                }
            }
        }
        near_primitives::types::BlockReference::SyncCheckpoint(_) => {
            TOTAL_REQUESTS_COUNTER
                .with_label_values(&[method_name, "historical"])
                .inc();
        }
    }
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
