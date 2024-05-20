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

// Is not a metric, but a global variable to track the optimistic updating status
lazy_static! {
    pub(crate) static ref OPTIMISTIC_UPDATING: OptimisticUpdating = OptimisticUpdating::new();
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

    pub(crate) static ref REQUESTS_COUNTER: IntCounterVec = register_int_counter_vec(
        "requests_counter",
        "Total number of requests",
        &["request_type"] // This declares a label named `request_type`
    ).unwrap();

    pub(crate) static ref OPTIMISTIC_STATUS: IntGauge = try_create_int_gauge(
        "optimistic_status",
        "Optimistic updating status. 0: working, 1: not working",
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
