use actix_web::{get, Responder};
use prometheus::{Encoder, IntCounter, IntGauge, Opts};

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

lazy_static! {
    pub(crate) static ref OPTIMISTIC_REQUESTS_TOTAL: IntCounter = try_create_int_counter(
        "total_optimistic_requests",
        "Total number of the request where finality was set to optimistic"
    )
    .unwrap();
    pub(crate) static ref SYNC_CHECKPOINT_REQUESTS_TOTAL: IntCounter = try_create_int_counter(
        "total_sync_checkpoint_requests",
        "Total number of the request where set sync_checkpoint"
    )
    .unwrap();
    pub(crate) static ref FINAL_BLOCK_HEIGHT: IntGauge = try_create_int_gauge(
        "final_block_height",
        "The final block height from the perspective of the READ RPC server"
    )
    .unwrap();

    // requests total counters
    // query requests counters
    pub(crate) static ref QUERY_VIEW_ACCOUNT_REQUESTS_TOTAL: IntCounter = try_create_int_counter(
        "query_view_account_requests_counter",
        "Total number requests to the query view account endpoint"
    )
    .unwrap();
    pub(crate) static ref QUERY_VIEW_CODE_REQUESTS_TOTAL: IntCounter = try_create_int_counter(
        "query_view_code_requests_counter",
        "Total number requests to the query view code endpoint"
    )
    .unwrap();
    pub(crate) static ref QUERY_VIEW_ACCESS_KEY_REQUESTS_TOTAL: IntCounter = try_create_int_counter(
        "query_view_access_key_requests_counter",
        "Total number requests to the query view access key endpoint"
    ).unwrap();
    pub(crate) static ref QUERY_VIEW_STATE_REQUESTS_TOTAL: IntCounter = try_create_int_counter(
        "query_view_state_requests_counter",
        "Total number requests to the query view state endpoint"
    ).unwrap();
    pub(crate) static ref QUERY_FUNCTION_CALL_REQUESTS_TOTAL: IntCounter = try_create_int_counter(
        "query_function_call_requests_counter",
        "Total number requests to the query function call endpoint"
    ).unwrap();
    pub(crate) static ref QUERY_VIEW_ACCESS_KEYS_LIST_REQUESTS_TOTAL: IntCounter = try_create_int_counter(
        "query_access_keys_list_requests_counter",
        "Total number requests to the query access keys list endpoint"
    ).unwrap();

    // blocks requests counters
    pub(crate) static ref BLOCK_REQUESTS_TOTAL: IntCounter = try_create_int_counter(
        "block_requests_counter",
        "Total number requests to the block endpoint"
    ).unwrap();
    pub(crate) static ref CHNGES_IN_BLOCK_BY_TYPE_REQUESTS_TOTAL: IntCounter = try_create_int_counter(
        "changes_in_block_by_type_requests_counter",
        "Total number requests to the changes in block by type endpoint"
    ).unwrap();
    pub(crate) static ref CHNGES_IN_BLOCK_REQUESTS_TOTAL: IntCounter = try_create_int_counter(
        "changes_in_block_requests_counter",
        "Total number requests to the changes in block endpoint"
    ).unwrap();
    pub(crate) static ref CHUNK_REQUESTS_TOTAL: IntCounter = try_create_int_counter(
        "chunk_requests_counter",
        "Total number requests to the chunk endpoint"
    ).unwrap();

    // transactions requests counters
    pub(crate) static ref TX_REQUESTS_TOTAL: IntCounter = try_create_int_counter(
        "tx_requests_counter",
        "Total number requests to the tx endpoint"
    ).unwrap();
    pub(crate) static ref TX_STATUS_REQUESTS_TOTAL: IntCounter = try_create_int_counter(
        "tx_status_requests_counter",
        "Total number requests to the tx status endpoint"
    ).unwrap();
    pub(crate) static ref RECEIPT_REQUESTS_TOTAL: IntCounter = try_create_int_counter(
        "receipt_requests_counter",
        "Total number requests to the receipt endpoint"
    ).unwrap();

    // Error proxies counters
    // query proxies counters
    pub(crate) static ref QUERY_VIEW_ACCOUNT_PROXIES_TOTAL: IntCounter = try_create_int_counter(
        "query_view_account_error_proxies_counter",
        "Total number proxies requests to the query view account endpoint"
    )
    .unwrap();
    pub(crate) static ref QUERY_VIEW_CODE_PROXIES_TOTAL: IntCounter = try_create_int_counter(
        "query_view_code_error_proxies_counter",
        "Total number proxies requests to the query view code endpoint"
    )
    .unwrap();
    pub(crate) static ref QUERY_VIEW_ACCESS_KEY_PROXIES_TOTAL: IntCounter = try_create_int_counter(
        "query_view_access_key_error_proxies_counter",
        "Total number proxies requests to the query view access key endpoint"
    ).unwrap();
    pub(crate) static ref QUERY_VIEW_STATE_PROXIES_TOTAL: IntCounter = try_create_int_counter(
        "query_view_state_error_proxies_counter",
        "Total number proxies requests to the query view state endpoint"
    ).unwrap();
    pub(crate) static ref QUERY_FUNCTION_CALL_PROXIES_TOTAL: IntCounter = try_create_int_counter(
        "query_function_call_error_proxies_counter",
        "Total number proxies requests to the query function call endpoint"
    ).unwrap();
    pub(crate) static ref QUERY_VIEW_ACCESS_KEYS_LIST_PROXIES_TOTAL: IntCounter = try_create_int_counter(
        "query_access_keys_list_error_proxies_counter",
        "Total number proxies requests to the query access keys list endpoint"
    ).unwrap();

    // blocks proxies counters
    pub(crate) static ref BLOCK_PROXIES_TOTAL: IntCounter = try_create_int_counter(
        "block_error_proxies_counter",
        "Total number proxies requests to the block endpoint"
    ).unwrap();
    pub(crate) static ref CHNGES_IN_BLOCK_BY_TYPE_PROXIES_TOTAL: IntCounter = try_create_int_counter(
        "changes_in_block_by_type_error_proxies_counter",
        "Total number proxies requests to the changes in block by type endpoint"
    ).unwrap();
    pub(crate) static ref CHNGES_IN_BLOCK_PROXIES_TOTAL: IntCounter = try_create_int_counter(
        "changes_in_block_error_proxies_counter",
        "Total number proxies requests to the changes in block endpoint"
    ).unwrap();
    pub(crate) static ref CHUNK_PROXIES_TOTAL: IntCounter = try_create_int_counter(
        "chunk_error_proxies_counter",
        "Total number proxies requests to the chunk endpoint"
    ).unwrap();

    // transactions proxies counters
    pub(crate) static ref TX_PROXIES_TOTAL: IntCounter = try_create_int_counter(
        "tx_error_proxies_counter",
        "Total number proxies requests to the tx endpoint"
    ).unwrap();
    pub(crate) static ref TX_STATUS_PROXIES_TOTAL: IntCounter = try_create_int_counter(
        "tx_status_error_proxies_counter",
        "Total number proxies requests to the tx status endpoint"
    ).unwrap();
    pub(crate) static ref RECEIPT_PROXIES_TOTAL: IntCounter = try_create_int_counter(
        "receipt_error_proxies_counter",
        "Total number proxies requests to the receipt endpoint"
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

    match String::from_utf8(buffer.clone()) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!("custom metrics could not be from_utf8'd: {}", e);
            String::default()
        }
    }
}
