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
    pub(crate) static ref FINAL_BLOCK_HEIGHT: IntGauge = try_create_int_gauge(
        "final_block_height",
        "The final block height from the perspective of the READ RPC server"
    )
    .unwrap();
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
