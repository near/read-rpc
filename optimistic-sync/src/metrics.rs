use actix_web::{get, App, HttpServer, Responder};
use lazy_static::lazy_static;
use prometheus::{Encoder, IntCounter, IntGauge, Opts};

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
}

#[get("/metrics")]
async fn get_metrics() -> impl Responder {
    let encoder = prometheus::TextEncoder::new();

    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&prometheus::gather(), &mut buffer) {
        tracing::error!(target: crate::INDEXER, "could not encode metrics: {}", e);
    };

    match String::from_utf8(buffer.clone()) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(target: crate::INDEXER, "custom metrics could not be from_utf8'd: {}", e);
            String::default()
        }
    }
}
