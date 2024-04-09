use actix_web::{get, App, HttpServer, Responder};

mod metrics;

pub async fn get_final_block(
    near_rpc_client: &near_jsonrpc_client::JsonRpcClient,
    optimistic: bool,
) -> anyhow::Result<near_primitives::views::BlockView> {
    let block_request_method = near_jsonrpc_client::methods::block::RpcBlockRequest {
        block_reference: near_primitives::types::BlockReference::Finality(if optimistic {
            near_primitives::types::Finality::None
        } else {
            near_primitives::types::Finality::Final
        }),
    };
    let block_view = near_rpc_client.call(block_request_method).await?;
    if !optimistic {
        // Updating the metric to expose the block height considered as final by the server
        // this metric can be used to calculate the lag between the server and the network
        // Prometheus Gauge Metric type do not support u64
        // https://github.com/tikv/rust-prometheus/issues/470
        crate::metrics::FINAL_BLOCK_HEIGHT.set(i64::try_from(block_view.header.height)?);
    }
    Ok(block_view)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {

    let client = near_jsonrpc_client::JsonRpcClient::connect(rpc_url);


    HttpServer::new(|| App::new().service(metrics::get_metrics))
        .bind(("0.0.0.0", 8887))?
        .disable_signals()
        .run();
    
    Ok(())
}
