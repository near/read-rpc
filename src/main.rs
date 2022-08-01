use clap::Parser;
use config::Cli;
use jsonrpc_v2::{Data, Error, Params, Server};
use serde_json::Value;
mod config;

async fn block(
    Params(params): Params<near_jsonrpc_primitives::types::blocks::RpcBlockRequest>,
) -> Result<
    near_jsonrpc_primitives::types::blocks::RpcBlockResponse,
    Error, // near_jsonrpc_primitives::types::blocks::RpcBlockError,
> {
    unreachable!("This method is not implemented yet")
}

async fn send_tx_async(
    // Params(params): Params<near_jsonrpc_primitives::types::transactions::RpcBroadcastTransactionRequest>
    Params(params): Params<Value>,
) -> Result<
    Value,
    // CryptoHash,
    Error,
> {
    unreachable!("This method is not implemented yet")
}

async fn send_tx_commit(
    // Params(params): Params<near_jsonrpc_primitives::types::transactions::RpcBroadcastTransactionRequest>
    Params(params): Params<Value>,
) -> Result<
    near_jsonrpc_primitives::types::transactions::RpcTransactionResponse,
    // near_jsonrpc_primitives::types::transactions::RpcTransactionError,
    Error,
> {
    unreachable!("This method is not implemented yet")
}

async fn chunk(
    Params(params): Params<near_jsonrpc_primitives::types::chunks::RpcChunkRequest>,
) -> Result<
    near_jsonrpc_primitives::types::chunks::RpcChunkResponse,
    // near_jsonrpc_primitives::types::chunks::RpcChunkError,
    Error,
> {
    unreachable!("This method is not implemented yet")
}

async fn gas_price(
    Params(params): Params<near_jsonrpc_primitives::types::gas_price::RpcGasPriceRequest>,
) -> Result<
    near_jsonrpc_primitives::types::gas_price::RpcGasPriceResponse,
    // near_jsonrpc_primitives::types::gas_price::RpcGasPriceError,
    Error,
> {
    unreachable!("This method is not implemented yet")
}

async fn health(
    Params(params): Params<Value>,
) -> Result<
    near_jsonrpc_primitives::types::status::RpcHealthResponse,
    // near_jsonrpc_primitives::types::status::RpcStatusError,
    Error,
> {
    unreachable!("This method is not implemented yet")
}

async fn light_client_execution_outcome_proof(
    Params(params): Params<
        near_jsonrpc_primitives::types::light_client::RpcLightClientExecutionProofRequest,
    >,
) -> Result<
    near_jsonrpc_primitives::types::light_client::RpcLightClientExecutionProofResponse,
    // near_jsonrpc_primitives::types::light_client::RpcLightClientProofError,
    Error,
> {
    unreachable!("This method is not implemented yet")
}

async fn next_light_client_block(
    Params(params): Params<
        near_jsonrpc_primitives::types::light_client::RpcLightClientNextBlockRequest,
    >,
) -> Result<
    near_jsonrpc_primitives::types::light_client::RpcLightClientNextBlockResponse,
    // near_jsonrpc_primitives::types::light_client::RpcLightClientNextBlockError,
    Error,
> {
    unreachable!("This method is not implemented yet")
}

async fn network_info(
    Params(params): Params<Value>,
) -> Result<
    near_jsonrpc_primitives::types::network_info::RpcNetworkInfoResponse,
    // near_jsonrpc_primitives::types::network_info::RpcNetworkInfoError,
    Error,
> {
    unreachable!("This method is not implemented yet")
}

async fn query(
    Params(params): Params<near_jsonrpc_primitives::types::query::RpcQueryRequest>,
) -> Result<
    near_jsonrpc_primitives::types::query::RpcQueryResponse,
    // near_jsonrpc_primitives::types::query::RpcQueryError,
    Error,
> {
    unreachable!("This method is not implemented yet")
}

async fn status(
    Params(params): Params<Value>,
) -> Result<
    near_jsonrpc_primitives::types::status::RpcStatusResponse,
    // near_jsonrpc_primitives::types::status::RpcStatusError,
    Error,
> {
    unreachable!("This method is not implemented yet")
}

async fn tx_status_common(
    Params(params): Params<
        // near_jsonrpc_primitives::types::transactions::RpcTransactionStatusCommonRequest,
        Value,
    >,
) -> Result<
    near_jsonrpc_primitives::types::transactions::RpcTransactionResponse,
    // near_jsonrpc_primitives::types::transactions::RpcTransactionError,
    Error,
> {
    unreachable!("This method is not implemented yet")
}

async fn validators(
    Params(params): Params<near_jsonrpc_primitives::types::validator::RpcValidatorRequest>,
) -> Result<
    near_jsonrpc_primitives::types::validator::RpcValidatorResponse,
    // near_jsonrpc_primitives::types::validator::RpcValidatorError,
    Error,
> {
    unreachable!("This method is not implemented yet")
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let cli_args = Cli::parse();
    let rpc = Server::new()
        .with_data(Data::new(cli_args))
        .with_method("block", block)
        .with_method("broadcast_tx_async", send_tx_async)
        .with_method("broadcast_tx_commit", send_tx_commit)
        .with_method("chunk", chunk)
        .with_method("gas_price", gas_price)
        .with_method("health", health)
        .with_method("light_client_proof", light_client_execution_outcome_proof)
        .with_method("next_light_client_block", next_light_client_block)
        .with_method("network_info", network_info)
        .with_method("query", query)
        .with_method("status", status)
        .with_method("tx", tx_status_common)
        .with_method("validators", validators)
        .finish();

    actix_web::HttpServer::new(move || {
        let rpc = rpc.clone();
        actix_web::App::new().service(
            actix_web::web::service("/")
                .guard(actix_web::guard::Post())
                .finish(rpc.into_web_service()),
        )
    })
    .bind("0.0.0.0:8888")?
    .run()
    .await
}
