use crate::config::ServerContext;
use crate::errors::RPCError;
use jsonrpc_v2::{Data, Params};

pub async fn gas_price(
    data: Data<ServerContext>,
    Params(params): Params<near_jsonrpc_primitives::types::gas_price::RpcGasPriceRequest>,
) -> Result<near_jsonrpc_primitives::types::gas_price::RpcGasPriceResponse, RPCError> {
    let gas_price_view = data.near_rpc_client.call(params).await?;
    Ok(near_jsonrpc_primitives::types::gas_price::RpcGasPriceResponse { gas_price_view })
}
