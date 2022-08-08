use jsonrpc_v2::{Error, Params};

pub async fn gas_price(
    Params(_params): Params<near_jsonrpc_primitives::types::gas_price::RpcGasPriceRequest>,
) -> Result<
    near_jsonrpc_primitives::types::gas_price::RpcGasPriceResponse,
    // near_jsonrpc_primitives::types::gas_price::RpcGasPriceError,
    Error,
> {
    unreachable!("This method is not implemented yet")
}
