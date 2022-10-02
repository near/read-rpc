use crate::errors::RPCError;
use jsonrpc_v2::Params;

pub async fn gas_price(
    Params(_params): Params<near_jsonrpc_primitives::types::gas_price::RpcGasPriceRequest>,
) -> Result<near_jsonrpc_primitives::types::gas_price::RpcGasPriceResponse, RPCError> {
    Err(RPCError::unimplemented_error(
        "This method is not implemented yet",
    ))
}
