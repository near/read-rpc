use crate::errors::RPCError;
use jsonrpc_v2::Params;
use serde_json::Value;

pub async fn status(
    Params(_params): Params<Value>,
) -> Result<
    near_jsonrpc_primitives::types::status::RpcStatusResponse,
    // near_jsonrpc_primitives::types::status::RpcStatusError,
    RPCError,
> {
    Err(RPCError::unimplemented_error(
        "This method is not implemented yet",
    ))
}

pub async fn network_info(
    Params(_params): Params<Value>,
) -> Result<
    near_jsonrpc_primitives::types::network_info::RpcNetworkInfoResponse,
    // near_jsonrpc_primitives::types::network_info::RpcNetworkInfoError,
    RPCError,
> {
    Err(RPCError::unimplemented_error(
        "This method is not implemented yet",
    ))
}

pub async fn validators(
    Params(_params): Params<near_jsonrpc_primitives::types::validator::RpcValidatorRequest>,
) -> Result<
    near_jsonrpc_primitives::types::validator::RpcValidatorResponse,
    // near_jsonrpc_primitives::types::validator::RpcValidatorError,
    RPCError,
> {
    Err(RPCError::unimplemented_error(
        "This method is not implemented yet",
    ))
}
