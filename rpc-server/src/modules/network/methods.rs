use crate::config::ServerContext;
use crate::errors::RPCError;
use crate::utils::proxy_rpc_call;
use jsonrpc_v2::{Data, Params};
use serde_json::Value;

pub async fn status(
    Params(_params): Params<Value>,
) -> Result<near_jsonrpc_primitives::types::status::RpcStatusResponse, RPCError> {
    Err(RPCError::unimplemented_error(
        "Method is not implemented on this type of node. Please send a request to NEAR JSON RPC instead.",
    ))
}

pub async fn network_info(
    Params(_params): Params<Value>,
) -> Result<near_jsonrpc_primitives::types::network_info::RpcNetworkInfoResponse, RPCError> {
    Err(RPCError::unimplemented_error(
        "Method is not implemented on this type of node. Please send a request to NEAR JSON RPC instead.",
    ))
}

pub async fn validators(
    data: Data<ServerContext>,
    Params(params): Params<near_jsonrpc_primitives::types::validator::RpcValidatorRequest>,
) -> Result<near_jsonrpc_primitives::types::validator::RpcValidatorResponse, RPCError> {
    let validator_info = proxy_rpc_call(&data.near_rpc_client, params).await?;
    Ok(near_jsonrpc_primitives::types::validator::RpcValidatorResponse { validator_info })
}

pub async fn genesis_config(
    data: Data<ServerContext>,
    Params(_params): Params<Value>,
) -> Result<near_chain_configs::GenesisConfig, RPCError> {
    Ok(data.genesis_config.clone())
}

pub async fn protocol_config(
    data: Data<ServerContext>,
    Params(params): Params<near_jsonrpc_primitives::types::config::RpcProtocolConfigRequest>,
) -> Result<near_jsonrpc_primitives::types::config::RpcProtocolConfigResponse, RPCError> {
    let config_view = proxy_rpc_call(&data.near_rpc_client, params).await?;
    Ok(near_jsonrpc_primitives::types::config::RpcProtocolConfigResponse { config_view })
}
