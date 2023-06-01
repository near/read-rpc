use crate::config::ServerContext;
use crate::errors::RPCError;
use crate::modules::network::parse_validator_request;
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
    Params(params): Params<Value>,
) -> Result<near_jsonrpc_primitives::types::validator::RpcValidatorResponse, RPCError> {
    match parse_validator_request(params).await {
        Ok(request) => {
            let validator_info = proxy_rpc_call(&data.near_rpc_client, request).await?;
            Ok(near_jsonrpc_primitives::types::validator::RpcValidatorResponse { validator_info })
        }
        Err(err) => Err(RPCError::parse_error(&err.to_string())),
    }
}

pub async fn validators_ordered(
    data: Data<ServerContext>,
    Params(params): Params<near_jsonrpc_primitives::types::validator::RpcValidatorsOrderedRequest>,
) -> Result<near_jsonrpc_primitives::types::validator::RpcValidatorsOrderedResponse, RPCError> {
    Ok(proxy_rpc_call(&data.near_rpc_client, params).await?)
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
    // Increase the OPTIMISTIC_REQUESTS_TOTAL metric if the request has optimistic finality.
    if let near_primitives::types::BlockReference::Finality(finality) = &params.block_reference {
        // Finality::None stands for optimistic finality.
        if finality == &near_primitives::types::Finality::None {
            crate::metrics::OPTIMISTIC_REQUESTS_TOTAL.inc();
        }
    }
    let config_view = proxy_rpc_call(&data.near_rpc_client, params).await?;
    Ok(near_jsonrpc_primitives::types::config::RpcProtocolConfigResponse { config_view })
}
