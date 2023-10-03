use crate::config::ServerContext;
use crate::errors::RPCError;
use crate::modules::network::{
    friendly_memory_size_format, parse_validator_request, StatusResponse,
};
use crate::utils::proxy_rpc_call;
use jsonrpc_v2::{Data, Params};
use serde_json::Value;
use sysinfo::{System, SystemExt};

pub async fn status(
    data: Data<ServerContext>,
    Params(_params): Params<Value>,
) -> Result<StatusResponse, RPCError> {
    let sys = System::new_all();
    let total_memory = sys.total_memory();
    let used_memory = sys.used_memory();
    let blocks_cache = data.blocks_cache.read().unwrap();
    let contract_code_cache = data.contract_code_cache.read().unwrap();
    let compiled_contract_code_cache = data
        .compiled_contract_code_cache
        .local_cache
        .read()
        .unwrap();
    let status = StatusResponse {
        total_memory: friendly_memory_size_format(total_memory as usize),
        used_memory: friendly_memory_size_format(used_memory as usize),
        available_memory: friendly_memory_size_format((total_memory - used_memory) as usize),

        blocks_in_cache: blocks_cache.len(),
        max_blocks_cache_size: friendly_memory_size_format(blocks_cache.max_size()),
        current_blocks_cache_size: friendly_memory_size_format(blocks_cache.current_size()),

        contracts_codes_in_cache: contract_code_cache.len(),
        max_contracts_codes_cache_size: friendly_memory_size_format(contract_code_cache.max_size()),
        current_contracts_codes_cache_size: friendly_memory_size_format(
            contract_code_cache.current_size(),
        ),

        compiled_contracts_codes_in_cache: compiled_contract_code_cache.len(),
        max_compiled_contracts_codes_cache_size: friendly_memory_size_format(
            compiled_contract_code_cache.max_size(),
        ),
        current_compiled_contracts_codes_cache_size: friendly_memory_size_format(
            compiled_contract_code_cache.current_size(),
        ),

        final_block_height: data
            .final_block_height
            .load(std::sync::atomic::Ordering::SeqCst),
    };
    Ok(status)
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
    let config_view = proxy_rpc_call(&data.near_rpc_client, params).await?;
    Ok(near_jsonrpc_primitives::types::config::RpcProtocolConfigResponse { config_view })
}
