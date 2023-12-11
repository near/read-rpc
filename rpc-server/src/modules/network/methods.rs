use crate::config::ServerContext;
use crate::errors::RPCError;
use crate::modules::blocks::utils::fetch_block_from_cache_or_get;
use crate::modules::network::{friendly_memory_size_format, StatusResponse};
#[cfg(feature = "shadow_data_consistency")]
use crate::utils::shadow_compare_results;
use jsonrpc_v2::{Data, Params};
use near_primitives::types::EpochReference;
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
    Params(params): Params<near_jsonrpc_primitives::types::validator::RpcValidatorRequest>,
) -> Result<near_jsonrpc_primitives::types::validator::RpcValidatorResponse, RPCError> {
    tracing::debug!("`gas_price` called with parameters: {:?}", params);
    crate::metrics::VALIDATORS_REQUESTS_TOTAL.inc();
    let validator_info = validators_call(&data, &params).await;

    #[cfg(feature = "shadow_data_consistency")]
    {
        let near_rpc_client = &data.near_rpc_client.clone();
        let meta_data = format!("{:?}", params);
        let (read_rpc_response_json, is_response_ok) = match &validator_info {
            Ok(validators) => (serde_json::to_value(&validators), true),
            Err(err) => (serde_json::to_value(err), false),
        };
        let comparison_result = shadow_compare_results(
            read_rpc_response_json,
            near_rpc_client.clone(),
            params,
            is_response_ok,
        )
        .await;
        match comparison_result {
            Ok(_) => {
                tracing::info!(target: "shadow_data_consistency", "Shadow data check: CORRECT\n{}", meta_data);
            }
            Err(err) => {
                crate::utils::capture_shadow_consistency_error!(err, meta_data, "VALIDATORS")
            }
        }
    }

    Ok(
        near_jsonrpc_primitives::types::validator::RpcValidatorResponse {
            validator_info: validator_info
                .map_err(near_jsonrpc_primitives::errors::RpcError::from)?,
        },
    )
}

pub async fn validators_ordered(
    data: Data<ServerContext>,
    Params(params): Params<near_jsonrpc_primitives::types::validator::RpcValidatorsOrderedRequest>,
) -> Result<near_jsonrpc_primitives::types::validator::RpcValidatorsOrderedResponse, RPCError> {
    Ok(data.near_rpc_client.call(params).await?)
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
    let config_view = protocol_config_call(data, params.block_reference)
        .await
        .map_err(near_jsonrpc_primitives::errors::RpcError::from)?;
    Ok(near_jsonrpc_primitives::types::config::RpcProtocolConfigResponse { config_view })
}

async fn validators_call(
    data: &Data<ServerContext>,
    validator_request: &near_jsonrpc_primitives::types::validator::RpcValidatorRequest,
) -> Result<
    near_primitives::views::EpochValidatorInfo,
    near_jsonrpc_primitives::types::validator::RpcValidatorError,
> {
    let epoch_id = match &validator_request.epoch_reference {
        EpochReference::EpochId(epoch_id) => epoch_id.0,
        EpochReference::BlockId(block_id) => {
            let block_reference = near_primitives::types::BlockReference::BlockId(block_id.clone());
            let block = fetch_block_from_cache_or_get(data, block_reference)
                .await
                .map_err(|_err| {
                    near_jsonrpc_primitives::types::validator::RpcValidatorError::UnknownEpoch
                })?;
            block.epoch_id
        }
        EpochReference::Latest => {
            crate::metrics::OPTIMISTIC_REQUESTS_TOTAL.inc();
            let block_reference = near_primitives::types::BlockReference::Finality(
                near_primitives::types::Finality::Final,
            );
            let block = fetch_block_from_cache_or_get(data, block_reference)
                .await
                .map_err(|_err| {
                    near_jsonrpc_primitives::types::validator::RpcValidatorError::UnknownEpoch
                })?;
            block.epoch_id
        }
    };
    let validators = data
        .db_manager
        .get_validators_by_epoch_id(epoch_id)
        .await
        .map_err(|_err| {
            near_jsonrpc_primitives::types::validator::RpcValidatorError::ValidatorInfoUnavailable
        })?;
    Ok(validators.validators_info)
}

async fn protocol_config_call(
    data: Data<ServerContext>,
    block_reference: near_primitives::types::BlockReference,
) -> Result<
    near_chain_configs::ProtocolConfigView,
    near_jsonrpc_primitives::types::config::RpcProtocolConfigError,
> {
    let block = fetch_block_from_cache_or_get(&data, block_reference)
        .await
        .map_err(|err| {
            near_jsonrpc_primitives::types::config::RpcProtocolConfigError::UnknownBlock {
                error_message: err.to_string(),
            }
        })?;
    let protocol_config = data
        .db_manager
        .get_protocol_config_by_epoch_id(block.epoch_id)
        .await
        .map_err(|err| {
            near_jsonrpc_primitives::types::config::RpcProtocolConfigError::InternalError {
                error_message: err.to_string(),
            }
        })?;
    Ok(protocol_config)
}
