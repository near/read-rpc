use crate::config::ServerContext;
use crate::errors::RPCError;
use crate::modules::blocks::utils::fetch_block_from_cache_or_get;
use crate::modules::network::{
    clone_protocol_config, friendly_memory_size_format, parse_validator_request, StatusResponse,
};
use jsonrpc_v2::{Data, Params};
use sysinfo::{System, SystemExt};

pub async fn status(
    data: Data<ServerContext>,
    Params(_params): Params<serde_json::Value>,
) -> Result<StatusResponse, RPCError> {
    let sys = System::new_all();
    let total_memory = sys.total_memory();
    let used_memory = sys.used_memory();
    let blocks_cache = data.blocks_cache.read().await;
    let contract_code_cache = data.contract_code_cache.read().await;
    let compiled_contract_code_cache = data.compiled_contract_code_cache.local_cache.read().await;
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
            .final_block_info
            .read()
            .await
            .final_block_cache
            .block_height,
    };
    Ok(status)
}

pub async fn network_info(
    Params(_params): Params<serde_json::Value>,
) -> Result<near_jsonrpc_primitives::types::network_info::RpcNetworkInfoResponse, RPCError> {
    Err(RPCError::unimplemented_error(
        "Method is not implemented on this type of node. Please send a request to NEAR JSON RPC instead.",
    ))
}

pub async fn validators(
    data: Data<ServerContext>,
    Params(params): Params<serde_json::Value>,
) -> Result<near_jsonrpc_primitives::types::validator::RpcValidatorResponse, RPCError> {
    let request = parse_validator_request(params)
        .await
        .map_err(|err| RPCError::parse_error(&err.to_string()))?;
    tracing::debug!("`validators` called with parameters: {:?}", request);
    crate::metrics::VALIDATORS_REQUESTS_TOTAL.inc();
    // Latest epoch validators fetches from the Near RPC node
    if let near_primitives::types::EpochReference::Latest = &request.epoch_reference {
        crate::metrics::OPTIMISTIC_REQUESTS_TOTAL.inc();
        let validator_info = data.near_rpc_client.call(request).await?;
        return Ok(
            near_jsonrpc_primitives::types::validator::RpcValidatorResponse { validator_info },
        );
    };

    // Current epoch validators fetches from the Near RPC node
    if let near_primitives::types::EpochReference::EpochId(epoch_id) = &request.epoch_reference {
        if data
            .final_block_info
            .read()
            .await
            .final_block_cache
            .epoch_id
            == epoch_id.0
        {
            let validator_info = data.near_rpc_client.call(request).await?;
            return Ok(
                near_jsonrpc_primitives::types::validator::RpcValidatorResponse { validator_info },
            );
        }
    };

    let validator_info = validators_call(&data, &request).await;

    #[cfg(feature = "shadow_data_consistency")]
    {
        if let Some(err_code) = crate::utils::shadow_compare_results_handler(
            crate::metrics::VALIDATORS_REQUESTS_TOTAL.get(),
            data.shadow_data_consistency_rate,
            &validator_info,
            data.near_rpc_client.clone(),
            request,
            "VALIDATORS",
        )
        .await
        {
            crate::utils::capture_shadow_consistency_error!(err_code, "VALIDATORS")
        };
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
    Params(_params): Params<serde_json::Value>,
) -> Result<near_chain_configs::GenesisConfig, RPCError> {
    Ok(data.genesis_info.genesis_config.clone())
}

pub async fn protocol_config(
    data: Data<ServerContext>,
    Params(params): Params<near_jsonrpc_primitives::types::config::RpcProtocolConfigRequest>,
) -> Result<near_jsonrpc_primitives::types::config::RpcProtocolConfigResponse, RPCError> {
    tracing::debug!(
        "`EXPERIMENTAL_protocol_config` called with parameters: {:?}",
        params
    );
    crate::metrics::PROTOCOL_CONFIG_REQUESTS_TOTAL.inc();

    match params.block_reference {
        near_primitives::types::BlockReference::Finality(_) => {
            crate::metrics::OPTIMISTIC_REQUESTS_TOTAL.inc();
        }
        _ => {}
    }

    let config_view = protocol_config_call(&data, params.block_reference.clone()).await;

    #[cfg(feature = "shadow_data_consistency")]
    {
        if let Some(err_code) = crate::utils::shadow_compare_results_handler(
            crate::metrics::PROTOCOL_CONFIG_REQUESTS_TOTAL.get(),
            data.shadow_data_consistency_rate,
            &config_view,
            data.near_rpc_client.clone(),
            params,
            "PROTOCOL_CONFIG",
        )
        .await
        {
            crate::utils::capture_shadow_consistency_error!(err_code, "PROTOCOL_CONFIG")
        };
    }

    Ok(
        near_jsonrpc_primitives::types::config::RpcProtocolConfigResponse {
            config_view: config_view.map_err(near_jsonrpc_primitives::errors::RpcError::from)?,
        },
    )
}

async fn validators_call(
    data: &Data<ServerContext>,
    validator_request: &near_jsonrpc_primitives::types::validator::RpcValidatorRequest,
) -> Result<
    near_primitives::views::EpochValidatorInfo,
    near_jsonrpc_primitives::types::validator::RpcValidatorError,
> {
    let validators = match &validator_request.epoch_reference {
        near_primitives::types::EpochReference::EpochId(epoch_id) => data
            .db_manager
            .get_validators_by_epoch_id(epoch_id.0)
            .await
            .map_err(|_err| {
                near_jsonrpc_primitives::types::validator::RpcValidatorError::UnknownEpoch
            })?,
        near_primitives::types::EpochReference::BlockId(block_id) => {
            let block_reference = near_primitives::types::BlockReference::BlockId(block_id.clone());
            let block = fetch_block_from_cache_or_get(data, block_reference)
                .await
                .map_err(|_err| {
                    near_jsonrpc_primitives::types::validator::RpcValidatorError::UnknownEpoch
                })?;
            data.db_manager
                .get_validators_by_end_block_height(block.block_height)
                .await.map_err(|_err| {
                near_jsonrpc_primitives::types::validator::RpcValidatorError::ValidatorInfoUnavailable
            })?
        }
        _ => {
            return Err(near_jsonrpc_primitives::types::validator::RpcValidatorError::UnknownEpoch)
        }
    };
    Ok(validators.validators_info)
}

async fn protocol_config_call(
    data: &Data<ServerContext>,
    block_reference: near_primitives::types::BlockReference,
) -> Result<
    near_chain_configs::ProtocolConfigView,
    near_jsonrpc_primitives::types::config::RpcProtocolConfigError,
> {
    let block = fetch_block_from_cache_or_get(data, block_reference)
        .await
        .map_err(|err| {
            near_jsonrpc_primitives::types::config::RpcProtocolConfigError::UnknownBlock {
                error_message: err.to_string(),
            }
        })?;
    let protocol_config = if data
        .final_block_info
        .read()
        .await
        .final_block_cache
        .epoch_id
        == block.epoch_id
    {
        let protocol_config = &data.final_block_info.read().await.current_protocol_config;
        clone_protocol_config(protocol_config)
    } else {
        data.db_manager
            .get_protocol_config_by_epoch_id(block.epoch_id)
            .await
            .map_err(|err| {
                near_jsonrpc_primitives::types::config::RpcProtocolConfigError::InternalError {
                    error_message: err.to_string(),
                }
            })?
    };
    Ok(protocol_config)
}
