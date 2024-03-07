use actix_web::cookie::time;
use jsonrpc_v2::{Data, Params};

use crate::config::ServerContext;
use crate::errors::RPCError;
use crate::modules::blocks::utils::fetch_block_from_cache_or_get;
use crate::modules::network::parse_validator_request;

pub async fn client_config(
    _data: Data<ServerContext>,
    Params(_params): Params<serde_json::Value>,
) -> Result<(), RPCError> {
    Err(RPCError::unimplemented_error("client_config"))
}

pub async fn maintenance_windows(
    _data: Data<ServerContext>,
    Params(_params): Params<serde_json::Value>,
) -> Result<(), RPCError> {
    Err(RPCError::unimplemented_error("maintenance_windows"))
}

pub async fn split_storage_info(
    _data: Data<ServerContext>,
    Params(_params): Params<serde_json::Value>,
) -> Result<(), RPCError> {
    Err(RPCError::unimplemented_error("split_storage_info"))
}

pub async fn status(
    data: Data<ServerContext>,
    Params(_params): Params<serde_json::Value>,
) -> Result<near_primitives::views::StatusResponse, RPCError> {
    let final_block_info = data.final_block_info.read().await;
    let validators = final_block_info
        .current_validators
        .current_validators
        .iter()
        .map(|validator| near_primitives::views::ValidatorInfo {
            account_id: validator.account_id.clone(),
            is_slashed: validator.is_slashed,
        })
        .collect();

    Ok(near_primitives::views::StatusResponse {
        version: data.version.clone(),
        chain_id: data.genesis_info.genesis_config.chain_id.clone(),
        protocol_version: near_primitives::version::PROTOCOL_VERSION,
        latest_protocol_version: final_block_info.final_block_cache.latest_protocol_version,
        // Address for current read_node RPC server.
        rpc_addr: Some(format!("0.0.0.0:{}", data.server_port)),
        validators,
        sync_info: near_primitives::views::StatusSyncInfo {
            latest_block_hash: final_block_info.final_block_cache.block_hash,
            latest_block_height: final_block_info.final_block_cache.block_height,
            latest_state_root: final_block_info.final_block_cache.state_root,
            latest_block_time: time::OffsetDateTime::from_unix_timestamp_nanos(
                final_block_info.final_block_cache.block_timestamp as i128,
            )
            .expect("Failed to parse timestamp"),
            // Always false because read_node is not need to sync
            syncing: false,
            earliest_block_hash: Some(data.genesis_info.genesis_block_cache.block_hash),
            earliest_block_height: Some(data.genesis_info.genesis_block_cache.block_height),
            earliest_block_time: Some(
                time::OffsetDateTime::from_unix_timestamp_nanos(
                    data.genesis_info.genesis_block_cache.block_timestamp as i128,
                )
                .expect("Failed to parse timestamp"),
            ),
            epoch_id: Some(near_primitives::types::EpochId(
                final_block_info.final_block_cache.epoch_id,
            )),
            epoch_start_height: Some(final_block_info.current_validators.epoch_start_height),
        },
        validator_account_id: None,
        validator_public_key: None,
        // Generate empty public key because read_node is not regular archiver node
        node_public_key: near_crypto::PublicKey::empty(near_crypto::KeyType::ED25519),
        node_key: None,
        // return uptime current read_node
        uptime_sec: chrono::Utc::now().timestamp() - data.boot_time_seconds,
        // Not using for status method
        detailed_debug_status: None,
    })
}

pub async fn health(
    data: Data<ServerContext>,
    Params(_params): Params<serde_json::Value>,
) -> Result<crate::health::RPCHealthStatusResponse, RPCError> {
    // TODO: Improve to return error after implementing optimistic block
    // see nearcore/chain/client/src/client_actor.rs:627 to get details
    Ok(crate::health::RPCHealthStatusResponse::new(&data).await)
}

pub async fn network_info(
    data: Data<ServerContext>,
    Params(_params): Params<serde_json::Value>,
) -> Result<near_jsonrpc_primitives::types::network_info::RpcNetworkInfoResponse, RPCError> {
    Ok(data
        .near_rpc_client
        .call(near_jsonrpc_client::methods::network_info::RpcNetworkInfoRequest)
        .await?)
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

    let store = near_parameters::RuntimeConfigStore::for_chain_id(
        &data.genesis_info.genesis_config.chain_id,
    );
    let runtime_config = store.get_config(block.latest_protocol_version);
    let protocol_config = near_chain_configs::ProtocolConfig {
        genesis_config: data.genesis_info.genesis_config.clone(),
        runtime_config: near_parameters::RuntimeConfig {
            fees: runtime_config.fees.clone(),
            wasm_config: runtime_config.wasm_config.clone(),
            account_creation_config: runtime_config.account_creation_config.clone(),
        },
    };
    Ok(protocol_config.into())
}
