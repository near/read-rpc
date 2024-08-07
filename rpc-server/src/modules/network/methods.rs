use jsonrpc_v2::{Data, Params};
use near_jsonrpc::RpcRequest;
use near_primitives::epoch_manager::{AllEpochConfig, EpochConfig};

use crate::config::ServerContext;
use crate::errors::RPCError;
use crate::modules::blocks::utils::fetch_block_from_cache_or_get;

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
    let final_block = data.blocks_info_by_finality.final_cache_block().await;
    let validators = data.blocks_info_by_finality.validators().await;
    let current_validators = validators
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
        latest_protocol_version: final_block.latest_protocol_version,
        // Address for current read_node RPC server.
        rpc_addr: Some(format!("0.0.0.0:{}", data.server_port)),
        validators: current_validators,
        sync_info: near_primitives::views::StatusSyncInfo {
            latest_block_hash: final_block.block_hash,
            latest_block_height: final_block.block_height,
            latest_state_root: final_block.state_root,
            latest_block_time: near_async::time::Utc::from_unix_timestamp_nanos(
                final_block.block_timestamp as i128,
            )
            .unwrap(),
            // Always false because read-rpc does not need to sync
            syncing: false,
            earliest_block_hash: Some(data.genesis_info.genesis_block_cache.block_hash),
            earliest_block_height: Some(data.genesis_info.genesis_block_cache.block_height),
            earliest_block_time: Some(
                near_async::time::Utc::from_unix_timestamp_nanos(
                    data.genesis_info.genesis_block_cache.block_timestamp as i128,
                )
                .unwrap(),
            ),
            epoch_id: Some(near_primitives::types::EpochId(final_block.epoch_id)),
            epoch_start_height: Some(validators.epoch_start_height),
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
        genesis_hash: data.genesis_info.genesis_block_cache.block_hash,
    })
}

pub async fn health(
    data: Data<ServerContext>,
    Params(_params): Params<serde_json::Value>,
) -> Result<crate::health::RPCHealthStatusResponse, RPCError> {
    Ok(crate::health::RPCHealthStatusResponse::new(&data).await)
}

pub async fn network_info(
    data: Data<ServerContext>,
    Params(_params): Params<serde_json::Value>,
) -> Result<near_jsonrpc::primitives::types::network_info::RpcNetworkInfoResponse, RPCError> {
    Ok(data
        .near_rpc_client
        .call(
            near_jsonrpc_client::methods::network_info::RpcNetworkInfoRequest,
            Some("network_info"),
        )
        .await?)
}

pub async fn validators(
    data: Data<ServerContext>,
    Params(params): Params<serde_json::Value>,
) -> Result<near_jsonrpc::primitives::types::validator::RpcValidatorResponse, RPCError> {
    let request = near_jsonrpc::primitives::types::validator::RpcValidatorRequest::parse(params)?;
    tracing::debug!("`validators` called with parameters: {:?}", request);
    // Latest epoch validators fetches from the Near RPC node
    if let near_primitives::types::EpochReference::Latest = &request.epoch_reference {
        let validator_info = data
            .near_rpc_client
            .call(request, Some("validators"))
            .await?;
        return Ok(
            near_jsonrpc::primitives::types::validator::RpcValidatorResponse { validator_info },
        );
    };

    // Current epoch validators fetches from the Near RPC node
    if let near_primitives::types::EpochReference::EpochId(epoch_id) = &request.epoch_reference {
        if data
            .blocks_info_by_finality
            .final_cache_block()
            .await
            .epoch_id
            == epoch_id.0
        {
            let validator_info = data
                .near_rpc_client
                .call(request, Some("validators"))
                .await?;
            return Ok(
                near_jsonrpc::primitives::types::validator::RpcValidatorResponse { validator_info },
            );
        }
    };

    let validator_info = validators_call(&data, &request).await;

    #[cfg(feature = "shadow_data_consistency")]
    {
        crate::utils::shadow_compare_results_handler(
            data.shadow_data_consistency_rate,
            &validator_info,
            data.near_rpc_client.clone(),
            request,
            "validators",
        )
        .await;
    }

    Ok(
        near_jsonrpc::primitives::types::validator::RpcValidatorResponse {
            validator_info: validator_info
                .map_err(near_jsonrpc::primitives::errors::RpcError::from)?,
        },
    )
}

pub async fn validators_ordered(
    data: Data<ServerContext>,
    Params(params): Params<serde_json::Value>,
) -> Result<near_jsonrpc::primitives::types::validator::RpcValidatorsOrderedResponse, RPCError> {
    let request =
        near_jsonrpc::primitives::types::validator::RpcValidatorsOrderedRequest::parse(params)?;

    if let Some(block_id) = &request.block_id {
        let block_reference = near_primitives::types::BlockReference::from(block_id.clone());
        if let Ok(block) = fetch_block_from_cache_or_get(
            &data,
            &block_reference,
            "EXPERIMENTAL_validators_ordered",
        )
        .await
        {
            let final_block = data.blocks_info_by_finality.final_cache_block().await;
            // `expected_earliest_available_block` calculated by formula:
            // `final_block_height` - `node_epoch_count` * `epoch_length`
            // Now near store 5 epochs, it can be changed in the future
            // epoch_length = 43200 blocks
            let expected_earliest_available_block =
                final_block.block_height - 5 * data.genesis_info.genesis_config.epoch_length;
            if block.block_height > expected_earliest_available_block {
                // Proxy to regular rpc if the block is available
                Ok(data
                    .near_rpc_client
                    .call(request, Some("EXPERIMENTAL_validators_ordered"))
                    .await?)
            } else {
                // Proxy to archival rpc if the block garbage collected
                Ok(data
                    .near_rpc_client
                    .archival_call(request, Some("EXPERIMENTAL_validators_ordered"))
                    .await?)
            }
        } else {
            Ok(data
                .near_rpc_client
                .call(request, Some("EXPERIMENTAL_validators_ordered"))
                .await?)
        }
    } else {
        // increase block category metrics
        crate::metrics::increase_request_category_metrics(
            &data,
            &near_primitives::types::BlockReference::Finality(
                near_primitives::types::Finality::Final,
            ),
            "EXPERIMENTAL_validators_ordered",
            None,
        )
        .await;
        Ok(data
            .near_rpc_client
            .call(request, Some("EXPERIMENTAL_validators_ordered"))
            .await?)
    }
}

pub async fn genesis_config(
    data: Data<ServerContext>,
    Params(_params): Params<serde_json::Value>,
) -> Result<near_chain_configs::GenesisConfig, RPCError> {
    Ok(data.genesis_info.genesis_config.clone())
}

pub async fn protocol_config(
    data: Data<ServerContext>,
    Params(params): Params<serde_json::Value>,
) -> Result<near_jsonrpc::primitives::types::config::RpcProtocolConfigResponse, RPCError> {
    tracing::debug!(
        "`EXPERIMENTAL_protocol_config` called with parameters: {:?}",
        params
    );
    let protocol_config_request =
        near_jsonrpc::primitives::types::config::RpcProtocolConfigRequest::parse(params)?;

    let config_view =
        protocol_config_call(&data, protocol_config_request.block_reference.clone()).await;

    #[cfg(feature = "shadow_data_consistency")]
    {
        crate::utils::shadow_compare_results_handler(
            data.shadow_data_consistency_rate,
            &config_view,
            data.near_rpc_client.clone(),
            protocol_config_request,
            "EXPERIMENTAL_protocol_config",
        )
        .await;
    }

    Ok(
        near_jsonrpc::primitives::types::config::RpcProtocolConfigResponse {
            config_view: config_view.map_err(near_jsonrpc::primitives::errors::RpcError::from)?,
        },
    )
}

async fn validators_call(
    data: &Data<ServerContext>,
    validator_request: &near_jsonrpc::primitives::types::validator::RpcValidatorRequest,
) -> Result<
    near_primitives::views::EpochValidatorInfo,
    near_jsonrpc::primitives::types::validator::RpcValidatorError,
> {
    let validators = match &validator_request.epoch_reference {
        near_primitives::types::EpochReference::EpochId(epoch_id) => {
            let validators = data
                .db_manager
                .get_validators_by_epoch_id(epoch_id.0, "validators")
                .await
                .map_err(|_err| {
                    near_jsonrpc::primitives::types::validator::RpcValidatorError::UnknownEpoch
                })?;
            // increase block category metrics
            crate::metrics::increase_request_category_metrics(
                data,
                &near_primitives::types::BlockReference::BlockId(
                    near_primitives::types::BlockId::Height(validators.epoch_start_height),
                ),
                "validators",
                Some(validators.epoch_start_height),
            )
            .await;
            validators
        }
        near_primitives::types::EpochReference::BlockId(block_id) => {
            let block_reference = near_primitives::types::BlockReference::BlockId(block_id.clone());
            let block = fetch_block_from_cache_or_get(data, &block_reference, "validators")
                .await
                .map_err(|_err| {
                    near_jsonrpc::primitives::types::validator::RpcValidatorError::UnknownEpoch
                })?;
            data.db_manager
                .get_validators_by_end_block_height(block.block_height, "validators")
                .await.map_err(|_err| {
                near_jsonrpc::primitives::types::validator::RpcValidatorError::ValidatorInfoUnavailable
            })?
        }
        _ => {
            return Err(near_jsonrpc::primitives::types::validator::RpcValidatorError::UnknownEpoch)
        }
    };
    Ok(validators.validators_info)
}

async fn protocol_config_call(
    data: &Data<ServerContext>,
    block_reference: near_primitives::types::BlockReference,
) -> Result<
    near_chain_configs::ProtocolConfigView,
    near_jsonrpc::primitives::types::config::RpcProtocolConfigError,
> {
    let block =
        fetch_block_from_cache_or_get(data, &block_reference, "EXPERIMENTAL_protocol_config")
            .await
            .map_err(|err| {
                near_jsonrpc::primitives::types::config::RpcProtocolConfigError::UnknownBlock {
                    error_message: err.to_string(),
                }
            })?;
    // Stores runtime config for each protocol version
    // Create store of runtime configs for the given chain id.
    //
    // For mainnet and other chains except testnet we don't need to override runtime config for
    // first protocol versions.
    // For testnet, runtime config for genesis block was (incorrectly) different, that's why we
    // need to override it specifically to preserve compatibility.
    let store = near_parameters::RuntimeConfigStore::for_chain_id(
        &data.genesis_info.genesis_config.chain_id,
    );
    let runtime_config = store.get_config(block.latest_protocol_version);

    // get default epoch config for genesis config
    let default_epoch_config = EpochConfig::from(&data.genesis_info.genesis_config);
    // AllEpochConfig manages protocol configs that might be changing throughout epochs (hence EpochConfig).
    // The main function in AllEpochConfig is ::for_protocol_version which takes a protocol version
    // and returns the EpochConfig that should be used for this protocol version.
    let all_epoch_config = AllEpochConfig::new(
        true,
        default_epoch_config,
        &data.genesis_info.genesis_config.chain_id,
    );
    let epoch_config = all_epoch_config.for_protocol_version(block.latest_protocol_version);

    // Looks strange, but we follow the same logic as in nearcore
    // nearcore/src/runtime/mod.rs:1203
    let mut genesis_config = data.genesis_info.genesis_config.clone();
    genesis_config.protocol_version = block.latest_protocol_version;

    genesis_config.epoch_length = epoch_config.epoch_length;
    genesis_config.num_block_producer_seats = epoch_config.num_block_producer_seats;
    genesis_config.num_block_producer_seats_per_shard =
        epoch_config.num_block_producer_seats_per_shard;
    genesis_config.avg_hidden_validator_seats_per_shard =
        epoch_config.avg_hidden_validator_seats_per_shard;
    genesis_config.block_producer_kickout_threshold = epoch_config.block_producer_kickout_threshold;
    genesis_config.chunk_producer_kickout_threshold = epoch_config.chunk_producer_kickout_threshold;
    genesis_config.max_kickout_stake_perc = epoch_config.validator_max_kickout_stake_perc;
    genesis_config.online_min_threshold = epoch_config.online_min_threshold;
    genesis_config.online_max_threshold = epoch_config.online_max_threshold;
    genesis_config.fishermen_threshold = epoch_config.fishermen_threshold;
    genesis_config.minimum_stake_divisor = epoch_config.minimum_stake_divisor;
    genesis_config.protocol_upgrade_stake_threshold = epoch_config.protocol_upgrade_stake_threshold;
    genesis_config.shard_layout = epoch_config.shard_layout;
    genesis_config.num_chunk_only_producer_seats = epoch_config
        .validator_selection_config
        .num_chunk_only_producer_seats;
    genesis_config.minimum_validators_per_shard = epoch_config
        .validator_selection_config
        .minimum_validators_per_shard;
    genesis_config.minimum_stake_ratio =
        epoch_config.validator_selection_config.minimum_stake_ratio;

    let protocol_config = near_chain_configs::ProtocolConfig {
        genesis_config,
        runtime_config: near_parameters::RuntimeConfig {
            fees: runtime_config.fees.clone(),
            wasm_config: runtime_config.wasm_config.clone(),
            account_creation_config: runtime_config.account_creation_config.clone(),
            congestion_control_config: runtime_config.congestion_control_config,
            witness_config: runtime_config.witness_config,
        },
    };
    Ok(protocol_config.into())
}
