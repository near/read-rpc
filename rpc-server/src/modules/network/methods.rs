use actix_web::web::Data;
use near_primitives::epoch_manager::{AllEpochConfig, EpochConfig};

use crate::config::ServerContext;
use crate::modules::blocks::utils::fetch_block_from_cache_or_get;
use crate::modules::network::get_protocol_version;

pub async fn client_config(
    _data: Data<ServerContext>,
) -> Result<(), near_jsonrpc::primitives::errors::RpcError> {
    let message = "Method `client_config` is not implemented on this type of node. Please send a request to NEAR JSON RPC instead.".to_string();
    Err(near_jsonrpc::primitives::errors::RpcError::new(
        32601, message, None,
    ))
}

pub async fn maintenance_windows(
    _data: Data<ServerContext>,
) -> Result<(), near_jsonrpc::primitives::errors::RpcError> {
    let message = "Method `maintenance_windows` is not implemented on this type of node. Please send a request to NEAR JSON RPC instead.".to_string();
    Err(near_jsonrpc::primitives::errors::RpcError::new(
        32601, message, None,
    ))
}

pub async fn split_storage_info(
    _data: Data<ServerContext>,
) -> Result<(), near_jsonrpc::primitives::errors::RpcError> {
    let message = "Method `split_storage_info` is not implemented on this type of node. Please send a request to NEAR JSON RPC instead.".to_string();
    Err(near_jsonrpc::primitives::errors::RpcError::new(
        32601, message, None,
    ))
}

pub async fn status(
    data: Data<ServerContext>,
) -> Result<
    near_primitives::views::StatusResponse,
    near_jsonrpc::primitives::types::status::RpcStatusError,
> {
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
        protocol_version: data
            .blocks_info_by_finality
            .current_protocol_version()
            .await,
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
) -> Result<
    crate::health::RPCHealthStatusResponse,
    near_jsonrpc::primitives::types::status::RpcStatusError,
> {
    Ok(crate::health::RPCHealthStatusResponse::new(&data).await)
}

pub async fn network_info(
    data: Data<ServerContext>,
) -> Result<
    near_jsonrpc::primitives::types::network_info::RpcNetworkInfoResponse,
    near_jsonrpc::primitives::types::network_info::RpcNetworkInfoError,
> {
    data.near_rpc_client
        .call(
            near_jsonrpc_client::methods::network_info::RpcNetworkInfoRequest,
            Some("network_info"),
        )
        .await
        .map_err(|err| {
            err.handler_error().cloned().unwrap_or(
                near_jsonrpc::primitives::types::network_info::RpcNetworkInfoError::InternalError {
                    error_message: err.to_string(),
                },
            )
        })
}

pub async fn validators(
    data: Data<ServerContext>,
    request_data: near_jsonrpc::primitives::types::validator::RpcValidatorRequest,
) -> Result<
    near_jsonrpc::primitives::types::validator::RpcValidatorResponse,
    near_jsonrpc::primitives::types::validator::RpcValidatorError,
> {
    tracing::debug!("`validators` called with parameters: {:?}", request_data);
    // Latest epoch validators fetches from the Near RPC node
    if let near_primitives::types::EpochReference::Latest = &request_data.epoch_reference {
        let validator_info =
            data.near_rpc_client
                .call(request_data, Some("validators"))
                .await
                .map_err(|err| {
                    err.handler_error().cloned().unwrap_or(
                near_jsonrpc::primitives::types::validator::RpcValidatorError::InternalError {
                    error_message: err.to_string(),
                })
                })?;
        return Ok(
            near_jsonrpc::primitives::types::validator::RpcValidatorResponse { validator_info },
        );
    };

    // Current epoch validators fetches from the Near RPC node
    if let near_primitives::types::EpochReference::EpochId(epoch_id) = &request_data.epoch_reference
    {
        if data
            .blocks_info_by_finality
            .final_cache_block()
            .await
            .epoch_id
            == epoch_id.0
        {
            let validator_info = data
                .near_rpc_client
                .call(request_data, Some("validators"))
                .await
                .map_err(|err| {
                    err.handler_error().cloned().unwrap_or(
                    near_jsonrpc::primitives::types::validator::RpcValidatorError::InternalError {
                        error_message: err.to_string(),
                    })
                })?;
            return Ok(
                near_jsonrpc::primitives::types::validator::RpcValidatorResponse { validator_info },
            );
        }
    };

    let validator_info = validators_call(&data, &request_data).await;

    #[cfg(feature = "shadow-data-consistency")]
    {
        crate::utils::shadow_compare_results_handler(
            data.shadow_data_consistency_rate,
            &validator_info,
            data.near_rpc_client.clone(),
            request_data,
            "validators",
        )
        .await;
    }

    Ok(
        near_jsonrpc::primitives::types::validator::RpcValidatorResponse {
            validator_info: validator_info?,
        },
    )
}

pub async fn validators_ordered(
    data: Data<ServerContext>,
    request_data: near_jsonrpc::primitives::types::validator::RpcValidatorsOrderedRequest,
) -> Result<
    near_jsonrpc::primitives::types::validator::RpcValidatorsOrderedResponse,
    near_jsonrpc::primitives::types::validator::RpcValidatorError,
> {
    if let Some(block_id) = &request_data.block_id {
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
                    .call(request_data, Some("EXPERIMENTAL_validators_ordered"))
                    .await.map_err(|err| {
                        err.handler_error().cloned().unwrap_or(
                        near_jsonrpc::primitives::types::validator::RpcValidatorError::InternalError {
                            error_message: err.to_string(),
                        })
                    })?)
            } else {
                // Proxy to archival rpc if the block garbage collected
                Ok(data
                    .near_rpc_client
                    .archival_call(request_data, Some("EXPERIMENTAL_validators_ordered"))
                    .await.map_err(|err| {
                        err.handler_error().cloned().unwrap_or(
                        near_jsonrpc::primitives::types::validator::RpcValidatorError::InternalError {
                            error_message: err.to_string(),
                        })
                    })?)
            }
        } else {
            Ok(data
                .near_rpc_client
                .call(request_data, Some("EXPERIMENTAL_validators_ordered"))
                .await
                .map_err(|err| {
                    err.handler_error().cloned().unwrap_or(
                    near_jsonrpc::primitives::types::validator::RpcValidatorError::InternalError {
                        error_message: err.to_string(),
                    })
                })?)
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
            .call(request_data, Some("EXPERIMENTAL_validators_ordered"))
            .await
            .map_err(|err| {
                err.handler_error().cloned().unwrap_or(
                    near_jsonrpc::primitives::types::validator::RpcValidatorError::InternalError {
                        error_message: err.to_string(),
                    },
                )
            })?)
    }
}

pub async fn genesis_config(
    data: Data<ServerContext>,
) -> Result<near_chain_configs::GenesisConfig, near_jsonrpc::primitives::errors::RpcError> {
    Ok(data.genesis_info.genesis_config.clone())
}

pub async fn protocol_config(
    data: Data<ServerContext>,
    request_data: near_jsonrpc::primitives::types::config::RpcProtocolConfigRequest,
) -> Result<
    near_jsonrpc::primitives::types::config::RpcProtocolConfigResponse,
    near_jsonrpc::primitives::types::config::RpcProtocolConfigError,
> {
    tracing::debug!(
        "`EXPERIMENTAL_protocol_config` called with parameters: {:?}",
        request_data
    );

    let config_view = protocol_config_call(
        &data,
        request_data.block_reference.clone(),
        "EXPERIMENTAL_protocol_config",
    )
    .await;

    #[cfg(feature = "shadow-data-consistency")]
    {
        crate::utils::shadow_compare_results_handler(
            data.shadow_data_consistency_rate,
            &config_view,
            data.near_rpc_client.clone(),
            request_data,
            "EXPERIMENTAL_protocol_config",
        )
        .await;
    }

    Ok(
        near_jsonrpc::primitives::types::config::RpcProtocolConfigResponse {
            config_view: config_view?,
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

pub async fn protocol_config_call(
    data: &Data<ServerContext>,
    block_reference: near_primitives::types::BlockReference,
    method_name: &str,
) -> Result<
    near_chain_configs::ProtocolConfigView,
    near_jsonrpc::primitives::types::config::RpcProtocolConfigError,
> {
    let protocol_version = get_protocol_version(data, block_reference, method_name)
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
    let runtime_config = store.get_config(protocol_version);

    // get default epoch config for genesis config
    let default_epoch_config = EpochConfig::from(&data.genesis_info.genesis_config);
    // AllEpochConfig manages protocol configs that might be changing throughout epochs (hence EpochConfig).
    // The main function in AllEpochConfig is ::for_protocol_version which takes a protocol version
    // and returns the EpochConfig that should be used for this protocol version.
    let all_epoch_config = AllEpochConfig::new(
        true,
        data.genesis_info.genesis_config.protocol_version,
        default_epoch_config,
        &data.genesis_info.genesis_config.chain_id,
    );
    let epoch_config = all_epoch_config.for_protocol_version(protocol_version);

    // Looks strange, but we follow the same logic as in nearcore
    // nearcore/src/runtime/mod.rs:1203
    let mut genesis_config = data.genesis_info.genesis_config.clone();
    genesis_config.protocol_version = protocol_version;

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
            use_state_stored_receipt: runtime_config.use_state_stored_receipt,
        },
    };
    Ok(protocol_config.into())
}
