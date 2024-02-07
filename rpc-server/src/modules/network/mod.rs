pub mod methods;

async fn parse_validator_request(
    value: serde_json::Value,
) -> anyhow::Result<near_jsonrpc_primitives::types::validator::RpcValidatorRequest> {
    let request = if let serde_json::Value::Object(_) = value {
        serde_json::from_value::<near_jsonrpc_primitives::types::validator::RpcValidatorRequest>(
            value,
        )?
    } else {
        let epoch_reference = match value[0].clone() {
            serde_json::Value::Null => near_primitives::types::EpochReference::Latest,
            _ => {
                let (block_id,) =
                    serde_json::from_value::<(near_primitives::types::BlockId,)>(value)?;
                near_primitives::types::EpochReference::BlockId(block_id)
            }
        };
        near_jsonrpc_primitives::types::validator::RpcValidatorRequest { epoch_reference }
    };
    Ok(request)
}

/// cannot move out of dereference of `futures_locks::RwLockReadGuard<FinalBlockInfo>`
/// move occurs because value `current_protocol_config` has type `ProtocolConfigView`,
/// which does not implement the `Copy` trait
pub fn clone_protocol_config(
    protocol_config: &near_chain_configs::ProtocolConfigView,
) -> near_chain_configs::ProtocolConfigView {
    near_chain_configs::ProtocolConfigView {
        protocol_version: protocol_config.protocol_version,
        genesis_time: protocol_config.genesis_time,
        chain_id: protocol_config.chain_id.clone(),
        genesis_height: protocol_config.genesis_height,
        num_block_producer_seats: protocol_config.num_block_producer_seats,
        num_block_producer_seats_per_shard: protocol_config
            .num_block_producer_seats_per_shard
            .clone(),
        avg_hidden_validator_seats_per_shard: protocol_config
            .avg_hidden_validator_seats_per_shard
            .clone(),
        dynamic_resharding: protocol_config.dynamic_resharding,
        protocol_upgrade_stake_threshold: protocol_config.protocol_upgrade_stake_threshold,
        epoch_length: protocol_config.epoch_length,
        gas_limit: protocol_config.gas_limit,
        min_gas_price: protocol_config.min_gas_price,
        max_gas_price: protocol_config.max_gas_price,
        block_producer_kickout_threshold: protocol_config.block_producer_kickout_threshold,
        chunk_producer_kickout_threshold: protocol_config.chunk_producer_kickout_threshold,
        online_min_threshold: protocol_config.online_min_threshold,
        online_max_threshold: protocol_config.online_max_threshold,
        gas_price_adjustment_rate: protocol_config.gas_price_adjustment_rate,
        runtime_config: protocol_config.runtime_config.clone(),
        transaction_validity_period: protocol_config.transaction_validity_period,
        protocol_reward_rate: protocol_config.protocol_reward_rate,
        max_inflation_rate: protocol_config.max_inflation_rate,
        num_blocks_per_year: protocol_config.num_blocks_per_year,
        protocol_treasury_account: protocol_config.protocol_treasury_account.clone(),
        fishermen_threshold: protocol_config.fishermen_threshold,
        minimum_stake_divisor: protocol_config.minimum_stake_divisor,
        max_kickout_stake_perc: protocol_config.max_kickout_stake_perc,
        minimum_stake_ratio: protocol_config.minimum_stake_ratio,
        minimum_validators_per_shard: protocol_config.minimum_validators_per_shard,
        num_chunk_only_producer_seats: protocol_config.num_chunk_only_producer_seats,
        shard_layout: protocol_config.shard_layout.clone(),
    }
}
