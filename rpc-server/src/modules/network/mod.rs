pub mod methods;

pub async fn epoch_config_from_protocol_config_view(
    protocol_config_view: near_chain_configs::ProtocolConfigView,
) -> near_primitives::epoch_manager::EpochConfig {
    near_primitives::epoch_manager::EpochConfig {
        epoch_length: protocol_config_view.epoch_length,
        num_block_producer_seats: protocol_config_view.num_block_producer_seats,
        num_block_producer_seats_per_shard: protocol_config_view
            .num_block_producer_seats_per_shard
            .clone(),
        avg_hidden_validator_seats_per_shard: protocol_config_view
            .avg_hidden_validator_seats_per_shard
            .clone(),
        block_producer_kickout_threshold: protocol_config_view.block_producer_kickout_threshold,
        chunk_producer_kickout_threshold: protocol_config_view.chunk_producer_kickout_threshold,
        fishermen_threshold: protocol_config_view.fishermen_threshold,
        online_min_threshold: protocol_config_view.online_min_threshold,
        online_max_threshold: protocol_config_view.online_max_threshold,
        protocol_upgrade_stake_threshold: protocol_config_view.protocol_upgrade_stake_threshold,
        minimum_stake_divisor: protocol_config_view.minimum_stake_divisor,
        shard_layout: protocol_config_view.shard_layout.clone(),
        validator_selection_config: near_primitives::epoch_manager::ValidatorSelectionConfig {
            num_chunk_only_producer_seats: protocol_config_view.num_chunk_only_producer_seats,
            minimum_validators_per_shard: protocol_config_view.minimum_validators_per_shard,
            minimum_stake_ratio: protocol_config_view.minimum_stake_ratio,
            shuffle_shard_assignment_for_chunk_producers: protocol_config_view
                .shuffle_shard_assignment_for_chunk_producers,
        },
        validator_max_kickout_stake_perc: protocol_config_view.max_kickout_stake_perc,
    }
}
