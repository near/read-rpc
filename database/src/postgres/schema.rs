// @generated automatically by Diesel CLI.

diesel::table! {
    account_state (account_id, data_key) {
        account_id -> Text,
        data_key -> Text,
    }
}

diesel::table! {
    block (block_hash) {
        block_height -> Numeric,
        block_hash -> Text,
    }
}

diesel::table! {
    chunk (chunk_hash, block_height) {
        chunk_hash -> Text,
        block_height -> Numeric,
        shard_id -> Numeric,
        stored_at_block_height -> Numeric,
    }
}

diesel::table! {
    meta (indexer_id) {
        indexer_id -> Text,
        last_processed_block_height -> Numeric,
    }
}

diesel::table! {
    protocol_configs (epoch_id) {
        epoch_id -> Text,
        epoch_height -> Numeric,
        epoch_start_height -> Numeric,
        epoch_end_height -> Nullable<Numeric>,
        protocol_config -> Jsonb,
    }
}

diesel::table! {
    receipt_map (receipt_id) {
        receipt_id -> Text,
        block_height -> Numeric,
        parent_transaction_hash -> Text,
        shard_id -> Numeric,
    }
}

diesel::table! {
    receipt_outcome (block_height, transaction_hash, receipt_id) {
        block_height -> Numeric,
        transaction_hash -> Text,
        receipt_id -> Text,
        receipt -> Bytea,
        outcome -> Bytea,
    }
}

diesel::table! {
    state_changes_access_key (account_id, data_key, block_height) {
        account_id -> Text,
        block_height -> Numeric,
        block_hash -> Text,
        data_key -> Text,
        data_value -> Nullable<Bytea>,
    }
}

diesel::table! {
    state_changes_access_keys (account_id, block_height) {
        account_id -> Text,
        block_height -> Numeric,
        block_hash -> Text,
        active_access_keys -> Nullable<Jsonb>,
    }
}

diesel::table! {
    state_changes_account (account_id, block_height) {
        account_id -> Text,
        block_height -> Numeric,
        block_hash -> Text,
        data_value -> Nullable<Bytea>,
    }
}

diesel::table! {
    state_changes_contract (account_id, block_height) {
        account_id -> Text,
        block_height -> Numeric,
        block_hash -> Text,
        data_value -> Nullable<Bytea>,
    }
}

diesel::table! {
    state_changes_data (account_id, data_key, block_height) {
        account_id -> Text,
        block_height -> Numeric,
        block_hash -> Text,
        data_key -> Text,
        data_value -> Nullable<Bytea>,
    }
}

diesel::table! {
    transaction_cache (block_height, transaction_hash) {
        block_height -> Numeric,
        transaction_hash -> Text,
        transaction_details -> Bytea,
    }
}

diesel::table! {
    transaction_detail (transaction_hash, block_height) {
        transaction_hash -> Text,
        block_height -> Numeric,
        account_id -> Text,
        transaction_details -> Bytea,
    }
}

diesel::table! {
    validators (epoch_id) {
        epoch_id -> Text,
        epoch_height -> Numeric,
        epoch_start_height -> Numeric,
        epoch_end_height -> Nullable<Numeric>,
        validators_info -> Jsonb,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    account_state,
    block,
    chunk,
    meta,
    protocol_configs,
    receipt_map,
    receipt_outcome,
    state_changes_access_key,
    state_changes_access_keys,
    state_changes_account,
    state_changes_contract,
    state_changes_data,
    transaction_cache,
    transaction_detail,
    validators,
);
