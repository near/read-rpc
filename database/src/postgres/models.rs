use crate::diesel::prelude::*;
use crate::schema::{
    account_state, block, chunk, meta, receipt_map, receipt_outcome, state_changes_access_key,
    state_changes_access_keys, state_changes_account, state_changes_contract, state_changes_data,
    transaction_cache, transaction_detail,
};

/// State-indexer tables
#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = state_changes_data)]
struct StateChangesData {
    account_id: String,
    block_height: bigdecimal::BigDecimal,
    block_hash: String,
    data_key: String,
    data_value: Option<Vec<u8>>,
}

#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = state_changes_access_key)]
struct StateChangesAccessKey {
    account_id: String,
    block_height: bigdecimal::BigDecimal,
    block_hash: String,
    data_key: String,
    data_value: Option<Vec<u8>>,
}

#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = state_changes_access_keys)]
struct StateChangesAccessKeys {
    account_id: String,
    block_height: bigdecimal::BigDecimal,
    block_hash: String,
    active_access_keys: Option<serde_json::Value>,
}

#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = state_changes_contract)]
struct StateChangesContract {
    account_id: String,
    block_height: bigdecimal::BigDecimal,
    block_hash: String,
    data_value: Option<Vec<u8>>,
}

#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = state_changes_account)]
struct StateChangesAccount {
    account_id: String,
    block_height: bigdecimal::BigDecimal,
    block_hash: String,
    data_value: Option<Vec<u8>>,
}

#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = block)]
struct Block {
    block_height: bigdecimal::BigDecimal,
    block_hash: String,
}

#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = chunk)]
struct Chunk {
    chunk_hash: String,
    block_height: bigdecimal::BigDecimal,
    shard_id: bigdecimal::BigDecimal,
    stored_at_block_height: bigdecimal::BigDecimal,
}

#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = account_state)]
struct AccountState {
    account_id: String,
    data_key: String,
}

/// Tx-indexer tables

#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = transaction_detail)]
struct TransactionDetail {
    transaction_hash: String,
    block_height: bigdecimal::BigDecimal,
    account_id: String,
    transaction_details: Vec<u8>,
}

#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = receipt_map)]
struct ReceiptMap {
    receipt_id: String,
    block_height: bigdecimal::BigDecimal,
    parent_transaction_hash: String,
    shard_id: bigdecimal::BigDecimal,
}

/// Tx-indexer cache tables
#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = transaction_cache)]
struct TransactionCache {
    block_height: bigdecimal::BigDecimal,
    transaction_hash: String,
    transaction_details: Vec<u8>,
}

#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = receipt_outcome)]
struct ReceiptOutcome {
    block_height: bigdecimal::BigDecimal,
    transaction_hash: String,
    receipt_id: String,
    receipt: Vec<u8>,
    outcome: Vec<u8>,
}

/// Metadata table
#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = meta)]
struct Meta {
    indexer_id: String,
    last_processed_block_height: bigdecimal::BigDecimal,
}
