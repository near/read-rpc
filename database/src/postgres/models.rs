use diesel::prelude::*;

/// State-indexer tables
#[derive(Insertable, Queryable, Selectable)]
struct StateChangesData {
    account_id: String,
    block_height: u64,
    block_hash: String,
    data_key: String,
    data_value: Option<Vec<u8>>,
}

#[derive(Insertable, Queryable, Selectable)]
struct StateChangesAccessKey {
    account_id: String,
    block_height: u64,
    block_hash: String,
    data_key: String,
    data_value: Option<Vec<u8>>,
}

#[derive(Insertable, Queryable, Selectable)]
struct StateChangesAccessKeys {
    account_id: String,
    block_height: u64,
    block_hash: String,
    active_access_keys: std::collections::HashMap<String, Vec<u8>>,
}

#[derive(Insertable, Queryable, Selectable)]
struct StateChangesContract {
    account_id: String,
    block_height: u64,
    block_hash: String,
    data_value: Option<Vec<u8>>,
}

#[derive(Insertable, Queryable, Selectable)]
struct StateChangesAccount {
    account_id: String,
    block_height: u64,
    block_hash: String,
    data_value: Option<Vec<u8>>,
}

#[derive(Insertable, Queryable, Selectable)]
struct Block {
    block_height: u64,
    block_hash: String,
}

#[derive(Insertable, Queryable, Selectable)]
struct Chunk {
    chunk_hash: String,
    block_height: u64,
    shard_id: u64,
    stored_at_block_height: u64,
}

#[derive(Insertable, Queryable, Selectable)]
struct Meta {
    indexer_id: String,
    last_processed_block_height: u64,
}

#[derive(Insertable, Queryable, Selectable)]
struct AccountState {
    account_id: String,
    data_key: String,
}

/// Tx-indexer tables

#[derive(Insertable, Queryable, Selectable)]
struct TransactionDetail {
    transaction_hash: String,
    block_height: u64,
    account_id: String,
    transaction_details: Vec<u8>,
}

#[derive(Insertable, Queryable, Selectable)]
struct ReceiptMap {
    receipt_id: String,
    block_height: u64,
    parent_transaction_hash: String,
    shard_id: u64,
}

/// Tx-indexer cache tables
#[derive(Insertable, Queryable, Selectable)]
struct TransactionCache {
    block_height: u64,
    transaction_hash: String,
    transaction_details: Vec<u8>,
}

#[derive(Insertable, Queryable, Selectable)]
struct ReceiptOutcome {
    block_height: u64,
    transaction_hash: String,
    receipt_id: u64,
    receipt: Vec<u8>,
    outcome: Vec<u8>,
}