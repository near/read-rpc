use crate::schema::*;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;

/// State-indexer tables
#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = state_changes_data)]
pub struct StateChangesData {
    pub account_id: String,
    pub block_height: bigdecimal::BigDecimal,
    pub block_hash: String,
    pub data_key: String,
    pub data_value: Option<Vec<u8>>,
}

impl StateChangesData {
    pub async fn save(&self, mut conn: crate::postgres::PgAsyncConn) -> anyhow::Result<()> {
        diesel::insert_into(state_changes_data::table)
            .values(self)
            .on_conflict_do_nothing()
            .execute(&mut conn)
            .await?;
        Ok(())
    }
}

#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = state_changes_access_key)]
pub struct StateChangesAccessKey {
    pub account_id: String,
    pub block_height: bigdecimal::BigDecimal,
    pub block_hash: String,
    pub data_key: String,
    pub data_value: Option<Vec<u8>>,
}

impl StateChangesAccessKey {
    pub async fn save(&self, mut conn: crate::postgres::PgAsyncConn) -> anyhow::Result<()> {
        diesel::insert_into(state_changes_access_key::table)
            .values(self)
            .on_conflict_do_nothing()
            .execute(&mut conn)
            .await?;
        Ok(())
    }
}

#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = state_changes_access_keys)]
pub struct StateChangesAccessKeys {
    pub account_id: String,
    pub block_height: bigdecimal::BigDecimal,
    pub block_hash: String,
    pub active_access_keys: Option<serde_json::Value>,
}

impl StateChangesAccessKeys {
    pub async fn save(&self, mut conn: crate::postgres::PgAsyncConn) -> anyhow::Result<()> {
        diesel::insert_into(state_changes_access_keys::table)
            .values(self)
            .on_conflict_do_nothing()
            .execute(&mut conn)
            .await?;
        Ok(())
    }

    pub async fn get_active_access_keys(
        mut conn: crate::postgres::PgAsyncConn,
        account_id: &str,
        block_height: u64,
    ) -> anyhow::Result<Option<serde_json::Value>> {
        let resp = state_changes_access_keys::table
            .filter(state_changes_access_keys::account_id.eq(account_id))
            .filter(
                state_changes_access_keys::block_height
                    .le(bigdecimal::BigDecimal::from(block_height)),
            )
            .select(Self::as_select())
            .first(&mut conn)
            .await?;

        Ok(resp.active_access_keys)
    }
}

#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = state_changes_contract)]
pub struct StateChangesContract {
    pub account_id: String,
    pub block_height: bigdecimal::BigDecimal,
    pub block_hash: String,
    pub data_value: Option<Vec<u8>>,
}

impl StateChangesContract {
    pub async fn save(&self, mut conn: crate::postgres::PgAsyncConn) -> anyhow::Result<()> {
        diesel::insert_into(state_changes_contract::table)
            .values(self)
            .on_conflict_do_nothing()
            .execute(&mut conn)
            .await?;
        Ok(())
    }
}

#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = state_changes_account)]
pub struct StateChangesAccount {
    pub account_id: String,
    pub block_height: bigdecimal::BigDecimal,
    pub block_hash: String,
    pub data_value: Option<Vec<u8>>,
}

impl StateChangesAccount {
    pub async fn save(&self, mut conn: crate::postgres::PgAsyncConn) -> anyhow::Result<()> {
        diesel::insert_into(state_changes_account::table)
            .values(self)
            .on_conflict_do_nothing()
            .execute(&mut conn)
            .await?;
        Ok(())
    }
}

#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = block)]
pub struct Block {
    pub block_height: bigdecimal::BigDecimal,
    pub block_hash: String,
}

impl Block {
    pub async fn save(&self, mut conn: crate::postgres::PgAsyncConn) -> anyhow::Result<()> {
        diesel::insert_into(block::table)
            .values(self)
            .on_conflict_do_nothing()
            .execute(&mut conn)
            .await?;
        Ok(())
    }
    pub async fn get_block_height_by_hash(
        mut conn: crate::postgres::PgAsyncConn,
        block_hash: near_primitives::hash::CryptoHash,
    ) -> anyhow::Result<bigdecimal::BigDecimal> {
        let resp = block::table
            .filter(block::block_hash.eq(block_hash.to_string()))
            .select(Self::as_select())
            .first(&mut conn)
            .await?;

        Ok(resp.block_height)
    }
}

#[derive(Insertable, Queryable, Selectable, Clone)]
#[diesel(table_name = chunk)]
pub struct Chunk {
    pub chunk_hash: String,
    pub block_height: bigdecimal::BigDecimal,
    pub shard_id: bigdecimal::BigDecimal,
    pub stored_at_block_height: bigdecimal::BigDecimal,
}

impl Chunk {
    pub async fn bulk_insert(
        chunks: Vec<Self>,
        mut conn: crate::postgres::PgAsyncConn,
    ) -> anyhow::Result<()> {
        diesel::insert_into(chunk::table)
            .values(&chunks)
            .on_conflict_do_nothing()
            .execute(&mut conn)
            .await?;
        Ok(())
    }

    pub async fn get_block_height_by_chunk_hash(
        mut conn: crate::postgres::PgAsyncConn,
        chunk_hash: near_primitives::hash::CryptoHash,
    ) -> anyhow::Result<(bigdecimal::BigDecimal, bigdecimal::BigDecimal)> {
        let resp = chunk::table
            .filter(chunk::chunk_hash.eq(chunk_hash.to_string()))
            .select(Self::as_select())
            .first(&mut conn)
            .await?;

        Ok((resp.stored_at_block_height, resp.shard_id))
    }
}

#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = account_state)]
pub struct AccountState {
    pub account_id: String,
    pub data_key: String,
}

impl AccountState {
    pub async fn save(&self, mut conn: crate::postgres::PgAsyncConn) -> anyhow::Result<()> {
        diesel::insert_into(account_state::table)
            .values(self)
            .on_conflict_do_nothing()
            .execute(&mut conn)
            .await?;
        Ok(())
    }
}
/// Tx-indexer tables

#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = transaction_detail)]
pub struct TransactionDetail {
    transaction_hash: String,
    block_height: bigdecimal::BigDecimal,
    account_id: String,
    transaction_details: Vec<u8>,
}

#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = receipt_map)]
pub struct ReceiptMap {
    receipt_id: String,
    block_height: bigdecimal::BigDecimal,
    parent_transaction_hash: String,
    shard_id: bigdecimal::BigDecimal,
}

/// Tx-indexer cache tables
#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = transaction_cache)]
pub struct TransactionCache {
    block_height: bigdecimal::BigDecimal,
    transaction_hash: String,
    transaction_details: Vec<u8>,
}

#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = receipt_outcome)]
pub struct ReceiptOutcome {
    block_height: bigdecimal::BigDecimal,
    transaction_hash: String,
    receipt_id: String,
    receipt: Vec<u8>,
    outcome: Vec<u8>,
}

/// Metadata table
#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = meta)]
pub struct Meta {
    pub indexer_id: String,
    pub last_processed_block_height: bigdecimal::BigDecimal,
}

impl Meta {
    pub async fn save(&self, mut conn: crate::postgres::PgAsyncConn) -> anyhow::Result<()> {
        diesel::insert_into(meta::table)
            .values(self)
            .on_conflict(meta::indexer_id)
            .do_update()
            .set(meta::last_processed_block_height.eq(self.last_processed_block_height.clone()))
            .execute(&mut conn)
            .await?;
        Ok(())
    }

    pub async fn get_last_processed_block_height(
        mut conn: crate::postgres::PgAsyncConn,
        indexer_id: &str,
    ) -> anyhow::Result<bigdecimal::BigDecimal> {
        let resp = meta::table
            .filter(meta::indexer_id.eq(indexer_id))
            .select(Self::as_select())
            .first(&mut conn)
            .await?;

        Ok(resp.last_processed_block_height)
    }
}
