use crate::schema::*;
use borsh::{BorshDeserialize, BorshSerialize};
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
    pub async fn insert_or_ignore(
        &self,
        mut conn: crate::postgres::PgAsyncConn,
    ) -> anyhow::Result<()> {
        diesel::insert_into(state_changes_data::table)
            .values(self)
            .on_conflict_do_nothing()
            .execute(&mut conn)
            .await?;
        Ok(())
    }

    pub async fn get_state_key_value(
        mut conn: crate::postgres::PgAsyncConn,
        account_id: &str,
        block_height: u64,
        data_key: String,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        let response = state_changes_data::table
            .filter(state_changes_data::account_id.eq(account_id))
            .filter(state_changes_data::block_height.le(bigdecimal::BigDecimal::from(block_height)))
            .filter(state_changes_data::data_key.eq(data_key))
            .order(state_changes_data::block_height.desc())
            .select(Self::as_select())
            .first(&mut conn)
            .await?;

        Ok(response.data_value)
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
    pub async fn insert_or_ignore(
        &self,
        mut conn: crate::postgres::PgAsyncConn,
    ) -> anyhow::Result<()> {
        diesel::insert_into(state_changes_access_key::table)
            .values(self)
            .on_conflict_do_nothing()
            .execute(&mut conn)
            .await?;
        Ok(())
    }

    pub async fn get_access_key(
        mut conn: crate::postgres::PgAsyncConn,
        account_id: &str,
        block_height: u64,
        data_key: String,
    ) -> anyhow::Result<Self> {
        let response = state_changes_access_key::table
            .filter(state_changes_access_key::account_id.eq(account_id))
            .filter(
                state_changes_access_key::block_height
                    .le(bigdecimal::BigDecimal::from(block_height)),
            )
            .filter(state_changes_access_key::data_key.eq(data_key))
            .order(state_changes_access_key::block_height.desc())
            .select(Self::as_select())
            .first(&mut conn)
            .await?;

        Ok(response)
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
    pub async fn insert_or_ignore(
        &self,
        mut conn: crate::postgres::PgAsyncConn,
    ) -> anyhow::Result<()> {
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
        let response = state_changes_access_keys::table
            .filter(state_changes_access_keys::account_id.eq(account_id))
            .filter(
                state_changes_access_keys::block_height
                    .le(bigdecimal::BigDecimal::from(block_height)),
            )
            .select(Self::as_select())
            .first(&mut conn)
            .await?;

        Ok(response.active_access_keys)
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
    pub async fn insert_or_ignore(
        &self,
        mut conn: crate::postgres::PgAsyncConn,
    ) -> anyhow::Result<()> {
        diesel::insert_into(state_changes_contract::table)
            .values(self)
            .on_conflict_do_nothing()
            .execute(&mut conn)
            .await?;
        Ok(())
    }

    pub async fn get_contract(
        mut conn: crate::postgres::PgAsyncConn,
        account_id: &str,
        block_height: u64,
    ) -> anyhow::Result<Self> {
        let response = state_changes_contract::table
            .filter(state_changes_contract::account_id.eq(account_id))
            .filter(
                state_changes_contract::block_height.le(bigdecimal::BigDecimal::from(block_height)),
            )
            .order(state_changes_contract::block_height.desc())
            .select(Self::as_select())
            .first(&mut conn)
            .await?;

        Ok(response)
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
    pub async fn insert_or_ignore(
        &self,
        mut conn: crate::postgres::PgAsyncConn,
    ) -> anyhow::Result<()> {
        diesel::insert_into(state_changes_account::table)
            .values(self)
            .on_conflict_do_nothing()
            .execute(&mut conn)
            .await?;
        Ok(())
    }

    pub async fn get_account(
        mut conn: crate::postgres::PgAsyncConn,
        account_id: &str,
        block_height: u64,
    ) -> anyhow::Result<Self> {
        let response = state_changes_account::table
            .filter(state_changes_account::account_id.eq(account_id))
            .filter(
                state_changes_account::block_height.le(bigdecimal::BigDecimal::from(block_height)),
            )
            .order(state_changes_account::block_height.desc())
            .select(Self::as_select())
            .first(&mut conn)
            .await?;

        Ok(response)
    }
}

#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = block)]
pub struct Block {
    pub block_height: bigdecimal::BigDecimal,
    pub block_hash: String,
}

impl Block {
    pub async fn insert_or_ignore(
        &self,
        mut conn: crate::postgres::PgAsyncConn,
    ) -> anyhow::Result<()> {
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
        let response = block::table
            .filter(block::block_hash.eq(block_hash.to_string()))
            .select(Self::as_select())
            .first(&mut conn)
            .await?;

        Ok(response.block_height)
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
        let response = chunk::table
            .filter(chunk::chunk_hash.eq(chunk_hash.to_string()))
            .select(Self::as_select())
            .first(&mut conn)
            .await?;

        Ok((response.stored_at_block_height, response.shard_id))
    }

    pub async fn get_stored_block_height(
        mut conn: crate::postgres::PgAsyncConn,
        block_height: u64,
        shard_id: u64,
    ) -> anyhow::Result<(bigdecimal::BigDecimal, bigdecimal::BigDecimal)> {
        let response = chunk::table
            .filter(chunk::block_height.eq(bigdecimal::BigDecimal::from(block_height)))
            .filter(chunk::shard_id.eq(bigdecimal::BigDecimal::from(shard_id)))
            .select(Self::as_select())
            .first(&mut conn)
            .await?;
        Ok((response.stored_at_block_height, response.shard_id))
    }
}

#[derive(borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Debug)]
struct PageState {
    page_size: i64,
    offset: i64,
}

impl PageState {
    fn new(page_size: i64) -> Self {
        Self {
            page_size,
            offset: 0,
        }
    }

    fn next_page(&self) -> Self {
        Self {
            page_size: self.page_size,
            offset: self.offset + self.page_size,
        }
    }
}

#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = account_state)]
pub struct AccountState {
    pub account_id: String,
    pub data_key: String,
}

impl AccountState {
    pub async fn insert_or_ignore(
        &self,
        mut conn: crate::postgres::PgAsyncConn,
    ) -> anyhow::Result<()> {
        diesel::insert_into(account_state::table)
            .values(self)
            .on_conflict_do_nothing()
            .execute(&mut conn)
            .await?;
        Ok(())
    }

    pub async fn get_state_keys_all(
        mut conn: crate::postgres::PgAsyncConn,
        account_id: &str,
    ) -> anyhow::Result<Vec<String>> {
        let response = account_state::table
            .filter(account_state::account_id.eq(account_id))
            .select(Self::as_select())
            .limit(25000)
            .load(&mut conn)
            .await?;

        Ok(response
            .into_iter()
            .map(|account_state_key| account_state_key.data_key)
            .collect())
    }

    pub async fn get_state_keys_by_page(
        mut conn: crate::postgres::PgAsyncConn,
        account_id: &str,
        page_token: crate::PageToken,
    ) -> anyhow::Result<(Vec<String>, crate::PageToken)> {
        let page_state = if let Some(page_state_token) = page_token {
            PageState::try_from_slice(&hex::decode(page_state_token)?)?
        } else {
            PageState::new(1000)
        };
        let response = account_state::table
            .filter(account_state::account_id.eq(account_id))
            .select(Self::as_select())
            .limit(page_state.page_size)
            .offset(page_state.offset)
            .load(&mut conn)
            .await?;

        let state_keys = response
            .into_iter()
            .map(|account_state_key| account_state_key.data_key)
            .collect::<Vec<String>>();

        if state_keys.len() < page_state.page_size as usize {
            Ok((state_keys, None))
        } else {
            Ok((
                state_keys,
                Some(hex::encode(page_state.next_page().try_to_vec()?)),
            ))
        }
    }

    pub async fn get_state_keys_by_prefix(
        mut conn: crate::postgres::PgAsyncConn,
        account_id: &str,
        prefix: String,
    ) -> anyhow::Result<Vec<String>> {
        let response = account_state::table
            .filter(account_state::account_id.eq(account_id))
            .filter(account_state::data_key.like(format!("{}%", prefix)))
            .select(Self::as_select())
            .load(&mut conn)
            .await?;

        Ok(response
            .into_iter()
            .map(|account_state_key| account_state_key.data_key)
            .collect())
    }
}
/// Tx-indexer tables

#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = transaction_detail)]
pub struct TransactionDetail {
    pub transaction_hash: String,
    pub block_height: bigdecimal::BigDecimal,
    pub account_id: String,
    pub transaction_details: Vec<u8>,
}

impl TransactionDetail {
    pub async fn insert_or_ignore(
        &self,
        mut conn: crate::postgres::PgAsyncConn,
    ) -> anyhow::Result<()> {
        diesel::insert_into(transaction_detail::table)
            .values(self)
            .on_conflict_do_nothing()
            .execute(&mut conn)
            .await?;
        Ok(())
    }

    pub async fn get_transaction_by_hash(
        mut conn: crate::postgres::PgAsyncConn,
        transaction_hash: &str,
    ) -> anyhow::Result<Vec<u8>> {
        let response = transaction_detail::table
            .filter(transaction_detail::transaction_hash.eq(transaction_hash))
            .select(Self::as_select())
            .first(&mut conn)
            .await?;

        Ok(response.transaction_details)
    }
}

#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = receipt_map)]
pub struct ReceiptMap {
    pub receipt_id: String,
    pub block_height: bigdecimal::BigDecimal,
    pub parent_transaction_hash: String,
    pub shard_id: bigdecimal::BigDecimal,
}

impl ReceiptMap {
    pub async fn insert_or_ignore(
        &self,
        mut conn: crate::postgres::PgAsyncConn,
    ) -> anyhow::Result<()> {
        diesel::insert_into(receipt_map::table)
            .values(self)
            .on_conflict_do_nothing()
            .execute(&mut conn)
            .await?;
        Ok(())
    }

    pub async fn get_receipt_by_id(
        mut conn: crate::postgres::PgAsyncConn,
        receipt_id: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<Self> {
        let response = receipt_map::table
            .filter(receipt_map::receipt_id.eq(receipt_id.to_string()))
            .select(Self::as_select())
            .first(&mut conn)
            .await?;

        Ok(response)
    }
}

/// Tx-indexer cache tables
#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = transaction_cache)]
pub struct TransactionCache {
    pub block_height: bigdecimal::BigDecimal,
    pub transaction_hash: String,
    pub transaction_details: Vec<u8>,
}

impl TransactionCache {
    pub async fn insert_or_ignore(
        &self,
        mut conn: crate::postgres::PgAsyncConn,
    ) -> anyhow::Result<()> {
        diesel::insert_into(transaction_cache::table)
            .values(self)
            .on_conflict_do_nothing()
            .execute(&mut conn)
            .await?;
        Ok(())
    }

    pub async fn get_transaction(
        mut conn: crate::postgres::PgAsyncConn,
        block_height: bigdecimal::BigDecimal,
        transaction_hash: &str,
    ) -> anyhow::Result<Vec<u8>> {
        let response = transaction_cache::table
            .filter(transaction_cache::block_height.eq(block_height))
            .filter(transaction_cache::transaction_hash.eq(transaction_hash))
            .select(Self::as_select())
            .first(&mut conn)
            .await?;

        Ok(response.transaction_details)
    }
    pub async fn get_transactions(
        mut conn: crate::postgres::PgAsyncConn,
        start_block_height: u64,
        cache_restore_blocks_range: u64,
    ) -> anyhow::Result<Vec<Self>> {
        let response = transaction_cache::table
            .filter(
                transaction_cache::block_height
                    .le(bigdecimal::BigDecimal::from(start_block_height)),
            )
            .filter(
                transaction_cache::block_height.ge(bigdecimal::BigDecimal::from(
                    start_block_height - cache_restore_blocks_range,
                )),
            )
            .select(Self::as_select())
            .load(&mut conn)
            .await?;

        Ok(response)
    }

    pub async fn delete_transaction(
        mut conn: crate::postgres::PgAsyncConn,
        block_height: u64,
        transaction_hash: &str,
    ) -> anyhow::Result<()> {
        diesel::delete(
            transaction_cache::table
                .filter(
                    transaction_cache::block_height.eq(bigdecimal::BigDecimal::from(block_height)),
                )
                .filter(transaction_cache::transaction_hash.eq(transaction_hash)),
        )
        .execute(&mut conn)
        .await?;
        Ok(())
    }
}

#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = receipt_outcome)]
pub struct ReceiptOutcome {
    pub block_height: bigdecimal::BigDecimal,
    pub transaction_hash: String,
    pub receipt_id: String,
    pub receipt: Vec<u8>,
    pub outcome: Vec<u8>,
}

impl ReceiptOutcome {
    pub async fn insert_or_ignore(
        &self,
        mut conn: crate::postgres::PgAsyncConn,
    ) -> anyhow::Result<()> {
        diesel::insert_into(receipt_outcome::table)
            .values(self)
            .on_conflict_do_nothing()
            .execute(&mut conn)
            .await?;
        Ok(())
    }

    pub async fn get_transaction_key(
        mut conn: crate::postgres::PgAsyncConn,
        receipt_id: &str,
    ) -> anyhow::Result<(bigdecimal::BigDecimal, String)> {
        let response = receipt_outcome::table
            .filter(receipt_outcome::receipt_id.eq(receipt_id.to_string()))
            .select(Self::as_select())
            .first(&mut conn)
            .await?;
        Ok((response.block_height, response.transaction_hash))
    }

    pub async fn get_receipt_outcome(
        mut conn: crate::postgres::PgAsyncConn,
        block_height: u64,
        transaction_hash: &str,
    ) -> anyhow::Result<Vec<Self>> {
        let response = receipt_outcome::table
            .filter(receipt_outcome::block_height.eq(bigdecimal::BigDecimal::from(block_height)))
            .filter(receipt_outcome::transaction_hash.eq(transaction_hash))
            .select(Self::as_select())
            .load(&mut conn)
            .await?;
        Ok(response)
    }

    pub async fn delete_receipt_outcome(
        mut conn: crate::postgres::PgAsyncConn,
        block_height: u64,
        transaction_hash: &str,
    ) -> anyhow::Result<()> {
        diesel::delete(
            receipt_outcome::table
                .filter(
                    receipt_outcome::block_height.eq(bigdecimal::BigDecimal::from(block_height)),
                )
                .filter(receipt_outcome::transaction_hash.eq(transaction_hash)),
        )
        .execute(&mut conn)
        .await?;
        Ok(())
    }
}

/// Metadata table
#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = meta)]
pub struct Meta {
    pub indexer_id: String,
    pub last_processed_block_height: bigdecimal::BigDecimal,
}

impl Meta {
    pub async fn insert_or_update(
        &self,
        mut conn: crate::postgres::PgAsyncConn,
    ) -> anyhow::Result<()> {
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
        let response = meta::table
            .filter(meta::indexer_id.eq(indexer_id))
            .select(Self::as_select())
            .first(&mut conn)
            .await?;

        Ok(response.last_processed_block_height)
    }
}

#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = validators)]
pub struct Validators {
    pub epoch_id: String,
    pub epoch_height: bigdecimal::BigDecimal,
    pub epoch_start_height: bigdecimal::BigDecimal,
    pub epoch_end_height: Option<bigdecimal::BigDecimal>,
    pub validators_info: serde_json::Value,
}

impl Validators {
    pub async fn insert_or_ignore(
        &self,
        mut conn: crate::postgres::PgAsyncConn,
    ) -> anyhow::Result<()> {
        diesel::insert_into(validators::table)
            .values(self)
            .on_conflict_do_nothing()
            .execute(&mut conn)
            .await?;
        Ok(())
    }

    pub async fn update_epoch_end_height(
        mut conn: crate::postgres::PgAsyncConn,
        epoch_id: near_indexer_primitives::CryptoHash,
        epoch_end_height: bigdecimal::BigDecimal,
    ) -> anyhow::Result<()> {
        diesel::update(validators::table)
            .filter(validators::epoch_id.eq(epoch_id.to_string()))
            .set(validators::epoch_end_height.eq(epoch_end_height))
            .execute(&mut conn)
            .await?;
        Ok(())
    }

    pub async fn get_validators(
        mut conn: crate::postgres::PgAsyncConn,
        epoch_id: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<Self> {
        let response = validators::table
            .filter(validators::epoch_id.eq(epoch_id.to_string()))
            .select(Self::as_select())
            .first(&mut conn)
            .await?;

        Ok(response)
    }
    pub async fn get_validators_epoch_end_height(
        mut conn: crate::postgres::PgAsyncConn,
        epoch_end_height: bigdecimal::BigDecimal,
    ) -> anyhow::Result<Self> {
        let response = validators::table
            .filter(validators::epoch_end_height.eq(epoch_end_height))
            .select(Self::as_select())
            .first(&mut conn)
            .await?;

        Ok(response)
    }
}

#[derive(Insertable, Queryable, Selectable)]
#[diesel(table_name = protocol_configs)]
pub struct ProtocolConfig {
    pub epoch_id: String,
    pub epoch_height: bigdecimal::BigDecimal,
    pub epoch_start_height: bigdecimal::BigDecimal,
    pub epoch_end_height: Option<bigdecimal::BigDecimal>,
    pub protocol_config: serde_json::Value,
}

impl ProtocolConfig {
    pub async fn insert_or_ignore(
        &self,
        mut conn: crate::postgres::PgAsyncConn,
    ) -> anyhow::Result<()> {
        diesel::insert_into(protocol_configs::table)
            .values(self)
            .on_conflict_do_nothing()
            .execute(&mut conn)
            .await?;
        Ok(())
    }

    pub async fn update_epoch_end_height(
        mut conn: crate::postgres::PgAsyncConn,
        epoch_id: near_indexer_primitives::CryptoHash,
        epoch_end_height: bigdecimal::BigDecimal,
    ) -> anyhow::Result<()> {
        diesel::update(protocol_configs::table)
            .filter(protocol_configs::epoch_id.eq(epoch_id.to_string()))
            .set(protocol_configs::epoch_end_height.eq(epoch_end_height))
            .execute(&mut conn)
            .await?;
        Ok(())
    }

    pub async fn get_protocol_config(
        mut conn: crate::postgres::PgAsyncConn,
        epoch_id: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<Self> {
        let response = protocol_configs::table
            .filter(protocol_configs::epoch_id.eq(epoch_id.to_string()))
            .select(Self::as_select())
            .first(&mut conn)
            .await?;

        Ok(response)
    }
}
