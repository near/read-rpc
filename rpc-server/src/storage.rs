use std::convert::TryFrom;
use std::str::FromStr;

use borsh::BorshDeserialize;
use num_traits::ToPrimitive;
use scylla::{prepared_statement::PreparedStatement, IntoTypedRows};

use database::ScyllaStorageManager;

pub struct BlockHeightShardId(pub u64, pub u64);
pub struct QueryData<T: BorshDeserialize> {
    pub data: T,
    pub block_height: u64,
}
pub struct ReceiptRecord {
    pub receipt_id: near_primitives::hash::CryptoHash,
    pub parent_transaction_hash: near_primitives::hash::CryptoHash,
    pub block_height: near_primitives::types::BlockHeight,
    pub shard_id: near_primitives::types::ShardId,
}

pub struct ScyllaDBManager {
    scylla_session: std::sync::Arc<scylla::Session>,
    get_block_by_hash: PreparedStatement,
    get_block_by_chunk_id: PreparedStatement,
    get_all_state_keys: PreparedStatement,
    get_state_keys_by_prefix: PreparedStatement,
    get_state_key_value: PreparedStatement,
    get_account: PreparedStatement,
    get_contract_code: PreparedStatement,
    get_access_key: PreparedStatement,
    #[cfg(feature = "account_access_keys")]
    get_account_access_keys: PreparedStatement,
    get_receipt: PreparedStatement,
    get_transaction_by_hash: PreparedStatement,
    get_stored_at_block_height_and_shard_id_by_block_height: PreparedStatement,
}

#[async_trait::async_trait]
impl ScyllaStorageManager for ScyllaDBManager {
    async fn prepare(
        scylla_db_session: std::sync::Arc<scylla::Session>,
    ) -> anyhow::Result<Box<Self>> {
        Ok(Box::new(Self {
            scylla_session: scylla_db_session.clone(),

            get_block_by_hash: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT block_height FROM state_indexer.blocks WHERE block_hash = ?",
            ).await?,

            get_block_by_chunk_id: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT stored_at_block_height, shard_id FROM state_indexer.chunks WHERE chunk_hash = ? LIMIT 1",
            ).await?,

            get_all_state_keys: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT data_key FROM state_indexer.account_state WHERE account_id = ?",
            ).await?,

            get_state_keys_by_prefix: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT data_key FROM state_indexer.account_state WHERE account_id = ? AND data_key LIKE ?",
            ).await?,

            get_state_key_value: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT data_value FROM state_indexer.state_changes_data WHERE account_id = ? AND block_height <= ? AND data_key = ? LIMIT 1",
            ).await?,

            get_account: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT block_height, data_value FROM state_indexer.state_changes_account WHERE account_id = ? AND block_height <= ? LIMIT 1",
            ).await?,

            get_contract_code: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT block_height, data_value FROM state_indexer.state_changes_contract WHERE account_id = ? AND block_height <= ? LIMIT 1",
            ).await?,

            get_access_key: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT block_height, data_value FROM state_indexer.state_changes_access_key WHERE account_id = ? AND block_height <= ? AND data_key = ? LIMIT 1",
            ).await?,
            #[cfg(feature = "account_access_keys")]
            get_account_access_keys: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT active_access_keys FROM state_indexer.account_access_keys WHERE account_id = ? AND block_height <= ? LIMIT 1",
            ).await?,

            get_receipt: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT receipt_id, parent_transaction_hash, block_height, shard_id FROM tx_indexer.receipts_map WHERE receipt_id = ?",
            ).await?,

            // Using LIMIT 1 here as transactions is expected to be ordered by block_height but we know about hash collisions
            // ref: https://github.com/near/near-indexer-for-explorer/issues/84
            get_transaction_by_hash: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT transaction_details FROM tx_indexer.transactions_details WHERE transaction_hash = ? LIMIT 1",
            ).await?,

            get_stored_at_block_height_and_shard_id_by_block_height: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT stored_at_block_height, shard_id FROM state_indexer.chunks WHERE block_height = ?",
            ).await?,
        }))
    }
}

impl ScyllaDBManager {
    /// Searches the block height by the given block hash
    pub async fn get_block_by_hash(
        &self,
        block_hash: near_primitives::hash::CryptoHash,
    ) -> anyhow::Result<u64> {
        let (result,) = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_block_by_hash,
            (block_hash.to_string(),),
        )
        .await?
        .single_row()?
        .into_typed::<(num_bigint::BigInt,)>()?;

        result
            .to_u64()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse `block_height` to u64"))
    }

    /// Searches the block height and shard id by the given chunk hash
    pub async fn get_block_by_chunk_hash(
        &self,
        chunk_hash: near_primitives::hash::CryptoHash,
    ) -> anyhow::Result<BlockHeightShardId> {
        let block_height_shard_id = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_block_by_chunk_id,
            (chunk_hash.to_string(),),
        )
        .await?
        .single_row()?
        .into_typed::<(num_bigint::BigInt, num_bigint::BigInt)>();

        block_height_shard_id
            .map(BlockHeightShardId::try_from)
            .unwrap_or_else(|err| {
                Err(anyhow::anyhow!(
                    "Block height and shard id not found for chunk hash {}\n{:?}",
                    chunk_hash,
                    err,
                ))
            })
    }

    // TODO: refactor to respond with meaningful struct instead of raw scylla::frame::response::result::Row
    pub async fn get_all_state_keys(
        &self,
        account_id: &near_primitives::types::AccountId,
    ) -> anyhow::Result<Vec<scylla::frame::response::result::Row>> {
        let result = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_all_state_keys,
            (account_id.to_string(),),
        )
        .await?
        .rows;
        if let Some(rows) = result {
            Ok(rows)
        } else {
            Ok(vec![])
        }
    }

    // TODO: refactor to respond with meaningful struct instead of raw scylla::frame::response::result::Row
    pub async fn get_state_keys_by_prefix(
        &self,
        account_id: &near_primitives::types::AccountId,
        prefix: &[u8],
    ) -> anyhow::Result<Vec<scylla::frame::response::result::Row>> {
        let hex_str_prefix = hex::encode(prefix);
        let result = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_state_keys_by_prefix,
            (
                account_id.to_string(),
                format!("{hex_str_prefix}%").to_string(),
            ),
        )
        .await?
        .rows;
        if let Some(rows) = result {
            Ok(rows)
        } else {
            Ok(vec![])
        }
    }

    // TODO: refactor to respond with meaningful struct instead of raw scylla::frame::response::result::Row
    pub async fn get_state_key_value(
        &self,
        account_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        key_data: Vec<u8>,
    ) -> anyhow::Result<scylla::frame::response::result::Row> {
        let result = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_state_key_value,
            (
                account_id.to_string(),
                num_bigint::BigInt::from(block_height),
                hex::encode(&key_data).to_string(),
            ),
        )
        .await?
        .single_row()?;
        Ok(result)
    }

    /// Returns the near_primitives::account::Account at the given block height
    pub async fn get_account(
        &self,
        account_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
    ) -> anyhow::Result<QueryData<near_primitives::account::Account>> {
        let (data_blob, block_height) = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_account,
            (
                account_id.to_string(),
                num_bigint::BigInt::from(block_height),
            ),
        )
        .await?
        .single_row()?
        .into_typed::<(Vec<u8>, num_bigint::BigInt)>()?;

        QueryData::<near_primitives::account::Account>::try_from((data_blob, block_height))
    }

    /// Returns the contract code at the given block height
    pub async fn get_contract_code(
        &self,
        account_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
    ) -> anyhow::Result<QueryData<Vec<u8>>> {
        let (contract_code, block_height) = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_contract_code,
            (
                account_id.to_string(),
                num_bigint::BigInt::from(block_height),
            ),
        )
        .await?
        .single_row()?
        .into_typed::<(Vec<u8>, num_bigint::BigInt)>()?;

        Ok(QueryData {
            data: contract_code,
            block_height: block_height
                .to_u64()
                .ok_or_else(|| anyhow::anyhow!("Failed to parse `block_height` to u64"))?,
        })
    }

    /// Returns the near_primitives::account::AccessKey at the given block height
    pub async fn get_access_key(
        &self,
        account_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        key_data: &Vec<u8>,
    ) -> anyhow::Result<QueryData<near_primitives::account::AccessKey>> {
        let result = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_access_key,
            (
                account_id.to_string(),
                num_bigint::BigInt::from(block_height),
                hex::encode(key_data).to_string(),
            ),
        )
        .await?
        .single_row()?
        .into_typed::<(Vec<u8>, num_bigint::BigInt)>()?;

        QueryData::<near_primitives::account::AccessKey>::try_from(result)
    }

    #[cfg(feature = "account_access_keys")]
    pub async fn get_account_access_keys(
        &self,
        account_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
    ) -> anyhow::Result<scylla::frame::response::result::Row> {
        let result = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_account_access_keys,
            (
                account_id.to_string(),
                num_bigint::BigInt::from(block_height),
            ),
        )
        .await?
        .single_row()?;
        Ok(result)
    }

    /// Returns the near_primitives::views::ReceiptView at the given receipt_id
    pub async fn get_receipt_by_id(
        &self,
        receipt_id: near_primitives::hash::CryptoHash,
    ) -> anyhow::Result<ReceiptRecord> {
        let row = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_receipt,
            (receipt_id.to_string(),),
        )
        .await?
        .single_row()?
        .into_typed::<(String, String, num_bigint::BigInt, num_bigint::BigInt)>()?;

        ReceiptRecord::try_from(row)
    }

    /// Returns the readnode_primitives::TransactionDetails at the given transaction hash
    pub async fn get_transaction_by_hash(
        &self,
        transaction_hash: &str,
    ) -> anyhow::Result<readnode_primitives::TransactionDetails> {
        let (data_value,) = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_transaction_by_hash,
            (transaction_hash.to_string(),),
        )
        .await?
        .single_row()?
        .into_typed::<(Vec<u8>,)>()?;

        Ok(readnode_primitives::TransactionDetails::try_from_slice(
            &data_value,
        )?)
    }

    /// Returns the block height and shard id by the given block height
    pub async fn get_block_by_height_and_shard_id(
        &self,
        block_height: near_primitives::types::BlockHeight,
        shard_id: near_primitives::types::ShardId,
    ) -> anyhow::Result<BlockHeightShardId> {
        let rows = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_stored_at_block_height_and_shard_id_by_block_height,
            (num_bigint::BigInt::from(block_height),),
        )
        .await?
        .rows()?;

        let block_height_and_shard_id = rows
            .into_typed::<(num_bigint::BigInt, num_bigint::BigInt)>()
            .filter_map(Result::ok)
            .find(|(_, shard)| shard == &num_bigint::BigInt::from(shard_id));

        block_height_and_shard_id
            .map(BlockHeightShardId::try_from)
            .unwrap_or_else(|| {
                Err(anyhow::anyhow!(
                    "Block height {} and shard id {} not found",
                    block_height,
                    shard_id
                ))
            })
    }
}

// TryFrom impls for defined types

impl TryFrom<(num_bigint::BigInt, num_bigint::BigInt)> for BlockHeightShardId {
    type Error = anyhow::Error;

    fn try_from(value: (num_bigint::BigInt, num_bigint::BigInt)) -> Result<Self, Self::Error> {
        let stored_at_block_height = value
            .0
            .to_u64()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse `stored_at_block_height` to u64"))?;

        let parsed_shard_id = value
            .1
            .to_u64()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse `shard_id` to u64"))?;

        Ok(BlockHeightShardId(stored_at_block_height, parsed_shard_id))
    }
}

impl<T> TryFrom<(Vec<u8>, num_bigint::BigInt)> for QueryData<T>
where
    T: BorshDeserialize,
{
    type Error = anyhow::Error;

    fn try_from(value: (Vec<u8>, num_bigint::BigInt)) -> Result<Self, Self::Error> {
        let data = T::try_from_slice(&value.0)?;

        let block_height = value
            .1
            .to_u64()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse `block_height` to u64"))?;

        Ok(Self { data, block_height })
    }
}

impl TryFrom<(String, String, num_bigint::BigInt, num_bigint::BigInt)> for ReceiptRecord {
    type Error = anyhow::Error;

    fn try_from(
        value: (String, String, num_bigint::BigInt, num_bigint::BigInt),
    ) -> Result<Self, Self::Error> {
        let receipt_id =
            near_primitives::hash::CryptoHash::try_from(value.0.as_bytes()).map_err(|err| {
                anyhow::anyhow!("Failed to parse `receipt_id` to CryptoHash: {}", err)
            })?;
        let parent_transaction_hash = near_primitives::hash::CryptoHash::from_str(&value.1)
            .map_err(|err| {
                anyhow::anyhow!(
                    "Failed to parse `parent_transaction_hash` to CryptoHash: {}",
                    err
                )
            })?;
        let block_height = value
            .2
            .to_u64()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse `block_height` to u64"))?;
        let shard_id = value
            .3
            .to_u64()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse `shard_id` to u64"))?;

        Ok(ReceiptRecord {
            receipt_id,
            parent_transaction_hash,
            block_height,
            shard_id,
        })
    }
}
