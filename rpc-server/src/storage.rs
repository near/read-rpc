use std::convert::TryFrom;

use borsh::BorshDeserialize;
use num_traits::ToPrimitive;
use scylla::{prepared_statement::PreparedStatement, IntoTypedRows};

use database::ScyllaStorageManager;

pub struct BlockHeightShardId(pub u64, pub u64);

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
                "SELECT block_height FROM state_indexer.blocks WHERE block_hash = ? LIMIT 1",
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
                "SELECT data_value FROM state_indexer.state_changes_account WHERE account_id = ? AND block_height <= ? LIMIT 1",
            ).await?,

            get_contract_code: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT data_value FROM state_indexer.state_changes_contract WHERE account_id = ? AND block_height <= ? LIMIT 1",
            ).await?,

            get_access_key: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT data_value FROM state_indexer.state_changes_access_key WHERE account_id = ? AND block_height <= ? AND data_key = ? LIMIT 1",
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
    pub async fn get_block_by_hash(
        &self,
        block_hash: near_primitives::hash::CryptoHash,
    ) -> anyhow::Result<scylla::frame::response::result::Row> {
        let result = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_block_by_hash,
            (block_hash.to_string(),),
        )
        .await?
        .single_row()?;
        Ok(result)
    }

    pub async fn get_block_by_chunk_id(
        &self,
        chunk_id: near_primitives::hash::CryptoHash,
    ) -> anyhow::Result<scylla::frame::response::result::Row> {
        let result = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_block_by_chunk_id,
            (chunk_id.to_string(),),
        )
        .await?
        .single_row()?;
        Ok(result)
    }

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

    pub async fn get_account(
        &self,
        account_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
    ) -> anyhow::Result<near_primitives::account::Account> {
        let (data_value,): (Vec<u8>,) = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_account,
            (
                account_id.to_string(),
                num_bigint::BigInt::from(block_height),
            ),
        )
        .await?
        .single_row()?
        .into_typed::<(Vec<u8>,)>()?;

        Ok(near_primitives::account::Account::try_from_slice(
            &data_value,
        )?)
    }

    pub async fn get_contract_code(
        &self,
        account_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
    ) -> anyhow::Result<Vec<u8>> {
        let (result,) = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_contract_code,
            (
                account_id.to_string(),
                num_bigint::BigInt::from(block_height),
            ),
        )
        .await?
        .single_row()?
        .into_typed::<(Vec<u8>,)>()?;

        Ok(result)
    }

    pub async fn get_access_key(
        &self,
        account_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        key_data: &Vec<u8>,
    ) -> anyhow::Result<scylla::frame::response::result::Row> {
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
        .single_row()?;
        Ok(result)
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

    pub async fn get_receipt_by_id(
        &self,
        receipt_id: near_primitives::hash::CryptoHash,
    ) -> anyhow::Result<scylla::frame::response::result::Row> {
        let result = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_receipt,
            (receipt_id.to_string(),),
        )
        .await?
        .single_row()?;
        Ok(result)
    }

    pub async fn get_transaction_by_hash(
        &self,
        transaction_hash: &str,
    ) -> anyhow::Result<readnode_primitives::TransactionDetails> {
        let row = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_transaction_by_hash,
            (transaction_hash.to_string(),),
        )
        .await?
        .single_row()?;

        let (data_value,): (Vec<u8>,) = row.into_typed::<(Vec<u8>,)>()?;

        Ok(readnode_primitives::TransactionDetails::try_from_slice(
            &data_value,
        )?)
    }

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
