use std::convert::TryFrom;

use borsh::{BorshDeserialize, BorshSerialize};
use num_traits::ToPrimitive;
use scylla::{prepared_statement::PreparedStatement, IntoTypedRows};

use crate::ScyllaStorageManager;

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
                "SELECT block_height, block_hash, data_value FROM state_indexer.state_changes_account WHERE account_id = ? AND block_height <= ? LIMIT 1",
            ).await?,

            get_contract_code: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT block_height, block_hash, data_value FROM state_indexer.state_changes_contract WHERE account_id = ? AND block_height <= ? LIMIT 1",
            ).await?,

            get_access_key: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT block_height, block_hash, data_value FROM state_indexer.state_changes_access_key WHERE account_id = ? AND block_height <= ? AND data_key = ? LIMIT 1",
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

#[async_trait::async_trait]
impl crate::RpcDbManager for ScyllaDBManager {
    /// Searches the block height by the given block hash
    async fn get_block_by_hash(
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
    async fn get_block_by_chunk_hash(
        &self,
        chunk_hash: near_primitives::hash::CryptoHash,
    ) -> anyhow::Result<readnode_primitives::BlockHeightShardId> {
        let block_height_shard_id = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_block_by_chunk_id,
            (chunk_hash.to_string(),),
        )
        .await?
        .single_row()?
        .into_typed::<(num_bigint::BigInt, num_bigint::BigInt)>();

        block_height_shard_id
            .map(readnode_primitives::BlockHeightShardId::try_from)
            .unwrap_or_else(|err| {
                Err(anyhow::anyhow!(
                    "Block height and shard id not found for chunk hash {}\n{:?}",
                    chunk_hash,
                    err,
                ))
            })
    }

    /// Returns all state keys for the given account id
    async fn get_all_state_keys(
        &self,
        account_id: &near_primitives::types::AccountId,
    ) -> anyhow::Result<Vec<readnode_primitives::StateKey>> {
        let result = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_all_state_keys,
            (account_id.to_string(),),
        )
        .await?
        .rows_typed::<(String,)>()?
        .filter_map(|row| row.ok().and_then(|(value,)| hex::decode(value).ok()));

        Ok(result.collect())
    }

    /// Returns state keys for the given account id filtered by the given prefix
    async fn get_state_keys_by_prefix(
        &self,
        account_id: &near_primitives::types::AccountId,
        prefix: &[u8],
    ) -> anyhow::Result<Vec<readnode_primitives::StateKey>> {
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
        .rows_typed::<(String,)>()?
        .filter_map(|row| row.ok().and_then(|(value,)| hex::decode(value).ok()));

        Ok(result.collect())
    }

    /// Returns the state value for the given key of the given account at the given block height
    async fn get_state_key_value(
        &self,
        account_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        key_data: readnode_primitives::StateKey,
    ) -> anyhow::Result<readnode_primitives::StateValue> {
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
        .single_row_typed::<(readnode_primitives::StateValue,)>()?;

        Ok(result.0)
    }

    /// Returns the near_primitives::account::Account at the given block height
    async fn get_account(
        &self,
        account_id: &near_primitives::types::AccountId,
        request_block_height: near_primitives::types::BlockHeight,
    ) -> anyhow::Result<readnode_primitives::QueryData<near_primitives::account::Account>> {
        let (block_height, block_hash, data_blob) = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_account,
            (
                account_id.to_string(),
                num_bigint::BigInt::from(request_block_height),
            ),
        )
        .await?
        .single_row()?
        .into_typed::<(num_bigint::BigInt, String, Vec<u8>)>()?;

        let block = readnode_primitives::BlockRecord::try_from((block_hash, block_height))?;

        readnode_primitives::QueryData::<near_primitives::account::Account>::try_from((
            data_blob,
            block.height,
            block.hash,
        ))
    }

    /// Returns the contract code at the given block height
    async fn get_contract_code(
        &self,
        account_id: &near_primitives::types::AccountId,
        request_block_height: near_primitives::types::BlockHeight,
    ) -> anyhow::Result<readnode_primitives::QueryData<Vec<u8>>> {
        let (block_height, block_hash, contract_code) = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_contract_code,
            (
                account_id.to_string(),
                num_bigint::BigInt::from(request_block_height),
            ),
        )
        .await?
        .single_row()?
        .into_typed::<(num_bigint::BigInt, String, Vec<u8>)>()?;

        let block = readnode_primitives::BlockRecord::try_from((block_hash, block_height))?;

        Ok(readnode_primitives::QueryData {
            data: contract_code,
            block_height: block.height,
            block_hash: block.hash,
        })
    }

    /// Returns the near_primitives::account::AccessKey at the given block height
    async fn get_access_key(
        &self,
        account_id: &near_primitives::types::AccountId,
        request_block_height: near_primitives::types::BlockHeight,
        public_key: near_crypto::PublicKey,
    ) -> anyhow::Result<readnode_primitives::QueryData<near_primitives::account::AccessKey>> {
        let key_data = public_key.try_to_vec()?;
        let (block_height, block_hash, data_blob) = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_access_key,
            (
                account_id.to_string(),
                num_bigint::BigInt::from(request_block_height),
                hex::encode(&key_data).to_string(),
            ),
        )
        .await?
        .single_row()?
        .into_typed::<(num_bigint::BigInt, String, Vec<u8>)>()?;

        let block = readnode_primitives::BlockRecord::try_from((block_hash, block_height))?;

        readnode_primitives::QueryData::<near_primitives::account::AccessKey>::try_from((
            data_blob,
            block.height,
            block.hash,
        ))
    }

    #[cfg(feature = "account_access_keys")]
    async fn get_account_access_keys(
        &self,
        account_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
    ) -> anyhow::Result<std::collections::HashMap<String, Vec<u8>>> {
        let (account_keys,) = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_account_access_keys,
            (
                account_id.to_string(),
                num_bigint::BigInt::from(block_height),
            ),
        )
        .await?
        .single_row()?
        .into_typed::<(std::collections::HashMap<String, Vec<u8>>,)>()?;
        Ok(account_keys)
    }

    /// Returns the near_primitives::views::ReceiptView at the given receipt_id
    async fn get_receipt_by_id(
        &self,
        receipt_id: near_primitives::hash::CryptoHash,
    ) -> anyhow::Result<readnode_primitives::ReceiptRecord> {
        let row = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_receipt,
            (receipt_id.to_string(),),
        )
        .await?
        .single_row()?
        .into_typed::<(String, String, num_bigint::BigInt, num_bigint::BigInt)>()?;

        readnode_primitives::ReceiptRecord::try_from(row)
    }

    /// Returns the readnode_primitives::TransactionDetails at the given transaction hash
    async fn get_transaction_by_hash(
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
    async fn get_block_by_height_and_shard_id(
        &self,
        block_height: near_primitives::types::BlockHeight,
        shard_id: near_primitives::types::ShardId,
    ) -> anyhow::Result<readnode_primitives::BlockHeightShardId> {
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
            .map(readnode_primitives::BlockHeightShardId::try_from)
            .unwrap_or_else(|| {
                Err(anyhow::anyhow!(
                    "Block height {} and shard id {} not found",
                    block_height,
                    shard_id
                ))
            })
    }
}
