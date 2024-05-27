use std::convert::TryFrom;
use std::str::FromStr;

use futures::StreamExt;
use num_traits::ToPrimitive;
use scylla::{prepared_statement::PreparedStatement, IntoTypedRows};

use crate::scylladb::ScyllaStorageManager;

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
    get_indexing_transaction_by_hash: PreparedStatement,
    get_indexing_transaction_receipts: PreparedStatement,
    get_stored_at_block_height_and_shard_id_by_block_height: PreparedStatement,
    get_validators_by_epoch_id: PreparedStatement,
    get_protocol_config_by_epoch_id: PreparedStatement,
    get_validators_by_end_block_height: PreparedStatement,
}

#[async_trait::async_trait]
impl crate::BaseDbManager for ScyllaDBManager {
    async fn new(config: &configuration::DatabaseConfig) -> anyhow::Result<Box<Self>> {
        let scylla_db_session = std::sync::Arc::new(
            Self::get_scylladb_session(
                &config.database_url,
                config.database_user.as_deref(),
                config.database_password.as_deref(),
                config.preferred_dc.as_deref(),
                config.keepalive_interval,
                config.max_retry,
                config.strict_mode,
            )
            .await?,
        );
        Self::new_from_session(scylla_db_session).await
    }
}

#[async_trait::async_trait]
impl ScyllaStorageManager for ScyllaDBManager {
    async fn create_tables(scylla_db_session: &scylla::Session) -> anyhow::Result<()> {
        // Creating index in the tx_indexer_cache.transactions
        // for faster search by transaction_hash to avoid ALLOW FILTERING
        scylla_db_session
            .query(
                "
                CREATE INDEX IF NOT EXISTS transaction_hash_key ON tx_indexer_cache.transactions (transaction_hash);
            ",
                &[],
            )
            .await?;
        Ok(())
    }

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
                "SELECT transaction_details FROM tx_indexer.transactions_details WHERE transaction_hash = ? ORDER BY block_height ASC LIMIT 1",
            ).await?,
            get_indexing_transaction_by_hash: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT transaction_details FROM tx_indexer_cache.transactions WHERE transaction_hash = ? LIMIT 1",
            ).await?,
            get_indexing_transaction_receipts: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT receipt, outcome FROM tx_indexer_cache.receipts_outcomes WHERE block_height = ? AND transaction_hash = ?"
            ).await?,
            get_stored_at_block_height_and_shard_id_by_block_height: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT stored_at_block_height, shard_id FROM state_indexer.chunks WHERE block_height = ?",
            ).await?,
            get_validators_by_epoch_id: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT epoch_height, validators_info FROM state_indexer.validators WHERE epoch_id = ?",
            ).await?,
            get_protocol_config_by_epoch_id: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT protocol_config FROM state_indexer.protocol_configs WHERE epoch_id = ?",
            ).await?,
            get_validators_by_end_block_height: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT epoch_id, epoch_height, validators_info FROM state_indexer.validators WHERE epoch_end_height = ?",
            ).await?,
        }))
    }
}

#[async_trait::async_trait]
impl crate::ReaderDbManager for ScyllaDBManager {
    /// Searches the block height by the given block hash
    async fn get_block_by_hash(
        &self,
        block_hash: near_primitives::hash::CryptoHash,
        method_name: &str,
    ) -> anyhow::Result<u64> {
        crate::metrics::DATABASE_READ_QUERIES
            .with_label_values(&[method_name, "state_indexer.blocks"])
            .inc();
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
        method_name: &str,
    ) -> anyhow::Result<readnode_primitives::BlockHeightShardId> {
        crate::metrics::DATABASE_READ_QUERIES
            .with_label_values(&[method_name, "state_indexer.chunks"])
            .inc();
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

    /// Returns 25000 state keys for the given account id
    /// We limited the number of state keys returned because of the following reasons:
    /// 1. The query is very slow and takes a lot of time to execute
    /// 2. The contract state could be very large and we don't want to return all of it at once
    /// To get all state keys use `get_state_keys_by_page` method
    async fn get_state_keys_all(
        &self,
        account_id: &near_primitives::types::AccountId,
        method_name: &str,
    ) -> anyhow::Result<Vec<readnode_primitives::StateKey>> {
        crate::metrics::DATABASE_READ_QUERIES
            .with_label_values(&[method_name, "state_indexer.account_state"])
            .inc();
        let mut paged_query = self.get_all_state_keys.clone();
        paged_query.set_page_size(25000);
        let mut rows_stream = self
            .scylla_session
            .execute_iter(paged_query, (account_id.to_string(),))
            .await?
            .into_typed::<(String,)>();
        let mut state_keys = vec![];
        while let Some(next_row_res) = rows_stream.next().await {
            let (value,): (String,) = next_row_res?;
            state_keys.push(hex::decode(value)?);
        }
        Ok(state_keys)
    }

    /// Return contract state keys by page
    /// The page size is 1000 keys
    /// The page_token is a hex string of the scylla page_state
    /// On the first call the page_token should be None
    /// On the last page the page_token will be None
    async fn get_state_keys_by_page(
        &self,
        account_id: &near_primitives::types::AccountId,
        page_token: crate::PageToken,
        method_name: &str,
    ) -> anyhow::Result<(Vec<readnode_primitives::StateKey>, crate::PageToken)> {
        crate::metrics::DATABASE_READ_QUERIES
            .with_label_values(&[method_name, "state_indexer.account_state"])
            .inc();
        let mut paged_query = self.get_all_state_keys.clone();
        paged_query.set_page_size(1000);

        let result = if let Some(page_state) = page_token {
            let page_state = bytes::Bytes::from(hex::decode(page_state)?);
            self.scylla_session
                .execute_paged(&paged_query, (account_id.to_string(),), Some(page_state))
                .await?
        } else {
            Self::execute_prepared_query(
                &self.scylla_session,
                &paged_query,
                (account_id.to_string(),),
            )
            .await?
        };

        let new_page_token = result.paging_state.as_ref().map(hex::encode);

        let state_keys = result
            .rows_typed::<(String,)>()?
            .filter_map(|row| row.ok().and_then(|(value,)| hex::decode(value).ok()));

        Ok((state_keys.collect(), new_page_token))
    }

    /// Returns state keys for the given account id filtered by the given prefix
    async fn get_state_keys_by_prefix(
        &self,
        account_id: &near_primitives::types::AccountId,
        prefix: &[u8],
        method_name: &str,
    ) -> anyhow::Result<Vec<readnode_primitives::StateKey>> {
        crate::metrics::DATABASE_READ_QUERIES
            .with_label_values(&[method_name, "state_indexer.account_state"])
            .inc();
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
        method_name: &str,
    ) -> (
        readnode_primitives::StateKey,
        readnode_primitives::StateValue,
    ) {
        crate::metrics::DATABASE_READ_QUERIES
            .with_label_values(&[method_name, "state_indexer.state_changes_data"])
            .inc();
        let value = match Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_state_key_value,
            (
                account_id.to_string(),
                num_bigint::BigInt::from(block_height),
                hex::encode(&key_data).to_string(),
            ),
        )
        .await
        {
            Ok(result) => {
                let (value,) = result
                    .single_row_typed::<(readnode_primitives::StateValue,)>()
                    .unwrap_or_default();
                value
            }
            Err(_) => readnode_primitives::StateValue::default(),
        };

        (key_data, value)
    }

    /// Returns the near_primitives::account::Account at the given block height
    async fn get_account(
        &self,
        account_id: &near_primitives::types::AccountId,
        request_block_height: near_primitives::types::BlockHeight,
        method_name: &str,
    ) -> anyhow::Result<readnode_primitives::QueryData<near_primitives::account::Account>> {
        crate::metrics::DATABASE_READ_QUERIES
            .with_label_values(&[method_name, "state_indexer.state_changes_account"])
            .inc();
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
        method_name: &str,
    ) -> anyhow::Result<readnode_primitives::QueryData<Vec<u8>>> {
        crate::metrics::DATABASE_READ_QUERIES
            .with_label_values(&[method_name, "state_indexer.state_changes_contract"])
            .inc();
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
        method_name: &str,
    ) -> anyhow::Result<readnode_primitives::QueryData<near_primitives::account::AccessKey>> {
        crate::metrics::DATABASE_READ_QUERIES
            .with_label_values(&[method_name, "state_indexer.state_changes_access_key"])
            .inc();
        let key_data = borsh::to_vec(&public_key)?;
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
        method_name: &str,
    ) -> anyhow::Result<std::collections::HashMap<String, Vec<u8>>> {
        crate::metrics::DATABASE_READ_QUERIES
            .with_label_values(&[method_name, "state_indexer.account_access_keys"])
            .inc();
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
        method_name: &str,
    ) -> anyhow::Result<readnode_primitives::ReceiptRecord> {
        crate::metrics::DATABASE_READ_QUERIES
            .with_label_values(&[method_name, "tx_indexer.receipts_map"])
            .inc();
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
        method_name: &str,
    ) -> anyhow::Result<readnode_primitives::TransactionDetails> {
        if let Ok(transaction) = self
            .get_indexed_transaction_by_hash(transaction_hash, method_name)
            .await
        {
            Ok(transaction)
        } else {
            self.get_indexing_transaction_by_hash(transaction_hash, method_name)
                .await
        }
    }

    /// Returns the readnode_primitives::TransactionDetails
    /// from tx_indexer.transactions_details at the given transaction hash
    async fn get_indexed_transaction_by_hash(
        &self,
        transaction_hash: &str,
        method_name: &str,
    ) -> anyhow::Result<readnode_primitives::TransactionDetails> {
        crate::metrics::DATABASE_READ_QUERIES
            .with_label_values(&[method_name, "tx_indexer.transactions_details"])
            .inc();
        let (data_value,) = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_transaction_by_hash,
            (transaction_hash.to_string(),),
        )
        .await?
        .single_row()?
        .into_typed::<(Vec<u8>,)>()?;
        match borsh::from_slice::<readnode_primitives::TransactionDetails>(&data_value) {
            Ok(tx) => Ok(tx),
            Err(e) => {
                tracing::warn!(
                    "Failed tx_details borsh deserialize: TX_HASH - {}, ERROR: {:?}",
                    transaction_hash,
                    e
                );
                anyhow::bail!("Failed to parse transaction details")
            }
        }
    }

    /// Returns the readnode_primitives::TransactionDetails
    /// from tx_indexer_cache.transactions at the given transaction hash
    async fn get_indexing_transaction_by_hash(
        &self,
        transaction_hash: &str,
        method_name: &str,
    ) -> anyhow::Result<readnode_primitives::TransactionDetails> {
        crate::metrics::DATABASE_READ_QUERIES
            .with_label_values(&[method_name, "tx_indexer_cache.transactions"])
            .inc();
        let (data_value,) = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_indexing_transaction_by_hash,
            (transaction_hash.to_string(),),
        )
        .await?
        .single_row()?
        .into_typed::<(Vec<u8>,)>()?;
        let mut transaction_details =
            borsh::from_slice::<readnode_primitives::CollectingTransactionDetails>(&data_value)?;

        crate::metrics::DATABASE_READ_QUERIES
            .with_label_values(&[method_name, "tx_indexer_cache.receipts_outcomes"])
            .inc();
        let mut rows_stream = self
            .scylla_session
            .execute_iter(
                self.get_indexing_transaction_receipts.clone(),
                (
                    num_bigint::BigInt::from(transaction_details.block_height),
                    transaction_hash.to_string(),
                ),
            )
            .await?
            .into_typed::<(Vec<u8>, Vec<u8>)>();
        while let Some(next_row_res) = rows_stream.next().await {
            let (receipt, outcome) = next_row_res?;
            let receipt = borsh::from_slice::<near_primitives::views::ReceiptView>(&receipt)?;
            let execution_outcome =
                borsh::from_slice::<near_primitives::views::ExecutionOutcomeWithIdView>(&outcome)?;
            transaction_details.receipts.push(receipt);
            transaction_details
                .execution_outcomes
                .push(execution_outcome);
        }

        Ok(transaction_details.into())
    }

    /// Returns the block height and shard id by the given block height
    async fn get_block_by_height_and_shard_id(
        &self,
        block_height: near_primitives::types::BlockHeight,
        shard_id: near_primitives::types::ShardId,
        method_name: &str,
    ) -> anyhow::Result<readnode_primitives::BlockHeightShardId> {
        crate::metrics::DATABASE_READ_QUERIES
            .with_label_values(&[method_name, "state_indexer.chunks"])
            .inc();
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

    async fn get_validators_by_epoch_id(
        &self,
        epoch_id: near_primitives::hash::CryptoHash,
        method_name: &str,
    ) -> anyhow::Result<readnode_primitives::EpochValidatorsInfo> {
        crate::metrics::DATABASE_READ_QUERIES
            .with_label_values(&[method_name, "state_indexer.validators"])
            .inc();
        let (epoch_height, validators_info) = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_validators_by_epoch_id,
            (epoch_id.to_string(),),
        )
        .await?
        .single_row()?
        .into_typed::<(num_bigint::BigInt, String)>()?;

        let validators_info: near_primitives::views::EpochValidatorInfo =
            serde_json::from_str(&validators_info)?;

        Ok(readnode_primitives::EpochValidatorsInfo {
            epoch_id,
            epoch_height: epoch_height
                .to_u64()
                .ok_or_else(|| anyhow::anyhow!("Failed to parse `epoch_height` to u64"))?,
            epoch_start_height: validators_info.epoch_start_height,
            validators_info,
        })
    }

    async fn get_protocol_config_by_epoch_id(
        &self,
        epoch_id: near_primitives::hash::CryptoHash,
        method_name: &str,
    ) -> anyhow::Result<near_chain_configs::ProtocolConfigView> {
        crate::metrics::DATABASE_READ_QUERIES
            .with_label_values(&[method_name, "state_indexer.protocol_configs"])
            .inc();
        let (protocol_config,) = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_protocol_config_by_epoch_id,
            (epoch_id.to_string(),),
        )
        .await?
        .single_row()?
        .into_typed::<(String,)>()?;

        let protocol_config: near_chain_configs::ProtocolConfigView =
            serde_json::from_str(&protocol_config)?;

        Ok(protocol_config)
    }

    async fn get_validators_by_end_block_height(
        &self,
        block_height: near_primitives::types::BlockHeight,
        method_name: &str,
    ) -> anyhow::Result<readnode_primitives::EpochValidatorsInfo> {
        crate::metrics::DATABASE_READ_QUERIES
            .with_label_values(&[method_name, "state_indexer.validators"])
            .inc();
        let (epoch_id, epoch_height, validators_info) = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_validators_by_end_block_height,
            (num_bigint::BigInt::from(block_height),),
        )
        .await?
        .single_row()?
        .into_typed::<(String, num_bigint::BigInt, String)>()?;

        let epoch_id = near_indexer_primitives::CryptoHash::from_str(&epoch_id)
            .map_err(|err| anyhow::anyhow!("Failed to parse `epoch_id` to CryptoHash: {}", err))?;

        let validators_info: near_primitives::views::EpochValidatorInfo =
            serde_json::from_str(&validators_info)?;

        Ok(readnode_primitives::EpochValidatorsInfo {
            epoch_id,
            epoch_height: epoch_height
                .to_u64()
                .ok_or_else(|| anyhow::anyhow!("Failed to parse `epoch_height` to u64"))?,
            epoch_start_height: validators_info.epoch_start_height,
            validators_info,
        })
    }
}
