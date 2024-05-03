use crate::scylladb::ScyllaStorageManager;
use futures::StreamExt;
use num_traits::ToPrimitive;
use scylla::prepared_statement::PreparedStatement;

pub struct ScyllaDBManager {
    scylla_session: std::sync::Arc<scylla::Session>,
    add_transaction: PreparedStatement,
    add_receipt: PreparedStatement,
    update_meta: PreparedStatement,
    last_processed_block_height: PreparedStatement,
    get_transaction_by_hash: PreparedStatement,

    cache_get_all_transactions: PreparedStatement,
    cache_get_transaction: PreparedStatement,
    cache_get_transaction_by_receipt_id: PreparedStatement,
    cache_get_receipts: PreparedStatement,
    cache_add_transaction: PreparedStatement,
    cache_delete_transaction: PreparedStatement,
    cache_add_receipt: PreparedStatement,
    cache_delete_receipts: PreparedStatement,
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

impl ScyllaDBManager {
    async fn task_get_transactions_by_token_range(
        session: std::sync::Arc<scylla::Session>,
        prepared: PreparedStatement,
        token_val_range_start: i64,
        token_val_range_end: i64,
    ) -> anyhow::Result<Vec<readnode_primitives::CollectingTransactionDetails>> {
        let mut result = vec![];
        let mut rows_stream = session
            .execute_iter(prepared, (token_val_range_start, token_val_range_end))
            .await?
            .into_typed::<(Vec<u8>,)>();
        while let Some(next_row_res) = rows_stream.next().await {
            let (transaction_details,) = next_row_res?;
            let transaction_details = borsh::from_slice::<
                readnode_primitives::CollectingTransactionDetails,
            >(&transaction_details)?;
            result.push(transaction_details);
        }
        Ok(result)
    }
}

#[async_trait::async_trait]
impl ScyllaStorageManager for ScyllaDBManager {
    async fn create_tables(scylla_db_session: &scylla::Session) -> anyhow::Result<()> {
        scylla_db_session.use_keyspace("tx_indexer", false).await?;
        scylla_db_session
            .query(
                "CREATE TABLE IF NOT EXISTS transactions_details (
                transaction_hash varchar,
                block_height varint,
                account_id varchar,
                transaction_details BLOB,
                PRIMARY KEY (transaction_hash, block_height)
            ) WITH CLUSTERING ORDER BY (block_height DESC)
            ",
                &[],
            )
            .await?;

        scylla_db_session
            .query(
                "CREATE TABLE IF NOT EXISTS receipts_map (
                receipt_id varchar,
                block_height varint,
                parent_transaction_hash varchar,
                shard_id varint,
                PRIMARY KEY (receipt_id)
            )
            ",
                &[],
            )
            .await?;

        scylla_db_session
            .query(
                "
                CREATE TABLE IF NOT EXISTS meta (
                    indexer_id varchar PRIMARY KEY,
                    last_processed_block_height varint
                )
            ",
                &[],
            )
            .await?;

        scylla_db_session
            .use_keyspace("tx_indexer_cache", false)
            .await?;
        scylla_db_session
            .query(
                "CREATE TABLE IF NOT EXISTS transactions (
                block_height varint,
                transaction_hash varchar,
                transaction_details BLOB,
                PRIMARY KEY (block_height, transaction_hash)
            )
            ",
                &[],
            )
            .await?;

        scylla_db_session
            .query(
                "CREATE TABLE IF NOT EXISTS receipts_outcomes (
                block_height varint,
                transaction_hash varchar,
                receipt_id varchar,
                receipt BLOB,
                outcome BLOB,
                PRIMARY KEY (block_height, transaction_hash, receipt_id)
            )
            ",
                &[],
            )
            .await?;
        scylla_db_session
            .query(
                "
                CREATE INDEX IF NOT EXISTS transaction_key_receipt_id ON receipts_outcomes (receipt_id);
            ",
                &[],
            )
            .await?;

        Ok(())
    }

    async fn create_keyspace(scylla_db_session: &scylla::Session) -> anyhow::Result<()> {
        scylla_db_session.query(
            "CREATE KEYSPACE IF NOT EXISTS tx_indexer WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}",
            &[]
        ).await?;
        scylla_db_session.query(
            "CREATE KEYSPACE IF NOT EXISTS tx_indexer_cache WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}",
            &[]
        ).await?;
        Ok(())
    }

    async fn prepare(
        scylla_db_session: std::sync::Arc<scylla::Session>,
    ) -> anyhow::Result<Box<Self>> {
        Ok(Box::new(Self {
            scylla_session: scylla_db_session.clone(),
            add_transaction: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO tx_indexer.transactions_details
                    (transaction_hash, block_height, account_id, transaction_details)
                    VALUES(?, ?, ?, ?)",
            )
                .await?,
            add_receipt: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO tx_indexer.receipts_map
                    (receipt_id, block_height, parent_transaction_hash, shard_id)
                    VALUES(?, ?, ?, ?)",
            )
                .await?,
            update_meta: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO tx_indexer.meta
                    (indexer_id, last_processed_block_height)
                    VALUES (?, ?)",
            )
                .await?,

            last_processed_block_height: Self::prepare_write_query(
                &scylla_db_session,
                "SELECT last_processed_block_height FROM tx_indexer.meta WHERE indexer_id = ?",
            )
                .await?,
            get_transaction_by_hash: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT transaction_details FROM tx_indexer.transactions_details WHERE transaction_hash = ? LIMIT 1",
            ).await?,
            cache_get_all_transactions: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT transaction_details FROM tx_indexer_cache.transactions WHERE token(block_height) >= ? AND token(block_height) <= ?"
            ).await?,

            cache_get_transaction: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT transaction_details FROM tx_indexer_cache.transactions WHERE block_height = ? AND transaction_hash = ?"
            ).await?,

            cache_get_transaction_by_receipt_id: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT block_height, transaction_hash FROM tx_indexer_cache.receipts_outcomes WHERE receipt_id = ? LIMIT 1"
            ).await?,
            cache_get_receipts: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT receipt, outcome FROM tx_indexer_cache.receipts_outcomes WHERE block_height = ? AND transaction_hash = ?"
            ).await?,

            cache_add_transaction: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO tx_indexer_cache.transactions
                    (block_height, transaction_hash, transaction_details)
                    VALUES(?, ?, ?)",
            )
                .await?,
            cache_delete_transaction: Self::prepare_write_query(
                &scylla_db_session,
                "DELETE FROM tx_indexer_cache.transactions WHERE block_height = ? AND transaction_hash = ?",
            )
                .await?,
            cache_add_receipt: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO tx_indexer_cache.receipts_outcomes
                    (block_height, transaction_hash, receipt_id, receipt, outcome)
                    VALUES(?, ?, ?, ?, ?)",
            )
                .await?,
            cache_delete_receipts: Self::prepare_write_query(
                &scylla_db_session,
                "DELETE FROM tx_indexer_cache.receipts_outcomes WHERE block_height = ? AND transaction_hash = ?",
            )
                .await?,
        }))
    }
}

#[async_trait::async_trait]
impl crate::TxIndexerDbManager for ScyllaDBManager {
    async fn add_transaction(
        &self,
        transaction_hash: &str,
        tx_bytes: Vec<u8>,
        block_height: u64,
        signer_id: &str,
    ) -> anyhow::Result<()> {
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.add_transaction,
            (
                transaction_hash.to_string(),
                num_bigint::BigInt::from(block_height),
                signer_id.to_string(),
                &tx_bytes,
            ),
        )
        .await?;

        Ok(())
    }

    // This function is used to validate that the transaction is saved correctly.
    // For some unknown reason, tx-indexer saves invalid data for transactions.
    // We want to avoid these problems and get more information.
    // That's why we added this method.
    async fn validate_saved_transaction_deserializable(
        &self,
        transaction_hash: &str,
        tx_bytes: &[u8],
    ) -> anyhow::Result<bool> {
        let (data_value,) = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_transaction_by_hash,
            (transaction_hash.to_string(),),
        )
        .await?
        .single_row()?
        .into_typed::<(Vec<u8>,)>()?;

        match borsh::from_slice::<readnode_primitives::TransactionDetails>(&data_value) {
            Ok(_) => Ok(true),
            Err(err) => {
                tracing::warn!(
                    "Failed tx_details borsh deserialize: TX_HASH - {}, ERROR: {:?}, DATA_EQUAL: {}",
                    transaction_hash,
                    err,
                    tx_bytes.eq(&data_value)
                );
                anyhow::bail!("Failed to parse transaction details")
            }
        }
    }

    async fn add_receipt(
        &self,
        receipt_id: &str,
        parent_tx_hash: &str,
        block_height: u64,
        shard_id: u64,
    ) -> anyhow::Result<()> {
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.add_receipt,
            (
                receipt_id,
                num_bigint::BigInt::from(block_height),
                parent_tx_hash,
                num_bigint::BigInt::from(shard_id),
            ),
        )
        .await?;
        Ok(())
    }

    async fn update_meta(&self, indexer_id: &str, block_height: u64) -> anyhow::Result<()> {
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.update_meta,
            (indexer_id, num_bigint::BigInt::from(block_height)),
        )
        .await?;
        Ok(())
    }

    async fn cache_add_transaction(
        &self,
        transaction_details: readnode_primitives::CollectingTransactionDetails,
    ) -> anyhow::Result<()> {
        let transaction_hash = transaction_details.transaction.hash.clone().to_string();
        let block_height = transaction_details.block_height;
        let transaction_details = borsh::to_vec(&transaction_details).map_err(|err| {
            tracing::error!(target: "tx_indexer", "Failed to serialize transaction details: {:?}", err);
            err})?;
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.cache_add_transaction,
            (
                num_bigint::BigInt::from(block_height),
                transaction_hash,
                transaction_details,
            ),
        )
        .await
        .map_err(|err| {
            tracing::error!(target: "tx_indexer", "Failed to cache_add_transaction: {:?}", err);
            err
        })?;
        Ok(())
    }

    async fn cache_add_receipt(
        &self,
        transaction_key: readnode_primitives::TransactionKey,
        indexer_execution_outcome_with_receipt: near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
    ) -> anyhow::Result<()> {
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.cache_add_receipt,
            (
                num_bigint::BigInt::from(transaction_key.block_height),
                transaction_key.transaction_hash,
                indexer_execution_outcome_with_receipt
                    .receipt
                    .receipt_id
                    .to_string(),
                borsh::to_vec(&indexer_execution_outcome_with_receipt.receipt)?,
                borsh::to_vec(&indexer_execution_outcome_with_receipt.execution_outcome)?,
            ),
        )
        .await?;
        Ok(())
    }

    async fn get_transactions_to_cache(
        &self,
        start_block_height: u64,
        cache_restore_blocks_range: u64,
        max_db_parallel_queries: i64,
    ) -> anyhow::Result<
        std::collections::HashMap<
            readnode_primitives::TransactionKey,
            readnode_primitives::CollectingTransactionDetails,
        >,
    > {
        // N = Parallel queries = (nodes in cluster) ✕ (cores in node) ✕ 3. 6 - nodes, 8 - cpus
        //
        // M = N * 100
        //
        // We will process M sub-ranges, but only N in parallel;
        // the rest will wait. As a sub-range query completes,
        // we will pick a new sub-range and start processing it,
        // until we have completed all M.
        let sub_ranges = max_db_parallel_queries * 100;
        let step = i64::MAX / (sub_ranges / 2);

        let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(
            max_db_parallel_queries as usize,
        ));
        let mut handlers = vec![];
        for token_val_range_start in (i64::MIN..=i64::MAX).step_by(step as usize) {
            let session = self.scylla_session.clone();
            let prepared = self.cache_get_all_transactions.clone();
            let permit = sem.clone().acquire_owned().await;
            handlers.push(tokio::task::spawn(async move {
                let result = Self::task_get_transactions_by_token_range(
                    session,
                    prepared,
                    token_val_range_start,
                    token_val_range_start.saturating_add(step).saturating_sub(1),
                )
                .await;
                let _permit = permit;
                result
            }));
        }

        let mut results = std::collections::HashMap::new();
        for thread in handlers {
            for transaction in thread.await?? {
                let transaction_key = transaction.transaction_key();

                // Collect transactions that the indexer could potentially collect.
                // For this, we use the range of blocks from the beginning of the index to minus 1000 blocks.
                // This range should include all transactions that the indexer can collect.
                if transaction_key.block_height <= start_block_height
                    && transaction_key.block_height
                        >= start_block_height - cache_restore_blocks_range
                {
                    results.insert(transaction_key.clone(), transaction);
                };
            }
        }
        Ok(results)
    }

    async fn get_transaction_by_receipt_id(
        &self,
        receipt_id: &str,
    ) -> anyhow::Result<readnode_primitives::CollectingTransactionDetails> {
        let (transaction_hash, block_height) = Self::execute_prepared_query(
            &self.scylla_session,
            &self.cache_get_transaction_by_receipt_id,
            (receipt_id,),
        )
        .await?
        .single_row()?
        .into_typed::<(String, num_bigint::BigInt)>()?;

        let (transaction_details,) = Self::execute_prepared_query(
            &self.scylla_session,
            &self.cache_get_transaction,
            (block_height, transaction_hash),
        )
        .await?
        .single_row()?
        .into_typed::<(Vec<u8>,)>()?;

        Ok(borsh::from_slice::<
            readnode_primitives::CollectingTransactionDetails,
        >(&transaction_details)?)
    }

    async fn get_receipts_in_cache(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
    ) -> anyhow::Result<Vec<near_indexer_primitives::IndexerExecutionOutcomeWithReceipt>> {
        let mut result = vec![];
        let mut rows_stream = self
            .scylla_session
            .execute_iter(
                self.cache_get_receipts.clone(),
                (
                    num_bigint::BigInt::from(transaction_key.block_height),
                    transaction_key.transaction_hash.clone(),
                ),
            )
            .await?
            .into_typed::<(Vec<u8>, Vec<u8>)>();
        while let Some(next_row_res) = rows_stream.next().await {
            let (receipt, outcome) = next_row_res?;
            let receipt = borsh::from_slice::<near_primitives::views::ReceiptView>(&receipt)?;
            let execution_outcome =
                borsh::from_slice::<near_primitives::views::ExecutionOutcomeWithIdView>(&outcome)?;
            result.push(
                near_indexer_primitives::IndexerExecutionOutcomeWithReceipt {
                    receipt,
                    execution_outcome,
                },
            );
        }
        Ok(result)
    }

    async fn cache_delete_transaction(
        &self,
        transaction_hash: &str,
        block_height: u64,
    ) -> anyhow::Result<()> {
        let delete_transaction_future = Self::execute_prepared_query(
            &self.scylla_session,
            &self.cache_delete_transaction,
            (num_bigint::BigInt::from(block_height), transaction_hash),
        );
        let delete_receipts_future = Self::execute_prepared_query(
            &self.scylla_session,
            &self.cache_delete_receipts,
            (num_bigint::BigInt::from(block_height), transaction_hash),
        );
        futures::try_join!(delete_transaction_future, delete_receipts_future)?;
        Ok(())
    }

    async fn get_last_processed_block_height(&self, indexer_id: &str) -> anyhow::Result<u64> {
        let (block_height,) = Self::execute_prepared_query(
            &self.scylla_session,
            &self.last_processed_block_height,
            (indexer_id,),
        )
        .await?
        .single_row()?
        .into_typed::<(num_bigint::BigInt,)>()?;
        block_height
            .to_u64()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse `block_height` to u64"))
    }
}
