use crate::storage::base::TxCollectingStorage;
use borsh::{BorshDeserialize, BorshSerialize};
use database::ScyllaStorageManager;
use scylla::prepared_statement::PreparedStatement;

pub(crate) struct ScyllaStorage {
    scylla_session: std::sync::Arc<scylla::Session>,
    add_transaction: PreparedStatement,
    get_transaction: PreparedStatement,
    delete_transaction: PreparedStatement,
    add_receipt: PreparedStatement,
    get_receipt_transaction_hash: PreparedStatement,
    delete_receipt: PreparedStatement,
    count_receipts: PreparedStatement,
    get_tx_to_save: PreparedStatement,
}

#[async_trait::async_trait]
impl ScyllaStorageManager for ScyllaStorage {
    async fn create_tables(scylla_db_session: &scylla::Session) -> anyhow::Result<()> {
        scylla_db_session
            .use_keyspace("tx_indexer_process", false)
            .await?;
        scylla_db_session
            .query(
                "CREATE TABLE IF NOT EXISTS transactions (
                transaction_hash varchar,
                transaction_details BLOB,
                is_completed boolean,
                PRIMARY KEY (transaction_hash)
            )
            ",
                &[],
            )
            .await?;

        scylla_db_session
            .query(
                "CREATE TABLE IF NOT EXISTS receipts_watching_list (
                receipt_id varchar,
                transaction_hash varchar,
                PRIMARY KEY (receipt_id, transaction_hash)
            )
            ",
                &[],
            )
            .await?;
        scylla_db_session
            .query(
                "
                CREATE INDEX IF NOT EXISTS transactions_is_completed ON transactions (is_completed);
            ",
                &[],
            )
            .await?;

        Ok(())
    }

    async fn create_keyspace(scylla_db_session: &scylla::Session) -> anyhow::Result<()> {
        scylla_db_session.query(
            "CREATE KEYSPACE IF NOT EXISTS tx_indexer_process WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}",
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
                "INSERT INTO tx_indexer_process.transactions
                    (transaction_hash, transaction_details, is_completed)
                    VALUES(?, ?, ?)",
            )
                .await?,
            get_transaction: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT transaction_details FROM tx_indexer_process.transactions WHERE transaction_hash = ?",
            )
                .await?,
            delete_transaction: Self::prepare_write_query(
                &scylla_db_session,
                "DELETE FROM tx_indexer_process.transactions WHERE transaction_hash in ?",
            ).await?,
            add_receipt: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO tx_indexer_process.receipts_watching_list
                    (receipt_id, transaction_hash)
                    VALUES(?, ?)",
            )
                .await?,
            get_receipt_transaction_hash: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT transaction_hash FROM tx_indexer_process.receipts_watching_list WHERE receipt_id = ?",
            )
                .await?,
            delete_receipt: Self::prepare_write_query(
                &scylla_db_session,
                "DELETE FROM tx_indexer_process.receipts_watching_list WHERE receipt_id = ?",
            ).await?,
            count_receipts: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT COUNT(1) FROM tx_indexer_process.receipts_watching_list WHERE transaction_hash = ?",
            )
                .await?,
            get_tx_to_save: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT transaction_details FROM tx_indexer_process.transactions WHERE is_completed = true",
            )
                .await?,
        }))
    }
}

#[async_trait::async_trait]
impl TxCollectingStorage for ScyllaStorage {
    async fn push_receipt_to_watching_list(
        &self,
        receipt_id: String,
        transaction_hash: String,
    ) -> anyhow::Result<()> {
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.add_receipt,
            (&receipt_id, &transaction_hash),
        )
        .await?;
        tracing::info!(
            target: crate::storage::STORAGE,
            "+R {} - {}",
            receipt_id,
            transaction_hash
        );
        Ok(())
    }

    async fn remove_receipt_from_watching_list(&self, receipt_id: &str, transaction_hash: &str) -> anyhow::Result<()> {
        Self::execute_prepared_query(&self.scylla_session, &self.delete_receipt, (&receipt_id,))
            .await?;
        tracing::info!(target: crate::storage::STORAGE, "-R {} - {}", receipt_id, transaction_hash);
        Ok(())
    }

    async fn receipts_transaction_hash_count(&self, transaction_hash: &str) -> anyhow::Result<u64> {
        let (count,) = Self::execute_prepared_query(
            &self.scylla_session,
            &self.count_receipts,
            (transaction_hash,),
        )
        .await?
        .single_row()?
        .into_typed::<(i64,)>()?;
        tracing::info!(target: crate::storage::STORAGE, "RC {} - {}", transaction_hash, count);
        Ok(count as u64)
    }

    async fn set_tx(
        &self,
        transaction_details: readnode_primitives::CollectingTransactionDetails,
    ) -> anyhow::Result<()> {
        let transaction_hash = transaction_details.transaction.hash.to_string();
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.add_transaction,
            (&transaction_hash, transaction_details.try_to_vec()?, false),
        )
        .await?;
        tracing::info!(target: crate::storage::STORAGE, "+T {}", transaction_hash);
        Ok(())
    }

    async fn get_tx(
        &self,
        transaction_hash: &str,
    ) -> anyhow::Result<readnode_primitives::CollectingTransactionDetails> {
        let (transaction_details,) = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_transaction,
            (transaction_hash,),
        )
        .await?
        .single_row()?
        .into_typed::<(Vec<u8>,)>()?;

        Ok(
            readnode_primitives::CollectingTransactionDetails::try_from_slice(
                &transaction_details,
            )?,
        )
    }

    async fn move_tx_to_save(
        &self,
        transaction_details: readnode_primitives::CollectingTransactionDetails,
    ) -> anyhow::Result<()> {
        let transaction_hash = transaction_details.transaction.hash.to_string();
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.add_transaction,
            (&transaction_hash, transaction_details.try_to_vec()?, true),
        )
        .await?;
        tracing::info!(target: crate::storage::STORAGE, "-T {}", transaction_hash);
        Ok(())
    }

    async fn get_transaction_hash_by_receipt_id(&self, receipt_id: &str) -> anyhow::Result<String> {
        let (result,) = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_receipt_transaction_hash,
            (receipt_id,),
        )
        .await?
        .single_row()?
        .into_typed::<(String,)>()?;
        Ok(result)
    }

    async fn transactions_to_save(
        &self,
    ) -> anyhow::Result<Vec<readnode_primitives::CollectingTransactionDetails>> {
        let result: Vec<readnode_primitives::CollectingTransactionDetails> =
            Self::execute_prepared_query(&self.scylla_session, &self.get_tx_to_save, ())
                .await?
                .rows_typed::<(Vec<u8>,)>()?
                .filter_map(|row| {
                    row.ok().and_then(|(tx_data,)| {
                        readnode_primitives::CollectingTransactionDetails::try_from_slice(&tx_data)
                            .ok()
                    })
                })
                .collect();
        let transaction_hashes: Vec<String> = result
            .iter()
            .map(|tx| tx.transaction.hash.to_string())
            .collect();
        if !transaction_hashes.is_empty() {
            Self::execute_prepared_query(
                &self.scylla_session,
                &self.delete_transaction,
                (transaction_hashes,),
            )
            .await?;
        }
        Ok(result)
    }

    async fn push_outcome_and_receipt(
        &self,
        transaction_hash: &str,
        indexer_execution_outcome_with_receipt: near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
    ) -> anyhow::Result<()> {
        if let Ok(mut transaction_details) = self.get_tx(transaction_hash).await {
            self.remove_receipt_from_watching_list(
                &indexer_execution_outcome_with_receipt
                    .receipt
                    .receipt_id
                    .to_string(),
                transaction_hash,
            )
            .await?;
            transaction_details
                .receipts
                .push(indexer_execution_outcome_with_receipt.receipt);
            transaction_details
                .execution_outcomes
                .push(indexer_execution_outcome_with_receipt.execution_outcome);
            let transaction_receipts_watching_count = self
                .receipts_transaction_hash_count(transaction_hash)
                .await?;
            if transaction_receipts_watching_count == 0 {
                self.move_tx_to_save(transaction_details).await?;
            } else {
                self.set_tx(transaction_details).await?;
            }
        } else {
            tracing::error!(
                target: crate::storage::STORAGE,
                "No such transaction hash {}",
                transaction_hash
            );
        }
        Ok(())
    }
}
