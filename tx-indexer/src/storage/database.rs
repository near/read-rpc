use crate::storage::base::TxCollectingStorage;
use futures::StreamExt;

const STORAGE: &str = "storage_tx";

pub struct HashStorageWithDB {
    scylla_db_manager: std::sync::Arc<crate::config::ScyllaDBManager>,
    transactions: futures_locks::RwLock<
        std::collections::HashMap<String, readnode_primitives::CollectingTransactionDetails>,
    >,
    receipts_counters: futures_locks::RwLock<std::collections::HashMap<String, u64>>,
    receipts_watching_list: futures_locks::RwLock<std::collections::HashMap<String, String>>,
    transactions_to_save: futures_locks::RwLock<
        std::collections::HashMap<String, readnode_primitives::CollectingTransactionDetails>,
    >,
}

impl HashStorageWithDB {
    /// Init storage without restore transactions with receipts after interruption
    pub(crate) async fn init_storage(
        scylla_db_client: std::sync::Arc<crate::config::ScyllaDBManager>,
    ) -> Self {
        Self {
            scylla_db_manager: scylla_db_client,
            transactions: futures_locks::RwLock::new(std::collections::HashMap::new()),
            receipts_counters: futures_locks::RwLock::new(std::collections::HashMap::new()),
            receipts_watching_list: futures_locks::RwLock::new(std::collections::HashMap::new()),
            transactions_to_save: futures_locks::RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// Init storage with restore transactions with receipts after interruption
    pub(crate) async fn init_with_restore(
        scylla_db_client: std::sync::Arc<crate::config::ScyllaDBManager>,
    ) -> anyhow::Result<Self> {
        let storage = Self::init_storage(scylla_db_client).await;
        storage
            .restore_transactions_with_receipts_after_interruption()
            .await?;
        Ok(storage)
    }

    /// Restore transactions with receipts after interruption
    async fn restore_transactions_with_receipts_after_interruption(&self) -> anyhow::Result<()> {
        for (transaction_hash, transaction_details) in self
            .scylla_db_manager
            .get_transactions_in_cache()
            .await?
            .iter()
        {
            self.update_tx(transaction_details.clone()).await?;
            let receipt_id = transaction_details
                .execution_outcomes
                .first()
                .expect("No execution outcomes")
                .outcome
                .receipt_ids
                .first()
                .expect("`receipt_ids` must contain one Receipt ID")
                .to_string();
            self.push_receipt_to_watching_list(receipt_id.clone(), transaction_hash.to_string())
                .await?;

            for indexer_execution_outcome_with_receipt in self
                .scylla_db_manager
                .get_receipts_in_cache(transaction_hash)
                .await?
                .iter()
            {
                let mut tasks = futures::stream::FuturesUnordered::new();
                tasks.extend(
                    indexer_execution_outcome_with_receipt
                        .execution_outcome
                        .outcome
                        .receipt_ids
                        .iter()
                        .map(|receipt_id| {
                            self.push_receipt_to_watching_list(
                                receipt_id.to_string(),
                                transaction_hash.clone(),
                            )
                        }),
                );
                while let Some(result) = tasks.next().await {
                    let _ = result.map_err(|e| {
                        tracing::debug!(
                            target: crate::INDEXER,
                            "Task encountered an error: {:#?}",
                            e
                        )
                    });
                }

                self.push_outcome_and_receipt_to_hash(
                    transaction_hash,
                    indexer_execution_outcome_with_receipt.clone(),
                )
                .await?;
            }
            tracing::info!(
                target: STORAGE,
                "Transaction uploaded from storage {}",
                transaction_hash,
            );
        }
        Ok(())
    }

    async fn push_outcome_and_receipt_to_db(
        &self,
        transaction_hash: String,
        indexer_execution_outcome_with_receipt: near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
    ) {
        let scylla_db_manager = self.scylla_db_manager.clone();
        tokio::spawn(async move {
            scylla_db_manager
                .cache_add_receipt(&transaction_hash, indexer_execution_outcome_with_receipt)
                .await
        });
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn push_outcome_and_receipt_to_hash(
        &self,
        transaction_hash: &str,
        indexer_execution_outcome_with_receipt: near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
    ) -> anyhow::Result<()> {
        let mut transaction_details = self.get_tx(transaction_hash).await?;
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
            self.move_tx_to_save(transaction_details.clone()).await?;
        } else {
            self.update_tx(transaction_details.clone()).await?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl TxCollectingStorage for HashStorageWithDB {
    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn push_receipt_to_watching_list(
        &self,
        receipt_id: String,
        transaction_hash: String,
    ) -> anyhow::Result<()> {
        self.receipts_counters
            .write()
            .await
            .entry(transaction_hash.to_string())
            .and_modify(|counter| *counter += 1)
            .or_insert(1);
        self.receipts_watching_list
            .write()
            .await
            .insert(receipt_id.clone(), transaction_hash.clone());
        tracing::debug!(target: STORAGE, "+R {} - {}", receipt_id, transaction_hash);
        Ok(())
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn remove_receipt_from_watching_list(
        &self,
        receipt_id: &str,
        _transaction_hash: &str,
    ) -> anyhow::Result<()> {
        if let Some(transaction_hash) = self.receipts_watching_list.write().await.remove(receipt_id)
        {
            if let Some(receipts_counter) = self
                .receipts_counters
                .write()
                .await
                .get_mut(&transaction_hash)
            {
                *receipts_counter -= 1;
            }
            tracing::debug!(target: STORAGE, "-R {} - {}", receipt_id, transaction_hash);
        };
        Ok(())
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn receipts_transaction_hash_count(&self, transaction_hash: &str) -> anyhow::Result<u64> {
        self.receipts_counters
            .read()
            .await
            .get(transaction_hash)
            .copied()
            .ok_or(anyhow::anyhow!(
                "No such transaction hash `receipts_transaction_hash_count` {}",
                transaction_hash
            ))
    }
    async fn update_tx(
        &self,
        transaction_details: readnode_primitives::CollectingTransactionDetails,
    ) -> anyhow::Result<()> {
        let transaction_hash = transaction_details.transaction.hash.clone().to_string();
        self.transactions
            .write()
            .await
            .insert(transaction_hash, transaction_details);
        Ok(())
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn set_tx(
        &self,
        transaction_details: readnode_primitives::CollectingTransactionDetails,
    ) -> anyhow::Result<()> {
        let transaction_details_clone = transaction_details.clone();
        let scylla_db_manager = self.scylla_db_manager.clone();
        tokio::spawn(async move {
            scylla_db_manager
                .cache_add_transaction(transaction_details_clone)
                .await
        });
        let transaction_hash = transaction_details.transaction.hash.clone().to_string();
        self.update_tx(transaction_details).await?;
        tracing::debug!(target: STORAGE, "+T {}", transaction_hash,);
        Ok(())
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn get_tx(
        &self,
        transaction_hash: &str,
    ) -> anyhow::Result<readnode_primitives::CollectingTransactionDetails> {
        match self
            .transactions
            .read()
            .await
            .get(transaction_hash)
            .cloned()
        {
            Some(transaction_details) => Ok(transaction_details),
            None => Err(anyhow::anyhow!(
                "No such transaction hash `get_tx` {}",
                transaction_hash
            )),
        }
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn move_tx_to_save(
        &self,
        transaction_details: readnode_primitives::CollectingTransactionDetails,
    ) -> anyhow::Result<()> {
        let transaction_hash = transaction_details.transaction.hash.to_string();
        self.transactions_to_save
            .write()
            .await
            .insert(transaction_hash.clone(), transaction_details);
        self.transactions.write().await.remove(&transaction_hash);
        self.receipts_counters
            .write()
            .await
            .remove(&transaction_hash);
        tracing::debug!(target: STORAGE, "-T {}", transaction_hash);
        Ok(())
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn get_transaction_hash_by_receipt_id(&self, receipt_id: &str) -> anyhow::Result<String> {
        match self
            .receipts_watching_list
            .read()
            .await
            .get(receipt_id)
            .cloned()
        {
            Some(transaction_hash) => Ok(transaction_hash),
            None => Err(anyhow::anyhow!("No such receipt id {}", receipt_id)),
        }
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn transactions_to_save(
        &self,
    ) -> anyhow::Result<Vec<readnode_primitives::CollectingTransactionDetails>> {
        let mut transactions = vec![];
        let mut transactions_hashes = vec![];
        for (transaction_hash, transaction_details) in self.transactions_to_save.read().await.iter()
        {
            transactions.push(transaction_details.clone());
            transactions_hashes.push(transaction_hash.clone());
        }
        for transaction_hash in transactions_hashes.iter() {
            self.transactions_to_save
                .write()
                .await
                .remove(transaction_hash);
        }
        Ok(transactions)
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn push_outcome_and_receipt(
        &self,
        transaction_hash: &str,
        indexer_execution_outcome_with_receipt: near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
    ) -> anyhow::Result<()> {
        self.push_outcome_and_receipt_to_db(
            transaction_hash.to_string(),
            indexer_execution_outcome_with_receipt.clone(),
        )
        .await;
        self.push_outcome_and_receipt_to_hash(
            transaction_hash,
            indexer_execution_outcome_with_receipt,
        )
        .await?;
        Ok(())
    }
}
