use crate::storage::base::TxCollectingStorage;

const STORAGE: &str = "storage_tx";

pub struct HashStorage {
    transactions: futures_locks::RwLock<
        std::collections::HashMap<String, readnode_primitives::CollectingTransactionDetails>,
    >,
    receipts_counters: futures_locks::RwLock<std::collections::HashMap<String, u64>>,
    receipts_watching_list: futures_locks::RwLock<std::collections::HashMap<String, String>>,
    transactions_to_save: futures_locks::RwLock<
        std::collections::HashMap<String, readnode_primitives::CollectingTransactionDetails>,
    >,
}

impl HashStorage {
    #[allow(unused)]
    pub(crate) fn new() -> Self {
        Self {
            transactions: futures_locks::RwLock::new(std::collections::HashMap::new()),
            receipts_counters: futures_locks::RwLock::new(std::collections::HashMap::new()),
            receipts_watching_list: futures_locks::RwLock::new(std::collections::HashMap::new()),
            transactions_to_save: futures_locks::RwLock::new(std::collections::HashMap::new()),
        }
    }
}

#[async_trait::async_trait]
impl TxCollectingStorage for HashStorage {
    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn push_receipt_to_watching_list(
        &self,
        receipt_id: String,
        transaction_hash: String,
    ) -> anyhow::Result<()> {
        *self
            .receipts_counters
            .write()
            .await
            .entry(transaction_hash.to_string())
            .or_insert(0) += 1;
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
    ) -> anyhow::Result<Option<String>> {
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
            Ok(Some(transaction_hash))
        } else {
            Ok(None)
        }
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn receipts_transaction_hash_count(&self, transaction_hash: &str) -> anyhow::Result<u64> {
        self.receipts_counters
            .read()
            .await
            .get(transaction_hash)
            .copied()
            .ok_or(anyhow::anyhow!(
                "No such transaction hash {}",
                transaction_hash
            ))
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn set_tx(
        &self,
        transaction_details: readnode_primitives::CollectingTransactionDetails,
    ) -> anyhow::Result<()> {
        let transaction_hash = transaction_details.transaction.hash.clone().to_string();
        self.transactions.write().await.insert(
            transaction_details.transaction.hash.to_string(),
            transaction_details,
        );
        tracing::debug!(target: STORAGE, "+T {}", transaction_hash,);
        Ok(())
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn get_tx(
        &self,
        transaction_hash: &str,
    ) -> Option<readnode_primitives::CollectingTransactionDetails> {
        self.transactions
            .read()
            .await
            .get(transaction_hash)
            .cloned()
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn push_tx_to_save(
        &self,
        transaction_details: readnode_primitives::CollectingTransactionDetails,
    ) -> anyhow::Result<()> {
        self.transactions_to_save.write().await.insert(
            transaction_details.transaction.hash.to_string(),
            transaction_details,
        );
        Ok(())
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn get_transaction_hash_by_receipt_id(
        &self,
        receipt_id: &str,
    ) -> anyhow::Result<Option<String>> {
        Ok(self
            .receipts_watching_list
            .read()
            .await
            .get(receipt_id)
            .cloned())
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
        if let Some(mut transaction_details) = self.get_tx(transaction_hash).await {
            self.remove_receipt_from_watching_list(
                &indexer_execution_outcome_with_receipt
                    .receipt
                    .receipt_id
                    .to_string(),
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
                self.push_tx_to_save(transaction_details.clone()).await?;
                self.transactions.write().await.remove(transaction_hash);
                self.receipts_counters
                    .write()
                    .await
                    .remove(transaction_hash);
                tracing::debug!(target: STORAGE, "-T {}", transaction_hash);
            } else {
                self.set_tx(transaction_details.clone()).await?;
            }
        } else {
            tracing::error!(
                target: STORAGE,
                "No such transaction hash {}",
                transaction_hash
            );
        }
        Ok(())
    }
}
