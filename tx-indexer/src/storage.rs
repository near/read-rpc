pub const STORAGE: &str = "storage_tx";

pub struct CacheStorage {
    storage: cache_storage::TxIndexerCache,
}

impl CacheStorage {
    /// Init storage without restore transactions with receipts after interruption
    pub(crate) async fn init_storage(redis_url: String) -> Self {
        let cache_storage = cache_storage::TxIndexerCache::new(redis_url)
            .await
            .expect("Failed connecting to redis");
        Self {
            storage: cache_storage,
        }
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    pub(crate) async fn push_receipt_to_watching_list(
        &self,
        receipt_id: near_indexer_primitives::CryptoHash,
        transaction_key: readnode_primitives::TransactionKey,
    ) -> anyhow::Result<()> {
        crate::metrics::RECEIPTS_IN_MEMORY_CACHE.inc();
        self.storage
            .set_receipt_to_watching_list(receipt_id, transaction_key.clone())
            .await?;
        tracing::debug!(
            target: STORAGE,
            "+R {} - {}",
            receipt_id,
            transaction_key.transaction_hash
        );
        Ok(())
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    pub(crate) async fn set_tx(
        &self,
        transaction_details: readnode_primitives::CollectingTransactionDetails,
    ) -> anyhow::Result<()> {
        let transaction_hash = transaction_details.transaction.hash.clone().to_string();
        self.storage.set_tx(transaction_details).await?;
        tracing::debug!(target: STORAGE, "+T {}", transaction_hash,);
        Ok(())
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    pub(crate) async fn get_tx(
        &self,
        transaction_key: readnode_primitives::TransactionKey,
    ) -> anyhow::Result<readnode_primitives::CollectingTransactionDetails> {
        self.storage.get_tx(&transaction_key).await
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    pub(crate) async fn remove_transaction_from_cache(
        &self,
        transaction_key: readnode_primitives::TransactionKey,
    ) -> anyhow::Result<()> {
        self.storage.del_tx(&transaction_key).await
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    pub(crate) async fn get_transaction_hash_by_receipt_id(
        &self,
        receipt_id: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<readnode_primitives::TransactionKey> {
        self.storage.get_tx_key_by_receipt_id(receipt_id).await
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    pub(crate) async fn transactions_to_save(
        &self,
    ) -> anyhow::Result<Vec<readnode_primitives::TransactionKey>> {
        self.storage.get_tx_to_save().await
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    pub(crate) async fn push_outcome_and_receipt(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
        indexer_execution_outcome_with_receipt: near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
    ) -> anyhow::Result<()> {
        // Check if transaction is in cache
        // And collect receipts for this transaction
        if let Ok(_) = self.storage.get_tx(transaction_key).await {
            // Remove receipt from watching list
            self.storage
                .del_receipt_from_watching_list(
                    indexer_execution_outcome_with_receipt.receipt.receipt_id,
                )
                .await?;
            crate::metrics::RECEIPTS_IN_MEMORY_CACHE.dec();
            tracing::debug!(
                target: STORAGE,
                "-R {} - {}",
                indexer_execution_outcome_with_receipt.receipt.receipt_id,
                transaction_key.transaction_hash
            );

            // Save outcome and receipt
            self.storage
                .set_outcomes_and_receipts(transaction_key, indexer_execution_outcome_with_receipt)
                .await?;

            // Check if all receipts are collected
            let transaction_receipts_watching_count = self
                .storage
                .get_receipts_counter(transaction_key.clone())
                .await?;
            if transaction_receipts_watching_count == 0 {
                // Mark transaction to save
                self.storage.set_tx_to_save(transaction_key.clone()).await?;
                tracing::debug!(
                    target: STORAGE,
                    "-T {}",
                    transaction_key.transaction_hash
                );
            }
        }
        Ok(())
    }
}
