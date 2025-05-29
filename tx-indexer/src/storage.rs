use std::ops::Deref;

pub const STORAGE: &str = "storage_tx";

#[derive(Default)]
pub struct ReceiptsAndOutcomesCacheStorage {
    pub receipts: std::collections::HashMap<String, readnode_primitives::ReceiptRecord>,
    pub outcomes: std::collections::HashMap<String, readnode_primitives::OutcomeRecord>,
}

impl From<&ReceiptsAndOutcomesCacheStorage> for ReceiptsAndOutcomesToSave {
    fn from(receipts_and_outcomes: &ReceiptsAndOutcomesCacheStorage) -> Self {
        Self {
            receipts: receipts_and_outcomes.receipts.values().cloned().collect(),
            outcomes: receipts_and_outcomes.outcomes.values().cloned().collect(),
        }
    }
}

#[derive(Default, Clone)]
pub struct ReceiptsAndOutcomesToSave {
    pub receipts: Vec<readnode_primitives::ReceiptRecord>,
    pub outcomes: Vec<readnode_primitives::OutcomeRecord>,
}

pub struct CacheStorage {
    storage: cache_storage::TxIndexerCache,
    transactions: futures_locks::RwLock<
        std::collections::HashMap<
            readnode_primitives::TransactionKey,
            readnode_primitives::CollectingTransactionDetails,
        >,
    >,
    receipts_counters:
        futures_locks::RwLock<std::collections::HashMap<readnode_primitives::TransactionKey, u64>>,
    receipts_watching_list: futures_locks::RwLock<
        std::collections::HashMap<String, readnode_primitives::TransactionKey>,
    >,
    transactions_to_save: futures_locks::RwLock<
        std::collections::HashMap<
            readnode_primitives::TransactionKey,
            readnode_primitives::CollectingTransactionDetails,
        >,
    >,
    outcomes_and_receipts_to_save: futures_locks::RwLock<ReceiptsAndOutcomesCacheStorage>,
}

impl CacheStorage {
    /// Init storage without restore transactions with receipts after interruption
    pub(crate) async fn init_storage(redis_url: String) -> Self {
        let cache_storage = cache_storage::TxIndexerCache::new(redis_url)
            .await
            .expect("Failed connecting to redis");
        Self {
            storage: cache_storage,
            transactions: futures_locks::RwLock::new(std::collections::HashMap::new()),
            receipts_counters: futures_locks::RwLock::new(std::collections::HashMap::new()),
            receipts_watching_list: futures_locks::RwLock::new(std::collections::HashMap::new()),
            transactions_to_save: futures_locks::RwLock::new(std::collections::HashMap::new()),
            outcomes_and_receipts_to_save: futures_locks::RwLock::new(
                ReceiptsAndOutcomesCacheStorage::default(),
            ),
        }
    }

    /// Init storage with restore transactions with receipts after interruption
    pub(crate) async fn init_with_restore(redis_url: String) -> anyhow::Result<Self> {
        let storage = Self::init_storage(redis_url).await;
        storage
            .restore_transactions_with_receipts_after_interruption()
            .await?;
        Ok(storage)
    }

    /// Restore transactions with receipts after interruption
    async fn restore_transactions_with_receipts_after_interruption(&self) -> anyhow::Result<()> {
        let tx_in_process = self.storage.get_txs_in_process().await.unwrap_or_default();
        let tx_futures = tx_in_process
            .iter()
            .map(|tx_key| self.restore_transaction_with_receipts(tx_key));
        futures::future::join_all(tx_futures)
            .await
            .into_iter()
            .collect::<anyhow::Result<()>>()?;
        tracing::debug!(
            target: STORAGE,
            "Restored {} transactions after interruption",
            tx_in_process.len()
        );
        Ok(())
    }

    async fn restore_transaction_with_receipts(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
    ) -> anyhow::Result<()> {
        tracing::debug!(
            target: STORAGE,
            "Transaction restoring from storage {}",
            transaction_key.transaction_hash,
        );
        // The indexers work pretty fast.
        // We use the KEYS method to get the list of transactions, which is relatively slow.
        // And sometimes we get into a situation where the indexer already running has time to save the transaction
        // and we get an error `Unexpected length of input`.
        // This hook will help to avoid such situations when launching several indexers.
        if let Ok(tx_details) = self.storage.get_tx(transaction_key).await {
            self.update_tx(tx_details.clone()).await?;
            let receipt_id = tx_details
                .transaction_outcome
                .outcome
                .receipt_ids
                .first()
                .expect("`receipt_ids` must contain one Receipt ID")
                .to_string();
            self.push_receipt_to_watching_list(receipt_id.clone(), transaction_key.clone())
                .await?;
            for outcome in self.storage.get_tx_outcomes(transaction_key).await? {
                // Skip the outcome that is already in the transaction
                if outcome.execution_outcome.id == tx_details.transaction_outcome.id {
                    continue;
                };
                for receipt_id in outcome.execution_outcome.outcome.receipt_ids.iter() {
                    self.push_receipt_to_watching_list(
                        receipt_id.to_string(),
                        transaction_key.clone(),
                    )
                    .await?;
                }
                self.push_outcome_and_receipt_to_cache(transaction_key, outcome)
                    .await?;
            }
        }
        Ok(())
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn push_outcome_and_receipt_to_storage(
        &self,
        transaction_key: readnode_primitives::TransactionKey,
        indexer_execution_outcome_with_receipt: near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
    ) -> anyhow::Result<()> {
        self.storage
            .set_outcomes_and_receipts(&transaction_key, indexer_execution_outcome_with_receipt)
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn push_outcome_and_receipt_to_cache(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
        indexer_execution_outcome_with_receipt: near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
    ) -> anyhow::Result<()> {
        if let Ok(mut transaction_details) = self.get_tx(transaction_key).await {
            self.remove_receipt_from_watching_list(
                &indexer_execution_outcome_with_receipt
                    .receipt
                    .receipt_id
                    .to_string(),
            )
            .await?;
            transaction_details
                .receipts
                .push(indexer_execution_outcome_with_receipt.receipt.clone());
            transaction_details
                .execution_outcomes
                .push(indexer_execution_outcome_with_receipt.execution_outcome);
            // Check receipts counter and if all receipts and outcomes already collected
            // then we move the transaction to save otherwise update it and wait for the rest of the receipts
            if self.receipts_transaction_count(transaction_key).await? == 0 {
                self.move_tx_to_save(transaction_details.clone()).await?;
            } else {
                self.update_tx(transaction_details.clone()).await?;
            }
        }
        Ok(())
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    pub(crate) async fn push_receipt_to_watching_list(
        &self,
        receipt_id: String,
        transaction_key: readnode_primitives::TransactionKey,
    ) -> anyhow::Result<()> {
        crate::metrics::RECEIPTS_IN_MEMORY_CACHE.inc();
        self.receipts_counters
            .write()
            .await
            .entry(transaction_key.clone())
            .and_modify(|counter| *counter += 1)
            .or_insert(1);
        self.receipts_watching_list
            .write()
            .await
            .insert(receipt_id.clone(), transaction_key.clone());
        tracing::debug!(
            target: STORAGE,
            "+R {} - {}",
            receipt_id,
            transaction_key.transaction_hash
        );
        Ok(())
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    pub(crate) async fn remove_receipt_from_watching_list(
        &self,
        receipt_id: &str,
    ) -> anyhow::Result<()> {
        if let Some(transaction_key) = self.receipts_watching_list.write().await.remove(receipt_id)
        {
            if let Some(receipts_counter) = self
                .receipts_counters
                .write()
                .await
                .get_mut(&transaction_key)
            {
                *receipts_counter -= 1;
            }
            crate::metrics::RECEIPTS_IN_MEMORY_CACHE.dec();
            tracing::debug!(
                target: STORAGE,
                "-R {} - {}",
                receipt_id,
                transaction_key.transaction_hash
            );
        };
        Ok(())
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn receipts_transaction_count(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
    ) -> anyhow::Result<u64> {
        self.receipts_counters
            .read()
            .await
            .get(transaction_key)
            .copied()
            .ok_or(anyhow::anyhow!(
                "No such transaction hash `receipts_transaction_count` {}",
                transaction_key.transaction_hash
            ))
    }
    async fn update_tx(
        &self,
        transaction_details: readnode_primitives::CollectingTransactionDetails,
    ) -> anyhow::Result<()> {
        self.transactions
            .write()
            .await
            .insert(transaction_details.transaction_key(), transaction_details);
        Ok(())
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    pub(crate) async fn set_tx(
        &self,
        transaction_details: readnode_primitives::CollectingTransactionDetails,
    ) -> anyhow::Result<()> {
        self.storage.set_tx(transaction_details.clone()).await?;
        let transaction_hash = transaction_details.transaction.hash.clone().to_string();
        self.update_tx(transaction_details).await?;
        tracing::debug!(target: STORAGE, "+T {}", transaction_hash,);
        Ok(())
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn get_tx(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
    ) -> anyhow::Result<readnode_primitives::CollectingTransactionDetails> {
        match self.transactions.read().await.get(transaction_key).cloned() {
            Some(transaction_details) => Ok(transaction_details),
            None => Err(anyhow::anyhow!(
                "No such transaction hash `get_tx` {}",
                transaction_key.transaction_hash
            )),
        }
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    pub(crate) async fn move_tx_to_save(
        &self,
        transaction_details: readnode_primitives::CollectingTransactionDetails,
    ) -> anyhow::Result<()> {
        let transaction_key = transaction_details.transaction_key();
        self.transactions_to_save
            .write()
            .await
            .insert(transaction_key.clone(), transaction_details);
        self.transactions.write().await.remove(&transaction_key);
        self.receipts_counters
            .write()
            .await
            .remove(&transaction_key);
        tracing::debug!(
            target: STORAGE,
            "-T {}",
            transaction_key.transaction_hash
        );
        Ok(())
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
        receipt_id: &str,
    ) -> anyhow::Result<readnode_primitives::TransactionKey> {
        match self
            .receipts_watching_list
            .read()
            .await
            .get(receipt_id)
            .cloned()
        {
            Some(transaction_key) => Ok(transaction_key),
            None => Err(anyhow::anyhow!("No such receipt id {}", receipt_id)),
        }
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    pub(crate) async fn transactions_to_save(
        &self,
    ) -> anyhow::Result<Vec<readnode_primitives::CollectingTransactionDetails>> {
        let mut transactions = vec![];
        let mut transactions_hashes = vec![];
        for (transaction_key, transaction_details) in self.transactions_to_save.read().await.iter()
        {
            transactions.push(transaction_details.clone());
            transactions_hashes.push(transaction_key.clone());
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
    pub(crate) async fn outcomes_and_receipts_to_save(
        &self,
    ) -> anyhow::Result<ReceiptsAndOutcomesToSave> {
        let outcomes_and_receipts: ReceiptsAndOutcomesToSave = self
            .outcomes_and_receipts_to_save
            .read()
            .await
            .deref()
            .into();
        let mut outcomes_and_receipts_to_save_lock =
            self.outcomes_and_receipts_to_save.write().await;
        for receipt in &outcomes_and_receipts.receipts {
            outcomes_and_receipts_to_save_lock
                .receipts
                .remove(&receipt.receipt_id.to_string());
        }
        for outcome in &outcomes_and_receipts.outcomes {
            outcomes_and_receipts_to_save_lock
                .outcomes
                .remove(&outcome.outcome_id.to_string());
        }
        drop(outcomes_and_receipts_to_save_lock);
        Ok(outcomes_and_receipts)
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    pub(crate) async fn push_outcome_and_receipt_to_save(
        &self,
        outcome_id: &near_indexer_primitives::CryptoHash,
        receipt_id: &near_indexer_primitives::CryptoHash,
        parent_tx_hash: &near_indexer_primitives::CryptoHash,
        receiver_id: &near_indexer_primitives::types::AccountId,
        block: readnode_primitives::BlockRecord,
        shard_id: u64,
    ) -> anyhow::Result<()> {
        let receipt_record = readnode_primitives::ReceiptRecord {
            receipt_id: *receipt_id,
            parent_transaction_hash: *parent_tx_hash,
            receiver_id: receiver_id.clone(),
            block_height: block.height,
            block_hash: block.hash,
            shard_id: shard_id.into(),
        };
        let outcome_record = readnode_primitives::OutcomeRecord {
            outcome_id: *outcome_id,
            parent_transaction_hash: *parent_tx_hash,
            receiver_id: receiver_id.clone(),
            block_height: block.height,
            block_hash: block.hash,
            shard_id: shard_id.into(),
        };
        let mut outcomes_and_receipts_to_save_lock =
            self.outcomes_and_receipts_to_save.write().await;
        outcomes_and_receipts_to_save_lock
            .receipts
            .insert(receipt_id.to_string(), receipt_record.clone());
        outcomes_and_receipts_to_save_lock
            .outcomes
            .insert(outcome_id.to_string(), outcome_record.clone());
        drop(outcomes_and_receipts_to_save_lock);
        Ok(())
    }

    pub(crate) async fn return_outcomes_to_save(
        &self,
        outcomes: Vec<readnode_primitives::OutcomeRecord>,
    ) {
        let outcomes = outcomes
            .into_iter()
            .map(|outcome| (outcome.outcome_id.to_string(), outcome))
            .collect::<std::collections::HashMap<_, _>>();

        self.outcomes_and_receipts_to_save
            .write()
            .await
            .outcomes
            .extend(outcomes);
    }

    pub(crate) async fn return_receipts_to_save(
        &self,
        receipts: Vec<readnode_primitives::ReceiptRecord>,
    ) {
        let receipts = receipts
            .into_iter()
            .map(|receipt| (receipt.receipt_id.to_string(), receipt))
            .collect::<std::collections::HashMap<_, _>>();

        self.outcomes_and_receipts_to_save
            .write()
            .await
            .receipts
            .extend(receipts);
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    pub(crate) async fn push_outcome_and_receipt(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
        indexer_execution_outcome_with_receipt: near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
    ) -> anyhow::Result<()> {
        self.push_outcome_and_receipt_to_storage(
            transaction_key.clone(),
            indexer_execution_outcome_with_receipt.clone(),
        )
        .await?;
        self.push_outcome_and_receipt_to_cache(
            transaction_key,
            indexer_execution_outcome_with_receipt,
        )
        .await?;
        Ok(())
    }
}
