use near_indexer_primitives::CryptoHash;

pub const STORAGE: &str = "storage_tx";

#[derive(Clone)]
struct RedisStorage {
    client: redis::aio::ConnectionManager,
}

impl RedisStorage {
    async fn new(redis_url: String) -> anyhow::Result<Self> {
        let redis_client = redis::Client::open(redis_url)?
            .get_connection_manager()
            .await?;
        Ok(Self {
            client: redis_client,
        })
    }

    async fn del(
        &self,
        transaction_key: readnode_primitives::TransactionKey,
    ) -> anyhow::Result<()> {
        redis::cmd("DEL")
            .arg(transaction_key.to_string())
            .query_async::<redis::aio::ConnectionManager, String>(&mut self.client.clone())
            .await?;
        Ok(())
    }

    // async fn cache_add_transaction(&self, transaction_details: readnode_primitives::CollectingTransactionDetails) -> anyhow::Result<()> {
    //     redis::cmd("HSET")
    //         .arg(transaction_details.transaction_key().to_string())
    //         .arg("tx_details")
    //         .arg(borsh::to_vec(&transaction_details)?)
    //         .query_async::<redis::aio::ConnectionManager, String>(&mut self.client.clone()).await?;
    //     Ok(())
    // }
    //
    // async fn cache_add_receipt(&self, transaction_key: readnode_primitives::TransactionKey, indexer_execution_outcome_with_receipt: near_indexer_primitives::IndexerExecutionOutcomeWithReceipt) -> anyhow::Result<()> {
    //     redis::cmd("HSET")
    //         .arg(transaction_key.to_string())
    //         .arg(indexer_execution_outcome_with_receipt.receipt.receipt_id.to_string())
    //         .arg("indexer_execution_outcome_with_receipt_json")
    //         .query_async::<redis::aio::ConnectionManager, String>(&mut self.client.clone()).await?;
    //     Ok(())
    // }
    //
}

pub struct CacheStorageWithRedis {
    redis_storage: RedisStorage,
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
}

impl CacheStorageWithRedis {
    /// Init storage without restore transactions with receipts after interruption
    pub(crate) async fn init_storage(redis_url: String) -> Self {
        let redis_storage = RedisStorage::new(redis_url)
            .await
            .expect("Failed connecting to redis");
        Self {
            redis_storage,
            transactions: futures_locks::RwLock::new(std::collections::HashMap::new()),
            receipts_counters: futures_locks::RwLock::new(std::collections::HashMap::new()),
            receipts_watching_list: futures_locks::RwLock::new(std::collections::HashMap::new()),
            transactions_to_save: futures_locks::RwLock::new(std::collections::HashMap::new()),
        }
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    pub(crate) async fn push_receipt_to_watching_list(
        &self,
        receipt_id: &CryptoHash,
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
            .insert(receipt_id.to_string(), transaction_key.clone());
        tracing::debug!(
            target: crate::storage::STORAGE,
            "+R {} - {}",
            receipt_id,
            transaction_key.transaction_hash
        );
        Ok(())
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn remove_receipt_from_watching_list(&self, receipt_id: &str) -> anyhow::Result<()> {
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
                target: crate::storage::STORAGE,
                "-R {} - {}",
                receipt_id,
                transaction_key.transaction_hash
            );
        };
        Ok(())
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn receipts_transaction_hash_count(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
    ) -> anyhow::Result<u64> {
        self.receipts_counters
            .read()
            .await
            .get(transaction_key)
            .copied()
            .ok_or(anyhow::anyhow!(
                "No such transaction hash `receipts_transaction_hash_count` {}",
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
        let transaction_hash = transaction_details.transaction.hash.clone().to_string();
        self.update_tx(transaction_details).await?;
        tracing::debug!(target: crate::storage::STORAGE, "+T {}", transaction_hash,);
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
    async fn move_tx_to_save(
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
            target: crate::storage::STORAGE,
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
        self.redis_storage.del(transaction_key).await?;
        Ok(())
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
    pub(crate) async fn push_outcome_and_receipt(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
        indexer_execution_outcome_with_receipt: near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
    ) -> anyhow::Result<()> {
        let mut transaction_details = self.get_tx(transaction_key).await?;
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
            .receipts_transaction_hash_count(transaction_key)
            .await?;
        if transaction_receipts_watching_count == 0 {
            self.move_tx_to_save(transaction_details.clone()).await?;
        } else {
            self.update_tx(transaction_details.clone()).await?;
        }
        Ok(())
    }
}
