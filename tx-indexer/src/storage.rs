use std::collections::HashMap;

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
        // Use redis database 3 for collecting transactions cache
        redis::cmd("SELECT")
            .arg(3)
            .query_async(&mut redis_client.clone())
            .await?;
        Ok(Self {
            client: redis_client,
        })
    }

    async fn del_tx(
        &self,
        transaction_key: readnode_primitives::TransactionKey,
    ) -> anyhow::Result<()> {
        redis::cmd("DEL")
            .arg(transaction_key.to_string())
            .query_async::<redis::aio::ConnectionManager, String>(&mut self.client.clone())
            .await?;
        Ok(())
    }

    async fn set_tx(
        &self,
        transaction_details: readnode_primitives::CollectingTransactionDetails,
    ) -> anyhow::Result<()> {
        redis::cmd("SET")
            .arg(transaction_details.transaction_key().to_string())
            .arg(borsh::to_vec(&transaction_details)?)
            .query_async::<redis::aio::ConnectionManager, String>(&mut self.client.clone())
            .await?;
        Ok(())
    }

    async fn get_tx(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
    ) -> anyhow::Result<readnode_primitives::CollectingTransactionDetails> {
        let result = redis::cmd("GET")
            .arg(transaction_key.to_string())
            .query_async::<redis::aio::ConnectionManager, Vec<u8>>(&mut self.client.clone())
            .await?;
        let mut tx =
            borsh::from_slice::<readnode_primitives::CollectingTransactionDetails>(&result)?;
        for (_, outcome) in self.get_outcomes(&tx.transaction_key()).await? {
            tx.execution_outcomes.push(borsh::from_slice::<
                near_indexer_primitives::views::ExecutionOutcomeWithIdView,
            >(&outcome)?)
        }
        for (_, receipt) in self.get_receipts(&tx.transaction_key()).await? {
            tx.receipts.push(borsh::from_slice::<
                near_indexer_primitives::views::ReceiptView,
            >(&receipt)?)
        }
        Ok(tx)
    }

    async fn add_outcomes_and_receipts(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
        indexer_execution_outcome_with_receipt: near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
    ) -> anyhow::Result<()> {
        redis::cmd("HSET")
            .arg(format!("receipts_{}", transaction_key.to_string()))
            .arg(
                indexer_execution_outcome_with_receipt
                    .receipt
                    .receipt_id
                    .to_string(),
            )
            .arg(borsh::to_vec(
                &indexer_execution_outcome_with_receipt.receipt,
            )?)
            .query_async::<redis::aio::ConnectionManager, Vec<u8>>(&mut self.client.clone())
            .await?;

        redis::cmd("HSET")
            .arg(format!("outcomes_{}", transaction_key.to_string()))
            .arg(
                indexer_execution_outcome_with_receipt
                    .execution_outcome
                    .id
                    .to_string(),
            )
            .arg(borsh::to_vec(
                &indexer_execution_outcome_with_receipt.execution_outcome,
            )?)
            .query_async::<redis::aio::ConnectionManager, Vec<u8>>(&mut self.client.clone())
            .await?;

        Ok(())
    }

    async fn get_outcomes(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
    ) -> anyhow::Result<HashMap<String, Vec<u8>>> {
        Ok(redis::cmd("HGETALL")
            .arg(format!("outcomes_{}", transaction_key.to_string()))
            .query_async::<redis::aio::ConnectionManager, HashMap<String, Vec<u8>>>(
                &mut self.client.clone(),
            )
            .await?)
    }

    async fn get_receipts(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
    ) -> anyhow::Result<HashMap<String, Vec<u8>>> {
        Ok(redis::cmd("HGETALL")
            .arg(format!("receipts_{}", transaction_key.to_string()))
            .query_async::<redis::aio::ConnectionManager, HashMap<String, Vec<u8>>>(
                &mut self.client.clone(),
            )
            .await?)
    }

    async fn del_outcomes_and_receipts(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
    ) -> anyhow::Result<()> {
        redis::cmd("DEL")
            .arg(format!("receipts_{}", transaction_key.to_string()))
            .query_async::<redis::aio::ConnectionManager, u64>(&mut self.client.clone())
            .await?;
        redis::cmd("DEL")
            .arg(format!("outcomes_{}", transaction_key.to_string()))
            .query_async::<redis::aio::ConnectionManager, u64>(&mut self.client.clone())
            .await?;
        Ok(())
    }

    async fn add_transactions_to_save(
        &self,
        transaction_key: readnode_primitives::TransactionKey,
    ) -> anyhow::Result<()> {
        redis::cmd("HSET")
            .arg("transactions_to_save")
            .arg(transaction_key.to_string())
            .arg(transaction_key.to_string())
            .query_async::<redis::aio::ConnectionManager, String>(&mut self.client.clone())
            .await?;
        Ok(())
    }

    async fn get_transactions_to_save(
        &self,
    ) -> anyhow::Result<Vec<readnode_primitives::TransactionKey>> {
        let result = redis::cmd("HGETALL")
            .arg("transactions_to_save")
            .query_async::<redis::aio::ConnectionManager, Vec<String>>(&mut self.client.clone())
            .await?;

        Ok(result
            .into_iter()
            .map(|key| readnode_primitives::TransactionKey::from(key))
            .collect())
    }

    async fn del_transactions_to_save(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
    ) -> anyhow::Result<()> {
        redis::cmd("HDEL")
            .arg("transactions_to_save")
            .arg(transaction_key.to_string())
            .query_async::<redis::aio::ConnectionManager, Vec<String>>(&mut self.client.clone())
            .await?;

        Ok(())
    }

    async fn add_receipts_watching_list(
        &self,
        receipt_id: near_indexer_primitives::CryptoHash,
        transaction_key: readnode_primitives::TransactionKey,
    ) -> anyhow::Result<()> {
        redis::cmd("HSET")
            .arg("receipts_watching_list")
            .arg(receipt_id.to_string())
            .arg(transaction_key.to_string())
            .query_async::<redis::aio::ConnectionManager, String>(&mut self.client.clone())
            .await?;
        Ok(())
    }

    async fn get_receipts_watching_list(
        &self,
        receipt_id: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<readnode_primitives::TransactionKey> {
        let result = redis::cmd("HGET")
            .arg("receipts_watching_list")
            .arg(receipt_id.to_string())
            .query_async::<redis::aio::ConnectionManager, String>(&mut self.client.clone())
            .await?;
        Ok(readnode_primitives::TransactionKey::from(result))
    }

    async fn remove_receipts_watching_list(
        &self,
        receipt_id: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<readnode_primitives::TransactionKey> {
        let result = redis::cmd("HDEL")
            .arg("receipts_watching_list")
            .arg(receipt_id.to_string())
            .query_async::<redis::aio::ConnectionManager, String>(&mut self.client.clone())
            .await?;
        Ok(readnode_primitives::TransactionKey::from(result))
    }

    async fn receipts_counters_inc(
        &self,
        transaction_key: readnode_primitives::TransactionKey,
    ) -> anyhow::Result<u64> {
        Ok(redis::cmd("HINCRBY")
            .arg("receipts_counters")
            .arg(transaction_key.to_string())
            .arg(1)
            .query_async::<redis::aio::ConnectionManager, u64>(&mut self.client.clone())
            .await?)
    }

    async fn receipts_counters_dec(
        &self,
        transaction_key: readnode_primitives::TransactionKey,
    ) -> anyhow::Result<u64> {
        Ok(redis::cmd("HINCRBY")
            .arg("receipts_counters")
            .arg(transaction_key.to_string())
            .arg(-1)
            .query_async::<redis::aio::ConnectionManager, u64>(&mut self.client.clone())
            .await?)
    }

    async fn receipts_counter_get(
        &self,
        transaction_key: readnode_primitives::TransactionKey,
    ) -> anyhow::Result<u64> {
        Ok(redis::cmd("HGET")
            .arg("receipts_counters")
            .arg(transaction_key.to_string())
            .query_async::<redis::aio::ConnectionManager, u64>(&mut self.client.clone())
            .await?)
    }

    async fn receipts_counter_del(
        &self,
        transaction_key: readnode_primitives::TransactionKey,
    ) -> anyhow::Result<u64> {
        Ok(redis::cmd("HDEL")
            .arg("receipts_counters")
            .arg(transaction_key.to_string())
            .query_async::<redis::aio::ConnectionManager, u64>(&mut self.client.clone())
            .await?)
    }
}

pub struct CacheStorageWithRedis {
    redis_storage: RedisStorage,
}

impl CacheStorageWithRedis {
    /// Init storage without restore transactions with receipts after interruption
    pub(crate) async fn init_storage(redis_url: String) -> Self {
        let redis_storage = RedisStorage::new(redis_url)
            .await
            .expect("Failed connecting to redis");
        Self { redis_storage }
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    pub(crate) async fn push_receipt_to_watching_list(
        &self,
        receipt_id: &near_indexer_primitives::CryptoHash,
        transaction_key: readnode_primitives::TransactionKey,
    ) -> anyhow::Result<()> {
        crate::metrics::RECEIPTS_IN_MEMORY_CACHE.inc();
        self.redis_storage
            .receipts_counters_inc(transaction_key.clone())
            .await?;
        self.redis_storage
            .add_receipts_watching_list(receipt_id.clone(), transaction_key.clone())
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
    async fn remove_receipt_from_watching_list(
        &self,
        receipt_id: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()> {
        let transaction_key = self
            .redis_storage
            .remove_receipts_watching_list(receipt_id)
            .await?;
        self.redis_storage
            .receipts_counters_dec(transaction_key.clone())
            .await?;
        crate::metrics::RECEIPTS_IN_MEMORY_CACHE.dec();
        tracing::debug!(
            target: STORAGE,
            "-R {} - {}",
            receipt_id,
            transaction_key.transaction_hash
        );

        Ok(())
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn receipts_transaction_hash_count(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
    ) -> anyhow::Result<u64> {
        self.redis_storage
            .receipts_counter_get(transaction_key.clone())
            .await
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    pub(crate) async fn set_tx(
        &self,
        transaction_details: readnode_primitives::CollectingTransactionDetails,
    ) -> anyhow::Result<()> {
        let transaction_hash = transaction_details.transaction.hash.clone().to_string();
        self.redis_storage.set_tx(transaction_details).await?;
        tracing::debug!(target: STORAGE, "+T {}", transaction_hash,);
        Ok(())
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn get_tx(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
    ) -> anyhow::Result<readnode_primitives::CollectingTransactionDetails> {
        self.redis_storage.get_tx(transaction_key).await
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn add_tx_to_save(
        &self,
        transaction_key: readnode_primitives::TransactionKey,
    ) -> anyhow::Result<()> {
        self.redis_storage
            .add_transactions_to_save(transaction_key.clone())
            .await?;
        self.redis_storage
            .receipts_counter_del(transaction_key.clone())
            .await?;
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
        self.redis_storage.del_tx(transaction_key.clone()).await?;
        self.redis_storage
            .del_outcomes_and_receipts(&transaction_key)
            .await?;
        self.redis_storage
            .del_transactions_to_save(&transaction_key)
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    pub(crate) async fn get_transaction_hash_by_receipt_id(
        &self,
        receipt_id: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<readnode_primitives::TransactionKey> {
        self.redis_storage
            .get_receipts_watching_list(receipt_id)
            .await
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    pub(crate) async fn transactions_to_save(
        &self,
    ) -> anyhow::Result<Vec<readnode_primitives::CollectingTransactionDetails>> {
        let mut transactions = vec![];
        let transactions_hashes = self.redis_storage.get_transactions_to_save().await?;
        for transaction_key in transactions_hashes {
            let tx = self.redis_storage.get_tx(&transaction_key).await?;
            transactions.push(tx);
        }
        Ok(transactions)
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    pub(crate) async fn push_outcome_and_receipt(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
        indexer_execution_outcome_with_receipt: near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
    ) -> anyhow::Result<()> {
        self.remove_receipt_from_watching_list(
            indexer_execution_outcome_with_receipt.receipt.receipt_id,
        )
        .await?;
        self.redis_storage
            .add_outcomes_and_receipts(transaction_key, indexer_execution_outcome_with_receipt)
            .await?;
        let transaction_receipts_watching_count = self
            .receipts_transaction_hash_count(transaction_key)
            .await?;
        if transaction_receipts_watching_count == 0 {
            self.add_tx_to_save(transaction_key.clone()).await?;
        }
        Ok(())
    }
}
