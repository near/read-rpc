use crate::base_storage::TxCollectingStorage;

const STORAGE: &str = "storage_tx";

pub struct RadisStorage {
    redis_connection_manager: redis::aio::ConnectionManager,
}

impl RadisStorage {
    async fn get_redis_client(redis_connection_str: &str) -> redis::Client {
        redis::Client::open(redis_connection_str).expect("Can't create redis client")
    }

    pub(crate) async fn new(redis_connection_str: &str) -> Self {
        Self {
            redis_connection_manager: Self::get_redis_client(redis_connection_str)
                .await
                .get_tokio_connection_manager()
                .await
                .expect("Can't connect to redis"),
        }
    }
}

#[async_trait::async_trait]
impl TxCollectingStorage for RadisStorage {
    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn push_receipt_to_watching_list(
        &mut self,
        receipt_id: String,
        transaction_hash: String,
    ) -> anyhow::Result<()> {
        let receipts_counter: u64 = match redis::cmd("HGET")
            .arg(transaction_hash.to_string())
            .arg("receipts_counter")
            .query_async(&mut self.redis_connection_manager.clone())
            .await?
        {
            Some(mut counter) => {
                counter += 1;
                counter
            }
            None => 1,
        };
        redis::cmd("HSET")
            .arg(transaction_hash.to_string())
            .arg("receipts_counter")
            .arg(receipts_counter)
            .query_async(&mut self.redis_connection_manager.clone())
            .await?;
        redis::cmd("SET")
            .arg(receipt_id.clone())
            .arg(transaction_hash.clone())
            .query_async(&mut self.redis_connection_manager.clone())
            .await?;
        tracing::debug!(target: STORAGE, "+R {} - {}", receipt_id, transaction_hash);
        Ok(())
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn remove_receipt_from_watching_list(
        &mut self,
        receipt_id: &str,
    ) -> anyhow::Result<Option<String>> {
        let transaction_hash: String = match redis::cmd("GET")
            .arg(receipt_id.to_string())
            .query_async(&mut self.redis_connection_manager.clone())
            .await?
        {
            Some(transaction_hash) => {
                redis::cmd("DEL")
                    .arg(receipt_id)
                    .query_async(&mut self.redis_connection_manager.clone())
                    .await?;
                transaction_hash
            }
            None => return Ok(None),
        };
        let receipts_counter: Option<u64> = redis::cmd("HGET")
            .arg(transaction_hash.to_string())
            .arg("receipts_counter")
            .query_async(&mut self.redis_connection_manager.clone())
            .await?;
        match receipts_counter {
            Some(mut receipts_counter) => {
                receipts_counter -= 1;
                redis::cmd("HSET")
                    .arg(transaction_hash.to_string())
                    .arg("receipts_counter")
                    .arg(receipts_counter)
                    .query_async(&mut self.redis_connection_manager.clone())
                    .await?;
                tracing::debug!(target: STORAGE, "-R {} - {}", receipt_id, transaction_hash);
                Ok(Some(transaction_hash))
            }
            None => Ok(None),
        }
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn receipts_transaction_hash_count(&self, transaction_hash: &str) -> anyhow::Result<u64> {
        match redis::cmd("HGET")
            .arg(transaction_hash.to_string())
            .arg("receipts_counter")
            .query_async(&mut self.redis_connection_manager.clone())
            .await?
        {
            Some(counter) => Ok(counter),
            None => Err(anyhow::anyhow!(
                "No such transaction hash {}",
                transaction_hash
            )),
        }
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn set_tx(
        &mut self,
        transaction_details: readnode_primitives::CollectingTransactionDetails,
    ) -> anyhow::Result<()> {
        let transaction_hash = transaction_details.transaction.hash.clone().to_string();
        redis::cmd("HSET").arg(transaction_hash).arg("transaction_details").arg(
            serde_json::to_string(&transaction_details)?
        ).query_async(&mut self.redis_connection_manager.clone()).await?;
        tracing::debug!(target: STORAGE, "+T {}", transaction_hash,);
        Ok(())
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn get_tx(
        &self,
        transaction_hash: &str,
    ) -> Option<readnode_primitives::CollectingTransactionDetails> {
        self.transactions.get(transaction_hash).cloned()
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn push_tx_to_save(
        &mut self,
        transaction_details: readnode_primitives::CollectingTransactionDetails,
    ) -> anyhow::Result<()> {
        self.transactions_to_save.insert(
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
        Ok(self.receipts_watching_list.get(receipt_id).cloned())
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn transactions_to_save(
        &mut self,
    ) -> anyhow::Result<Vec<readnode_primitives::CollectingTransactionDetails>> {
        let mut transactions = vec![];
        let mut transactions_hashes = vec![];
        for (transaction_hash, transaction_details) in self.transactions_to_save.iter() {
            transactions.push(transaction_details.clone());
            transactions_hashes.push(transaction_hash.clone());
        }
        for transaction_hash in transactions_hashes.iter() {
            self.transactions_to_save.remove(transaction_hash);
        }
        Ok(transactions)
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn push_outcome_and_receipt(
        &mut self,
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
                self.transactions.remove(transaction_hash);
                self.receipts_counters.remove(transaction_hash);
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
