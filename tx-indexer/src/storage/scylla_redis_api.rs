use crate::storage::base::TxCollectingStorage;
use borsh::{BorshDeserialize, BorshSerialize};

const STORAGE: &str = "storage_tx";

pub struct ScyllaDBRedisAPIStorage {
    redis_connection_manager: redis::aio::ConnectionManager,
}

impl ScyllaDBRedisAPIStorage {
    async fn get_redis_client(redis_connection_str: &str) -> redis::Client {
        redis::Client::open(redis_connection_str).expect("Can't create redis client")
    }

    #[allow(unused)]
    pub(crate) async fn new(redis_connection_str: &str) -> Self {
        Self {
            redis_connection_manager: Self::get_redis_client(redis_connection_str)
                .await
                .get_tokio_connection_manager()
                .await
                .expect("Can't connect to redis"),
        }
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn _get_tx(
        &self,
        transaction_hash: &str,
    ) -> anyhow::Result<Option<readnode_primitives::CollectingTransactionDetails>> {
        let redis_data: Option<String> = redis::cmd("HGET")
            .arg(transaction_hash)
            .arg("transaction_details")
            .query_async(&mut self.redis_connection_manager.clone())
            .await?;

        if let Some(transaction_details) = redis_data {
            Ok(Some(
                readnode_primitives::CollectingTransactionDetails::try_from_slice(&hex::decode(
                    transaction_details,
                )?)?,
            ))
        } else {
            Ok(None)
        }
    }
}

#[async_trait::async_trait]
impl TxCollectingStorage for ScyllaDBRedisAPIStorage {
    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn push_receipt_to_watching_list(
        &self,
        receipt_id: String,
        transaction_hash: String,
    ) -> anyhow::Result<()> {
        redis::cmd("SET")
            .arg(receipt_id.clone())
            .arg(transaction_hash.clone())
            .query_async(&mut self.redis_connection_manager.clone())
            .await?;
        let receipts_counter: u64 = (redis::cmd("HGET")
            .arg(transaction_hash.to_string())
            .arg("receipts_counter")
            .query_async(&mut self.redis_connection_manager.clone())
            .await)
            .unwrap_or(0);

        redis::cmd("HSET")
            .arg(transaction_hash.to_string())
            .arg("receipts_counter")
            .arg(receipts_counter + 1)
            .query_async(&mut self.redis_connection_manager.clone())
            .await?;
        tracing::warn!(
            target: STORAGE,
            "+R {} - {} RC {}",
            receipt_id,
            transaction_hash,
            receipts_counter + 1
        );
        Ok(())
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn remove_receipt_from_watching_list(
        &self,
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
                tracing::warn!(
                    target: STORAGE,
                    "-R {} - {} RC {}",
                    receipt_id,
                    transaction_hash,
                    receipts_counter
                );
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
            Some(counter) => {
                tracing::warn!(target: STORAGE, "T {} - RC {}", transaction_hash, counter);
                Ok(counter)
            }
            None => Err(anyhow::anyhow!(
                "No such transaction hash `receipts_transaction_hash_count` {}",
                transaction_hash
            )),
        }
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn set_tx(
        &self,
        transaction_details: readnode_primitives::CollectingTransactionDetails,
    ) -> anyhow::Result<()> {
        let transaction_hash = transaction_details.transaction.hash;
        redis::cmd("HSET")
            .arg(transaction_hash.to_string())
            .arg("transaction_details")
            .arg(hex::encode(&transaction_details.try_to_vec()?).to_string())
            .query_async(&mut self.redis_connection_manager.clone())
            .await?;
        tracing::warn!(target: STORAGE, "+T {}", transaction_hash);
        Ok(())
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn get_tx(
        &self,
        transaction_hash: &str,
    ) -> Option<readnode_primitives::CollectingTransactionDetails> {
        match self._get_tx(transaction_hash).await {
            Ok(result) => result,
            Err(_) => None,
        }
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn push_tx_to_save(
        &self,
        transaction_details: readnode_primitives::CollectingTransactionDetails,
    ) -> anyhow::Result<()> {
        let transaction_hash = transaction_details.transaction.hash.clone().to_string();
        redis::cmd("HSET")
            .arg("transactions_to_save")
            .arg(transaction_hash.to_string())
            .arg(transaction_hash.to_string())
            .query_async(&mut self.redis_connection_manager.clone())
            .await?;
        redis::cmd("HSET")
            .arg("transactions_to_save_with_details")
            .arg(transaction_hash)
            .arg(hex::encode(transaction_details.try_to_vec()?).to_string())
            .query_async(&mut self.redis_connection_manager.clone())
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn get_transaction_hash_by_receipt_id(
        &self,
        receipt_id: &str,
    ) -> anyhow::Result<Option<String>> {
        Ok(redis::cmd("GET")
            .arg(receipt_id.to_string())
            .query_async(&mut self.redis_connection_manager.clone())
            .await?)
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    async fn transactions_to_save(
        &self,
    ) -> anyhow::Result<Vec<readnode_primitives::CollectingTransactionDetails>> {
        let mut transactions = vec![];
        let transactions_to_save: Vec<String> = redis::cmd("HGETALL")
            .arg("transactions_to_save")
            .query_async(&mut self.redis_connection_manager.clone())
            .await?;
        for transaction_hash in transactions_to_save.iter() {
            let transaction_details: Option<String> = redis::cmd("HGET")
                .arg("transactions_to_save_with_details")
                .arg(transaction_hash)
                .query_async(&mut self.redis_connection_manager.clone())
                .await?;
            if let Some(transaction_details) = transaction_details {
                transactions.push(
                    readnode_primitives::CollectingTransactionDetails::try_from_slice(
                        &hex::decode(&transaction_details)?,
                    )?,
                )
            }
            redis::cmd("HDEL")
                .arg("transactions_to_save_with_details")
                .arg(transaction_hash)
                .query_async(&mut self.redis_connection_manager.clone())
                .await?;
            redis::cmd("HDEL")
                .arg("transactions_to_save")
                .arg(transaction_hash)
                .query_async(&mut self.redis_connection_manager.clone())
                .await?;
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
                redis::cmd("HDEL")
                    .arg(transaction_hash)
                    .arg("receipts_counter")
                    .arg("transaction_details")
                    .query_async(&mut self.redis_connection_manager.clone())
                    .await?;
                tracing::warn!(target: STORAGE, "-T {}", transaction_hash);
            } else {
                self.set_tx(transaction_details.clone()).await?;
            }
        } else {
            tracing::error!(
                target: STORAGE,
                "No such transaction hash `push_outcome_and_receipt` {}",
                transaction_hash
            );
        }
        Ok(())
    }
}
