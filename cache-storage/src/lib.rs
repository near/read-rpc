#[derive(Clone)]
struct RedisCacheStorage {
    client: redis::aio::ConnectionManager,
}

impl RedisCacheStorage {
    // Create a new instance of the `RedisCacheStorage` struct.
    // param `redis_url` - Redis connection URL.
    // param `database_number` - Number of the database to use.
    // We use database 1 for handling the blocks by finality cache.
    // We use database 3 for collecting transactions cache.
    // Different databases are used to avoid key conflicts.
    async fn new(redis_url: String, database_number: usize) -> anyhow::Result<Self> {
        let redis_client = redis::Client::open(redis_url)?
            .get_connection_manager()
            .await?;
        redis::cmd("SELECT")
            .arg(database_number)
            .query_async(&mut redis_client.clone())
            .await?;
        Ok(Self {
            client: redis_client,
        })
    }

    async fn get_keys(&self, key_prefix: String) -> anyhow::Result<Vec<String>> {
        Ok(redis::cmd("KEYS")
            .arg(format!("{key_prefix}*"))
            .query_async(&mut self.client.clone())
            .await?)
    }

    async fn get<V: redis::FromRedisValue + std::fmt::Debug>(
        &self,
        key: impl redis::ToRedisArgs + std::fmt::Debug,
    ) -> anyhow::Result<V> {
        let value: V = redis::cmd("GET")
            .arg(&key)
            .query_async(&mut self.client.clone())
            .await?;
        Ok(value)
    }

    async fn set(
        &self,
        key: impl redis::ToRedisArgs + std::fmt::Debug,
        value: impl redis::ToRedisArgs + std::fmt::Debug,
    ) -> anyhow::Result<()> {
        redis::cmd("SET")
            .arg(&key)
            .arg(&value)
            .query_async(&mut self.client.clone())
            .await?;
        Ok(())
    }

    pub async fn del(&self, key: impl redis::ToRedisArgs + std::fmt::Debug) -> anyhow::Result<()> {
        redis::cmd("DEL")
            .arg(&key)
            .query_async(&mut self.client.clone())
            .await?;
        Ok(())
    }

    async fn rpush(
        &self,
        key: impl redis::ToRedisArgs + std::fmt::Debug,
        value: impl redis::ToRedisArgs + std::fmt::Debug,
    ) -> anyhow::Result<()> {
        redis::cmd("RPUSH")
            .arg(&key)
            .arg(&value)
            .query_async(&mut self.client.clone())
            .await?;
        Ok(())
    }

    async fn lrange<V: redis::FromRedisValue + std::fmt::Debug>(
        &self,
        key: impl redis::ToRedisArgs + std::fmt::Debug,
    ) -> anyhow::Result<V> {
        let value: V = redis::cmd("LRANGE")
            .arg(&key)
            .arg(0)
            .arg(-1)
            .query_async(&mut self.client.clone())
            .await?;
        Ok(value)
    }
}

#[derive(Clone)]
pub struct BlocksByFinalityCache {
    cache_storage: RedisCacheStorage,
}

/// Sets the keys in Redis shared between the ReadRPC components about the most recent
/// blocks based on finalities (final or optimistic).
/// `final_height` of `optimistic_height` depending on `block_type` passed.
/// Additionally, sets the JSON serialized `StreamerMessage` into keys `final` or `optimistic`
/// accordingly.
impl BlocksByFinalityCache {
    // Use redis database 0(default for redis) for handling the blocks by finality cache.
    pub async fn new(redis_url: String) -> anyhow::Result<Self> {
        Ok(Self {
            cache_storage: RedisCacheStorage::new(redis_url, 0).await?,
        })
    }

    pub async fn update_block_by_finality(
        &self,
        finality: near_indexer_primitives::near_primitives::types::Finality,
        streamer_message: &near_indexer_primitives::StreamerMessage,
    ) -> anyhow::Result<()> {
        let block_height = streamer_message.block.header.height;
        let block_type = serde_json::to_string(&finality)?;

        let last_height = self
            .cache_storage
            .get(format!("{}_height", block_type))
            .await
            .unwrap_or(0);

        // If the block height is greater than the last height, update the block streamer message
        // if we have a few indexers running, we need to make sure that we are not updating the same block
        // or block which is already processed or block less than the last processed block
        if block_height > last_height {
            let json_streamer_message = serde_json::to_string(streamer_message)?;
            // Update the last block height
            // Create a clone of the redis client and redis cmd to avoid borrowing issues
            let update_height_feature = self
                .cache_storage
                .set(format!("{}_height", block_type), block_height);

            // Update the block streamer message
            // Create a clone of the redis client and redis cmd to avoid borrowing issues
            let update_stream_msg_feature =
                self.cache_storage.set(block_type, json_streamer_message);

            // Wait for both futures to complete
            futures::try_join!(update_height_feature, update_stream_msg_feature)?;
        };
        Ok(())
    }

    pub async fn get_block_by_finality(
        &self,
        finality: near_indexer_primitives::near_primitives::types::Finality,
    ) -> anyhow::Result<near_indexer_primitives::StreamerMessage> {
        let block_type = serde_json::to_string(&finality)?;
        let resp: String = self.cache_storage.get(block_type).await?;
        Ok(serde_json::from_str(&resp)?)
    }
}

#[derive(Clone)]
pub struct TxIndexerCache {
    cache_storage: RedisCacheStorage,
}

impl TxIndexerCache {
    // Use redis database 3 for collecting transactions cache
    pub async fn new(redis_url: String) -> anyhow::Result<Self> {
        Ok(Self {
            cache_storage: RedisCacheStorage::new(redis_url, 3).await?,
        })
    }

    pub async fn get_tx_by_tx_hash(
        &self,
        tx_hash: &near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<readnode_primitives::TransactionDetails> {
        let tx_key = match self
            .cache_storage
            .get_keys(format!("transaction_{}", tx_hash))
            .await?
            .first()
        {
            Some(value) => {
                readnode_primitives::TransactionKey::from(value.replace("transaction_", ""))
            }
            None => anyhow::bail!("Key does not exists"),
        };
        let tx_details = self.get_tx_full(&tx_key).await?;
        Ok(tx_details.into())
    }

    pub async fn get_txs_in_process(
        &self,
    ) -> anyhow::Result<Vec<readnode_primitives::TransactionKey>> {
        Ok(self
            .cache_storage
            .get_keys("transaction_".to_string())
            .await?
            .into_iter()
            .map(|key| readnode_primitives::TransactionKey::from(key.replace("transaction_", "")))
            .collect())
    }

    pub async fn get_tx(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
    ) -> anyhow::Result<readnode_primitives::CollectingTransactionDetails> {
        let result: Vec<u8> = self
            .cache_storage
            .get(format!("transaction_{}", transaction_key))
            .await?;
        Ok(borsh::from_slice::<
            readnode_primitives::CollectingTransactionDetails,
        >(&result)?)
    }

    pub async fn get_tx_outcomes(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
    ) -> anyhow::Result<Vec<readnode_primitives::ExecutionOutcomeWithReceipt>> {
        Ok(self
            .cache_storage
            .lrange::<Vec<Vec<u8>>>(format!("outcomes_{}", transaction_key))
            .await?
            .iter()
            .map(|outcome| {
                borsh::from_slice::<readnode_primitives::ExecutionOutcomeWithReceipt>(outcome)
                    .expect("Failed to deserialize ExecutionOutcome")
            })
            .collect())
    }

    pub async fn get_tx_full(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
    ) -> anyhow::Result<readnode_primitives::CollectingTransactionDetails> {
        let mut tx = self.get_tx(transaction_key).await?;
        for outcome in self.get_tx_outcomes(transaction_key).await? {
            tx.execution_outcomes.push(outcome.execution_outcome);
            tx.receipts.push(outcome.receipt);
        }
        Ok(tx)
    }

    pub async fn set_tx(
        &self,
        transaction_details: readnode_primitives::CollectingTransactionDetails,
    ) -> anyhow::Result<()> {
        self.cache_storage
            .set(
                format!("transaction_{}", transaction_details.transaction_key()),
                borsh::to_vec(&transaction_details)?,
            )
            .await
    }

    pub async fn del_tx(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
    ) -> anyhow::Result<()> {
        let del_tx_future = self
            .cache_storage
            .del(format!("transaction_{}", transaction_key));
        let del_tx_outcomes_future = self
            .cache_storage
            .del(format!("outcomes_{}", transaction_key));
        futures::try_join!(del_tx_future, del_tx_outcomes_future,)?;
        Ok(())
    }

    pub async fn set_outcomes_and_receipts(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
        indexer_execution_outcome_with_receipt: near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
    ) -> anyhow::Result<()> {
        // We should not store outcomes for not indexed transactions
        if self.get_tx(transaction_key).await.is_ok() {
            let execution_outcome_with_receipt = readnode_primitives::ExecutionOutcomeWithReceipt {
                execution_outcome: indexer_execution_outcome_with_receipt
                    .execution_outcome
                    .clone(),
                receipt: indexer_execution_outcome_with_receipt.receipt,
            };
            self.cache_storage
                .rpush(
                    format!("outcomes_{}", transaction_key),
                    borsh::to_vec(&execution_outcome_with_receipt)?,
                )
                .await?
        };
        Ok(())
    }
}
