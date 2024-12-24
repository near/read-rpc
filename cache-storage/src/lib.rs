use futures::FutureExt;
use near_indexer_primitives::near_primitives;

mod utils;

#[derive(Clone)]
struct RedisCacheStorage {
    client: redis::aio::ConnectionManager,
}

impl RedisCacheStorage {
    // Create a new instance of the `RedisCacheStorage` struct.
    // param `redis_url` - Redis connection URL.
    // param `database_number` - Number of the database to use.
    // We use database 0 for handling the blocks by finality cache.
    // We use database 2 for collecting transactions cache.
    // Different databases are used to avoid key conflicts.
    async fn new(redis_url: String, database_number: usize) -> anyhow::Result<Self> {
        let redis_client = redis::Client::open(redis_url)?
            .get_connection_manager()
            .await?;
        redis::cmd("SELECT")
            .arg(database_number)
            .query_async::<()>(&mut redis_client.clone())
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
            .query_async::<()>(&mut self.client.clone())
            .await?;
        Ok(())
    }

    pub async fn delete(
        &self,
        key: impl redis::ToRedisArgs + std::fmt::Debug,
    ) -> anyhow::Result<()> {
        redis::cmd("DEL")
            .arg(&key)
            .query_async::<()>(&mut self.client.clone())
            .await?;
        Ok(())
    }

    // Insert all the specified values at the tail of the list stored at key.
    // If key does not exist, it is created as empty list before performing the push operation.
    async fn insert_or_create(
        &self,
        key: impl redis::ToRedisArgs + std::fmt::Debug,
        value: impl redis::ToRedisArgs + std::fmt::Debug,
    ) -> anyhow::Result<()> {
        redis::cmd("RPUSH")
            .arg(&key)
            .arg(&value)
            .query_async::<()>(&mut self.client.clone())
            .await?;
        Ok(())
    }

    // Inserts specified values at the tail of the list stored at key, only if key already exists and holds a list.
    // In contrary to RPUSH, no operation will be performed when key does not yet exist.
    async fn insert_or_ignore(
        &self,
        key: impl redis::ToRedisArgs + std::fmt::Debug,
        value: impl redis::ToRedisArgs + std::fmt::Debug,
    ) -> anyhow::Result<()> {
        redis::cmd("RPUSHX")
            .arg(&key)
            .arg(&value)
            .query_async::<()>(&mut self.client.clone())
            .await?;
        Ok(())
    }

    // Returns the specified elements of the list stored at key.
    async fn list<V: redis::FromRedisValue + std::fmt::Debug>(
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
            futures::future::join_all([
                update_height_feature.boxed(),
                update_stream_msg_feature.boxed(),
            ])
            .await
            .into_iter()
            .collect::<anyhow::Result<()>>()?;
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

    pub async fn update_protocol_version(
        &self,
        protocol_version: near_primitives::types::ProtocolVersion,
    ) -> anyhow::Result<()> {
        self.cache_storage
            .set("protocol_version", protocol_version)
            .await
    }

    pub async fn get_protocol_version(
        &self,
    ) -> anyhow::Result<near_primitives::types::ProtocolVersion> {
        self.cache_storage.get("protocol_version").await
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
            cache_storage: RedisCacheStorage::new(redis_url, 2).await?,
        })
    }

    // Try to find the transaction by the transaction hash in the cache storage.
    // If the transaction is found by key prefix, return the transaction details by the first key.
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
        let tx_details = self.get_tx_with_outcomes(&tx_key).await?;
        Ok(tx_details.into())
    }

    // Help method to get all the transactions in process and restore after tx indexer interruption.
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

    // get the transaction details without outcomes by the transaction key.
    pub async fn get_tx(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
    ) -> anyhow::Result<readnode_primitives::CollectingTransactionDetails> {
        let result: Vec<u8> = self
            .cache_storage
            .get(format!("transaction_{}", transaction_key))
            .await?;
        utils::from_slice::<readnode_primitives::CollectingTransactionDetails>(&result)
    }

    // get the transaction outcomes by the transaction key.
    pub async fn get_tx_outcomes(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
    ) -> anyhow::Result<Vec<near_indexer_primitives::IndexerExecutionOutcomeWithReceipt>> {
        Ok(self
            .cache_storage
            .list::<Vec<Vec<u8>>>(format!("outcomes_{}", transaction_key))
            .await?
            .iter()
            .map(|outcome| {
                utils::from_slice::<near_indexer_primitives::IndexerExecutionOutcomeWithReceipt>(
                    outcome,
                )
            })
            .filter_map(|outcome| outcome.ok())
            .collect())
    }

    // Get the transaction details with outcomes by the transaction key.
    pub async fn get_tx_with_outcomes(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
    ) -> anyhow::Result<readnode_primitives::CollectingTransactionDetails> {
        let mut tx = self.get_tx(transaction_key).await?;
        for outcome in self.get_tx_outcomes(transaction_key).await? {
            // Skip the outcome that is already in the transaction
            if outcome.execution_outcome.id == tx.transaction_outcome.id {
                continue;
            };
            tx.execution_outcomes.push(outcome.execution_outcome);
            tx.receipts.push(outcome.receipt);
        }
        Ok(tx)
    }

    // Set the transaction details and outcomes by the transaction key.
    pub async fn set_tx(
        &self,
        transaction_details: readnode_primitives::CollectingTransactionDetails,
    ) -> anyhow::Result<()> {
        // Set the transaction details into the cache storage
        let set_tx_future = self.cache_storage.set(
            format!("transaction_{}", transaction_details.transaction_key()),
            utils::to_vec(&transaction_details)?,
        );
        // Set the transaction outcomes into the cache storage
        let set_outcomes_future = self.cache_storage.insert_or_create(
            format!("outcomes_{}", transaction_details.transaction_key()),
            utils::to_vec(&transaction_details.transaction_outcome)?,
        );
        futures::future::join_all([set_tx_future.boxed(), set_outcomes_future.boxed()])
            .await
            .into_iter()
            .collect::<anyhow::Result<_>>()
    }

    // Delete the transaction details and outcomes from cache storage by the transaction key.
    pub async fn del_tx(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
    ) -> anyhow::Result<()> {
        // Delete the transaction details
        let del_tx_future = self
            .cache_storage
            .delete(format!("transaction_{}", transaction_key));
        // Delete the transaction outcomes
        let del_tx_outcomes_future = self
            .cache_storage
            .delete(format!("outcomes_{}", transaction_key));

        futures::future::join_all([del_tx_future.boxed(), del_tx_outcomes_future.boxed()])
            .await
            .into_iter()
            .collect::<anyhow::Result<_>>()
    }

    // Add the transaction outcomes by the transaction key.
    // This inserts the outcomes into the list stored at key, only if key already exists and holds a list.
    // This is needed to avoid adding outcomes to the list if the transaction is already deleted.
    pub async fn set_outcomes_and_receipts(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
        indexer_execution_outcome_with_receipt: near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
    ) -> anyhow::Result<()> {
        self.cache_storage
            .insert_or_ignore(
                format!("outcomes_{}", transaction_key),
                utils::to_vec(&indexer_execution_outcome_with_receipt)?,
            )
            .await?;
        Ok(())
    }
}
