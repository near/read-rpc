#[derive(Clone)]
struct RedisCacheStorage {
    client: redis::aio::ConnectionManager,
}

impl RedisCacheStorage {
    async fn new(redis_url: String, database: usize) -> anyhow::Result<Self> {
        let redis_client = redis::Client::open(redis_url)?
            .get_connection_manager()
            .await?;
        // Use redis database 3 for collecting transactions cache
        redis::cmd("SELECT")
            .arg(database)
            .query_async(&mut redis_client.clone())
            .await?;
        Ok(Self {
            client: redis_client,
        })
    }

    //return first key by prefix
    async fn get_key(&self, key_prefix: String) -> anyhow::Result<String> {
        let values: Vec<String> = redis::cmd("KEYS")
            .arg(format!("{key_prefix}*"))
            .query_async(&mut self.client.clone())
            .await?;
        match values.first() {
            Some(value) => Ok(value.clone()),
            None => anyhow::bail!("Key does not exists"),
        }
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

    async fn hget<V: redis::FromRedisValue + std::fmt::Debug>(
        &self,
        key: impl redis::ToRedisArgs + std::fmt::Debug,
        field: impl redis::ToRedisArgs + std::fmt::Debug,
    ) -> anyhow::Result<V> {
        let value: V = redis::cmd("HGET")
            .arg(&key)
            .arg(&field)
            .query_async(&mut self.client.clone())
            .await?;
        Ok(value)
    }

    async fn hgetall<V: redis::FromRedisValue + std::fmt::Debug>(
        &self,
        key: impl redis::ToRedisArgs + std::fmt::Debug,
    ) -> anyhow::Result<V> {
        let value: V = redis::cmd("HGETALL")
            .arg(&key)
            .query_async(&mut self.client.clone())
            .await?;
        Ok(value)
    }

    async fn hset(
        &self,
        key: impl redis::ToRedisArgs + std::fmt::Debug,
        field: impl redis::ToRedisArgs + std::fmt::Debug,
        value: impl redis::ToRedisArgs + std::fmt::Debug,
    ) -> anyhow::Result<()> {
        redis::cmd("HSET")
            .arg(&key)
            .arg(&field)
            .arg(&value)
            .query_async(&mut self.client.clone())
            .await?;
        Ok(())
    }

    async fn hdel(
        &self,
        key: impl redis::ToRedisArgs + std::fmt::Debug,
        field: impl redis::ToRedisArgs + std::fmt::Debug,
    ) -> anyhow::Result<()> {
        redis::cmd("HDEL")
            .arg(&key)
            .arg(&field)
            .query_async(&mut self.client.clone())
            .await?;
        Ok(())
    }

    async fn hincrby<V: redis::FromRedisValue + std::fmt::Debug>(
        &self,
        key: impl redis::ToRedisArgs + std::fmt::Debug,
        field: impl redis::ToRedisArgs + std::fmt::Debug,
        value: i64, // 1 - to incr, -1 to decr
    ) -> anyhow::Result<V> {
        let value: V = redis::cmd("HINCRBY")
            .arg(&key)
            .arg(&field)
            .arg(value)
            .query_async(&mut self.client.clone())
            .await?;
        Ok(value)
    }

    async fn hincr<V: redis::FromRedisValue + std::fmt::Debug>(
        &self,
        key: impl redis::ToRedisArgs + std::fmt::Debug,
        field: impl redis::ToRedisArgs + std::fmt::Debug,
    ) -> anyhow::Result<V> {
        self.hincrby(key, field, 1).await
    }

    async fn hdecr<V: redis::FromRedisValue + std::fmt::Debug>(
        &self,
        key: impl redis::ToRedisArgs + std::fmt::Debug,
        field: impl redis::ToRedisArgs + std::fmt::Debug,
    ) -> anyhow::Result<V> {
        self.hincrby(key, field, -1).await
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
    pub async fn new(redis_url: String) -> anyhow::Result<Self> {
        Ok(Self {
            cache_storage: RedisCacheStorage::new(redis_url, 1).await?,
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
    pub async fn new(redis_url: String) -> anyhow::Result<Self> {
        Ok(Self {
            cache_storage: RedisCacheStorage::new(redis_url, 3).await?,
        })
    }

    pub async fn get_tx_by_tx_hash(
        &self,
        tx_hash: &near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<readnode_primitives::TransactionDetails> {
        let tx_key_string = self.cache_storage.get_key(tx_hash.to_string()).await?;
        let tx_key = readnode_primitives::TransactionKey::from(tx_key_string);
        let tx_details = self.get_tx(&tx_key).await?;
        Ok(tx_details.into())
    }

    pub async fn get_tx(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
    ) -> anyhow::Result<readnode_primitives::CollectingTransactionDetails> {
        let result: Vec<u8> = self.cache_storage.get(transaction_key.to_string()).await?;
        let mut tx =
            borsh::from_slice::<readnode_primitives::CollectingTransactionDetails>(&result)?;
        for (_, outcome) in self
            .cache_storage
            .hgetall::<std::collections::HashMap<String, Vec<u8>>>(format!(
                "outcomes_{}",
                transaction_key
            ))
            .await?
        {
            tx.execution_outcomes.push(borsh::from_slice::<
                near_indexer_primitives::views::ExecutionOutcomeWithIdView,
            >(&outcome)?)
        }
        for (_, receipt) in self
            .cache_storage
            .hgetall::<std::collections::HashMap<String, Vec<u8>>>(format!(
                "receipts_{}",
                transaction_key
            ))
            .await?
        {
            tx.receipts.push(borsh::from_slice::<
                near_indexer_primitives::views::ReceiptView,
            >(&receipt)?)
        }
        Ok(tx)
    }

    pub async fn set_tx(
        &self,
        transaction_details: readnode_primitives::CollectingTransactionDetails,
    ) -> anyhow::Result<()> {
        self.cache_storage
            .set(
                transaction_details.transaction_key().to_string(),
                borsh::to_vec(&transaction_details)?,
            )
            .await
    }

    pub async fn del_tx(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
    ) -> anyhow::Result<()> {
        self.cache_storage.del(transaction_key.to_string()).await?;
        self.cache_storage
            .del(format!("receipts_{}", transaction_key))
            .await?;
        self.cache_storage
            .del(format!("outcomes_{}", transaction_key))
            .await?;
        self.cache_storage
            .hdel("receipts_counters", transaction_key.to_string())
            .await?;
        self.cache_storage
            .hdel("transactions_to_save", transaction_key.to_string())
            .await?;
        Ok(())
    }

    pub async fn set_outcomes_and_receipts(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
        indexer_execution_outcome_with_receipt: near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
    ) -> anyhow::Result<()> {
        let receipt_future = self.cache_storage.hset(
            format!("receipts_{}", transaction_key),
            indexer_execution_outcome_with_receipt
                .receipt
                .receipt_id
                .to_string(),
            borsh::to_vec(&indexer_execution_outcome_with_receipt.receipt)?,
        );

        let outcome_future = self.cache_storage.hset(
            format!("outcomes_{}", transaction_key),
            indexer_execution_outcome_with_receipt
                .execution_outcome
                .id
                .to_string(),
            borsh::to_vec(&indexer_execution_outcome_with_receipt.execution_outcome)?,
        );
        futures::try_join!(receipt_future, outcome_future)?;
        Ok(())
    }

    pub async fn get_tx_key_by_receipt_id(
        &self,
        receipt_id: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<readnode_primitives::TransactionKey> {
        let tx_key_string: String = self
            .cache_storage
            .hget("receipts_watching_list", receipt_id.to_string())
            .await?;
        Ok(readnode_primitives::TransactionKey::from(tx_key_string))
    }

    pub async fn set_receipt_to_watching_list(
        &self,
        receipt_id: near_indexer_primitives::CryptoHash,
        transaction_key: readnode_primitives::TransactionKey,
    ) -> anyhow::Result<()> {
        self.cache_storage
            .hset(
                "receipts_watching_list",
                receipt_id.to_string(),
                transaction_key.to_string(),
            )
            .await?;
        self.cache_storage
            .hincr("receipts_counters", transaction_key.to_string())
            .await?;
        Ok(())
    }

    pub async fn del_receipt_from_watching_list(
        &self,
        receipt_id: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()> {
        let tx_key = self.get_tx_key_by_receipt_id(receipt_id).await?;
        self.cache_storage
            .hdel("receipts_watching_list", receipt_id.to_string())
            .await?;
        self.cache_storage
            .hdecr("receipts_counters", tx_key.to_string())
            .await?;
        Ok(())
    }

    pub async fn get_receipts_counter(
        &self,
        transaction_key: readnode_primitives::TransactionKey,
    ) -> anyhow::Result<u64> {
        self.cache_storage
            .hget("receipts_counters", transaction_key.to_string())
            .await
    }

    pub async fn set_tx_to_save(
        &self,
        transaction_key: readnode_primitives::TransactionKey,
    ) -> anyhow::Result<()> {
        self.cache_storage
            .hset(
                "transactions_to_save",
                transaction_key.to_string(),
                transaction_key.to_string(),
            )
            .await
    }

    pub async fn get_tx_to_save(
        &self,
    ) -> anyhow::Result<Vec<readnode_primitives::CollectingTransactionDetails>> {
        let mut transactions = vec![];
        for (tx_key_string, _) in self
            .cache_storage
            .hgetall::<std::collections::HashMap<String, String>>("transactions_to_save")
            .await?
        {
            let tx = self
                .get_tx(&readnode_primitives::TransactionKey::from(tx_key_string))
                .await?;
            transactions.push(tx);
        }
        Ok(transactions)
    }
}
