use crate::postgres::PostgresStorageManager;
use crate::AdditionalDatabaseOptions;
use bigdecimal::ToPrimitive;
use borsh::BorshSerialize;

pub(crate) struct PostgresDBManager {
    pg_pool: crate::postgres::PgAsyncPool,
}

#[async_trait::async_trait]
impl crate::BaseDbManager for PostgresDBManager {
    async fn new(
        database_url: &str,
        database_user: Option<&str>,
        database_password: Option<&str>,
        database_options: AdditionalDatabaseOptions,
    ) -> anyhow::Result<Box<Self>> {
        let pg_pool = Self::create_pool(
            database_url,
            database_user,
            database_password,
            database_options,
        )
        .await?;
        Ok(Box::new(Self { pg_pool }))
    }
}

#[async_trait::async_trait]
impl PostgresStorageManager for PostgresDBManager {}

#[async_trait::async_trait]
impl crate::TxIndexerDbManager for PostgresDBManager {
    async fn add_transaction(
        &self,
        transaction: readnode_primitives::TransactionDetails,
        block_height: u64,
    ) -> anyhow::Result<()> {
        let transaction_details = transaction
            .try_to_vec()
            .expect("Failed to borsh-serialize the Transaction");
        crate::models::TransactionDetail {
            transaction_hash: transaction.transaction.hash.to_string(),
            block_height: bigdecimal::BigDecimal::from(block_height),
            account_id: transaction.transaction.signer_id.to_string(),
            transaction_details,
        }
        .save(Self::get_connection(&self.pg_pool).await?)
        .await
    }

    async fn add_receipt(
        &self,
        receipt_id: &str,
        parent_tx_hash: &str,
        block_height: u64,
        shard_id: u64,
    ) -> anyhow::Result<()> {
        crate::models::ReceiptMap {
            receipt_id: receipt_id.to_string(),
            parent_transaction_hash: parent_tx_hash.to_string(),
            block_height: bigdecimal::BigDecimal::from(block_height),
            shard_id: bigdecimal::BigDecimal::from(shard_id),
        }
        .save(Self::get_connection(&self.pg_pool).await?)
        .await
    }

    async fn update_meta(&self, indexer_id: &str, block_height: u64) -> anyhow::Result<()> {
        crate::models::Meta {
            indexer_id: indexer_id.to_string(),
            last_processed_block_height: bigdecimal::BigDecimal::from(block_height),
        }
        .save(Self::get_connection(&self.pg_pool).await?)
        .await
    }

    async fn cache_add_transaction(
        &self,
        transaction_details: readnode_primitives::CollectingTransactionDetails,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn cache_add_receipt(
        &self,
        transaction_key: readnode_primitives::TransactionKey,
        indexer_execution_outcome_with_receipt: near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn get_transactions_to_cache(
        &self,
        start_block_height: u64,
        cache_restore_blocks_range: u64,
        max_db_parallel_queries: i64,
    ) -> anyhow::Result<
        std::collections::HashMap<
            readnode_primitives::TransactionKey,
            readnode_primitives::CollectingTransactionDetails,
        >,
    > {
        todo!()
    }

    async fn get_transaction_by_receipt_id(
        &self,
        receipt_id: &str,
    ) -> anyhow::Result<readnode_primitives::CollectingTransactionDetails> {
        todo!()
    }

    async fn get_receipts_in_cache(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
    ) -> anyhow::Result<Vec<near_indexer_primitives::IndexerExecutionOutcomeWithReceipt>> {
        todo!()
    }

    async fn cache_delete_transaction(
        &self,
        transaction_hash: &str,
        block_height: u64,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn get_last_processed_block_height(&self, indexer_id: &str) -> anyhow::Result<u64> {
        let block_height = crate::models::Meta::get_last_processed_block_height(
            Self::get_connection(&self.pg_pool).await?,
            indexer_id,
        )
        .await?;
        block_height
            .to_u64()
            .ok_or_else(|| anyhow::anyhow!("Failed to parse `block_height` to u64"))
    }
}
