use crate::postgres::PostgresStorageManager;
use crate::AdditionalDatabaseOptions;
use bigdecimal::ToPrimitive;
use borsh::{BorshDeserialize, BorshSerialize};

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
        .insert_or_ignore(Self::get_connection(&self.pg_pool).await?)
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
        .insert_or_ignore(Self::get_connection(&self.pg_pool).await?)
        .await
    }

    async fn update_meta(&self, indexer_id: &str, block_height: u64) -> anyhow::Result<()> {
        crate::models::Meta {
            indexer_id: indexer_id.to_string(),
            last_processed_block_height: bigdecimal::BigDecimal::from(block_height),
        }
        .insert_or_update(Self::get_connection(&self.pg_pool).await?)
        .await
    }

    async fn cache_add_transaction(
        &self,
        transaction_details: readnode_primitives::CollectingTransactionDetails,
    ) -> anyhow::Result<()> {
        let transaction_hash = transaction_details.transaction.hash.clone().to_string();
        let block_height = transaction_details.block_height;
        let transaction_details = transaction_details.try_to_vec().map_err(|err| {
            tracing::error!(target: "tx_indexer", "Failed to serialize transaction details: {:?}", err);
            err})?;
        crate::models::TransactionCache {
            block_height: bigdecimal::BigDecimal::from(block_height),
            transaction_hash,
            transaction_details,
        }
        .insert_or_ignore(Self::get_connection(&self.pg_pool).await?)
        .await?;
        Ok(())
    }

    async fn cache_add_receipt(
        &self,
        transaction_key: readnode_primitives::TransactionKey,
        indexer_execution_outcome_with_receipt: near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
    ) -> anyhow::Result<()> {
        crate::models::ReceiptOutcome {
            block_height: bigdecimal::BigDecimal::from(transaction_key.block_height),
            transaction_hash: transaction_key.transaction_hash,
            receipt_id: indexer_execution_outcome_with_receipt
                .receipt
                .receipt_id
                .to_string(),
            receipt: indexer_execution_outcome_with_receipt
                .receipt
                .try_to_vec()?,
            outcome: indexer_execution_outcome_with_receipt
                .execution_outcome
                .try_to_vec()?,
        }
        .insert_or_ignore(Self::get_connection(&self.pg_pool).await?)
        .await
    }

    async fn get_transactions_to_cache(
        &self,
        start_block_height: u64,
        cache_restore_blocks_range: u64,
        _max_db_parallel_queries: i64,
    ) -> anyhow::Result<
        std::collections::HashMap<
            readnode_primitives::TransactionKey,
            readnode_primitives::CollectingTransactionDetails,
        >,
    > {
        let transactions = crate::models::TransactionCache::get_transactions(
            Self::get_connection(&self.pg_pool).await?,
            start_block_height,
            cache_restore_blocks_range,
        )
        .await?;
        Ok(transactions
            .into_iter()
            .map(|tx| {
                let transaction_details =
                    readnode_primitives::CollectingTransactionDetails::try_from_slice(
                        &tx.transaction_details,
                    )
                    .expect("Failed to deserialize transaction details");
                (transaction_details.transaction_key(), transaction_details)
            })
            .collect())
    }

    async fn get_transaction_by_receipt_id(
        &self,
        receipt_id: &str,
    ) -> anyhow::Result<readnode_primitives::CollectingTransactionDetails> {
        let (block_height, transaction_hash) = crate::models::ReceiptOutcome::get_transaction_key(
            Self::get_connection(&self.pg_pool).await?,
            receipt_id,
        )
        .await?;
        let transaction_details = crate::models::TransactionCache::get_transaction(
            Self::get_connection(&self.pg_pool).await?,
            block_height,
            &transaction_hash,
        )
        .await?;
        Ok(
            readnode_primitives::CollectingTransactionDetails::try_from_slice(
                &transaction_details,
            )?,
        )
    }

    async fn get_receipts_in_cache(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
    ) -> anyhow::Result<Vec<near_indexer_primitives::IndexerExecutionOutcomeWithReceipt>> {
        let result = crate::models::ReceiptOutcome::get_receipt_outcome(
            Self::get_connection(&self.pg_pool).await?,
            transaction_key.block_height,
            &transaction_key.transaction_hash,
        )
        .await?;
        Ok(result
            .into_iter()
            .map(|receipt_outcome| {
                let receipt =
                    near_primitives::views::ReceiptView::try_from_slice(&receipt_outcome.receipt)
                        .expect("Failed to deserialize receipt");
                let execution_outcome =
                    near_primitives::views::ExecutionOutcomeWithIdView::try_from_slice(
                        &receipt_outcome.outcome,
                    )
                    .expect("Failed to deserialize execution outcome");
                near_indexer_primitives::IndexerExecutionOutcomeWithReceipt {
                    receipt,
                    execution_outcome,
                }
            })
            .collect())
    }

    async fn cache_delete_transaction(
        &self,
        transaction_hash: &str,
        block_height: u64,
    ) -> anyhow::Result<()> {
        crate::models::TransactionCache::delete_transaction(
            Self::get_connection(&self.pg_pool).await?,
            block_height,
            transaction_hash,
        )
        .await?;
        crate::models::ReceiptOutcome::delete_receipt_outcome(
            Self::get_connection(&self.pg_pool).await?,
            block_height,
            transaction_hash,
        )
        .await?;
        Ok(())
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
