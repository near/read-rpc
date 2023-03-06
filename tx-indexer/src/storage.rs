pub use redis::{self, aio::ConnectionManager, FromRedisValue, ToRedisArgs};
use near_indexer_primitives::IndexerExecutionOutcomeWithReceipt;
use borsh::{BorshDeserialize, BorshSerialize};
use crate::types::TransactionDetails;

const STORAGE: &str = "storage_tx";
const TX_TO_SEND_LIST_KEY: &str = "transactions_to_send";

pub async fn get_redis_client(redis_connection_str: &str) -> redis::Client {
    redis::Client::open(redis_connection_str).expect("can create redis client")
}

pub async fn connect(redis_connection_str: &str) -> anyhow::Result<ConnectionManager> {
    Ok(get_redis_client(redis_connection_str)
        .await
        .get_tokio_connection_manager()
        .await?)
}

pub async fn del(
    redis_connection_manager: &ConnectionManager,
    key: impl ToRedisArgs + std::fmt::Debug,
) -> anyhow::Result<()> {
    redis::cmd("DEL")
        .arg(&key)
        .query_async(&mut redis_connection_manager.clone())
        .await?;
    tracing::debug!(target: STORAGE, "DEL: {:?}", key);
    Ok(())
}

pub async fn set(
    redis_connection_manager: &ConnectionManager,
    key: impl ToRedisArgs + std::fmt::Debug,
    value: impl ToRedisArgs + std::fmt::Debug,
) -> anyhow::Result<()> {
    redis::cmd("SET")
        .arg(&key)
        .arg(&value)
        .query_async(&mut redis_connection_manager.clone())
        .await?;
    tracing::debug!(target: STORAGE, "SET: {:?}: {:?}", key, value,);
    Ok(())
}

pub async fn get<V: FromRedisValue + std::fmt::Debug>(
    redis_connection_manager: &ConnectionManager,
    key: impl ToRedisArgs + std::fmt::Debug,
) -> anyhow::Result<V> {
    let value: V = redis::cmd("GET")
        .arg(&key)
        .query_async(&mut redis_connection_manager.clone())
        .await?;
    tracing::debug!(target: STORAGE, "GET: {:?}: {:?}", &key, &value,);
    Ok(value)
}

/// Sets the key `receipt_id: &str` with value `transaction_hash: &str` to the Redis storage.
/// Increments the counter `receipts_{transaction_hash}` by one.
/// The counter holds how many Receipts related to the Transaction are in watching list
pub async fn push_receipt_to_watching_list(
    redis_connection_manager: &ConnectionManager,
    receipt_id: &str,
    cache_value: &str,
) -> anyhow::Result<()> {
    set(redis_connection_manager, receipt_id, cache_value).await?;
    redis::cmd("INCR")
        .arg(format!("receipts_{}", cache_value))
        .query_async(&mut redis_connection_manager.clone())
        .await?;
    Ok(())
}

/// Removes key `receipt_id: &str` from Redis storage.
/// If the key exists in the storage decreases the `receipts_{transaction_hash}` counter.
pub async fn remove_receipt_from_watching_list(
    redis_connection_manager: &ConnectionManager,
    receipt_id: &str,
) -> anyhow::Result<Option<String>> {
    match get::<Option<String>>(redis_connection_manager, receipt_id).await {
        Ok(maybe_transaction_hash) => {
            if let Some(ref transaction_hash) = maybe_transaction_hash {
                redis::cmd("DECR")
                    .arg(format!("receipts_{}", transaction_hash))
                    .query_async(&mut redis_connection_manager.clone())
                    .await?;
                tracing::debug!(target: STORAGE, "DECR: receipts_{}", transaction_hash);
                del(redis_connection_manager, receipt_id).await?;
            }
            Ok(maybe_transaction_hash)
        }
        Err(e) => {
            anyhow::bail!(e)
        }
    }
}

/// Returns the value of the `receipts_{transaction_hash}` counter
pub async fn receipts_transaction_hash_count(
    redis_connection_manager: &ConnectionManager,
    transaction_hash: &str,
) -> anyhow::Result<u64> {
    get::<u64>(
        redis_connection_manager,
        format!("receipts_{}", transaction_hash),
    )
        .await
}


pub async fn get_last_indexed_block(
    redis_connection_manager: &ConnectionManager,
) -> anyhow::Result<u64> {
    Ok(redis::cmd("GET")
        .arg("last_indexed_block")
        .query_async(&mut redis_connection_manager.clone())
        .await?)
}

pub async fn set_tx(
    redis_connection_manager: &ConnectionManager,
    transaction_details: TransactionDetails,
) -> anyhow::Result<()> {
    let transaction_hash_string = transaction_details.transaction.hash.to_string();
    let encoded_tx_details = transaction_details.try_to_vec()?;

    set(
        redis_connection_manager,
        &transaction_hash_string,
        &encoded_tx_details,
    )
        .await?;

    tracing::debug!(
        target: crate::INDEXER,
        "TX added for collecting {}",
        &transaction_hash_string
    );
    Ok(())
}

pub async fn get_tx(
    redis_connection_manager: &ConnectionManager,
    transaction_hash: &str,
) -> anyhow::Result<Option<TransactionDetails>> {
    let value: Vec<u8> = get(redis_connection_manager, transaction_hash).await?;

    Ok(Some(TransactionDetails::try_from_slice(&value)?))
}

pub async fn push_tx_to_send(
    redis_connection_manager: &ConnectionManager,
    transaction_details: TransactionDetails,
) -> anyhow::Result<()> {
    let encoded_tx_details = transaction_details.try_to_vec()?;

    redis::cmd("RPUSH")
        .arg(TX_TO_SEND_LIST_KEY)
        .arg(&encoded_tx_details)
        .query_async(&mut redis_connection_manager.clone())
        .await?;

    Ok(())
}

pub async fn transactions_to_send(
    redis_connection_manager: &ConnectionManager,
) -> anyhow::Result<Vec<TransactionDetails>> {

    let length: usize = redis::cmd("LLEN")
        .arg(TX_TO_SEND_LIST_KEY)
        .query_async(&mut redis_connection_manager.clone())
        .await?;

    let values: Vec<Vec<u8>> = redis::cmd("LPOP")
        .arg(TX_TO_SEND_LIST_KEY)
        .arg(length)
        .query_async(&mut redis_connection_manager.clone())
        .await?;

    let tx_details: Vec<TransactionDetails> = values
        .iter()
        .filter_map(|value| TransactionDetails::try_from_slice(value).ok())
        .collect();

    Ok(tx_details)
}

pub async fn push_outcome_and_receipt(
    redis_connection_manager: &ConnectionManager,
    transaction_hash: &str,
    indexer_execution_outcome_with_receipt: IndexerExecutionOutcomeWithReceipt,
) -> anyhow::Result<()> {
    if let Ok(Some(mut transaction_details)) =
        get_tx(redis_connection_manager, transaction_hash).await
    {
        tracing::debug!(
            target: crate::INDEXER,
            "-R {}",
            &indexer_execution_outcome_with_receipt
                .receipt
                .receipt_id
                .to_string(),
        );
        remove_receipt_from_watching_list(
            redis_connection_manager,
            &indexer_execution_outcome_with_receipt
                .receipt
                .receipt_id
                .to_string(),
        )
            .await?;
        transaction_details
            .receipts
            .push(indexer_execution_outcome_with_receipt.receipt);

        transaction_details.execution_outcomes.push(
            indexer_execution_outcome_with_receipt
                .execution_outcome
                .clone(),
        );

        let transaction_receipts_watching_count =
            receipts_transaction_hash_count(redis_connection_manager, transaction_hash)
                .await?;

        if transaction_receipts_watching_count == 0 {
            tracing::debug!(target: crate::INDEXER, "Finished TX {}", &transaction_hash,);

            push_tx_to_send(redis_connection_manager, transaction_details).await?;
            del(redis_connection_manager, transaction_hash).await?;
        } else {
            tracing::debug!(
                target: crate::INDEXER,
                "{} | UPDATE TX {}",
                transaction_receipts_watching_count,
                &transaction_hash
            );
            set_tx(redis_connection_manager, transaction_details).await?;
        }
    } else {
        tracing::debug!(target: crate::INDEXER, "Missing TX {}", &transaction_hash);
    }
    Ok(())
}
