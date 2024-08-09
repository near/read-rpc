use crate::config::ServerContext;
use jsonrpc_v2::Data;

pub mod methods;

async fn fetch_tx_details(
    tx_hash: &near_indexer_primitives::CryptoHash,
) -> anyhow::Result<readnode_primitives::TransactionDetails> {
    let tx_web_server_url =
        std::env::var("TX_WEB_SERVER_URL").unwrap_or("http://localhost:9099".to_string());
    let data: serde_json::Value = reqwest::get(format!("{}/get-tx/{}", tx_web_server_url, tx_hash))
        .await?
        .json()
        .await?;
    Ok(serde_json::from_value(data)?)
}

/// MIGRATION NOTE: Additionally this method will try to find the transaction
/// in the legacy database table between object storage and cache table.
/// REMOVE this comment after the migration to the new object storage is completely finished.
///
/// Helper method that tries to find the transaction in the object storage by its hash.
/// If the transaction is not found in the storage it might be because it hasn't finished yet
/// and can be retrieved from the database cache table.
/// If the transaction is not found neither in the storage nor in the cache table this helper returns error.
pub(crate) async fn try_get_transaction_details_by_hash(
    data: &Data<ServerContext>,
    tx_hash: &near_indexer_primitives::CryptoHash,
    method_name: &str,
) -> anyhow::Result<readnode_primitives::TransactionDetails> {
    match fetch_tx_details(tx_hash).await {
        Ok(transaction_details) => Ok(transaction_details),
        Err(_) => {
            tracing::debug!(
                "Transaction with hash {} is not found in the object storage. Trying to find it in the legacy database table.",
                tx_hash
            );
            // TODO: remove this logic after the migration to the new object storage is completely finished
            match legacy_try_get_transaction_details_by_hash(
                data,
                &tx_hash.to_string(),
                method_name,
            )
            .await
            {
                Ok(transaction_details) => Ok(transaction_details),
                Err(_err) => {
                    tracing::error!(
                        "Transaction with hash {} is not found in the legacy database table. Last try to find it in the cache table of the database",
                        tx_hash,
                    );
                    // TODO: Except this. The cache search should stay, though refactored to the new cache solution
                    // increase legacy database transaction details lookup metrics for in_progress transactions
                    crate::metrics::LEGACY_DATABASE_TX_DETAILS
                        .with_label_values(&["in_progress"])
                        .inc();
                    let (_, transaction_details) = data
                        .db_manager
                        .get_indexing_transaction_by_hash(&tx_hash.to_string(), method_name)
                        .await?;
                    Ok(transaction_details)
                }
            }
        }
    }
}

// TODO: remove this after the migration to the new object storage is completely finished
/// Helper method that tries to find the transaction in the legacy database table by its hash.
async fn legacy_try_get_transaction_details_by_hash(
    data: &Data<ServerContext>,
    tx_hash: &str,
    method_name: &str,
) -> anyhow::Result<readnode_primitives::TransactionDetails> {
    // increase legacy database transaction details lookup metrics for finished transactions
    crate::metrics::LEGACY_DATABASE_TX_DETAILS
        .with_label_values(&["finished"])
        .inc();
    let (block_height, transaction_details) = data
        .db_manager
        .get_transaction_by_hash(tx_hash, method_name)
        .await?;

    // increase block category metrics
    crate::metrics::increase_request_category_metrics(
        data,
        &near_primitives::types::BlockReference::BlockId(near_primitives::types::BlockId::Height(
            block_height,
        )),
        method_name,
        Some(block_height),
    )
    .await;

    Ok(transaction_details)
}
