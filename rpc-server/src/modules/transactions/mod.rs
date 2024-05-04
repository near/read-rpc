use jsonrpc_v2::Data;

use crate::config::ServerContext;

pub mod methods;

/// Helper method that tries to find the transaction in the object storage by its hash.
/// If the transaction is not found in the storage it might be because it hasn't finished yet
/// and can be retrieved from the database cache table.
/// If the transaction is not found neither in the storage nor in the cache table this helper returns error.
pub(crate) async fn try_get_transaction_details_by_hash(
    data: &Data<ServerContext>,
    tx_hash: &str,
) -> anyhow::Result<readnode_primitives::TransactionDetails> {
    match data.tx_details_storage.retrieve(tx_hash).await {
        Ok(transaction_details_bytes) => {
            let transaction_details = borsh::from_slice::<readnode_primitives::TransactionDetails>(
                &transaction_details_bytes,
            )?;
            Ok(transaction_details)
        }
        Err(_) => {
            tracing::debug!(
                "Transaction with hash {} is not found in the object storage. Trying to find it in the cache table",
                tx_hash
            );
            let transaction_details = data
                .db_manager
                .get_indexing_transaction_by_hash(tx_hash)
                .await?;
            Ok(transaction_details)
        }
    }
}
