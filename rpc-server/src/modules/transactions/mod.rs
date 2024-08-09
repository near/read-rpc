use crate::config::ServerContext;
use jsonrpc_v2::Data;

pub mod methods;

async fn fetch_tx_details(
    tx_hash: &near_indexer_primitives::CryptoHash,
) -> anyhow::Result<readnode_primitives::TransactionDetails> {
    let data: serde_json::Value = reqwest::get(format!("http://localhost:9099/get-tx/{}", tx_hash.to_string())).await?.json().await?;
    Ok(serde_json::from_value(data)?)
}

pub(crate) async fn try_get_transaction_details_by_hash(
    data: &Data<ServerContext>,
    tx_hash: &near_indexer_primitives::CryptoHash,
) -> anyhow::Result<readnode_primitives::TransactionDetails> {
    if let Ok(transaction_details_bytes) =
        fetch_tx_details(tx_hash).await
    {
        Ok(transaction_details_bytes)
    } else if let Some(tx_cache_storage) = data.tx_cache_storage.clone() {
        Ok(tx_cache_storage.get_tx_by_tx_hash(tx_hash).await?)
    } else {
        anyhow::bail!("Transaction not found")
    }
}
