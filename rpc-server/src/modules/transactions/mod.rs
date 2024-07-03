use jsonrpc_v2::Data;

use crate::config::ServerContext;

pub mod methods;

pub(crate) async fn try_get_transaction_details_by_hash(
    data: &Data<ServerContext>,
    tx_hash: &near_indexer_primitives::CryptoHash,
) -> anyhow::Result<readnode_primitives::TransactionDetails> {
    let transaction_details_bytes = data
        .tx_details_storage
        .retrieve(&tx_hash.to_string())
        .await?;
    Ok(borsh::from_slice::<readnode_primitives::TransactionDetails>(&transaction_details_bytes)?)
}
