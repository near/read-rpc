use borsh::BorshDeserialize;

use jsonrpc_v2::{Data, Params};

use crate::config::ServerContext;
use crate::errors::RPCError;
use crate::utils::proxy_rpc_call;

pub async fn fetch_receipt(
    data: &Data<ServerContext>,
    request: &near_jsonrpc_primitives::types::receipts::RpcReceiptRequest,
) -> anyhow::Result<near_primitives::views::ReceiptView> {
    let receipt_id = request.receipt_reference.receipt_id.clone();

    let (_receipt_id, tx_hash, _block_height, _shard_id) =
        match data.scylla_db_manager.get_receipt_by_id(receipt_id).await {
            Ok(row) => {
                row.into_typed::<(String, String, num_bigint::BigInt, num_bigint::BigInt)>()?
            }
            Err(err) => anyhow::bail!(
                "receipts_map doesn't contain receipt_id {}\n{:#?}",
                receipt_id,
                err
            ),
        };

    let transaction_details = match data
        .scylla_db_manager
        .get_transaction_by_hash(&tx_hash)
        .await
    {
        Ok(row) => {
            let (tx_details_blob,): (Vec<u8>,) = row.into_typed::<(Vec<u8>,)>()?;
            readnode_primitives::TransactionDetails::try_from_slice(&tx_details_blob)?
        }
        Err(err) => anyhow::bail!(
            "Failed to get TransactionDetails {} in ScyllaDB:\n{:?}",
            tx_hash,
            err
        ),
    };

    let receipt_view = match transaction_details
        .receipts
        .into_iter()
        .find(|receipt| receipt.receipt_id == receipt_id)
    {
        Some(receipt) => receipt,
        None => anyhow::bail!(
            "Couldn't find Receipt {} in TransactionDetails {}",
            receipt_id,
            tx_hash
        ),
    };

    Ok(receipt_view)
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn receipt(
    data: Data<ServerContext>,
    Params(params): Params<serde_json::Value>,
) -> Result<near_jsonrpc_primitives::types::receipts::RpcReceiptResponse, RPCError> {
    tracing::debug!("`receipt` call. Params: {:?}", params);

    let request: near_jsonrpc_primitives::types::receipts::RpcReceiptRequest =
        match serde_json::from_value(params.clone()) {
            Ok(request) => request,
            Err(err) => return Err(RPCError::parse_error(&err.to_string())),
        };

    let receipt_view = match fetch_receipt(&data, &request).await {
        Ok(resp) => resp,
        Err(err) => {
            tracing::debug!("Receipt not found: {:#?}", err);
            let receipt_view = proxy_rpc_call(&data.near_rpc_client, request).await?;
            receipt_view
        }
    };

    Ok(near_jsonrpc_primitives::types::receipts::RpcReceiptResponse { receipt_view })
}
