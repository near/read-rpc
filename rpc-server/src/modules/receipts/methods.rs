use jsonrpc_v2::{Data, Params};
use near_jsonrpc::RpcRequest;

use crate::config::ServerContext;
use crate::errors::RPCError;

/// Fetches a receipt by it's ID (as is, without a status or execution outcome)
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn receipt(
    data: Data<ServerContext>,
    Params(params): Params<serde_json::Value>,
) -> Result<near_jsonrpc::primitives::types::receipts::RpcReceiptResponse, RPCError> {
    tracing::debug!("`receipt` call. Params: {:?}", params);
    let receipt_request =
        near_jsonrpc::primitives::types::receipts::RpcReceiptRequest::parse(params)?;

    let result = fetch_receipt(&data, &receipt_request).await;

    #[cfg(feature = "shadow_data_consistency")]
    {
        crate::utils::shadow_compare_results_handler(
            data.shadow_data_consistency_rate,
            &result,
            data.near_rpc_client.clone(),
            receipt_request,
            "EXPERIMENTAL_receipt",
        )
        .await;
    }

    Ok(result.map_err(near_jsonrpc::primitives::errors::RpcError::from)?)
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn fetch_receipt(
    data: &Data<ServerContext>,
    request: &near_jsonrpc::primitives::types::receipts::RpcReceiptRequest,
) -> Result<
    near_jsonrpc::primitives::types::receipts::RpcReceiptResponse,
    near_jsonrpc::primitives::types::receipts::RpcReceiptError,
> {
    let receipt_id = request.receipt_reference.receipt_id;

    let receipt_record = data
        .db_manager
        .get_receipt_by_id(receipt_id, "EXPERIMENTAL_receipt")
        .await
        .map_err(|err| {
            tracing::warn!("Error in `receipt` call: {:?}", err);
            near_jsonrpc::primitives::types::receipts::RpcReceiptError::UnknownReceipt {
                receipt_id,
            }
        })?;

    // Getting the raw Vec<u8> of the TransactionDetails from ScyllaDB
    let transaction_details = data
        .db_manager
        .get_transaction_by_hash(
            &receipt_record.parent_transaction_hash.to_string(),
            "EXPERIMENTAL_receipt",
        )
        .await
        .map_err(|err| {
            tracing::warn!("Error in `receipt` call: {:?}", err);
            near_jsonrpc::primitives::types::receipts::RpcReceiptError::UnknownReceipt {
                receipt_id,
            }
        })?;

    let receipt_view = transaction_details
        .receipts
        .into_iter()
        .find(|receipt| receipt.receipt_id == receipt_id)
        .ok_or_else(|| {
            tracing::warn!(
                "Receipt is not found in the TransactionDetails. tx hash: {}, receipt_id: {}",
                &receipt_record.parent_transaction_hash.to_string(),
                receipt_id
            );

            near_jsonrpc::primitives::types::receipts::RpcReceiptError::UnknownReceipt {
                receipt_id,
            }
        })?;

    Ok(near_jsonrpc::primitives::types::receipts::RpcReceiptResponse { receipt_view })
}
