use jsonrpc_v2::{Data, Params};

use crate::config::ServerContext;
use crate::errors::RPCError;

/// Fetches a receipt by it's ID (as is, without a status or execution outcome)
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn receipt(
    data: Data<ServerContext>,
    Params(params): Params<near_jsonrpc_primitives::types::receipts::RpcReceiptRequest>,
) -> Result<near_jsonrpc_primitives::types::receipts::RpcReceiptResponse, RPCError> {
    tracing::debug!("`receipt` call. Params: {:?}", params);
    crate::metrics::RECEIPT_REQUESTS_TOTAL.inc();

    let result = fetch_receipt(&data, &params).await;

    #[cfg(feature = "shadow_data_consistency")]
    {
        if let Some(err_code) = crate::utils::shadow_compare_results_handler(
            crate::metrics::RECEIPT_REQUESTS_TOTAL.get(),
            data.shadow_data_consistency_rate,
            &result,
            data.near_rpc_client.clone(),
            params,
            "RECEIPT",
        )
        .await
        {
            crate::utils::capture_shadow_consistency_error!(err_code, "RECEIPT")
        };
    }

    Ok(result.map_err(near_jsonrpc_primitives::errors::RpcError::from)?)
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn fetch_receipt(
    data: &Data<ServerContext>,
    request: &near_jsonrpc_primitives::types::receipts::RpcReceiptRequest,
) -> Result<
    near_jsonrpc_primitives::types::receipts::RpcReceiptResponse,
    near_jsonrpc_primitives::types::receipts::RpcReceiptError,
> {
    let receipt_id = request.receipt_reference.receipt_id;

    let receipt_record = data
        .db_manager
        .get_receipt_by_id(receipt_id)
        .await
        .map_err(|err| {
            tracing::warn!("Error in `receipt` call: {:?}", err);
            near_jsonrpc_primitives::types::receipts::RpcReceiptError::UnknownReceipt { receipt_id }
        })?;

    // Getting the raw Vec<u8> of the TransactionDetails from ScyllaDB
    let transaction_details = data
        .db_manager
        .get_transaction_by_hash(&receipt_record.parent_transaction_hash.to_string())
        .await
        .map_err(|err| {
            tracing::warn!("Error in `receipt` call: {:?}", err);
            near_jsonrpc_primitives::types::receipts::RpcReceiptError::UnknownReceipt { receipt_id }
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

            near_jsonrpc_primitives::types::receipts::RpcReceiptError::UnknownReceipt { receipt_id }
        })?;

    Ok(near_jsonrpc_primitives::types::receipts::RpcReceiptResponse { receipt_view })
}
