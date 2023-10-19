use jsonrpc_v2::{Data, Params};

use crate::config::ServerContext;
use crate::errors::RPCError;
#[cfg(feature = "shadow_data_consistency")]
use crate::utils::shadow_compare_results;

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
        let near_rpc_client = data.near_rpc_client.clone();
        let meta_data = format!("{:?}", params);
        let (read_rpc_response_json, is_response_ok) = match &result {
            Ok(res) => (serde_json::to_value(res), true),
            Err(err) => (serde_json::to_value(err), false),
        };

        let comparison_result = shadow_compare_results(
            read_rpc_response_json,
            near_rpc_client,
            params,
            is_response_ok,
        )
        .await;

        match comparison_result {
            Ok(_) => {
                tracing::info!(target: "shadow_data_consistency", "Shadow data check: CORRECT\n{}", meta_data);
            }
            Err(err) => {
                crate::utils::capture_shadow_consistency_error!(err, meta_data, "RECEIPT");
            }
        }
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
