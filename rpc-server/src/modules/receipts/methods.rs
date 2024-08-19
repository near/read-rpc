use actix_web::web::Data;
use jsonrpc_v2::Params;
use near_jsonrpc::RpcRequest;

use crate::config::ServerContext;
use crate::errors::RPCError;
use crate::modules::transactions::try_get_transaction_details_by_hash;

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

/// Fetches a receipt record by it's ID
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn view_receipt_record(
    data: Data<ServerContext>,
    request_data: near_jsonrpc::primitives::types::receipts::RpcReceiptRequest,
) -> Result<crate::modules::receipts::RpcReceiptRecordResponse, RPCError> {
    tracing::debug!("`view_receipt_record` call. Params: {:?}", request_data);

    let result = fetch_receipt_record(&data, &request_data, "view_receipt_record").await;

    Ok(result
        .map_err(near_jsonrpc::primitives::errors::RpcError::from)?
        .into())
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

    let receipt_record = fetch_receipt_record(data, request, "EXPERIMENTAL_receipt").await?;

    let transaction_details =
        try_get_transaction_details_by_hash(data, &receipt_record.parent_transaction_hash)
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

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn fetch_receipt_record(
    data: &Data<ServerContext>,
    request: &near_jsonrpc::primitives::types::receipts::RpcReceiptRequest,
    method_name: &str,
) -> Result<
    readnode_primitives::ReceiptRecord,
    near_jsonrpc::primitives::types::receipts::RpcReceiptError,
> {
    let receipt_id = request.receipt_reference.receipt_id;
    let result = data
        .db_manager
        .get_receipt_by_id(receipt_id, method_name)
        .await
        .map_err(|err| {
            tracing::warn!("Error in `{}` call: {:?}", method_name, err);
            near_jsonrpc::primitives::types::receipts::RpcReceiptError::UnknownReceipt {
                receipt_id,
            }
        });
    if let Ok(receipt_record) = &result {
        // increase block category metrics
        crate::metrics::increase_request_category_metrics(
            data,
            &near_primitives::types::BlockReference::BlockId(
                near_primitives::types::BlockId::Height(receipt_record.block_height),
            ),
            method_name,
            Some(receipt_record.block_height),
        )
        .await;
    };
    result
}
