use actix_web::web::Data;
use near_primitives::views::FinalExecutionOutcomeViewEnum::{
    FinalExecutionOutcome, FinalExecutionOutcomeWithReceipt,
};

use crate::config::ServerContext;

pub async fn send_tx(
    data: Data<ServerContext>,
    request_data: near_jsonrpc::primitives::types::transactions::RpcSendTransactionRequest,
) -> Result<
    near_jsonrpc::primitives::types::transactions::RpcTransactionResponse,
    near_jsonrpc::primitives::types::transactions::RpcTransactionError,
> {
    Ok(data
        .near_rpc_client
        .call(request_data, Some("send_tx"))
        .await
        .map_err(|err| {
            err.handler_error().cloned().unwrap_or(
                near_jsonrpc::primitives::types::transactions::RpcTransactionError::InternalError {
                    debug_info: err.to_string(),
                },
            )
        })?)
}

/// Queries status of a transaction by hash and returns the final transaction result.
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn tx(
    data: Data<ServerContext>,
    request_data: near_jsonrpc::primitives::types::transactions::RpcTransactionStatusRequest,
) -> Result<
    near_jsonrpc::primitives::types::transactions::RpcTransactionResponse,
    near_jsonrpc::primitives::types::transactions::RpcTransactionError,
> {
    tracing::debug!("`tx` call. Params: {:?}", request_data);

    let tx_result = tx_status_common(&data, &request_data.transaction_info, false).await;

    #[cfg(feature = "shadow-data-consistency")]
    {
        crate::utils::shadow_compare_results_handler(
            data.shadow_data_consistency_rate,
            &tx_result,
            data.near_rpc_client.clone(),
            // Note there is a difference in the implementation of the `tx` method in the `near_jsonrpc_client`
            // The method is `near_jsonrpc_client::methods::tx::RpcTransactionStatusRequest` in the client
            // so we can't just pass `params` there, instead we need to craft a request manually
            // tx_status_request,
            near_jsonrpc_client::methods::tx::RpcTransactionStatusRequest {
                transaction_info: request_data.transaction_info,
                wait_until: request_data.wait_until,
            },
            "tx",
        )
        .await;
    }
    tx_result
}

/// Queries status of a transaction by hash, returning the final transaction result and details of all receipts.
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn tx_status(
    data: Data<ServerContext>,
    request_data: near_jsonrpc::primitives::types::transactions::RpcTransactionStatusRequest,
) -> Result<
    near_jsonrpc::primitives::types::transactions::RpcTransactionResponse,
    near_jsonrpc::primitives::types::transactions::RpcTransactionError,
> {
    tracing::debug!("`tx_status` call. Params: {:?}", request_data);

    let tx_result = tx_status_common(&data, &request_data.transaction_info, true).await;

    #[cfg(feature = "shadow-data-consistency")]
    {
        crate::utils::shadow_compare_results_handler(
            data.shadow_data_consistency_rate,
            &tx_result,
            data.near_rpc_client.clone(),
            // Note there is a difference in the implementation of the `EXPERIMENTAL_tx_status` method in the `near_jsonrpc_client`
            // The method is `near_jsonrpc_client::methods::EXPERIMENTAL_tx_status` in the client
            // so we can't just pass `params` there, instead we need to craft a request manually
            near_jsonrpc_client::methods::EXPERIMENTAL_tx_status::RpcTransactionStatusRequest {
                transaction_info: request_data.transaction_info,
                wait_until: request_data.wait_until,
            },
            "EXPERIMENTAL_tx_status",
        )
        .await;
    }

    tx_result
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn broadcast_tx_async(
    data: Data<ServerContext>,
    request_data: near_jsonrpc::primitives::types::transactions::RpcSendTransactionRequest,
) -> Result<near_primitives::hash::CryptoHash, near_jsonrpc::primitives::errors::RpcError> {
    tracing::debug!("`broadcast_tx_async` call. Params: {:?}", request_data);
    let proxy_params =
        near_jsonrpc_client::methods::broadcast_tx_async::RpcBroadcastTxAsyncRequest {
            signed_transaction: request_data.signed_transaction,
        };
    match data
        .near_rpc_client
        .call(proxy_params, Some("broadcast_tx_async"))
        .await
    {
        Ok(resp) => Ok(resp),
        Err(err) => Err(
            near_jsonrpc::primitives::errors::RpcError::new_internal_error(None, err.to_string()),
        ),
    }
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn broadcast_tx_commit(
    data: Data<ServerContext>,
    request_data: near_jsonrpc::primitives::types::transactions::RpcSendTransactionRequest,
) -> Result<
    near_jsonrpc::primitives::types::transactions::RpcTransactionResponse,
    near_jsonrpc::primitives::types::transactions::RpcTransactionError,
> {
    tracing::debug!("`broadcast_tx_commit` call. Params: {:?}", request_data);
    let proxy_params =
        near_jsonrpc_client::methods::broadcast_tx_commit::RpcBroadcastTxCommitRequest {
            signed_transaction: request_data.signed_transaction,
        };
    let result = data
        .near_rpc_client
        .call(proxy_params, Some("broadcast_tx_commit"))
        .await
        .map_err(|err| {
            err.handler_error().cloned().unwrap_or(
                near_jsonrpc::primitives::types::transactions::RpcTransactionError::InternalError {
                    debug_info: err.to_string(),
                },
            )
        })?;
    Ok(
        near_jsonrpc::primitives::types::transactions::RpcTransactionResponse {
            final_execution_outcome: Some(FinalExecutionOutcome(result)),
            final_execution_status: near_primitives::views::TxExecutionStatus::Final,
        },
    )
}

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(data, transaction_info))
)]
async fn tx_status_common(
    data: &Data<ServerContext>,
    transaction_info: &near_jsonrpc::primitives::types::transactions::TransactionInfo,
    fetch_receipt: bool,
) -> Result<
    near_jsonrpc::primitives::types::transactions::RpcTransactionResponse,
    near_jsonrpc::primitives::types::transactions::RpcTransactionError,
> {
    tracing::debug!("`tx_status_common` call.");
    let tx_hash = match &transaction_info {
        near_jsonrpc::primitives::types::transactions::TransactionInfo::Transaction(
            near_jsonrpc::primitives::types::transactions::SignedTransaction::SignedTransaction(tx),
        ) => tx.get_hash(),
        near_jsonrpc::primitives::types::transactions::TransactionInfo::TransactionId {
            tx_hash,
            ..
        } => *tx_hash,
    };

    let transaction_details = super::try_get_transaction_details_by_hash(data, &tx_hash)
        .await
        .map_err(|err| {
            // logging the error at debug level since it's expected to see some "not found"
            // errors in the logs that doesn't mean that something is really wrong, but want to
            // keep track of them to see if there are any patterns
            tracing::debug!("Error while fetching transaction details: {:?}", err);
            near_jsonrpc::primitives::types::transactions::RpcTransactionError::UnknownTransaction {
                requested_transaction_hash: tx_hash,
            }
        })?;

    // TODO (@kobayurii): rewrite this since we support optimistic finalities already
    if fetch_receipt {
        Ok(
            near_jsonrpc::primitives::types::transactions::RpcTransactionResponse {
                final_execution_outcome: Some(FinalExecutionOutcomeWithReceipt(
                    transaction_details.to_final_execution_outcome_with_receipts(),
                )),
                // With the fact that we don't support non-finalised data yet,
                // final_execution_status field can be always filled with FINAL.
                // This logic will be more complicated when we add support of optimistic blocks.
                final_execution_status: near_primitives::views::TxExecutionStatus::Final,
            },
        )
    } else {
        Ok(
            near_jsonrpc::primitives::types::transactions::RpcTransactionResponse {
                final_execution_outcome: Some(FinalExecutionOutcome(
                    transaction_details.to_final_execution_outcome(),
                )),
                // With the fact that we don't support non-finalised data yet,
                // final_execution_status field can be always filled with FINAL.
                // This logic will be more complicated when we add support of optimistic blocks.
                final_execution_status: near_primitives::views::TxExecutionStatus::Final,
            },
        )
    }
}
