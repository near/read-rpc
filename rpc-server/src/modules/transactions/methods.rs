use crate::config::ServerContext;
use crate::errors::RPCError;
use jsonrpc_v2::{Data, Params};
use near_jsonrpc::RpcRequest;
use near_primitives::views::FinalExecutionOutcomeViewEnum::{
    FinalExecutionOutcome, FinalExecutionOutcomeWithReceipt,
};

pub async fn send_tx(
    data: Data<ServerContext>,
    Params(params): Params<serde_json::Value>,
) -> Result<near_jsonrpc::primitives::types::transactions::RpcTransactionResponse, RPCError> {
    let request = near_jsonrpc_client::methods::send_tx::RpcSendTransactionRequest::parse(params)?;
    Ok(data.near_rpc_client.call(request).await?)
}

/// Queries status of a transaction by hash and returns the final transaction result.
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn tx(
    data: Data<ServerContext>,
    Params(params): Params<serde_json::Value>,
) -> Result<near_jsonrpc::primitives::types::transactions::RpcTransactionResponse, RPCError> {
    tracing::debug!("`tx` call. Params: {:?}", params);

    let tx_status_request =
        near_jsonrpc::primitives::types::transactions::RpcTransactionStatusRequest::parse(params)?;

    let result = tx_status_common(&data, &tx_status_request.transaction_info, false, "tx").await;

    #[cfg(feature = "shadow_data_consistency")]
    {
        crate::utils::shadow_compare_results_handler(
            data.shadow_data_consistency_rate,
            &result,
            data.near_rpc_client.clone(),
            // Note there is a difference in the implementation of the `tx` method in the `near_jsonrpc_client`
            // The method is `near_jsonrpc_client::methods::tx::RpcTransactionStatusRequest` in the client
            // so we can't just pass `params` there, instead we need to craft a request manually
            // tx_status_request,
            near_jsonrpc_client::methods::tx::RpcTransactionStatusRequest {
                transaction_info: tx_status_request.transaction_info,
                wait_until: tx_status_request.wait_until,
            },
            "tx",
        )
        .await;
    }

    Ok(result.map_err(near_jsonrpc::primitives::errors::RpcError::from)?)
}

/// Queries status of a transaction by hash, returning the final transaction result and details of all receipts.
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn tx_status(
    data: Data<ServerContext>,
    Params(params): Params<serde_json::Value>,
) -> Result<near_jsonrpc::primitives::types::transactions::RpcTransactionResponse, RPCError> {
    tracing::debug!("`tx_status` call. Params: {:?}", params);

    let tx_status_request =
        near_jsonrpc::primitives::types::transactions::RpcTransactionStatusRequest::parse(params)?;

    let result = tx_status_common(
        &data,
        &tx_status_request.transaction_info,
        true,
        "EXPERIMENTAL_tx_status",
    )
    .await;

    #[cfg(feature = "shadow_data_consistency")]
    {
        crate::utils::shadow_compare_results_handler(
            data.shadow_data_consistency_rate,
            &result,
            data.near_rpc_client.clone(),
            // Note there is a difference in the implementation of the `EXPERIMENTAL_tx_status` method in the `near_jsonrpc_client`
            // The method is `near_jsonrpc_client::methods::EXPERIMENTAL_tx_status` in the client
            // so we can't just pass `params` there, instead we need to craft a request manually
            near_jsonrpc_client::methods::EXPERIMENTAL_tx_status::RpcTransactionStatusRequest {
                transaction_info: tx_status_request.transaction_info,
                wait_until: tx_status_request.wait_until,
            },
            "EXPERIMENTAL_tx_status",
        )
        .await;
    }

    Ok(result.map_err(near_jsonrpc::primitives::errors::RpcError::from)?)
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn broadcast_tx_async(
    data: Data<ServerContext>,
    Params(params): Params<serde_json::Value>,
) -> Result<near_primitives::hash::CryptoHash, RPCError> {
    tracing::debug!("`broadcast_tx_async` call. Params: {:?}", params);
    if cfg!(feature = "send_tx_methods") {
        let tx_async_request =
            near_jsonrpc::primitives::types::transactions::RpcSendTransactionRequest::parse(
                params,
            )?;
        let proxy_params =
            near_jsonrpc_client::methods::broadcast_tx_async::RpcBroadcastTxAsyncRequest {
                signed_transaction: tx_async_request.signed_transaction,
            };
        match data.near_rpc_client.call(proxy_params).await {
            Ok(resp) => Ok(resp),
            Err(err) => Err(RPCError::internal_error(&err.to_string())),
        }
    } else {
        Err(RPCError::internal_error(
            "This method is not available because the `send_tx_methods` feature flag is disabled",
        ))
    }
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn broadcast_tx_commit(
    data: Data<ServerContext>,
    Params(params): Params<serde_json::Value>,
) -> Result<near_jsonrpc::primitives::types::transactions::RpcTransactionResponse, RPCError> {
    tracing::debug!("`broadcast_tx_commit` call. Params: {:?}", params);
    if cfg!(feature = "send_tx_methods") {
        let tx_commit_request =
            near_jsonrpc::primitives::types::transactions::RpcSendTransactionRequest::parse(
                params,
            )?;
        let proxy_params =
            near_jsonrpc_client::methods::broadcast_tx_commit::RpcBroadcastTxCommitRequest {
                signed_transaction: tx_commit_request.signed_transaction,
            };
        let result = data.near_rpc_client.call(proxy_params).await?;
        Ok(
            near_jsonrpc::primitives::types::transactions::RpcTransactionResponse {
                final_execution_outcome: Some(FinalExecutionOutcome(result)),
                final_execution_status: near_primitives::views::TxExecutionStatus::Final,
            },
        )
    } else {
        Err(RPCError::internal_error(
            "This method is not available because the `send_tx_methods` feature flag is disabled",
        ))
    }
}

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(data, transaction_info))
)]
async fn tx_status_common(
    data: &Data<ServerContext>,
    transaction_info: &near_jsonrpc::primitives::types::transactions::TransactionInfo,
    fetch_receipt: bool,
    method_name: &str,
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

    let transaction_details = data
        .db_manager
        .get_transaction_by_hash(&tx_hash.to_string(), method_name)
        .await
        .map_err(|_err| {
            near_jsonrpc::primitives::types::transactions::RpcTransactionError::UnknownTransaction {
                requested_transaction_hash: tx_hash,
            }
        })?;

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
