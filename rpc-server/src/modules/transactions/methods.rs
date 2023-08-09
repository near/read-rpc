use crate::config::ServerContext;
use crate::errors::RPCError;
use crate::modules::transactions::{
    parse_signed_transaction, parse_transaction_status_common_request,
};
use crate::utils::proxy_rpc_call;
#[cfg(feature = "shadow_data_consistency")]
use crate::utils::shadow_compare_results;
use jsonrpc_v2::{Data, Params};
use near_primitives::views::FinalExecutionOutcomeViewEnum::{
    FinalExecutionOutcome, FinalExecutionOutcomeWithReceipt,
};
use serde_json::Value;

/// Queries status of a transaction by hash and returns the final transaction result.
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn tx(
    data: Data<ServerContext>,
    Params(params): Params<Value>,
) -> Result<near_jsonrpc_primitives::types::transactions::RpcTransactionResponse, RPCError> {
    tracing::debug!("`tx` call. Params: {:?}", params);
    crate::metrics::TX_REQUESTS_TOTAL.inc();

    let tx_status_request = parse_transaction_status_common_request(params.clone()).await?;

    let result = tx_status_common(&data, &tx_status_request.transaction_info, false).await;

    #[cfg(feature = "shadow_data_consistency")]
    {
        let near_rpc_client = data.near_rpc_client.clone();
        let error_meta = format!("TX: {:?}", params);
        let (read_rpc_response_json, is_response_ok) = match &result {
            Ok(res) => (serde_json::to_value(res), true),
            Err(err) => (serde_json::to_value(err), false),
        };

        let comparison_result = shadow_compare_results(
            read_rpc_response_json,
            near_rpc_client,
            // Note there is a difference in the implementation of the `tx` method in the `near_jsonrpc_client`
            // The method is `near_jsonrpc_client::methods::tx::RpcTransactionStatusRequest` in the client
            // so we can't just pass `params` there, instead we need to craft a request manually
            near_jsonrpc_client::methods::tx::RpcTransactionStatusRequest {
                transaction_info: tx_status_request.transaction_info,
            },
            is_response_ok,
        )
        .await;

        match comparison_result {
            Ok(_) => {
                tracing::info!(target: "shadow_data_consistency", "Shadow data check: CORRECT\n{}", error_meta);
            }
            Err(err) => {
                crate::utils::capture_shadow_consistency_error!(err, error_meta, "TX");
            }
        }
    }

    Ok(result.map_err(near_jsonrpc_primitives::errors::RpcError::from)?)
}

/// Queries status of a transaction by hash, returning the final transaction result and details of all receipts.
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn tx_status(
    data: Data<ServerContext>,
    Params(params): Params<Value>,
) -> Result<near_jsonrpc_primitives::types::transactions::RpcTransactionResponse, RPCError> {
    tracing::debug!("`tx_status` call. Params: {:?}", params);
    crate::metrics::TX_STATUS_REQUESTS_TOTAL.inc();

    let tx_status_request = parse_transaction_status_common_request(params.clone()).await?;

    let result = tx_status_common(&data, &tx_status_request.transaction_info, true).await;

    #[cfg(feature = "shadow_data_consistency")]
    {
        let near_rpc_client = data.near_rpc_client.clone();
        let error_meta = format!("EXPERIMENTAL_TX_STATUS: {:?}", params);

        let (read_rpc_response_json, is_response_ok) = match &result {
            Ok(res) => (serde_json::to_value(res), true),
            Err(err) => (serde_json::to_value(err), false),
        };

        let comparison_result = shadow_compare_results(
            read_rpc_response_json,
            near_rpc_client,
            // Note there is a difference in the implementation of the `EXPERIMENTAL_tx_status` method in the `near_jsonrpc_client`
            // The method is `near_jsonrpc_client::methods::EXPERIMENTAL_tx_status` in the client
            // so we can't just pass `params` there, instead we need to craft a request manually
            near_jsonrpc_client::methods::EXPERIMENTAL_tx_status::RpcTransactionStatusRequest {
                transaction_info: tx_status_request.transaction_info,
            },
            is_response_ok,
        )
        .await;

        match comparison_result {
            Ok(_) => {
                tracing::info!(target: "shadow_data_consistency", "Shadow data check: CORRECT\n{}", error_meta);
            }
            Err(err) => {
                crate::utils::capture_shadow_consistency_error!(
                    err,
                    error_meta,
                    "EXPERIMENTAL_TX_STATUS"
                );
            }
        }
    }

    Ok(result.map_err(near_jsonrpc_primitives::errors::RpcError::from)?)
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn send_tx_async(
    data: Data<ServerContext>,
    Params(params): Params<Value>,
) -> Result<near_primitives::hash::CryptoHash, RPCError> {
    tracing::debug!("`send_tx_async` call. Params: {:?}", params);
    if cfg!(feature = "send_tx_methods") {
        let signed_transaction = match parse_signed_transaction(params).await {
            Ok(signed_transaction) => signed_transaction,
            Err(err) => return Err(RPCError::parse_error(&err.to_string())),
        };
        let proxy_params =
            near_jsonrpc_client::methods::broadcast_tx_async::RpcBroadcastTxAsyncRequest {
                signed_transaction,
            };
        match proxy_rpc_call(&data.near_rpc_client, proxy_params).await {
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
pub async fn send_tx_commit(
    data: Data<ServerContext>,
    Params(params): Params<Value>,
) -> Result<near_jsonrpc_primitives::types::transactions::RpcTransactionResponse, RPCError> {
    tracing::debug!("`send_tx_commit` call. Params: {:?}", params);
    if cfg!(feature = "send_tx_methods") {
        let signed_transaction = match parse_signed_transaction(params).await {
            Ok(signed_transaction) => signed_transaction,
            Err(err) => return Err(RPCError::parse_error(&err.to_string())),
        };
        let proxy_params =
            near_jsonrpc_client::methods::broadcast_tx_commit::RpcBroadcastTxCommitRequest {
                signed_transaction,
            };
        match proxy_rpc_call(&data.near_rpc_client, proxy_params).await {
            Ok(resp) => Ok(
                near_jsonrpc_primitives::types::transactions::RpcTransactionResponse {
                    final_execution_outcome: FinalExecutionOutcome(resp),
                },
            ),
            Err(err) => Err(RPCError::from(err)),
        }
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
    transaction_info: &near_jsonrpc_primitives::types::transactions::TransactionInfo,
    fetch_receipt: bool,
) -> Result<
    near_jsonrpc_primitives::types::transactions::RpcTransactionResponse,
    near_jsonrpc_primitives::types::transactions::RpcTransactionError,
> {
    tracing::debug!("`tx_status_common` call.");
    let tx_hash = match &transaction_info {
        near_jsonrpc_primitives::types::transactions::TransactionInfo::Transaction(tx) => {
            tx.get_hash()
        }
        near_jsonrpc_primitives::types::transactions::TransactionInfo::TransactionId {
            hash,
            ..
        } => *hash,
    };

    let transaction_details = data
        .scylla_db_manager
        .get_transaction_by_hash(&tx_hash.to_string())
        .await
        .map_err(|_err| {
            near_jsonrpc_primitives::types::transactions::RpcTransactionError::UnknownTransaction {
                requested_transaction_hash: tx_hash,
            }
        })?;

    if fetch_receipt {
        Ok(
            near_jsonrpc_primitives::types::transactions::RpcTransactionResponse {
                final_execution_outcome: FinalExecutionOutcomeWithReceipt(
                    transaction_details.to_final_execution_outcome_with_receipts(),
                ),
            },
        )
    } else {
        Ok(
            near_jsonrpc_primitives::types::transactions::RpcTransactionResponse {
                final_execution_outcome: FinalExecutionOutcome(
                    transaction_details.to_final_execution_outcome(),
                ),
            },
        )
    }
}
