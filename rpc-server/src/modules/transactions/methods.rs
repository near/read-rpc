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
    data.near_rpc_client
        .call(request_data, Some("send_tx"))
        .await
        .map_err(|err| {
            err.handler_error().cloned().unwrap_or(
                near_jsonrpc::primitives::types::transactions::RpcTransactionError::InternalError {
                    debug_info: err.to_string(),
                },
            )
        })
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
    let (tx_hash, sender_id) = match &transaction_info {
        near_jsonrpc::primitives::types::transactions::TransactionInfo::Transaction(
            near_jsonrpc::primitives::types::transactions::SignedTransaction::SignedTransaction(tx),
        ) => (tx.get_hash(), tx.transaction.signer_id().clone()),
        near_jsonrpc::primitives::types::transactions::TransactionInfo::TransactionId {
            tx_hash,
            sender_account_id,
        } => (*tx_hash, sender_account_id.clone()),
    };

    let shard_id = data.db_manager.get_shard_id_by_account_id(&sender_id);

    let transaction_details = super::try_get_transaction_details_by_hash(data, &tx_hash, &shard_id)
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

/// Emulates the execution of a transaction by processing its actions and collecting the results.
///
/// This function takes a signed transaction request, iterates over its actions, and processes
/// each supported action (currently only `FunctionCall` actions are supported). It collects
/// the results of these actions into a vector of `EmulateTransactionResponse`. Additionally,
/// it processes any cross-contract actions collected during the emulation and appends their
/// results as well.
///
/// # Arguments
///
/// * `data` - Shared server context containing dependencies and state.
/// * `request_data` - The transaction request containing the signed transaction to emulate.
///
/// # Returns
///
/// Returns a `Result` containing a vector of `EmulateTransactionResponse` on success,
/// or an `RpcError` if an error occurs during emulation.
pub async fn emulate_tx(
    data: Data<ServerContext>,
    request_data: near_jsonrpc::primitives::types::transactions::RpcSendTransactionRequest,
) -> Result<
    crate::modules::transactions::EmulateTransactionResponse,
    near_jsonrpc::primitives::errors::RpcError,
> {
    // Extracts the signer account ID from the signed transaction, prepares a results vector,
    // and initializes a transaction actions collector. Iterates over each action in the transaction,
    // processing only `FunctionCall` actions by invoking the function call processor and collecting
    // the results. Unsupported actions are logged for debugging purposes.
    let account_id = request_data.signed_transaction.transaction.signer_id();
    let receiver_id = request_data.signed_transaction.transaction.receiver_id();
    let block = data.blocks_info_by_finality.final_block_view().await;
    let store = near_parameters::RuntimeConfigStore::for_chain_id(
        &data.genesis_info.genesis_config.chain_id,
    );
    let protocol_version = data
        .blocks_info_by_finality
        .current_protocol_version()
        .await;
    let runtime_config = store.get_config(protocol_version);
    let results = actions_call(
        &data,
        account_id,
        receiver_id,
        &request_data
            .signed_transaction
            .transaction
            .actions()
            .to_vec(),
        runtime_config,
    )
    .await?;

    Ok(crate::modules::transactions::EmulateTransactionResponse {
        results,
        block_height: block.header.height,
        gas_price: block.header.gas_price,
    })
}

/// Processes a list of transaction actions, emulating their execution and collecting results.
///
/// For each action in `tx_actions`, if it is a `FunctionCall`, this function processes the call,
/// collects the outcome and fee, and recursively processes any cross-contract actions collected
/// during the emulation. For other action types, it computes the fee and collects the result.
///
/// # Arguments
/// * `data` - Shared server context.
/// * `account_id` - The signer account ID.
/// * `receiver_id` - The receiver account ID.
/// * `tx_actions` - The list of actions to emulate.
/// * `runtime_config` - The runtime configuration for fee calculation.
///
/// # Returns
/// A `Result` containing a vector of `EmulateTransactionActionResult` on success,
/// or an `RpcError` if an error occurs.
pub async fn actions_call(
    data: &Data<ServerContext>,
    account_id: &near_primitives::types::AccountId,
    receiver_id: &near_primitives::types::AccountId,
    tx_actions: &Vec<near_primitives::transaction::Action>,
    runtime_config: &near_parameters::RuntimeConfig,
) -> Result<
    Vec<crate::modules::transactions::EmulateTransactionActionResult>,
    near_jsonrpc::primitives::errors::RpcError,
> {
    let tx_actions_collector =
        std::sync::Arc::new(crate::modules::transactions::TxActionsCollector::new());
    let mut results = vec![];
    for tx_action in tx_actions {
        match tx_action {
            near_primitives::transaction::Action::FunctionCall(action) => {
                let method_name = action.method_name.clone();
                let args = action.args.clone();
                let block = data.blocks_info_by_finality.final_block_view().await;
                let call_results = crate::modules::queries::methods::process_function_call(
                    data,
                    &block,
                    account_id,
                    &method_name,
                    &args.into(),
                    false,
                    Some(tx_actions_collector.clone()),
                    true,
                )
                .await?;

                let fee = node_runtime::config::exec_fee(runtime_config, tx_action, receiver_id);
                results.push(
                    crate::modules::transactions::EmulateTransactionActionResult::FunctionCall {
                        outcome: Box::new(call_results.into()),
                        fee,
                    },
                );
                let cross_results = Box::pin(actions_call(
                    data,
                    account_id,
                    receiver_id,
                    &tx_actions_collector.get_actions().await,
                    runtime_config,
                ))
                .await?;
                results.extend(cross_results);
            }
            _ => {
                let fee = node_runtime::config::exec_fee(runtime_config, tx_action, receiver_id);
                results.push(
                    crate::modules::transactions::EmulateTransactionActionResult::from_tx_action(
                        tx_action, fee,
                    ),
                );
            }
        }
    }
    Ok(results)
}
