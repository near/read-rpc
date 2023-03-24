use crate::config::ServerContext;
use crate::errors::RPCError;
use crate::modules::transactions::{
    parse_signed_transaction, parse_transaction_status_common_request,
};
use crate::utils::proxy_rpc_call;
use borsh::BorshDeserialize;
use jsonrpc_v2::{Data, Params};
use near_primitives::views::FinalExecutionOutcomeViewEnum::{
    FinalExecutionOutcome, FinalExecutionOutcomeWithReceipt,
};
use serde_json::Value;

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn send_tx_async(
    data: Data<ServerContext>,
    Params(params): Params<Value>,
) -> Result<near_primitives::hash::CryptoHash, RPCError> {
    tracing::debug!("`send_tx_async` call. Params: {:?}", params);
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
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn send_tx_commit(
    data: Data<ServerContext>,
    Params(params): Params<Value>,
) -> Result<near_jsonrpc_primitives::types::transactions::RpcTransactionResponse, RPCError> {
    tracing::debug!("`send_tx_commit` call. Params: {:?}", params);
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
                final_execution_outcome:
                    near_primitives::views::FinalExecutionOutcomeViewEnum::FinalExecutionOutcome(
                        resp,
                    ),
            },
        ),
        Err(err) => Err(RPCError::from(err)),
    }
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn tx_status_common(
    data: &Data<ServerContext>,
    transaction_info: &near_jsonrpc_primitives::types::transactions::TransactionInfo,
    fetch_receipt: bool,
) -> anyhow::Result<near_primitives::views::FinalExecutionOutcomeViewEnum> {
    tracing::debug!("`tx_status_common` call.");
    let (tx_hash, account_id) = match &transaction_info {
        near_jsonrpc_primitives::types::transactions::TransactionInfo::Transaction(tx) => {
            (tx.get_hash(), tx.transaction.signer_id.clone())
        }
        near_jsonrpc_primitives::types::transactions::TransactionInfo::TransactionId {
            hash,
            account_id,
        } => (*hash, account_id.clone()),
    };
    let row = data
        .scylla_db_manager
        .get_transaction_by_hash_and_account_id(tx_hash, account_id)
        .await?;
    let (data_value,): (Vec<u8>,) = row.into_typed::<(Vec<u8>,)>()?;
    let transaction: readnode_primitives::TransactionDetails =
        readnode_primitives::TransactionDetails::try_from_slice(&data_value)?;
    if fetch_receipt {
        Ok(FinalExecutionOutcomeWithReceipt(
            transaction.to_final_execution_outcome_with_receipt(),
        ))
    } else {
        Ok(FinalExecutionOutcome(
            transaction.to_final_execution_outcome(),
        ))
    }
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn tx(
    data: Data<ServerContext>,
    Params(params): Params<Value>,
) -> Result<near_jsonrpc_primitives::types::transactions::RpcTransactionResponse, RPCError> {
    tracing::debug!("`tx` call. Params: {:?}", params);
    match parse_transaction_status_common_request(params.clone()).await {
        Ok(request) => match tx_status_common(&data, &request.transaction_info, false).await {
            Ok(transaction) => Ok(
                near_jsonrpc_primitives::types::transactions::RpcTransactionResponse {
                    final_execution_outcome: transaction,
                },
            ),
            Err(err) => {
                tracing::debug!("Transaction not found: {:#?}", err);
                let resp = proxy_rpc_call(
                    &data.near_rpc_client,
                    near_jsonrpc_client::methods::tx::RpcTransactionStatusRequest {
                        transaction_info: request.transaction_info,
                    },
                )
                .await?;
                Ok(
                    near_jsonrpc_primitives::types::transactions::RpcTransactionResponse {
                        final_execution_outcome: FinalExecutionOutcome(resp),
                    },
                )
            }
        },
        Err(err) => {
            tracing::debug!("Transaction pars params error: {:#?}", err);
            Err(RPCError::parse_error(&err.to_string()))
        }
    }
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn tx_status(
    data: Data<ServerContext>,
    Params(params): Params<Value>,
) -> Result<near_jsonrpc_primitives::types::transactions::RpcTransactionResponse, RPCError> {
    tracing::debug!("`tx_status` call. Params: {:?}", params);
    match parse_transaction_status_common_request(params.clone()).await {
        Ok(request) => match tx_status_common(&data, &request.transaction_info, true).await {
            Ok(transaction) => Ok(
                near_jsonrpc_primitives::types::transactions::RpcTransactionResponse {
                    final_execution_outcome: transaction,
                },
            ),
            Err(err) => {
                tracing::debug!("Transaction not found: {:#?}", err);
                let rep = proxy_rpc_call(
                        &data.near_rpc_client,
                        near_jsonrpc_client::methods::EXPERIMENTAL_tx_status::RpcTransactionStatusRequest{
                            transaction_info: request.transaction_info
                        },
                    ).await?;
                Ok(
                    near_jsonrpc_primitives::types::transactions::RpcTransactionResponse {
                        final_execution_outcome: FinalExecutionOutcomeWithReceipt(rep),
                    },
                )
            }
        },
        Err(err) => {
            tracing::debug!("Transaction pars params error: {:#?}", err);
            Err(RPCError::parse_error(&err.to_string()))
        }
    }
}
