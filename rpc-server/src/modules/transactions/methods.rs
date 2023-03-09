use crate::config::ServerContext;
use crate::errors::RPCError;
use crate::utils::proxy_rpc_call;
use borsh::BorshDeserialize;
use jsonrpc_v2::{Data, Params};
use serde_json::Value;

async fn parse_signed_transaction_base64(
    params: Vec<Value>,
) -> anyhow::Result<near_primitives::transaction::SignedTransaction> {
    match params.first().cloned() {
        Some(val) => {
            let encoded: String = serde_json::from_value(val)?;
            let bytes: Vec<u8> = near_primitives::serialize::from_base64(&encoded).unwrap();
            let signed_transaction =
                near_primitives::transaction::SignedTransaction::try_from_slice(&bytes)?;
            Ok(signed_transaction)
        }
        None => anyhow::bail!("Invalid params"),
    }
}

pub async fn send_tx_async(
    data: Data<ServerContext>,
    Params(params): Params<Vec<Value>>,
) -> Result<near_primitives::hash::CryptoHash, RPCError> {
    tracing::debug!("`send_tx_async` call. Params: {:?}", params);
    let signed_transaction = match parse_signed_transaction_base64(params).await {
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

pub async fn send_tx_commit(
    data: Data<ServerContext>,
    Params(params): Params<Vec<Value>>,
) -> Result<near_jsonrpc_primitives::types::transactions::RpcTransactionResponse, RPCError> {
    tracing::debug!("`send_tx_commit` call. Params: {:?}", params);
    let signed_transaction = match parse_signed_transaction_base64(params).await {
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

pub async fn tx_status_common(
    Params(_params): Params<
        // near_jsonrpc_primitives::types::transactions::RpcTransactionStatusCommonRequest,
        Value,
    >,
) -> Result<
    near_jsonrpc_primitives::types::transactions::RpcTransactionResponse,
    // near_jsonrpc_primitives::types::transactions::RpcTransactionError,
    RPCError,
> {
    Err(RPCError::unimplemented_error(
        "This method is not implemented yet",
    ))
}
