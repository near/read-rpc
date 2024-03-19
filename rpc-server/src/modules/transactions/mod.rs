pub mod methods;
use serde_json::Value;

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(value)))]
pub async fn parse_transaction_status_common_request(
    value: Value,
) -> Result<
    near_jsonrpc_primitives::types::transactions::RpcTransactionStatusRequest,
    near_jsonrpc_primitives::errors::RpcError,
> {
    tracing::debug!("`parse_transaction_status_common_request` call.");
    if let Ok(request) = serde_json::from_value::<
        near_jsonrpc_primitives::types::transactions::RpcTransactionStatusRequest,
    >(value.clone())
    {
        Ok(request)
    } else if let Ok((tx_hash, sender_account_id)) = serde_json::from_value::<(
        near_primitives::hash::CryptoHash,
        near_primitives::types::AccountId,
    )>(value.clone())
    {
        let transaction_info =
            near_jsonrpc_primitives::types::transactions::TransactionInfo::TransactionId {
                tx_hash,
                sender_account_id,
            };
        Ok(
            near_jsonrpc_primitives::types::transactions::RpcTransactionStatusRequest {
                transaction_info,
                wait_until: near_primitives::views::TxExecutionStatus::Final,
            },
        )
    } else {
        let signed_transaction = parse_signed_transaction(value).await.map_err(|err| {
            near_jsonrpc_primitives::errors::RpcError::parse_error(err.to_string())
        })?;

        let transaction_info =
            near_jsonrpc_primitives::types::transactions::TransactionInfo::Transaction(
                near_jsonrpc_primitives::types::transactions::SignedTransaction::SignedTransaction(
                    signed_transaction,
                ),
            );
        Ok(
            near_jsonrpc_primitives::types::transactions::RpcTransactionStatusRequest {
                transaction_info,
                wait_until: near_primitives::views::TxExecutionStatus::Final,
            },
        )
    }
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(value)))]
pub async fn parse_signed_transaction(
    value: Value,
) -> anyhow::Result<near_primitives::transaction::SignedTransaction> {
    tracing::debug!("`parse_signed_transaction` call.");
    if let Ok(signed_transaction) =
        serde_json::from_value::<near_primitives::transaction::SignedTransaction>(value.clone())
    {
        Ok(signed_transaction)
    } else {
        let (encoded,) = serde_json::from_value::<(String,)>(value)?;
        let bytes = near_primitives::serialize::from_base64(&encoded)?;
        Ok(borsh::from_slice::<
            near_primitives::transaction::SignedTransaction,
        >(&bytes)?)
    }
}
