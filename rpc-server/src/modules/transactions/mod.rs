pub mod methods;
use borsh::BorshDeserialize;
use serde_json::Value;

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(value)))]
pub async fn parse_transaction_status_common_request(
    value: Value,
) -> anyhow::Result<near_jsonrpc_primitives::types::transactions::RpcTransactionStatusCommonRequest>
{
    tracing::debug!("`parse_transaction_status_common_request` call.");
    if let Ok((hash, account_id)) = serde_json::from_value::<(
        near_primitives::hash::CryptoHash,
        near_primitives::types::AccountId,
    )>(value.clone())
    {
        let transaction_info =
            near_jsonrpc_primitives::types::transactions::TransactionInfo::TransactionId {
                hash,
                account_id,
            };
        Ok(
            near_jsonrpc_primitives::types::transactions::RpcTransactionStatusCommonRequest {
                transaction_info,
            },
        )
    } else {
        let signed_transaction = parse_signed_transaction(value).await?;
        let transaction_info =
            near_jsonrpc_primitives::types::transactions::TransactionInfo::Transaction(
                signed_transaction,
            );
        Ok(
            near_jsonrpc_primitives::types::transactions::RpcTransactionStatusCommonRequest {
                transaction_info,
            },
        )
    }
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(value)))]
pub async fn parse_signed_transaction(
    value: Value,
) -> anyhow::Result<near_primitives::transaction::SignedTransaction> {
    tracing::debug!("`parse_signed_transaction` call.");
    let (encoded,) = serde_json::from_value::<(String,)>(value)?;
    let bytes = near_primitives::serialize::from_base64(&encoded).unwrap();
    Ok(near_primitives::transaction::SignedTransaction::try_from_slice(&bytes)?)
}
