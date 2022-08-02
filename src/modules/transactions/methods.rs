use jsonrpc_v2::{Error, Params};
use serde_json::Value;

pub async fn send_tx_async(
    // Params(params): Params<near_jsonrpc_primitives::types::transactions::RpcBroadcastTransactionRequest>
    Params(_params): Params<Value>,
) -> Result<
    Value,
    // CryptoHash,
    Error,
> {
    unreachable!("This method is not implemented yet")
}

pub async fn send_tx_commit(
    // Params(params): Params<near_jsonrpc_primitives::types::transactions::RpcBroadcastTransactionRequest>
    Params(_params): Params<Value>,
) -> Result<
    near_jsonrpc_primitives::types::transactions::RpcTransactionResponse,
    // near_jsonrpc_primitives::types::transactions::RpcTransactionError,
    Error,
> {
    unreachable!("This method is not implemented yet")
}

pub async fn tx_status_common(
    Params(_params): Params<
        // near_jsonrpc_primitives::types::transactions::RpcTransactionStatusCommonRequest,
        Value,
    >,
) -> Result<
    near_jsonrpc_primitives::types::transactions::RpcTransactionResponse,
    // near_jsonrpc_primitives::types::transactions::RpcTransactionError,
    Error,
> {
    unreachable!("This method is not implemented yet")
}
