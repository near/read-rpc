use crate::{TestResult, TxInfo};
use futures::future::join_all;
use near_jsonrpc_client::{methods, JsonRpcClient};
use near_jsonrpc_primitives::types::transactions::TransactionInfo;
use std::time::{Duration, Instant};

async fn get_random_transaction(tx: &TxInfo, client: &JsonRpcClient) -> anyhow::Result<Duration> {
    let now = Instant::now();
    // I'll always use shard 0 because it should always exist and it should be enough for perf test
    let _ = client
        .call(methods::tx::RpcTransactionStatusRequest {
            transaction_info: TransactionInfo::TransactionId {
                tx_hash: tx.hash,
                sender_account_id: tx.sender_id.clone(),
            },
        })
        .await?;
    let elapsed = now.elapsed();
    tracing::info!(
        target: crate::TARGET,
        "tx {} account {}",
        tx.hash,
        tx.sender_id
    );
    Ok(elapsed)
}

pub(crate) async fn test_transactions(
    name: &str,
    rpc_client: &JsonRpcClient,
    transactions: &[TxInfo],
) -> TestResult {
    let transactions_elapsed = join_all(
        transactions
            .iter()
            .map(|tx| get_random_transaction(tx, rpc_client)),
    )
    .await;
    crate::collect_perf_test_results(name, &transactions_elapsed)
}
