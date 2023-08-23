use crate::{TestResult, TxInfo};
use futures::future::join_all;
use near_jsonrpc_client::{methods, JsonRpcClient};
use near_primitives::types::{BlockId, BlockReference};
use near_primitives::views::QueryRequest::ViewAccount;
use std::time::{Duration, Instant};

async fn get_random_account(tx: &TxInfo, client: &JsonRpcClient) -> anyhow::Result<Duration> {
    let now = Instant::now();
    let _ = client
        .call(methods::query::RpcQueryRequest {
            block_reference: BlockReference::BlockId(BlockId::Height(tx.block_height)),
            request: ViewAccount {
                account_id: tx.sender_id.clone(),
            },
        })
        .await?;
    let elapsed = now.elapsed();
    Ok(elapsed)
}

pub(crate) async fn test_accounts(
    name: &str,
    rpc_client: &JsonRpcClient,
    transactions: &[TxInfo],
) -> TestResult {
    let accounts_elapsed = join_all(
        transactions
            .iter()
            .map(|tx| get_random_account(tx, rpc_client)),
    )
    .await;
    crate::collect_perf_test_results(name, &accounts_elapsed)
}
