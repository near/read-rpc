use crate::TestResult;
use futures::future::join_all;
use near_jsonrpc_client::{methods, JsonRpcClient};
use near_primitives::borsh::BorshSerialize;
use near_primitives::types::{AccountId, BlockHeight, BlockId, BlockReference, FunctionArgs};
use near_primitives::views::QueryRequest::CallFunction;
use std::time::{Duration, Instant};

#[derive(Debug, PartialEq, Eq, Clone)]
struct CallFnInfo {
    contract_id: AccountId,
    start_block_height: BlockHeight,
    method_name: String,
    args: FunctionArgs,
}

async fn send_random_fn_call(
    function: &CallFnInfo,
    block_height: BlockHeight,
    client: &JsonRpcClient,
) -> anyhow::Result<Duration> {
    let now = Instant::now();
    let _ = client
        .call(methods::query::RpcQueryRequest {
            block_reference: BlockReference::BlockId(BlockId::Height(block_height)),
            request: CallFunction {
                account_id: function.contract_id.clone(),
                method_name: function.method_name.clone(),
                args: function.args.clone(),
            },
        })
        .await?;
    let elapsed = now.elapsed();
    tracing::info!(
        target: crate::TARGET,
        "fn call at block {} Details: {:#?}",
        block_height,
        function,
    );
    Ok(elapsed)
}

pub(crate) async fn test_call_functions(
    name: &str,
    rpc_client: &JsonRpcClient,
    from_block_height: BlockHeight,
    to_block_height: BlockHeight,
    calls_count: usize,
) -> TestResult {
    // We can't automate the search of read-only fn calls, so let's hardcode some of them
    let items = vec![
        CallFnInfo {
            contract_id: "wrap.near".parse().unwrap(),
            start_block_height: 30000000,
            method_name: "ft_balance_of".to_string(),
            args: "{\"account_id\": \"olga.near\"}"
                .try_to_vec()
                .unwrap()
                .into(),
        },
        CallFnInfo {
            contract_id: "qbit.poolv1.near".parse().unwrap(),
            start_block_height: 60000000,
            method_name: "get_account_total_balance".to_string(),
            args: "{\"account_id\": \"frol.near\"}"
                .try_to_vec()
                .unwrap()
                .into(),
        },
        CallFnInfo {
            contract_id: "thebullishbulls.near".parse().unwrap(),
            start_block_height: 70000000,
            method_name: "nft_token".to_string(),
            args: "{\"token_id\": \"1394\"}".try_to_vec().unwrap().into(),
        },
    ];
    let fn_calls_elapsed: Vec<anyhow::Result<Duration>> = join_all(items.iter().map(|f| async {
        join_all(
            crate::generate_heights(
                std::cmp::max(from_block_height, f.start_block_height),
                to_block_height,
                calls_count / items.len(),
            )
            .iter()
            .map(|h| async { send_random_fn_call(f, *h, rpc_client).await }),
        )
        .await
    }))
    .await
    .into_iter()
    .flatten()
    .collect();
    crate::collect_perf_test_results(name, &fn_calls_elapsed)
}
