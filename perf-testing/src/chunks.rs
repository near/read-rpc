use std::time::{Duration, Instant};

use futures::future::join_all;
use near_jsonrpc_client::{methods, JsonRpcClient};
use near_jsonrpc_primitives::types::chunks::ChunkReference;
use near_primitives::types::{BlockHeight, BlockId};

use crate::{TestResult, TxInfo};

// While testing chunks, it's important to collect accounts and transactions for further tests
async fn get_random_chunk(
    random_block_height: BlockHeight,
    client: &JsonRpcClient,
) -> anyhow::Result<(Duration, Vec<TxInfo>)> {
    let now = Instant::now();
    // I'll always use shard 0 because it should always exist and it should be enough for perf test
    let chunk = client
        .call(methods::chunk::RpcChunkRequest {
            chunk_reference: ChunkReference::BlockShardId {
                block_id: BlockId::Height(random_block_height),
                shard_id: 0,
            },
        })
        .await?;
    let elapsed = now.elapsed();

    let transactions: Vec<TxInfo> = chunk
        .transactions
        .iter()
        .map(|tx| TxInfo {
            hash: tx.hash,
            block_height: random_block_height,
            sender_id: tx.signer_id.clone(),
        })
        .collect();

    tracing::info!(
        target: crate::TARGET,
        "block {} tx count {}",
        random_block_height,
        transactions.len()
    );
    Ok((elapsed, transactions))
}

pub(crate) async fn test_chunks(
    name: &str,
    rpc_client: &JsonRpcClient,
    heights: &[u64],
) -> (TestResult, Vec<TxInfo>) {
    let chunks = join_all(
        heights
            .iter()
            .map(|height| get_random_chunk(*height, rpc_client)),
    )
    .await;
    let mut results: Vec<anyhow::Result<Duration>> = vec![];
    let mut transactions: Vec<TxInfo> = vec![];
    for chunk in chunks {
        match chunk {
            Ok((duration, tx)) => {
                results.push(Ok(duration));
                transactions.extend(tx);
            }
            Err(e) => {
                results.push(Err(e));
            }
        }
    }
    (
        crate::collect_perf_test_results(name, &results),
        transactions,
    )
}
