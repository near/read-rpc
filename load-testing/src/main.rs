mod chunks;
mod query_accounts;
mod query_call_functions;
mod transactions;

use std::iter::zip;
use std::time::Duration;

use chrono::prelude::*;
use dotenv::dotenv;
use futures::join;
use rand::Rng;

use near_jsonrpc_client::{methods, JsonRpcClient};
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, BlockHeight, BlockReference, Finality};

// TODO fn call

struct TestResult {
    name: String,
    median: u128,
    errors_count: usize,
}

struct TxInfo {
    hash: CryptoHash,
    block_height: BlockHeight,
    sender_id: AccountId,
}

const RUNS_COUNT: usize = 100;
const TARGET: &str = "rpc_load_test";

fn collect_perf_test_results(name: &str, results: &[anyhow::Result<Duration>]) -> TestResult {
    let mut elapsed_timings: Vec<&Duration> =
        results.iter().filter_map(|r| r.as_ref().ok()).collect();
    elapsed_timings.sort();
    println!(
        "Finalising results for {}: {} out of {} requests are successful",
        name,
        elapsed_timings.len(),
        results.len()
    );
    let median = if elapsed_timings.is_empty() {
        u128::MAX
    } else {
        elapsed_timings[elapsed_timings.len() / 2].as_millis()
    };
    TestResult {
        name: name.to_string(),
        median,
        errors_count: results.len() - elapsed_timings.len(),
    }
}

fn generate_heights(from_height: BlockHeight, to_height: BlockHeight, n: usize) -> Vec<u64> {
    (0..n)
        .map(|_| rand::thread_rng().gen_range(from_height..to_height))
        .collect()
}

async fn test(rpc_url: &str, name: &str) -> Vec<TestResult> {
    let mut results = vec![];
    let rpc_client = JsonRpcClient::connect(rpc_url);

    // Hardcoding mainnet first after genesis block
    let genesis_block_height = 9820214;
    let final_block = rpc_client
        .call(methods::block::RpcBlockRequest {
            block_reference: BlockReference::Finality(Finality::Final),
        })
        .await
        .unwrap_or_else(|_| panic!("Unable to query final block from {}", name));
    let epoch_len = 43200;

    let final_block_dt =
        NaiveDateTime::from_timestamp_micros(final_block.header.timestamp as i64 / 1000)
            .unwrap_or_else(|| panic!("Unable to parse final block timestamp from {}", name));

    println!(
        "{} current block_height {} ({:?})\n",
        name, final_block.header.height, final_block_dt
    );

    // Hot storage contains 5 last epochs, but I will ignore 2 boundary epochs to be sure the results are not spoiled
    let final_cold_storage_block = final_block.header.height - epoch_len * 6;
    println!(
        "{} cold storage has block range [{}, {}]",
        name, genesis_block_height, final_cold_storage_block
    );
    let cold_storage_heights: Vec<u64> =
        generate_heights(genesis_block_height, final_cold_storage_block, RUNS_COUNT);

    // I'm microoptimising and preparing data for next tests during running the other tests
    // (OFK it does not affect the benchmarks)
    let (cold_storage_chunks_result, cold_storage_transactions) = chunks::test_chunks(
        &format!("{} cold storage chunks", name),
        &rpc_client,
        &cold_storage_heights,
    )
    .await;
    results.push(cold_storage_chunks_result);
    results.push(
        transactions::test_transactions(
            &format!("{} cold storage transactions", name),
            &rpc_client,
            &cold_storage_transactions,
        )
        .await,
    );
    results.push(
        query_accounts::test_accounts(
            &format!("{} cold storage accounts", name),
            &rpc_client,
            &cold_storage_transactions,
        )
        .await,
    );
    results.push(
        query_call_functions::test_call_functions(
            &format!("{} cold storage function calls", name),
            &rpc_client,
            genesis_block_height,
            final_cold_storage_block,
            RUNS_COUNT,
        )
        .await,
    );

    // Hot storage contains 5 last epochs, but I will ignore 2 boundary epochs to be sure the results are not spoiled
    let first_hot_storage_block = final_block.header.height - epoch_len * 4;
    println!(
        "{} hot storage has block range [{}, {}]",
        name, first_hot_storage_block, final_block.header.height
    );
    let hot_storage_heights: Vec<u64> = generate_heights(
        first_hot_storage_block,
        final_block.header.height,
        RUNS_COUNT,
    );

    let (hot_storage_chunks_result, hot_storage_transactions) = chunks::test_chunks(
        &format!("{} hot storage chunks", name),
        &rpc_client,
        &hot_storage_heights,
    )
    .await;
    results.push(hot_storage_chunks_result);
    results.push(
        transactions::test_transactions(
            &format!("{} hot storage transactions", name),
            &rpc_client,
            &hot_storage_transactions,
        )
        .await,
    );
    results.push(
        query_accounts::test_accounts(
            &format!("{} hot storage accounts", name),
            &rpc_client,
            &hot_storage_transactions,
        )
        .await,
    );
    results.push(
        query_call_functions::test_call_functions(
            &format!("{} hot storage function calls", name),
            &rpc_client,
            first_hot_storage_block,
            final_block.header.height,
            RUNS_COUNT,
        )
        .await,
    );

    results
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    let read_rpc_url = std::env::var("READ_RPC_URL").expect("READ_RPC_URL env var expected");
    let archival_rpc_url =
        std::env::var("ARCHIVAL_RPC_URL").expect("ARCHIVAL_RPC_URL env var expected");
    let (rr_results, ar_results) = join!(test(&read_rpc_url, "RR"), test(&archival_rpc_url, "AR"));
    println!("Read RPC (errors)\tArchival RPC (errors)");
    for (rr_result, ar_result) in zip(rr_results, ar_results) {
        println!(
            "{} ms ({}) \t\t\t {} ms ({})\t\t\t\t{}/{}",
            rr_result.median,
            rr_result.errors_count,
            ar_result.median,
            ar_result.errors_count,
            rr_result.name,
            ar_result.name,
        );
    }
    Ok(())
}
