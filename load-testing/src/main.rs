mod chunks;
mod config;
mod query_accounts;
mod query_call_functions;
mod transactions;

use std::iter::zip;
use std::time::Duration;

use chrono::prelude::*;
use clap::Parser;
use dotenv::dotenv;
use futures::join;
use rand::Rng;

use crate::config::Opts;
use near_jsonrpc_client::{methods, JsonRpcClient};
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, BlockHeight, BlockReference, Finality};

struct TestResult {
    name: String,
    median: u128,
    success_count: usize,
    total_count: usize,
}

struct TxInfo {
    hash: CryptoHash,
    block_height: BlockHeight,
    sender_id: AccountId,
}

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
        success_count: elapsed_timings.len(),
        total_count: results.len(),
    }
}

fn generate_heights(from_height: BlockHeight, to_height: BlockHeight, n: usize) -> Vec<u64> {
    (0..n)
        .map(|_| rand::thread_rng().gen_range(from_height..to_height))
        .collect()
}

async fn test(rpc_url: &http::Uri, name: &str, queries_count: usize) -> Vec<TestResult> {
    let mut results = vec![];
    let rpc_client = JsonRpcClient::connect(rpc_url.to_string());

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
        "{} current block_height {} ({:?})",
        name, final_block.header.height, final_block_dt
    );

    // Hot storage contains 5 last epochs, but I will ignore 2 boundary epochs to be sure the results are not spoiled
    let final_cold_storage_block = final_block.header.height - epoch_len * 6;
    println!(
        "{} cold storage has block range [{}, {}]",
        name, genesis_block_height, final_cold_storage_block
    );
    let cold_storage_heights: Vec<u64> = generate_heights(
        genesis_block_height,
        final_cold_storage_block,
        queries_count,
    );
    let first_hot_storage_block = final_block.header.height - epoch_len * 4;
    println!(
        "{} hot storage has block range [{}, {}]",
        name, first_hot_storage_block, final_block.header.height
    );
    let hot_storage_heights: Vec<u64> = generate_heights(
        first_hot_storage_block,
        final_block.header.height,
        queries_count,
    );

    // I'm microoptimising and preparing data for next tests during running the other tests
    // (OFK it does not affect the benchmarks)
    let ((chunks_cold, transactions_cold), (chunks_hot, transactions_hot)) = join!(
        chunks::test_chunks("cold chunks", &rpc_client, &cold_storage_heights,),
        chunks::test_chunks("hot chunks", &rpc_client, &hot_storage_heights,)
    );
    results.push(chunks_cold);
    results.push(chunks_hot);

    let (tx_cold, tx_hot) = join!(
        transactions::test_transactions("cold transactions", &rpc_client, &transactions_cold,),
        transactions::test_transactions("hot transactions", &rpc_client, &transactions_hot,)
    );
    results.push(tx_cold);
    results.push(tx_hot);

    let (accounts_cold, accounts_hot) = join!(
        query_accounts::test_accounts("cold accounts", &rpc_client, &transactions_cold,),
        query_accounts::test_accounts("hot accounts", &rpc_client, &transactions_hot,)
    );
    results.push(accounts_cold);
    results.push(accounts_hot);

    let (fn_cold, fn_hot) = join!(
        query_call_functions::test_call_functions(
            "cold function calls",
            &rpc_client,
            genesis_block_height,
            final_cold_storage_block,
            queries_count,
        ),
        query_call_functions::test_call_functions(
            "hot function calls",
            &rpc_client,
            first_hot_storage_block,
            final_block.header.height,
            queries_count,
        )
    );
    results.push(fn_cold);
    results.push(fn_hot);

    results
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    let opts: Opts = Opts::parse();

    let (rr_results, ar_results) = join!(
        test(&opts.read_rpc_url, "RR", opts.queries_count_per_command),
        test(&opts.near_rpc_url, "AR", opts.queries_count_per_command)
    );
    println!("Read RPC (success/total)\tArchival RPC (success/total)");
    println!("-------------------------------------------");
    for (rr_result, ar_result) in zip(rr_results, ar_results) {
        assert_eq!(rr_result.name, ar_result.name);
        println!(
            "{} ms ({}/{})\t\t{} ms ({}/{})\t\t{}",
            rr_result.median,
            rr_result.success_count,
            rr_result.total_count,
            ar_result.median,
            ar_result.success_count,
            ar_result.total_count,
            rr_result.name,
        );
        if rr_result.name.starts_with("hot") {
            println!("-------------------------------------------");
        }
    }
    Ok(())
}
