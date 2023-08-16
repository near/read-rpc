use std::time::{Duration, Instant};

use chrono::prelude::*;
use dotenv::dotenv;
use futures::future::join_all;
use rand::Rng;

use near_jsonrpc_client::{methods, JsonRpcClient};
use near_jsonrpc_primitives::types::chunks::ChunkReference;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, BlockHeight, BlockId, BlockReference, Finality};

struct ChunkInfo {
    transactions: Vec<CryptoHash>,
    accounts: Vec<AccountId>,
}

const RUNS_COUNT: i32 = 3;

// While testing chunks, it's important to collect accounts and transactions for further tests
async fn get_random_chunk(random_block_height: BlockHeight, client: &JsonRpcClient) -> anyhow::Result<(Duration, ChunkInfo)> {
    let now = Instant::now();
    // I'll always use shard 0 because it should always exist and it should be enough for perf test
    let chunk = client.call(methods::chunk::RpcChunkRequest {
        chunk_reference: ChunkReference::BlockShardId { block_id: BlockId::Height(random_block_height), shard_id: 0 },
    }).await?;
    let elapsed = now.elapsed();

    let a = ChunkInfo {
        transactions: chunk.transactions.iter().map(|tx| tx.hash).collect(),
        accounts: chunk.transactions.iter().flat_map(|tx| vec![tx.signer_id.clone(), tx.receiver_id.clone()]).collect(),
    };
    println!("{} tx {} accs {}", random_block_height, a.transactions.len(), a.accounts.len());
    Ok((elapsed, a))
}


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    let read_rpc_url = std::env::var("READ_RPC_URL").expect("READ_RPC_URL env var expected");
    let read_rpc_client = JsonRpcClient::connect(read_rpc_url);

    let archival_rpc_url = std::env::var("ARCHIVAL_RPC_URL").expect("ARCHIVAL_RPC_URL env var expected");
    let archival_rpc_client = JsonRpcClient::connect(archival_rpc_url);

    // Hardcoding mainnet first after genesis block
    let genesis_block_height = 9820214;
    let final_block = read_rpc_client.call(methods::block::RpcBlockRequest {
        block_reference: BlockReference::Finality(Finality::Final),
    }).await?;
    let epoch_len = 43200;

    // let mut accounts = vec![];
    // let mut transactions = vec![];

    let final_block_dt = NaiveDateTime::from_timestamp_micros(final_block.header.timestamp as i64 / 1000).expect("Unable to parse final block timestamp");
    println!("Current block_height is {} ({:?})\n", final_block.header.height, final_block_dt);
    // Hot storage contains 5 last epochs, but I will ignore 2 boundary epochs to be sure the results are not spoiled
    let final_cold_storage_block = final_block.header.height - epoch_len * 6;
    println!("Cold storage has block range [{}, {}]", genesis_block_height, final_cold_storage_block);

    let heights: Vec<u64> = (0..RUNS_COUNT).map(|_| {
        rand::thread_rng().gen_range(genesis_block_height..final_cold_storage_block)
    }).collect();

    println!("TEST #1: random chunks query, cold storage");
    let read_rpc_chunks = join_all(heights.iter().map(|height|
        get_random_chunk(*height, &read_rpc_client)
    )).await;
    // let (read_rpc_chunks_errors, read_rpc_chunks_median_time_waiting) = read_rpc_chunks.iter().reduce(|a| {
    //     match
    // })

    let b = join_all(heights.iter().map(|height|
        get_random_chunk(*height, &archival_rpc_client)
    )).await;




    let block2 = final_block.header.height;
    println!("{:#?}", final_block);



    // let tx_status_request = methods::tx::RpcTransactionStatusRequest {
    //     transaction_info: TransactionInfo::TransactionId {
    //         hash: "9FtHUFBQsZ2MG77K3x3MJ9wjX3UT8zE1TczCrhZEcG8U".parse()?,
    //         account_id: "miraclx.near".parse()?,
    //     },
    // };
    //
    // let tx_status = mainnet_client.call(tx_status_request).await?;
    // println!("{:?}", tx_status);
    // let client = reqwest::Client::new();
    // let res = client.post(read_rpc_url)
    //     .body(get_block())
    //     .send()
    //     .await?;
    //
    // let a = res.json::<RpcBlockResponse>().await?;



    // EXPERIMENTAL_tx_status
    // chunk
    // query (different options) view_account
    // fn call
    Ok(())
}
