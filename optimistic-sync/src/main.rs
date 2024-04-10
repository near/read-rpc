use actix_web::{App, HttpServer};

mod metrics;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct BlockData {
    block: near_primitives::views::BlockView,
    block_type: String,
}

pub async fn get_final_block(
    near_rpc_client: &near_jsonrpc_client::JsonRpcClient,
    optimistic: bool,
) -> anyhow::Result<near_primitives::views::BlockView> {
    let block_request_method = near_jsonrpc_client::methods::block::RpcBlockRequest {
        block_reference: near_primitives::types::BlockReference::Finality(if optimistic {
            near_primitives::types::Finality::None
        } else {
            near_primitives::types::Finality::Final
        }),
    };
    let block_view = near_rpc_client.call(block_request_method).await?;
    
    if optimistic {
        crate::metrics::OPTIMISTIC_BLOCK_HEIGHT.set(i64::try_from(block_view.header.height)?);
    } else {
        crate::metrics::FINAL_BLOCK_HEIGHT.set(i64::try_from(block_view.header.height)?);
    }
    Ok(block_view)
}

pub async fn save_block_to_redis(
    key: &str,
    block_data: BlockData,
    redis_client: redis::aio::ConnectionManager,
) -> anyhow::Result<()> {
    let json_block_data = serde_json::to_string(block_data)?;
    redis::cmd("SET")
        .arg(key)
        .arg(json_block_data)
        .query_async(&mut redis_client.clone())
        .await?;
    Ok(())
}

async fn fetch_optimistic_block_regularly(
    near_rpc_client: near_jsonrpc_client::JsonRpcClient,
    redis_client: redis::aio::ConnectionManager,
) {
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let block = get_final_block(&near_rpc_client, true).await;
        println!("Optimistic {:?}", block);
    }
}

async fn fetch_final_block_regularly(
    near_rpc_client: near_jsonrpc_client::JsonRpcClient,
    redis_client: redis::aio::ConnectionManager,
) {
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let block = get_final_block(&near_rpc_client, false).await;
        println!("Final {:?}", block);
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let rpc_client = near_jsonrpc_client::JsonRpcClient::connect(
        "https://rpc.mainnet.near.org"
    );

    let redis_client = redis::Client::open("redis://127.0.0.1:6379")?
        .get_connection_manager()
        .await?;
    
    tokio::spawn(fetch_optimistic_block_regularly(rpc_client.clone(), redis_client.clone()));
    tokio::spawn(fetch_final_block_regularly(rpc_client.clone(), redis_client.clone()));

    HttpServer::new(|| App::new().service(metrics::get_metrics))
        .bind(("0.0.0.0", 8887))?
        .disable_signals()
        .run()
        .await?;

    Ok(())
}
