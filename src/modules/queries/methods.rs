use crate::config::ServerContext;
use crate::errors::RPCError;
use crate::modules::blocks::utils::{fetch_block_from_s3, fetch_block_height_from_db};
use crate::modules::queries::utils::{
    fetch_account_from_redis, fetch_latest_block_height_from_redis,
};
use jsonrpc_v2::{Data, Params};

async fn view_account(
    data: &Data<ServerContext>,
    block_reference: near_primitives::types::BlockReference,
    account_id: &near_indexer_primitives::types::AccountId,
) -> anyhow::Result<near_jsonrpc_primitives::types::query::RpcQueryResponse> {
    let block_height = match block_reference {
        near_primitives::types::BlockReference::BlockId(block_id) => match block_id {
            near_primitives::types::BlockId::Height(block_height) => block_height,
            near_primitives::types::BlockId::Hash(block_hash) => {
                fetch_block_height_from_db(&data.db_client, block_hash.clone()).await?
            }
        },
        near_primitives::types::BlockReference::Finality(finality) => match finality {
            near_primitives::types::Finality::Final => {
                fetch_latest_block_height_from_redis(data.redis_client.clone()).await?
            }
            _ => anyhow::bail!("Finality other than final is not supported"),
        },
        near_primitives::types::BlockReference::SyncCheckpoint(_) => {
            anyhow::bail!("SyncCheckpoint is not supported")
        }
    };

    let block = fetch_block_from_s3(&data.s3_client, &data.s3_bucket_name, block_height).await?;

    let account =
        fetch_account_from_redis(data.redis_client.clone(), account_id, block_height).await?;

    Ok(near_jsonrpc_primitives::types::query::RpcQueryResponse {
        kind: near_jsonrpc_primitives::types::query::QueryResponseKind::ViewAccount(
            near_indexer_primitives::views::AccountView::from(account),
        ),
        block_height,
        block_hash: block.header.hash,
    })
}

pub async fn query(
    data: Data<ServerContext>,
    Params(params): Params<near_jsonrpc_primitives::types::query::RpcQueryRequest>,
) -> Result<near_jsonrpc_primitives::types::query::RpcQueryResponse, RPCError> {
    match params.request {
        near_primitives::views::QueryRequest::ViewAccount { ref account_id } => {
            match view_account(&data, params.block_reference.clone(), account_id).await {
                Ok(result) => Ok(result),
                Err(_) => Ok(data.near_rpc_client.call(params).await?),
            }
        }
        _ => Err(RPCError::invalid_params()),
    }
}
