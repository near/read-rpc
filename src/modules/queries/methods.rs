use crate::config::ServerContext;
use crate::errors::RPCError;
use crate::modules::blocks::utils::{fetch_block_from_s3, fetch_block_height_from_db};
use crate::modules::queries::utils::{
    fetch_account_from_redis, fetch_latest_block_height_from_redis,
};
use jsonrpc_v2::{Data, Error, Params};
use std::ops::Deref;

async fn view_account(
    data: &Data<ServerContext>,
    block_reference: near_primitives::types::BlockReference,
    account_id: &near_indexer_primitives::types::AccountId,
) -> Option<near_jsonrpc_primitives::types::query::RpcQueryResponse> {
    let block_height = match block_reference {
        near_primitives::types::BlockReference::BlockId(block_id) => match block_id {
            near_primitives::types::BlockId::Height(block_height) => Some(block_height),
            near_primitives::types::BlockId::Hash(block_hash) => {
                match fetch_block_height_from_db(&data.db_client, block_hash.clone()).await {
                    Ok(block_height) => Some(block_height),
                    Err(_) => None,
                }
            }
        },
        near_primitives::types::BlockReference::Finality(finality) => match finality {
            near_primitives::types::Finality::Final => {
                fetch_latest_block_height_from_redis(data.redis_client.clone()).await
            }
            _ => None,
        },
        _ => None,
    };

    if let Some(block_height) = block_height {
        if let Ok(block) =
            fetch_block_from_s3(&data.s3_client, &data.s3_bucket_name, block_height).await
        {
            if let Some(account) =
                fetch_account_from_redis(data.redis_client.clone(), account_id, block_height).await
            {
                Some(near_jsonrpc_primitives::types::query::RpcQueryResponse {
                    kind: near_jsonrpc_primitives::types::query::QueryResponseKind::ViewAccount(
                        near_indexer_primitives::views::AccountView::from(account),
                    ),
                    block_height,
                    block_hash: block.header.hash,
                })
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    }
}

pub async fn query(
    data: Data<ServerContext>,
    Params(params): Params<near_jsonrpc_primitives::types::query::RpcQueryRequest>,
) -> Result<near_jsonrpc_primitives::types::query::RpcQueryResponse, RPCError> {
    match params.request {
        near_primitives::views::QueryRequest::ViewAccount { ref account_id } => {
            match view_account(&data, params.block_reference.clone(), account_id).await {
                Some(result) => Ok(result),
                None => {
                    println!("Proxy!!!!!!!!!!!");
                    match data.near_rpc_client.call(params).await {
                        Ok(result) => Ok(result),
                        Err(err) => Err(RPCError::from(err.handler_error().unwrap())),
                    }
                }
            }
        }
        _ => Err(RPCError::invalid_params()),
    }
}
