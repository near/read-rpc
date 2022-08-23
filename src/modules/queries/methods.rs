use crate::config::ServerContext;
use crate::errors::RPCError;
use crate::modules::blocks::methods::fetch_block;
use crate::modules::queries::utils::{
    fetch_access_key_from_redis, fetch_account_from_redis, fetch_code_from_redis,
    fetch_state_from_redis,
};
use borsh::BorshSerialize;
use jsonrpc_v2::{Data, Params};

async fn view_account(
    data: &Data<ServerContext>,
    block_reference: near_primitives::types::BlockReference,
    account_id: &near_indexer_primitives::types::AccountId,
) -> anyhow::Result<near_jsonrpc_primitives::types::query::RpcQueryResponse> {
    let block = fetch_block(&data, block_reference.clone()).await?;

    let account =
        fetch_account_from_redis(data.redis_client.clone(), account_id, block.header.height)
            .await?;

    Ok(near_jsonrpc_primitives::types::query::RpcQueryResponse {
        kind: near_jsonrpc_primitives::types::query::QueryResponseKind::ViewAccount(
            near_indexer_primitives::views::AccountView::from(account),
        ),
        block_height: block.header.height,
        block_hash: block.header.hash,
    })
}

async fn view_code(
    data: &Data<ServerContext>,
    block_reference: near_primitives::types::BlockReference,
    account_id: &near_indexer_primitives::types::AccountId,
) -> anyhow::Result<near_jsonrpc_primitives::types::query::RpcQueryResponse> {
    let block = fetch_block(&data, block_reference.clone()).await?;

    let contract_code =
        fetch_code_from_redis(data.redis_client.clone(), account_id, block.header.height).await?;

    Ok(near_jsonrpc_primitives::types::query::RpcQueryResponse {
        kind: near_jsonrpc_primitives::types::query::QueryResponseKind::ViewCode(
            near_primitives::views::ContractCodeView::from(contract_code),
        ),
        block_height: block.header.height,
        block_hash: block.header.hash,
    })
}

async fn view_state(
    data: &Data<ServerContext>,
    block_reference: near_primitives::types::BlockReference,
    account_id: &near_indexer_primitives::types::AccountId,
    prefix: &[u8],
) -> anyhow::Result<near_jsonrpc_primitives::types::query::RpcQueryResponse> {
    let block = fetch_block(&data, block_reference.clone()).await?;

    let contract_state = fetch_state_from_redis(
        data.redis_client.clone(),
        account_id,
        block.header.height,
        prefix,
    )
    .await?;

    Ok(near_jsonrpc_primitives::types::query::RpcQueryResponse {
        kind: near_jsonrpc_primitives::types::query::QueryResponseKind::ViewState(contract_state),
        block_height: block.header.height,
        block_hash: block.header.hash,
    })
}

async fn view_access_key(
    data: &Data<ServerContext>,
    block_reference: near_primitives::types::BlockReference,
    account_id: &near_indexer_primitives::types::AccountId,
    key_data: Vec<u8>,
) -> anyhow::Result<near_jsonrpc_primitives::types::query::RpcQueryResponse> {
    let block = fetch_block(&data, block_reference.clone()).await?;

    let access_key = fetch_access_key_from_redis(
        data.redis_client.clone(),
        account_id,
        block.header.height,
        key_data,
    )
    .await?;

    Ok(near_jsonrpc_primitives::types::query::RpcQueryResponse {
        kind: near_jsonrpc_primitives::types::query::QueryResponseKind::AccessKey(
            near_primitives::views::AccessKeyView::from(access_key),
        ),
        block_height: block.header.height,
        block_hash: block.header.hash,
    })
}

pub async fn query(
    data: Data<ServerContext>,
    Params(params): Params<near_jsonrpc_primitives::types::query::RpcQueryRequest>,
) -> Result<near_jsonrpc_primitives::types::query::RpcQueryResponse, RPCError> {
    match params.request.clone() {
        near_primitives::views::QueryRequest::ViewAccount { account_id } => {
            match view_account(&data, params.block_reference.clone(), &account_id).await {
                Ok(result) => Ok(result),
                Err(_) => Ok(data.near_rpc_client.call(params).await?),
            }
        }
        near_primitives::views::QueryRequest::ViewCode { account_id } => {
            match view_code(&data, params.block_reference.clone(), &account_id).await {
                Ok(result) => Ok(result),
                Err(_) => Ok(data.near_rpc_client.call(params).await?),
            }
        }
        near_primitives::views::QueryRequest::ViewAccessKey {
            account_id,
            public_key,
        } => {
            match view_access_key(
                &data,
                params.block_reference.clone(),
                &account_id,
                public_key.try_to_vec().unwrap(),
            )
            .await
            {
                Ok(result) => Ok(result),
                Err(_) => Ok(data.near_rpc_client.call(params).await?),
            }
        }
        near_primitives::views::QueryRequest::ViewState { account_id, prefix } => match view_state(
            &data,
            params.block_reference.clone(),
            &account_id,
            prefix.as_ref(),
        )
        .await
        {
            Ok(result) => Ok(result),
            Err(_) => Ok(data.near_rpc_client.call(params).await?),
        },
        near_primitives::views::QueryRequest::ViewAccessKeyList { account_id: _ } => {
            unimplemented!("ViewAccessKeyList - Unimplemented")
        }
        near_primitives::views::QueryRequest::CallFunction {
            account_id: _,
            method_name: _,
            args: _,
        } => {
            unimplemented!("CallFunction - Unimplemented")
        }
    }
}
