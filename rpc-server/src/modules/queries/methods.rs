use crate::config::ServerContext;
use crate::errors::RPCError;
use crate::modules::blocks::utils::fetch_block_from_cache_or_get;
use crate::modules::blocks::CacheBlock;
use crate::modules::queries::utils::{
    fetch_access_key_from_scylla_db, fetch_account_from_scylla_db,
    fetch_contract_code_from_scylla_db, fetch_list_access_keys_from_scylla_db,
    fetch_state_from_scylla_db, run_contract,
};
use crate::utils::proxy_rpc_call;
#[cfg(feature = "shadow_data_consistency")]
use crate::utils::shadow_compare_results;
use borsh::BorshSerialize;
use jsonrpc_v2::{Data, Params};

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn view_account(
    data: &Data<ServerContext>,
    block: CacheBlock,
    account_id: &near_primitives::types::AccountId,
) -> anyhow::Result<near_jsonrpc_primitives::types::query::RpcQueryResponse> {
    tracing::debug!(
        "`view_account` call. AccountID {}, Block {}",
        account_id,
        block.block_height
    );

    let account =
        fetch_account_from_scylla_db(&data.scylla_db_manager, account_id, block.block_height)
            .await?;

    Ok(near_jsonrpc_primitives::types::query::RpcQueryResponse {
        kind: near_jsonrpc_primitives::types::query::QueryResponseKind::ViewAccount(
            near_primitives::views::AccountView::from(account),
        ),
        block_height: block.block_height,
        block_hash: block.block_hash,
    })
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn view_code(
    data: &Data<ServerContext>,
    block: CacheBlock,
    account_id: &near_primitives::types::AccountId,
) -> anyhow::Result<near_jsonrpc_primitives::types::query::RpcQueryResponse> {
    tracing::debug!(
        "`view_code` call. AccountID {}, Block {}",
        account_id,
        block.block_height
    );
    let code_data_from_db =
        fetch_contract_code_from_scylla_db(&data.scylla_db_manager, account_id, block.block_height)
            .await?;
    Ok(near_jsonrpc_primitives::types::query::RpcQueryResponse {
        kind: near_jsonrpc_primitives::types::query::QueryResponseKind::ViewCode(
            near_primitives::views::ContractCodeView::from(
                near_primitives::contract::ContractCode::new(code_data_from_db, None),
            ),
        ),
        block_height: block.block_height,
        block_hash: block.block_hash,
    })
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn function_call(
    data: &Data<ServerContext>,
    block: CacheBlock,
    account_id: near_primitives::types::AccountId,
    method_name: &str,
    args: near_primitives::types::FunctionArgs,
) -> anyhow::Result<near_jsonrpc_primitives::types::query::RpcQueryResponse> {
    tracing::debug!(
        "`function_call` call. AccountID {}, block {}, method_name {}, args {:?}",
        account_id,
        block.block_height,
        method_name,
        args,
    );
    let call_results = run_contract(
        account_id,
        method_name,
        args,
        data.scylla_db_manager.clone(),
        &data.compiled_contract_code_cache,
        &data.contract_code_cache,
        block.block_height,
        block.block_timestamp,
        block.latest_protocol_version,
    )
    .await?;
    match call_results.return_data.as_value() {
        Some(val) => Ok(near_jsonrpc_primitives::types::query::RpcQueryResponse {
            kind: near_jsonrpc_primitives::types::query::QueryResponseKind::CallResult(
                near_primitives::views::CallResult {
                    result: val,
                    logs: call_results.logs,
                },
            ),
            block_height: block.block_height,
            block_hash: block.block_hash,
        }),
        None => anyhow::bail!("Invalid function call result"),
    }
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn view_state(
    data: &Data<ServerContext>,
    block: CacheBlock,
    account_id: &near_primitives::types::AccountId,
    prefix: &[u8],
) -> anyhow::Result<near_jsonrpc_primitives::types::query::RpcQueryResponse> {
    tracing::debug!(
        "`view_state` call. AccountID {}, block {}, prefix {:?}",
        account_id,
        block.block_height,
        prefix,
    );
    let contract_state = fetch_state_from_scylla_db(
        &data.scylla_db_manager,
        account_id,
        block.block_height,
        prefix,
    )
    .await?;

    Ok(near_jsonrpc_primitives::types::query::RpcQueryResponse {
        kind: near_jsonrpc_primitives::types::query::QueryResponseKind::ViewState(contract_state),
        block_height: block.block_height,
        block_hash: block.block_hash,
    })
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn view_access_key(
    data: &Data<ServerContext>,
    block: CacheBlock,
    account_id: &near_primitives::types::AccountId,
    key_data: Vec<u8>,
) -> anyhow::Result<near_jsonrpc_primitives::types::query::RpcQueryResponse> {
    tracing::debug!(
        "`view_access_key` call. AccountID {}, block {}, key_data {:?}",
        account_id,
        block.block_height,
        key_data,
    );

    let access_key = fetch_access_key_from_scylla_db(
        &data.scylla_db_manager,
        account_id,
        block.block_height,
        key_data,
    )
    .await?;

    Ok(near_jsonrpc_primitives::types::query::RpcQueryResponse {
        kind: near_jsonrpc_primitives::types::query::QueryResponseKind::AccessKey(
            near_primitives::views::AccessKeyView::from(access_key),
        ),
        block_height: block.block_height,
        block_hash: block.block_hash,
    })
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn view_access_keys_list(
    data: &Data<ServerContext>,
    block: CacheBlock,
    account_id: &near_primitives::types::AccountId,
) -> anyhow::Result<near_jsonrpc_primitives::types::query::RpcQueryResponse> {
    tracing::debug!(
        "`view_access_key` call. AccountID {}, block {}",
        account_id,
        block.block_height,
    );

    let access_keys = fetch_list_access_keys_from_scylla_db(
        &data.scylla_db_manager,
        account_id,
        block.block_height,
    )
    .await?;

    Ok(near_jsonrpc_primitives::types::query::RpcQueryResponse {
        kind: near_jsonrpc_primitives::types::query::QueryResponseKind::AccessKeyList(
            near_primitives::views::AccessKeyList { keys: access_keys },
        ),
        block_height: block.block_height,
        block_hash: block.block_hash,
    })
}

#[allow(unused)]
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn query(
    data: Data<ServerContext>,
    Params(mut params): Params<near_jsonrpc_primitives::types::query::RpcQueryRequest>,
) -> Result<near_jsonrpc_primitives::types::query::RpcQueryResponse, RPCError> {
    tracing::debug!("`query` call. Params: {:?}", params,);
    let block = fetch_block_from_cache_or_get(&data, params.block_reference.clone()).await;
    let result = match params.request.clone() {
        near_primitives::views::QueryRequest::ViewAccount { account_id } => {
            view_account(&data, block, &account_id).await
        }
        near_primitives::views::QueryRequest::ViewCode { account_id } => {
            view_code(&data, block, &account_id).await
        }
        near_primitives::views::QueryRequest::ViewAccessKey {
            account_id,
            public_key,
        } => view_access_key(&data, block, &account_id, public_key.try_to_vec().unwrap()).await,
        near_primitives::views::QueryRequest::ViewState {
            account_id,
            prefix,
            include_proof: _,
        } => view_state(&data, block, &account_id, prefix.as_ref()).await,
        near_primitives::views::QueryRequest::CallFunction {
            account_id,
            method_name,
            args,
        } => function_call(&data, block, account_id, &method_name, args.clone()).await,
        near_primitives::views::QueryRequest::ViewAccessKeyList { account_id } => {
            view_access_keys_list(&data, block, &account_id).await
        }
    };
    match result {
        Ok(result) => {
            #[cfg(feature = "shadow_data_consistency")]
            {
                let near_rpc_client = data.near_rpc_client.clone();
                if let near_primitives::types::BlockReference::Finality(_) = params.block_reference
                {
                    params.block_reference = near_primitives::types::BlockReference::from(
                        near_primitives::types::BlockId::Height(block.block_height),
                    )
                }
                tokio::task::spawn(shadow_compare_results(
                    serde_json::to_value(&result),
                    near_rpc_client,
                    params,
                ));
            };
            Ok(result)
        }
        Err(e) => {
            tracing::debug!("Result not found: {:?}, Params: {:?}", e, params);
            Ok(proxy_rpc_call(&data.near_rpc_client, params).await?)
        }
    }
}
