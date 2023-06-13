use crate::config::ServerContext;
use crate::errors::RPCError;
use crate::modules::blocks::utils::fetch_block_from_cache_or_get;
use crate::modules::blocks::CacheBlock;
#[cfg(feature = "account_access_keys")]
use crate::modules::queries::utils::fetch_list_access_keys_from_scylla_db;
use crate::modules::queries::utils::{
    fetch_access_key_from_scylla_db, fetch_contract_code_from_scylla_db,
    fetch_state_from_scylla_db, run_contract,
};
#[cfg(feature = "shadow_data_consistency")]
use crate::utils::shadow_compare_results;
use borsh::BorshSerialize;
use jsonrpc_v2::{Data, Params};

#[allow(unused_mut)]
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn query(
    data: Data<ServerContext>,
    Params(mut params): Params<near_jsonrpc_primitives::types::query::RpcQueryRequest>,
) -> Result<near_jsonrpc_primitives::types::query::RpcQueryResponse, RPCError> {
    tracing::debug!("`query` call. Params: {:?}", params,);

    // Increase the OPTIMISTIC_REQUESTS_TOTAL metric if the request has optimistic finality.
    if let near_primitives::types::BlockReference::Finality(finality) = &params.block_reference {
        // Finality::None stands for optimistic finality.
        if finality == &near_primitives::types::Finality::None {
            crate::metrics::OPTIMISTIC_REQUESTS_TOTAL.inc();
        }
    }

    let block = fetch_block_from_cache_or_get(&data, params.block_reference.clone()).await;
    let result = match params.request.clone() {
        near_primitives::views::QueryRequest::ViewAccount { account_id } => {
            crate::metrics::QUERY_VIEW_ACCOUNT_REQUESTS_TOTAL.inc();
            let account_result = view_account(&data, block, &account_id).await;
            if let Err(err) = &account_result {
                tracing::warn!("Error in `view_account` call: {:?}", err);
                crate::metrics::QUERY_VIEW_ACCOUNT_PROXIES_TOTAL.inc();
            }
            account_result
        }
        near_primitives::views::QueryRequest::ViewCode { account_id } => {
            crate::metrics::QUERY_VIEW_CODE_REQUESTS_TOTAL.inc();
            let code_result = view_code(&data, block, &account_id).await;
            if let Err(err) = &code_result {
                tracing::warn!("Error in `view_code` call: {:?}", err);
                crate::metrics::QUERY_VIEW_CODE_PROXIES_TOTAL.inc();
            }
            code_result
        }
        near_primitives::views::QueryRequest::ViewAccessKey {
            account_id,
            public_key,
        } => {
            crate::metrics::QUERY_VIEW_ACCESS_KEY_REQUESTS_TOTAL.inc();
            let access_key_result =
                view_access_key(&data, block, &account_id, public_key.try_to_vec().unwrap()).await;
            if let Err(err) = &access_key_result {
                tracing::warn!("Error in `view_access_key` call: {:?}", err);
                crate::metrics::QUERY_VIEW_ACCESS_KEY_PROXIES_TOTAL.inc();
            }
            access_key_result
        }
        near_primitives::views::QueryRequest::ViewState {
            account_id,
            prefix,
            include_proof: _,
        } => {
            crate::metrics::QUERY_VIEW_STATE_REQUESTS_TOTAL.inc();
            let state_result = view_state(&data, block, &account_id, prefix.as_ref()).await;
            if let Err(err) = &state_result {
                tracing::warn!("Error in `view_state` call: {:?}", err);
                crate::metrics::QUERY_VIEW_STATE_PROXIES_TOTAL.inc();
            }
            state_result
        }
        near_primitives::views::QueryRequest::CallFunction {
            account_id,
            method_name,
            args,
        } => {
            crate::metrics::QUERY_FUNCTION_CALL_REQUESTS_TOTAL.inc();
            let function_call_result =
                function_call(&data, block, account_id, &method_name, args.clone()).await;
            if let Err(err) = &function_call_result {
                tracing::warn!("Error in `call_function` call: {:?}", err);
                crate::metrics::QUERY_FUNCTION_CALL_PROXIES_TOTAL.inc();
            };
            function_call_result
        }
        near_primitives::views::QueryRequest::ViewAccessKeyList { account_id } => {
            crate::metrics::QUERY_VIEW_ACCESS_KEYS_LIST_REQUESTS_TOTAL.inc();
            #[cfg(not(feature = "account_access_keys"))]
            return Ok(proxy_rpc_call(&data.near_rpc_client, params).await?);
            #[cfg(feature = "account_access_keys")]
            {
                let access_keys_result = view_access_keys_list(&data, block, &account_id).await;
                if let Err(err) = &access_keys_result {
                    tracing::warn!("Error in `view_access_keys_list` call: {:?}", err);
                    crate::metrics::QUERY_VIEW_ACCESS_KEYS_LIST_PROXIES_TOTAL.inc();
                };
                access_keys_result
            }
        }
    };

    #[cfg(feature = "shadow_data_consistency")]
    {
        let near_rpc_client = data.near_rpc_client.clone();
        if let near_primitives::types::BlockReference::Finality(_) = params.block_reference {
            params.block_reference = near_primitives::types::BlockReference::from(
                near_primitives::types::BlockId::Height(block.block_height),
            )
        }

        let comparison_result =
            shadow_compare_results(serde_json::to_value(&result), near_rpc_client, params).await;

        match comparison_result {
            Ok(_) => {
                tracing::info!(target: "shadow_data_consistency", "Shadow data check: CORRECT");
            }
            Err(err) => {
                tracing::warn!(target: "shadow_data_consistency", "Shadow data check: ERROR {:?}", err);
            }
        }
    }

    Ok(result.map_err(near_jsonrpc_primitives::errors::RpcError::from)?)
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn view_account(
    data: &Data<ServerContext>,
    block: CacheBlock,
    account_id: &near_primitives::types::AccountId,
) -> Result<
    near_jsonrpc_primitives::types::query::RpcQueryResponse,
    near_jsonrpc_primitives::types::query::RpcQueryError,
> {
    tracing::debug!(
        "`view_account` call. AccountID {}, Block {}",
        account_id,
        block.block_height
    );

    let account = data
        .scylla_db_manager
        .get_account(account_id, block.block_height)
        .await
        .map_err(|err| {
            tracing::warn!("Error in `view_account` call: {:?}", err);
            near_jsonrpc_primitives::types::query::RpcQueryError::UnknownAccount {
                requested_account_id: account_id.clone(),
                block_height: block.block_height,
                block_hash: block.block_hash,
            }
        })?;

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
) -> Result<
    near_jsonrpc_primitives::types::query::RpcQueryResponse,
    near_jsonrpc_primitives::types::query::RpcQueryError,
> {
    tracing::debug!(
        "`view_code` call. AccountID {}, Block {}",
        account_id,
        block.block_height
    );
    let code_data_from_db =
        fetch_contract_code_from_scylla_db(&data.scylla_db_manager, account_id, block.block_height)
            .await
            .map_err(|err| {
                tracing::warn!("Error in `view_code` call: {:?}", err);
                near_jsonrpc_primitives::types::query::RpcQueryError::NoContractCode {
                    contract_account_id: account_id.clone(),
                    block_height: block.block_height,
                    block_hash: block.block_hash,
                }
            })?;
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
) -> Result<
    near_jsonrpc_primitives::types::query::RpcQueryResponse,
    near_jsonrpc_primitives::types::query::RpcQueryError,
> {
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
    .await
    .map_err(|err| {
        tracing::debug!("Failed function call: {:?}", err);
        near_jsonrpc_primitives::types::query::RpcQueryError::InternalError {
            error_message: format!("Function call failed: {:?}", err),
        }
    })?;
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
        None => Err(
            near_jsonrpc_primitives::types::query::RpcQueryError::InternalError {
                error_message: "Failed function call: empty result".to_string(),
            },
        ),
    }
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn view_state(
    data: &Data<ServerContext>,
    block: CacheBlock,
    account_id: &near_primitives::types::AccountId,
    prefix: &[u8],
) -> Result<
    near_jsonrpc_primitives::types::query::RpcQueryResponse,
    near_jsonrpc_primitives::types::query::RpcQueryError,
> {
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
    .await
    .map_err(|err| {
        tracing::warn!("Error in `view_state` call: {:?}", err);
        near_jsonrpc_primitives::types::query::RpcQueryError::UnknownAccount {
            requested_account_id: account_id.clone(),
            block_height: block.block_height,
            block_hash: block.block_hash,
        }
    })?;

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
) -> Result<
    near_jsonrpc_primitives::types::query::RpcQueryResponse,
    near_jsonrpc_primitives::types::query::RpcQueryError,
> {
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
        &key_data,
    )
    .await
    .map_err(|err| {
        tracing::warn!("Error in `view_access_key` call: {:?}", err);
        match near_crypto::ED25519PublicKey::try_from(key_data.as_slice()) {
            Ok(public_key) => {
                near_jsonrpc_primitives::types::query::RpcQueryError::UnknownAccessKey {
                    public_key: public_key.into(),
                    block_height: block.block_height,
                    block_hash: block.block_hash,
                }
            }
            Err(_) => near_jsonrpc_primitives::types::query::RpcQueryError::InternalError {
                error_message: "Failed to parse public key".to_string(),
            },
        }
    })?;

    Ok(near_jsonrpc_primitives::types::query::RpcQueryResponse {
        kind: near_jsonrpc_primitives::types::query::QueryResponseKind::AccessKey(
            near_primitives::views::AccessKeyView::from(access_key),
        ),
        block_height: block.block_height,
        block_hash: block.block_hash,
    })
}

#[cfg(feature = "account_access_keys")]
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn view_access_keys_list(
    data: &Data<ServerContext>,
    block: CacheBlock,
    account_id: &near_primitives::types::AccountId,
) -> Result<
    near_jsonrpc_primitives::types::query::RpcQueryResponse,
    near_jsonrpc_primitives::types::query::RpcQueryError,
> {
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
    .await
    // TODO: review this once we implement the `account_access_keys` after the redesign
    // this error has to be the same the real NEAR JSON RPC returns in this case
    .map_err(
        |err| near_jsonrpc_primitives::types::query::RpcQueryError::InternalError {
            error_message: format!("Failed to fetch access keys: {}", err),
        },
    )?;

    Ok(near_jsonrpc_primitives::types::query::RpcQueryResponse {
        kind: near_jsonrpc_primitives::types::query::QueryResponseKind::AccessKeyList(
            near_primitives::views::AccessKeyList { keys: access_keys },
        ),
        block_height: block.block_height,
        block_hash: block.block_hash,
    })
}
