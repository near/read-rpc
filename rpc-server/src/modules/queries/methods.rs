use crate::config::ServerContext;
use crate::errors::RPCError;
use crate::modules::blocks::utils::fetch_block_from_cache_or_get;
use crate::modules::blocks::CacheBlock;
#[cfg(feature = "account_access_keys")]
use crate::modules::queries::utils::fetch_list_access_keys_from_db;
use crate::modules::queries::utils::{get_state_keys_from_db, run_contract, RunContractResponse};
use jsonrpc_v2::{Data, Params};
use near_jsonrpc::RpcRequest;

/// `query` rpc method implementation
/// calls proxy_rpc_call to get `query` from near-rpc if request parameters not supported by read-rpc
/// as example: BlockReference for Finality::None is not supported by read-rpc when near_state_indexer is not running
/// another way to get `query` from read-rpc using `query_call`
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn query(
    data: Data<ServerContext>,
    Params(params): Params<serde_json::Value>,
) -> Result<near_jsonrpc::primitives::types::query::RpcQueryResponse, RPCError> {
    let query_request = near_jsonrpc::primitives::types::query::RpcQueryRequest::parse(params)?;

    if let near_primitives::types::BlockReference::Finality(
        near_primitives::types::Finality::None,
    ) = &query_request.block_reference
    {
        // Increase the OPTIMISTIC_REQUESTS_TOTAL metric
        // if the request has optimistic finality
        crate::metrics::OPTIMISTIC_REQUESTS_TOTAL.inc();
        return if crate::metrics::OPTIMISTIC_UPDATING.is_not_working() {
            // Increase the PROXY_OPTIMISTIC_REQUESTS_TOTAL metric
            // if optimistic not updating and proxy to near-rpc
            crate::metrics::PROXY_OPTIMISTIC_REQUESTS_TOTAL.inc();
            Ok(data.near_rpc_client.call(query_request).await?)
        } else {
            // query_call with optimistic block
            query_call(data, query_request, true).await
        };
    };

    query_call(data, query_request, false).await
}

/// fetch query result from read-rpc
#[allow(unused_mut)]
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn query_call(
    data: Data<ServerContext>,
    mut query_request: near_jsonrpc::primitives::types::query::RpcQueryRequest,
    is_optimistic: bool,
) -> Result<near_jsonrpc::primitives::types::query::RpcQueryResponse, RPCError> {
    tracing::debug!("`query` call. Params: {:?}", query_request,);

    let block = fetch_block_from_cache_or_get(&data, query_request.block_reference.clone())
        .await
        .map_err(near_jsonrpc::primitives::errors::RpcError::from)?;
    let result = match query_request.request.clone() {
        near_primitives::views::QueryRequest::ViewAccount { account_id } => {
            crate::metrics::QUERY_VIEW_ACCOUNT_REQUESTS_TOTAL.inc();
            view_account(&data, block, &account_id, is_optimistic).await
        }
        near_primitives::views::QueryRequest::ViewCode { account_id } => {
            crate::metrics::QUERY_VIEW_CODE_REQUESTS_TOTAL.inc();
            view_code(&data, block, &account_id, is_optimistic).await
        }
        near_primitives::views::QueryRequest::ViewAccessKey {
            account_id,
            public_key,
        } => {
            crate::metrics::QUERY_VIEW_ACCESS_KEY_REQUESTS_TOTAL.inc();
            view_access_key(&data, block, &account_id, public_key, is_optimistic).await
        }
        near_primitives::views::QueryRequest::ViewState {
            account_id,
            prefix,
            include_proof,
        } => {
            crate::metrics::QUERY_VIEW_STATE_REQUESTS_TOTAL.inc();
            if include_proof {
                // TODO: We can calculate the proof for state only on regular or archival nodes.
                let blocks_info_by_finality = data.blocks_info_by_finality.read().await;
                // `expected_earliest_available_block` calculated by formula:
                // `final_block_height` - `node_epoch_count` * `epoch_length`
                // Now near store 5 epochs, it can be changed in the future
                // epoch_length = 43200 blocks
                let expected_earliest_available_block =
                    blocks_info_by_finality.final_block.block_cache.block_height
                        - 5 * data.genesis_info.genesis_config.epoch_length;
                return if block.block_height > expected_earliest_available_block {
                    // Proxy to regular rpc if the block is available
                    Ok(data.near_rpc_client.call(query_request).await?)
                } else {
                    // Increase the QUERY_VIEW_STATE_INCLUDE_PROOFS metric if we proxy to archival rpc
                    crate::metrics::ARCHIVAL_PROXY_QUERY_VIEW_STATE_WITH_INCLUDE_PROOFS.inc();
                    // Proxy to archival rpc if the block garbage collected
                    Ok(data.near_rpc_client.archival_call(query_request).await?)
                };
            } else {
                view_state(&data, block, &account_id, prefix.as_ref(), is_optimistic).await
            }
        }
        near_primitives::views::QueryRequest::CallFunction {
            account_id,
            method_name,
            args,
        } => {
            crate::metrics::QUERY_FUNCTION_CALL_REQUESTS_TOTAL.inc();
            function_call(
                &data,
                block,
                account_id,
                &method_name,
                args.clone(),
                is_optimistic,
            )
            .await
        }
        #[allow(unused_variables)]
        // `account_id` is used in the `#[cfg(feature = "account_access_keys")]` branch.
        near_primitives::views::QueryRequest::ViewAccessKeyList { account_id } => {
            crate::metrics::QUERY_VIEW_ACCESS_KEYS_LIST_REQUESTS_TOTAL.inc();
            #[cfg(not(feature = "account_access_keys"))]
            return Ok(data.near_rpc_client.call(query_request).await?);
            #[cfg(feature = "account_access_keys")]
            {
                view_access_keys_list(&data, block, &account_id).await
            }
        }
    };

    #[cfg(feature = "shadow_data_consistency")]
    {
        let request_copy = query_request.request.clone();

        // Since we do queries with the clause WHERE block_height <= X, we need to
        // make sure that the block we are doing a shadow data consistency check for
        // matches the one we got the result for.
        // That's why we are using the block_height from the result.
        let block_height = match &result {
            Ok(res) => res.block_height,
            // If the result is an error it does not contain the block_height, so we
            // will use the block_height considered as final from the cache.
            Err(_err) => block.block_height,
        };
        query_request.block_reference = near_primitives::types::BlockReference::from(
            near_primitives::types::BlockId::Height(block_height),
        );

        // When the data check fails, we want to emit the log message and increment the
        // corresponding metric. Despite the metrics have "proxies" in their names, we
        // are not proxying the requests anymore and respond with the error to the client.
        // Since we already have the dashboard using these metric names, we don't want to
        // change them and reuse them for the observability of the shadow data consistency checks.
        match request_copy {
            near_primitives::views::QueryRequest::ViewAccount { .. } => {
                if let Some(err_code) = crate::utils::shadow_compare_results_handler(
                    crate::metrics::QUERY_VIEW_ACCOUNT_REQUESTS_TOTAL.get(),
                    data.shadow_data_consistency_rate,
                    &result,
                    data.near_rpc_client.clone(),
                    query_request,
                    "QUERY_VIEW_ACCOUNT",
                )
                .await
                {
                    crate::utils::capture_shadow_consistency_error!(err_code, "QUERY_VIEW_ACCOUNT")
                };
            }
            near_primitives::views::QueryRequest::ViewCode { .. } => {
                if let Some(err_code) = crate::utils::shadow_compare_results_handler(
                    crate::metrics::QUERY_VIEW_CODE_REQUESTS_TOTAL.get(),
                    data.shadow_data_consistency_rate,
                    &result,
                    data.near_rpc_client.clone(),
                    query_request,
                    "QUERY_VIEW_CODE",
                )
                .await
                {
                    crate::utils::capture_shadow_consistency_error!(err_code, "QUERY_VIEW_CODE")
                };
            }
            near_primitives::views::QueryRequest::ViewAccessKey { .. } => {
                if let Some(err_code) = crate::utils::shadow_compare_results_handler(
                    crate::metrics::QUERY_VIEW_ACCESS_KEY_REQUESTS_TOTAL.get(),
                    data.shadow_data_consistency_rate,
                    &result,
                    data.near_rpc_client.clone(),
                    query_request,
                    "QUERY_VIEW_ACCESS_KEY",
                )
                .await
                {
                    crate::utils::capture_shadow_consistency_error!(
                        err_code,
                        "QUERY_VIEW_ACCESS_KEY"
                    )
                };
            }
            near_primitives::views::QueryRequest::ViewState { .. } => {
                if let Some(err_code) = crate::utils::shadow_compare_results_handler(
                    crate::metrics::QUERY_VIEW_STATE_REQUESTS_TOTAL.get(),
                    data.shadow_data_consistency_rate,
                    &result,
                    data.near_rpc_client.clone(),
                    query_request,
                    "QUERY_VIEW_STATE",
                )
                .await
                {
                    crate::utils::capture_shadow_consistency_error!(err_code, "QUERY_VIEW_STATE")
                };
            }
            near_primitives::views::QueryRequest::CallFunction { .. } => {
                if let Some(err_code) = crate::utils::shadow_compare_results_handler(
                    crate::metrics::QUERY_FUNCTION_CALL_REQUESTS_TOTAL.get(),
                    data.shadow_data_consistency_rate,
                    &result,
                    data.near_rpc_client.clone(),
                    query_request,
                    "QUERY_FUNCTION_CALL",
                )
                .await
                {
                    crate::utils::capture_shadow_consistency_error!(err_code, "QUERY_FUNCTION_CALL")
                };
            }
            near_primitives::views::QueryRequest::ViewAccessKeyList { .. } => {
                if let Some(err_code) = crate::utils::shadow_compare_results_handler(
                    crate::metrics::QUERY_VIEW_ACCESS_KEYS_LIST_REQUESTS_TOTAL.get(),
                    data.shadow_data_consistency_rate,
                    &result,
                    data.near_rpc_client.clone(),
                    query_request,
                    "QUERY_VIEW_ACCESS_KEY_LIST",
                )
                .await
                {
                    crate::utils::capture_shadow_consistency_error!(
                        err_code,
                        "QUERY_VIEW_ACCESS_KEY_LIST"
                    )
                };
            }
        };
    }

    Ok(result.map_err(near_jsonrpc::primitives::errors::RpcError::from)?)
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn view_account(
    data: &Data<ServerContext>,
    block: CacheBlock,
    account_id: &near_primitives::types::AccountId,
    is_optimistic: bool,
) -> Result<
    near_jsonrpc::primitives::types::query::RpcQueryResponse,
    near_jsonrpc::primitives::types::query::RpcQueryError,
> {
    tracing::debug!(
        "`view_account` call. AccountID {}, Block {}, optimistic {}",
        account_id,
        block.block_height,
        is_optimistic
    );
    let account_view = if is_optimistic {
        optimistic_view_account(data, block, account_id).await?
    } else {
        database_view_account(data, block, account_id).await?
    };
    Ok(near_jsonrpc::primitives::types::query::RpcQueryResponse {
        kind: near_jsonrpc::primitives::types::query::QueryResponseKind::ViewAccount(account_view),
        block_height: block.block_height,
        block_hash: block.block_hash,
    })
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn optimistic_view_account(
    data: &Data<ServerContext>,
    block: CacheBlock,
    account_id: &near_primitives::types::AccountId,
) -> Result<
    near_primitives::views::AccountView,
    near_jsonrpc::primitives::types::query::RpcQueryError,
> {
    if let Ok(result) = data
        .blocks_info_by_finality
        .read()
        .await
        .optimistic_block
        .account_changes_in_block(account_id)
        .await
    {
        if let Some(account_view) = result {
            Ok(account_view)
        } else {
            Err(
                near_jsonrpc::primitives::types::query::RpcQueryError::UnknownAccount {
                    requested_account_id: account_id.clone(),
                    block_height: block.block_height,
                    block_hash: block.block_hash,
                },
            )
        }
    } else {
        database_view_account(data, block, account_id).await
    }
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn database_view_account(
    data: &Data<ServerContext>,
    block: CacheBlock,
    account_id: &near_primitives::types::AccountId,
) -> Result<
    near_primitives::views::AccountView,
    near_jsonrpc::primitives::types::query::RpcQueryError,
> {
    let account = data
        .db_manager
        .get_account(account_id, block.block_height)
        .await
        .map_err(
            |_err| near_jsonrpc::primitives::types::query::RpcQueryError::UnknownAccount {
                requested_account_id: account_id.clone(),
                block_height: block.block_height,
                block_hash: block.block_hash,
            },
        )?
        .data;
    Ok(near_primitives::views::AccountView::from(account))
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn view_code(
    data: &Data<ServerContext>,
    block: CacheBlock,
    account_id: &near_primitives::types::AccountId,
    is_optimistic: bool,
) -> Result<
    near_jsonrpc::primitives::types::query::RpcQueryResponse,
    near_jsonrpc::primitives::types::query::RpcQueryError,
> {
    tracing::debug!(
        "`view_code` call. AccountID {}, Block {}, optimistic {}",
        account_id,
        block.block_height,
        is_optimistic
    );
    let (code, account) = if is_optimistic {
        tokio::try_join!(
            optimistic_view_code(data, block, account_id),
            optimistic_view_account(data, block, account_id),
        )?
    } else {
        tokio::try_join!(
            database_view_code(data, block, account_id),
            database_view_account(data, block, account_id),
        )?
    };

    Ok(near_jsonrpc::primitives::types::query::RpcQueryResponse {
        kind: near_jsonrpc::primitives::types::query::QueryResponseKind::ViewCode(
            near_primitives::views::ContractCodeView {
                code,
                hash: account.code_hash,
            },
        ),
        block_height: block.block_height,
        block_hash: block.block_hash,
    })
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn optimistic_view_code(
    data: &Data<ServerContext>,
    block: CacheBlock,
    account_id: &near_primitives::types::AccountId,
) -> Result<Vec<u8>, near_jsonrpc::primitives::types::query::RpcQueryError> {
    let contract_code = if let Ok(result) = data
        .blocks_info_by_finality
        .read()
        .await
        .optimistic_block
        .code_changes_in_block(account_id)
        .await
    {
        if let Some(code) = result {
            code
        } else {
            return Err(
                near_jsonrpc::primitives::types::query::RpcQueryError::NoContractCode {
                    contract_account_id: account_id.clone(),
                    block_height: block.block_height,
                    block_hash: block.block_hash,
                },
            );
        }
    } else {
        database_view_code(data, block, account_id).await?
    };
    Ok(contract_code)
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn database_view_code(
    data: &Data<ServerContext>,
    block: CacheBlock,
    account_id: &near_primitives::types::AccountId,
) -> Result<Vec<u8>, near_jsonrpc::primitives::types::query::RpcQueryError> {
    Ok(data
        .db_manager
        .get_contract_code(account_id, block.block_height)
        .await
        .map_err(
            |_err| near_jsonrpc::primitives::types::query::RpcQueryError::NoContractCode {
                contract_account_id: account_id.clone(),
                block_height: block.block_height,
                block_hash: block.block_hash,
            },
        )?
        .data)
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn function_call(
    data: &Data<ServerContext>,
    block: CacheBlock,
    account_id: near_primitives::types::AccountId,
    method_name: &str,
    args: near_primitives::types::FunctionArgs,
    is_optimistic: bool,
) -> Result<
    near_jsonrpc::primitives::types::query::RpcQueryResponse,
    near_jsonrpc::primitives::types::query::RpcQueryError,
> {
    tracing::debug!(
        "`function_call` call. AccountID {}, block {}, method_name {}, args {:?}, optimistic {}",
        account_id,
        block.block_height,
        method_name,
        args,
        is_optimistic,
    );

    let call_results = if is_optimistic {
        optimistic_function_call(data, block, account_id, method_name, args).await
    } else {
        database_function_call(data, block, account_id, method_name, args).await
    };
    let call_results =
        call_results.map_err(|err| err.to_rpc_query_error(block.block_height, block.block_hash))?;
    Ok(near_jsonrpc::primitives::types::query::RpcQueryResponse {
        kind: near_jsonrpc::primitives::types::query::QueryResponseKind::CallResult(
            near_primitives::views::CallResult {
                result: call_results.result,
                logs: call_results.logs,
            },
        ),
        block_height: block.block_height,
        block_hash: block.block_hash,
    })
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn optimistic_function_call(
    data: &Data<ServerContext>,
    block: CacheBlock,
    account_id: near_primitives::types::AccountId,
    method_name: &str,
    args: near_primitives::types::FunctionArgs,
) -> Result<RunContractResponse, crate::errors::FunctionCallError> {
    let optimistic_data = data
        .blocks_info_by_finality
        .read()
        .await
        .optimistic_block
        .state_changes_in_block(&account_id, &[])
        .await;
    run_contract(
        account_id,
        method_name,
        args,
        data.db_manager.clone(),
        &data.compiled_contract_code_cache,
        &data.contract_code_cache,
        &data.blocks_info_by_finality,
        block,
        data.max_gas_burnt,
        optimistic_data, // run contract with optimistic data
    )
    .await
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn database_function_call(
    data: &Data<ServerContext>,
    block: CacheBlock,
    account_id: near_primitives::types::AccountId,
    method_name: &str,
    args: near_primitives::types::FunctionArgs,
) -> Result<RunContractResponse, crate::errors::FunctionCallError> {
    run_contract(
        account_id,
        method_name,
        args,
        data.db_manager.clone(),
        &data.compiled_contract_code_cache,
        &data.contract_code_cache,
        &data.blocks_info_by_finality,
        block,
        data.max_gas_burnt,
        Default::default(), // run contract with empty optimistic data
    )
    .await
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn view_state(
    data: &Data<ServerContext>,
    block: CacheBlock,
    account_id: &near_primitives::types::AccountId,
    prefix: &[u8],
    is_optimistic: bool,
) -> Result<
    near_jsonrpc::primitives::types::query::RpcQueryResponse,
    near_jsonrpc::primitives::types::query::RpcQueryError,
> {
    tracing::debug!(
        "`view_state` call. AccountID {}, block {}, prefix {:?}, optimistic {}",
        account_id,
        block.block_height,
        prefix,
        is_optimistic,
    );

    let state_item = if is_optimistic {
        optimistic_view_state(data, block, account_id, prefix).await?
    } else {
        database_view_state(data, block, account_id, prefix).await?
    };

    Ok(near_jsonrpc::primitives::types::query::RpcQueryResponse {
        kind: near_jsonrpc::primitives::types::query::QueryResponseKind::ViewState(
            near_primitives::views::ViewStateResult {
                values: state_item,
                proof: vec![], // TODO: this is hardcoded empty value since we don't support proofs yet
            },
        ),
        block_height: block.block_height,
        block_hash: block.block_hash,
    })
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn optimistic_view_state(
    data: &Data<ServerContext>,
    block: CacheBlock,
    account_id: &near_primitives::types::AccountId,
    prefix: &[u8],
) -> Result<
    Vec<near_primitives::views::StateItem>,
    near_jsonrpc::primitives::types::query::RpcQueryError,
> {
    let mut optimistic_data = data
        .blocks_info_by_finality
        .read()
        .await
        .optimistic_block
        .state_changes_in_block(account_id, prefix)
        .await;
    let state_from_db =
        get_state_keys_from_db(&data.db_manager, account_id, block.block_height, prefix).await;
    if state_from_db.is_empty() && optimistic_data.is_empty() {
        Err(
            near_jsonrpc::primitives::types::query::RpcQueryError::UnknownAccount {
                requested_account_id: account_id.clone(),
                block_height: block.block_height,
                block_hash: block.block_hash,
            },
        )
    } else {
        let mut values: Vec<near_primitives::views::StateItem> = state_from_db
            .into_iter()
            .filter_map(|(key, value)| {
                let value = if let Some(value) = optimistic_data.remove(&key) {
                    value.clone()
                } else {
                    Some(value)
                };
                value.map(|value| near_primitives::views::StateItem {
                    key: key.into(),
                    value: value.into(),
                })
            })
            .collect();
        let optimistic_items: Vec<near_primitives::views::StateItem> = optimistic_data
            .iter()
            .filter_map(|(key, value)| {
                value
                    .clone()
                    .map(|value| near_primitives::views::StateItem {
                        key: key.clone().into(),
                        value: value.clone().into(),
                    })
            })
            .collect();
        values.extend(optimistic_items);
        Ok(values)
    }
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn database_view_state(
    data: &Data<ServerContext>,
    block: CacheBlock,
    account_id: &near_primitives::types::AccountId,
    prefix: &[u8],
) -> Result<
    Vec<near_primitives::views::StateItem>,
    near_jsonrpc::primitives::types::query::RpcQueryError,
> {
    let state_from_db =
        get_state_keys_from_db(&data.db_manager, account_id, block.block_height, prefix).await;
    if state_from_db.is_empty() {
        Err(
            near_jsonrpc::primitives::types::query::RpcQueryError::UnknownAccount {
                requested_account_id: account_id.clone(),
                block_height: block.block_height,
                block_hash: block.block_hash,
            },
        )
    } else {
        let values: Vec<near_primitives::views::StateItem> = state_from_db
            .into_iter()
            .map(|(key, value)| near_primitives::views::StateItem {
                key: key.into(),
                value: value.into(),
            })
            .collect();
        Ok(values)
    }
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn view_access_key(
    data: &Data<ServerContext>,
    block: CacheBlock,
    account_id: &near_primitives::types::AccountId,
    public_key: near_crypto::PublicKey,
    is_optimistic: bool,
) -> Result<
    near_jsonrpc::primitives::types::query::RpcQueryResponse,
    near_jsonrpc::primitives::types::query::RpcQueryError,
> {
    tracing::debug!(
        "`view_access_key` call. AccountID {}, block {}, key_data {:?}, optimistic {}",
        account_id,
        block.block_height,
        public_key.to_string(),
        is_optimistic,
    );
    let access_key_view = if is_optimistic {
        optimistic_view_access_key(data, block, account_id, public_key).await?
    } else {
        database_view_access_key(data, block, account_id, public_key).await?
    };
    Ok(near_jsonrpc::primitives::types::query::RpcQueryResponse {
        kind: near_jsonrpc::primitives::types::query::QueryResponseKind::AccessKey(access_key_view),
        block_height: block.block_height,
        block_hash: block.block_hash,
    })
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn optimistic_view_access_key(
    data: &Data<ServerContext>,
    block: CacheBlock,
    account_id: &near_primitives::types::AccountId,
    public_key: near_crypto::PublicKey,
) -> Result<
    near_primitives::views::AccessKeyView,
    near_jsonrpc::primitives::types::query::RpcQueryError,
> {
    if let Ok(result) = data
        .blocks_info_by_finality
        .read()
        .await
        .optimistic_block
        .access_key_changes_in_block(account_id, &public_key)
        .await
    {
        if let Some(access_key) = result {
            Ok(access_key)
        } else {
            Err(
                near_jsonrpc::primitives::types::query::RpcQueryError::UnknownAccessKey {
                    public_key,
                    block_height: block.block_height,
                    block_hash: block.block_hash,
                },
            )
        }
    } else {
        database_view_access_key(data, block, account_id, public_key).await
    }
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn database_view_access_key(
    data: &Data<ServerContext>,
    block: CacheBlock,
    account_id: &near_primitives::types::AccountId,
    public_key: near_crypto::PublicKey,
) -> Result<
    near_primitives::views::AccessKeyView,
    near_jsonrpc::primitives::types::query::RpcQueryError,
> {
    let access_key = data
        .db_manager
        .get_access_key(account_id, block.block_height, public_key.clone())
        .await
        .map_err(
            |_err| near_jsonrpc::primitives::types::query::RpcQueryError::UnknownAccessKey {
                public_key,
                block_height: block.block_height,
                block_hash: block.block_hash,
            },
        )?
        .data;
    Ok(near_primitives::views::AccessKeyView::from(access_key))
}

#[cfg(feature = "account_access_keys")]
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn view_access_keys_list(
    data: &Data<ServerContext>,
    block: CacheBlock,
    account_id: &near_primitives::types::AccountId,
) -> Result<
    near_jsonrpc::primitives::types::query::RpcQueryResponse,
    near_jsonrpc::primitives::types::query::RpcQueryError,
> {
    tracing::debug!(
        "`view_access_key` call. AccountID {}, block {}",
        account_id,
        block.block_height,
    );

    let access_keys =
        fetch_list_access_keys_from_db(&data.db_manager, account_id, block.block_height)
            .await
            // TODO: review this once we implement the `account_access_keys` after the redesign
            // this error has to be the same the real NEAR JSON RPC returns in this case
            .map_err(|err| {
                near_jsonrpc::primitives::types::query::RpcQueryError::InternalError {
                    error_message: format!("Failed to fetch access keys: {}", err),
                }
            })?;

    Ok(near_jsonrpc::primitives::types::query::RpcQueryResponse {
        kind: near_jsonrpc::primitives::types::query::QueryResponseKind::AccessKeyList(
            near_primitives::views::AccessKeyList { keys: access_keys },
        ),
        block_height: block.block_height,
        block_hash: block.block_hash,
    })
}
