use actix_web::web::Data;

use crate::config::ServerContext;
use crate::modules::blocks::utils::fetch_block_from_cache_or_get;

use super::contract_runner;
use super::utils::get_state_from_db;

/// `query` rpc method implementation
/// calls proxy_rpc_call to get `query` from near-rpc if request parameters not supported by read-rpc
/// as example: BlockReference for Finality::None is not supported by read-rpc when near_data is not running
/// another way to get `query` from read-rpc using `query_call`
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn query(
    data: Data<ServerContext>,
    request_data: near_jsonrpc::primitives::types::query::RpcQueryRequest,
) -> Result<
    near_jsonrpc::primitives::types::query::RpcQueryResponse,
    near_jsonrpc::primitives::types::query::RpcQueryError,
> {
    // When the data check fails, we want to emit the log message and increment the
    // corresponding metric. Despite the metrics have "proxies" in their names, we
    // are not proxying the requests anymore and respond with the error to the client.
    // Since we already have the dashboard using these metric names, we don't want to
    // change them and reuse them for the observability of the shadow data consistency checks.
    let method_name = match &request_data.request {
        near_primitives::views::QueryRequest::ViewAccount { .. } => "query_view_account",
        near_primitives::views::QueryRequest::ViewCode { .. } => "query_view_code",
        near_primitives::views::QueryRequest::ViewAccessKey { .. } => "query_view_access_key",
        near_primitives::views::QueryRequest::ViewState { .. } => "query_view_state",
        near_primitives::views::QueryRequest::CallFunction { .. } => "query_call_function",
        near_primitives::views::QueryRequest::ViewAccessKeyList { .. } => {
            "query_view_access_key_list"
        }
    };
    // increase query method calls counter
    crate::metrics::METHOD_CALLS_COUNTER
        .with_label_values(&[method_name])
        .inc();

    if let near_primitives::types::BlockReference::Finality(
        near_primitives::types::Finality::None,
    ) = &request_data.block_reference
    {
        return if crate::metrics::OPTIMISTIC_UPDATING.is_not_working() {
            // Proxy if the optimistic updating is not working
            Ok(data
                .near_rpc_client
                .call(request_data, Some(method_name))
                .await
                .map_err(|err| {
                    err.handler_error().cloned().unwrap_or(
                        near_jsonrpc::primitives::types::query::RpcQueryError::InternalError {
                            error_message: err.to_string(),
                        },
                    )
                })?)
        } else {
            // query_call with optimistic block
            query_call(&data, request_data, method_name, true).await
        };
    };

    query_call(&data, request_data, method_name, false).await
}

/// fetch query result from read-rpc
#[allow(unused_mut)]
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn query_call(
    data: &Data<ServerContext>,
    mut query_request: near_jsonrpc::primitives::types::query::RpcQueryRequest,
    method_name: &str,
    is_optimistic: bool,
) -> Result<
    near_jsonrpc::primitives::types::query::RpcQueryResponse,
    near_jsonrpc::primitives::types::query::RpcQueryError,
> {
    tracing::debug!("`query` call. Params: {:?}", query_request,);
    let block = fetch_block_from_cache_or_get(data, &query_request.block_reference, method_name)
        .await
        .map_err(
            |_err| near_jsonrpc::primitives::types::query::RpcQueryError::UnknownBlock {
                block_reference: query_request.block_reference.clone(),
            },
        )?;

    let result = match &query_request.request {
        near_primitives::views::QueryRequest::ViewAccount { account_id } => {
            view_account(data, &block, account_id, is_optimistic).await
        }
        near_primitives::views::QueryRequest::ViewCode { account_id } => {
            view_code(data, &block, account_id, is_optimistic).await
        }
        near_primitives::views::QueryRequest::ViewAccessKey {
            account_id,
            public_key,
        } => view_access_key(data, &block, account_id, public_key, is_optimistic).await,
        near_primitives::views::QueryRequest::ViewState {
            account_id,
            prefix,
            include_proof,
        } => {
            if *include_proof {
                // TODO: We can calculate the proof for state only on regular or archival nodes.
                let final_block = data.blocks_info_by_finality.final_block_view().await;
                // `expected_earliest_available_block` calculated by formula:
                // `final_block_height` - `node_epoch_count` * `epoch_length`
                // Now near store 5 epochs, it can be changed in the future
                // epoch_length = 43200 blocks
                let expected_earliest_available_block =
                    final_block.header.height - 5 * data.genesis_info.genesis_config.epoch_length;
                return if block.header.height > expected_earliest_available_block {
                    // Proxy to regular rpc if the block is available
                    Ok(data
                        .near_rpc_client
                        .call(query_request, Some("query_view_state_proofs"))
                        .await
                        .map_err(|err| {
                            err.handler_error().cloned().unwrap_or(
                            near_jsonrpc::primitives::types::query::RpcQueryError::InternalError {
                                error_message: err.to_string(),
                            },
                        )
                        })?)
                } else {
                    // Proxy to archival rpc if the block garbage collected
                    Ok(data
                        .near_rpc_client
                        .archival_call(query_request, Some("query_view_state_proofs"))
                        .await
                        .map_err(|err| {
                            err.handler_error().cloned().unwrap_or(
                            near_jsonrpc::primitives::types::query::RpcQueryError::InternalError {
                                error_message: err.to_string(),
                            },
                        )
                        })?)
                };
            } else {
                view_state(data, &block, account_id, prefix, is_optimistic).await
            }
        }
        near_primitives::views::QueryRequest::CallFunction {
            account_id,
            method_name,
            args,
        } => function_call(data, &block, account_id, method_name, args, is_optimistic).await,
        near_primitives::views::QueryRequest::ViewAccessKeyList { account_id } => {
            view_access_keys_list(data, &block, account_id).await
        }
    };

    #[cfg(feature = "shadow-data-consistency")]
    {
        // Since we do queries with the clause WHERE block_height <= X, we need to
        // make sure that the block we are doing a shadow data consistency check for
        // matches the one we got the result for.
        // That's why we are using the block_height from the result.
        let block_height = match &result {
            Ok(res) => res.block_height,
            // If the result is an error it does not contain the block_height, so we
            // will use the block_height considered as final from the cache.
            Err(_err) => block.header.height,
        };
        query_request.block_reference = near_primitives::types::BlockReference::from(
            near_primitives::types::BlockId::Height(block_height),
        );

        crate::utils::shadow_compare_results_handler(
            data.shadow_data_consistency_rate,
            &result,
            data.near_rpc_client.clone(),
            query_request,
            method_name,
        )
        .await;
    }

    result
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn view_account(
    data: &Data<ServerContext>,
    block: &near_primitives::views::BlockView,
    account_id: &near_primitives::types::AccountId,
    is_optimistic: bool,
) -> Result<
    near_jsonrpc::primitives::types::query::RpcQueryResponse,
    near_jsonrpc::primitives::types::query::RpcQueryError,
> {
    tracing::debug!(
        "`view_account` call. AccountID {}, Block {}, optimistic {}",
        account_id,
        block.header.height,
        is_optimistic
    );
    let account_view = if is_optimistic {
        optimistic_view_account(data, block, account_id, "query_view_account").await?
    } else {
        database_view_account(data, block, account_id, "query_view_account").await?
    };
    Ok(near_jsonrpc::primitives::types::query::RpcQueryResponse {
        kind: near_jsonrpc::primitives::types::query::QueryResponseKind::ViewAccount(account_view),
        block_height: block.header.height,
        block_hash: block.header.hash,
    })
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn optimistic_view_account(
    data: &Data<ServerContext>,
    block: &near_primitives::views::BlockView,
    account_id: &near_primitives::types::AccountId,
    method_name: &str,
) -> Result<
    near_primitives::views::AccountView,
    near_jsonrpc::primitives::types::query::RpcQueryError,
> {
    if let Ok(result) = data
        .blocks_info_by_finality
        .optimistic_account_changes_in_block(account_id)
        .await
    {
        if let Some(account_view) = result {
            Ok(account_view)
        } else {
            Err(
                near_jsonrpc::primitives::types::query::RpcQueryError::UnknownAccount {
                    requested_account_id: account_id.clone(),
                    block_height: block.header.height,
                    block_hash: block.header.hash,
                },
            )
        }
    } else {
        database_view_account(data, block, account_id, method_name).await
    }
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn database_view_account(
    data: &Data<ServerContext>,
    block: &near_primitives::views::BlockView,
    account_id: &near_primitives::types::AccountId,
    method_name: &str,
) -> Result<
    near_primitives::views::AccountView,
    near_jsonrpc::primitives::types::query::RpcQueryError,
> {
    let account = data
        .db_manager
        .get_account(account_id, block.header.height, method_name)
        .await
        .map_err(
            |_err| near_jsonrpc::primitives::types::query::RpcQueryError::UnknownAccount {
                requested_account_id: account_id.clone(),
                block_height: block.header.height,
                block_hash: block.header.hash,
            },
        )?
        .data;
    Ok(near_primitives::views::AccountView::from(account))
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn view_code(
    data: &Data<ServerContext>,
    block: &near_primitives::views::BlockView,
    account_id: &near_primitives::types::AccountId,
    is_optimistic: bool,
) -> Result<
    near_jsonrpc::primitives::types::query::RpcQueryResponse,
    near_jsonrpc::primitives::types::query::RpcQueryError,
> {
    tracing::debug!(
        "`view_code` call. AccountID {}, Block {}, optimistic {}",
        account_id,
        block.header.height,
        is_optimistic
    );
    let (code, account) = if is_optimistic {
        futures::try_join!(
            optimistic_view_code(data, block, account_id, "query_view_code"),
            optimistic_view_account(data, block, account_id, "query_view_code"),
        )?
    } else {
        futures::try_join!(
            database_view_code(data, block, account_id, "query_view_code"),
            database_view_account(data, block, account_id, "query_view_code"),
        )?
    };

    Ok(near_jsonrpc::primitives::types::query::RpcQueryResponse {
        kind: near_jsonrpc::primitives::types::query::QueryResponseKind::ViewCode(
            near_primitives::views::ContractCodeView {
                code,
                hash: account.code_hash,
            },
        ),
        block_height: block.header.height,
        block_hash: block.header.hash,
    })
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn optimistic_view_code(
    data: &Data<ServerContext>,
    block: &near_primitives::views::BlockView,
    account_id: &near_primitives::types::AccountId,
    method_name: &str,
) -> Result<Vec<u8>, near_jsonrpc::primitives::types::query::RpcQueryError> {
    let contract_code = if let Ok(result) = data
        .blocks_info_by_finality
        .optimistic_code_changes_in_block(account_id)
        .await
    {
        if let Some(code) = result {
            code
        } else {
            return Err(
                near_jsonrpc::primitives::types::query::RpcQueryError::NoContractCode {
                    contract_account_id: account_id.clone(),
                    block_height: block.header.height,
                    block_hash: block.header.hash,
                },
            );
        }
    } else {
        database_view_code(data, block, account_id, method_name).await?
    };
    Ok(contract_code)
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn database_view_code(
    data: &Data<ServerContext>,
    block: &near_primitives::views::BlockView,
    account_id: &near_primitives::types::AccountId,
    method_name: &str,
) -> Result<Vec<u8>, near_jsonrpc::primitives::types::query::RpcQueryError> {
    Ok(data
        .db_manager
        .get_contract_code(account_id, block.header.height, method_name)
        .await
        .map_err(
            |_err| near_jsonrpc::primitives::types::query::RpcQueryError::NoContractCode {
                contract_account_id: account_id.clone(),
                block_height: block.header.height,
                block_hash: block.header.hash,
            },
        )?
        .data)
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn function_call(
    data: &Data<ServerContext>,
    block: &near_primitives::views::BlockView,
    account_id: &near_primitives::types::AccountId,
    method_name: &str,
    args: &near_primitives::types::FunctionArgs,
    is_optimistic: bool,
) -> Result<
    near_jsonrpc::primitives::types::query::RpcQueryResponse,
    near_jsonrpc::primitives::types::query::RpcQueryError,
> {
    tracing::debug!(
        "`function_call` call. AccountID {}, block {}, method_name {}, args {:?}, optimistic {}",
        account_id,
        block.header.height,
        method_name,
        args,
        is_optimistic,
    );

    // Depending on the optimistic flag we need to run the contract with the optimistic
    // state changes or not.
    let maybe_optimistic_data = if is_optimistic {
        data.blocks_info_by_finality
            .optimistic_state_changes_in_block(account_id, &[])
            .await
    } else {
        Default::default()
    };

    let call_results = contract_runner::run_contract(
        account_id,
        method_name,
        args,
        &data.db_manager,
        &data.compiled_contract_code_cache,
        &data.contract_code_cache,
        &data.blocks_info_by_finality,
        block,
        data.max_gas_burnt,
        maybe_optimistic_data,
        data.prefetch_state_size_limit,
    )
    .await?;

    Ok(near_jsonrpc::primitives::types::query::RpcQueryResponse {
        kind: near_jsonrpc::primitives::types::query::QueryResponseKind::CallResult(
            near_primitives::views::CallResult {
                result: call_results.result,
                logs: call_results.logs,
            },
        ),
        block_height: block.header.height,
        block_hash: block.header.hash,
    })
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn view_state(
    data: &Data<ServerContext>,
    block: &near_primitives::views::BlockView,
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
        block.header.height,
        prefix,
        is_optimistic,
    );

    let account = data
        .db_manager
        .get_account(account_id, block.header.height, "query_view_state")
        .await
        .map_err(
            |_err| near_jsonrpc::primitives::types::query::RpcQueryError::UnknownAccount {
                requested_account_id: account_id.clone(),
                block_height: block.header.height,
                block_hash: block.header.hash,
            },
        )?;

    // Calculate the state size excluding the contract code size to check if it's too large to fetch.
    // The state size is the storage usage minus the code size.
    // more details: nearcore/runtime/runtime/src/state_viewer/mod.rs:150
    let code_len = data
        .db_manager
        .get_contract_code(account_id, block.header.height, "query_view_state")
        .await
        .map(|code| code.data.len() as u64)
        .unwrap_or_default();
    let state_size = account.data.storage_usage().saturating_sub(code_len);
    // If the prefix is empty and the state size is larger than the limit, return an error.
    if prefix.is_empty() && state_size > data.prefetch_state_size_limit {
        return Err(
            near_jsonrpc::primitives::types::query::RpcQueryError::TooLargeContractState {
                contract_account_id: account_id.clone(),
                block_height: block.header.height,
                block_hash: block.header.hash,
            },
        );
    }

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
        block_height: block.header.height,
        block_hash: block.header.hash,
    })
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn optimistic_view_state(
    data: &Data<ServerContext>,
    block: &near_primitives::views::BlockView,
    account_id: &near_primitives::types::AccountId,
    prefix: &[u8],
) -> Result<
    Vec<near_primitives::views::StateItem>,
    near_jsonrpc::primitives::types::query::RpcQueryError,
> {
    let mut optimistic_data = data
        .blocks_info_by_finality
        .optimistic_state_changes_in_block(account_id, prefix)
        .await;
    let state_from_db = get_state_from_db(
        &data.db_manager,
        account_id,
        block.header.height,
        prefix,
        "query_view_state",
    )
    .await;

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

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn database_view_state(
    data: &Data<ServerContext>,
    block: &near_primitives::views::BlockView,
    account_id: &near_primitives::types::AccountId,
    prefix: &[u8],
) -> Result<
    Vec<near_primitives::views::StateItem>,
    near_jsonrpc::primitives::types::query::RpcQueryError,
> {
    let state_from_db = get_state_from_db(
        &data.db_manager,
        account_id,
        block.header.height,
        prefix,
        "query_view_state",
    )
    .await;

    let values: Vec<near_primitives::views::StateItem> = state_from_db
        .into_iter()
        .map(|(key, value)| near_primitives::views::StateItem {
            key: key.into(),
            value: value.into(),
        })
        .collect();
    Ok(values)
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn view_access_key(
    data: &Data<ServerContext>,
    block: &near_primitives::views::BlockView,
    account_id: &near_primitives::types::AccountId,
    public_key: &near_crypto::PublicKey,
    is_optimistic: bool,
) -> Result<
    near_jsonrpc::primitives::types::query::RpcQueryResponse,
    near_jsonrpc::primitives::types::query::RpcQueryError,
> {
    tracing::debug!(
        "`view_access_key` call. AccountID {}, block {}, key_data {:?}, optimistic {}",
        account_id,
        block.header.height,
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
        block_height: block.header.height,
        block_hash: block.header.hash,
    })
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn optimistic_view_access_key(
    data: &Data<ServerContext>,
    block: &near_primitives::views::BlockView,
    account_id: &near_primitives::types::AccountId,
    public_key: &near_crypto::PublicKey,
) -> Result<
    near_primitives::views::AccessKeyView,
    near_jsonrpc::primitives::types::query::RpcQueryError,
> {
    if let Ok(result) = data
        .blocks_info_by_finality
        .optimistic_access_key_changes_in_block(account_id, public_key)
        .await
    {
        if let Some(access_key) = result {
            Ok(access_key)
        } else {
            Err(
                near_jsonrpc::primitives::types::query::RpcQueryError::UnknownAccessKey {
                    public_key: public_key.clone(),
                    block_height: block.header.height,
                    block_hash: block.header.hash,
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
    block: &near_primitives::views::BlockView,
    account_id: &near_primitives::types::AccountId,
    public_key: &near_crypto::PublicKey,
) -> Result<
    near_primitives::views::AccessKeyView,
    near_jsonrpc::primitives::types::query::RpcQueryError,
> {
    let access_key = data
        .db_manager
        .get_access_key(
            account_id,
            block.header.height,
            public_key.clone(),
            "query_view_access_key",
        )
        .await
        .map_err(
            |_err| near_jsonrpc::primitives::types::query::RpcQueryError::UnknownAccessKey {
                public_key: public_key.clone(),
                block_height: block.header.height,
                block_hash: block.header.hash,
            },
        )?
        .data;
    Ok(near_primitives::views::AccessKeyView::from(access_key))
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn view_access_keys_list(
    data: &Data<ServerContext>,
    block: &near_primitives::views::BlockView,
    account_id: &near_primitives::types::AccountId,
) -> Result<
    near_jsonrpc::primitives::types::query::RpcQueryResponse,
    near_jsonrpc::primitives::types::query::RpcQueryError,
> {
    tracing::debug!(
        "`view_access_key` call. AccountID {}, block {}",
        account_id,
        block.header.height,
    );

    let access_keys = data
        .db_manager
        .get_account_access_keys(
            account_id,
            block.header.height,
            "query_view_access_key_list",
        )
        .await
        .map_err(
            |err| near_jsonrpc::primitives::types::query::RpcQueryError::InternalError {
                error_message: format!("Failed to fetch access keys: {}", err),
            },
        )?;

    Ok(near_jsonrpc::primitives::types::query::RpcQueryResponse {
        kind: near_jsonrpc::primitives::types::query::QueryResponseKind::AccessKeyList(
            near_primitives::views::AccessKeyList { keys: access_keys },
        ),
        block_height: block.header.height,
        block_hash: block.header.hash,
    })
}
