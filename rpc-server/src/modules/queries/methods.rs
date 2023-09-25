use crate::config::ServerContext;
use crate::errors::RPCError;
use crate::modules::blocks::utils::fetch_block_from_cache_or_get;
use crate::modules::blocks::CacheBlock;
#[cfg(feature = "account_access_keys")]
use crate::modules::queries::utils::fetch_list_access_keys_from_scylla_db;
use crate::modules::queries::utils::{fetch_state_from_scylla_db, run_contract};
use crate::utils::proxy_rpc_call;
#[cfg(feature = "shadow_data_consistency")]
use crate::utils::shadow_compare_results;
use jsonrpc_v2::{Data, Params};

/// `query` rpc method implementation
/// calls proxy_rpc_call to get `query` from near-rpc if request parameters not supported by read-rpc
/// as example: BlockReference for SyncCheckpoint is not supported by read-rpc
/// another way to get `query` from read-rpc using `query_call`
#[allow(unused_mut)]
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn query(
    data: Data<ServerContext>,
    Params(mut params): Params<near_jsonrpc_primitives::types::query::RpcQueryRequest>,
) -> Result<near_jsonrpc_primitives::types::query::RpcQueryResponse, RPCError> {
    match &params.block_reference {
        near_primitives::types::BlockReference::SyncCheckpoint(_) => {
            // Increase the SYNC_CHECKPOINT_REQUESTS_TOTAL metric if the request has
            // genesis sync_checkpoint or earliest_available sync_checkpoint
            // and proxy to near-rpc
            crate::metrics::SYNC_CHECKPOINT_REQUESTS_TOTAL.inc();
            Ok(proxy_rpc_call(&data.near_rpc_client, params).await?)
        }
        near_primitives::types::BlockReference::Finality(finality) => {
            if finality != &near_primitives::types::Finality::Final {
                // Increase the OPTIMISTIC_REQUESTS_TOTAL metric if the request has
                // optimistic finality or doom_slug finality
                // and proxy to near-rpc
                crate::metrics::OPTIMISTIC_REQUESTS_TOTAL.inc();
                Ok(proxy_rpc_call(&data.near_rpc_client, params).await?)
            } else {
                query_call(data, Params(params)).await
            }
        }
        near_primitives::types::BlockReference::BlockId(_) => {
            query_call(data, Params(params)).await
        }
    }
}

/// fetch query result from read-rpc
#[allow(unused_mut)]
#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn query_call(
    data: Data<ServerContext>,
    Params(mut params): Params<near_jsonrpc_primitives::types::query::RpcQueryRequest>,
) -> Result<near_jsonrpc_primitives::types::query::RpcQueryResponse, RPCError> {
    tracing::debug!("`query` call. Params: {:?}", params,);

    let block = fetch_block_from_cache_or_get(&data, params.block_reference.clone())
        .await
        .map_err(near_jsonrpc_primitives::errors::RpcError::from)?;
    let result = match params.request.clone() {
        near_primitives::views::QueryRequest::ViewAccount { account_id } => {
            crate::metrics::QUERY_VIEW_ACCOUNT_REQUESTS_TOTAL.inc();
            view_account(&data, block, &account_id).await
        }
        near_primitives::views::QueryRequest::ViewCode { account_id } => {
            crate::metrics::QUERY_VIEW_CODE_REQUESTS_TOTAL.inc();
            view_code(&data, block, &account_id).await
        }
        near_primitives::views::QueryRequest::ViewAccessKey {
            account_id,
            public_key,
        } => {
            crate::metrics::QUERY_VIEW_ACCESS_KEY_REQUESTS_TOTAL.inc();
            view_access_key(&data, block, &account_id, public_key).await
        }
        near_primitives::views::QueryRequest::ViewState {
            account_id,
            prefix,
            include_proof: _,
        } => {
            crate::metrics::QUERY_VIEW_STATE_REQUESTS_TOTAL.inc();
            view_state(&data, block, &account_id, prefix.as_ref()).await
        }
        near_primitives::views::QueryRequest::CallFunction {
            account_id,
            method_name,
            args,
        } => {
            crate::metrics::QUERY_FUNCTION_CALL_REQUESTS_TOTAL.inc();
            function_call(&data, block, account_id, &method_name, args.clone()).await
        }
        #[allow(unused_variables)]
        // `account_id` is used in the `#[cfg(feature = "account_access_keys")]` branch.
        near_primitives::views::QueryRequest::ViewAccessKeyList { account_id } => {
            crate::metrics::QUERY_VIEW_ACCESS_KEYS_LIST_REQUESTS_TOTAL.inc();
            #[cfg(not(feature = "account_access_keys"))]
            return Ok(crate::utils::proxy_rpc_call(&data.near_rpc_client, params).await?);
            #[cfg(feature = "account_access_keys")]
            {
                view_access_keys_list(&data, block, &account_id).await
            }
        }
    };

    #[cfg(feature = "shadow_data_consistency")]
    {
        let request_copy = params.request.clone();
        let meta_data = format!("{:?}", params);
        let near_rpc_client = data.near_rpc_client.clone();

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
        params.block_reference = near_primitives::types::BlockReference::from(
            near_primitives::types::BlockId::Height(block_height),
        );

        let (read_rpc_response_json, is_response_ok) = match &result {
            Ok(res) => (serde_json::to_value(res), true),
            Err(err) => (serde_json::to_value(err), false),
        };

        let comparison_result = shadow_compare_results(
            read_rpc_response_json,
            near_rpc_client,
            params,
            is_response_ok,
        )
        .await;

        match comparison_result {
            Ok(_) => {
                tracing::info!(
                    target: "shadow_data_consistency",
                    "Shadow data check: CORRECT\n{}",
                    format!("QUERY: {:?}", meta_data)
                );
            }
            Err(err) => {
                // When the data check fails, we want to emit the log message and increment the
                // corresponding metric. Despite the metrics have "proxies" in their names, we
                // are not proxying the requests anymore and respond with the error to the client.
                // Since we already have the dashboard using these metric names, we don't want to
                // change them and reuse them for the observability of the shadow data consistency checks.
                match request_copy {
                    near_primitives::views::QueryRequest::ViewAccount { .. } => {
                        crate::utils::capture_shadow_consistency_error!(
                            err,
                            meta_data,
                            "QUERY_VIEW_ACCOUNT"
                        );
                    }
                    near_primitives::views::QueryRequest::ViewCode { .. } => {
                        crate::utils::capture_shadow_consistency_error!(
                            err,
                            meta_data,
                            "QUERY_VIEW_CODE"
                        );
                    }
                    near_primitives::views::QueryRequest::ViewAccessKey { .. } => {
                        crate::utils::capture_shadow_consistency_error!(
                            err,
                            meta_data,
                            "QUERY_VIEW_ACCESS_KEY"
                        );
                    }
                    near_primitives::views::QueryRequest::ViewState { .. } => {
                        crate::utils::capture_shadow_consistency_error!(
                            err,
                            meta_data,
                            "QUERY_VIEW_STATE"
                        );
                    }
                    near_primitives::views::QueryRequest::CallFunction { .. } => {
                        crate::utils::capture_shadow_consistency_error!(
                            err,
                            meta_data,
                            "QUERY_FUNCTION_CALL"
                        );
                    }
                    near_primitives::views::QueryRequest::ViewAccessKeyList { .. } => {
                        crate::utils::capture_shadow_consistency_error!(
                            err,
                            meta_data,
                            "QUERY_VIEW_ACCESS_KEY_LIST"
                        );
                    }
                };
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
        .map_err(
            |_err| near_jsonrpc_primitives::types::query::RpcQueryError::UnknownAccount {
                requested_account_id: account_id.clone(),
                block_height: block.block_height,
                block_hash: block.block_hash,
            },
        )?;

    Ok(near_jsonrpc_primitives::types::query::RpcQueryResponse {
        kind: near_jsonrpc_primitives::types::query::QueryResponseKind::ViewAccount(
            near_primitives::views::AccountView::from(account.data),
        ),
        block_height: account.block_height,
        block_hash: account.block_hash,
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
    let contract = data
        .scylla_db_manager
        .get_account(account_id, block.block_height)
        .await
        .map_err(
            |_err| near_jsonrpc_primitives::types::query::RpcQueryError::UnknownAccount {
                requested_account_id: account_id.clone(),
                block_height: block.block_height,
                block_hash: block.block_hash,
            },
        )?;
    let contract_code = data
        .scylla_db_manager
        .get_contract_code(account_id, block.block_height)
        .await
        .map_err(
            |_err| near_jsonrpc_primitives::types::query::RpcQueryError::NoContractCode {
                contract_account_id: account_id.clone(),
                block_height: block.block_height,
                block_hash: block.block_hash,
            },
        )?;
    Ok(near_jsonrpc_primitives::types::query::RpcQueryResponse {
        kind: near_jsonrpc_primitives::types::query::QueryResponseKind::ViewCode(
            near_primitives::views::ContractCodeView::from(
                near_primitives::contract::ContractCode::new(
                    contract_code.data,
                    Some(contract.data.code_hash()),
                ),
            ),
        ),
        block_height: contract.block_height,
        block_hash: contract.block_hash,
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
        block,
        data.max_gas_burnt,
    )
    .await
    .map_err(|err| err.to_rpc_query_error(block.block_height, block.block_hash))?;
    Ok(near_jsonrpc_primitives::types::query::RpcQueryResponse {
        kind: near_jsonrpc_primitives::types::query::QueryResponseKind::CallResult(
            near_primitives::views::CallResult {
                result: call_results.result,
                logs: call_results.logs,
            },
        ),
        block_height: call_results.block_height,
        block_hash: call_results.block_hash,
    })
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
    .map_err(
        |_err| near_jsonrpc_primitives::types::query::RpcQueryError::UnknownAccount {
            requested_account_id: account_id.clone(),
            block_height: block.block_height,
            block_hash: block.block_hash,
        },
    )?;

    Ok(near_jsonrpc_primitives::types::query::RpcQueryResponse {
        kind: near_jsonrpc_primitives::types::query::QueryResponseKind::ViewState(contract_state),
        // We cannot return the block height and hash for the state, since different state keys
        // can be from different blocks.
        // We return the block height and hash for the requested block instead.
        block_height: block.block_height,
        block_hash: block.block_hash,
    })
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
async fn view_access_key(
    data: &Data<ServerContext>,
    block: CacheBlock,
    account_id: &near_primitives::types::AccountId,
    public_key: near_crypto::PublicKey,
) -> Result<
    near_jsonrpc_primitives::types::query::RpcQueryResponse,
    near_jsonrpc_primitives::types::query::RpcQueryError,
> {
    tracing::debug!(
        "`view_access_key` call. AccountID {}, block {}, key_data {:?}",
        account_id,
        block.block_height,
        public_key.to_string(),
    );

    let access_key = data
        .scylla_db_manager
        .get_access_key(account_id, block.block_height, public_key.clone())
        .await
        .map_err(
            |_err| near_jsonrpc_primitives::types::query::RpcQueryError::UnknownAccessKey {
                public_key,
                block_height: block.block_height,
                block_hash: block.block_hash,
            },
        )?;

    Ok(near_jsonrpc_primitives::types::query::RpcQueryResponse {
        kind: near_jsonrpc_primitives::types::query::QueryResponseKind::AccessKey(
            near_primitives::views::AccessKeyView::from(access_key.data),
        ),
        block_height: access_key.block_height,
        block_hash: access_key.block_hash,
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
