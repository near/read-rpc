use std::collections::HashMap;

use futures::StreamExt;
use near_vm_runner::internal::VMKindExt;
use near_vm_runner::ContractRuntimeCache;

use crate::errors::FunctionCallError;
use crate::modules::blocks::BlocksInfoByFinality;
use crate::modules::queries::CodeStorage;

pub struct RunContractResponse {
    pub result: Vec<u8>,
    pub logs: Vec<String>,
    pub block_height: near_primitives::types::BlockHeight,
    pub block_hash: near_primitives::hash::CryptoHash,
}

// Function to get state key value from the database
// This function to wrap the database call to get state key value
// It is using for debug in jaeger tracing
#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(db_manager))
)]
pub async fn get_state_key_value_from_db(
    db_manager: &std::sync::Arc<Box<dyn database::ReaderDbManager + Sync + Send + 'static>>,
    account_id: &near_primitives::types::AccountId,
    block_height: near_primitives::types::BlockHeight,
    key_data: readnode_primitives::StateKey,
) -> (
    readnode_primitives::StateKey,
    readnode_primitives::StateValue,
) {
    db_manager
        .get_state_key_value(account_id, block_height, key_data)
        .await
}

// Function to get state keys from the database
// This function to wrap the database call to get state keys
// It is using for debug in jaeger tracing
#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(db_manager))
)]
pub async fn get_state_keys_from_db(
    db_manager: &std::sync::Arc<Box<dyn database::ReaderDbManager + Sync + Send + 'static>>,
    account_id: &near_primitives::types::AccountId,
    prefix: &[u8],
) -> anyhow::Result<Vec<readnode_primitives::StateKey>> {
    if !prefix.is_empty() {
        db_manager
            .get_state_keys_by_prefix(account_id, prefix)
            .await
    } else {
        db_manager.get_state_keys_all(account_id).await
    }
}

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(db_manager))
)]
pub async fn get_state_from_db(
    db_manager: &std::sync::Arc<Box<dyn database::ReaderDbManager + Sync + Send + 'static>>,
    account_id: &near_primitives::types::AccountId,
    block_height: near_primitives::types::BlockHeight,
    prefix: &[u8],
    method_name: &str,
) -> HashMap<readnode_primitives::StateKey, readnode_primitives::StateValue> {
    tracing::debug!(
        "`get_state_from_db` call. AccountId {}, block {}, prefix {:?}",
        account_id,
        block_height,
        prefix,
    );
    let mut data: HashMap<readnode_primitives::StateKey, readnode_primitives::StateValue> =
        HashMap::new();
    crate::metrics::SCYLLA_QUERIES
        .with_label_values(&[method_name, "state_indexer.account_state"])
        .inc();
    match get_state_keys_from_db(db_manager, account_id, prefix).await {
        Ok(state_keys) => {
            // 3 nodes * 8 cpus * 100 = 2400
            // TODO: 2400 is hardcoded value. Make it configurable.
            for state_keys_chunk in state_keys.chunks(2400) {
                let futures = state_keys_chunk.iter().map(|state_key| {
                    get_state_key_value_from_db(
                        db_manager,
                        account_id,
                        block_height,
                        state_key.clone(),
                    )
                });
                let mut tasks = futures::stream::FuturesUnordered::from_iter(futures);
                while let Some((state_key, state_value)) = tasks.next().await {
                    crate::metrics::SCYLLA_QUERIES
                        .with_label_values(&[method_name, "state_indexer.state_changes_data"])
                        .inc();
                    if !state_value.is_empty() {
                        data.insert(state_key, state_value);
                    }
                }
            }
            data
        }
        Err(_) => data,
    }
}

#[cfg(feature = "account_access_keys")]
#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(db_manager))
)]
pub async fn fetch_list_access_keys_from_db(
    db_manager: &std::sync::Arc<Box<dyn database::ReaderDbManager + Sync + Send + 'static>>,
    account_id: &near_primitives::types::AccountId,
    block_height: near_primitives::types::BlockHeight,
) -> anyhow::Result<Vec<near_primitives::views::AccessKeyInfoView>> {
    tracing::debug!(
        "`fetch_list_access_keys_from_db` call. AccountID {}, block {}",
        account_id,
        block_height,
    );
    let account_keys = db_manager
        .get_account_access_keys(account_id, block_height)
        .await?;
    let account_keys_view = account_keys
        .into_iter()
        .map(
            |(public_key_hex, access_key)| near_primitives::views::AccessKeyInfoView {
                public_key: borsh::from_slice::<near_crypto::PublicKey>(
                    &hex::decode(public_key_hex).unwrap(),
                )
                .unwrap(),
                access_key: near_primitives::views::AccessKeyView::from(
                    borsh::from_slice::<near_primitives::account::AccessKey>(&access_key).unwrap(),
                ),
            },
        )
        .collect();
    Ok(account_keys_view)
}

#[allow(clippy::too_many_arguments)]
#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(context, code_storage, contract_code, compiled_contract_code_cache))
)]
async fn run_code_in_vm_runner(
    code_hash: near_primitives::hash::CryptoHash,
    contract_code: Option<near_vm_runner::ContractCode>,
    method_name: String,
    context: near_vm_runner::logic::VMContext,
    mut code_storage: CodeStorage,
    vm_config: near_parameters::vm::Config,
    compiled_contract_code_cache: &std::sync::Arc<crate::config::CompiledCodeCache>,
) -> Result<near_vm_runner::logic::VMOutcome, near_vm_runner::logic::errors::VMRunnerError> {
    let compiled_contract_code_cache_handle = compiled_contract_code_cache.handle();
    let span = tracing::debug_span!("run_code_in_vm_runner");

    let results = tokio::task::spawn_blocking(move || {
        let _entered = span.entered();
        let promise_results = vec![];
        let fees = near_parameters::RuntimeFeesConfig::free();

        let runtime = vm_config
            .vm_kind
            .runtime(vm_config.clone())
            .expect("runtime has not been enabled at compile time");

        if let Some(code) = &contract_code {
            runtime
                .precompile(code, &compiled_contract_code_cache_handle)
                .expect("Compilation failed")
                .expect("Cache failed");
        };

        runtime.run(
            code_hash,
            None,
            &method_name,
            &mut code_storage,
            &context,
            &fees,
            &promise_results,
            Some(&compiled_contract_code_cache_handle),
        )
    })
    .await;
    match results {
        Ok(result) => result,
        Err(err) => Err(
            near_vm_runner::logic::errors::VMRunnerError::WasmUnknownError {
                debug_message: format!("Failed to run contract: {:?}", err),
            },
        ),
    }
}

#[allow(clippy::too_many_arguments)]
#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(db_manager, compiled_contract_code_cache, contract_code_cache))
)]
pub async fn run_contract(
    account_id: &near_primitives::types::AccountId,
    method_name: &str,
    args: &near_primitives::types::FunctionArgs,
    db_manager: &std::sync::Arc<Box<dyn database::ReaderDbManager + Sync + Send + 'static>>,
    compiled_contract_code_cache: &std::sync::Arc<crate::config::CompiledCodeCache>,
    contract_code_cache: &std::sync::Arc<
        crate::cache::RwLockLruMemoryCache<near_primitives::hash::CryptoHash, Vec<u8>>,
    >,
    blocks_info_by_finality: &std::sync::Arc<BlocksInfoByFinality>,
    block: crate::modules::blocks::CacheBlock,
    max_gas_burnt: near_primitives::types::Gas,
    optimistic_data: HashMap<
        readnode_primitives::StateKey,
        Option<readnode_primitives::StateValue>,
    >,
) -> Result<RunContractResponse, FunctionCallError> {
    crate::metrics::SCYLLA_QUERIES
        .with_label_values(&["query_call_function", "state_indexer.state_changes_account"])
        .inc();
    let contract = db_manager
        .get_account(account_id, block.block_height)
        .await
        .map_err(|_| FunctionCallError::AccountDoesNotExist {
            requested_account_id: account_id.clone(),
        })?;

    let (epoch_height, epoch_validators) =
        if blocks_info_by_finality.final_cache_block().await.epoch_id == block.epoch_id {
            let validators = blocks_info_by_finality.validators().await;
            (validators.epoch_height, validators.current_validators)
        } else {
            crate::metrics::SCYLLA_QUERIES
                .with_label_values(&["query_call_function", "state_indexer.validators"])
                .inc();
            let validators = db_manager
                .get_validators_by_epoch_id(block.epoch_id)
                .await
                .map_err(|_| FunctionCallError::InternalError {
                    error_message: "Failed to get epoch info".to_string(),
                })?;
            (
                validators.epoch_height,
                validators.validators_info.current_validators,
            )
        };
    let validators = epoch_validators
        .iter()
        .map(|validator| (validator.account_id.clone(), validator.stake))
        .collect();

    // Prepare context for the VM run contract
    let public_key = near_crypto::PublicKey::empty(near_crypto::KeyType::ED25519);
    let random_seed = near_primitives::utils::create_random_seed(
        block.latest_protocol_version,
        near_primitives::hash::CryptoHash::default(),
        block.state_root,
    );
    let context = near_vm_runner::logic::VMContext {
        current_account_id: account_id.clone(),
        signer_account_id: account_id.clone(),
        signer_account_pk: borsh::to_vec(&public_key).expect("Failed to serialize"),
        predecessor_account_id: account_id.clone(),
        input: args.to_vec(),
        block_height: block.block_height,
        block_timestamp: block.block_timestamp,
        epoch_height,
        account_balance: contract.data.amount(),
        account_locked_balance: contract.data.locked(),
        storage_usage: contract.data.storage_usage(),
        attached_deposit: 0,
        prepaid_gas: max_gas_burnt,
        random_seed,
        view_config: Some(near_primitives::config::ViewConfig { max_gas_burnt }),
        output_data_receivers: vec![],
    };

    // Init runtime config for each protocol version
    let store = near_parameters::RuntimeConfigStore::free();
    let config = store
        .get_config(block.latest_protocol_version)
        .wasm_config
        .clone();
    let vm_config = near_parameters::vm::Config {
        vm_kind: config.vm_kind.replace_with_wasmtime_if_unsupported(),
        ..config
    };
    let code_hash = contract.data.code_hash();
    // Check if the contract code is already in the cache

    let key = near_vm_runner::get_contract_cache_key(code_hash, &vm_config);
    let contract_code = if compiled_contract_code_cache.has(&key).unwrap_or(false) {
        None
    } else {
        Some(match contract_code_cache.get(&code_hash).await {
            Some(code) => near_vm_runner::ContractCode::new(code, Some(code_hash)),
            None => {
                let code = db_manager
                    .get_contract_code(account_id, block.block_height)
                    .await
                    .map_err(|_| FunctionCallError::InvalidAccountId {
                        requested_account_id: account_id.clone(),
                    })?;
                contract_code_cache
                    .put(contract.data.code_hash(), code.data.clone())
                    .await;
                near_vm_runner::ContractCode::new(code.data, Some(contract.data.code_hash()))
            }
        })
    };

    // TODO: Refactor this part. It's a temporary solution to fetch state keys from DB for the poolv1.near contracts
    // https://github.com/near/read-rpc/issues/150
    let contract_state = if account_id.to_string().ends_with("poolv1.near") {
        if let Ok(result) = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            get_state_from_db(db_manager, account_id, block.block_height, &[], "query_call_function"),
        )
        .await
        {
            tracing::debug!("State keys fetched from DB");
            Some(result)
        } else {
            tracing::error!("Failed to fetch state keys from DB");
            None
        }
    } else {
        None
    };

    // Init an external scylla interface for the Runtime logic
    let code_storage = CodeStorage::init(
        db_manager.clone(),
        account_id.clone(),
        block.block_height,
        validators,
        contract_state,
        optimistic_data,
    );

    // Execute the contract in the near VM
    let result = run_code_in_vm_runner(
        code_hash,
        contract_code,
        method_name.to_string(),
        context,
        code_storage,
        vm_config,
        compiled_contract_code_cache,
    )
    .await
    .map_err(|e| FunctionCallError::InternalError {
        error_message: e.to_string(),
    })?;

    if let Some(err) = result.aborted {
        let message = format!("wasm execution failed with error: {:?}", err);
        Err(FunctionCallError::VMError {
            error_message: message,
        })
    } else {
        let logs = result.logs;
        let result = match result.return_data {
            near_vm_runner::logic::ReturnData::Value(buf) => buf,
            near_vm_runner::logic::ReturnData::ReceiptIndex(_)
            | near_vm_runner::logic::ReturnData::None => vec![],
        };
        Ok(RunContractResponse {
            result,
            logs,
            block_height: block.block_height,
            block_hash: block.block_hash,
        })
    }
}
