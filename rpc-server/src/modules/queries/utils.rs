use std::collections::HashMap;
use std::ops::Deref;

use futures::StreamExt;

use crate::config::CompiledCodeCache;
use crate::errors::FunctionCallError;
use crate::modules::blocks::FinalityBlocksInfo;
use crate::modules::queries::CodeStorage;

pub struct RunContractResponse {
    pub result: Vec<u8>,
    pub logs: Vec<String>,
    pub block_height: near_primitives::types::BlockHeight,
    pub block_hash: near_primitives::hash::CryptoHash,
}

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(db_manager))
)]
pub async fn get_state_keys_from_db(
    db_manager: &std::sync::Arc<Box<dyn database::ReaderDbManager + Sync + Send + 'static>>,
    account_id: &near_primitives::types::AccountId,
    block_height: near_primitives::types::BlockHeight,
    prefix: &[u8],
) -> HashMap<readnode_primitives::StateKey, readnode_primitives::StateValue> {
    tracing::debug!(
        "`get_state_keys_from_db` call. AccountId {}, block {}, prefix {:?}",
        account_id,
        block_height,
        prefix,
    );
    let mut data: HashMap<readnode_primitives::StateKey, readnode_primitives::StateValue> =
        HashMap::new();
    let result = {
        if !prefix.is_empty() {
            db_manager
                .get_state_keys_by_prefix(account_id, prefix)
                .await
        } else {
            db_manager.get_state_keys_all(account_id).await
        }
    };
    match result {
        Ok(state_keys) => {
            // 3 nodes * 8 cpus * 100 = 2400
            // TODO: 2400 is hardcoded value. Make it configurable.
            for state_keys_chunk in state_keys.chunks(2400) {
                let futures = state_keys_chunk.iter().map(|state_key| {
                    db_manager.get_state_key_value(account_id, block_height, state_key.clone())
                });
                let mut tasks = futures::stream::FuturesUnordered::from_iter(futures);
                while let Some((state_key, state_value)) = tasks.next().await {
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
    contract_code: near_vm_runner::ContractCode,
    method_name: &str,
    context: near_vm_runner::logic::VMContext,
    mut code_storage: CodeStorage,
    protocol_version: near_primitives::types::ProtocolVersion,
    compiled_contract_code_cache: &std::sync::Arc<CompiledCodeCache>,
) -> Result<near_vm_runner::logic::VMOutcome, near_primitives::errors::RuntimeError> {
    let contract_method_name = String::from(method_name);
    let code_cache = std::sync::Arc::clone(compiled_contract_code_cache);

    let store = near_parameters::RuntimeConfigStore::free();
    let config = store.get_config(protocol_version).wasm_config.clone();
    let mut vm_config = near_parameters::vm::Config {
        vm_kind: config.vm_kind.replace_with_wasmtime_if_unsupported(),
        ..config
    };
    vm_config.make_free();
    let results = tokio::task::spawn_blocking(move || {
        near_vm_runner::run(
            &contract_code,
            &contract_method_name,
            &mut code_storage,
            context,
            &vm_config,
            &near_parameters::RuntimeFeesConfig::free(),
            &[],
            Some(code_cache.deref()),
        )
    })
    .await;
    match results {
        Ok(result) => {
            // There are many specific errors that the runtime can encounter.
            // Some can be translated to the more general `RuntimeError`, which allows to pass
            // the error up to the caller. For all other cases, panicking here is better
            // than leaking the exact details further up.
            // Note that this does not include errors caused by user code / input, those are
            // stored in outcome.aborted.
            result.map_err(|e| match e {
                near_vm_runner::logic::errors::VMRunnerError::ExternalError(any_err) => {
                    let err = any_err
                        .downcast()
                        .expect("Downcasting AnyError should not fail");
                    near_primitives::errors::RuntimeError::ValidatorError(err)
                }
                near_vm_runner::logic::errors::VMRunnerError::InconsistentStateError(
                    err @ near_vm_runner::logic::errors::InconsistentStateError::IntegerOverflow,
                ) => {
                    near_primitives::errors::StorageError::StorageInconsistentState(err.to_string())
                        .into()
                }
                near_vm_runner::logic::errors::VMRunnerError::CacheError(err) => {
                    near_primitives::errors::StorageError::StorageInconsistentState(err.to_string())
                        .into()
                }
                near_vm_runner::logic::errors::VMRunnerError::LoadingError(msg) => {
                    panic!("Contract runtime failed to load a contract: {msg}")
                }
                near_vm_runner::logic::errors::VMRunnerError::Nondeterministic(msg) => {
                    panic!(
                        "Contract runner returned non-deterministic error '{}', aborting",
                        msg
                    )
                }
                near_vm_runner::logic::errors::VMRunnerError::WasmUnknownError {
                    debug_message,
                } => {
                    panic!("Wasmer returned unknown message: {}", debug_message)
                }
            })
        }
        Err(_) => Err(near_primitives::errors::RuntimeError::UnexpectedIntegerOverflow),
    }
}

#[allow(clippy::too_many_arguments)]
#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(db_manager, compiled_contract_code_cache, contract_code_cache))
)]
pub async fn run_contract(
    account_id: near_primitives::types::AccountId,
    method_name: &str,
    args: near_primitives::types::FunctionArgs,
    db_manager: std::sync::Arc<Box<dyn database::ReaderDbManager + Sync + Send + 'static>>,
    compiled_contract_code_cache: &std::sync::Arc<CompiledCodeCache>,
    contract_code_cache: &std::sync::Arc<
        futures_locks::RwLock<
            crate::cache::LruMemoryCache<near_primitives::hash::CryptoHash, Vec<u8>>,
        >,
    >,
    finality_blocks_info: &std::sync::Arc<futures_locks::RwLock<FinalityBlocksInfo>>,
    block: crate::modules::blocks::CacheBlock,
    max_gas_burnt: near_primitives::types::Gas,
    optimistic_data: HashMap<
        readnode_primitives::StateKey,
        Option<readnode_primitives::StateValue>,
    >,
) -> Result<RunContractResponse, FunctionCallError> {
    let contract = db_manager
        .get_account(&account_id, block.block_height)
        .await
        .map_err(|_| FunctionCallError::AccountDoesNotExist {
            requested_account_id: account_id.clone(),
        })?;

    let code: Option<Vec<u8>> = contract_code_cache
        .write()
        .await
        .get(&contract.data.code_hash())
        .cloned();

    let contract_code = match code {
        Some(code) => near_vm_runner::ContractCode::new(code, Some(contract.data.code_hash())),
        None => {
            let code = db_manager
                .get_contract_code(&account_id, block.block_height)
                .await
                .map_err(|_| FunctionCallError::InvalidAccountId {
                    requested_account_id: account_id.clone(),
                })?;
            contract_code_cache
                .write()
                .await
                .put(contract.data.code_hash(), code.data.clone());
            near_vm_runner::ContractCode::new(code.data, Some(contract.data.code_hash()))
        }
    };

    let (epoch_height, epoch_validators) = if finality_blocks_info
        .read()
        .await
        .final_block
        .block_cache
        .epoch_id
        == block.epoch_id
    {
        let validators = finality_blocks_info.read().await.current_validators.clone();
        (validators.epoch_height, validators.current_validators)
    } else {
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
        input: args.into(),
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

    let code_storage = CodeStorage::init(
        db_manager.clone(),
        account_id,
        block.block_height,
        epoch_validators
            .iter()
            .map(|validator| (validator.account_id.clone(), validator.stake))
            .collect(),
        optimistic_data,
    );

    let result = run_code_in_vm_runner(
        contract_code,
        method_name,
        context,
        code_storage,
        block.latest_protocol_version,
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
