use std::collections::HashMap;
use std::ops::Deref;

#[cfg(feature = "account_access_keys")]
use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use near_crypto::{KeyType, PublicKey};
use near_primitives::utils::create_random_seed;
use tokio::task;

use crate::config::CompiledCodeCache;
use crate::errors::FunctionCallError;
use crate::modules::queries::{CodeStorage, MAX_LIMIT};
use crate::storage::ScyllaDBManager;

pub struct RunContractResponse {
    pub result: Vec<u8>,
    pub logs: Vec<String>,
    pub block_height: near_primitives::types::BlockHeight,
    pub block_hash: near_primitives::hash::CryptoHash,
}

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(scylla_db_manager))
)]
pub async fn get_state_keys_from_scylla(
    scylla_db_manager: &std::sync::Arc<ScyllaDBManager>,
    account_id: &near_primitives::types::AccountId,
    block_height: near_primitives::types::BlockHeight,
    prefix: &[u8],
) -> HashMap<Vec<u8>, Vec<u8>> {
    tracing::debug!(
        "`get_state_keys_from_scylla` call. AccountId {}, block {}, prefix {:?}",
        account_id,
        block_height,
        prefix,
    );
    let mut data: HashMap<crate::storage::StateKey, crate::storage::StateValue> = HashMap::new();
    let result = {
        if !prefix.is_empty() {
            scylla_db_manager
                .get_state_keys_by_prefix(account_id, prefix)
                .await
        } else {
            scylla_db_manager.get_all_state_keys(account_id).await
        }
    };
    match result {
        Ok(state_keys) => {
            for state_key in state_keys {
                let state_value_result = scylla_db_manager
                    .get_state_key_value(account_id, block_height, state_key.clone())
                    .await;
                if let Ok(state_value) = state_value_result {
                    if !state_value.is_empty() {
                        data.insert(state_key, state_value);
                    }
                };
                let keys_count = data.keys().len() as u8;
                if keys_count > MAX_LIMIT {
                    return data;
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
    tracing::instrument(skip(scylla_db_manager))
)]
pub async fn fetch_list_access_keys_from_scylla_db(
    scylla_db_manager: &std::sync::Arc<ScyllaDBManager>,
    account_id: &near_primitives::types::AccountId,
    block_height: near_primitives::types::BlockHeight,
) -> anyhow::Result<Vec<near_primitives::views::AccessKeyInfoView>> {
    tracing::debug!(
        "`fetch_list_access_keys_from_scylla_db` call. AccountID {}, block {}",
        account_id,
        block_height,
    );
    let row = scylla_db_manager
        .get_account_access_keys(account_id, block_height)
        .await?;
    let (account_keys,): (HashMap<String, Vec<u8>>,) =
        row.into_typed::<(HashMap<String, Vec<u8>>,)>()?;
    let account_keys_view = account_keys
        .into_iter()
        .map(
            |(public_key_hex, access_key)| near_primitives::views::AccessKeyInfoView {
                public_key: near_crypto::PublicKey::try_from_slice(
                    &hex::decode(public_key_hex).unwrap(),
                )
                .unwrap(),
                access_key: near_primitives::views::AccessKeyView::from(
                    near_primitives::account::AccessKey::try_from_slice(&access_key).unwrap(),
                ),
            },
        )
        .collect();
    Ok(account_keys_view)
}

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(scylla_db_manager))
)]
pub async fn fetch_state_from_scylla_db(
    scylla_db_manager: &std::sync::Arc<ScyllaDBManager>,
    account_id: &near_primitives::types::AccountId,
    block_height: near_primitives::types::BlockHeight,
    prefix: &[u8],
) -> anyhow::Result<near_primitives::views::ViewStateResult> {
    tracing::debug!(
        "`fetch_state_from_scylla_db` call. AccountID {}, block {}, prefix {:?}",
        account_id,
        block_height,
        prefix,
    );
    let state_from_db =
        get_state_keys_from_scylla(scylla_db_manager, account_id, block_height, prefix).await;
    if state_from_db.is_empty() {
        anyhow::bail!("Data not found in db")
    } else {
        let mut values = Vec::new();
        for (key, value) in state_from_db.iter() {
            let state_item = near_primitives::views::StateItem {
                key: key.to_vec().into(),
                value: value.to_vec().into(),
            };
            values.push(state_item)
        }
        Ok(near_primitives::views::ViewStateResult {
            values,
            proof: vec![], // TODO: this is hardcoded empty value since we don't support proofs yet
        })
    }
}

#[allow(clippy::too_many_arguments)]
#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(
        scylla_db_manager,
        context,
        contract_code,
        compiled_contract_code_cache
    ))
)]
async fn run_code_in_vm_runner(
    contract_code: near_primitives::contract::ContractCode,
    method_name: &str,
    context: near_vm_logic::VMContext,
    account_id: near_primitives::types::AccountId,
    block_height: near_primitives::types::BlockHeight,
    scylla_db_manager: std::sync::Arc<ScyllaDBManager>,
    latest_protocol_version: near_primitives::types::ProtocolVersion,
    compiled_contract_code_cache: &std::sync::Arc<CompiledCodeCache>,
) -> Result<near_vm_logic::VMOutcome, near_primitives::errors::RuntimeError> {
    let contract_method_name = String::from(method_name);
    let mut external = CodeStorage::init(scylla_db_manager.clone(), account_id, block_height);
    let code_cache = std::sync::Arc::clone(compiled_contract_code_cache);

    let results = task::spawn_blocking(move || {
        near_vm_runner::run(
            &contract_code,
            &contract_method_name,
            &mut external,
            context,
            &near_vm_logic::VMConfig::free(),
            &near_primitives_core::runtime::fees::RuntimeFeesConfig::free(),
            &[],
            latest_protocol_version,
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
                near_vm_errors::VMRunnerError::ExternalError(any_err) => {
                    let err = any_err
                        .downcast()
                        .expect("Downcasting AnyError should not fail");
                    near_primitives::errors::RuntimeError::ValidatorError(err)
                }
                near_vm_errors::VMRunnerError::InconsistentStateError(
                    err @ near_vm_errors::InconsistentStateError::IntegerOverflow,
                ) => {
                    near_primitives::errors::StorageError::StorageInconsistentState(err.to_string())
                        .into()
                }
                near_vm_errors::VMRunnerError::CacheError(err) => {
                    near_primitives::errors::StorageError::StorageInconsistentState(err.to_string())
                        .into()
                }
                near_vm_errors::VMRunnerError::LoadingError(msg) => {
                    panic!("Contract runtime failed to load a contract: {msg}")
                }
                near_vm_errors::VMRunnerError::Nondeterministic(msg) => {
                    panic!(
                        "Contract runner returned non-deterministic error '{}', aborting",
                        msg
                    )
                }
                near_vm_errors::VMRunnerError::WasmUnknownError { debug_message } => {
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
    tracing::instrument(skip(
        scylla_db_manager,
        compiled_contract_code_cache,
        contract_code_cache
    ))
)]
pub async fn run_contract(
    account_id: near_primitives::types::AccountId,
    method_name: &str,
    args: near_primitives::types::FunctionArgs,
    scylla_db_manager: std::sync::Arc<ScyllaDBManager>,
    compiled_contract_code_cache: &std::sync::Arc<CompiledCodeCache>,
    contract_code_cache: &std::sync::Arc<
        std::sync::RwLock<crate::cache::LruMemoryCache<near_primitives::hash::CryptoHash, Vec<u8>>>,
    >,
    block: crate::modules::blocks::CacheBlock,
    max_gas_burnt: near_primitives_core::types::Gas,
) -> Result<RunContractResponse, FunctionCallError> {
    let contract = scylla_db_manager
        .get_account(&account_id, block.block_height)
        .await
        .map_err(|_| FunctionCallError::AccountDoesNotExist {
            requested_account_id: account_id.clone(),
        })?;

    let code: Option<Vec<u8>> = contract_code_cache
        .write()
        .unwrap()
        .get(&contract.data.code_hash())
        .cloned();

    let contract_code = match code {
        Some(code) => {
            near_primitives::contract::ContractCode::new(code, Some(contract.data.code_hash()))
        }
        None => {
            let code = scylla_db_manager
                .get_contract_code(&account_id, block.block_height)
                .await
                .map_err(|_| FunctionCallError::InvalidAccountId {
                    requested_account_id: account_id.clone(),
                })?;
            contract_code_cache
                .write()
                .unwrap()
                .put(contract.data.code_hash(), code.data.clone());
            near_primitives::contract::ContractCode::new(code.data, Some(contract.data.code_hash()))
        }
    };
    let public_key = PublicKey::empty(KeyType::ED25519);
    let random_seed = create_random_seed(
        block.latest_protocol_version,
        near_primitives_core::hash::CryptoHash::default(),
        block.state_root,
    );
    let context = near_vm_logic::VMContext {
        current_account_id: account_id.parse().unwrap(),
        signer_account_id: account_id.parse().unwrap(),
        signer_account_pk: public_key.try_to_vec().expect("Failed to serialize"),
        predecessor_account_id: account_id.parse().unwrap(),
        input: args.into(),
        block_height: block.block_height,
        block_timestamp: block.block_timestamp,
        epoch_height: 0, // TODO: implement indexing of epoch_height and pass it here
        account_balance: contract.data.amount(),
        account_locked_balance: contract.data.locked(),
        storage_usage: contract.data.storage_usage(),
        attached_deposit: 0,
        prepaid_gas: max_gas_burnt,
        random_seed,
        view_config: Some(near_primitives::config::ViewConfig { max_gas_burnt }),
        output_data_receivers: vec![],
    };

    let result = run_code_in_vm_runner(
        contract_code,
        method_name,
        context,
        account_id,
        block.block_height,
        scylla_db_manager.clone(),
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
            near_vm_logic::ReturnData::Value(buf) => buf,
            near_vm_logic::ReturnData::ReceiptIndex(_) | near_vm_logic::ReturnData::None => vec![],
        };
        Ok(RunContractResponse {
            result,
            logs,
            block_height: block.block_height,
            block_hash: block.block_hash,
        })
    }
}
