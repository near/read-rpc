use std::collections::HashMap;

use near_vm_runner::internal::VMKindExt;
use near_vm_runner::ContractRuntimeCache;

use crate::errors::FunctionCallError;
use crate::modules::blocks::BlocksInfoByFinality;

use code_storage::CodeStorage;

mod code_storage;

pub struct RunContractResponse {
    pub result: Vec<u8>,
    pub logs: Vec<String>,
    pub block_height: near_primitives::types::BlockHeight,
    pub block_hash: near_primitives::hash::CryptoHash,
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
    let contract = db_manager
        .get_account(account_id, block.block_height, "query_call_function")
        .await
        .map_err(|_| FunctionCallError::AccountDoesNotExist {
            requested_account_id: account_id.clone(),
        })?;

    let (epoch_height, validators) =
        epoch_height_and_validators_with_balances(db_manager, blocks_info_by_finality, block)
            .await?;

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
                    .get_contract_code(account_id, block.block_height, "query_call_function")
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

    // Init an external scylla interface for the Runtime logic
    let code_storage = CodeStorage::init(
        db_manager.clone(),
        account_id.clone(),
        block.block_height,
        validators,
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

async fn epoch_height_and_validators_with_balances(
    db_manager: &std::sync::Arc<Box<dyn database::ReaderDbManager + Sync + Send + 'static>>,
    blocks_info_by_finality: &std::sync::Arc<BlocksInfoByFinality>,
    block: crate::modules::blocks::CacheBlock,
) -> Result<(u64, HashMap<near_primitives::types::AccountId, u128>), FunctionCallError> {
    let (epoch_height, epoch_validators) =
        if blocks_info_by_finality.final_cache_block().await.epoch_id == block.epoch_id {
            let validators = blocks_info_by_finality.validators().await;
            (validators.epoch_height, validators.current_validators)
        } else {
            let validators = db_manager
                .get_validators_by_epoch_id(block.epoch_id, "query_call_function")
                .await
                .map_err(|_| FunctionCallError::InternalError {
                    error_message: "Failed to get epoch info".to_string(),
                })?;
            (
                validators.epoch_height,
                validators.validators_info.current_validators,
            )
        };
    Ok((
        epoch_height,
        epoch_validators
            .iter()
            .map(|validator| (validator.account_id.clone(), validator.stake))
            .collect(),
    ))
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
