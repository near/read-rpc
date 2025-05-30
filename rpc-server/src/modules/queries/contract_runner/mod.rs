use std::collections::HashMap;

use crate::modules::blocks::BlocksInfoByFinality;
use code_storage::CodeStorage;
use near_vm_runner::ContractRuntimeCache;

mod code_storage;

pub struct Contract {
    pub contract_code: Option<std::sync::Arc<near_vm_runner::ContractCode>>,
    pub hash: near_primitives::hash::CryptoHash,
}

impl Contract {
    pub fn new(code: Option<Vec<u8>>, hash: near_primitives::hash::CryptoHash) -> Self {
        code.map(|code| Self {
            contract_code: Some(std::sync::Arc::new(near_vm_runner::ContractCode::new(
                code,
                Some(hash),
            ))),
            hash,
        })
        .unwrap_or_else(|| Self {
            contract_code: None,
            hash,
        })
    }
}
impl near_vm_runner::Contract for Contract {
    fn hash(&self) -> near_primitives::hash::CryptoHash {
        self.hash
    }

    fn get_code(&self) -> Option<std::sync::Arc<near_vm_runner::ContractCode>> {
        self.contract_code.clone()
    }
}

pub struct RunContractResponse {
    pub result: Vec<u8>,
    pub logs: Vec<String>,
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
    block: &near_primitives::views::BlockView,
    max_gas_burnt: near_primitives::types::Gas,
    optimistic_data: HashMap<
        readnode_primitives::StateKey,
        Option<readnode_primitives::StateValue>,
    >,
    prefetch_state_size_limit: u64,
) -> Result<RunContractResponse, near_jsonrpc::primitives::types::query::RpcQueryError> {
    let contract = db_manager
        .get_account(account_id, block.header.height, "query_call_function")
        .await
        .map_err(
            |_| near_jsonrpc::primitives::types::query::RpcQueryError::UnknownAccount {
                requested_account_id: account_id.clone(),
                block_height: block.header.height,
                block_hash: block.header.hash,
            },
        )?;

    let (epoch_height, validators) =
        epoch_height_and_validators_with_balances(db_manager, blocks_info_by_finality, block)
            .await?;

    // Prepare context for the VM run contract
    let public_key = near_crypto::PublicKey::empty(near_crypto::KeyType::ED25519);
    let random_seed = near_primitives::utils::create_random_seed(
        block.header.latest_protocol_version,
        near_primitives::hash::CryptoHash::default(),
        block.header.prev_state_root,
    );
    let context = near_vm_runner::logic::VMContext {
        current_account_id: account_id.clone(),
        signer_account_id: account_id.clone(),
        signer_account_pk: borsh::to_vec(&public_key).expect("Failed to serialize"),
        predecessor_account_id: account_id.clone(),
        input: args.to_vec(),
        promise_results: Vec::new().into(),
        block_height: block.header.height,
        block_timestamp: block.header.timestamp,
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
        .get_config(block.header.latest_protocol_version)
        .wasm_config
        .clone();
    let vm_config = near_parameters::vm::Config {
        vm_kind: config.vm_kind.replace_with_wasmtime_if_unsupported(),
        ..near_parameters::vm::Config::clone(&config)
    };
    let code_hash = contract
        .data
        .local_contract_hash()
        .unwrap_or(contract.data.global_contract_hash().unwrap_or_default());

    // Check if the contract code is already in the cache
    let key = near_vm_runner::get_contract_cache_key(code_hash, &vm_config);
    let contract_code = if compiled_contract_code_cache.has(&key).unwrap_or(false) {
        Contract::new(None, code_hash)
    } else {
        match contract_code_cache.get(&code_hash).await {
            Some(code) => Contract::new(Some(code), code_hash),
            None => {
                let code = db_manager
                    .get_contract_code(account_id, block.header.height, "query_call_function")
                    .await
                    .map_err(|_| {
                        near_jsonrpc::primitives::types::query::RpcQueryError::InvalidAccount {
                            requested_account_id: account_id.clone(),
                            block_height: block.header.height,
                            block_hash: block.header.hash,
                        }
                    })?;
                contract_code_cache.put(code_hash, code.data.clone()).await;
                Contract::new(Some(code.data), code_hash)
            }
        }
    };

    // We need to calculate the state size of the contract to determine if we should prefetch the state or not.
    // The state size is the storage usage minus the code size.
    // If the state size is less than the prefetch_state_size_limit, we prefetch the state.
    let code_len = if let Some(contract_code) = &contract_code.contract_code {
        contract_code.code().len()
    } else if let Some(code) = contract_code_cache.get(&code_hash).await {
        code.len()
    } else {
        db_manager
            .get_contract_code(account_id, block.header.height, "query_call_function")
            .await
            .map(|code| code.data.len())
            .unwrap_or_default()
    };
    let state_size = contract
        .data
        .storage_usage()
        .saturating_sub(code_len as u64);
    // Init an external database interface for the Runtime logic
    let code_storage = CodeStorage::init(
        db_manager.clone(),
        account_id.clone(),
        block.header.height,
        validators,
        optimistic_data,
        state_size <= prefetch_state_size_limit,
    )
    .await;

    // Execute the contract in the near VM
    let result = run_code_in_vm_runner(
        contract_code,
        method_name.to_string(),
        context,
        code_storage,
        vm_config,
        compiled_contract_code_cache,
    )
    .await
    .map_err(
        |e| near_jsonrpc::primitives::types::query::RpcQueryError::InternalError {
            error_message: e.to_string(),
        },
    )?;

    if let Some(err) = result.aborted {
        let message = format!("wasm execution failed with error: {:?}", err);
        Err(
            near_jsonrpc::primitives::types::query::RpcQueryError::ContractExecutionError {
                vm_error: message,
                block_height: block.header.height,
                block_hash: block.header.hash,
            },
        )
    } else {
        let logs = result.logs;
        let result = match result.return_data {
            near_vm_runner::logic::ReturnData::Value(buf) => buf,
            near_vm_runner::logic::ReturnData::ReceiptIndex(_)
            | near_vm_runner::logic::ReturnData::None => vec![],
        };
        Ok(RunContractResponse { result, logs })
    }
}

async fn epoch_height_and_validators_with_balances(
    db_manager: &std::sync::Arc<Box<dyn database::ReaderDbManager + Sync + Send + 'static>>,
    blocks_info_by_finality: &std::sync::Arc<BlocksInfoByFinality>,
    block: &near_primitives::views::BlockView,
) -> Result<
    (u64, HashMap<near_primitives::types::AccountId, u128>),
    near_jsonrpc::primitives::types::query::RpcQueryError,
> {
    let (epoch_height, epoch_validators) = if blocks_info_by_finality
        .final_block_view()
        .await
        .header
        .epoch_id
        == block.header.epoch_id
    {
        let validators = blocks_info_by_finality.validators().await;
        (validators.epoch_height, validators.current_validators)
    } else {
        let validators = db_manager
            .get_validators_by_epoch_id(block.header.epoch_id, "query_call_function")
            .await
            .map_err(
                |_| near_jsonrpc::primitives::types::query::RpcQueryError::InternalError {
                    error_message: "Failed to get epoch info".to_string(),
                },
            )?;
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
    tracing::instrument(skip(context, code_storage, contract, compiled_contract_code_cache))
)]
async fn run_code_in_vm_runner(
    contract: Contract,
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

        let prepared_contract = near_vm_runner::prepare(
            &contract,
            std::sync::Arc::from(vm_config.clone()),
            Some(&compiled_contract_code_cache_handle),
            context.make_gas_counter(&vm_config),
            &method_name,
        );
        near_vm_runner::run(
            prepared_contract,
            &mut code_storage,
            &context,
            std::sync::Arc::from(near_parameters::RuntimeFeesConfig::free()),
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
