use crate::config::ServerContext;
use actix_web::web::Data;

pub mod methods;

pub(crate) async fn try_get_transaction_details_by_hash(
    data: &Data<ServerContext>,
    tx_hash: &near_indexer_primitives::CryptoHash,
    shard_id: &near_primitives::types::ShardId,
) -> anyhow::Result<readnode_primitives::TransactionDetails> {
    if let Ok(transaction_details_bytes) = &data
        .tx_details_storage
        .retrieve_tx(&tx_hash.to_string(), shard_id)
        .await
    {
        readnode_primitives::TransactionDetails::tx_deserialize(transaction_details_bytes)
    } else if let Some(tx_cache_storage) = data.tx_cache_storage.clone() {
        Ok(tx_cache_storage.get_tx_by_tx_hash(tx_hash).await?)
    } else {
        anyhow::bail!("Transaction not found")
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Default)]
pub struct FunctionCallOutcome {
    pub balance: near_primitives::types::Balance,
    pub storage_usage: near_primitives::types::StorageUsage,
    pub return_data: Option<Vec<u8>>,
    pub burnt_gas: near_primitives::types::Gas,
    pub used_gas: near_primitives::types::Gas,
    pub compute_usage: near_primitives::types::Compute,
    pub logs: Vec<String>,
    /// Data collected from making a contract call
    pub profile: ProfileData,
    pub aborted: Option<near_jsonrpc::primitives::types::query::RpcQueryError>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Default)]
pub struct ProfileData {
    /// Gas spent on sending or executing actions.
    pub actions_profile: std::collections::HashMap<String, near_primitives::types::Gas>,
    /// Non-action gas spent outside the WASM VM while executing a contract.
    pub wasm_ext_profile: std::collections::HashMap<String, near_primitives::types::Gas>,
    /// Gas spent on execution inside the WASM VM.
    pub wasm_gas: near_primitives::types::Gas,
}

impl From<near_vm_runner::ProfileDataV3> for ProfileData {
    fn from(profile: near_vm_runner::ProfileDataV3) -> Self {
        ProfileData {
            actions_profile: profile
                .actions_profile
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
            wasm_ext_profile: profile
                .wasm_ext_profile
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
            wasm_gas: profile.wasm_gas,
        }
    }
}

impl From<crate::modules::queries::contract_runner::RunContractResponse> for FunctionCallOutcome {
    fn from(
        contract_response: crate::modules::queries::contract_runner::RunContractResponse,
    ) -> Self {
        FunctionCallOutcome {
            balance: contract_response.result.balance,
            storage_usage: contract_response.result.storage_usage,
            return_data: contract_response.result.return_data.as_value(),
            burnt_gas: contract_response.result.burnt_gas,
            used_gas: contract_response.result.used_gas,
            compute_usage: contract_response.result.compute_usage,
            logs: contract_response.result.logs,
            profile: contract_response.result.profile.into(),
            aborted: contract_response.result.aborted.map(|err| {
                let message = format!("wasm execution failed with error: {:?}", err);
                near_jsonrpc::primitives::types::query::RpcQueryError::ContractExecutionError {
                    vm_error: message,
                    block_height: contract_response.block_height,
                    block_hash: contract_response.block_hash,
                }
            }),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub enum EmulateTransactionActionResult {
    CreateAccount {
        fee: near_primitives::types::Gas,
    },
    DeployContract {
        fee: near_primitives::types::Gas,
    },
    FunctionCall {
        outcome: Box<FunctionCallOutcome>,
        fee: near_primitives::types::Gas,
    },
    Transfer {
        fee: near_primitives::types::Gas,
    },
    Stake {
        fee: near_primitives::types::Gas,
    },
    AddKey {
        fee: near_primitives::types::Gas,
    },
    DeleteKey {
        fee: near_primitives::types::Gas,
    },
    DeleteAccount {
        fee: near_primitives::types::Gas,
    },
    Delegate {
        fee: near_primitives::types::Gas,
    },
    DeployGlobalContract {
        fee: near_primitives::types::Gas,
    },
    UseGlobalContract {
        fee: near_primitives::types::Gas,
    },
}

impl EmulateTransactionActionResult {
    pub fn from_tx_action(
        tx_action: &near_primitives::transaction::Action,
        fee: near_primitives::types::Gas,
    ) -> Self {
        match tx_action {
            near_primitives::transaction::Action::CreateAccount(_) => Self::CreateAccount { fee },
            near_primitives::transaction::Action::DeployContract(_) => Self::DeployContract { fee },
            near_primitives::transaction::Action::FunctionCall(_) => Self::FunctionCall {
                outcome: Box::new(FunctionCallOutcome::default()),
                fee,
            },
            near_primitives::transaction::Action::Transfer(_) => Self::Transfer { fee },
            near_primitives::transaction::Action::Stake(_) => Self::Stake { fee },
            near_primitives::transaction::Action::AddKey(_) => Self::AddKey { fee },
            near_primitives::transaction::Action::DeleteKey(_) => Self::DeleteKey { fee },
            near_primitives::transaction::Action::DeleteAccount(_) => Self::DeleteAccount { fee },
            near_primitives::transaction::Action::Delegate(_) => Self::Delegate { fee },
            near_primitives::transaction::Action::DeployGlobalContract(_) => {
                Self::DeployGlobalContract { fee }
            }
            near_primitives::transaction::Action::UseGlobalContract(_) => {
                Self::UseGlobalContract { fee }
            }
        }
    }
}
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct EmulateTransactionResponse {
    pub results: Vec<EmulateTransactionActionResult>,
    pub block_height: near_primitives::types::BlockHeight,
    pub gas_price: near_primitives::types::Balance,
}

impl near_jsonrpc_client::methods::RpcHandlerResponse for EmulateTransactionResponse {}

/// Collector for transaction actions, providing thread-safe storage and retrieval.
///
/// Stores a vector of `Action` instances using an async RwLock.
#[derive(Default)]
pub struct TxActionsCollector {
    inner: futures_locks::RwLock<Vec<near_primitives::transaction::Action>>,
}

impl TxActionsCollector {
    /// Creates a new, empty `TxActionsCollector`.
    pub fn new() -> Self {
        Self {
            inner: futures_locks::RwLock::new(Vec::new()),
        }
    }

    /// Pushes a new `Action` into the collector.
    ///
    /// This method blocks on acquiring a write lock.
    pub fn push_mock_action(
        &self,
        mock_action: near_vm_runner::logic::mocks::mock_external::MockAction,
    ) {
        let tx_action = match mock_action {
            near_vm_runner::logic::mocks::mock_external::MockAction::CreateAccount { .. } => {
                Some(near_primitives::transaction::Action::CreateAccount(
                    near_primitives::transaction::CreateAccountAction {},
                ))
            }
            near_vm_runner::logic::mocks::mock_external::MockAction::DeployContract {
                code,
                ..
            } => Some(near_primitives::transaction::Action::DeployContract(
                near_primitives::transaction::DeployContractAction { code },
            )),
            near_vm_runner::logic::mocks::mock_external::MockAction::FunctionCallWeight {
                method_name,
                args,
                gas_weight,
                ..
            } => Some(near_primitives::transaction::Action::FunctionCall(
                Box::new(near_primitives::transaction::FunctionCallAction {
                    method_name: String::from_utf8(method_name)
                        .expect("Method name should be valid UTF-8"),
                    args,
                    gas: gas_weight.0,
                    deposit: 0,
                }),
            )),
            near_vm_runner::logic::mocks::mock_external::MockAction::Transfer {
                deposit, ..
            } => Some(near_primitives::transaction::Action::Transfer(
                near_primitives::transaction::TransferAction { deposit },
            )),
            near_vm_runner::logic::mocks::mock_external::MockAction::Stake {
                stake,
                public_key,
                ..
            } => Some(near_primitives::transaction::Action::Stake(Box::new(
                near_primitives::transaction::StakeAction { stake, public_key },
            ))),
            near_vm_runner::logic::mocks::mock_external::MockAction::AddKeyWithFullAccess {
                public_key,
                nonce,
                ..
            } => Some(near_primitives::transaction::Action::AddKey(Box::new(
                near_primitives::transaction::AddKeyAction {
                    public_key,
                    access_key: near_primitives::account::AccessKey {
                        nonce,
                        permission: near_primitives::account::AccessKeyPermission::FullAccess,
                    },
                },
            ))),
            near_vm_runner::logic::mocks::mock_external::MockAction::AddKeyWithFunctionCall {
                public_key,
                nonce,
                allowance,
                receiver_id,
                method_names,
                ..
            } => Some(near_primitives::transaction::Action::AddKey(Box::new(
                near_primitives::transaction::AddKeyAction {
                    public_key,
                    access_key: near_primitives::account::AccessKey {
                        nonce,
                        permission: near_primitives::account::AccessKeyPermission::FunctionCall(
                            near_primitives::account::FunctionCallPermission {
                                allowance,
                                receiver_id: receiver_id.into(),
                                method_names: method_names
                                    .into_iter()
                                    .map(|method_name| {
                                        String::from_utf8(method_name)
                                            .expect("Method name should be valid UTF-8")
                                    })
                                    .collect(),
                            },
                        ),
                    },
                },
            ))),
            near_vm_runner::logic::mocks::mock_external::MockAction::DeleteKey {
                public_key,
                ..
            } => Some(near_primitives::transaction::Action::DeleteKey(Box::new(
                near_primitives::transaction::DeleteKeyAction { public_key },
            ))),
            near_vm_runner::logic::mocks::mock_external::MockAction::DeleteAccount {
                beneficiary_id,
                ..
            } => Some(near_primitives::transaction::Action::DeleteAccount(
                near_primitives::transaction::DeleteAccountAction { beneficiary_id },
            )),
            _ => {
                // For unsupported actions, we can log or handle them as needed.
                None
            }
        };
        if let Some(action) = tx_action {
            futures::executor::block_on(async { self.inner.write().await.push(action) });
        }
    }

    /// Asynchronously retrieves a clone of all collected actions.
    pub async fn get_actions(&self) -> Vec<near_primitives::transaction::Action> {
        self.inner.read().await.clone()
    }
}
