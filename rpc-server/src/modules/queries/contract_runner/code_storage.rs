use std::collections::HashMap;

use crate::modules::queries::utils;
use crate::modules::queries::utils::get_state_key_value_from_db;
use database::ReaderDbManager;
use futures::executor::block_on;
use near_vm_runner::logic::StorageAccessTracker;

pub type Result<T> = ::std::result::Result<T, near_vm_runner::logic::VMLogicError>;

pub struct CodeStorage {
    db_manager: std::sync::Arc<Box<dyn ReaderDbManager + Sync + Send + 'static>>,
    account_id: near_primitives::types::AccountId,
    block_height: near_primitives::types::BlockHeight,
    validators: HashMap<near_primitives::types::AccountId, near_primitives::types::Balance>,
    data_count: u64,

    is_optimistic: bool,
    optimistic_data:
        HashMap<readnode_primitives::StateKey, Option<readnode_primitives::StateValue>>,

    is_prefetch_state: bool,
    prefetch_state_data: HashMap<readnode_primitives::StateKey, readnode_primitives::StateValue>,

    tx_actions_collector: Option<std::sync::Arc<crate::modules::transactions::TxActionsCollector>>,
    tx_actions: Vec<near_vm_runner::logic::mocks::mock_external::MockAction>,
    tx_storage: HashMap<readnode_primitives::StateKey, Option<readnode_primitives::StateValue>>,
    is_tx_emulator: bool,
}

pub struct StorageValuePtr {
    value: Vec<u8>,
}

impl near_vm_runner::logic::ValuePtr for StorageValuePtr {
    fn len(&self) -> u32 {
        self.value.len() as u32
    }

    fn deref(&self, _storage_tracker: &mut dyn StorageAccessTracker) -> Result<Vec<u8>> {
        Ok(self.value.clone())
    }
}

impl CodeStorage {
    #[allow(clippy::too_many_arguments)]
    pub async fn init(
        db_manager: std::sync::Arc<Box<dyn ReaderDbManager + Sync + Send + 'static>>,
        account_id: near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        validators: HashMap<near_primitives::types::AccountId, near_primitives::types::Balance>,
        optimistic_data: HashMap<
            readnode_primitives::StateKey,
            Option<readnode_primitives::StateValue>,
        >,
        prefetch_state: bool,
        tx_actions_collector: Option<
            std::sync::Arc<crate::modules::transactions::TxActionsCollector>,
        >,
        is_tx_emulator: bool,
    ) -> Self {
        let prefetch_state_data = if prefetch_state {
            utils::get_state_from_db(
                &db_manager,
                &account_id,
                block_height,
                &[],
                "query_call_function",
            )
            .await
        } else {
            HashMap::new()
        };

        Self {
            db_manager,
            account_id,
            block_height,
            validators,
            data_count: Default::default(), // TODO: Using for generate_data_id
            is_optimistic: !optimistic_data.is_empty(),
            optimistic_data,
            is_prefetch_state: !prefetch_state_data.is_empty(),
            prefetch_state_data,
            tx_actions_collector,
            tx_actions: vec![],
            tx_storage: Default::default(),
            is_tx_emulator,
        }
    }

    fn push_action(&mut self, action: near_vm_runner::logic::mocks::mock_external::MockAction) {
        if let Some(collector) = &self.tx_actions_collector {
            collector.push_mock_action(action.clone());
        }
        self.tx_actions.push(action);
    }

    fn get_state_key_data(&self, key: &[u8]) -> readnode_primitives::StateValue {
        if self.is_prefetch_state {
            self.prefetch_state_data
                .get(key)
                .cloned()
                .unwrap_or_default()
        } else {
            let get_db_data = get_state_key_value_from_db(
                &self.db_manager,
                &self.account_id,
                self.block_height,
                key.to_vec(),
                "query_call_function",
            );
            let (_, data) = block_on(get_db_data);
            data
        }
    }

    fn optimistic_storage_get(
        &self,
        key: &[u8],
    ) -> Result<Option<Box<dyn near_vm_runner::logic::ValuePtr>>> {
        if let Some(value) = self.optimistic_data.get(key) {
            Ok(value.as_ref().map(|data| {
                Box::new(StorageValuePtr {
                    value: data.clone(),
                }) as Box<_>
            }))
        } else {
            self.database_storage_get(key)
        }
    }

    fn database_storage_get(
        &self,
        key: &[u8],
    ) -> Result<Option<Box<dyn near_vm_runner::logic::ValuePtr>>> {
        let data = self.get_state_key_data(key);
        Ok(if !data.is_empty() {
            Some(Box::new(StorageValuePtr { value: data }) as Box<_>)
        } else {
            None
        })
    }

    fn optimistic_storage_has_key(&mut self, key: &[u8]) -> Result<bool> {
        if let Some(value) = self.optimistic_data.get(key) {
            Ok(value.is_some())
        } else {
            self.database_storage_has_key(key)
        }
    }

    fn database_storage_has_key(&mut self, key: &[u8]) -> Result<bool> {
        Ok(!self.get_state_key_data(key).is_empty())
    }
}

impl near_vm_runner::logic::External for CodeStorage {
    #[cfg_attr(
        feature = "tracing-instrumentation",
        tracing::instrument(skip(self, _access_tracker))
    )]
    fn storage_set(
        &mut self,
        _access_tracker: &mut dyn StorageAccessTracker,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        if !self.is_tx_emulator {
            return Err(near_vm_runner::logic::VMLogicError::HostError(
                near_vm_runner::logic::HostError::ProhibitedInView {
                    method_name: String::from("storage_set"),
                },
            ));
        };
        let tx_value = self.tx_storage.insert(key.to_vec(), Some(value.to_vec()));
        // Returns the previous value for the given key if it exists in the transaction storage,
        // otherwise fetches the value from the database. If the database value is not empty,
        // returns it as Some; otherwise, returns None.
        let result = if let Some(tx_value) = tx_value {
            tx_value
        } else {
            let db_value = self.get_state_key_data(key);
            if !db_value.is_empty() {
                Some(db_value)
            } else {
                None
            }
        };
        Ok(result)
    }

    #[cfg_attr(
        feature = "tracing-instrumentation",
        tracing::instrument(skip(self, _access_tracker))
    )]
    fn storage_get(
        &self,
        _access_tracker: &mut dyn StorageAccessTracker,
        key: &[u8],
    ) -> Result<Option<Box<dyn near_vm_runner::logic::ValuePtr>>> {
        // If we are in transaction emulator mode, we should return the value from the transaction storage
        if self.is_tx_emulator {
            let val = self.tx_storage.get(key);
            if let Some(Some(value)) = val {
                return Ok(Some(Box::new(StorageValuePtr {
                    value: value.clone(),
                }) as Box<_>));
            } else if let Some(None) = val {
                return Ok(None);
            }
        }
        // If the key is not in the transaction storage, we should return the value from the database
        if self.is_optimistic {
            self.optimistic_storage_get(key)
        } else {
            self.database_storage_get(key)
        }
    }

    #[cfg_attr(
        feature = "tracing-instrumentation",
        tracing::instrument(skip(self, _access_tracker))
    )]
    fn storage_remove(
        &mut self,
        _access_tracker: &mut dyn StorageAccessTracker,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        if !self.is_tx_emulator {
            return Err(near_vm_runner::logic::VMLogicError::HostError(
                near_vm_runner::logic::HostError::ProhibitedInView {
                    method_name: String::from("storage_remove"),
                },
            ));
        }
        // We set the value to None to emulate the removal of the key
        let value = self.tx_storage.insert(key.to_vec(), None);
        // Returns the previous value for the given key, if it exists.
        // If the key was already present in the transaction storage, return its value (which may be Some or None).
        // Otherwise, fetch the value from the database. If the database value is not empty, return it as Some; otherwise, return None.
        let result = if let Some(value) = value {
            value
        } else {
            let db_val = self.get_state_key_data(key);
            if !db_val.is_empty() {
                Some(db_val)
            } else {
                None
            }
        };
        Ok(result)
    }

    #[cfg_attr(
        feature = "tracing-instrumentation",
        tracing::instrument(skip(self, _access_tracker))
    )]
    fn storage_has_key(
        &mut self,
        _access_tracker: &mut dyn StorageAccessTracker,
        key: &[u8],
    ) -> Result<bool> {
        // If we are in transaction emulator mode, we should check the transaction storage
        if self.is_tx_emulator {
            let val = self.tx_storage.get(key);
            if let Some(Some(_)) = val {
                return Ok(true);
            } else if let Some(None) = val {
                return Ok(false);
            }
        }
        if self.is_optimistic {
            self.optimistic_storage_has_key(key)
        } else {
            self.database_storage_has_key(key)
        }
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(self)))]
    fn generate_data_id(&mut self) -> near_primitives::hash::CryptoHash {
        // TODO: Should be improvement in future
        // Generates some hash for the data ID to receive data.
        // This hash should not be functionality
        let data_id = near_primitives::hash::hash(&self.data_count.to_le_bytes());
        self.data_count += 1;
        data_id
    }

    fn get_recorded_storage_size(&self) -> usize {
        0
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(self)))]
    fn validator_stake(
        &self,
        account_id: &near_primitives::types::AccountId,
    ) -> Result<Option<near_primitives::types::Balance>> {
        Ok(self.validators.get(account_id).cloned())
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(self)))]
    fn validator_total_stake(&self) -> Result<near_primitives::types::Balance> {
        Ok(self.validators.values().sum())
    }

    fn create_action_receipt(
        &mut self,
        receipt_indices: Vec<near_vm_runner::logic::types::ReceiptIndex>,
        receiver_id: near_primitives::types::AccountId,
    ) -> Result<near_vm_runner::logic::types::ReceiptIndex> {
        if !self.is_tx_emulator {
            return Err(near_vm_runner::logic::VMLogicError::HostError(
                near_vm_runner::logic::HostError::ProhibitedInView {
                    method_name: String::from("create_action_receipt"),
                },
            ));
        }
        let index = self.tx_actions.len();
        self.push_action(
            near_vm_runner::logic::mocks::mock_external::MockAction::CreateReceipt {
                receipt_indices,
                receiver_id,
            },
        );
        Ok(index as u64)
    }

    fn create_promise_yield_receipt(
        &mut self,
        receiver_id: near_primitives::types::AccountId,
    ) -> Result<(
        near_vm_runner::logic::types::ReceiptIndex,
        near_indexer_primitives::CryptoHash,
    )> {
        if !self.is_tx_emulator {
            return Err(near_vm_runner::logic::VMLogicError::HostError(
                near_vm_runner::logic::HostError::ProhibitedInView {
                    method_name: String::from("create_promise_yield_receipt"),
                },
            ));
        }
        let index = self.tx_actions.len();
        let data_id = self.generate_data_id();
        self.push_action(
            near_vm_runner::logic::mocks::mock_external::MockAction::YieldCreate {
                data_id,
                receiver_id,
            },
        );
        Ok((index as u64, data_id))
    }

    fn submit_promise_resume_data(
        &mut self,
        data_id: near_indexer_primitives::CryptoHash,
        data: Vec<u8>,
    ) -> Result<bool> {
        if !self.is_tx_emulator {
            return Err(near_vm_runner::logic::VMLogicError::HostError(
                near_vm_runner::logic::HostError::ProhibitedInView {
                    method_name: String::from("submit_promise_resume_data"),
                },
            ));
        }
        self.push_action(
            near_vm_runner::logic::mocks::mock_external::MockAction::YieldResume { data_id, data },
        );
        for action in &self.tx_actions {
            let near_vm_runner::logic::mocks::mock_external::MockAction::YieldCreate {
                data_id: done,
                receiver_id,
            } = action
            else {
                continue;
            };
            if data_id == *done && self.account_id == *receiver_id {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn append_action_create_account(
        &mut self,
        receipt_index: near_vm_runner::logic::types::ReceiptIndex,
    ) -> Result<()> {
        self.push_action(
            near_vm_runner::logic::mocks::mock_external::MockAction::CreateAccount {
                receipt_index,
            },
        );
        Ok(())
    }

    fn append_action_deploy_contract(
        &mut self,
        receipt_index: near_vm_runner::logic::types::ReceiptIndex,
        code: Vec<u8>,
    ) -> Result<()> {
        self.push_action(
            near_vm_runner::logic::mocks::mock_external::MockAction::DeployContract {
                receipt_index,
                code,
            },
        );
        Ok(())
    }

    fn append_action_function_call_weight(
        &mut self,
        receipt_index: near_vm_runner::logic::types::ReceiptIndex,
        method_name: Vec<u8>,
        args: Vec<u8>,
        attached_deposit: near_primitives::types::Balance,
        prepaid_gas: near_primitives::types::Gas,
        gas_weight: near_primitives::types::GasWeight,
    ) -> Result<()> {
        self.push_action(
            near_vm_runner::logic::mocks::mock_external::MockAction::FunctionCallWeight {
                receipt_index,
                method_name,
                args,
                attached_deposit,
                prepaid_gas,
                gas_weight,
            },
        );
        Ok(())
    }

    fn append_action_transfer(
        &mut self,
        receipt_index: near_vm_runner::logic::types::ReceiptIndex,
        deposit: near_primitives::types::Balance,
    ) -> Result<()> {
        self.push_action(
            near_vm_runner::logic::mocks::mock_external::MockAction::Transfer {
                receipt_index,
                deposit,
            },
        );
        Ok(())
    }

    fn append_action_stake(
        &mut self,
        receipt_index: near_vm_runner::logic::types::ReceiptIndex,
        stake: near_primitives::types::Balance,
        public_key: near_crypto::PublicKey,
    ) {
        self.push_action(
            near_vm_runner::logic::mocks::mock_external::MockAction::Stake {
                receipt_index,
                stake,
                public_key,
            },
        );
    }

    fn append_action_add_key_with_full_access(
        &mut self,
        receipt_index: near_vm_runner::logic::types::ReceiptIndex,
        public_key: near_crypto::PublicKey,
        nonce: near_primitives::types::Nonce,
    ) {
        self.push_action(
            near_vm_runner::logic::mocks::mock_external::MockAction::AddKeyWithFullAccess {
                receipt_index,
                public_key,
                nonce,
            },
        );
    }

    fn append_action_add_key_with_function_call(
        &mut self,
        receipt_index: near_vm_runner::logic::types::ReceiptIndex,
        public_key: near_crypto::PublicKey,
        nonce: near_primitives::types::Nonce,
        allowance: Option<near_primitives::types::Balance>,
        receiver_id: near_primitives::types::AccountId,
        method_names: Vec<Vec<u8>>,
    ) -> Result<()> {
        self.push_action(
            near_vm_runner::logic::mocks::mock_external::MockAction::AddKeyWithFunctionCall {
                receipt_index,
                public_key,
                nonce,
                allowance,
                receiver_id,
                method_names,
            },
        );
        Ok(())
    }

    fn append_action_delete_key(
        &mut self,
        receipt_index: near_vm_runner::logic::types::ReceiptIndex,
        public_key: near_crypto::PublicKey,
    ) {
        self.push_action(
            near_vm_runner::logic::mocks::mock_external::MockAction::DeleteKey {
                receipt_index,
                public_key,
            },
        );
    }

    fn append_action_delete_account(
        &mut self,
        receipt_index: near_vm_runner::logic::types::ReceiptIndex,
        beneficiary_id: near_primitives::types::AccountId,
    ) -> Result<()> {
        self.push_action(
            near_vm_runner::logic::mocks::mock_external::MockAction::DeleteAccount {
                receipt_index,
                beneficiary_id,
            },
        );
        Ok(())
    }

    fn get_receipt_receiver(
        &self,
        receipt_index: near_vm_runner::logic::types::ReceiptIndex,
    ) -> &near_primitives::types::AccountId {
        if !self.is_tx_emulator {
            panic!("Prohibited in view. `get_receipt_receiver`");
        }
        match &self.tx_actions[receipt_index as usize] {
            near_vm_runner::logic::mocks::mock_external::MockAction::CreateReceipt {
                receiver_id,
                ..
            } => receiver_id,
            _ => panic!("not a valid receipt index!"),
        }
    }

    fn append_action_deploy_global_contract(
        &mut self,
        receipt_index: near_vm_runner::logic::types::ReceiptIndex,
        code: Vec<u8>,
        mode: near_vm_runner::logic::types::GlobalContractDeployMode,
    ) -> Result<()> {
        self.push_action(
            near_vm_runner::logic::mocks::mock_external::MockAction::DeployGlobalContract {
                receipt_index,
                code,
                mode,
            },
        );
        Ok(())
    }

    fn append_action_use_global_contract(
        &mut self,
        receipt_index: near_vm_runner::logic::types::ReceiptIndex,
        contract_id: near_vm_runner::logic::types::GlobalContractIdentifier,
    ) -> Result<()> {
        self.push_action(
            near_vm_runner::logic::mocks::mock_external::MockAction::UseGlobalContract {
                receipt_index,
                contract_id,
            },
        );
        Ok(())
    }
}
