use std::collections::HashMap;

use database::ReaderDbManager;
use futures::executor::block_on;

pub mod methods;
pub mod utils;

pub type Result<T> = ::std::result::Result<T, near_vm_runner::logic::VMLogicError>;

pub struct CodeStorage {
    db_manager: std::sync::Arc<Box<dyn ReaderDbManager + Sync + Send + 'static>>,
    account_id: near_primitives::types::AccountId,
    block_height: near_primitives::types::BlockHeight,
    validators: HashMap<near_primitives::types::AccountId, near_primitives::types::Balance>,
    data_count: u64,
    optimistic_data:
        HashMap<readnode_primitives::StateKey, Option<readnode_primitives::StateValue>>,
}

pub struct StorageValuePtr {
    value: Vec<u8>,
}

impl near_vm_runner::logic::ValuePtr for StorageValuePtr {
    fn len(&self) -> u32 {
        self.value.len() as u32
    }

    fn deref(&self) -> Result<Vec<u8>> {
        Ok(self.value.clone())
    }
}

impl CodeStorage {
    pub fn init(
        db_manager: std::sync::Arc<Box<dyn ReaderDbManager + Sync + Send + 'static>>,
        account_id: near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        validators: HashMap<near_primitives::types::AccountId, near_primitives::types::Balance>,
        optimistic_data: HashMap<
            readnode_primitives::StateKey,
            Option<readnode_primitives::StateValue>,
        >,
    ) -> Self {
        Self {
            db_manager,
            account_id,
            block_height,
            validators,
            data_count: Default::default(), // TODO: Using for generate_data_id
            optimistic_data,
        }
    }
}

impl near_vm_runner::logic::External for CodeStorage {
    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(self)))]
    fn storage_set(&mut self, _key: &[u8], _value: &[u8]) -> Result<()> {
        Err(near_vm_runner::logic::VMLogicError::HostError(
            near_vm_runner::logic::HostError::ProhibitedInView {
                method_name: String::from("storage_set"),
            },
        ))
    }

    #[cfg_attr(
        feature = "tracing-instrumentation",
        tracing::instrument(skip(self, _mode))
    )]
    fn storage_get(
        &self,
        key: &[u8],
        _mode: near_vm_runner::logic::StorageGetMode,
    ) -> Result<Option<Box<dyn near_vm_runner::logic::ValuePtr>>> {
        if let Some(value) = self.optimistic_data.get(key) {
            Ok(value.as_ref().map(|data| {
                Box::new(StorageValuePtr {
                    value: data.clone(),
                }) as Box<_>
            }))
        } else {
            let get_db_data = self.db_manager.get_state_key_value(
                &self.account_id,
                self.block_height,
                key.to_vec(),
            );
            let (_, data) = block_on(get_db_data);
            Ok(if !data.is_empty() {
                Some(Box::new(StorageValuePtr { value: data }) as Box<_>)
            } else {
                None
            })
        }
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(self)))]
    fn storage_remove(&mut self, _key: &[u8]) -> Result<()> {
        Err(near_vm_runner::logic::VMLogicError::HostError(
            near_vm_runner::logic::HostError::ProhibitedInView {
                method_name: String::from("storage_remove"),
            },
        ))
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(self)))]
    fn storage_remove_subtree(&mut self, _prefix: &[u8]) -> Result<()> {
        Err(near_vm_runner::logic::VMLogicError::HostError(
            near_vm_runner::logic::HostError::ProhibitedInView {
                method_name: String::from("storage_remove_subtree"),
            },
        ))
    }

    #[cfg_attr(
        feature = "tracing-instrumentation",
        tracing::instrument(skip(self, _mode))
    )]
    fn storage_has_key(
        &mut self,
        key: &[u8],
        _mode: near_vm_runner::logic::StorageGetMode,
    ) -> Result<bool> {
        if let Some(value) = self.optimistic_data.get(key) {
            Ok(value.is_some())
        } else {
            let get_db_state_keys = self.db_manager.get_state_key_value(
                &self.account_id,
                self.block_height,
                key.to_vec(),
            );
            let (_, data) = block_on(get_db_state_keys);
            Ok(!data.is_empty())
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

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(self)))]
    fn get_trie_nodes_count(&self) -> near_vm_runner::logic::TrieNodesCount {
        near_vm_runner::logic::TrieNodesCount {
            db_reads: 0,
            mem_reads: 0,
        }
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
        _receipt_indices: Vec<near_vm_runner::logic::types::ReceiptIndex>,
        _receiver_id: near_primitives::types::AccountId,
    ) -> Result<near_vm_runner::logic::types::ReceiptIndex> {
        Err(near_vm_runner::logic::VMLogicError::HostError(
            near_vm_runner::logic::HostError::ProhibitedInView {
                method_name: String::from("create_action_receipt"),
            },
        ))
    }

    fn create_promise_yield_receipt(
        &mut self,
        _receiver_id: near_primitives::types::AccountId,
    ) -> Result<(
        near_vm_runner::logic::types::ReceiptIndex,
        near_indexer_primitives::CryptoHash,
    )> {
        Err(near_vm_runner::logic::VMLogicError::HostError(
            near_vm_runner::logic::HostError::ProhibitedInView {
                method_name: String::from("create_promise_yield_receipt"),
            },
        ))
    }

    fn submit_promise_resume_data(
        &mut self,
        _data_id: near_indexer_primitives::CryptoHash,
        _data: Vec<u8>,
    ) -> Result<bool> {
        Err(near_vm_runner::logic::VMLogicError::HostError(
            near_vm_runner::logic::HostError::ProhibitedInView {
                method_name: String::from("submit_promise_resume_data"),
            },
        ))
    }

    fn append_action_create_account(
        &mut self,
        _receipt_index: near_vm_runner::logic::types::ReceiptIndex,
    ) -> Result<()> {
        Ok(())
    }

    fn append_action_deploy_contract(
        &mut self,
        _receipt_index: near_vm_runner::logic::types::ReceiptIndex,
        _code: Vec<u8>,
    ) -> Result<()> {
        Ok(())
    }

    fn append_action_function_call_weight(
        &mut self,
        _receipt_index: near_vm_runner::logic::types::ReceiptIndex,
        _method_name: Vec<u8>,
        _args: Vec<u8>,
        _attached_deposit: near_primitives::types::Balance,
        _prepaid_gas: near_primitives::types::Gas,
        _gas_weight: near_primitives::types::GasWeight,
    ) -> Result<()> {
        Ok(())
    }

    fn append_action_transfer(
        &mut self,
        _receipt_index: near_vm_runner::logic::types::ReceiptIndex,
        _deposit: near_primitives::types::Balance,
    ) -> Result<()> {
        Ok(())
    }

    fn append_action_stake(
        &mut self,
        _receipt_index: near_vm_runner::logic::types::ReceiptIndex,
        _stake: near_primitives::types::Balance,
        _public_key: near_crypto::PublicKey,
    ) {
    }

    fn append_action_add_key_with_full_access(
        &mut self,
        _receipt_index: near_vm_runner::logic::types::ReceiptIndex,
        _public_key: near_crypto::PublicKey,
        _nonce: near_primitives::types::Nonce,
    ) {
    }

    fn append_action_add_key_with_function_call(
        &mut self,
        _receipt_index: near_vm_runner::logic::types::ReceiptIndex,
        _public_key: near_crypto::PublicKey,
        _nonce: near_primitives::types::Nonce,
        _allowance: Option<near_primitives::types::Balance>,
        _receiver_id: near_primitives::types::AccountId,
        _method_names: Vec<Vec<u8>>,
    ) -> Result<()> {
        Ok(())
    }

    fn append_action_delete_key(
        &mut self,
        _receipt_index: near_vm_runner::logic::types::ReceiptIndex,
        _public_key: near_crypto::PublicKey,
    ) {
    }

    fn append_action_delete_account(
        &mut self,
        _receipt_index: near_vm_runner::logic::types::ReceiptIndex,
        _beneficiary_id: near_primitives::types::AccountId,
    ) -> Result<()> {
        Ok(())
    }

    fn get_receipt_receiver(
        &self,
        _receipt_index: near_vm_runner::logic::types::ReceiptIndex,
    ) -> &near_primitives::types::AccountId {
        panic!("Prohibited in view. `get_receipt_receiver`");
    }
}
