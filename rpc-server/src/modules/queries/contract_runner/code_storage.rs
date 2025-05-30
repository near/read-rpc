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
        }
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
        _key: &[u8],
        _value: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        Err(near_vm_runner::logic::VMLogicError::HostError(
            near_vm_runner::logic::HostError::ProhibitedInView {
                method_name: String::from("storage_set"),
            },
        ))
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
        _key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        Err(near_vm_runner::logic::VMLogicError::HostError(
            near_vm_runner::logic::HostError::ProhibitedInView {
                method_name: String::from("storage_remove"),
            },
        ))
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
        _receipt_indices: Vec<near_vm_runner::logic::types::ReceiptIndex>,
        _receiver_id: near_primitives::types::AccountId,
    ) -> Result<near_vm_runner::logic::types::ReceiptIndex> {
        panic!("Prohibited in view. `create_action_receipt`");
    }

    fn create_promise_yield_receipt(
        &mut self,
        _receiver_id: near_primitives::types::AccountId,
    ) -> Result<(
        near_vm_runner::logic::types::ReceiptIndex,
        near_indexer_primitives::CryptoHash,
    )> {
        panic!("Prohibited in view. `create_promise_yield_receipt`");
    }

    fn submit_promise_resume_data(
        &mut self,
        _data_id: near_indexer_primitives::CryptoHash,
        _data: Vec<u8>,
    ) -> Result<bool> {
        panic!("Prohibited in view. `submit_promise_resume_data`");
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
