use crate::modules::queries::utils::{fetch_data_from_scylla_db, get_stata_keys_from_scylla};
use futures::executor::block_on;
use std::collections::HashMap;

pub mod methods;
pub mod utils;

const ACCOUNT_SCOPE: &str = "account";
const CODE_SCOPE: &str = "contract";
const ACCESS_KEY_SCOPE: &str = "access_key";
const DATA_SCOPE: &str = "data";

const MAX_LIMIT: u8 = 100;

pub type Result<T> = ::std::result::Result<T, near_vm_logic::VMLogicError>;

pub struct CodeStorage {
    scylla_db_client: std::sync::Arc<scylla::Session>,
    account_id: near_primitives::types::AccountId,
    block_height: near_primitives::types::BlockHeight,
    validators: HashMap<near_primitives::types::AccountId, near_primitives::types::Balance>,
    data_count: u64,
}

pub struct StorageValuePtr {
    value: Vec<u8>,
}

impl near_vm_logic::ValuePtr for StorageValuePtr {
    fn len(&self) -> u32 {
        self.value.len() as u32
    }

    fn deref(&self) -> Result<Vec<u8>> {
        Ok(self.value.clone())
    }
}

impl CodeStorage {
    pub fn init(
        scylla_db_client: std::sync::Arc<scylla::Session>,
        account_id: near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
    ) -> Self {
        Self {
            scylla_db_client,
            account_id,
            block_height,
            validators: Default::default(), // TODO: Should be store list of validators in the current epoch.
            data_count: Default::default(), // TODO: Using for generate_data_id
        }
    }
}

impl near_vm_logic::External for CodeStorage {
    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(self)))]
    fn storage_set(&mut self, _key: &[u8], _value: &[u8]) -> Result<()> {
        Err(near_vm_logic::VMLogicError::HostError(
            near_vm_logic::HostError::ProhibitedInView {
                method_name: String::from("storage_set"),
            },
        ))
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(self)))]
    fn storage_get(&self, key: &[u8]) -> Result<Option<Box<dyn near_vm_logic::ValuePtr>>> {
        let get_db_data = fetch_data_from_scylla_db(
            DATA_SCOPE,
            &self.scylla_db_client,
            &self.account_id,
            self.block_height,
            Some(key.to_vec()),
        );
        let db_data = block_on(get_db_data).unwrap();
        if db_data.is_empty() {
            Ok(None)
        } else {
            Ok(Some(
                Box::new(StorageValuePtr {
                    value: db_data,
                }) as Box<_>
            ))
        }
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(self)))]
    fn storage_remove(&mut self, _key: &[u8]) -> Result<()> {
        Err(near_vm_logic::VMLogicError::HostError(
            near_vm_logic::HostError::ProhibitedInView {
                method_name: String::from("storage_remove"),
            },
        ))
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(self)))]
    fn storage_remove_subtree(&mut self, _prefix: &[u8]) -> Result<()> {
        Err(near_vm_logic::VMLogicError::HostError(
            near_vm_logic::HostError::ProhibitedInView {
                method_name: String::from("storage_remove_subtree"),
            },
        ))
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(self)))]
    fn storage_has_key(&mut self, key: &[u8]) -> Result<bool> {
        let get_db_stata_keys = get_stata_keys_from_scylla(
            DATA_SCOPE,
            &self.scylla_db_client,
            &self.account_id,
            self.block_height,
            key,
        );
        let db_data = block_on(get_db_stata_keys);
        Ok(db_data.contains_key(key))
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
    fn get_trie_nodes_count(&self) -> near_primitives::types::TrieNodesCount {
        near_primitives::types::TrieNodesCount {
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
        // TODO: Should be works after implementing validators. See comment above.
        // Ok(self.validators.values().sum())
        Err(near_vm_logic::VMLogicError::HostError(
            near_vm_logic::HostError::ProhibitedInView {
                method_name: String::from("validator_total_stake"),
            },
        ))
    }
}
