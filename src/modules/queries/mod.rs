use crate::modules::queries::utils::get_redis_stata_keys;
use futures::executor::block_on;
use std::collections::HashMap;
pub mod methods;
pub mod utils;

pub const ACCOUNT_SCOPE: &[u8] = b"a";
const CODE_SCOPE: &[u8] = b"c";
const ACCESS_KEY_SCOPE: &[u8] = b"k";
const DATA_SCOPE: &[u8] = b"d";

const MAX_LIMIT: u8 = 100;

#[tracing::instrument]
fn build_redis_block_hash_key(
    scope: &[u8],
    account_id: &near_primitives::types::AccountId,
    key_data: Option<&[u8]>,
) -> Vec<u8> {
    match key_data {
        Some(data) => [b"h:", scope, b":", account_id.as_bytes(), b":", data].concat(),
        None => [b"h:", scope, b":", account_id.as_bytes()].concat(),
    }
}

#[tracing::instrument]
fn build_redis_data_key(
    scope: &[u8],
    account_id: &near_primitives::types::AccountId,
    block_hash: Vec<u8>,
    key_data: Option<&[u8]>,
) -> Vec<u8> {
    match key_data {
        Some(data) => [
            b"d",
            scope,
            b":",
            account_id.as_bytes(),
            b":",
            data,
            b":",
            block_hash.as_slice(),
        ]
        .concat(),
        None => [
            b"d",
            scope,
            b":",
            account_id.as_bytes(),
            b":",
            block_hash.as_slice(),
        ]
        .concat(),
    }
}

#[tracing::instrument]
fn build_redis_state_key(scope: &[u8], account_id: &near_primitives::types::AccountId) -> Vec<u8> {
    [b"k:", scope, b":", account_id.as_bytes()].concat()
}

pub type Result<T> = ::std::result::Result<T, near_vm_logic::VMLogicError>;

pub struct CodeStorage {
    redis_client: redis::aio::ConnectionManager,
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
        redis_client: redis::aio::ConnectionManager,
        account_id: near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
    ) -> Self {
        Self {
            redis_client,
            account_id,
            block_height,
            validators: Default::default(), // TODO: Should be store list of validators in the current epoch.
            data_count: Default::default(), // TODO: Using for generate_data_id
        }
    }
}

impl near_vm_logic::External for CodeStorage {
    #[tracing::instrument(skip(self))]
    fn storage_set(&mut self, _key: &[u8], _value: &[u8]) -> Result<()> {
        Err(near_vm_logic::VMLogicError::HostError(
            near_vm_logic::HostError::ProhibitedInView {
                method_name: String::from("storage_set"),
            },
        ))
    }

    #[tracing::instrument(skip(self))]
    fn storage_get(&self, key: &[u8]) -> Result<Option<Box<dyn near_vm_logic::ValuePtr>>> {
        let get_redis_stata_keys = get_redis_stata_keys(
            DATA_SCOPE,
            self.redis_client.clone(),
            &self.account_id,
            self.block_height,
            key,
        );
        let redis_data = block_on(get_redis_stata_keys);
        Ok(redis_data.get(key).map(|value| {
            Box::new(StorageValuePtr {
                value: value.clone(),
            }) as Box<_>
        }))
    }

    #[tracing::instrument(skip(self))]
    fn storage_remove(&mut self, _key: &[u8]) -> Result<()> {
        Err(near_vm_logic::VMLogicError::HostError(
            near_vm_logic::HostError::ProhibitedInView {
                method_name: String::from("storage_remove"),
            },
        ))
    }

    #[tracing::instrument(skip(self))]
    fn storage_remove_subtree(&mut self, _prefix: &[u8]) -> Result<()> {
        Err(near_vm_logic::VMLogicError::HostError(
            near_vm_logic::HostError::ProhibitedInView {
                method_name: String::from("storage_remove_subtree"),
            },
        ))
    }

    #[tracing::instrument(skip(self))]
    fn storage_has_key(&mut self, key: &[u8]) -> Result<bool> {
        let get_redis_stata_keys = get_redis_stata_keys(
            DATA_SCOPE,
            self.redis_client.clone(),
            &self.account_id,
            self.block_height,
            key,
        );
        let redis_data = block_on(get_redis_stata_keys);
        Ok(redis_data.contains_key(key))
    }

    #[tracing::instrument(skip(self))]
    fn generate_data_id(&mut self) -> near_primitives::hash::CryptoHash {
        // TODO: Should be improvement in future
        // Generates some hash for the data ID to receive data.
        // This hash should not be functionality
        let data_id = near_primitives::hash::hash(&self.data_count.to_le_bytes());
        self.data_count += 1;
        data_id
    }

    #[tracing::instrument(skip(self))]
    fn get_trie_nodes_count(&self) -> near_primitives::types::TrieNodesCount {
        near_primitives::types::TrieNodesCount {
            db_reads: 0,
            mem_reads: 0,
        }
    }

    #[tracing::instrument(skip(self))]
    fn validator_stake(
        &self,
        account_id: &near_primitives::types::AccountId,
    ) -> Result<Option<near_primitives::types::Balance>> {
        Ok(self.validators.get(account_id).cloned())
    }

    #[tracing::instrument(skip(self))]
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
