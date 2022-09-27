use std::collections::HashMap;
pub mod methods;
pub mod utils;

pub const ACCOUNT_SCOPE: &[u8] = b"a";
const CODE_SCOPE: &[u8] = b"c";
const ACCESS_KEY_SCOPE: &[u8] = b"k";
const DATA_SCOPE: &[u8] = b"d";

const MAX_LIMIT: u8 = 100;

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

fn build_redis_state_key(scope: &[u8], account_id: &near_primitives::types::AccountId) -> Vec<u8> {
    [b"k:", scope, b":", account_id.as_bytes()].concat()
}

pub type Result<T> = ::std::result::Result<T, near_vm_logic::VMLogicError>;

pub struct CodeStorage {
    state: HashMap<Vec<u8>, Vec<u8>>,
    validators:
        HashMap<near_primitives_core::types::AccountId, near_primitives_core::types::Balance>,
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
    pub fn init(state: HashMap<Vec<u8>, Vec<u8>>) -> Self {
        Self {
            state,
            validators: Default::default(),
        }
    }
}

impl near_vm_logic::External for CodeStorage {
    fn storage_set(&mut self, _key: &[u8], _value: &[u8]) -> Result<()> {
        unimplemented!("Method not available for view calls")
    }

    fn storage_get(&self, key: &[u8]) -> Result<Option<Box<dyn near_vm_logic::ValuePtr>>> {
        Ok(self.state.get(key).map(|value| {
            Box::new(StorageValuePtr {
                value: value.clone(),
            }) as Box<_>
        }))
    }

    fn storage_remove(&mut self, _key: &[u8]) -> Result<()> {
        unimplemented!("Method not available for view calls")
    }

    fn storage_remove_subtree(&mut self, _prefix: &[u8]) -> Result<()> {
        unimplemented!("Method not available for view calls")
    }

    fn storage_has_key(&mut self, key: &[u8]) -> Result<bool> {
        Ok(self.state.contains_key(key))
    }

    fn generate_data_id(&mut self) -> near_primitives::hash::CryptoHash {
        unimplemented!("Method not available for view calls")
    }

    fn get_trie_nodes_count(&self) -> near_primitives::types::TrieNodesCount {
        near_primitives::types::TrieNodesCount {
            db_reads: 0,
            mem_reads: 0,
        }
    }

    fn validator_stake(
        &self,
        account_id: &near_primitives::types::AccountId,
    ) -> Result<Option<near_primitives::types::Balance>> {
        Ok(self.validators.get(account_id).cloned())
    }

    fn validator_total_stake(&self) -> Result<near_primitives::types::Balance> {
        Ok(self.validators.values().sum())
    }
}
