use crate::PageToken;
use near_crypto::PublicKey;
use near_indexer_primitives::CryptoHash;
use near_primitives::account::{AccessKey, Account};
use near_primitives::types::{AccountId, BlockHeight, ShardId};
use readnode_primitives::{
    BlockHeightShardId, EpochValidatorsInfo, QueryData, ReceiptRecord, StateKey, StateValue,
    TransactionDetails,
};
use std::collections::HashMap;

pub struct MockDBManager {
    blocks: HashMap<CryptoHash, BlockHeight>,
    chunks: HashMap<CryptoHash, BlockHeightShardId>,
    state_keys: HashMap<AccountId, Vec<StateKey>>,
    state: HashMap<AccountId, HashMap<BlockHeight, HashMap<StateKey, StateValue>>>,
    accounts: HashMap<AccountId, HashMap<BlockHeight, Account>>,
    contract_codes: HashMap<AccountId, HashMap<BlockHeight, Vec<u8>>>,
    access_keys: HashMap<AccountId, HashMap<BlockHeight, HashMap<PublicKey, AccessKey>>>,

    receipt: HashMap<CryptoHash, ReceiptRecord>,
    transactions: HashMap<String, TransactionDetails>,
    block_height_shard_id: HashMap<BlockHeight, HashMap<ShardId, BlockHeightShardId>>,
    epoch_validators: HashMap<CryptoHash, EpochValidatorsInfo>,
}

impl MockDBManager {
    pub fn new() -> Self {
        // TODO: Add some default data
        Self {
            blocks: Default::default(),
            chunks: Default::default(),
            state_keys: Default::default(),
            state: Default::default(),
            accounts: Default::default(),
            contract_codes: Default::default(),
            access_keys: Default::default(),
            receipt: Default::default(),
            transactions: Default::default(),
            block_height_shard_id: Default::default(),
            epoch_validators: Default::default(),
        }
    }
}
#[async_trait::async_trait]
impl crate::BaseDbManager for MockDBManager {
    async fn new(_config: &configuration::DatabaseConfig) -> anyhow::Result<Box<Self>> {
        Ok(Box::new(Self::new()))
    }
}

#[async_trait::async_trait]
impl crate::ReaderDbManager for MockDBManager {
    async fn get_block_by_hash(&self, block_hash: CryptoHash) -> anyhow::Result<u64> {
        self.blocks
            .get(&block_hash)
            .map(|v| *v)
            .ok_or_else(|| anyhow::anyhow!("Block not found"))
    }
    async fn get_block_by_chunk_hash(
        &self,
        chunk_hash: CryptoHash,
    ) -> anyhow::Result<BlockHeightShardId> {
        self.chunks
            .get(&chunk_hash)
            .map(|v| BlockHeightShardId(v.0, v.1))
            .ok_or_else(|| anyhow::anyhow!("Chunk not found"))
    }

    async fn get_state_keys_all(&self, account_id: &AccountId) -> anyhow::Result<Vec<StateKey>> {
        self.state_keys
            .get(account_id)
            .map(|v| v.clone())
            .ok_or_else(|| anyhow::anyhow!("State keys not found"))
    }

    async fn get_state_keys_by_page(
        &self,
        account_id: &AccountId,
        page_token: PageToken,
    ) -> anyhow::Result<(Vec<StateKey>, PageToken)> {
        // we don't have pagination in mock db
        self.state_keys
            .get(account_id)
            .map(|v| (v.clone(), page_token))
            .ok_or_else(|| anyhow::anyhow!("State keys not found"))
    }

    async fn get_state_keys_by_prefix(
        &self,
        account_id: &AccountId,
        prefix: &[u8],
    ) -> anyhow::Result<Vec<StateKey>> {
        Ok(self
            .state_keys
            .get(account_id)
            .map(|v| v.clone())
            .unwrap_or(vec![])
            .into_iter()
            .filter(|k| k.starts_with(prefix))
            .collect())
    }

    async fn get_state_key_value(
        &self,
        account_id: &AccountId,
        block_height: BlockHeight,
        key_data: StateKey,
    ) -> (StateKey, StateValue) {
        self.state
            .get(account_id)
            .and_then(|v| v.get(&block_height))
            .and_then(|v| v.get(&key_data))
            .map(|v| (key_data.clone(), v.clone()))
            .unwrap_or_else(|| (key_data, StateValue::default()))
    }

    async fn get_account(
        &self,
        account_id: &AccountId,
        request_block_height: BlockHeight,
    ) -> anyhow::Result<QueryData<Account>> {
        self.accounts
            .get(account_id)
            .and_then(|v| v.get(&request_block_height))
            .map(|v| {
                readnode_primitives::QueryData {
                    data: v.clone(),
                    block_height: request_block_height,
                    block_hash: Default::default(),
                }
            })
            .ok_or_else(|| anyhow::anyhow!("Account not found"))
    }

    async fn get_contract_code(
        &self,
        account_id: &AccountId,
        request_block_height: BlockHeight,
    ) -> anyhow::Result<QueryData<Vec<u8>>> {
        self.contract_codes
            .get(account_id)
            .and_then(|v| v.get(&request_block_height))
            .map(|v| {
                readnode_primitives::QueryData {
                    data: v.clone(),
                    block_height: request_block_height,
                    block_hash: Default::default(),
                }
            })
            .ok_or_else(|| anyhow::anyhow!("Contract code not found"))
    }

    async fn get_access_key(
        &self,
        account_id: &AccountId,
        request_block_height: BlockHeight,
        public_key: PublicKey,
    ) -> anyhow::Result<QueryData<AccessKey>> {
        self.access_keys
            .get(account_id)
            .and_then(|v| v.get(&request_block_height))
            .and_then(|v| v.get(&public_key))
            .map(|v| {
                readnode_primitives::QueryData {
                    data: v.clone(),
                    block_height: request_block_height,
                    block_hash: Default::default(),
                }
            })
            .ok_or_else(|| anyhow::anyhow!("Access key not found"))
    }

    async fn get_account_access_keys(
        &self,
        account_id: &AccountId,
        block_height: BlockHeight,
    ) -> anyhow::Result<HashMap<String, Vec<u8>>> {
        self.access_keys
            .get(account_id)
            .and_then(|v| v.get(&block_height))
            .map(|v| {
                v.iter()
                    .map(|(k, v)| (k.to_string(), borsh::to_vec(v).unwrap())).collect()
            })
            .ok_or_else(|| anyhow::anyhow!("Access keys not found"))
    }

    async fn get_receipt_by_id(&self, receipt_id: CryptoHash) -> anyhow::Result<ReceiptRecord> {
        self.receipt
            .get(&receipt_id)
            .map(|v| *v.clone())
            .ok_or_else(|| anyhow::anyhow!("Receipt not found"))
    }

    async fn get_transaction_by_hash(
        &self,
        transaction_hash: &str,
    ) -> anyhow::Result<TransactionDetails> {
        match self.get_indexed_transaction_by_hash(transaction_hash).await {
            Ok(v) => Ok(v),
            Err(_) => self.get_indexing_transaction_by_hash(transaction_hash).await,
        }
    }

    async fn get_indexed_transaction_by_hash(
        &self,
        transaction_hash: &str,
    ) -> anyhow::Result<TransactionDetails> {
        self.transactions
            .get(transaction_hash)
            .map(|v| v.clone())
            .ok_or_else(|| anyhow::anyhow!("Transaction not found"))
    }

    async fn get_indexing_transaction_by_hash(
        &self,
        transaction_hash: &str,
    ) -> anyhow::Result<TransactionDetails> {
        self.transactions
            .get(transaction_hash)
            .map(|v| v.clone())
            .ok_or_else(|| anyhow::anyhow!("Transaction not found"))
    }

    async fn get_block_by_height_and_shard_id(
        &self,
        block_height: BlockHeight,
        shard_id: ShardId,
    ) -> anyhow::Result<BlockHeightShardId> {
        self.block_height_shard_id
            .get(&block_height)
            .and_then(|v| v.get(&shard_id))
            .map(|v| *v.clone())
            .ok_or_else(|| anyhow::anyhow!("Block not found"))
    }

    async fn get_validators_by_epoch_id(
        &self,
        epoch_id: CryptoHash,
    ) -> anyhow::Result<EpochValidatorsInfo> {
        self.epoch_validators
            .get(&epoch_id)
            .map(|v| *v.clone())
            .ok_or_else(|| anyhow::anyhow!("Validators not found"))
    }

    async fn get_validators_by_end_block_height(
        &self,
        block_height: BlockHeight,
    ) -> anyhow::Result<EpochValidatorsInfo> {
        self.epoch_validators
            .values()
            .find(|v| v.epoch_start_height == block_height)
            .map(|v| *v.clone())
            .ok_or_else(|| anyhow::anyhow!("Validators not found"))
    }
}
