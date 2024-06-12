use near_chain_configs::ProtocolConfigView;
use near_crypto::PublicKey;
use near_indexer_primitives::CryptoHash;
use near_primitives::account::{AccessKey, Account};
use near_primitives::types::{AccountId, BlockHeight, ShardId};
use readnode_primitives::{BlockHeightShardId, EpochValidatorsInfo, QueryData, ReceiptRecord, StateKey, StateValue, TransactionDetails};
use crate::PageToken;
use crate::postgres::PostgresDBManager;


#[async_trait::async_trait]
impl crate::ReaderDbManager for PostgresDBManager {
    async fn get_block_by_hash(&self, block_hash: CryptoHash, method_name: &str) -> anyhow::Result<u64> {
        todo!()
    }

    async fn get_block_by_chunk_hash(&self, chunk_hash: CryptoHash, method_name: &str) -> anyhow::Result<BlockHeightShardId> {
        todo!()
    }

    async fn get_state_keys_all(&self, account_id: &AccountId, method_name: &str) -> anyhow::Result<Vec<StateKey>> {
        todo!()
    }

    async fn get_state_keys_by_page(&self, account_id: &AccountId, page_token: PageToken, method_name: &str) -> anyhow::Result<(Vec<StateKey>, PageToken)> {
        todo!()
    }

    async fn get_state_keys_by_prefix(&self, account_id: &AccountId, prefix: &[u8], method_name: &str) -> anyhow::Result<Vec<StateKey>> {
        todo!()
    }

    async fn get_state_key_value(&self, account_id: &AccountId, block_height: BlockHeight, key_data: StateKey, method_name: &str) -> (StateKey, StateValue) {
        todo!()
    }

    async fn get_account(&self, account_id: &AccountId, request_block_height: BlockHeight, method_name: &str) -> anyhow::Result<QueryData<Account>> {
        todo!()
    }

    async fn get_contract_code(&self, account_id: &AccountId, request_block_height: BlockHeight, method_name: &str) -> anyhow::Result<QueryData<Vec<u8>>> {
        todo!()
    }

    async fn get_access_key(&self, account_id: &AccountId, request_block_height: BlockHeight, public_key: PublicKey, method_name: &str) -> anyhow::Result<QueryData<AccessKey>> {
        todo!()
    }

    async fn get_receipt_by_id(&self, receipt_id: CryptoHash, method_name: &str) -> anyhow::Result<ReceiptRecord> {
        todo!()
    }

    async fn get_transaction_by_hash(&self, transaction_hash: &str, method_name: &str) -> anyhow::Result<(u64, TransactionDetails)> {
        todo!()
    }

    async fn get_indexed_transaction_by_hash(&self, transaction_hash: &str, method_name: &str) -> anyhow::Result<(u64, TransactionDetails)> {
        todo!()
    }

    async fn get_indexing_transaction_by_hash(&self, transaction_hash: &str, method_name: &str) -> anyhow::Result<(u64, TransactionDetails)> {
        todo!()
    }

    async fn get_block_by_height_and_shard_id(&self, block_height: BlockHeight, shard_id: ShardId, method_name: &str) -> anyhow::Result<BlockHeightShardId> {
        todo!()
    }

    async fn get_validators_by_epoch_id(&self, epoch_id: CryptoHash, method_name: &str) -> anyhow::Result<EpochValidatorsInfo> {
        todo!()
    }

    async fn get_validators_by_end_block_height(&self, block_height: BlockHeight, method_name: &str) -> anyhow::Result<EpochValidatorsInfo> {
        todo!()
    }

    async fn get_protocol_config_by_epoch_id(&self, epoch_id: CryptoHash, method_name: &str) -> anyhow::Result<ProtocolConfigView> {
        todo!()
    }
}