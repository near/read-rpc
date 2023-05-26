use crate::modules::blocks::CacheBlock;
use clap::Parser;
use database::ScyllaStorageManager;
use scylla::prepared_statement::PreparedStatement;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
pub struct Opts {
    // near network rpc url
    #[clap(long, env = "NEAR_RPC_URL")]
    pub rpc_url: http::Uri,

    // Indexer bucket name
    #[clap(long, env = "AWS_BUCKET_NAME")]
    pub s3_bucket_name: String,

    // Scylla db url
    #[clap(long, env)]
    pub scylla_url: String,

    /// ScyllaDB user(login)
    #[clap(long, env)]
    pub scylla_user: Option<String>,

    /// ScyllaDB password
    #[clap(long, env)]
    pub scylla_password: Option<String>,

    /// ScyllaDB keepalive interval
    #[clap(long, env, default_value = "60")]
    pub scylla_keepalive_interval: u64,

    // AWS access key id
    #[clap(long, env = "AWS_ACCESS_KEY_ID")]
    pub access_key_id: String,

    // AWS secret access key
    #[clap(long, env = "AWS_SECRET_ACCESS_KEY")]
    pub secret_access_key: String,

    // AWS default region
    #[clap(long, env = "AWS_DEFAULT_REGION")]
    pub region: String,

    // AWS default region
    #[clap(long, env, default_value = "8000")]
    pub server_port: u16,
}

pub struct ServerContext {
    pub s3_client: aws_sdk_s3::Client,
    pub scylla_db_manager: std::sync::Arc<ScyllaDBManager>,
    pub near_rpc_client: near_jsonrpc_client::JsonRpcClient,
    pub s3_bucket_name: String,
    pub genesis_config: near_chain_configs::GenesisConfig,
    pub blocks_cache: std::sync::Arc<std::sync::RwLock<lru::LruCache<u64, CacheBlock>>>,
    pub final_block_height: std::sync::Arc<std::sync::atomic::AtomicU64>,
    pub compiled_contract_code_cache: std::sync::Arc<CompiledCodeCache>,
    pub contract_code_cache: std::sync::Arc<
        std::sync::RwLock<lru::LruCache<near_primitives::hash::CryptoHash, Vec<u8>>>,
    >,
}

pub struct CompiledCodeCache {
    pub local_cache: std::sync::Arc<
        std::sync::RwLock<
            lru::LruCache<
                near_primitives::hash::CryptoHash,
                near_primitives::types::CompiledContract,
            >,
        >,
    >,
}

impl near_primitives::types::CompiledContractCache for CompiledCodeCache {
    fn put(
        &self,
        key: &near_primitives::hash::CryptoHash,
        value: near_primitives::types::CompiledContract,
    ) -> std::io::Result<()> {
        self.local_cache.write().unwrap().put(*key, value);
        Ok(())
    }

    fn get(
        &self,
        key: &near_primitives::hash::CryptoHash,
    ) -> std::io::Result<Option<near_primitives::types::CompiledContract>> {
        Ok(self.local_cache.write().unwrap().get(key).cloned())
    }

    fn has(&self, key: &near_primitives::hash::CryptoHash) -> std::io::Result<bool> {
        Ok(self.local_cache.write().unwrap().get(key).is_some())
    }
}

pub struct ScyllaDBManager {
    scylla_session: std::sync::Arc<scylla::Session>,
    get_block_by_hash: PreparedStatement,
    get_block_by_chunk_id: PreparedStatement,
    get_all_state_keys: PreparedStatement,
    get_state_keys_by_prefix: PreparedStatement,
    get_state_key_value: PreparedStatement,
    get_account: PreparedStatement,
    get_contract_code: PreparedStatement,
    get_access_key: PreparedStatement,
    #[cfg(feature = "account_access_keys")]
    get_account_access_keys: PreparedStatement,
    get_receipt: PreparedStatement,
    get_transaction_by_hash: PreparedStatement,
}

#[async_trait::async_trait]
impl ScyllaStorageManager for ScyllaDBManager {
    async fn prepare(
        scylla_db_session: std::sync::Arc<scylla::Session>,
    ) -> anyhow::Result<Box<Self>> {
        Ok(Box::new(Self {
            scylla_session: scylla_db_session.clone(),

            get_block_by_hash: Self::prepare_query(
                &scylla_db_session,
                "SELECT block_height FROM state_indexer.chunks WHERE block_hash = ? LIMIT 1",
            ).await?,

            get_block_by_chunk_id: Self::prepare_query(
                &scylla_db_session,
                "SELECT block_height, shard_id FROM state_indexer.chunks WHERE chunk_hash = ? LIMIT 1",
            ).await?,

            get_all_state_keys: Self::prepare_query(
                &scylla_db_session,
                "SELECT data_key FROM state_indexer.account_state WHERE account_id = ?"
            ).await?,

            get_state_keys_by_prefix: Self::prepare_query(
                &scylla_db_session,
                "SELECT data_key FROM state_indexer.account_state WHERE account_id = ? AND data_key LIKE ?"
            ).await?,

            get_state_key_value: Self::prepare_query(
                &scylla_db_session,
                "SELECT data_value FROM state_indexer.state_changes_data WHERE account_id = ? AND block_height <= ? AND data_key = ? LIMIT 1"
            ).await?,

            get_account: Self::prepare_query(
                &scylla_db_session,
                "SELECT data_value FROM state_indexer.state_changes_account WHERE account_id = ? AND block_height <= ? LIMIT 1"
            ).await?,

            get_contract_code: Self::prepare_query(
                &scylla_db_session,
                "SELECT data_value FROM state_indexer.state_changes_contract WHERE account_id = ? AND block_height <= ? LIMIT 1"
            ).await?,

            get_access_key: Self::prepare_query(
                &scylla_db_session,
                "SELECT data_value FROM state_indexer.state_changes_access_key WHERE account_id = ? AND block_height <= ? AND data_key = ? LIMIT 1"
            ).await?,
            #[cfg(feature = "account_access_keys")]
            get_account_access_keys: Self::prepare_query(
                &scylla_db_session,
                "SELECT active_access_keys FROM state_indexer.account_access_keys WHERE account_id = ? AND block_height <= ? LIMIT 1"
            ).await?,

            get_receipt: Self::prepare_query(
                &scylla_db_session,
                "SELECT receipt_id, parent_transaction_hash, block_height, shard_id FROM tx_indexer.receipts_map WHERE receipt_id = ?"
            ).await?,

            // Using LIMIT 1 here as transactions is expected to be ordered by block_height but we know about hash collisions
            // ref: https://github.com/near/near-indexer-for-explorer/issues/84
            get_transaction_by_hash: Self::prepare_query(
                &scylla_db_session,
                "SELECT transaction_details FROM tx_indexer.transactions_details WHERE transaction_hash = ? LIMIT 1"
            ).await?,
        }))
    }
}

impl ScyllaDBManager {
    pub async fn get_block_by_hash(
        &self,
        block_hash: near_primitives::hash::CryptoHash,
    ) -> anyhow::Result<scylla::frame::response::result::Row> {
        let result = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_block_by_hash,
            (block_hash.to_string(),),
        )
        .await?
        .single_row()?;
        Ok(result)
    }

    pub async fn get_block_by_chunk_id(
        &self,
        chunk_id: near_primitives::hash::CryptoHash,
    ) -> anyhow::Result<scylla::frame::response::result::Row> {
        let result = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_block_by_chunk_id,
            (chunk_id.to_string(),),
        )
        .await?
        .single_row()?;
        Ok(result)
    }

    pub async fn get_all_state_keys(
        &self,
        account_id: &near_primitives::types::AccountId,
    ) -> anyhow::Result<Vec<scylla::frame::response::result::Row>> {
        let result = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_all_state_keys,
            (account_id.to_string(),),
        )
        .await?
        .rows;
        if let Some(rows) = result {
            Ok(rows)
        } else {
            Ok(vec![])
        }
    }

    pub async fn get_state_keys_by_prefix(
        &self,
        account_id: &near_primitives::types::AccountId,
        prefix: &[u8],
    ) -> anyhow::Result<Vec<scylla::frame::response::result::Row>> {
        let hex_str_prefix = hex::encode(prefix);
        let result = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_state_keys_by_prefix,
            (
                account_id.to_string(),
                format!("{hex_str_prefix}%").to_string(),
            ),
        )
        .await?
        .rows;
        if let Some(rows) = result {
            Ok(rows)
        } else {
            Ok(vec![])
        }
    }

    pub async fn get_state_key_value(
        &self,
        account_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        key_data: Vec<u8>,
    ) -> anyhow::Result<scylla::frame::response::result::Row> {
        let result = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_state_key_value,
            (
                account_id.to_string(),
                num_bigint::BigInt::from(block_height),
                hex::encode(&key_data).to_string(),
            ),
        )
        .await?
        .single_row()?;
        Ok(result)
    }

    pub async fn get_account(
        &self,
        account_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
    ) -> anyhow::Result<scylla::frame::response::result::Row> {
        let result = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_account,
            (
                account_id.to_string(),
                num_bigint::BigInt::from(block_height),
            ),
        )
        .await?
        .single_row()?;
        Ok(result)
    }

    pub async fn get_contract_code(
        &self,
        account_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
    ) -> anyhow::Result<scylla::frame::response::result::Row> {
        let result = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_contract_code,
            (
                account_id.to_string(),
                num_bigint::BigInt::from(block_height),
            ),
        )
        .await?
        .single_row()?;
        Ok(result)
    }

    pub async fn get_access_key(
        &self,
        account_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
        key_data: Vec<u8>,
    ) -> anyhow::Result<scylla::frame::response::result::Row> {
        let result = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_access_key,
            (
                account_id.to_string(),
                num_bigint::BigInt::from(block_height),
                hex::encode(&key_data).to_string(),
            ),
        )
        .await?
        .single_row()?;
        Ok(result)
    }

    #[cfg(feature = "account_access_keys")]
    pub async fn get_account_access_keys(
        &self,
        account_id: &near_primitives::types::AccountId,
        block_height: near_primitives::types::BlockHeight,
    ) -> anyhow::Result<scylla::frame::response::result::Row> {
        let result = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_account_access_keys,
            (
                account_id.to_string(),
                num_bigint::BigInt::from(block_height),
            ),
        )
        .await?
        .single_row()?;
        Ok(result)
    }

    pub async fn get_receipt_by_id(
        &self,
        receipt_id: near_primitives::hash::CryptoHash,
    ) -> anyhow::Result<scylla::frame::response::result::Row> {
        let result = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_receipt,
            (receipt_id.to_string(),),
        )
        .await?
        .single_row()?;
        Ok(result)
    }

    pub async fn get_transaction_by_hash(
        &self,
        transaction_hash: &str,
    ) -> anyhow::Result<scylla::frame::response::result::Row> {
        let result = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_transaction_by_hash,
            (transaction_hash.to_string(),),
        )
        .await?
        .single_row()?;
        Ok(result)
    }
}
