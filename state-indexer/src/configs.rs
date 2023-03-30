use bigdecimal::ToPrimitive;
pub use clap::{Parser, Subcommand};
use database::ScyllaStorageManager;
use near_jsonrpc_client::{methods, JsonRpcClient};
use near_lake_framework::near_indexer_primitives::types::{BlockReference, Finality};
use scylla::prepared_statement::PreparedStatement;
use std::collections::HashMap;

/// NEAR Indexer for Explorer
/// Watches for stream of blocks from the chain
#[derive(Parser, Debug)]
#[clap(
    version,
    author,
    about,
    setting(clap::AppSettings::DisableHelpSubcommand),
    setting(clap::AppSettings::PropagateVersion),
    setting(clap::AppSettings::NextLineHelp)
)]
pub(crate) struct Opts {
    /// Indexer ID to handle meta data about the instance
    #[clap(long, env)]
    pub indexer_id: String,
    /// ScyllaDB connection string
    #[clap(long, default_value = "127.0.0.1:9042", env)]
    pub scylla_url: String,
    /// ScyllaDB user(login)
    #[clap(long, env)]
    pub scylla_user: Option<String>,
    /// ScyllaDB password
    #[clap(long, env)]
    pub scylla_password: Option<String>,
    /// Metrics HTTP server port
    #[clap(long, default_value = "8080", env)]
    pub port: u16,
    /// Chain ID: testnet or mainnet
    #[clap(subcommand)]
    pub chain_id: ChainId,
}

#[derive(Subcommand, Debug, Clone)]
pub enum ChainId {
    #[clap(subcommand)]
    Mainnet(StartOptions),
    #[clap(subcommand)]
    Testnet(StartOptions),
}

#[allow(clippy::enum_variant_names)]
#[derive(Subcommand, Debug, Clone)]
pub enum StartOptions {
    FromBlock { height: u64 },
    FromInterruption,
    FromLatest,
}

impl Opts {
    /// Returns [StartOptions] for current [Opts]
    pub fn start_options(&self) -> &StartOptions {
        match &self.chain_id {
            ChainId::Mainnet(start_options) | ChainId::Testnet(start_options) => start_options,
        }
    }

    pub fn rpc_url(&self) -> &str {
        match self.chain_id {
            ChainId::Mainnet(_) => "https://rpc.mainnet.near.org",
            ChainId::Testnet(_) => "https://rpc.testnet.near.org",
        }
    }
}

impl Opts {
    pub async fn to_lake_config(
        &self,
        scylladb_session: &std::sync::Arc<scylla::Session>,
    ) -> anyhow::Result<near_lake_framework::LakeConfig> {
        let config_builder = near_lake_framework::LakeConfigBuilder::default();

        Ok(match &self.chain_id {
            ChainId::Mainnet(_) => config_builder
                .mainnet()
                .start_block_height(get_start_block_height(self, scylladb_session).await?),
            ChainId::Testnet(_) => config_builder
                .testnet()
                .start_block_height(get_start_block_height(self, scylladb_session).await?),
        }
        .build()
        .expect("Failed to build LakeConfig"))
    }
}

async fn get_start_block_height(
    opts: &Opts,
    scylladb_session: &std::sync::Arc<scylla::Session>,
) -> anyhow::Result<u64> {
    match opts.start_options() {
        StartOptions::FromBlock { height } => Ok(*height),
        StartOptions::FromInterruption => {
            let row = scylladb_session
                .query("SELECT last_processed_block_height FROM meta WHERE indexer_id = ?", (&opts.indexer_id,))
                .await?
                .single_row();

            if let Ok(row) = row {
                let (block_height,): (num_bigint::BigInt,) = row.into_typed::<(num_bigint::BigInt,)>()?;
                Ok(block_height.to_u64().expect("Failed to convert BigInt to u64"))
            } else {
                Ok(final_block_height(opts).await)
            }
        }
        StartOptions::FromLatest => Ok(final_block_height(opts).await),
    }
}

async fn final_block_height(opts: &Opts) -> u64 {
    tracing::debug!(target: crate::INDEXER, "Fetching final block from NEAR RPC",);
    let client = JsonRpcClient::connect(opts.rpc_url());
    let request = methods::block::RpcBlockRequest {
        block_reference: BlockReference::Finality(Finality::Final),
    };

    let latest_block = client.call(request).await.unwrap();

    latest_block.header.height
}

pub(crate) struct ScyllaDBManager {
    scylla_session: std::sync::Arc<scylla::Session>,

    add_state_changes: PreparedStatement,
    delete_state_changes: PreparedStatement,

    add_access_key: PreparedStatement,
    delete_access_key: PreparedStatement,
    add_account_access_keys: PreparedStatement,
    get_account_access_keys: PreparedStatement,

    add_contract: PreparedStatement,
    delete_contract: PreparedStatement,

    add_account: PreparedStatement,
    delete_account: PreparedStatement,

    add_block: PreparedStatement,
    add_account_state: PreparedStatement,
    update_meta: PreparedStatement,
}

#[async_trait::async_trait]
impl ScyllaStorageManager for ScyllaDBManager {
    async fn create_tables(scylla_db_session: &scylla::Session) -> anyhow::Result<()> {
        scylla_db_session.use_keyspace("state_indexer", false).await?;
        scylla_db_session
            .query(
                "
                CREATE TABLE IF NOT EXISTS state_changes_data (
                    account_id varchar,
                    block_height varint,
                    block_hash varchar,
                    data_key varchar,
                    data_value BLOB,
                    PRIMARY KEY ((account_id, data_key), block_height)
                ) WITH CLUSTERING ORDER BY (block_height DESC)
            ",
                &[],
            )
            .await?;

        scylla_db_session
            .query(
                "
                CREATE TABLE IF NOT EXISTS state_changes_access_key (
                    account_id varchar,
                    block_height varint,
                    block_hash varchar,
                    data_key varchar,
                    data_value BLOB,
                    PRIMARY KEY ((account_id, data_key), block_height)
                ) WITH CLUSTERING ORDER BY (block_height DESC)
            ",
                &[],
            )
            .await?;

        scylla_db_session
            .query(
                "
                CREATE TABLE IF NOT EXISTS account_access_keys (
                    account_id varchar,
                    block_height varint,
                    active_access_keys map<varchar, BLOB>,
                    PRIMARY KEY (account_id, block_height)
                ) WITH CLUSTERING ORDER BY (block_height DESC)
            ",
                &[],
            )
            .await?;

        scylla_db_session
            .query(
                "
                CREATE TABLE IF NOT EXISTS state_changes_contract (
                    account_id varchar,
                    block_height varint,
                    block_hash varchar,
                    data_value BLOB,
                    PRIMARY KEY (account_id, block_height)
                ) WITH CLUSTERING ORDER BY (block_height DESC)
            ",
                &[],
            )
            .await?;

        scylla_db_session
            .query(
                "
                CREATE TABLE IF NOT EXISTS state_changes_account (
                    account_id varchar,
                    block_height varint,
                    block_hash varchar,
                    data_value BLOB,
                    PRIMARY KEY (account_id, block_height)
                ) WITH CLUSTERING ORDER BY (block_height DESC)
            ",
                &[],
            )
            .await?;

        scylla_db_session
            .query(
                "
                CREATE TABLE IF NOT EXISTS blocks (
                    block_height varint,
                    block_hash varchar,
                    chunks list<varchar>,
                    PRIMARY KEY (block_hash)
                )
            ",
                &[],
            )
            .await?;

        scylla_db_session
            .query(
                "
                CREATE TABLE IF NOT EXISTS meta (
                    indexer_id varchar PRIMARY KEY,
                    last_processed_block_height varint
                )
            ",
                &[],
            )
            .await?;

        scylla_db_session
            .query(
                "
                CREATE TABLE IF NOT EXISTS account_state (
                    account_id varchar,
                    data_key varchar,
                    PRIMARY KEY (account_id, data_key),
                )
            ",
                &[],
            )
            .await?;
        Ok(())
    }

    async fn create_keyspace(scylla_db_session: &scylla::Session) -> anyhow::Result<()> {
        scylla_db_session.query(
            "CREATE KEYSPACE IF NOT EXISTS state_indexer WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}",
            &[]
        ).await?;
        Ok(())
    }

    async fn prepare(scylla_db_session: std::sync::Arc<scylla::Session>) -> anyhow::Result<Box<Self>> {
        Ok(Box::new(Self {
            scylla_session: scylla_db_session.clone(),
            add_state_changes: Self::prepare_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.state_changes_data
                    (account_id, block_height, block_hash, data_key, data_value)
                    VALUES(?, ?, ?, ?, ?)",
            )
            .await?,
            delete_state_changes: Self::prepare_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.state_changes_data
                    (account_id, block_height, block_hash, data_key, data_value)
                    VALUES(?, ?, ?, ?, NULL)",
            )
            .await?,

            add_access_key: Self::prepare_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.state_changes_access_key
                    (account_id, block_height, block_hash, data_key, data_value)
                    VALUES(?, ?, ?, ?, ?)",
            )
            .await?,
            delete_access_key: Self::prepare_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.state_changes_access_key
                    (account_id, block_height, block_hash, data_key, data_value)
                    VALUES(?, ?, ?, ?, NULL)",
            )
            .await?,
            add_account_access_keys: Self::prepare_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.account_access_keys
                    (account_id, block_height, active_access_keys)
                    VALUES(?, ?, ?)",
            )
            .await?,
            get_account_access_keys: Self::prepare_query(
                &scylla_db_session,
                "SELECT active_access_keys FROM state_indexer.account_access_keys
                    WHERE account_id = ? AND block_height < ? LIMIT 1",
            )
            .await?,

            add_contract: Self::prepare_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.state_changes_contract
                    (account_id, block_height, block_hash, data_value)
                    VALUES(?, ?, ?, ?)",
            )
            .await?,
            delete_contract: Self::prepare_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.state_changes_contract
                    (account_id, block_height, block_hash, data_value)
                    VALUES(?, ?, ?, NULL)",
            )
            .await?,

            add_account: Self::prepare_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.state_changes_account
                    (account_id, block_height, block_hash, data_value)
                    VALUES(?, ?, ?, ?)",
            )
            .await?,
            delete_account: Self::prepare_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.state_changes_account
                    (account_id, block_height, block_hash, data_value)
                    VALUES(?, ?, ?, NULL)",
            )
            .await?,

            add_block: Self::prepare_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.blocks
                    (block_height, block_hash, chunks)
                    VALUES (?, ?, ?)",
            )
            .await?,
            add_account_state: Self::prepare_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.account_state
                    (account_id, data_key)
                    VALUES(?, ?)",
            )
            .await?,
            update_meta: Self::prepare_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.meta
                    (indexer_id, last_processed_block_height)
                    VALUES (?, ?)",
            )
            .await?,
        }))
    }
}

impl ScyllaDBManager {
    pub(crate) async fn scylla_session(&self) -> std::sync::Arc<scylla::Session> {
        self.scylla_session.clone()
    }

    pub(crate) async fn add_state_changes(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: bigdecimal::BigDecimal,
        block_hash: near_indexer_primitives::CryptoHash,
        key: &[u8],
        value: &[u8],
    ) -> anyhow::Result<()> {
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.add_state_changes,
            (
                account_id.to_string(),
                block_height,
                block_hash.to_string(),
                hex::encode(key).to_string(),
                value.to_vec(),
            ),
        )
        .await?;
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.add_account_state,
            (account_id.to_string(), hex::encode(key).to_string()),
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn delete_state_changes(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: bigdecimal::BigDecimal,
        block_hash: near_indexer_primitives::CryptoHash,
        key: &[u8],
    ) -> anyhow::Result<()> {
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.delete_state_changes,
            (account_id.to_string(), block_height, block_hash.to_string(), hex::encode(key).to_string()),
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn add_access_key(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: bigdecimal::BigDecimal,
        block_hash: near_indexer_primitives::CryptoHash,
        public_key: &[u8],
        access_key: &[u8],
    ) -> anyhow::Result<()> {
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.add_access_key,
            (
                account_id.to_string(),
                block_height,
                block_hash.to_string(),
                hex::encode(public_key).to_string(),
                access_key.to_vec(),
            ),
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn delete_access_key(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: bigdecimal::BigDecimal,
        block_hash: near_indexer_primitives::CryptoHash,
        public_key: &[u8],
    ) -> anyhow::Result<()> {
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.delete_access_key,
            (account_id.to_string(), block_height, block_hash.to_string(), hex::encode(public_key).to_string()),
        )
        .await?;
        Ok(())
    }
    async fn get_access_keys(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: bigdecimal::BigDecimal,
    ) -> anyhow::Result<scylla::frame::response::result::Row> {
        let result = Self::execute_prepared_query(
            &self.scylla_session,
            &self.get_account_access_keys,
            (account_id.to_string(), block_height),
        )
        .await?
        .single_row()?;
        Ok(result)
    }

    pub(crate) async fn add_account_access_keys(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: bigdecimal::BigDecimal,
        public_key: &[u8],
        access_key: Option<&[u8]>,
    ) -> anyhow::Result<()> {
        let public_key_hex = hex::encode(public_key).to_string();

        let mut account_keys = match self.get_access_keys(account_id.clone(), block_height.clone()).await {
            Ok(row) => match row.into_typed::<(HashMap<String, Vec<u8>>,)>() {
                Ok((account_keys,)) => account_keys,
                Err(_) => HashMap::new(),
            },
            Err(_) => HashMap::new(),
        };

        match access_key {
            Some(access_key) => {
                account_keys.insert(public_key_hex, access_key.to_vec());
            }
            None => {
                account_keys.remove(&public_key_hex);
            }
        }

        Self::execute_prepared_query(
            &self.scylla_session,
            &self.add_account_access_keys,
            (account_id.to_string(), block_height, &account_keys),
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn add_contract_code(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: bigdecimal::BigDecimal,
        block_hash: near_indexer_primitives::CryptoHash,
        code: &[u8],
    ) -> anyhow::Result<()> {
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.add_contract,
            (account_id.to_string(), block_height, block_hash.to_string(), code.to_vec()),
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn delete_contract_code(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: bigdecimal::BigDecimal,
        block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()> {
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.delete_contract,
            (account_id.to_string(), block_height, block_hash.to_string()),
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn add_account(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: bigdecimal::BigDecimal,
        block_hash: near_indexer_primitives::CryptoHash,
        account: Vec<u8>,
    ) -> anyhow::Result<()> {
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.add_account,
            (account_id.to_string(), block_height, block_hash.to_string(), account),
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn delete_account(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: bigdecimal::BigDecimal,
        block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()> {
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.delete_account,
            (account_id.to_string(), block_height, block_hash.to_string()),
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn add_block(
        &self,
        block_height: bigdecimal::BigDecimal,
        block_hash: near_indexer_primitives::CryptoHash,
        chunks: Vec<String>,
    ) -> anyhow::Result<scylla::QueryResult> {
        let query_result = Self::execute_prepared_query(
            &self.scylla_session,
            &self.add_block,
            (block_height, block_hash.to_string(), chunks),
        )
        .await?;
        Ok(query_result)
    }

    pub(crate) async fn update_meta(
        &self,
        indexer_id: &str,
        block_height: bigdecimal::BigDecimal,
    ) -> anyhow::Result<()> {
        Self::execute_prepared_query(&self.scylla_session, &self.update_meta, (indexer_id, block_height)).await?;
        Ok(())
    }
}
