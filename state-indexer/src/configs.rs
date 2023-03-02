use bigdecimal::ToPrimitive;
pub use clap::{Parser, Subcommand};

use scylla::{Session, SessionBuilder};

use near_jsonrpc_client::{methods, JsonRpcClient};
use near_lake_framework::near_indexer_primitives::types::{BlockReference, Finality};

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
    /// ScyllaDB keyspace
    #[clap(long, default_value = "state_indexer", env)]
    pub scylla_keyspace: String,
    /// ScyllaDB user(login)
    #[clap(long, env)]
    pub scylla_user: Option<String>,
    /// ScyllaDB password
    #[clap(long, env)]
    pub scylla_password: Option<String>,
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
    pub async fn to_lake_config(&self) -> anyhow::Result<near_lake_framework::LakeConfig> {
        migrate(&self.scylla_url, &self.scylla_keyspace, self.scylla_user.as_deref(), self.scylla_password.as_deref())
            .await?;

        let config_builder = near_lake_framework::LakeConfigBuilder::default();

        Ok(match &self.chain_id {
            ChainId::Mainnet(_) => config_builder
                .mainnet()
                .start_block_height(get_start_block_height(self).await?),
            ChainId::Testnet(_) => config_builder
                .testnet()
                .start_block_height(get_start_block_height(self).await?),
        }
        .build()
        .expect("Failed to build LakeConfig"))
    }
}

async fn get_start_block_height(opts: &Opts) -> anyhow::Result<u64> {
    match opts.start_options() {
        StartOptions::FromBlock { height } => Ok(*height),
        StartOptions::FromInterruption => {
            let scylladb_session = get_scylladb_session(
                &opts.scylla_url,
                &opts.scylla_keyspace,
                opts.scylla_user.as_deref(),
                opts.scylla_password.as_deref(),
            )
            .await?;

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

// Database schema
// Main table to keep all the state changes happening in NEAR Protocol
// CREATE TABLE state_changes (
//     account_id varchar,
//     block_height varint,
//     block_hash varchar,
//     change_scope varchar,
//     data_key BLOB,
//     data_value BLOB,
//     PRIMARY KEY ((account_id, change_scope), block_height) -- prim key like this because we mostly are going to query by these 3 fields
// );
//
// Map-table to store relation between block_hash-block_height and included chunk hashes
// CREATE TABLE IF NOT EXISTS blocks (
//     block_hash varchar,
//     bloch_height varint,
//     chunks set<varchar>,
//     PRIMARY KEY (block_height)
// );
//
// Meta table to provide start from interruption
// CREATE TABLE IF NOT EXISTS meta (
//     indexer_id varchar PRIMARY KEY,
//     last_processed_block_height varint
// )
pub(crate) async fn migrate(
    scylla_url: &str,
    scylla_keyspace: &str,
    scylla_user: Option<&str>,
    scylla_password: Option<&str>,
) -> anyhow::Result<()> {
    let mut scylladb_session_builder = SessionBuilder::new().known_node(scylla_url);
    if let Some(user) = scylla_user {
        if let Some(password) = scylla_password {
            scylladb_session_builder = scylladb_session_builder.user(user, password);
        }
    }
    let scylladb_session = scylladb_session_builder.build().await?;

    let mut str_query = format!("CREATE KEYSPACE IF NOT EXISTS {scylla_keyspace} ");
    str_query.push_str("WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};");

    scylladb_session.query(str_query, &[]).await?;

    scylladb_session.use_keyspace(scylla_keyspace, false).await?;

    scylladb_session
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

    scylladb_session
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

    scylladb_session
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

    scylladb_session
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

    scylladb_session
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

    scylladb_session
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

    scylladb_session
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

pub(crate) async fn get_scylladb_session(
    scylla_url: &str,
    scylla_keyspace: &str,
    scylla_user: Option<&str>,
    scylla_password: Option<&str>,
) -> anyhow::Result<Session> {
    tracing::debug!(target: crate::INDEXER, "Connecting to ScyllaDB");
    let mut session: SessionBuilder = SessionBuilder::new().known_node(scylla_url);

    if let Some(user) = scylla_user {
        tracing::debug!(target: crate::INDEXER, "Got ScyllaDB credentials, authenticating...");
        if let Some(password) = scylla_password {
            session = session.user(user, password);
        }
    }

    session = session.use_keyspace(scylla_keyspace, false);

    Ok(session.build().await?)
}
