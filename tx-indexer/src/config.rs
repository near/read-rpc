use crate::storage;
use borsh::BorshSerialize;
pub use clap::{Parser, Subcommand};
use database::ScyllaStorageManager;
use near_indexer_primitives::types::{BlockReference, Finality};
use near_jsonrpc_client::{methods, JsonRpcClient};
use scylla::prepared_statement::PreparedStatement;
use tracing_subscriber::EnvFilter;

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
    /// Connection string to connect to the Redis instance for cache. Default: "redis://127.0.0.1"
    #[clap(long, default_value = "redis://127.0.0.1", env)]
    pub redis_connection_string: String,
    /// Indexer ID to handle meta data about the instance
    #[clap(long, env)]
    pub indexer_id: String,
    /// Port for metrics server
    #[clap(long, default_value = "8080", env)]
    pub port: u16,
    /// ScyllaDB connection string. Default: "127.0.0.1:9042"
    #[clap(long, default_value = "127.0.0.1:9042", env)]
    pub scylla_url: String,
    /// ScyllaDB keyspace
    #[clap(long, default_value = "tx_indexer", env)]
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
        let config_builder = near_lake_framework::LakeConfigBuilder::default();

        Ok(match &self.chain_id {
            ChainId::Mainnet(_) => config_builder
                .mainnet()
                .start_block_height(get_start_block_height(self).await),
            ChainId::Testnet(_) => config_builder
                .testnet()
                .start_block_height(get_start_block_height(self).await),
        }
        .build()
        .expect("Failed to build LakeConfig"))
    }
}

async fn get_start_block_height(opts: &Opts) -> u64 {
    match opts.start_options() {
        StartOptions::FromBlock { height } => *height,
        StartOptions::FromInterruption => {
            let redis_connection_manager = match storage::connect(&opts.redis_connection_string)
                .await
            {
                Ok(connection_manager) => connection_manager,
                Err(err) => {
                    tracing::warn!(
                        target: "tx_indexer",
                        "Failed to connect to Redis to get last synced block, failing to the latest...\n{:#?}",
                        err,
                    );
                    return final_block_height(opts).await;
                }
            };
            match storage::get_last_indexed_block(&redis_connection_manager).await {
                Ok(last_indexed_block) => last_indexed_block,
                Err(err) => {
                    tracing::warn!(
                        target: "tx_indexer",
                        "Failed to get last indexer block from Redis. Failing to the latest one...\n{:#?}",
                        err
                    );
                    final_block_height(opts).await
                }
            }
        }
        StartOptions::FromLatest => final_block_height(opts).await,
    }
}

async fn final_block_height(opts: &Opts) -> u64 {
    let client = JsonRpcClient::connect(opts.rpc_url().to_string());
    let request = methods::block::RpcBlockRequest {
        block_reference: BlockReference::Finality(Finality::Final),
    };

    let latest_block = client.call(request).await.unwrap();

    latest_block.header.height
}

pub fn init_tracing() {
    let mut env_filter = EnvFilter::new("near_lake_framework=info,tx_indexer=info");

    if let Ok(rust_log) = std::env::var("RUST_LOG") {
        if !rust_log.is_empty() {
            for directive in rust_log.split(',').filter_map(|s| match s.parse() {
                Ok(directive) => Some(directive),
                Err(err) => {
                    eprintln!("Ignoring directive `{}`: {}", s, err);
                    None
                }
            }) {
                env_filter = env_filter.add_directive(directive);
            }
        }
    }

    tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .init();
}

pub(crate) struct ScyllaDBManager {
    scylla_session: std::sync::Arc<scylla::Session>,
    add_transaction: PreparedStatement,
}

#[async_trait::async_trait]
impl ScyllaStorageManager for ScyllaDBManager {
    async fn create_tables(scylla_db_session: &scylla::Session) -> anyhow::Result<()> {
        scylla_db_session.use_keyspace("tx_indexer", false).await?;
        scylla_db_session
            .query(
                "CREATE TABLE IF NOT EXISTS transactions_details (
                transaction_hash varchar,
                block_height varint,
                account_id varchar,
                transaction_details BLOB,
                PRIMARY KEY ((transaction_hash, account_id), block_height)
            ) WITH CLUSTERING ORDER BY (block_height DESC)
            ",
                &[],
            )
            .await?;
        Ok(())
    }

    async fn create_keyspace(scylla_db_session: &scylla::Session) -> anyhow::Result<()> {
        scylla_db_session.query(
            "CREATE KEYSPACE IF NOT EXISTS tx_indexer WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}",
            &[]
        ).await?;
        Ok(())
    }

    async fn prepare(
        scylla_db_session: std::sync::Arc<scylla::Session>,
    ) -> anyhow::Result<Box<Self>> {
        Ok(Box::new(Self {
            scylla_session: scylla_db_session.clone(),
            add_transaction: Self::prepare_query(
                &scylla_db_session,
                "INSERT INTO tx_indexer.transactions_details
                    (transaction_hash, block_height, account_id, transaction_details)
                    VALUES(?, ?, ?, ?)",
            )
            .await?,
        }))
    }
}

impl ScyllaDBManager {
    pub async fn add_transaction(
        &self,
        transaction: readnode_primitives::TransactionDetails,
        block_height: u64,
    ) -> anyhow::Result<()> {
        let transaction_details = transaction
            .try_to_vec()
            .expect("Failed to borsh-serialize the Transaction");
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.add_transaction,
            (
                transaction.transaction.hash.to_string(),
                num_bigint::BigInt::from(block_height),
                transaction.transaction.signer_id.to_string(),
                &transaction_details,
            ),
        )
        .await?;
        Ok(())
    }
}
