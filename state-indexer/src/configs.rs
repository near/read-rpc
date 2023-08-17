pub use clap::{Parser, Subcommand};
use database::ScyllaStorageManager;
use near_jsonrpc_client::{methods, JsonRpcClient};
use near_lake_framework::near_indexer_primitives::types::{BlockReference, Finality};
use num_traits::ToPrimitive;
use scylla::prepared_statement::PreparedStatement;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub type ChunkHash = String;
pub type HeightIncluded = u64;
pub type ShardId = u64;

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
    /// ScyllaDB preferred DataCenter
    /// Accepts the DC name of the ScyllaDB to filter the connection to that DC only (preferrably).
    /// If you connect to multi-DC cluter, you might experience big latencies while working with the DB. This is due to the fact that ScyllaDB driver tries to connect to any of the nodes in the cluster disregarding of the location of the DC. This option allows to filter the connection to the DC you need. Example: "DC1" where DC1 is located in the same region as the application.
    #[clap(long, env)]
    pub scylla_preferred_dc: Option<String>,
    /// Metrics HTTP server port
    #[clap(long, default_value = "8080", env)]
    pub port: u16,
    #[clap(long, default_value = "1", env)]
    pub concurrency: usize,
    /// Chain ID: testnet or mainnet
    #[clap(subcommand)]
    pub chain_id: ChainId,
    /// Max retry count for ScyllaDB if `strict_mode` is `false`
    #[clap(long, default_value = "5", env)]
    pub max_retry: u8,
    /// Attempts to store data in the database should be infinite to ensure no data is missing.
    /// Disable it to perform a limited write attempts (`max_retry`)
    /// before skipping giving up and moving to the next piece of data
    #[clap(long, default_value = "true", env)]
    pub strict_mode: bool,
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
    FromBlock {
        height: u64,
    },
    FromInterruption {
        /// Fallback start block height if interruption block is not found
        height: Option<u64>,
    },
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
        match &self.chain_id {
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
        StartOptions::FromInterruption { height } => {
            let row = scylladb_session
                .query(
                    "SELECT last_processed_block_height FROM state_indexer.meta WHERE indexer_id = ?",
                    (&opts.indexer_id,),
                )
                .await?
                .single_row();

            if let Ok(row) = row {
                let (block_height,): (num_bigint::BigInt,) = row.into_typed::<(num_bigint::BigInt,)>()?;
                Ok(block_height.to_u64().expect("Failed to convert BigInt to u64"))
            } else {
                if let Some(height) = height {
                    return Ok(*height);
                }
                Ok(final_block_height(opts.rpc_url()).await?)
            }
        }
        StartOptions::FromLatest => Ok(final_block_height(opts.rpc_url()).await?),
    }
}

pub(crate) async fn final_block_height(rpc_url: &str) -> anyhow::Result<u64> {
    tracing::debug!(target: crate::INDEXER, "Fetching final block from NEAR RPC",);
    let client = JsonRpcClient::connect(rpc_url);
    let request = methods::block::RpcBlockRequest {
        block_reference: BlockReference::Finality(Finality::Final),
    };

    let latest_block = client.call(request).await?;

    Ok(latest_block.header.height)
}

pub(crate) fn init_tracing() -> anyhow::Result<()> {
    let mut env_filter = tracing_subscriber::EnvFilter::new("state_indexer=info");

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

    opentelemetry::global::shutdown_tracer_provider();

    opentelemetry::global::set_text_map_propagator(opentelemetry::sdk::propagation::TraceContextPropagator::new());

    #[cfg(feature = "tracing-instrumentation")]
    let subscriber = {
        let tracer = opentelemetry_jaeger::new_collector_pipeline()
            .with_service_name("state_indexer")
            .with_endpoint(std::env::var("OTEL_EXPORTER_JAEGER_ENDPOINT").unwrap_or_default())
            .with_isahc()
            .with_batch_processor_config(
                opentelemetry::sdk::trace::BatchConfig::default()
                    .with_max_queue_size(10_000)
                    .with_max_export_batch_size(10_000)
                    .with_max_concurrent_exports(100),
            )
            .install_batch(opentelemetry::runtime::TokioCurrentThread)?;
        let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

        tracing_subscriber::Registry::default().with(env_filter).with(telemetry)
    };

    #[cfg(not(feature = "tracing-instrumentation"))]
    let subscriber = tracing_subscriber::Registry::default().with(env_filter);

    if std::env::var("ENABLE_JSON_LOGS").is_ok() {
        subscriber.with(tracing_stackdriver::layer()).try_init()?;
    } else {
        subscriber
            .with(tracing_subscriber::fmt::Layer::default().compact())
            .try_init()?;
    }

    Ok(())
}

#[derive(Debug)]
pub(crate) struct ScyllaDBManager {
    scylla_session: std::sync::Arc<scylla::Session>,

    add_state_changes: PreparedStatement,
    delete_state_changes: PreparedStatement,

    add_access_key: PreparedStatement,
    delete_access_key: PreparedStatement,

    #[cfg(feature = "account_access_keys")]
    add_account_access_keys: PreparedStatement,
    #[cfg(feature = "account_access_keys")]
    get_account_access_keys: PreparedStatement,

    add_contract: PreparedStatement,
    delete_contract: PreparedStatement,

    add_account: PreparedStatement,
    delete_account: PreparedStatement,

    add_block: PreparedStatement,
    add_chunk: PreparedStatement,
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

        #[cfg(feature = "account_access_keys")]
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
                    block_hash varchar,
                    block_height varint,
                    PRIMARY KEY (block_hash)
                )
                ",
                &[],
            )
            .await?;

        scylla_db_session
            .query(
                "
                CREATE TABLE IF NOT EXISTS chunks (
                    chunk_hash varchar,
                    block_height varint,
                    shard_id varint,
                    stored_at_block_height varint,
                    PRIMARY KEY (chunk_hash, block_height)
                )
            ",
                &[],
            )
            .await?;
        scylla_db_session
            .query(
                "
                CREATE INDEX IF NOT EXISTS chunk_block_height ON chunks (block_height);
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
            add_state_changes: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.state_changes_data
                    (account_id, block_height, block_hash, data_key, data_value)
                    VALUES(?, ?, ?, ?, ?)",
            )
            .await?,
            delete_state_changes: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.state_changes_data
                    (account_id, block_height, block_hash, data_key, data_value)
                    VALUES(?, ?, ?, ?, NULL)",
            )
            .await?,

            add_access_key: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.state_changes_access_key
                    (account_id, block_height, block_hash, data_key, data_value)
                    VALUES(?, ?, ?, ?, ?)",
            )
            .await?,
            delete_access_key: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.state_changes_access_key
                    (account_id, block_height, block_hash, data_key, data_value)
                    VALUES(?, ?, ?, ?, NULL)",
            )
            .await?,

            #[cfg(feature = "account_access_keys")]
            add_account_access_keys: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.account_access_keys
                    (account_id, block_height, active_access_keys)
                    VALUES(?, ?, ?)",
            )
            .await?,

            #[cfg(feature = "account_access_keys")]
            get_account_access_keys: Self::prepare_write_query(
                &scylla_db_session,
                "SELECT active_access_keys FROM state_indexer.account_access_keys
                    WHERE account_id = ? AND block_height < ? LIMIT 1",
            )
            .await?,

            add_contract: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.state_changes_contract
                    (account_id, block_height, block_hash, data_value)
                    VALUES(?, ?, ?, ?)",
            )
            .await?,
            delete_contract: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.state_changes_contract
                    (account_id, block_height, block_hash, data_value)
                    VALUES(?, ?, ?, NULL)",
            )
            .await?,

            add_account: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.state_changes_account
                    (account_id, block_height, block_hash, data_value)
                    VALUES(?, ?, ?, ?)",
            )
            .await?,
            delete_account: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.state_changes_account
                    (account_id, block_height, block_hash, data_value)
                    VALUES(?, ?, ?, NULL)",
            )
            .await?,
            add_block: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.blocks
                    (block_hash, block_height)
                    VALUES (?, ?)",
            )
            .await?,
            add_chunk: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.chunks
                    (chunk_hash, block_height, shard_id, stored_at_block_height)
                    VALUES (?, ?, ?, ?)",
            )
            .await?,
            add_account_state: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO state_indexer.account_state
                    (account_id, data_key)
                    VALUES(?, ?)",
            )
            .await?,
            update_meta: Self::prepare_write_query(
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

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(self, key, value)))]
    pub(crate) async fn add_state_changes(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: num_bigint::BigInt,
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

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(self, key)))]
    pub(crate) async fn delete_state_changes(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: num_bigint::BigInt,
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

    #[cfg_attr(
        feature = "tracing-instrumentation",
        tracing::instrument(skip(self, public_key, access_key))
    )]
    pub(crate) async fn add_access_key(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: num_bigint::BigInt,
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

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(self, public_key)))]
    pub(crate) async fn delete_access_key(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: num_bigint::BigInt,
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

    #[cfg(feature = "account_access_keys")]
    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(self)))]
    async fn get_access_keys(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: num_bigint::BigInt,
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

    #[cfg(feature = "account_access_keys")]
    #[cfg_attr(
        feature = "tracing-instrumentation",
        tracing::instrument(skip(self, public_key, access_key))
    )]
    pub(crate) async fn add_account_access_keys(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: num_bigint::BigInt,
        public_key: &[u8],
        access_key: Option<&[u8]>,
    ) -> anyhow::Result<()> {
        let public_key_hex = hex::encode(public_key).to_string();

        let mut account_keys = match self.get_access_keys(account_id.clone(), block_height.clone()).await {
            Ok(row) => match row.into_typed::<(std::collections::HashMap<String, Vec<u8>>,)>() {
                Ok((account_keys,)) => account_keys,
                Err(_) => std::collections::HashMap::new(),
            },
            Err(_) => std::collections::HashMap::new(),
        };

        match access_key {
            Some(access_key) => {
                account_keys.insert(public_key_hex, access_key.to_vec());
            }
            None => {
                account_keys.remove(&public_key_hex);
            }
        }
        self.update_account_access_keys(account_id.to_string(), block_height, account_keys)
            .await?;
        Ok(())
    }

    #[cfg(feature = "account_access_keys")]
    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(self, account_keys)))]
    async fn update_account_access_keys(
        &self,
        account_id: String,
        block_height: num_bigint::BigInt,
        account_keys: std::collections::HashMap<String, Vec<u8>>,
    ) -> anyhow::Result<()> {
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.add_account_access_keys,
            (account_id.to_string(), block_height, &account_keys),
        )
        .await?;
        Ok(())
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(self, code)))]
    pub(crate) async fn add_contract_code(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: num_bigint::BigInt,
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

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(self)))]
    pub(crate) async fn delete_contract_code(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: num_bigint::BigInt,
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

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(self, account)))]
    pub(crate) async fn add_account(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: num_bigint::BigInt,
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

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(self)))]
    pub(crate) async fn delete_account(
        &self,
        account_id: near_indexer_primitives::types::AccountId,
        block_height: num_bigint::BigInt,
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

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(self)))]
    pub(crate) async fn add_block(
        &self,
        block_height: num_bigint::BigInt,
        block_hash: near_indexer_primitives::CryptoHash,
    ) -> anyhow::Result<()> {
        Self::execute_prepared_query(&self.scylla_session, &self.add_block, (block_hash.to_string(), block_height))
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(self)))]
    pub(crate) async fn add_chunks(
        &self,
        block_height: num_bigint::BigInt,
        chunks: Vec<(String, ShardId, HeightIncluded)>,
    ) -> anyhow::Result<()> {
        let save_chunks_futures = chunks.iter().map(|(chunk_hash, shard_id, height_included)| {
            Self::execute_prepared_query(
                &self.scylla_session,
                &self.add_chunk,
                (
                    chunk_hash,
                    block_height.clone(),
                    num_bigint::BigInt::from(*shard_id),
                    num_bigint::BigInt::from(*height_included),
                ),
            )
        });
        futures::future::try_join_all(save_chunks_futures).await?;
        Ok(())
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(self, indexer_id)))]
    pub(crate) async fn update_meta(&self, indexer_id: &str, block_height: num_bigint::BigInt) -> anyhow::Result<()> {
        Self::execute_prepared_query(&self.scylla_session, &self.update_meta, (indexer_id, block_height)).await?;
        Ok(())
    }
}
