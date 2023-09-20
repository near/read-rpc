use borsh::{BorshDeserialize, BorshSerialize};
pub use clap::{Parser, Subcommand};
use database::ScyllaStorageManager;
use futures::StreamExt;
use near_indexer_primitives::near_primitives;
use near_indexer_primitives::types::{BlockReference, Finality};
use near_jsonrpc_client::{methods, JsonRpcClient};
use num_traits::ToPrimitive;
use scylla::prepared_statement::PreparedStatement;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

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
    /// ScyllaDB preferred DataCenter
    /// Accepts the DC name of the ScyllaDB to filter the connection to that DC only (preferrably).
    /// If you connect to multi-DC cluter, you might experience big latencies while working with the DB. This is due to the fact that ScyllaDB driver tries to connect to any of the nodes in the cluster disregarding of the location of the DC. This option allows to filter the connection to the DC you need. Example: "DC1" where DC1 is located in the same region as the application.
    #[clap(long, env)]
    pub scylla_preferred_dc: Option<String>,
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
        /// Fallback start block height if interruption is not found
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
        StartOptions::FromInterruption { height } => {
            let row = scylladb_session
                .query(
                    "SELECT last_processed_block_height FROM tx_indexer.meta WHERE indexer_id = ?",
                    (&opts.indexer_id,),
                )
                .await?
                .single_row();

            if let Ok(row) = row {
                let (block_height,): (num_bigint::BigInt,) =
                    row.into_typed::<(num_bigint::BigInt,)>()?;
                Ok(block_height
                    .to_u64()
                    .expect("Failed to convert BigInt to u64"))
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

pub async fn final_block_height(rpc_url: &str) -> anyhow::Result<u64> {
    let client = JsonRpcClient::connect(rpc_url.to_string());
    let request = methods::block::RpcBlockRequest {
        block_reference: BlockReference::Finality(Finality::Final),
    };

    let latest_block = client.call(request).await?;

    Ok(latest_block.header.height)
}

pub fn init_tracing() -> anyhow::Result<()> {
    let mut env_filter = tracing_subscriber::EnvFilter::new("tx_indexer=info");

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

    opentelemetry::global::set_text_map_propagator(
        opentelemetry::sdk::propagation::TraceContextPropagator::new(),
    );

    #[cfg(feature = "tracing-instrumentation")]
    let subscriber = {
        let tracer = opentelemetry_jaeger::new_collector_pipeline()
            .with_service_name("tx_indexer")
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

        tracing_subscriber::Registry::default()
            .with(env_filter)
            .with(telemetry)
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

pub(crate) struct ScyllaDBManager {
    scylla_session: std::sync::Arc<scylla::Session>,
    add_transaction: PreparedStatement,
    add_receipt: PreparedStatement,
    update_meta: PreparedStatement,

    add_transaction_process: PreparedStatement,
    delete_transaction_process: PreparedStatement,
    add_receipt_process: PreparedStatement,
    delete_receipts_process: PreparedStatement,
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
                PRIMARY KEY (transaction_hash, block_height)
            ) WITH CLUSTERING ORDER BY (block_height DESC)
            ",
                &[],
            )
            .await?;

        scylla_db_session
            .query(
                "CREATE TABLE IF NOT EXISTS receipts_map (
                receipt_id varchar,
                block_height varint,
                parent_transaction_hash varchar,
                shard_id varint,
                PRIMARY KEY (receipt_id)
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
            .use_keyspace("tx_indexer_process", false)
            .await?;
        scylla_db_session
            .query(
                "CREATE TABLE IF NOT EXISTS transactions (
                transaction_hash varchar,
                transaction_details BLOB,
                PRIMARY KEY (transaction_hash)
            )
            ",
                &[],
            )
            .await?;

        scylla_db_session
            .query(
                "CREATE TABLE IF NOT EXISTS receipts_outcomes (
                transaction_hash varchar,
                receipt_id varchar,
                receipt BLOB,
                outcome BLOB,
                PRIMARY KEY (transaction_hash, receipt_id)
            )
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
        scylla_db_session.query(
            "CREATE KEYSPACE IF NOT EXISTS tx_indexer_process WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}",
            &[]
        ).await?;
        Ok(())
    }

    async fn prepare(
        scylla_db_session: std::sync::Arc<scylla::Session>,
    ) -> anyhow::Result<Box<Self>> {
        Ok(Box::new(Self {
            scylla_session: scylla_db_session.clone(),
            add_transaction: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO tx_indexer.transactions_details
                    (transaction_hash, block_height, account_id, transaction_details)
                    VALUES(?, ?, ?, ?)",
            )
            .await?,
            add_receipt: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO tx_indexer.receipts_map
                    (receipt_id, block_height, parent_transaction_hash, shard_id)
                    VALUES(?, ?, ?, ?)",
            )
            .await?,
            update_meta: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO tx_indexer.meta
                    (indexer_id, last_processed_block_height)
                    VALUES (?, ?)",
            )
            .await?,

            add_transaction_process: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO tx_indexer_process.transactions
                    (transaction_hash, transaction_details)
                    VALUES(?, ?)",
            )
            .await?,
            delete_transaction_process: Self::prepare_write_query(
                &scylla_db_session,
                "DELETE FROM tx_indexer_process.transactions WHERE transaction_hash = ?",
            )
            .await?,
            add_receipt_process: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO tx_indexer_process.receipts_outcomes
                    (transaction_hash, receipt_id, receipt, outcome)
                    VALUES(?, ?, ?, ?)",
            )
            .await?,
            delete_receipts_process: Self::prepare_write_query(
                &scylla_db_session,
                "DELETE FROM tx_indexer_process.receipts_outcomes WHERE transaction_hash = ?",
            )
            .await?,
        }))
    }
}

impl ScyllaDBManager {
    pub(crate) async fn scylla_session(&self) -> std::sync::Arc<scylla::Session> {
        self.scylla_session.clone()
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
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

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    pub async fn add_receipt(
        &self,
        receipt_id: &str,
        parent_tx_hash: &str,
        block_height: u64,
        shard_id: u64,
    ) -> anyhow::Result<()> {
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.add_receipt,
            (
                receipt_id,
                num_bigint::BigInt::from(block_height),
                parent_tx_hash,
                num_bigint::BigInt::from(shard_id),
            ),
        )
        .await?;
        Ok(())
    }

    #[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
    pub(crate) async fn update_meta(
        &self,
        indexer_id: &str,
        block_height: u64,
    ) -> anyhow::Result<()> {
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.update_meta,
            (indexer_id, num_bigint::BigInt::from(block_height)),
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn add_transaction_process(
        &self,
        transaction_details: readnode_primitives::CollectingTransactionDetails,
    ) -> anyhow::Result<()> {
        let transaction_hash = transaction_details.transaction.hash.clone().to_string();
        let transaction_details = transaction_details.try_to_vec()?;
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.add_transaction_process,
            (transaction_hash, transaction_details),
        )
        .await?;
        Ok(())
    }
    pub(crate) async fn add_receipt_process(
        &self,
        transaction_hash: &str,
        indexer_execution_outcome_with_receipt: near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
    ) -> anyhow::Result<()> {
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.add_receipt_process,
            (
                transaction_hash,
                indexer_execution_outcome_with_receipt
                    .receipt
                    .receipt_id
                    .to_string(),
                indexer_execution_outcome_with_receipt
                    .receipt
                    .try_to_vec()?,
                indexer_execution_outcome_with_receipt
                    .execution_outcome
                    .try_to_vec()?,
            ),
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn get_transactions_in_process(
        &self,
    ) -> anyhow::Result<
        std::collections::HashMap<String, readnode_primitives::CollectingTransactionDetails>,
    > {
        let mut result = std::collections::HashMap::new();
        let mut rows_stream = self
            .scylla_session
            .query_iter(
                "SELECT transaction_hash, transaction_details FROM tx_indexer_process.transactions",
                &[],
            )
            .await?
            .into_typed::<(String, Vec<u8>)>();
        while let Some(next_row_res) = rows_stream.next().await {
            let (transaction_hash, transaction_details) = next_row_res?;
            result.insert(
                transaction_hash.clone(),
                readnode_primitives::CollectingTransactionDetails::try_from_slice(
                    &transaction_details,
                )?,
            );
        }
        Ok(result)
    }

    pub(crate) async fn get_receipts_in_process(
        &self,
        transaction_hash: &str,
    ) -> anyhow::Result<Vec<near_indexer_primitives::IndexerExecutionOutcomeWithReceipt>> {
        let mut result = vec![];
        let mut rows_stream = self
            .scylla_session
            .query_iter(
                "SELECT receipt, outcome FROM tx_indexer_process.receipts_outcomes WHERE transaction_hash = ?",
                (transaction_hash,),
            )
            .await?
            .into_typed::<(Vec<u8>, Vec<u8>)>();
        while let Some(next_row_res) = rows_stream.next().await {
            let (receipt, outcome) = next_row_res?;
            let receipt = near_primitives::views::ReceiptView::try_from_slice(&receipt)?;
            let execution_outcome =
                near_primitives::views::ExecutionOutcomeWithIdView::try_from_slice(&outcome)?;
            result.push(
                near_indexer_primitives::IndexerExecutionOutcomeWithReceipt {
                    receipt,
                    execution_outcome,
                },
            );
        }
        Ok(result)
    }

    pub(crate) async fn delete_transaction_process(
        &self,
        transaction_hash: &str,
    ) -> anyhow::Result<()> {
        let delete_transaction_feature = Self::execute_prepared_query(
            &self.scylla_session,
            &self.delete_transaction_process,
            (transaction_hash,),
        );
        let delete_receipts_feature = Self::execute_prepared_query(
            &self.scylla_session,
            &self.delete_receipts_process,
            (transaction_hash,),
        );
        futures::try_join!(delete_transaction_feature, delete_receipts_feature)?;
        Ok(())
    }
}
