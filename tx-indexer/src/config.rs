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
    /// To restore cache from scylla db we use smart range blocks
    /// Regular transaction takes some blocks to be finalized
    /// We don't need to restore too old transactions for the indexer because we will probably never be able to reassemble them.
    /// We use a range of 1000 blocks for our peace of mind. We also leave the option to increase or decrease this range
    #[clap(long, default_value = "1000", env)]
    pub cache_restore_blocks_range: u64,

    /// Parallel queries = (nodes in cluster) ✕ (cores in node) ✕ 3
    /// Current we have 6 - nodes with 8 - cpus
    /// 6 ✕ 8 ✕ 3 = 144
    #[clap(long, env, default_value = "144")]
    pub scylla_parallel_queries: i64,
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
        start_block_height: u64,
    ) -> anyhow::Result<near_lake_framework::LakeConfig> {
        let config_builder = near_lake_framework::LakeConfigBuilder::default();

        Ok(match &self.chain_id {
            ChainId::Mainnet(_) => config_builder
                .mainnet()
                .start_block_height(start_block_height),
            ChainId::Testnet(_) => config_builder
                .testnet()
                .start_block_height(start_block_height),
        }
        .build()
        .expect("Failed to build LakeConfig"))
    }
}

pub(crate) async fn get_start_block_height(
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

    cache_get_all_transactions: PreparedStatement,
    cache_get_transaction: PreparedStatement,
    cache_get_transaction_by_receipt_id: PreparedStatement,
    cache_get_receipts: PreparedStatement,
    cache_add_transaction: PreparedStatement,
    cache_delete_transaction: PreparedStatement,
    cache_add_receipt: PreparedStatement,
    cache_delete_receipts: PreparedStatement,
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
            .use_keyspace("tx_indexer_cache", false)
            .await?;
        scylla_db_session
            .query(
                "CREATE TABLE IF NOT EXISTS transactions (
                block_height varint,
                transaction_hash varchar,
                transaction_details BLOB,
                PRIMARY KEY (block_height, transaction_hash)
            )
            ",
                &[],
            )
            .await?;

        scylla_db_session
            .query(
                "CREATE TABLE IF NOT EXISTS receipts_outcomes (
                block_height varint,
                transaction_hash varchar,
                receipt_id varchar,
                receipt BLOB,
                outcome BLOB,
                PRIMARY KEY (block_height, transaction_hash, receipt_id)
            )
            ",
                &[],
            )
            .await?;
        scylla_db_session
            .query(
                "
                CREATE INDEX IF NOT EXISTS transaction_key_receipt_id ON receipts_outcomes (receipt_id);
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
            "CREATE KEYSPACE IF NOT EXISTS tx_indexer_cache WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}",
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

            cache_get_all_transactions: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT transaction_details FROM tx_indexer_cache.transactions WHERE token(block_height) >= ? AND token(block_height) <= ?"
            ).await?,

            cache_get_transaction: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT transaction_details FROM tx_indexer_cache.transactions WHERE block_height = ? AND transaction_hash = ?"
            ).await?,

            cache_get_transaction_by_receipt_id: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT block_height, transaction_hash FROM tx_indexer_cache.receipts_outcomes WHERE receipt_id = ? LIMIT 1"
            ).await?,
            cache_get_receipts: Self::prepare_read_query(
                &scylla_db_session,
                "SELECT receipt, outcome FROM tx_indexer_cache.receipts_outcomes WHERE block_height = ? AND transaction_hash = ?"
            ).await?,

            cache_add_transaction: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO tx_indexer_cache.transactions
                    (block_height, transaction_hash, transaction_details)
                    VALUES(?, ?, ?)",
            )
            .await?,
            cache_delete_transaction: Self::prepare_write_query(
                &scylla_db_session,
                "DELETE FROM tx_indexer_cache.transactions WHERE block_height = ? AND transaction_hash = ?",
            )
            .await?,
            cache_add_receipt: Self::prepare_write_query(
                &scylla_db_session,
                "INSERT INTO tx_indexer_cache.receipts_outcomes
                    (block_height, transaction_hash, receipt_id, receipt, outcome)
                    VALUES(?, ?, ?, ?, ?)",
            )
            .await?,
            cache_delete_receipts: Self::prepare_write_query(
                &scylla_db_session,
                "DELETE FROM tx_indexer_cache.receipts_outcomes WHERE block_height = ? AND transaction_hash = ?",
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

    pub(crate) async fn cache_add_transaction(
        &self,
        transaction_details: readnode_primitives::CollectingTransactionDetails,
    ) -> anyhow::Result<()> {
        let transaction_hash = transaction_details.transaction.hash.clone().to_string();
        let block_height = transaction_details.block_height;
        let transaction_details = transaction_details.try_to_vec().map_err(|err| {
            tracing::error!(target: "tx_indexer", "Failed to serialize transaction details: {:?}", err);
            err})?;
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.cache_add_transaction,
            (
                num_bigint::BigInt::from(block_height),
                transaction_hash,
                transaction_details,
            ),
        )
        .await
        .map_err(|err| {
            tracing::error!(target: "tx_indexer", "Failed to cache_add_transaction: {:?}", err);
            err
        })?;
        Ok(())
    }
    pub(crate) async fn cache_add_receipt(
        &self,
        transaction_key: readnode_primitives::TransactionKey,
        indexer_execution_outcome_with_receipt: near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
    ) -> anyhow::Result<()> {
        Self::execute_prepared_query(
            &self.scylla_session,
            &self.cache_add_receipt,
            (
                num_bigint::BigInt::from(transaction_key.block_height),
                transaction_key.transaction_hash,
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

    async fn task_get_transactions_by_token_range(
        session: std::sync::Arc<scylla::Session>,
        prepared: PreparedStatement,
        token_val_range_start: i64,
        token_val_range_end: i64,
    ) -> anyhow::Result<Vec<readnode_primitives::CollectingTransactionDetails>> {
        let mut result = vec![];
        let mut rows_stream = session
            .execute_iter(prepared, (token_val_range_start, token_val_range_end))
            .await?
            .into_typed::<(Vec<u8>,)>();
        while let Some(next_row_res) = rows_stream.next().await {
            let (transaction_details,) = next_row_res?;
            let transaction_details =
                readnode_primitives::CollectingTransactionDetails::try_from_slice(
                    &transaction_details,
                )?;
            result.push(transaction_details);
        }
        Ok(result)
    }

    pub(crate) async fn get_transactions_in_cache(
        &self,
        start_block_height: u64,
        cache_restore_blocks_range: u64,
        scylla_parallel_queries: i64,
    ) -> anyhow::Result<
        std::collections::HashMap<
            readnode_primitives::TransactionKey,
            readnode_primitives::CollectingTransactionDetails,
        >,
    > {
        // N = Parallel queries = (nodes in cluster) ✕ (cores in node) ✕ 3. 6 - nodes, 8 - cpus
        //
        // M = N * 100
        //
        // We will process M sub-ranges, but only N in parallel;
        // the rest will wait. As a sub-range query completes,
        // we will pick a new sub-range and start processing it,
        // until we have completed all M.
        let sub_ranges = scylla_parallel_queries * 100;
        let step = i64::MAX / (sub_ranges / 2);

        let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(
            scylla_parallel_queries as usize,
        ));
        let mut handlers = vec![];
        for token_val_range_start in (i64::MIN..=i64::MAX).step_by(step as usize) {
            let session = self.scylla_session.clone();
            let prepared = self.cache_get_all_transactions.clone();
            let permit = sem.clone().acquire_owned().await;
            tracing::debug!(
                target: crate::storage::STORAGE,
                "Creating Task to get transactions..."
            );
            handlers.push(tokio::task::spawn(async move {
                let result = Self::task_get_transactions_by_token_range(
                    session,
                    prepared,
                    token_val_range_start,
                    token_val_range_start + step - 1,
                )
                .await;
                let _permit = permit;
                result
            }));
        }

        tracing::info!(
            target: crate::storage::STORAGE,
            "Waiting tasks to complete...",
        );

        let mut results = std::collections::HashMap::new();
        for thread in handlers {
            for transaction in thread.await?? {
                let transaction_key = transaction.transaction_key();

                // Collect transactions that the indexer could potentially collect.
                // For this, we use the range of blocks from the beginning of the index to minus 1000 blocks.
                // This range should include all transactions that the indexer can collect.
                if transaction_key.block_height <= start_block_height
                    && transaction_key.block_height
                        >= start_block_height - cache_restore_blocks_range
                {
                    results.insert(transaction_key.clone(), transaction);
                    tracing::info!(
                        target: crate::storage::STORAGE,
                        "Transaction uploaded from db {} - {}",
                        transaction_key.transaction_hash,
                        transaction_key.block_height
                    );
                };
            }
        }
        Ok(results)
    }
    pub async fn get_transaction_by_receipt_id(
        &self,
        receipt_id: &str,
    ) -> anyhow::Result<readnode_primitives::CollectingTransactionDetails> {
        let (transaction_hash, block_height) = Self::execute_prepared_query(
            &self.scylla_session,
            &self.cache_get_transaction_by_receipt_id,
            (receipt_id,),
        )
        .await?
        .single_row()?
        .into_typed::<(String, num_bigint::BigInt)>()?;

        let (transaction_details,) = Self::execute_prepared_query(
            &self.scylla_session,
            &self.cache_get_transaction,
            (block_height, transaction_hash),
        )
        .await?
        .single_row()?
        .into_typed::<(Vec<u8>,)>()?;

        Ok(
            readnode_primitives::CollectingTransactionDetails::try_from_slice(
                &transaction_details,
            )?,
        )
    }

    pub(crate) async fn get_receipts_in_cache(
        &self,
        transaction_key: &readnode_primitives::TransactionKey,
    ) -> anyhow::Result<Vec<near_indexer_primitives::IndexerExecutionOutcomeWithReceipt>> {
        let mut result = vec![];
        let mut rows_stream = self
            .scylla_session
            .execute_iter(
                self.cache_get_receipts.clone(),
                (
                    num_bigint::BigInt::from(transaction_key.block_height),
                    transaction_key.transaction_hash.clone(),
                ),
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

    pub(crate) async fn cache_delete_transaction(
        &self,
        transaction_hash: &str,
        block_height: u64,
    ) -> anyhow::Result<()> {
        let delete_transaction_feature = Self::execute_prepared_query(
            &self.scylla_session,
            &self.cache_delete_transaction,
            (num_bigint::BigInt::from(block_height), transaction_hash),
        );
        let delete_receipts_feature = Self::execute_prepared_query(
            &self.scylla_session,
            &self.cache_delete_receipts,
            (num_bigint::BigInt::from(block_height), transaction_hash),
        );
        futures::try_join!(delete_transaction_feature, delete_receipts_feature)?;
        Ok(())
    }
}
