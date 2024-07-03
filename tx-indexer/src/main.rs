use clap::Parser;
use futures::StreamExt;

use tx_details_storage::TxDetailsStorage;

mod collector;
mod config;
mod metrics;
mod storage;

#[macro_use]
extern crate lazy_static;

pub(crate) const INDEXER: &str = "tx_indexer";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    configuration::init_tracing(INDEXER).await?;
    let indexer_config =
        configuration::read_configuration::<configuration::TxIndexerConfig>().await?;

    let opts = config::Opts::parse();

    tracing::info!(target: INDEXER, "Connecting to db...");
    #[cfg(feature = "scylla_db")]
    let db_manager: std::sync::Arc<
        Box<dyn database::TxIndexerDbManager + Sync + Send + 'static>,
    > = std::sync::Arc::new(Box::new(
        database::prepare_db_manager::<database::scylladb::tx_indexer::ScyllaDBManager>(
            &indexer_config.database,
        )
        .await?,
    ));
    #[cfg(all(feature = "postgres_db", not(feature = "scylla_db")))]
    let db_manager: std::sync::Arc<
        Box<dyn database::TxIndexerDbManager + Sync + Send + 'static>,
    > = std::sync::Arc::new(Box::new(
        database::prepare_db_manager::<database::postgres::tx_indexer::PostgresDBManager>(
            &indexer_config.database,
        )
        .await?,
    ));

    let rpc_client =
        near_jsonrpc_client::JsonRpcClient::connect(&indexer_config.general.near_rpc_url);
    let start_block_height = config::get_start_block_height(
        &rpc_client,
        &db_manager,
        &opts.start_options,
        &indexer_config.general.indexer_id,
    )
    .await?;

    tracing::info!(target: INDEXER, "Generating LakeConfig...");
    let lake_config = indexer_config
        .lake_config
        .lake_config(start_block_height)
        .await?;

    tracing::info!(target: INDEXER, "Creating hash storage...");
    let tx_collecting_storage = std::sync::Arc::new(
        storage::HashStorageWithDB::init_with_restore(
            db_manager.clone(),
            start_block_height,
            indexer_config.general.cache_restore_blocks_range,
            indexer_config.database.max_db_parallel_queries,
        )
        .await?,
    );

    tracing::info!(target: INDEXER, "Instantiating the tx_details storage client...");
    let tx_details_storage = std::sync::Arc::new(TxDetailsStorage::new(
        indexer_config.tx_details_storage.storage_client().await,
        indexer_config.tx_details_storage.aws_bucket_name.clone(),
    ));

    tracing::info!(target: INDEXER, "Instantiating the stream...",);
    let (sender, stream) = near_lake_framework::streamer(lake_config);

    // Initiate metrics http server
    tokio::spawn(
        metrics::init_server(indexer_config.general.metrics_server_port)
            .expect("Failed to start metrics server"),
    );

    let stats = std::sync::Arc::new(tokio::sync::RwLock::new(metrics::Stats::new()));
    tokio::spawn(metrics::state_logger(
        std::sync::Arc::clone(&stats),
        rpc_client.clone(),
    ));

    tracing::info!(target: INDEXER, "Starting tx indexer...",);
    let mut handlers = tokio_stream::wrappers::ReceiverStream::new(stream)
        .map(|streamer_message| {
            handle_streamer_message(
                streamer_message,
                &db_manager,
                &tx_collecting_storage,
                &tx_details_storage,
                indexer_config.clone(),
                std::sync::Arc::clone(&stats),
            )
        })
        .buffer_unordered(1usize);

    while let Some(_handle_message) = handlers.next().await {
        if let Err(err) = _handle_message {
            tracing::warn!(target: INDEXER, "{:?}", err);
        }
    }
    drop(handlers); // close the channel so the sender will stop

    // propagate errors from the sender
    match sender.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(e) => Err(anyhow::Error::from(e)), // JoinError
    }
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip_all))]
async fn handle_streamer_message(
    streamer_message: near_indexer_primitives::StreamerMessage,
    db_manager: &std::sync::Arc<Box<dyn database::TxIndexerDbManager + Sync + Send + 'static>>,
    tx_collecting_storage: &std::sync::Arc<storage::HashStorageWithDB>,
    tx_details_storage: &std::sync::Arc<TxDetailsStorage>,
    indexer_config: configuration::TxIndexerConfig,
    stats: std::sync::Arc<tokio::sync::RwLock<metrics::Stats>>,
) -> anyhow::Result<u64> {
    let block_height = streamer_message.block.header.height;
    tracing::debug!(target: INDEXER, "Block {}", block_height);

    stats
        .write()
        .await
        .block_heights_processing
        .insert(block_height);

    let tx_future = collector::index_transactions(
        &streamer_message,
        db_manager,
        tx_collecting_storage,
        tx_details_storage,
        &indexer_config,
    );

    let update_meta_future = db_manager.update_meta(
        &indexer_config.general.indexer_id,
        streamer_message.block.header.height,
    );

    match futures::try_join!(tx_future, update_meta_future) {
        Ok(_) => tracing::debug!(
            target: INDEXER,
            "#{} collecting transaction details successful",
            streamer_message.block.header.height,
        ),
        Err(e) => tracing::error!(
            target: INDEXER,
            "#{} an error occurred during collecting transaction details\n{:#?}",
            streamer_message.block.header.height,
            e
        ),
    };

    metrics::BLOCK_PROCESSED_TOTAL.inc();
    // Prometheus Gauge Metric type do not support u64
    // https://github.com/tikv/rust-prometheus/issues/470
    metrics::LATEST_BLOCK_HEIGHT.set(i64::try_from(streamer_message.block.header.height)?);

    let mut stats_lock = stats.write().await;
    stats_lock.block_heights_processing.remove(&block_height);
    stats_lock.blocks_processed_count += 1;
    stats_lock.last_processed_block_height = block_height;

    Ok(block_height)
}
