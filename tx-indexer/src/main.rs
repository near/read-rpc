use crate::config::{init_tracing, Opts};
use clap::Parser;
use futures::StreamExt;
mod collector;
mod config;
mod metrics;
mod storage;

#[macro_use]
extern crate lazy_static;

pub(crate) const INDEXER: &str = "tx_indexer";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    init_tracing()?;

    let opts: Opts = Opts::parse();

    tracing::info!(target: INDEXER, "Connecting to db...");
    let db_manager: std::sync::Arc<Box<dyn database::TxIndexerDbManager + Sync + Send + 'static>> =
        std::sync::Arc::new(Box::new(
            database::prepare_tx_indexer_db_manager(
                &opts.database_url,
                opts.database_user.as_deref(),
                opts.database_password.as_deref(),
                opts.to_additional_database_options().await,
            )
            .await?,
        ));

    let start_block_height = config::get_start_block_height(&opts, &db_manager).await?;
    tracing::info!(target: INDEXER, "Generating LakeConfig...");
    let config: near_lake_framework::LakeConfig = opts.to_lake_config(start_block_height).await?;

    tracing::info!(target: INDEXER, "Creating hash storage...");
    let tx_collecting_storage = std::sync::Arc::new(
        storage::database::HashStorageWithDB::init_with_restore(
            db_manager.clone(),
            start_block_height,
            opts.cache_restore_blocks_range,
            opts.max_db_parallel_queries,
        )
        .await?,
    );

    tracing::info!(target: INDEXER, "Instantiating the stream...",);
    let (sender, stream) = near_lake_framework::streamer(config);

    // Initiate metrics http server
    tokio::spawn(metrics::init_server(opts.port).expect("Failed to start metrics server"));

    let stats = std::sync::Arc::new(tokio::sync::RwLock::new(metrics::Stats::new()));
    tokio::spawn(metrics::state_logger(
        std::sync::Arc::clone(&stats),
        opts.rpc_url().to_string(),
    ));

    tracing::info!(target: INDEXER, "Starting tx indexer...",);
    let mut handlers = tokio_stream::wrappers::ReceiverStream::new(stream)
        .map(|streamer_message| {
            handle_streamer_message(
                opts.chain_id.clone(),
                streamer_message,
                &db_manager,
                &tx_collecting_storage,
                &opts.indexer_id,
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
    chain_id: config::ChainId,
    streamer_message: near_indexer_primitives::StreamerMessage,
    db_manager: &std::sync::Arc<Box<dyn database::TxIndexerDbManager + Sync + Send + 'static>>,
    tx_collecting_storage: &std::sync::Arc<impl storage::base::TxCollectingStorage>,
    indexer_id: &str,
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
        chain_id,
        &streamer_message,
        db_manager,
        tx_collecting_storage,
    );

    let update_meta_future =
        db_manager.update_meta(indexer_id, streamer_message.block.header.height);

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
