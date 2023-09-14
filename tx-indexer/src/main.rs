use crate::config::{init_tracing, Opts};
use clap::Parser;
use database::ScyllaStorageManager;
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
    tracing::info!(target: INDEXER, "Creating hash storage...");
    // let tx_collecting_storage = std::sync::Arc::new(storage::hash::HashStorage::new());
    let tx_collecting_storage = std::sync::Arc::new(
        storage::scylla_redis_api::ScyllaDBRedisAPIStorage::new("redis://127.0.0.1").await,
    );

    tracing::info!(target: INDEXER, "Connecting to scylla db...");
    let scylla_db_client: std::sync::Arc<config::ScyllaDBManager> = std::sync::Arc::new(
        *config::ScyllaDBManager::new(
            &opts.scylla_url,
            opts.scylla_user.as_deref(),
            opts.scylla_password.as_deref(),
            opts.scylla_preferred_dc.as_deref(),
            None,
            opts.max_retry,
            opts.strict_mode,
        )
        .await?,
    );

    tracing::info!(target: INDEXER, "Generating LakeConfig...");
    let scylla_session = scylla_db_client.scylla_session().await;
    let config: near_lake_framework::LakeConfig = opts.to_lake_config(&scylla_session).await?;

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
                streamer_message,
                &scylla_db_client,
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
    streamer_message: near_indexer_primitives::StreamerMessage,
    scylla_db_client: &std::sync::Arc<config::ScyllaDBManager>,
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

    let tx_future =
        collector::index_transactions(&streamer_message, scylla_db_client, tx_collecting_storage);

    let update_meta_future =
        scylla_db_client.update_meta(indexer_id, streamer_message.block.header.height);

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
