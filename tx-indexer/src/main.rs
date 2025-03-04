use clap::Parser;
use futures::{FutureExt, StreamExt};
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
    tracing::info!(
        "Starting {} v{}",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION"),
    );

    let indexer_config =
        configuration::read_configuration::<configuration::TxIndexerConfig>().await?;

    let opts = config::Opts::parse();

    let mut rpc_client =
        near_jsonrpc_client::JsonRpcClient::connect(&indexer_config.general.near_rpc_url);
    if let Some(auth_token) = &indexer_config.general.rpc_auth_token {
        rpc_client = rpc_client.header(near_jsonrpc_client::auth::Authorization::bearer(
            auth_token,
        )?);
    }

    tracing::info!(target: INDEXER, "Instantiating the tx_details storage client...");
    let tx_details_storage = std::sync::Arc::new(
        TxDetailsStorage::new(indexer_config.tx_details_storage.scylla_client().await).await?,
    );

    let start_block_height = config::get_start_block_height(
        &rpc_client,
        &tx_details_storage,
        &opts.start_options,
        &indexer_config.general.indexer_id,
    )
    .await?;

    tracing::info!(target: INDEXER, "Generating LakeConfig...");
    let lake_config = indexer_config
        .lake_config
        .lake_config(start_block_height, indexer_config.general.chain_id.clone())
        .await?;

    tracing::info!(target: INDEXER, "Creating cache storage...");
    let tx_collecting_storage = std::sync::Arc::new(
        storage::CacheStorage::init_with_restore(indexer_config.general.redis_url.to_string())
            .await?,
    );

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
    tx_collecting_storage: &std::sync::Arc<storage::CacheStorage>,
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
        tx_collecting_storage,
        tx_details_storage,
        &indexer_config,
    );

    let update_meta_future = tx_details_storage.update_meta(
        &indexer_config.general.indexer_id,
        streamer_message.block.header.height,
    );

    match futures::future::join_all([tx_future.boxed(), update_meta_future.boxed()])
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
    {
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
