use crate::config::{init_tracing, Opts};
use clap::Parser;
use database::ScyllaStorageManager;
use futures::StreamExt;
mod collector;
mod config;
mod storage;

pub(crate) const INDEXER: &str = "tx_indexer";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    init_tracing();
    let opts: Opts = Opts::parse();
    tracing::info!(target: INDEXER, "Connecting to redis...");
    let redis_connection_manager = storage::connect(&opts.redis_connection_string).await?;

    tracing::info!(target: INDEXER, "Connecting to scylla db...");
    let scylla_db_client: std::sync::Arc<config::ScyllaDBManager> = std::sync::Arc::new(
        *config::ScyllaDBManager::new(
            &opts.scylla_url,
            opts.scylla_user.as_deref(),
            opts.scylla_password.as_deref(),
            None,
        )
        .await?,
    );

    tracing::info!(target: INDEXER, "Generating LakeConfig...");
    let config: near_lake_framework::LakeConfig = opts.to_lake_config().await?;

    tracing::info!(target: INDEXER, "Instantiating the stream...",);
    let (sender, stream) = near_lake_framework::streamer(config);

    tracing::info!(target: INDEXER, "Starting tx indexer...",);
    let mut handlers = tokio_stream::wrappers::ReceiverStream::new(stream)
        .map(|streamer_message| {
            handle_streamer_message(
                streamer_message,
                &scylla_db_client,
                &redis_connection_manager,
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

async fn handle_streamer_message(
    streamer_message: near_indexer_primitives::StreamerMessage,
    scylla_db_client: &std::sync::Arc<config::ScyllaDBManager>,
    redis_connection_manager: &redis::aio::ConnectionManager,
) -> anyhow::Result<u64> {
    tracing::info!(
        target: INDEXER,
        "Block {}",
        streamer_message.block.header.height
    );

    let tx_future = collector::index_transactions(
        &streamer_message,
        scylla_db_client,
        redis_connection_manager,
    );

    match futures::try_join!(tx_future) {
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

    storage::set(
        redis_connection_manager,
        "last_indexed_block",
        &streamer_message.block.header.height.to_string(),
    )
    .await?;

    Ok(streamer_message.block.header.height)
}
