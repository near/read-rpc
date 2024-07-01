use clap::Parser;
use futures::StreamExt;

use crate::configs::Opts;

use logic_state_indexer::{handle_streamer_message, NearClient, INDEXER};

mod configs;
mod metrics;
mod near_client;
mod utils;

// Categories for logging
// pub(crate) const INDEXER: &str = "near_state_indexer";

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    configuration::init_tracing(INDEXER).await?;
    // We use it to automatically search the for root certificates to perform HTTPS calls
    // (sending telemetry and downloading genesis)
    openssl_probe::init_ssl_cert_env_vars();
    let opts: Opts = Opts::parse();
    let home_dir = opts.home.unwrap_or_else(near_indexer::get_default_home);

    match opts.subcmd {
        configs::SubCommand::Run => {
            // This will set the near_lake_build_info metric
            // e.g. near_lake_build_info{build="1.37.1",release="0.1.29",rustc_version="1.76.0"}
            metrics::NODE_BUILD_INFO.reset();
            metrics::NODE_BUILD_INFO
                .with_label_values(&[
                    env!("CARGO_PKG_VERSION"),
                    env!("BUILD_VERSION"),
                    env!("RUSTC_VERSION"),
                ])
                .inc();
            run(home_dir).await?
        }
        configs::SubCommand::Init(init_config) => {
            // To avoid error like: `Cannot start a runtime from within a runtime.`,
            // you need to run the code that creates the second Tokio runtime on a completely independent thread.
            // The easiest way to do this is to use `std::thread::spawn`.
            // For improved performance, better  to use a thread pool instead of creating a new thread each time.
            // Tokio itself provides such a thread pool via spawn_blocking
            let _ = tokio::task::spawn_blocking(move || {
                near_indexer::indexer_init_configs(&home_dir, init_config.into())
            })
            .await?;
        }
    };
    Ok(())
}

async fn run(home_dir: std::path::PathBuf) -> anyhow::Result<()> {
    tracing::info!(target: INDEXER, "Read configuration ...");
    let state_indexer_config =
        configuration::read_configuration::<configuration::NearStateIndexerConfig>().await?;

    tracing::info!(target: INDEXER, "Connecting to redis...");
    let redis_client = redis::Client::open(state_indexer_config.general.redis_url.clone())?
        .get_connection_manager()
        .await?;

    tracing::info!(target: INDEXER, "Setup near_indexer...");
    let indexer_config = near_indexer::IndexerConfig {
        home_dir,
        // TODO: consider making this configurable in the future
        // For now, this near-indexer-based state-indexer is expected to run in addition to the Lake-based indexer(s)
        // Since the main purpose of this one is to be in sync and provide the optimistic data, we tend to use the latest synced data
        sync_mode: near_indexer::SyncModeEnum::LatestSynced,
        await_for_node_synced: near_indexer::AwaitForNodeSyncedEnum::WaitForFullSync,
        validate_genesis: false, // we don't want to validate genesis it takes too long and not necessary for this kind of node
    };
    let indexer = near_indexer::Indexer::new(indexer_config)?;

    // Regular indexer process starts here
    tracing::info!(target: INDEXER, "Instantiating the stream...");
    let stream = indexer.streamer();
    let (view_client, client) = indexer.client_actors();

    let near_client = near_client::NearViewClient::new(view_client.clone());
    let protocol_config_view = near_client.protocol_config().await?;

    tracing::info!(target: INDEXER, "Connecting to db...");
    let db_manager = database::prepare_db_manager::<database::PostgresDBManager>(
        &state_indexer_config.database,
        protocol_config_view.shard_layout.clone(),
    )
    .await?;

    let stats = std::sync::Arc::new(tokio::sync::RwLock::new(metrics::Stats::new()));
    tokio::spawn(metrics::state_logger(
        std::sync::Arc::clone(&stats),
        near_client.clone(),
    ));

    // Initiate the job of updating the optimistic blocks to Redis
    tokio::spawn(utils::update_block_in_redis_by_finality(
        view_client.clone(),
        client.clone(),
        redis_client.clone(),
        near_indexer_primitives::near_primitives::types::Finality::None,
    ));
    // And the same job for the final blocks
    tokio::spawn(utils::update_block_in_redis_by_finality(
        view_client.clone(),
        client.clone(),
        redis_client.clone(),
        near_indexer_primitives::near_primitives::types::Finality::Final,
    ));

    // ! Note that the `handle_streamer_message` doesn't interact with the Redis
    tracing::info!(target: INDEXER, "Starting near_state_indexer...");
    let mut handlers = tokio_stream::wrappers::ReceiverStream::new(stream)
        .map(|streamer_message| {
            handle_streamer_message(
                streamer_message,
                &db_manager,
                &near_client,
                state_indexer_config.clone(),
                std::sync::Arc::clone(&stats),
                &protocol_config_view.shard_layout,
            )
        })
        .buffer_unordered(state_indexer_config.general.concurrency);

    while let Some(_handle_message) = handlers.next().await {
        if let Err(err) = _handle_message {
            tracing::warn!(target: INDEXER, "{:?}", err);
        }
    }
    drop(handlers); // close the channel so the sender will stop

    Ok(())
}
