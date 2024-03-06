use clap::Parser;
use futures::StreamExt;

use crate::configs::Opts;
use near_indexer::near_primitives::hash::CryptoHash;
use near_indexer::near_primitives::views::{StateChangeValueView, StateChangeWithCauseView};

mod configs;
mod metrics;
mod utils;

#[macro_use]
extern crate lazy_static;

// Categories for logging
pub(crate) const INDEXER: &str = "near_state_indexer";

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(streamer_message, db_manager))
)]
async fn handle_streamer_message(
    streamer_message: near_indexer::StreamerMessage,
    db_manager: &(impl database::StateIndexerDbManager + Sync + Send + 'static),
    client: &actix::Addr<near_client::ViewClientActor>,
    indexer_config: configuration::NearStateIndexerConfig,
    stats: std::sync::Arc<tokio::sync::RwLock<metrics::Stats>>,
) -> anyhow::Result<()> {
    let block_height = streamer_message.block.header.height;
    let block_hash = streamer_message.block.header.hash;

    let current_epoch_id = streamer_message.block.header.epoch_id;
    let next_epoch_id = streamer_message.block.header.next_epoch_id;

    tracing::debug!(target: INDEXER, "Block height {}", block_height,);

    stats
        .write()
        .await
        .block_heights_processing
        .insert(block_height);

    let handle_epoch_future = handle_epoch(
        stats.read().await.current_epoch_id,
        stats.read().await.current_epoch_height,
        current_epoch_id,
        next_epoch_id,
        db_manager,
        client,
    );
    let handle_block_future = db_manager.save_block(
        block_height,
        block_hash,
        streamer_message
            .block
            .chunks
            .iter()
            .map(|chunk| {
                (
                    chunk.chunk_hash.to_string(),
                    chunk.shard_id,
                    chunk.height_included,
                )
            })
            .collect(),
    );
    let handle_state_change_future = handle_state_changes(
        streamer_message,
        db_manager,
        block_height,
        block_hash,
        &indexer_config,
    );

    let update_meta_future =
        db_manager.update_meta(&indexer_config.general.indexer_id, block_height);

    futures::try_join!(
        handle_epoch_future,
        handle_block_future,
        handle_state_change_future,
        update_meta_future
    )?;

    metrics::BLOCK_PROCESSED_TOTAL.inc();
    // Prometheus Gauge Metric type do not support u64
    // https://github.com/tikv/rust-prometheus/issues/470
    metrics::LATEST_BLOCK_HEIGHT.set(i64::try_from(block_height)?);

    let mut stats_lock = stats.write().await;
    stats_lock.block_heights_processing.remove(&block_height);
    stats_lock.blocks_processed_count += 1;
    stats_lock.last_processed_block_height = block_height;
    if let Some(stats_epoch_id) = stats_lock.current_epoch_id {
        if current_epoch_id != stats_epoch_id {
            stats_lock.current_epoch_id = Some(current_epoch_id);
            if stats_epoch_id == CryptoHash::default() {
                stats_lock.current_epoch_height = 1;
            } else {
                stats_lock.current_epoch_height += 1;
            }
        }
    } else {
        // handle first indexing epoch
        let epoch_info = utils::fetch_epoch_validators_info(current_epoch_id, client).await?;
        stats_lock.current_epoch_id = Some(current_epoch_id);
        stats_lock.current_epoch_height = epoch_info.epoch_height;
    }
    Ok(())
}

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(db_manager))
)]
async fn handle_epoch(
    stats_current_epoch_id: Option<CryptoHash>,
    stats_current_epoch_height: u64,
    current_epoch_id: CryptoHash,
    next_epoch_id: CryptoHash,
    db_manager: &(impl database::StateIndexerDbManager + Sync + Send + 'static),
    client: &actix::Addr<near_client::ViewClientActor>,
) -> anyhow::Result<()> {
    if let Some(stats_epoch_id) = stats_current_epoch_id {
        if stats_epoch_id == current_epoch_id {
            // If epoch didn't change, we don't need handle it
            Ok(())
        } else {
            // If epoch changed, we need to save epoch info and update epoch_end_height
            let validators_info =
                utils::fetch_epoch_validators_info(stats_epoch_id, client).await?;

            db_manager
                .add_validators(
                    stats_epoch_id,
                    stats_current_epoch_height,
                    validators_info.epoch_start_height,
                    &validators_info,
                )
                .await?;

            db_manager
                .update_epoch_end_height(stats_epoch_id, next_epoch_id)
                .await?;
            Ok(())
        }
    } else {
        // If stats_current_epoch_id is None, we don't need handle it
        Ok(())
    }
}

/// This function will iterate over all StateChangesWithCauseViews in order to collect
/// a single StateChangesWithCauseView for a unique account and unique change kind, and unique key.
/// The reasoning behind this is that in a single Block (StreamerMessage) there might be a bunch of
/// changes to the same change kind to the same account to the same key (state key or public key) and
/// we want to ensure we store the very last of them.
/// It's impossible to achieve it with handling all of them one by one asynchronously (they might be handled
/// in any order) so it's easier for us to skip all the changes except the latest one.
#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(streamer_message, db_manager))
)]
async fn handle_state_changes(
    streamer_message: near_indexer::StreamerMessage,
    db_manager: &(impl database::StateIndexerDbManager + Sync + Send + 'static),
    block_height: u64,
    block_hash: CryptoHash,
    indexer_config: &configuration::NearStateIndexerConfig,
) -> anyhow::Result<Vec<()>> {
    let mut state_changes_to_store =
        std::collections::HashMap::<String, StateChangeWithCauseView>::new();

    let initial_state_changes = streamer_message
        .shards
        .into_iter()
        .flat_map(|shard| shard.state_changes.into_iter());

    // Collecting a unique list of StateChangeWithCauseView for account_id + change kind + suffix
    // by overwriting the records in the HashMap
    for state_change in initial_state_changes {
        if !indexer_config.state_should_be_indexed(&state_change.value) {
            continue;
        };
        let key = match &state_change.value {
            StateChangeValueView::DataUpdate {
                account_id, key, ..
            }
            | StateChangeValueView::DataDeletion { account_id, key } => {
                // returning a hex-encoded key to ensure we store data changes to the key
                // (if there is more than one change to the same key)
                let key: &[u8] = key.as_ref();
                format!("{}_data_{}", account_id.as_str(), hex::encode(key))
            }
            StateChangeValueView::AccessKeyUpdate {
                account_id,
                public_key,
                ..
            }
            | StateChangeValueView::AccessKeyDeletion {
                account_id,
                public_key,
            } => {
                // returning a hex-encoded key to ensure we store data changes to the key
                // (if there is more than one change to the same key)
                let data_key = borsh::to_vec(&public_key)?;
                format!(
                    "{}_access_key_{}",
                    account_id.as_str(),
                    hex::encode(data_key)
                )
            }
            // ContractCode and Account changes is not separate-able by any key, we can omit the suffix
            StateChangeValueView::ContractCodeUpdate { account_id, .. }
            | StateChangeValueView::ContractCodeDeletion { account_id } => {
                format!("{}_contract", account_id.as_str())
            }
            StateChangeValueView::AccountUpdate { account_id, .. }
            | StateChangeValueView::AccountDeletion { account_id } => {
                format!("{}_account", account_id.as_str())
            }
        };
        // This will override the previous record for this account_id + state change kind + suffix
        state_changes_to_store.insert(key, state_change);
    }

    // Asynchronous storing of StateChangeWithCauseView into the storage.
    let futures = state_changes_to_store
        .into_values()
        .map(|state_change_with_cause| {
            db_manager.save_state_change(state_change_with_cause, block_height, block_hash)
        });

    futures::future::try_join_all(futures).await
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    configuration::init_tracing(INDEXER).await?;
    // We use it to automatically search the for root certificates to perform HTTPS calls
    // (sending telemetry and downloading genesis)
    openssl_probe::init_ssl_cert_env_vars();
    let opts: Opts = Opts::parse();
    let home_dir = opts.home.unwrap_or_else(near_indexer::get_default_home);

    match opts.subcmd {
        configs::SubCommand::Run => run(home_dir).await?,
        configs::SubCommand::Init(init_config) => {
            near_indexer::indexer_init_configs(&home_dir, init_config.into())?
        }
    };
    Ok(())
}

async fn run(home_dir: std::path::PathBuf) -> anyhow::Result<()> {
    let state_indexer_config =
        configuration::read_configuration::<configuration::NearStateIndexerConfig>().await?;

    #[cfg(feature = "scylla_db")]
    let db_manager = database::prepare_db_manager::<
        database::scylladb::state_indexer::ScyllaDBManager,
    >(&state_indexer_config.database)
    .await?;

    #[cfg(all(feature = "postgres_db", not(feature = "scylla_db")))]
    let db_manager = database::prepare_db_manager::<
        database::postgres::state_indexer::PostgresDBManager,
    >(&state_indexer_config.database)
    .await?;

    // Initiate metrics http server
    tokio::spawn(
        metrics::init_server(state_indexer_config.general.metrics_server_port)
            .expect("Failed to start metrics server"),
    );

    let indexer_config = near_indexer::IndexerConfig {
        home_dir,
        sync_mode: near_indexer::SyncModeEnum::LatestSynced,
        await_for_node_synced: near_indexer::AwaitForNodeSyncedEnum::StreamWhileSyncing,
        validate_genesis: true,
    };
    let indexer = near_indexer::Indexer::new(indexer_config)?;

    // Regular indexer process starts here
    let stream = indexer.streamer();
    let (view_client, _) = indexer.client_actors();

    let stats = std::sync::Arc::new(tokio::sync::RwLock::new(metrics::Stats::new()));
    tokio::spawn(metrics::state_logger(
        std::sync::Arc::clone(&stats),
        view_client.clone(),
    ));
    tokio::spawn(utils::optimistic_stream(view_client.clone()));

    let mut handlers = tokio_stream::wrappers::ReceiverStream::new(stream)
        .map(|streamer_message| {
            handle_streamer_message(
                streamer_message,
                &db_manager,
                &view_client,
                state_indexer_config.clone(),
                std::sync::Arc::clone(&stats),
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
