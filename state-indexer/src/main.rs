use clap::Parser;
use futures::StreamExt;

use crate::configs::Opts;
use near_indexer_primitives::views::StateChangeValueView;
use near_indexer_primitives::CryptoHash;
use near_primitives::account::Account;

mod configs;
mod metrics;

#[macro_use]
extern crate lazy_static;

// Categories for logging
pub(crate) const INDEXER: &str = "state_indexer";

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(streamer_message, db_manager))
)]
async fn handle_streamer_message(
    streamer_message: near_indexer_primitives::StreamerMessage,
    db_manager: &(impl database::StateIndexerDbManager + Sync + Send + 'static),
    rpc_client: &near_jsonrpc_client::JsonRpcClient,
    indexer_config: configuration::StateIndexerConfig,
    stats: std::sync::Arc<tokio::sync::RwLock<metrics::Stats>>,
) -> anyhow::Result<()> {
    let block_height = streamer_message.block.header.height;
    let block_hash = streamer_message.block.header.hash;

    let current_epoch_id = streamer_message.block.header.epoch_id;
    let next_epoch_id = streamer_message.block.header.next_epoch_id;

    tracing::debug!(target: INDEXER, "Block height {}", block_height,);

    stats.write().await.block_heights_processing.insert(block_height);

    let handle_epoch_future = handle_epoch(
        stats.read().await.current_epoch_id,
        stats.read().await.current_epoch_height,
        current_epoch_id,
        next_epoch_id,
        rpc_client,
        db_manager,
    );
    let handle_block_future = handle_block(
        block_height,
        block_hash,
        streamer_message
            .block
            .chunks
            .iter()
            .map(|chunk| (chunk.chunk_hash.to_string(), chunk.shard_id, chunk.height_included))
            .collect(),
        db_manager,
    );
    let handle_state_change_future =
        handle_state_changes(streamer_message, db_manager, block_height, block_hash, &indexer_config);

    let update_meta_future = db_manager.update_meta(&indexer_config.general.indexer_id, block_height);

    futures::try_join!(handle_epoch_future, handle_block_future, handle_state_change_future, update_meta_future)?;

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
        let epoch_info = epoch_indexer::get_epoch_info_by_id(current_epoch_id, rpc_client).await?;
        stats_lock.current_epoch_id = Some(current_epoch_id);
        stats_lock.current_epoch_height = epoch_info.epoch_height;
    }
    Ok(())
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(db_manager)))]
async fn handle_block(
    block_height: u64,
    block_hash: CryptoHash,
    chunks: Vec<(database::primitives::ChunkHash, database::primitives::ShardId, database::primitives::HeightIncluded)>,
    db_manager: &(impl database::StateIndexerDbManager + Sync + Send + 'static),
) -> anyhow::Result<()> {
    let add_block_future = db_manager.add_block(block_height, block_hash);
    let add_chunks_future = db_manager.add_chunks(block_height, chunks);

    futures::try_join!(add_block_future, add_chunks_future)?;
    Ok(())
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(db_manager)))]
async fn handle_epoch(
    stats_current_epoch_id: Option<CryptoHash>,
    stats_current_epoch_height: u64,
    current_epoch_id: CryptoHash,
    next_epoch_id: CryptoHash,
    rpc_client: &near_jsonrpc_client::JsonRpcClient,
    db_manager: &(impl database::StateIndexerDbManager + Sync + Send + 'static),
) -> anyhow::Result<()> {
    if let Some(stats_epoch_id) = stats_current_epoch_id {
        if stats_epoch_id == current_epoch_id {
            // If epoch didn't change, we don't need handle it
            Ok(())
        } else {
            // If epoch changed, we need to save epoch info and update epoch_end_height
            let epoch_info = epoch_indexer::get_epoch_info_by_id(stats_epoch_id, rpc_client).await?;
            epoch_indexer::save_epoch_info(&epoch_info, db_manager, Some(stats_current_epoch_height)).await?;
            epoch_indexer::update_epoch_end_height(db_manager, Some(stats_epoch_id), next_epoch_id).await?;
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
    streamer_message: near_indexer_primitives::StreamerMessage,
    db_manager: &(impl database::StateIndexerDbManager + Sync + Send + 'static),
    block_height: u64,
    block_hash: CryptoHash,
    indexer_config: &configuration::StateIndexerConfig,
) -> anyhow::Result<Vec<()>> {
    let mut state_changes_to_store =
        std::collections::HashMap::<String, near_indexer_primitives::views::StateChangeWithCauseView>::new();

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
            StateChangeValueView::DataUpdate { account_id, key, .. }
            | StateChangeValueView::DataDeletion { account_id, key } => {
                // returning a hex-encoded key to ensure we store data changes to the key
                // (if there is more than one change to the same key)
                let key: &[u8] = key.as_ref();
                format!("{}_data_{}", account_id.as_str(), hex::encode(key))
            }
            StateChangeValueView::AccessKeyUpdate {
                account_id, public_key, ..
            }
            | StateChangeValueView::AccessKeyDeletion { account_id, public_key } => {
                // returning a hex-encoded key to ensure we store data changes to the key
                // (if there is more than one change to the same key)
                let data_key = borsh::to_vec(&public_key)?;
                format!("{}_access_key_{}", account_id.as_str(), hex::encode(data_key))
            }
            // ContractCode and Account changes is not separate-able by any key, we can omit the suffix
            StateChangeValueView::ContractCodeUpdate { account_id, .. }
            | StateChangeValueView::ContractCodeDeletion { account_id } => format!("{}_contract", account_id.as_str()),
            StateChangeValueView::AccountUpdate { account_id, .. }
            | StateChangeValueView::AccountDeletion { account_id } => format!("{}_account", account_id.as_str()),
        };
        // This will override the previous record for this account_id + state change kind + suffix
        state_changes_to_store.insert(key, state_change);
    }

    // Asynchronous storing of StateChangeWithCauseView into the storage.
    let futures = state_changes_to_store.into_values().map(|state_change_with_cause| {
        store_state_change(state_change_with_cause, db_manager, block_height, block_hash)
    });

    futures::future::try_join_all(futures).await
}

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(state_change, db_manager))
)]
async fn store_state_change(
    state_change: near_indexer_primitives::views::StateChangeWithCauseView,
    db_manager: &(impl database::StateIndexerDbManager + Sync + Send + 'static),
    block_height: u64,
    block_hash: CryptoHash,
) -> anyhow::Result<()> {
    match state_change.value {
        StateChangeValueView::DataUpdate { account_id, key, value } => {
            db_manager
                .add_state_changes(account_id, block_height, block_hash, key.as_ref(), value.as_ref())
                .await?
        }
        StateChangeValueView::DataDeletion { account_id, key } => {
            db_manager
                .delete_state_changes(account_id, block_height, block_hash, key.as_ref())
                .await?
        }
        StateChangeValueView::AccessKeyUpdate {
            account_id,
            public_key,
            access_key,
        } => {
            let data_key = borsh::to_vec(&public_key)?;
            let data_value = borsh::to_vec(&access_key)?;
            let add_access_key_future =
                db_manager.add_access_key(account_id.clone(), block_height, block_hash, &data_key, &data_value);

            #[cfg(feature = "account_access_keys")]
            {
                let add_account_access_keys_future =
                    db_manager.add_account_access_keys(account_id, block_height, &data_key, Some(&data_value));
                futures::try_join!(add_access_key_future, add_account_access_keys_future)?;
            }
            #[cfg(not(feature = "account_access_keys"))]
            add_access_key_future.await?;
        }
        StateChangeValueView::AccessKeyDeletion { account_id, public_key } => {
            let data_key = borsh::to_vec(&public_key)?;
            let delete_access_key_future =
                db_manager.delete_access_key(account_id.clone(), block_height, block_hash, &data_key);

            #[cfg(feature = "account_access_keys")]
            {
                let delete_account_access_keys_future =
                    db_manager.add_account_access_keys(account_id, block_height, &data_key, None);
                futures::try_join!(delete_access_key_future, delete_account_access_keys_future)?;
            }
            #[cfg(not(feature = "account_access_keys"))]
            delete_access_key_future.await?;
        }
        StateChangeValueView::ContractCodeUpdate { account_id, code } => {
            db_manager
                .add_contract_code(account_id, block_height, block_hash, code.as_ref())
                .await?
        }
        StateChangeValueView::ContractCodeDeletion { account_id } => {
            db_manager
                .delete_contract_code(account_id, block_height, block_hash)
                .await?
        }
        StateChangeValueView::AccountUpdate { account_id, account } => {
            let value = borsh::to_vec(&Account::from(account))?;
            db_manager
                .add_account(account_id, block_height, block_hash, value)
                .await?
        }
        StateChangeValueView::AccountDeletion { account_id } => {
            db_manager.delete_account(account_id, block_height, block_hash).await?
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // We use it to automatically search the for root certificates to perform HTTPS calls
    // (sending telemetry and downloading genesis)
    openssl_probe::init_ssl_cert_env_vars();

    configuration::init_tracing(INDEXER).await?;
    let indexer_config = configuration::read_configuration::<configuration::StateIndexerConfig>().await?;
    let opts: Opts = Opts::parse();

    #[cfg(feature = "scylla_db")]
    let db_manager =
        database::prepare_db_manager::<database::scylladb::state_indexer::ScyllaDBManager>(&indexer_config.database)
            .await?;

    #[cfg(all(feature = "postgres_db", not(feature = "scylla_db")))]
    let db_manager =
        database::prepare_db_manager::<database::postgres::state_indexer::PostgresDBManager>(&indexer_config.database)
            .await?;

    let rpc_client = near_jsonrpc_client::JsonRpcClient::connect(&indexer_config.general.near_rpc_url);
    let start_block_height = configs::get_start_block_height(
        &rpc_client,
        &db_manager,
        &opts.start_options,
        &indexer_config.general.indexer_id,
    )
    .await?;

    let lake_config = indexer_config.lake_config.lake_config(start_block_height).await?;
    let (sender, stream) = near_lake_framework::streamer(lake_config);

    // Initiate metrics http server
    tokio::spawn(
        metrics::init_server(indexer_config.general.metrics_server_port).expect("Failed to start metrics server"),
    );

    let stats = std::sync::Arc::new(tokio::sync::RwLock::new(metrics::Stats::new()));
    tokio::spawn(metrics::state_logger(std::sync::Arc::clone(&stats), rpc_client.clone()));

    let mut handlers = tokio_stream::wrappers::ReceiverStream::new(stream)
        .map(|streamer_message| {
            handle_streamer_message(
                streamer_message,
                &db_manager,
                &rpc_client,
                indexer_config.clone(),
                std::sync::Arc::clone(&stats),
            )
        })
        .buffer_unordered(indexer_config.general.concurrency);

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
