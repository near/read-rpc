use itertools::Itertools;
use std::collections::HashMap;

use clap::Parser;
use futures::StreamExt;

use crate::configs::Opts;
use near_indexer_primitives::views::StateChangeValueView;
use near_indexer_primitives::CryptoHash;

mod configs;
mod metrics;

#[macro_use]
extern crate lazy_static;

// Categories for logging
pub(crate) const INDEXER: &str = "state_indexer";
type AccountId = String;

#[derive(Debug)]
struct StateChangesToStore {
    data: HashMap<AccountId, ShardedStateChangesWithCause>,
    access_key: HashMap<AccountId, ShardedStateChangesWithCause>,
    contract: HashMap<AccountId, ShardedStateChangesWithCause>,
    account: HashMap<AccountId, ShardedStateChangesWithCause>,
}

/// Wrapper around Statechanges with cause to store shard_id
/// based on the account_id and ShardLayout settings
#[derive(Debug)]
struct ShardedStateChangesWithCause {
    shard_id: u64,
    state_change: near_indexer_primitives::views::StateChangeWithCauseView,
}

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
    shard_layout: &near_primitives::shard_layout::ShardLayout,
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
    let handle_block_future = db_manager.save_block(
        block_height,
        block_hash,
        streamer_message
            .block
            .chunks
            .iter()
            .map(|chunk| (chunk.chunk_hash.to_string(), chunk.shard_id, chunk.height_included))
            .collect(),
    );
    let handle_state_change_future =
        handle_state_changes(&streamer_message, db_manager, block_height, block_hash, &indexer_config, &shard_layout);

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
async fn handle_epoch(
    stats_current_epoch_id: Option<CryptoHash>,
    stats_current_epoch_height: u64,
    current_epoch_id: CryptoHash,
    next_epoch_id: CryptoHash,
    rpc_client: &near_jsonrpc_client::JsonRpcClient,
    db_manager: &(impl database::StateIndexerDbManager + Sync + Send + 'static),
) -> anyhow::Result<()> {
    if let Some(stats_epoch_id) = stats_current_epoch_id {
        if stats_epoch_id != current_epoch_id {
            // If epoch changed, we need to save epoch info and update epoch_end_height
            let epoch_info = epoch_indexer::get_epoch_info_by_id(stats_epoch_id, rpc_client).await?;
            epoch_indexer::save_epoch_info(&epoch_info, db_manager, Some(stats_current_epoch_height), next_epoch_id)
                .await?;
        }
    }
    Ok(())
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
    streamer_message: &near_indexer_primitives::StreamerMessage,
    db_manager: &(impl database::StateIndexerDbManager + Sync + Send + 'static),
    block_height: u64,
    block_hash: CryptoHash,
    indexer_config: &configuration::StateIndexerConfig,
    shard_layout: &near_primitives::shard_layout::ShardLayout,
) -> anyhow::Result<()> {
    let mut state_changes_to_store = StateChangesToStore {
        data: HashMap::new(),
        access_key: HashMap::new(),
        contract: HashMap::new(),
        account: HashMap::new(),
    };

    let initial_state_changes = streamer_message
        .shards
        .iter()
        .flat_map(|shard| shard.clone().state_changes.into_iter());

    // Collecting a unique list of StateChangeWithCauseView for account_id + change kind + suffix
    // by overwriting the records in the HashMap
    // TODO: Refactor to add shard_id to the value, perhaps we need to create a separate wrapper structure
    // TODO: move the logic from the database to the state-indexer to handle the saving data to a specific shard
    // TODO: otherwise there is an implicit spawn of a bunch of futures that are not controlled
    for state_change in initial_state_changes.into_iter() {
        if !indexer_config.state_should_be_indexed(&state_change.value) {
            continue;
        };
        match &state_change.value {
            StateChangeValueView::DataUpdate { account_id, key, .. }
            | StateChangeValueView::DataDeletion { account_id, key } => {
                let shard_id = near_primitives::shard_layout::account_id_to_shard_id(account_id, shard_layout);
                // returning a hex-encoded key to ensure we store data changes to the key
                // (if there is more than one change to the same key)
                let data_key: &[u8] = key.as_ref();
                let key = format!("{}_data_{}", account_id.as_str(), hex::encode(data_key));
                // This will override the previous record for this account_id + state change kind + suffix
                state_changes_to_store
                    .data
                    .insert(key, ShardedStateChangesWithCause { shard_id, state_change });
            }
            StateChangeValueView::AccessKeyUpdate {
                account_id, public_key, ..
            }
            | StateChangeValueView::AccessKeyDeletion { account_id, public_key } => {
                let shard_id = near_primitives::shard_layout::account_id_to_shard_id(account_id, shard_layout);
                // returning a hex-encoded key to ensure we store data changes to the key
                // (if there is more than one change to the same key)
                let key = format!("{}_access_key_{}", account_id.as_str(), hex::encode(borsh::to_vec(&public_key)?));
                // This will override the previous record for this account_id + state change kind + suffix
                state_changes_to_store
                    .access_key
                    .insert(key, ShardedStateChangesWithCause { shard_id, state_change });
            }
            // ContractCode and Account changes is not separate-able by any key, we can omit the suffix
            StateChangeValueView::ContractCodeUpdate { account_id, .. }
            | StateChangeValueView::ContractCodeDeletion { account_id } => {
                let shard_id = near_primitives::shard_layout::account_id_to_shard_id(account_id, shard_layout);
                let key = format!("{}_contract", account_id.as_str());
                // This will override the previous record for this account_id + state change kind + suffix
                state_changes_to_store
                    .contract
                    .insert(key, ShardedStateChangesWithCause { shard_id, state_change });
            }
            StateChangeValueView::AccountUpdate { account_id, .. }
            | StateChangeValueView::AccountDeletion { account_id } => {
                let shard_id = near_primitives::shard_layout::account_id_to_shard_id(account_id, shard_layout);
                let key = format!("{}_account", account_id.as_str());
                // This will override the previous record for this account_id + state change kind + suffix
                state_changes_to_store
                    .account
                    .insert(key, ShardedStateChangesWithCause { shard_id, state_change });
            }
        };
    }

    let futures = futures::stream::FuturesUnordered::new();

    // Unpack the state_changes_to_store into futures split by shard_id
    // and store them asynchronously using try_join!
    state_changes_to_store
        .data
        .values()
        .chunk_by(|sharded_state_change| sharded_state_change.shard_id)
        .into_iter()
        .for_each(|(shard_id, sharded_state_changes)| {
            let state_changes = sharded_state_changes
                .map(|sharded_state_change| sharded_state_change.state_change.clone())
                .collect();
            futures.push(db_manager.save_state_changes_data(shard_id, state_changes, block_height, block_hash));
        });

    state_changes_to_store
        .access_key
        .values()
        .chunk_by(|sharded_state_change| sharded_state_change.shard_id)
        .into_iter()
        .for_each(|(shard_id, sharded_state_changes)| {
            let state_changes = sharded_state_changes
                .map(|sharded_state_change| sharded_state_change.state_change.clone())
                .collect();
            futures.push(db_manager.save_state_changes_access_key(shard_id, state_changes, block_height, block_hash))
        });

    state_changes_to_store
        .contract
        .values()
        .chunk_by(|sharded_state_change| sharded_state_change.shard_id)
        .into_iter()
        .for_each(|(shard_id, sharded_state_changes)| {
            let state_changes = sharded_state_changes
                .map(|sharded_state_change| sharded_state_change.state_change.clone())
                .collect();
            futures.push(db_manager.save_state_changes_contract(shard_id, state_changes, block_height, block_hash))
        });

    state_changes_to_store
        .account
        .values()
        .chunk_by(|sharded_state_change| sharded_state_change.shard_id)
        .into_iter()
        .for_each(|(shard_id, sharded_state_changes)| {
            let state_changes = sharded_state_changes
                .map(|sharded_state_change| sharded_state_change.state_change.clone())
                .collect();
            futures.push(db_manager.save_state_changes_account(shard_id, state_changes, block_height, block_hash))
        });

    futures::future::try_join_all(futures).await?;
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

    let rpc_client = near_jsonrpc_client::JsonRpcClient::connect(&indexer_config.general.near_rpc_url)
        .header(("Referer", indexer_config.general.referer_header_value.clone()))?;

    let protocol_config_view = rpc_client
        .call(near_jsonrpc_client::methods::EXPERIMENTAL_protocol_config::RpcProtocolConfigRequest {
            block_reference: near_primitives::types::BlockReference::Finality(near_primitives::types::Finality::Final),
        })
        .await?;

    let db_manager = database::prepare_db_manager::<database::PostgresDBManager>(
        &indexer_config.database,
        protocol_config_view.shard_layout.clone(),
    )
    .await?;
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
                &protocol_config_view.shard_layout,
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
