pub use near_client::{NearClient, NearJsonRpc};
use near_indexer_primitives::views::StateChangeValueView;
use near_indexer_primitives::CryptoHash;
use near_primitives::types::ShardId;
use std::collections::HashMap;

use futures::FutureExt;

#[macro_use]
extern crate lazy_static;

use tokio_retry::{strategy::FixedInterval, Retry};

pub mod configs;
mod epoch;
pub mod metrics;
mod near_client;

const SAVE_ATTEMPTS: usize = 20;

// Target for tracing logs
pub const INDEXER: &str = "state_indexer";

pub type AccountIdStateChangeKey = String;

#[derive(Debug, Default)]
struct StateChangesToStore {
    data: HashMap<
        ShardId,
        HashMap<AccountIdStateChangeKey, near_indexer_primitives::views::StateChangeWithCauseView>,
    >,
    access_key: HashMap<
        ShardId,
        HashMap<AccountIdStateChangeKey, near_indexer_primitives::views::StateChangeWithCauseView>,
    >,
    contract: HashMap<
        ShardId,
        HashMap<AccountIdStateChangeKey, near_indexer_primitives::views::StateChangeWithCauseView>,
    >,
    account: HashMap<
        ShardId,
        HashMap<AccountIdStateChangeKey, near_indexer_primitives::views::StateChangeWithCauseView>,
    >,
}

impl StateChangesToStore {
    // Unpack the state_changes_data into futures split by shard_id
    // and store them asynchronously using join_all
    async fn save_data(
        &self,
        db_manager: &(impl database::StateIndexerDbManager + Sync + Send + 'static),
        block_height: u64,
    ) -> anyhow::Result<()> {
        if !self.data.is_empty() {
            let futures = self.data.iter().map(|(shard_id, state_changes)| {
                db_manager.save_state_changes_data(
                    *shard_id,
                    state_changes.values().cloned().collect(),
                    block_height,
                )
            });
            futures::future::join_all(futures)
                .await
                .into_iter()
                .collect::<anyhow::Result<()>>()?;
        }
        Ok(())
    }

    // Unpack the state_changes_access_key into futures split by shard_id
    // and store them asynchronously using join_all
    async fn save_access_key(
        &self,
        db_manager: &(impl database::StateIndexerDbManager + Sync + Send + 'static),
        block_height: u64,
    ) -> anyhow::Result<()> {
        if !self.access_key.is_empty() {
            let futures = self.access_key.iter().map(|(shard_id, state_changes)| {
                db_manager.save_state_changes_access_key(
                    *shard_id,
                    state_changes.values().cloned().collect(),
                    block_height,
                )
            });
            futures::future::join_all(futures)
                .await
                .into_iter()
                .collect::<anyhow::Result<()>>()?;
        }
        Ok(())
    }

    // Unpack the state_changes_contract into futures split by shard_id
    // and store them asynchronously using join_all
    async fn save_contract(
        &self,
        db_manager: &(impl database::StateIndexerDbManager + Sync + Send + 'static),
        block_height: u64,
    ) -> anyhow::Result<()> {
        if !self.contract.is_empty() {
            let futures = self.contract.iter().map(|(shard_id, state_changes)| {
                db_manager.save_state_changes_contract(
                    *shard_id,
                    state_changes.values().cloned().collect(),
                    block_height,
                )
            });
            futures::future::join_all(futures)
                .await
                .into_iter()
                .collect::<anyhow::Result<()>>()?;
        }
        Ok(())
    }

    // Unpack the state_changes_account into futures split by shard_id
    // and store them asynchronously using join_all
    async fn save_account(
        &self,
        db_manager: &(impl database::StateIndexerDbManager + Sync + Send + 'static),
        block_height: u64,
    ) -> anyhow::Result<()> {
        if !self.account.is_empty() {
            let futures = self.account.iter().map(|(shard_id, state_changes)| {
                db_manager.save_state_changes_account(
                    *shard_id,
                    state_changes.values().cloned().collect(),
                    block_height,
                )
            });
            futures::future::join_all(futures)
                .await
                .into_iter()
                .collect::<anyhow::Result<()>>()?;
        }
        Ok(())
    }

    async fn save_state_changes(
        &self,
        db_manager: &(impl database::StateIndexerDbManager + Sync + Send + 'static),
        block_height: u64,
    ) -> anyhow::Result<()> {
        let save_data_future = self.save_data(db_manager, block_height);
        let save_access_key_future = self.save_access_key(db_manager, block_height);
        let save_contract_future = self.save_contract(db_manager, block_height);
        let save_account_future = self.save_account(db_manager, block_height);

        futures::future::join_all([
            save_data_future.boxed(),
            save_access_key_future.boxed(),
            save_contract_future.boxed(),
            save_account_future.boxed(),
        ])
        .await
        .into_iter()
        .collect::<anyhow::Result<()>>()?;

        Ok(())
    }
}

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(streamer_message, db_manager))
)]
pub async fn handle_streamer_message(
    streamer_message: near_indexer_primitives::StreamerMessage,
    db_manager: &(impl database::StateIndexerDbManager + Sync + Send + 'static),
    near_client: &(impl NearClient + std::fmt::Debug + Sync),
    indexer_config: impl configuration::RightsizingConfig
        + configuration::IndexerConfig
        + std::fmt::Debug
        + Sync,
    stats: std::sync::Arc<tokio::sync::RwLock<metrics::Stats>>,
    shard_layout: &near_primitives::shard_layout::ShardLayout,
) -> anyhow::Result<()> {
    let block_height = streamer_message.block.header.height;
    let block_hash = streamer_message.block.header.hash;

    let range_id = configuration::utils::get_data_range_id(&block_height).await?;
    if stats.read().await.current_range_id < range_id {
        db_manager.create_new_range_tables(range_id).await?;
        stats.write().await.current_range_id = range_id;
    }

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
        near_client,
        db_manager,
    );
    let update_meta_future =
        db_manager.update_meta(indexer_config.indexer_id().as_ref(), block_height);

    let retry_strategy = FixedInterval::from_millis(500).take(SAVE_ATTEMPTS);

    let handle_block_future = Retry::spawn(retry_strategy.clone(), || async {
        db_manager
            .save_block_with_chunks(
                block_height,
                block_hash,
                streamer_message
                    .block
                    .chunks
                    .iter()
                    .map(|chunk| {
                        (
                            chunk.chunk_hash.to_string(),
                            chunk.shard_id.into(),
                            chunk.height_included,
                        )
                    })
                    .collect(),
            )
            .await
            .map_err(|e| {
                tracing::warn!(
                    target: crate::INDEXER,
                    "Failed to save block with chunks: {}",
                    e
                );
                e
            })
    });
    let handle_state_change_future = Retry::spawn(retry_strategy, || async {
        handle_state_changes(
            &streamer_message,
            db_manager,
            block_height,
            &indexer_config,
            shard_layout,
        )
        .await
        .map_err(|e| {
            tracing::warn!(
                target: crate::INDEXER,
                "Failed to save state changes: {}",
                e
            );
            e
        })
    });

    futures::future::join_all([
        handle_epoch_future.boxed(),
        handle_block_future.boxed(),
        handle_state_change_future.boxed(),
        update_meta_future.boxed(),
    ])
    .await
    .into_iter()
    .collect::<anyhow::Result<()>>()?;

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
        let epoch_info = epoch::get_epoch_info_by_id(current_epoch_id, near_client).await?;
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
    near_client: &(impl NearClient + std::fmt::Debug),
    db_manager: &(impl database::StateIndexerDbManager + Sync + Send + 'static),
) -> anyhow::Result<()> {
    if let Some(stats_epoch_id) = stats_current_epoch_id {
        if stats_epoch_id != current_epoch_id {
            // If epoch changed, we need to save epoch info and update epoch_end_height
            let epoch_info = epoch::get_epoch_info_by_id(stats_epoch_id, near_client).await?;
            epoch::save_epoch_info(
                &epoch_info,
                db_manager,
                Some(stats_current_epoch_height),
                next_epoch_id,
            )
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
    indexer_config: &(impl configuration::RightsizingConfig + std::fmt::Debug),
    shard_layout: &near_primitives::shard_layout::ShardLayout,
) -> anyhow::Result<()> {
    let mut state_changes_to_store = StateChangesToStore::default();

    let initial_state_changes = streamer_message
        .shards
        .iter()
        .flat_map(|shard| shard.clone().state_changes.into_iter());

    // Collecting a unique list of StateChangeWithCauseView for account_id + change kind + suffix
    // by overwriting the records in the HashMap
    for state_change in initial_state_changes.into_iter() {
        if !indexer_config.state_should_be_indexed(&state_change.value) {
            continue;
        };
        match &state_change.value {
            StateChangeValueView::DataUpdate {
                account_id, key, ..
            }
            | StateChangeValueView::DataDeletion { account_id, key } => {
                let shard_id = shard_layout.account_id_to_shard_id(account_id);
                // returning a hex-encoded key to ensure we store data changes to the key
                // (if there is more than one change to the same key)
                let data_key: &[u8] = key.as_ref();
                let key = format!("{}_data_{}", account_id.as_str(), hex::encode(data_key));
                // This will override the previous record for this account_id + state change kind + suffix
                state_changes_to_store
                    .data
                    .entry(shard_id)
                    .and_modify(|sharded_state_changes| {
                        sharded_state_changes.insert(key.clone(), state_change.clone());
                    })
                    .or_insert(HashMap::from([(key, state_change)]));
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
                let shard_id = shard_layout.account_id_to_shard_id(account_id);
                // returning a hex-encoded key to ensure we store data changes to the key
                // (if there is more than one change to the same key)
                let key = format!(
                    "{}_access_key_{}",
                    account_id.as_str(),
                    hex::encode(borsh::to_vec(&public_key)?)
                );
                // This will override the previous record for this account_id + state change kind + suffix
                state_changes_to_store
                    .access_key
                    .entry(shard_id)
                    .and_modify(|sharded_state_changes| {
                        sharded_state_changes.insert(key.clone(), state_change.clone());
                    })
                    .or_insert(HashMap::from([(key, state_change)]));
            }
            // ContractCode and Account changes is not separate-able by any key, we can omit the suffix
            StateChangeValueView::ContractCodeUpdate { account_id, .. }
            | StateChangeValueView::ContractCodeDeletion { account_id } => {
                let shard_id = shard_layout.account_id_to_shard_id(account_id);
                let key = format!("{}_contract", account_id.as_str());
                // This will override the previous record for this account_id + state change kind + suffix
                state_changes_to_store
                    .contract
                    .entry(shard_id)
                    .and_modify(|sharded_state_changes| {
                        sharded_state_changes.insert(key.clone(), state_change.clone());
                    })
                    .or_insert(HashMap::from([(key, state_change)]));
            }
            StateChangeValueView::AccountUpdate { account_id, .. }
            | StateChangeValueView::AccountDeletion { account_id } => {
                let shard_id = shard_layout.account_id_to_shard_id(account_id);
                let key = format!("{}_account", account_id.as_str());
                // This will override the previous record for this account_id + state change kind + suffix
                state_changes_to_store
                    .account
                    .entry(shard_id)
                    .and_modify(|sharded_state_changes| {
                        sharded_state_changes.insert(key.clone(), state_change.clone());
                    })
                    .or_insert(HashMap::from([(key, state_change)]));
            }
        };
    }

    state_changes_to_store
        .save_state_changes(db_manager, block_height)
        .await
}
