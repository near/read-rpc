use borsh::BorshSerialize;
use clap::Parser;
use database::ScyllaStorageManager;
use futures::StreamExt;

use crate::configs::Opts;
use near_indexer_primitives::views::StateChangeValueView;
use near_indexer_primitives::CryptoHash;
use near_primitives_core::account::Account;

mod configs;
mod metrics;

#[macro_use]
extern crate lazy_static;

// Categories for logging
pub(crate) const INDEXER: &str = "state_indexer";

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(streamer_message, scylla_storage, indexer_id))
)]
async fn handle_streamer_message(
    streamer_message: near_indexer_primitives::StreamerMessage,
    scylla_storage: &configs::ScyllaDBManager,
    indexer_id: &str,
) -> anyhow::Result<()> {
    let block_height = streamer_message.block.header.height;
    let block_hash = streamer_message.block.header.hash;
    tracing::info!(target: INDEXER, "Block height {}", block_height,);

    let handle_block_future = handle_block(
        block_height,
        block_hash,
        streamer_message
            .block
            .chunks
            .iter()
            .map(|c| c.chunk_hash.to_string())
            .collect(),
        scylla_storage,
    );
    let handle_state_change_future = handle_state_changes(streamer_message, scylla_storage, block_height, block_hash);

    let update_meta_future = scylla_storage.update_meta(indexer_id, num_bigint::BigInt::from(block_height));

    futures::try_join!(handle_block_future, handle_state_change_future, update_meta_future)?;

    metrics::BLOCK_PROCESSED_TOTAL.inc();
    // Prometheus Gauge Metric type do not support u64
    // https://github.com/tikv/rust-prometheus/issues/470
    metrics::LATEST_BLOCK_HEIGHT.set(i64::try_from(block_height)?);
    Ok(())
}

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(scylla_storage)))]
async fn handle_block(
    block_height: u64,
    block_hash: CryptoHash,
    chunks: Vec<String>,
    scylla_storage: &configs::ScyllaDBManager,
) -> anyhow::Result<()> {
    scylla_storage
        .add_block(num_bigint::BigInt::from(block_height), block_hash, chunks)
        .await
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
    tracing::instrument(skip(streamer_message, scylla_storage))
)]
async fn handle_state_changes(
    streamer_message: near_indexer_primitives::StreamerMessage,
    scylla_storage: &configs::ScyllaDBManager,
    block_height: u64,
    block_hash: CryptoHash,
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
        let key = match &state_change.value {
            StateChangeValueView::DataUpdate { account_id, key, .. }
            | StateChangeValueView::DataDeletion { account_id, key } => {
                // returning a hex-encoded key to ensure we store data changes to the key
                // (if there is more than one change to the same key)
                let key: &[u8] = key.as_ref();
                format!("{}_data_{}", account_id.as_ref(), hex::encode(key))
            }
            StateChangeValueView::AccessKeyUpdate {
                account_id, public_key, ..
            }
            | StateChangeValueView::AccessKeyDeletion { account_id, public_key } => {
                // returning a hex-encoded key to ensure we store data changes to the key
                // (if there is more than one change to the same key)
                let data_key = public_key
                    .try_to_vec()
                    .expect("Failed to borsh-serialize the PublicKey");
                format!("{}_access_key_{}", account_id.as_ref(), hex::encode(data_key))
            }
            // ContractCode and Account changes is not separate-able by any key, we can omit the suffix
            StateChangeValueView::ContractCodeUpdate { account_id, .. }
            | StateChangeValueView::ContractCodeDeletion { account_id } => format!("{}_contract", account_id.as_ref()),
            StateChangeValueView::AccountUpdate { account_id, .. }
            | StateChangeValueView::AccountDeletion { account_id } => format!("{}_account", account_id.as_ref()),
        };
        // This will override the previous record for this account_id + state change kind + suffix
        state_changes_to_store.insert(key, state_change);
    }

    // Asynchronous storing of StateChangeWithCauseView into the storage.
    let futures = state_changes_to_store.into_values().map(|state_change_with_cause| {
        store_state_change(state_change_with_cause, scylla_storage, block_height, block_hash)
    });

    futures::future::try_join_all(futures).await
}

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(state_change, scylla_storage))
)]
async fn store_state_change(
    state_change: near_indexer_primitives::views::StateChangeWithCauseView,
    scylla_storage: &configs::ScyllaDBManager,
    block_height: u64,
    block_hash: CryptoHash,
) -> anyhow::Result<()> {
    let block_height = num_bigint::BigInt::from(block_height);
    match state_change.value {
        StateChangeValueView::DataUpdate { account_id, key, value } => {
            scylla_storage
                .add_state_changes(account_id, block_height, block_hash, key.as_ref(), value.as_ref())
                .await?
        }
        StateChangeValueView::DataDeletion { account_id, key } => {
            scylla_storage
                .delete_state_changes(account_id, block_height, block_hash, key.as_ref())
                .await?
        }
        StateChangeValueView::AccessKeyUpdate {
            account_id,
            public_key,
            access_key,
        } => {
            let data_key = public_key
                .try_to_vec()
                .expect("Failed to borsh-serialize the PublicKey");
            let data_value = access_key
                .try_to_vec()
                .expect("Failed to borsh-serialize the AccessKey");
            let add_access_key_future = scylla_storage.add_access_key(
                account_id.clone(),
                block_height.clone(),
                block_hash,
                &data_key,
                &data_value,
            );

            #[cfg(feature = "account_access_keys")]
            let add_account_access_keys_future =
                scylla_storage.add_account_access_keys(account_id, block_height, &data_key, Some(&data_value));
            #[cfg(feature = "account_access_keys")]
            futures::try_join!(add_access_key_future, add_account_access_keys_future)?;
            #[cfg(not(feature = "account_access_keys"))]
            add_access_key_future.await?;
        }
        StateChangeValueView::AccessKeyDeletion { account_id, public_key } => {
            let data_key = public_key
                .try_to_vec()
                .expect("Failed to borsh-serialize the PublicKey");
            let delete_access_key_future =
                scylla_storage.delete_access_key(account_id.clone(), block_height.clone(), block_hash, &data_key);

            #[cfg(feature = "account_access_keys")]
            let add_account_access_keys_future =
                scylla_storage.add_account_access_keys(account_id, block_height, &data_key, None);
            #[cfg(feature = "account_access_keys")]
            futures::try_join!(delete_access_key_future, add_account_access_keys_future)?;

            #[cfg(not(feature = "account_access_keys"))]
            delete_access_key_future.await?;
        }
        StateChangeValueView::ContractCodeUpdate { account_id, code } => {
            scylla_storage
                .add_contract_code(account_id, block_height, block_hash, code.as_ref())
                .await?
        }
        StateChangeValueView::ContractCodeDeletion { account_id } => {
            scylla_storage
                .delete_contract_code(account_id, block_height, block_hash)
                .await?
        }
        StateChangeValueView::AccountUpdate { account_id, account } => {
            let value = Account::from(account)
                .try_to_vec()
                .expect("Failed to borsh-serialize the Account");
            scylla_storage
                .add_account(account_id, block_height, block_hash, value)
                .await?
        }
        StateChangeValueView::AccountDeletion { account_id } => {
            scylla_storage
                .delete_account(account_id, block_height, block_hash)
                .await?
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // We use it to automatically search the for root certificates to perform HTTPS calls
    // (sending telemetry and downloading genesis)
    dotenv::dotenv().ok();

    openssl_probe::init_ssl_cert_env_vars();

    configs::init_tracing()?;

    let opts: Opts = Opts::parse();

    let scylla_storage = configs::ScyllaDBManager::new(
        &opts.scylla_url,
        opts.scylla_user.as_deref(),
        opts.scylla_password.as_deref(),
        None,
    )
    .await?;
    let scylla_session = scylla_storage.scylla_session().await;
    let config: near_lake_framework::LakeConfig = opts.to_lake_config(&scylla_session).await?;
    let (sender, stream) = near_lake_framework::streamer(config);

    // Initiate metrics http server
    tokio::spawn(metrics::init_server(opts.port).expect("Failed to start metrics server"));

    let mut handlers = tokio_stream::wrappers::ReceiverStream::new(stream)
        .map(|streamer_message| handle_streamer_message(streamer_message, &scylla_storage, &opts.indexer_id))
        .buffer_unordered(opts.concurrency);

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
