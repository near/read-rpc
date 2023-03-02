use clap::Parser;

use bigdecimal::FromPrimitive;
use borsh::BorshSerialize;
use futures::StreamExt;
use scylla::{QueryResult, Session};

use tracing_subscriber::EnvFilter;

use crate::configs::Opts;
use near_indexer_primitives::views::StateChangeValueView;
use near_indexer_primitives::CryptoHash;
use near_primitives_core::account::Account;

mod configs;

// Categories for logging
const INDEXER: &str = "state_indexer";

async fn handle_streamer_message(
    streamer_message: near_indexer_primitives::StreamerMessage,
    scylladb_session: &Session,
    indexer_id: &str,
) -> anyhow::Result<()> {
    let block_height = streamer_message.block.header.height;
    let block_hash = streamer_message.block.header.hash;
    tracing::info!(target: INDEXER, "Block height {}", block_height,);

    handle_block(&streamer_message.block, scylladb_session).await?;

    let futures = streamer_message.shards.into_iter().flat_map(|shard| {
        shard.state_changes.into_iter().map(|state_change_with_cause| {
            handle_state_change(state_change_with_cause, scylladb_session, block_height, block_hash)
        })
    });

    futures::future::try_join_all(futures).await?;

    tracing::debug!(
        target: INDEXER,
        "INSERT INTO meta
        (indexer_id, last_processed_block_height)
        VALUES ({}, {})",
        indexer_id,
        block_height,
    );
    let mut query = scylla::statement::query::Query::new(
        "INSERT INTO meta
        (indexer_id, last_processed_block_height)
        VALUES (?, ?)",
    );
    query.set_consistency(scylla::frame::types::Consistency::All);
    scylladb_session
        .query(query, (indexer_id, bigdecimal::BigDecimal::from_u64(block_height).unwrap()))
        .await?;
    Ok(())
}

async fn handle_block(
    block: &near_indexer_primitives::views::BlockView,
    scylladb_session: &Session,
) -> anyhow::Result<QueryResult> {
    tracing::debug!(
        target: INDEXER,
        "INSERT INTO blocks
        (block_height, block_hash, chunks)
        VALUES ({}, {}, {:?})",
        block.header.height,
        block.header.hash.to_string(),
        block
            .chunks
            .iter()
            .map(|chunk_header_view| chunk_header_view.chunk_hash.to_string())
            .collect::<Vec<String>>(),
    );
    let mut query = scylla::statement::query::Query::new(
        "INSERT INTO blocks
        (block_height, block_hash, chunks)
        VALUES (?, ?, ?)",
    );
    query.set_consistency(scylla::frame::types::Consistency::All);
    Ok(scylladb_session
        .query(
            query,
            (
                bigdecimal::BigDecimal::from_u64(block.header.height).unwrap(),
                block.header.hash.to_string(),
                block
                    .chunks
                    .iter()
                    .map(|chunk_header_view| chunk_header_view.chunk_hash.to_string())
                    .collect::<Vec<String>>(),
            ),
        )
        .await?)
}

async fn handle_state_change(
    state_change: near_indexer_primitives::views::StateChangeWithCauseView,
    scylladb_session: &Session,
    block_height: u64,
    block_hash: CryptoHash,
) -> anyhow::Result<()> {
    match state_change.value {
        StateChangeValueView::DataUpdate { account_id, key, value } => {
            let key: &[u8] = key.as_ref();
            let value: &[u8] = value.as_ref();
            tracing::debug!(
                target: INDEXER,
                "INSERT INTO state_changes_data
                (account_id, block_height, block_hash, data_key, data_value)
                VALUES({}, {}, {}, {}, {:?})",
                account_id.to_string(),
                block_height,
                block_hash.to_string(),
                hex::encode(key),
                value.to_vec(),
            );
            let mut query = scylla::statement::query::Query::new(
                "INSERT INTO state_changes_data
                (account_id, block_height, block_hash, data_key, data_value)
                VALUES(?, ?, ?, ?, ?)",
            );
            query.set_consistency(scylla::frame::types::Consistency::All);
            match scylladb_session
                .query(
                    query,
                    (
                        account_id.to_string(),
                        bigdecimal::BigDecimal::from_u64(block_height).unwrap(),
                        block_hash.to_string(),
                        hex::encode(key).to_string(),
                        value.to_vec(),
                    ),
                )
                .await
            {
                Ok(_) => {}
                Err(err) => anyhow::bail!("DATAUPDATE ERROR {:?} | {}", err, hex::encode(key)),
            };
            tracing::debug!(
                target: INDEXER,
                "INSERT INTO account_state (account_id, data_key) VALUES({}, {})",
                account_id.to_string(),
                hex::encode(key),
            );
            let mut query = scylla::statement::query::Query::new(
                "INSERT INTO account_state
                    (account_id, data_key)
                    VALUES(?, ?)",
            );
            query.set_consistency(scylla::frame::types::Consistency::All);
            match scylladb_session
                .query(query, (account_id.to_string(), hex::encode(key).to_string()))
                .await
            {
                Ok(_) => {}
                Err(err) => anyhow::bail!("STATE ERROR {:?} | {}", err, hex::encode(key)),
            };
        }
        StateChangeValueView::DataDeletion { account_id, key } => {
            let key: &[u8] = key.as_ref();
            tracing::debug!(
                target: INDEXER,
                "INSERT INTO state_changes_data
                (account_id, block_height, block_hash, data_key, data_value)
                VALUES({}, {}, {}, {}, NULL)",
                account_id.to_string(),
                block_height,
                block_hash.to_string(),
                hex::encode(key),
            );
            let mut query = scylla::statement::query::Query::new(
                "INSERT INTO state_changes_data
                    (account_id, block_height, block_hash, data_key, data_value)
                    VALUES(?, ?, ?, ?, NULL)",
            );
            query.set_consistency(scylla::frame::types::Consistency::All);
            match scylladb_session
                .query(
                    query,
                    (
                        account_id.to_string(),
                        bigdecimal::BigDecimal::from_u64(block_height).unwrap(),
                        block_hash.to_string(),
                        hex::encode(key).to_string(),
                    ),
                )
                .await
            {
                Ok(_) => {}
                Err(err) => anyhow::bail!("DATADELETE ERROR {:?} | {}", err, hex::encode(key)),
            };
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
            tracing::debug!(
                target: INDEXER,
                "INSERT INTO state_changes_access_key
                (account_id, block_height, block_hash, data_key, data_value)
                VALUES({}, {}, {}, {}, {:?})",
                account_id.to_string(),
                block_height,
                block_hash.to_string(),
                hex::encode(&data_key),
                data_value,
            );
            let mut query = scylla::statement::query::Query::new(
                "INSERT INTO state_changes_access_key
                    (account_id, block_height, block_hash, data_key, data_value)
                    VALUES(?, ?, ?, ?, ?)",
            );
            query.set_consistency(scylla::frame::types::Consistency::All);
            match scylladb_session
                .query(
                    query,
                    (
                        account_id.to_string(),
                        bigdecimal::BigDecimal::from_u64(block_height).unwrap(),
                        block_hash.to_string(),
                        hex::encode(&data_key).to_string(),
                        data_value,
                    ),
                )
                .await
            {
                Ok(_) => {}
                Err(err) => anyhow::bail!("ACCESSKEYUPDATE ERROR {:?} | {}", err, hex::encode(data_key)),
            };
        }
        StateChangeValueView::AccessKeyDeletion { account_id, public_key } => {
            let data_key = public_key
                .try_to_vec()
                .expect("Failed to borsh-serialize the PublicKey");
            tracing::debug!(
                target: INDEXER,
                "INSERT INTO state_changes_access_key
                (account_id, block_height, block_hash, data_key, data_value)
                VALUES({}, {}, {}, {}, NULL)",
                account_id.to_string(),
                block_height,
                block_hash.to_string(),
                hex::encode(&data_key),
            );
            let mut query = scylla::statement::query::Query::new(
                "INSERT INTO state_changes_access_key
                    (account_id, block_height, block_hash, data_key, data_value)
                    VALUES(?, ?, ?, ?, NULL)",
            );
            query.set_consistency(scylla::frame::types::Consistency::All);
            match scylladb_session
                .query(
                    query,
                    (
                        account_id.to_string(),
                        bigdecimal::BigDecimal::from_u64(block_height).unwrap(),
                        block_hash.to_string(),
                        hex::encode(&data_key).to_string(),
                    ),
                )
                .await
            {
                Ok(_) => {}
                Err(err) => anyhow::bail!("ACCESSKEYDELETION ERROR {:?} | {}", err, hex::encode(data_key)),
            };
        }
        StateChangeValueView::ContractCodeUpdate { account_id, code } => {
            let code: &[u8] = code.as_ref();
            tracing::debug!(
                target: INDEXER,
                "INSERT INTO state_changes_contract
                (account_id, block_height, block_hash, data_value)
                VALUES({}, {}, {}, {:?})",
                account_id.to_string(),
                block_height,
                block_hash.to_string(),
                code.to_vec(),
            );
            let mut query = scylla::statement::query::Query::new(
                "INSERT INTO state_changes_contract
                    (account_id, block_height, block_hash, data_value)
                    VALUES(?, ?, ?, ?)",
            );
            query.set_consistency(scylla::frame::types::Consistency::All);
            match scylladb_session
                .query(
                    query,
                    (
                        account_id.to_string(),
                        bigdecimal::BigDecimal::from_u64(block_height).unwrap(),
                        block_hash.to_string(),
                        code.to_vec(),
                    ),
                )
                .await
            {
                Ok(_) => {}
                Err(err) => anyhow::bail!("CONTRACTCODEUPDATE ERROR {:?}", err),
            };
        }
        StateChangeValueView::ContractCodeDeletion { account_id } => {
            tracing::debug!(
                target: INDEXER,
                "INSERT INTO state_changes_contract
                (account_id, block_height, block_hash, data_value)
                VALUES({}, {}, {}, NULL)",
                account_id.to_string(),
                block_height,
                block_hash.to_string(),
            );
            let mut query = scylla::statement::query::Query::new(
                "INSERT INTO state_changes_contract
                    (account_id, block_height, block_hash, data_value)
                    VALUES(?, ?, ?, NULL)",
            );
            query.set_consistency(scylla::frame::types::Consistency::All);
            match scylladb_session
                .query(
                    query,
                    (
                        account_id.to_string(),
                        bigdecimal::BigDecimal::from_u64(block_height).unwrap(),
                        block_hash.to_string(),
                    ),
                )
                .await
            {
                Ok(_) => {}
                Err(err) => anyhow::bail!("CONTRACTCODEDELETION ERROR {:?}", err),
            };
        }
        StateChangeValueView::AccountUpdate { account_id, account } => {
            let value = Account::from(account)
                .try_to_vec()
                .expect("Failed to borsh-serialize the Account");
            tracing::debug!(
                target: INDEXER,
                "INSERT INTO state_changes_account
                (account_id, block_height, block_hash, data_value)
                VALUES({}, {}, {}, {:?})",
                account_id.to_string(),
                block_height,
                block_hash.to_string(),
                &value,
            );
            let mut query = scylla::statement::query::Query::new(
                "INSERT INTO state_changes_account
                    (account_id, block_height, block_hash, data_value)
                    VALUES(?, ?, ?, ?)",
            );
            query.set_consistency(scylla::frame::types::Consistency::All);
            match scylladb_session
                .query(
                    query,
                    (
                        account_id.to_string(),
                        bigdecimal::BigDecimal::from_u64(block_height).unwrap(),
                        block_hash.to_string(),
                        value,
                    ),
                )
                .await
            {
                Ok(_) => {}
                Err(err) => anyhow::bail!("ACCOUNTUPDATE ERROR {:?}", err),
            };
        }
        StateChangeValueView::AccountDeletion { account_id } => {
            tracing::debug!(
                target: INDEXER,
                "INSERT INTO state_changes_account
                (account_id, block_height, block_hash, data_value)
                VALUES({}, {}, {}, NULL)",
                account_id.to_string(),
                block_height,
                block_hash.to_string(),
            );
            let mut query = scylla::statement::query::Query::new(
                "INSERT INTO state_changes_account
                    (account_id, block_height, block_hash, data_value)
                    VALUES(?, ?, ?, NULL)",
            );
            query.set_consistency(scylla::frame::types::Consistency::All);
            match scylladb_session
                .query(
                    query,
                    (
                        account_id.to_string(),
                        bigdecimal::BigDecimal::from_u64(block_height).unwrap(),
                        block_hash.to_string(),
                    ),
                )
                .await
            {
                Ok(_) => {}
                Err(err) => anyhow::bail!("ACCOUNTDELETION ERROR {:?}", err),
            };
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // We use it to automatically search the for root certificates to perform HTTPS calls
    // (sending telemetry and downloading genesis)
    openssl_probe::init_ssl_cert_env_vars();

    let mut env_filter = EnvFilter::new("state_indexer=info");

    if let Ok(rust_log) = std::env::var("RUST_LOG") {
        if !rust_log.is_empty() {
            for directive in rust_log.split(',').filter_map(|s| match s.parse() {
                Ok(directive) => Some(directive),
                Err(err) => {
                    eprintln!("Ignoring directive `{}`: {}", s, err);
                    None
                }
            }) {
                env_filter = env_filter.add_directive(directive);
            }
        }
    }

    tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .init();

    dotenv::dotenv().ok();

    let opts: Opts = Opts::parse();

    let config: near_lake_framework::LakeConfig = opts.to_lake_config().await?;
    let (sender, stream) = near_lake_framework::streamer(config);

    // let redis_connection = get_redis_connection().await?;
    let scylladb_session = configs::get_scylladb_session(
        &opts.scylla_url,
        &opts.scylla_keyspace,
        opts.scylla_user.as_deref(),
        opts.scylla_password.as_deref(),
    )
    .await?;

    let mut handlers = tokio_stream::wrappers::ReceiverStream::new(stream)
        .map(|streamer_message| handle_streamer_message(streamer_message, &scylladb_session, &opts.indexer_id))
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
