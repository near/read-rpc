use crate::config::{Opts, StartOptions};
use clap::Parser;
use database::StateIndexerDbManager;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

mod config;

async fn index_epochs(
    s3_client: &near_lake_framework::s3_fetchers::LakeS3Client,
    s3_bucket_name: &str,
    db_manager: impl StateIndexerDbManager + Sync + Send + 'static,
    rpc_client: near_jsonrpc_client::JsonRpcClient,
    indexer_id: &str,
    start_epoch: readnode_primitives::IndexedEpochInfoWithPreviousAndNextEpochId,
) -> anyhow::Result<()> {
    let mut epoch = start_epoch;
    loop {
        let epoch_info =
            match epoch_indexer::get_next_epoch(&epoch, s3_client, s3_bucket_name, &rpc_client)
                .await
            {
                Ok(next_epoch) => next_epoch,
                Err(e) => {
                    anyhow::bail!("Error fetching next epoch: {:?}", e);
                }
            };

        if let Err(e) =
            epoch_indexer::save_epoch_info(&epoch_info.epoch_info, &db_manager, None).await
        {
            tracing::warn!("Error saving epoch info: {:?}", e);
        }
        if let Err(e) = epoch_indexer::update_epoch_end_height(
            &db_manager,
            epoch_info.previous_epoch_id,
            epoch_info.next_epoch_id,
        )
        .await
        {
            tracing::warn!("Error update epoch_end_height: {:?}", e);
        }
        db_manager
            .update_meta(indexer_id, epoch.epoch_info.epoch_start_height)
            .await?;
        epoch = epoch_info;
    }
}

pub(crate) fn init_tracing() -> anyhow::Result<()> {
    let mut env_filter = tracing_subscriber::EnvFilter::new("epoch_indexer=info");

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

    opentelemetry::global::shutdown_tracer_provider();

    opentelemetry::global::set_text_map_propagator(
        opentelemetry::sdk::propagation::TraceContextPropagator::new(),
    );

    let subscriber = tracing_subscriber::Registry::default().with(env_filter);
    subscriber
        .with(tracing_subscriber::fmt::Layer::default().compact())
        .try_init()?;

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    let opts: Opts = Opts::parse();

    init_tracing()?;

    #[cfg(feature = "scylla_db")]
    let db_manager =
        database::prepare_db_manager::<database::scylladb::state_indexer::ScyllaDBManager>(
            &opts.database_url,
            opts.database_user.as_deref(),
            opts.database_password.as_deref(),
            opts.to_additional_database_options().await,
        )
        .await?;

    #[cfg(all(feature = "postgres_db", not(feature = "scylla_db")))]
    let db_manager =
        database::prepare_db_manager::<database::postgres::state_indexer::PostgresDBManager>(
            &opts.database_url,
            opts.database_user.as_deref(),
            opts.database_password.as_deref(),
            opts.to_additional_database_options().await,
        )
        .await?;

    let s3_client = opts.to_s3_client().await;
    let rpc_client = near_jsonrpc_client::JsonRpcClient::connect(opts.rpc_url());

    let epoch = match opts.start_options() {
        StartOptions::FromGenesis => {
            epoch_indexer::first_epoch(&s3_client, &opts.s3_bucket_name, &rpc_client).await?
        }
        StartOptions::FromInterruption => {
            let block_height = db_manager
                .get_last_processed_block_height(opts.indexer_id.as_str())
                .await?;
            epoch_indexer::get_epoch_info_by_block_height(
                block_height,
                &s3_client,
                &opts.s3_bucket_name,
                &rpc_client,
            )
            .await?
        }
    };
    epoch_indexer::save_epoch_info(&epoch.epoch_info, &db_manager, None).await?;

    index_epochs(
        &s3_client,
        &opts.s3_bucket_name,
        db_manager,
        rpc_client,
        &opts.indexer_id,
        epoch,
    )
    .await?;

    Ok(())
}
