use crate::config::{Opts, StartOptions};
use clap::Parser;
use database::StateIndexerDbManager;

mod config;

// Categories for logging
pub(crate) const INDEXER: &str = "epoch_indexer";

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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    configuration::init_tracing(INDEXER).await?;
    let indexer_config = configuration::read_configuration().await?;

    let opts: Opts = Opts::parse();

    #[cfg(feature = "scylla_db")]
    let db_manager = database::prepare_db_manager::<
        database::scylladb::state_indexer::ScyllaDBManager,
    >(&indexer_config.database)
    .await?;

    #[cfg(all(feature = "postgres_db", not(feature = "scylla_db")))]
    let db_manager = database::prepare_db_manager::<
        database::postgres::state_indexer::PostgresDBManager,
    >(&indexer_config.database)
    .await?;

    let indexer_id = &indexer_config.general.state_indexer.indexer_id;
    let s3_client = indexer_config.to_s3_client().await;
    let rpc_client =
        near_jsonrpc_client::JsonRpcClient::connect(&indexer_config.general.near_rpc_url);

    let epoch = match opts.start_options {
        StartOptions::FromGenesis => {
            epoch_indexer::first_epoch(
                &s3_client,
                &indexer_config.lake_config.aws_bucket_name,
                &rpc_client,
            )
            .await?
        }
        StartOptions::FromInterruption => {
            let block_height = db_manager
                .get_last_processed_block_height(indexer_id)
                .await?;
            epoch_indexer::get_epoch_info_by_block_height(
                block_height,
                &s3_client,
                &indexer_config.lake_config.aws_bucket_name,
                &rpc_client,
            )
            .await?
        }
    };
    epoch_indexer::save_epoch_info(&epoch.epoch_info, &db_manager, None).await?;

    index_epochs(
        &s3_client,
        &indexer_config.lake_config.aws_bucket_name,
        db_manager,
        rpc_client,
        indexer_id,
        epoch,
    )
    .await?;

    Ok(())
}
