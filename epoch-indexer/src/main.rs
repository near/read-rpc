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

        if let Err(e) = epoch_indexer::save_epoch_info(
            &epoch.epoch_info,
            &db_manager,
            None,
            epoch_info.next_epoch_id,
        )
        .await
        {
            tracing::warn!("Error saving epoch info: {:?}", e);
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
    let indexer_config =
        configuration::read_configuration::<configuration::EpochIndexerConfig>().await?;

    let opts: Opts = Opts::parse();

    let indexer_id = &indexer_config.general.indexer_id;
    let s3_client = indexer_config.lake_config.lake_s3_client().await;
    let rpc_client =
        near_jsonrpc_client::JsonRpcClient::connect(&indexer_config.general.near_rpc_url)
            .header(("Referer", indexer_config.general.referer_header_value))?;

    let protocol_config_view = rpc_client
        .call(
            near_jsonrpc_client::methods::EXPERIMENTAL_protocol_config::RpcProtocolConfigRequest {
                block_reference:
                    near_indexer_primitives::near_primitives::types::BlockReference::Finality(
                        near_indexer_primitives::near_primitives::types::Finality::Final,
                    ),
            },
        )
        .await?;

    let db_manager = database::prepare_db_manager::<database::PostgresDBManager>(
        &indexer_config.database,
        protocol_config_view.shard_layout,
    )
    .await?;

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
