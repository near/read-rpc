use itertools::Itertools;
use std::collections::HashMap;

use clap::Parser;
use futures::StreamExt;

use logic_state_indexer::{configs, handle_streamer_message, metrics, NearClient, INDEXER};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // We use it to automatically search the for root certificates to perform HTTPS calls
    // (sending telemetry and downloading genesis)
    openssl_probe::init_ssl_cert_env_vars();

    configuration::init_tracing(INDEXER).await?;
    let indexer_config = configuration::read_configuration::<configuration::StateIndexerConfig>().await?;
    let opts: configs::Opts = configs::Opts::parse();

    // Here we have to get the latest ProtocolConfigView to get the up-to-date ShardLayout
    // we use the Referer header to ensure we take it from the native RPC node
    let rpc_client = near_jsonrpc_client::JsonRpcClient::connect(&indexer_config.general.near_rpc_url)
        .header(("Referer", indexer_config.general.referer_header_value.clone()))?;
    let near_client = logic_state_indexer::NearJsonRpc::new(rpc_client);

    let protocol_config_view = near_client.protocol_config().await?;

    let db_manager = database::prepare_db_manager::<database::PostgresDBManager>(
        &indexer_config.database,
        protocol_config_view.shard_layout.clone(),
    )
    .await?;
    let start_block_height = configs::get_start_block_height(
        &near_client,
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
    tokio::spawn(metrics::state_logger(std::sync::Arc::clone(&stats), near_client.clone()));

    let mut handlers = tokio_stream::wrappers::ReceiverStream::new(stream)
        .map(|streamer_message| {
            handle_streamer_message(
                streamer_message,
                &db_manager,
                &near_client,
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
