use near_indexer::near_primitives;
use near_o11y::WithSpanContextExt;

const INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

pub(crate) async fn fetch_epoch_validators_info(
    epoch_id: near_primitives::hash::CryptoHash,
    client: &actix::Addr<near_client::ViewClientActor>,
) -> anyhow::Result<near_primitives::views::EpochValidatorInfo> {
    Ok(client
        .send(
            near_client::GetValidatorInfo {
                epoch_reference: near_primitives::types::EpochReference::EpochId(
                    near_primitives::types::EpochId(epoch_id),
                ),
            }
            .with_span_context(),
        )
        .await??)
}

/// Fetches the status to retrieve `latest_block_height` to determine if we need to fetch
/// entire block or we already fetched this block.
pub(crate) async fn fetch_latest_block(
    client: &actix::Addr<near_client::ViewClientActor>,
) -> anyhow::Result<u64> {
    let block = client
        .send(
            near_client::GetBlock(near_primitives::types::BlockReference::Finality(
                near_primitives::types::Finality::Final,
            ))
            .with_span_context(),
        )
        .await??;
    Ok(block.header.height)
}

pub(crate) async fn fetch_optimistic_block(
    client: &actix::Addr<near_client::ViewClientActor>,
) -> anyhow::Result<near_primitives::views::BlockView> {
    Ok(client
        .send(near_client::GetBlock::latest().with_span_context())
        .await??)
}

pub(crate) async fn fetch_status(
    client: &actix::Addr<near_client::ClientActor>,
) -> anyhow::Result<near_primitives::views::StatusResponse> {
    tracing::debug!(target: crate::INDEXER, "Fetching status");
    Ok(client
        .send(
            near_client::Status {
                is_health_check: false,
                detailed: false,
            }
            .with_span_context(),
        )
        .await??)
}

pub(crate) async fn update_block_streamer_message(
    block_type: near_primitives::types::Finality,
    streamer_message: &near_indexer::StreamerMessage,
    redis_client: redis::aio::ConnectionManager,
) -> anyhow::Result<()> {
    let block_height = streamer_message.block.header.height;
    let json_streamer_message = serde_json::to_string(streamer_message)?;
    let block_type = serde_json::to_string(&block_type)?;

    let last_height = redis::cmd("GET")
        .arg(format!("{}_height", block_type))
        .query_async(&mut redis_client.clone())
        .await
        .unwrap_or(0);

    // If the block height is greater than the last height, update the block streamer message
    // if we have a few indexers running, we need to make sure that we are not updating the same block
    // or block which is already processed or block less than the last processed block
    if block_height > last_height {
        // Update the last block height
        // Create a clone of the redis client and redis cmd to avoid borrowing issues
        let mut redis_client_clone = redis_client.clone();
        let mut redis_set_cmd = redis::cmd("SET");
        let update_height_feature = redis_set_cmd
            .arg(format!("{}_height", block_type))
            .arg(block_height)
            .query_async::<redis::aio::ConnectionManager, String>(&mut redis_client_clone);

        // Update the block streamer message
        // Create a clone of the redis client and redis cmd to avoid borrowing issues
        let mut redis_client_clone = redis_client.clone();
        let mut redis_set_cmd = redis::cmd("SET");
        let update_stream_msg_feature =
            redis_set_cmd
                .arg(block_type)
                .arg(json_streamer_message)
                .query_async::<redis::aio::ConnectionManager, String>(&mut redis_client_clone);

        // Wait for both futures to complete
        futures::try_join!(update_height_feature, update_stream_msg_feature)?;
    };
    Ok(())
}

pub async fn optimistic_stream(
    view_client: actix::Addr<near_client::ViewClientActor>,
    client: actix::Addr<near_client::ClientActor>,
    redis_client: redis::aio::ConnectionManager,
) {
    tracing::info!(target: crate::INDEXER, "Starting Optimistic Streamer...");

    let mut optimistic_block_height: Option<u64> = None;
    loop {
        tokio::time::sleep(INTERVAL).await;

        // wait for node to be fully synced
        if let Ok(status) = fetch_status(&client).await {
            if status.sync_info.syncing {
                continue;
            }
        }

        if let Ok(block) = fetch_optimistic_block(&view_client).await {
            let height = block.header.height;
            if let Some(block_height) = optimistic_block_height {
                if height <= block_height {
                    continue;
                } else {
                    optimistic_block_height = Some(height);
                }
            } else {
                optimistic_block_height = Some(height);
            };
            let response = near_indexer::build_streamer_message(&view_client, block).await;
            match response {
                Ok(streamer_message) => {
                    tracing::debug!(target: crate::INDEXER, "Optimistic block {:?}", &optimistic_block_height);
                    if let Err(err) = update_block_streamer_message(
                        near_primitives::types::Finality::None,
                        &streamer_message,
                        redis_client.clone(),
                    )
                    .await
                    {
                        tracing::error!(
                            target: crate::INDEXER,
                            "Failed to publish optimistic block streamer message: {:#?}", err
                        );
                    };
                }
                Err(err) => {
                    tracing::error!(
                        target: crate::INDEXER,
                        "Missing data, skipping block #{}...", height
                    );
                    tracing::error!(target: crate::INDEXER, "{:#?}", err);
                }
            }
        };
    }
}
