use near_indexer::near_primitives;
use near_o11y::WithSpanContextExt;

const INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

/// The universal function that fetches the block by the given finality.
/// It is used in the `update_block_in_redis_by_finality` function.
/// ! The function does not support the DoomSlug finality.
pub(crate) async fn fetch_block_by_finality(
    client: &actix::Addr<near_client::ViewClientActor>,
    finality: &near_primitives::types::Finality,
) -> anyhow::Result<near_primitives::views::BlockView> {
    let block_reference = near_primitives::types::BlockReference::Finality(finality.clone());
    Ok(client
        .send(near_client::GetBlock(block_reference).with_span_context())
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

/// Sets the keys in Redis shared between the ReadRPC components about the most recent
/// blocks based on finalities (final or optimistic).
/// `final_height` of `optimistic_height` depending on `block_type` passed.
/// Additionally, sets the JSON serialized `StreamerMessage` into keys `final` or `optimistic`
/// accordingly.
pub(crate) async fn update_block_streamer_message(
    finality: near_primitives::types::Finality,
    streamer_message: &near_indexer::StreamerMessage,
    redis_client: redis::aio::ConnectionManager,
) -> anyhow::Result<()> {
    let block_height = streamer_message.block.header.height;
    let block_type = serde_json::to_string(&finality)?;

    let last_height = redis::cmd("GET")
        .arg(format!("{}_height", block_type))
        .query_async(&mut redis_client.clone())
        .await
        .unwrap_or(0);

    // If the block height is greater than the last height, update the block streamer message
    // if we have a few indexers running, we need to make sure that we are not updating the same block
    // or block which is already processed or block less than the last processed block
    if block_height > last_height {
        let json_streamer_message = serde_json::to_string(streamer_message)?;
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

/// This function starts a busy-loop that does the similar job to the near-indexer one.
/// However, this one deals with the blocks by provided finality, and instead of streaming them to
/// the client, it stores the block directly to the Redis instance shared between
/// ReadRPC components.
pub async fn update_block_in_redis_by_finality(
    view_client: actix::Addr<near_client::ViewClientActor>,
    client: actix::Addr<near_client::ClientActor>,
    redis_client: redis::aio::ConnectionManager,
    finality: near_primitives::types::Finality,
) {
    let block_type = serde_json::to_string(&finality).unwrap();
    tracing::info!(target: crate::INDEXER, "Starting [{}] block update job...", block_type);

    let mut last_stored_block_height: Option<u64> = None;
    loop {
        tokio::time::sleep(INTERVAL).await;

        // If the node is not fully synced the optimistic blocks are outdated
        // and are useless for our case. To avoid any misleading in our Redis
        // we don't update blocks until the node is fully synced.
        if let Ok(status) = fetch_status(&client).await {
            if status.sync_info.syncing {
                continue;
            }
        }

        if let Ok(block) = fetch_block_by_finality(&view_client, &finality).await {
            let height = block.header.height;
            if let Some(block_height) = last_stored_block_height {
                if height <= block_height {
                    continue;
                } else {
                    last_stored_block_height = Some(height);
                }
            } else {
                last_stored_block_height = Some(height);
            };
            let response = near_indexer::build_streamer_message(&view_client, block).await;
            match response {
                Ok(streamer_message) => {
                    tracing::debug!(target: crate::INDEXER, "[{}] block {:?}", block_type, last_stored_block_height);
                    if let Err(err) = update_block_streamer_message(
                        near_primitives::types::Finality::None,
                        &streamer_message,
                        redis_client.clone(),
                    )
                    .await
                    {
                        tracing::error!(
                            target: crate::INDEXER,
                            "Failed to publish [{}] block streamer message: {:?}", block_type, err
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
