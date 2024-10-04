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

/// This function starts a busy-loop that does the similar job to the near-indexer one.
/// However, this one deals with the blocks by provided finality, and instead of streaming them to
/// the client, it stores the block directly to the Redis instance shared between
/// ReadRPC components.
pub async fn update_block_in_redis_by_finality(
    view_client: actix::Addr<near_client::ViewClientActor>,
    client: actix::Addr<near_client::ClientActor>,
    finality_blocks_storage: cache_storage::BlocksByFinalityCache,
    finality: near_primitives::types::Finality,
) {
    let block_type = serde_json::to_string(&finality).unwrap();
    tracing::info!(target: crate::INDEXER, "Starting [{}] block update job...", block_type);

    let mut last_stored_block_height: Option<near_primitives::types::BlockHeight> = None;
    loop {
        tokio::time::sleep(INTERVAL).await;

        if let Ok(status) = fetch_status(&client).await {
            // Update protocol version in Redis
            // This is need for read-rpc to know the current protocol version
            if let Err(err) = finality_blocks_storage
                .update_protocol_version(status.protocol_version)
                .await
            {
                tracing::error!(
                    target: crate::INDEXER,
                    "Failed to update protocol version in Redis: {:?}", err
                );
            };

            // If the node is not fully synced the optimistic blocks are outdated
            // and are useless for our case. To avoid any misleading in our Redis
            // we don't update blocks until the node is fully synced.
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
                    if let Err(err) = finality_blocks_storage
                        .update_block_by_finality(
                            near_primitives::types::Finality::None,
                            &streamer_message,
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
