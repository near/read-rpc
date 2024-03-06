use near_o11y::WithSpanContextExt;

const INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);

/// Fetches the status to retrieve `latest_block_height` to determine if we need to fetch
/// entire block or we already fetched this block.
pub(crate) async fn fetch_latest_block(
    client: &actix::Addr<near_client::ViewClientActor>,
) -> anyhow::Result<u64> {
    let block = client
        .send(
            near_client::GetBlock(
                near_indexer::near_primitives::types::BlockReference::Finality(
                    near_indexer::near_primitives::types::Finality::Final,
                ),
            )
            .with_span_context(),
        )
        .await??;
    Ok(block.header.height)
}

pub(crate) async fn fetch_optimistic_block(
    client: &actix::Addr<near_client::ViewClientActor>,
) -> anyhow::Result<near_indexer::near_primitives::views::BlockView> {
    Ok(client
        .send(near_client::GetBlock::latest().with_span_context())
        .await??)
}

pub async fn optimistic_stream(view_client: actix::Addr<near_client::ViewClientActor>) {
    tracing::info!(target: crate::INDEXER, "Starting Optimistic Streamer...");

    loop {
        tokio::time::sleep(INTERVAL).await;
        let optimistic_feature = fetch_optimistic_block(&view_client);
        let finale_feature = fetch_latest_block(&view_client);
        let (finel_block, optimistic_block) = tokio::join!(finale_feature, optimistic_feature);
        if let Ok(block) = optimistic_block {
            let height = block.header.height;
            let response = near_indexer::build_streamer_message(&view_client, block).await;
            match response {
                Ok(streamer_message) => {
                    tracing::debug!(target: crate::INDEXER, "{:#?}", &streamer_message);
                    tracing::info!(target: crate::INDEXER, "Finale block {:#?}, Optimistic block {:#?}", finel_block.unwrap(), &streamer_message.block.header.height);
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
