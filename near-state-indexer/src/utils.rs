use near_o11y::WithSpanContextExt;

pub(crate) async fn fetch_epoch_validators_info(
    epoch_id: near_indexer::near_primitives::hash::CryptoHash,
    client: &actix::Addr<near_client::ViewClientActor>,
) -> anyhow::Result<near_indexer::near_primitives::views::EpochValidatorInfo> {
    Ok(client
        .send(
            near_client::GetValidatorInfo {
                epoch_reference: near_indexer::near_primitives::types::EpochReference::EpochId(
                    near_indexer::near_primitives::types::EpochId(epoch_id),
                ),
            }
            .with_span_context(),
        )
        .await??)
}

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
    let mut optimistic_block_height: Option<u64> = None;
    loop {
        tokio::time::sleep(INTERVAL).await;
        if let Ok(block) = fetch_optimistic_block(&view_client).await {
            let height = block.header.height;
            if let Some(block_height) = optimistic_block_height {
                if height <= block_height {
                    continue;
                } else {
                    optimistic_block_height = Some(height);
                }
            };
            let response = near_indexer::build_streamer_message(&view_client, block).await;
            match response {
                Ok(streamer_message) => {
                    // TODO: implement handling of streamer message for optimistic block
                    tracing::info!(target: crate::INDEXER, "Optimistic block {:#?}", &streamer_message.block.header.height);
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
