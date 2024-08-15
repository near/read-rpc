use near_indexer_primitives::{near_primitives, CryptoHash};

use tokio_retry::{strategy::FixedInterval, Retry};

const SAVE_ATTEMPTS: usize = 20;

pub async fn get_epoch_validators(
    epoch_id: CryptoHash,
    near_client: &impl crate::NearClient,
) -> anyhow::Result<near_primitives::views::EpochValidatorInfo> {
    let retry_strategy = FixedInterval::from_millis(500).take(SAVE_ATTEMPTS);

    let result = Retry::spawn(retry_strategy, || async {
        near_client
            .validators_by_epoch_id(epoch_id)
            .await
            .map_err(|e| {
                tracing::debug!(
                    "Error fetching epoch validators for epoch_id {}: {:?}",
                    epoch_id,
                    e
                );
                e
            })
    })
    .await;

    result.map_err(|e| {
        anyhow::anyhow!(
            "Failed to fetch epoch validators for epoch_id {}: {}",
            epoch_id,
            e
        )
    })
}

pub async fn get_epoch_info_by_id(
    epoch_id: CryptoHash,
    near_client: &impl crate::NearClient,
) -> anyhow::Result<readnode_primitives::IndexedEpochInfo> {
    let validators_info = get_epoch_validators(epoch_id, near_client).await?;
    Ok(readnode_primitives::IndexedEpochInfo {
        epoch_id,
        epoch_height: validators_info.epoch_height,
        epoch_start_height: validators_info.epoch_start_height,
        validators_info,
    })
}

pub async fn save_epoch_info(
    epoch: &readnode_primitives::IndexedEpochInfo,
    db_manager: &(impl database::StateIndexerDbManager + Sync + Send + 'static),
    handled_epoch_height: Option<u64>,
    epoch_end_block_hash: CryptoHash,
) -> anyhow::Result<()> {
    let epoch_height = if let Some(epoch_height) = handled_epoch_height {
        epoch_height
    } else {
        epoch.epoch_height
    };

    db_manager
        .save_validators(
            epoch.epoch_id,
            epoch_height,
            epoch.epoch_start_height,
            &epoch.validators_info,
            epoch_end_block_hash,
        )
        .await?;

    tracing::info!(
        "Save epoch info: epoch_id: {:?}, epoch_height: {:?}, epoch_start_height: {}",
        epoch.epoch_id,
        epoch_height,
        epoch.epoch_start_height,
    );
    Ok(())
}
