use near_indexer_primitives::{near_primitives, CryptoHash};

pub async fn get_epoch_validators(
    epoch_id: CryptoHash,
    near_client: &impl crate::NearClient,
) -> anyhow::Result<near_primitives::views::EpochValidatorInfo> {
    let mut attempt_counter = 0;
    loop {
        match near_client.validators_by_epoch_id(epoch_id).await {
            Ok(response) => return Ok(response),
            Err(e) => {
                attempt_counter += 1;
                tracing::debug!(
                    "Attempt: {}.Epoch_id: {}. Error fetching epoch validators: {:?}",
                    attempt_counter,
                    epoch_id,
                    e
                );
                if attempt_counter > 20 {
                    anyhow::bail!("Failed to fetch epoch validators. Epoch_id: {}", epoch_id)
                }
            }
        }
    }
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
