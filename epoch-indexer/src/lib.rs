use near_indexer_primitives::{near_primitives, CryptoHash};

pub async fn get_epoch_validators(
    epoch_id: CryptoHash,
    client: &near_jsonrpc_client::JsonRpcClient,
) -> anyhow::Result<near_primitives::views::EpochValidatorInfo> {
    let mut attempt_counter = 0;
    loop {
        let params = near_jsonrpc_client::methods::validators::RpcValidatorRequest {
            epoch_reference: near_primitives::types::EpochReference::EpochId(
                near_primitives::types::EpochId(epoch_id),
            ),
        };
        match client.call(params).await {
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

/// util to fetch protocol config by epoch_start_height
/// try_another_blocks - special flag to try another blocks if we have error
/// in case of error we try to fetch protocol config for next block
pub async fn get_protocol_config(
    epoch_start_height: u64,
    client: &near_jsonrpc_client::JsonRpcClient,
    try_another_blocks: bool,
) -> anyhow::Result<near_chain_configs::ProtocolConfigView> {
    let mut attempt_counter = 0;
    let mut block_height = epoch_start_height;
    loop {
        let params =
            near_jsonrpc_client::methods::EXPERIMENTAL_protocol_config::RpcProtocolConfigRequest {
                block_reference: near_primitives::types::BlockReference::BlockId(
                    near_indexer_primitives::types::BlockId::Height(block_height),
                ),
            };
        match client.call(params).await {
            Ok(response) => return Ok(response),
            Err(e) => {
                attempt_counter += 1;
                if try_another_blocks {
                    block_height += 1;
                }
                tracing::debug!(
                    "Attempt: {}. Epoch_start_height {}. Error fetching protocol config: {:?}",
                    attempt_counter,
                    block_height,
                    e
                );
                if attempt_counter > 20 {
                    anyhow::bail!(
                        "Failed to fetch epoch protocol config for epoch_start_height: {}",
                        epoch_start_height
                    )
                }
            }
        }
    }
}

pub async fn get_epoch_info_by_id(
    epoch_id: CryptoHash,
    rpc_client: &near_jsonrpc_client::JsonRpcClient,
) -> anyhow::Result<readnode_primitives::IndexedEpochInfo> {
    let validators_info = get_epoch_validators(epoch_id, rpc_client).await?;

    let protocol_config =
        match get_protocol_config(validators_info.epoch_start_height, rpc_client, false).await {
            Ok(protocol_config) => protocol_config,
            Err(e) => {
                tracing::warn!(
                    "Error fetching protocol config: {:?}. Try with another blocks",
                    e
                );
                get_protocol_config(validators_info.epoch_start_height, rpc_client, true).await?
            }
        };

    Ok(readnode_primitives::IndexedEpochInfo {
        epoch_id,
        epoch_height: validators_info.epoch_height,
        epoch_start_height: validators_info.epoch_start_height,
        epoch_end_height: None,
        validators_info,
        protocol_config,
    })
}

pub async fn get_epoch_info_by_block_height(
    block_height: u64,
    s3_client: &near_lake_framework::s3_fetchers::LakeS3Client,
    s3_bucket_name: &str,
    rpc_client: &near_jsonrpc_client::JsonRpcClient,
) -> anyhow::Result<readnode_primitives::IndexedEpochInfoWithPreviousAndNextEpochId> {
    let block_heights = near_lake_framework::s3_fetchers::list_block_heights(
        s3_client,
        s3_bucket_name,
        block_height,
    )
    .await?;
    let block = near_lake_framework::s3_fetchers::fetch_block_or_retry(
        s3_client,
        s3_bucket_name,
        block_heights[0],
    )
    .await?;
    let epoch_info = get_epoch_info_by_id(block.header.epoch_id, rpc_client).await?;

    Ok(
        readnode_primitives::IndexedEpochInfoWithPreviousAndNextEpochId {
            previous_epoch_id: None,
            epoch_info,
            next_epoch_id: block.header.next_epoch_id,
        },
    )

}

pub async fn first_epoch(
    s3_client: &near_lake_framework::s3_fetchers::LakeS3Client,
    s3_bucket_name: &str,
    rpc_client: &near_jsonrpc_client::JsonRpcClient,
) -> anyhow::Result<readnode_primitives::IndexedEpochInfoWithPreviousAndNextEpochId> {
    let epoch_info = get_epoch_info_by_id(CryptoHash::default(), rpc_client).await?;
    let first_epoch_block = near_lake_framework::s3_fetchers::fetch_block_or_retry(
        s3_client,
        s3_bucket_name,
        epoch_info.epoch_start_height,
    )
        .await?;
    Ok(readnode_primitives::IndexedEpochInfoWithPreviousAndNextEpochId {
        previous_epoch_id: None,
        epoch_info,
        next_epoch_id: first_epoch_block.header.next_epoch_id,
    })
}

pub async fn get_next_epoch(
    current_epoch: &readnode_primitives::IndexedEpochInfoWithPreviousAndNextEpochId,
    s3_client: &near_lake_framework::s3_fetchers::LakeS3Client,
    s3_bucket_name: &str,
    rpc_client: &near_jsonrpc_client::JsonRpcClient,
) -> anyhow::Result<readnode_primitives::IndexedEpochInfoWithPreviousAndNextEpochId> {

    let mut epoch_info = get_epoch_info_by_id(current_epoch.next_epoch_id, rpc_client).await?;

    let epoch_info_first_block = near_lake_framework::s3_fetchers::fetch_block_or_retry(
        s3_client,
        s3_bucket_name,
        epoch_info.epoch_start_height,
    )
        .await?;
    if current_epoch.epoch_info.epoch_id == CryptoHash::default() {
        epoch_info.epoch_height = 1;
    } else {
        epoch_info.epoch_height = current_epoch.epoch_info.epoch_height + 1
    };
    Ok(
        readnode_primitives::IndexedEpochInfoWithPreviousAndNextEpochId {
            previous_epoch_id: Some(current_epoch.epoch_info.epoch_id),
            epoch_info,
            next_epoch_id: epoch_info_first_block.header.next_epoch_id,
        },
    )
}

pub async fn update_epoch_end_height(
    db_manager: &(impl database::StateIndexerDbManager + Sync + Send + 'static),
    epoch_id: Option<CryptoHash>,
    epoch_end_block_hash: CryptoHash,
) -> anyhow::Result<()> {
    if let Some(epoch_id) = epoch_id {
        tracing::info!(
            "Update epoch_end_height: epoch_id: {:?}, epoch_end_height: {}",
            epoch_id,
            epoch_end_block_hash
        );
        db_manager
            .update_epoch_end_height(epoch_id, epoch_end_block_hash)
            .await?;
    }
    Ok(())
}

pub async fn save_epoch_info(
    epoch: &readnode_primitives::IndexedEpochInfo,
    db_manager: &(impl database::StateIndexerDbManager + Sync + Send + 'static),
    handled_epoch_height: Option<u64>,
) -> anyhow::Result<()> {
    let epoch_height = if let Some(epoch_height) = handled_epoch_height {
        epoch_height
    } else {
        epoch.epoch_height
    };

    let save_validators_feature = db_manager.add_validators(
        epoch.epoch_id,
        epoch_height,
        epoch.epoch_start_height,
        &epoch.validators_info,
    );

    let save_protocol_config_feature = db_manager.add_protocol_config(
        epoch.epoch_id,
        epoch_height,
        epoch.epoch_start_height,
        &epoch.protocol_config,
    );

    futures::try_join!(save_validators_feature, save_protocol_config_feature)?;
    tracing::info!(
        "Save epoch info: epoch_id: {:?}, epoch_height: {:?}, epoch_start_height: {}",
        epoch.epoch_id,
        epoch.epoch_height,
        epoch.epoch_start_height,
    );
    Ok(())
}
