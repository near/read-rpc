pub mod methods;

// Helper function to get the protocol version
pub(crate) async fn get_protocol_version(
    data: &actix_web::web::Data<crate::config::ServerContext>,
    block_reference: near_primitives::types::BlockReference,
    method_name: &str,
) -> anyhow::Result<near_primitives::types::ProtocolVersion> {
    match &block_reference {
        // return the current protocol version for all finality types
        near_primitives::types::BlockReference::Finality(_)
        | near_primitives::types::BlockReference::SyncCheckpoint(_) => Ok(data
            .blocks_info_by_finality
            .current_protocol_version()
            .await),
        // if the block is oldest then current epoch started, return the protocol version of the block
        // otherwise return the current protocol version
        near_primitives::types::BlockReference::BlockId(_) => {
            let epoch_start_height = data
                .blocks_info_by_finality
                .current_epoch_start_height()
                .await;
            let block = crate::modules::blocks::utils::fetch_block_from_cache_or_get(
                data,
                &block_reference,
                method_name,
            )
            .await?;
            if block.header.height < epoch_start_height {
                Ok(block.header.latest_protocol_version)
            } else {
                Ok(data
                    .blocks_info_by_finality
                    .current_protocol_version()
                    .await)
            }
        }
    }
}
