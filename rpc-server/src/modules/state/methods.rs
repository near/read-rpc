use crate::config::ServerContext;
use crate::errors::RPCError;
use crate::modules::blocks::utils::fetch_block_from_cache_or_get;
use crate::modules::state::utils::get_state_keys_from_db_paginated;
use jsonrpc_v2::{Data, Params};

pub async fn view_state_paginated(
    data: Data<ServerContext>,
    Params(params): Params<crate::modules::state::RpcViewStatePaginatedRequest>,
) -> Result<crate::modules::state::RpcViewStatePaginatedResponse, RPCError> {
    let block_reference = near_primitives::types::BlockReference::BlockId(params.block_id.clone());
    let block = fetch_block_from_cache_or_get(&data, block_reference)
        .await
        .map_err(near_jsonrpc_primitives::errors::RpcError::from)?;

    let state_value = get_state_keys_from_db_paginated(
        &data.db_manager,
        &params.account_id,
        block.block_height,
        params.next_page,
    )
    .await;

    Ok(crate::modules::state::RpcViewStatePaginatedResponse {
        values: state_value.values,
        next_page: state_value.next_page,
        block_height: block.block_height,
        block_hash: block.block_hash,
    })
}
