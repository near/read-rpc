use crate::config::ServerContext;
use crate::errors::RPCError;
use crate::modules::blocks::utils::fetch_block_from_cache_or_get;
use crate::modules::state::utils::get_state_from_db_paginated;

use actix_web::web::Data;

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn view_state_paginated(
    data: Data<ServerContext>,
    request_data: crate::modules::state::RpcViewStatePaginatedRequest,
) -> Result<crate::modules::state::RpcViewStatePaginatedResponse, RPCError> {
    let block_reference =
        near_primitives::types::BlockReference::BlockId(request_data.block_id.clone());
    let block = fetch_block_from_cache_or_get(&data, &block_reference, "view_state_paginated")
        .await
        .map_err(near_jsonrpc::primitives::errors::RpcError::from)?;

    let state_values = get_state_from_db_paginated(
        &data.db_manager,
        &request_data.account_id,
        block.block_height,
        request_data.next_page_token,
    )
    .await;

    Ok(crate::modules::state::RpcViewStatePaginatedResponse {
        values: state_values.values,
        next_page_token: state_values.next_page_token,
        block_height: block.block_height,
        block_hash: block.block_hash,
    })
}
