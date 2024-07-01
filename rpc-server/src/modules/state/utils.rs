#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(db_manager))
)]
pub async fn get_state_from_db_paginated(
    db_manager: &std::sync::Arc<Box<dyn database::ReaderDbManager + Sync + Send + 'static>>,
    account_id: &near_primitives::types::AccountId,
    block_height: near_primitives::types::BlockHeight,
    page_token: database::PageToken,
) -> crate::modules::state::PageStateValues {
    tracing::debug!(
        "`get_state_from_db_paginated` call. AccountId {}, block {}, page_token {:?}",
        account_id,
        block_height,
        page_token,
    );
    if let Ok((values, next_page_token)) = db_manager
        .get_state_by_page(account_id, block_height, page_token, "view_state_paginated")
        .await
    {
        crate::modules::state::PageStateValues {
            values,
            next_page_token,
        }
    } else {
        crate::modules::state::PageStateValues::default()
    }
}
