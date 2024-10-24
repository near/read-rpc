#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(db_manager))
)]
pub async fn get_state_from_db_paginated(
    db_manager: &std::sync::Arc<Box<dyn database::ReaderDbManager + Sync + Send + 'static>>,
    account_id: &near_primitives::types::AccountId,
    block_height: near_primitives::types::BlockHeight,
    page_token: database::PageToken,
) -> Result<crate::modules::state::PageStateValues, near_jsonrpc::primitives::errors::RpcError> {
    tracing::debug!(
        "`get_state_from_db_paginated` call. AccountId {}, block {}, page_token {:?}",
        account_id,
        block_height,
        page_token,
    );

    let (values, next_page_token) = db_manager
        .get_state_by_page(account_id, block_height, page_token, "view_state_paginated")
        .await
        .map_err(|err| {
            near_jsonrpc::primitives::errors::RpcError::new_internal_error(
                Some(serde_json::Value::String(err.to_string())),
                "Failed to get page state from DB. Please try again!".to_string(),
            )
        })?;
    Ok(crate::modules::state::PageStateValues {
        values: values
            .into_iter()
            .map(|(k, v)| near_primitives::views::StateItem {
                key: k.into(),
                value: v.into(),
            })
            .collect(),
        next_page_token,
    })
}
