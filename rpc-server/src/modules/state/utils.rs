use futures::StreamExt;
use std::collections::HashMap;

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(db_manager))
)]
pub async fn get_state_keys_from_db_paginated(
    db_manager: &std::sync::Arc<Box<dyn database::ReaderDbManager + Sync + Send + 'static>>,
    account_id: &near_primitives::types::AccountId,
    block_height: near_primitives::types::BlockHeight,
    page_token: database::PageToken,
) -> crate::modules::state::PageStateValues {
    tracing::debug!(
        "`get_state_keys_from_db_paginated` call. AccountId {}, block {}, page_token {:?}",
        account_id,
        block_height,
        page_token,
    );
    if let Ok((state_keys, next_page_token)) = db_manager
        .get_state_keys_by_page(account_id, page_token)
        .await
    {
        let futures = state_keys.iter().map(|state_key| {
            db_manager.get_state_key_value(account_id, block_height, state_key.clone())
        });
        let mut tasks = futures::stream::FuturesUnordered::from_iter(futures);
        let mut data: HashMap<readnode_primitives::StateKey, readnode_primitives::StateValue> =
            HashMap::new();
        while let Some((state_key, state_value)) = tasks.next().await {
            if !state_value.is_empty() {
                data.insert(state_key, state_value);
            }
        }
        let values = data
            .into_iter()
            .map(|(key, value)| near_primitives::views::StateItem {
                key: key.into(),
                value: value.into(),
            })
            .collect();
        crate::modules::state::PageStateValues {
            values,
            next_page_token,
        }
    } else {
        crate::modules::state::PageStateValues::default()
    }
}
