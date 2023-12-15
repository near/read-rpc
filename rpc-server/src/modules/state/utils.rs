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
    page_state: Option<String>,
) -> crate::modules::state::PageStateValues {
    tracing::debug!(
        "`get_state_keys_from_db_paginated` call. AccountId {}, block {}, page_state {:?}",
        account_id,
        block_height,
        page_state,
    );
    let mut data: HashMap<readnode_primitives::StateKey, readnode_primitives::StateValue> =
        HashMap::new();
    let mut new_page_state = None;
    let result = db_manager
        .get_state_keys_by_page(account_id, page_state)
        .await;
    if let Ok((state_keys, page_state)) = result {
        new_page_state = page_state;
        for state_keys_chunk in state_keys.chunks(1000) {
            // 3 nodes * 8 cpus * 100 = 2400
            // TODO: 2400 is hardcoded value. Make it configurable.
            let mut tasks = futures::stream::FuturesUnordered::new();
            for state_key in state_keys_chunk {
                let state_value_result_future =
                    db_manager.get_state_key_value(account_id, block_height, state_key.clone());
                tasks.push(state_value_result_future);
            }
            while let Some((state_key, state_value)) = tasks.next().await {
                if !state_value.is_empty() {
                    data.insert(state_key, state_value);
                }
            }
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
        next_page: new_page_state,
    }
}
