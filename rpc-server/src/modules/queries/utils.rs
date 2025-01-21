use std::collections::HashMap;

// Function to get state key value from the database
// This function to wrap the database call to get state key value
// It is using for debug in jaeger tracing
#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(db_manager))
)]
pub async fn get_state_key_value_from_db(
    db_manager: &std::sync::Arc<Box<dyn database::ReaderDbManager + Sync + Send + 'static>>,
    account_id: &near_primitives::types::AccountId,
    block_height: near_primitives::types::BlockHeight,
    key_data: readnode_primitives::StateKey,
    method_name: &str,
) -> (
    readnode_primitives::StateKey,
    readnode_primitives::StateValue,
) {
    db_manager
        .get_state_key_value(account_id, block_height, key_data.clone(), method_name)
        .await
        .unwrap_or_else(|_| (key_data, readnode_primitives::StateValue::default()))
}

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(db_manager))
)]
pub async fn get_state_from_db(
    db_manager: &std::sync::Arc<Box<dyn database::ReaderDbManager + Sync + Send + 'static>>,
    account_id: &near_primitives::types::AccountId,
    block_height: near_primitives::types::BlockHeight,
    prefix: &[u8],
    method_name: &str,
) -> anyhow::Result<HashMap<readnode_primitives::StateKey, readnode_primitives::StateValue>> {
    tracing::debug!(
        "`get_state_from_db` call. AccountId {}, block {}, prefix {:?}",
        account_id,
        block_height,
        prefix,
    );
    db_manager
        .get_account_state(account_id, block_height, prefix, method_name)
        .await
}
