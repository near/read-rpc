use std::collections::HashMap;

use futures::StreamExt;

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
    if let Ok(result) = db_manager
        .get_state_key_value(account_id, block_height, key_data.clone(), method_name)
        .await
    {
        result
    } else {
        (key_data, readnode_primitives::StateValue::default())
    }
}

// Function to get state keys from the database
// This function to wrap the database call to get state keys
// It is using for debug in jaeger tracing
#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(db_manager))
)]
pub async fn get_state_keys_from_db(
    db_manager: &std::sync::Arc<Box<dyn database::ReaderDbManager + Sync + Send + 'static>>,
    account_id: &near_primitives::types::AccountId,
    prefix: &[u8],
    method_name: &str,
) -> anyhow::Result<Vec<readnode_primitives::StateKey>> {
    if !prefix.is_empty() {
        db_manager
            .get_state_keys_by_prefix(account_id, prefix, method_name)
            .await
    } else {
        db_manager.get_state_keys_all(account_id, method_name).await
    }
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
) -> HashMap<readnode_primitives::StateKey, readnode_primitives::StateValue> {
    tracing::debug!(
        "`get_state_from_db` call. AccountId {}, block {}, prefix {:?}",
        account_id,
        block_height,
        prefix,
    );
    let mut data: HashMap<readnode_primitives::StateKey, readnode_primitives::StateValue> =
        HashMap::new();

    match get_state_keys_from_db(db_manager, account_id, prefix, method_name).await {
        Ok(state_keys) => {
            // 3 nodes * 8 cpus * 100 = 2400
            // TODO: 2400 is hardcoded value. Make it configurable.
            for state_keys_chunk in state_keys.chunks(2400) {
                let futures = state_keys_chunk.iter().map(|state_key| {
                    get_state_key_value_from_db(
                        db_manager,
                        account_id,
                        block_height,
                        state_key.clone(),
                        method_name,
                    )
                });
                let mut tasks = futures::stream::FuturesUnordered::from_iter(futures);
                while let Some((state_key, state_value)) = tasks.next().await {
                    if !state_value.is_empty() {
                        data.insert(state_key, state_value);
                    }
                }
            }
            data
        }
        Err(_) => data,
    }
}

#[cfg(feature = "account_access_keys")]
#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(db_manager))
)]
pub async fn fetch_list_access_keys_from_db(
    db_manager: &std::sync::Arc<Box<dyn database::ReaderDbManager + Sync + Send + 'static>>,
    account_id: &near_primitives::types::AccountId,
    block_height: near_primitives::types::BlockHeight,
) -> anyhow::Result<Vec<near_primitives::views::AccessKeyInfoView>> {
    tracing::debug!(
        "`fetch_list_access_keys_from_db` call. AccountID {}, block {}",
        account_id,
        block_height,
    );
    let account_keys = db_manager
        .get_account_access_keys(account_id, block_height, "query_view_access_key_list")
        .await?;
    let account_keys_view = account_keys
        .into_iter()
        .map(
            |(public_key_hex, access_key)| near_primitives::views::AccessKeyInfoView {
                public_key: borsh::from_slice::<near_crypto::PublicKey>(
                    &hex::decode(public_key_hex).unwrap(),
                )
                .unwrap(),
                access_key: near_primitives::views::AccessKeyView::from(
                    borsh::from_slice::<near_primitives::account::AccessKey>(&access_key).unwrap(),
                ),
            },
        )
        .collect();
    Ok(account_keys_view)
}
