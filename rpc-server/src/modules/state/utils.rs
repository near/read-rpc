use crate::{config::ServerContext, modules::queries::methods::database_view_state};
use actix_web::web::Data;

#[cfg_attr(feature = "tracing-instrumentation", tracing::instrument(skip(data)))]
pub async fn get_state_from_db_paginated(
    data: &Data<ServerContext>,
    account_id: &near_primitives::types::AccountId,
    block: &near_primitives::views::BlockView,
    page_token: database::PageToken,
) -> Result<crate::modules::state::PageStateValues, near_jsonrpc::primitives::errors::RpcError> {
    tracing::debug!(
        "`get_state_from_db_paginated` call. AccountId {}, block {}, page_token {:?}",
        account_id,
        block.header.height,
        page_token,
    );

    let account = data
        .db_manager
        .get_account(account_id, block.header.height, "view_state_paginated")
        .await
        .map_err(
            |_err| near_jsonrpc::primitives::types::query::RpcQueryError::UnknownAccount {
                requested_account_id: account_id.clone(),
                block_height: block.header.height,
                block_hash: block.header.hash,
            },
        )?;

    // Calculate the state size excluding the contract code size to check if it's too large to fetch.
    // The state size is the storage usage minus the code size.
    // more details: nearcore/runtime/runtime/src/state_viewer/mod.rs:150
    let code_len = data
        .db_manager
        .get_contract_code(account_id, block.header.height, "view_state_paginated")
        .await
        .map(|code| code.data.len() as u64)
        .unwrap_or_default();

    let state_size = account.data.storage_usage().saturating_sub(code_len);
    let (values, next_page_token) = if state_size <= 1_000_000 {
        let values = database_view_state(data, block, account_id, &[]).await?;
        (values, None)
    } else {
        // If the state size is too large, we try to fetch the state in pages.
        // This is a fallback mechanism to avoid fetching too much data at once.
        let (raw_values, next_page_token) = data
            .db_manager
            .get_state_by_page(
                account_id,
                block.header.height,
                page_token,
                "view_state_paginated",
            )
            .await
            .map_err(|err| {
                near_jsonrpc::primitives::errors::RpcError::new_internal_error(
                    Some(serde_json::Value::String(err.to_string())),
                    "Failed to get page state from DB. Please try again!".to_string(),
                )
            })?;
        let values = raw_values
            .into_iter()
            .map(|(k, v)| near_primitives::views::StateItem {
                key: k.into(),
                value: v.into(),
            })
            .collect();
        (values, next_page_token)
    };

    Ok(crate::modules::state::PageStateValues {
        values,
        next_page_token,
    })
}
