use crate::config::CompiledCodeCache;
use crate::modules::queries::{
    CodeStorage, ACCESS_KEY_SCOPE, ACCOUNT_SCOPE, CODE_SCOPE, DATA_SCOPE, MAX_LIMIT,
};
use borsh::{BorshDeserialize, BorshSerialize};
use scylla::IntoTypedRows;
use std::collections::HashMap;
use std::ops::Deref;
use tokio::task;

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(scylla_db_client))
)]
pub async fn fetch_data_from_scylla_db(
    scope: &str,
    scylla_db_client: &std::sync::Arc<scylla::Session>,
    account_id: &near_primitives::types::AccountId,
    block_height: near_primitives::types::BlockHeight,
    key_data: Option<Vec<u8>>,
) -> anyhow::Result<Vec<u8>> {
    let mut query_str = format!(
        "SELECT data_value FROM state_changes_{scope} WHERE account_id = ? AND block_height <= ? "
    );
    let result = match key_data {
        Some(data_key) => {
            query_str.push_str("AND data_key = ? LIMIT 1");
            scylla_db_client
                .query(
                    query_str,
                    (
                        account_id.to_string(),
                        num_bigint::BigInt::from(block_height),
                        hex::encode(&data_key).to_string(),
                    ),
                )
                .await?
                .single_row()
        }
        None => {
            query_str.push_str("LIMIT 1");
            scylla_db_client
                .query(
                    query_str,
                    (
                        account_id.to_string(),
                        num_bigint::BigInt::from(block_height),
                    ),
                )
                .await?
                .single_row()
        }
    };
    if let Ok(row) = result {
        let (data_value,): (Vec<u8>,) = row.into_typed::<(Vec<u8>,)>()?;
        Ok(data_value)
    } else {
        Ok(vec![])
    }
}

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(scylla_db_client))
)]
pub async fn get_stata_keys_from_scylla(
    scope: &str,
    scylla_db_client: &std::sync::Arc<scylla::Session>,
    account_id: &near_primitives::types::AccountId,
    block_height: near_primitives::types::BlockHeight,
    prefix: &[u8],
) -> HashMap<Vec<u8>, Vec<u8>> {
    tracing::debug!(
        "`get_stata_keys_from_scylla` call. AccountId {}, block {}, prefix {:?}",
        account_id,
        block_height,
        prefix,
    );
    let mut data: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let result = {
        if !prefix.is_empty() {
            let hex_str_prefix = hex::encode(prefix);
            scylla_db_client
                .query(
                    "SELECT data_key FROM account_state WHERE account_id = ? AND data_key LIKE ?",
                    (
                        account_id.to_string(),
                        format!("{hex_str_prefix}%").to_string()
                    ),
                )
                .await
                .expect("Invalid query into `account_state` table")
                .rows
        } else {
            scylla_db_client
                .query(
                    "SELECT data_key FROM account_state WHERE account_id = ?",
                    (account_id.to_string(),),
                )
                .await
                .expect("Invalid query into `account_state` table")
                .rows
        }
    };
    match result {
        Some(rows) => {
            for row in rows.into_typed::<(String,)>() {
                let (hex_data_key,): (String,) = row.expect("Invalid data");
                let data_key = hex::decode(hex_data_key).unwrap();
                let data_value = fetch_data_from_scylla_db(
                    scope,
                    scylla_db_client,
                    account_id,
                    block_height,
                    Some(data_key.clone()),
                )
                .await
                .expect("Invalid data");
                if !data_value.is_empty() {
                    data.insert(data_key, data_value);
                }

                let keys_count = data.keys().len() as u8;
                if keys_count > MAX_LIMIT {
                    return data;
                }
            }
            data
        }
        None => data,
    }
}

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(scylla_db_client))
)]
pub async fn fetch_account_from_scylla_db(
    scylla_db_client: &std::sync::Arc<scylla::Session>,
    account_id: &near_primitives::types::AccountId,
    block_height: near_primitives::types::BlockHeight,
) -> anyhow::Result<near_primitives::account::Account> {
    tracing::debug!(
        "`fetch_account_from_scylla_db` call. AccountID {}, block {}",
        account_id,
        block_height,
    );

    let account_from_db = fetch_data_from_scylla_db(
        ACCOUNT_SCOPE,
        scylla_db_client,
        account_id,
        block_height,
        None,
    )
    .await?;
    Ok(near_primitives::account::Account::try_from_slice(
        &account_from_db,
    )?)
}

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(scylla_db_client))
)]
pub async fn fetch_contract_code_from_scylla_db(
    scylla_db_client: &std::sync::Arc<scylla::Session>,
    account_id: &near_primitives::types::AccountId,
    block_height: near_primitives::types::BlockHeight,
) -> anyhow::Result<Vec<u8>> {
    tracing::debug!(
        "`fetch_contract_code_from_scylla_db` call. AccountID {}, block {}",
        account_id,
        block_height,
    );
    let code_data_from_scylla_db =
        fetch_data_from_scylla_db(CODE_SCOPE, scylla_db_client, account_id, block_height, None)
            .await?;
    if code_data_from_scylla_db.is_empty() {
        anyhow::bail!("Contract code for {} on block height {} is not found in ScyllaDB", account_id, block_height)
    } else {
        Ok(code_data_from_scylla_db)
    }
}

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(scylla_db_client))
)]
pub async fn fetch_access_key_from_scylla_db(
    scylla_db_client: &std::sync::Arc<scylla::Session>,
    account_id: &near_primitives::types::AccountId,
    block_height: near_primitives::types::BlockHeight,
    key_data: Vec<u8>,
) -> anyhow::Result<near_primitives::account::AccessKey> {
    tracing::debug!(
        "`fetch_access_key_from_scylla_db` call. AccountID {}, block {}, key_data {:?}",
        account_id,
        block_height,
        key_data,
    );
    let access_key_from_scylla_db = fetch_data_from_scylla_db(
        ACCESS_KEY_SCOPE,
        scylla_db_client,
        account_id,
        block_height,
        Some(key_data),
    )
    .await?;
    Ok(near_primitives::account::AccessKey::try_from_slice(
        &access_key_from_scylla_db,
    )?)
}

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(scylla_db_client))
)]
pub async fn fetch_state_from_scylla_db(
    scylla_db_client: &std::sync::Arc<scylla::Session>,
    account_id: &near_primitives::types::AccountId,
    block_height: near_primitives::types::BlockHeight,
    prefix: &[u8],
) -> anyhow::Result<near_primitives::views::ViewStateResult> {
    tracing::debug!(
        "`fetch_state_from_scylla_db` call. AccountID {}, block {}, prefix {:?}",
        account_id,
        block_height,
        prefix,
    );
    let state_from_db = get_stata_keys_from_scylla(
        DATA_SCOPE,
        scylla_db_client,
        account_id,
        block_height,
        prefix,
    )
    .await;
    if state_from_db.is_empty() {
        anyhow::bail!("Data not found in db")
    } else {
        let mut values = Vec::new();
        for (key, value) in state_from_db.iter() {
            let state_item = near_primitives::views::StateItem {
                key: key.to_vec(),
                value: value.to_vec(),
                proof: vec![],
            };
            values.push(state_item)
        }
        Ok(near_primitives::views::ViewStateResult {
            values,
            proof: vec![],
        })
    }
}

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(
        scylla_db_client,
        context,
        contract_code,
        compiled_contract_code_cache
    ))
)]
async fn run_code_in_vm_runner(
    contract_code: near_primitives::contract::ContractCode,
    method_name: &str,
    context: near_vm_logic::VMContext,
    account_id: near_primitives::types::AccountId,
    block_height: near_primitives::types::BlockHeight,
    scylla_db_client: std::sync::Arc<scylla::Session>,
    latest_protocol_version: near_primitives::types::ProtocolVersion,
    compiled_contract_code_cache: &std::sync::Arc<CompiledCodeCache>,
) -> anyhow::Result<near_vm_logic::VMOutcome> {
    let contract_method_name = String::from(method_name);
    let mut external = CodeStorage::init(scylla_db_client.clone(), account_id, block_height);
    let code_cache = std::sync::Arc::clone(compiled_contract_code_cache);

    let results = task::spawn_blocking(move || {
        // We use our own cache to store the precompiled codes,
        // so we need to call the precompilation function manually.
        //
        // Precompiles contract for the current default VM, and stores result to the cache.
        // Returns `Ok(true)` if compiled code was added to the cache, and `Ok(false)` if element
        // is already in the cache, or if cache is `None`.
        near_vm_runner::precompile_contract(
            &contract_code,
            &near_vm_logic::VMConfig::test(),
            latest_protocol_version,
            Some(code_cache.deref()),
        )
        .ok();
        near_vm_runner::run(
            &contract_code,
            &contract_method_name,
            &mut external,
            context,
            &near_vm_logic::VMConfig::test(),
            &near_primitives::runtime::fees::RuntimeFeesConfig::test(),
            &[],
            latest_protocol_version,
            Some(code_cache.deref()),
        )
    })
    .await?;
    match results {
        near_vm_runner::VMResult::Ok(result) => Ok(result),
        near_vm_runner::VMResult::Aborted(output, err) => anyhow::bail!("Run contract abort!\n{:#?}\n{:#?}", output, err),
    }
}

#[cfg_attr(
    feature = "tracing-instrumentation",
    tracing::instrument(skip(scylla_db_client, compiled_contract_code_cache))
)]
pub async fn run_contract(
    account_id: near_primitives::types::AccountId,
    method_name: &str,
    args: near_primitives::types::FunctionArgs,
    scylla_db_client: std::sync::Arc<scylla::Session>,
    compiled_contract_code_cache: &std::sync::Arc<CompiledCodeCache>,
    contract_code_cache: &std::sync::Arc<
        std::sync::RwLock<lru::LruCache<near_primitives::hash::CryptoHash, Vec<u8>>>,
    >,
    block_height: near_primitives::types::BlockHeight,
    timestamp: u64,
    latest_protocol_version: near_primitives::types::ProtocolVersion,
) -> anyhow::Result<near_vm_logic::VMOutcome> {
    let contract =
        fetch_account_from_scylla_db(&scylla_db_client, &account_id, block_height).await?;
    let code: Option<Vec<u8>> = contract_code_cache
        .write()
        .unwrap()
        .get(&contract.code_hash())
        .cloned();
    let contract_code = match code {
        Some(code) => {
            near_primitives::contract::ContractCode::new(code, Some(contract.code_hash()))
        }
        None => {
            let code = fetch_contract_code_from_scylla_db(
                &scylla_db_client,
                &account_id,
                block_height,
            )
            .await?;
            contract_code_cache
                .write()
                .unwrap()
                .put(contract.code_hash(), code.clone());
            near_primitives::contract::ContractCode::new(code, Some(contract.code_hash()))
        }
    };
    let context = near_vm_logic::VMContext {
        current_account_id: account_id.parse().unwrap(),
        signer_account_id: account_id.parse().unwrap(),
        signer_account_pk: vec![],
        predecessor_account_id: account_id.parse().unwrap(),
        input: <near_primitives::types::FunctionArgs as AsRef<[u8]>>::as_ref(&args).to_vec(),
        block_height,
        block_timestamp: timestamp,
        epoch_height: 0, // TODO: implement indexing of epoch_height and pass it here
        account_balance: contract.amount(),
        account_locked_balance: contract.locked(),
        storage_usage: contract.storage_usage(),
        attached_deposit: 0,
        prepaid_gas: 0,
        random_seed: vec![], // TODO: test the contracts where random is used.
        view_config: Some(near_primitives::config::ViewConfig {
            max_gas_burnt: 300_000_000_000_000, // TODO: extract it into a configuration option
        }),
        output_data_receivers: vec![],
    };
    run_code_in_vm_runner(
        contract_code,
        method_name,
        context,
        account_id,
        block_height,
        scylla_db_client.clone(),
        latest_protocol_version,
        compiled_contract_code_cache,
    )
    .await
}
