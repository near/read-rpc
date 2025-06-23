use crate::config::ServerContext;
use actix_web::web::Data;

pub mod methods;

pub(crate) async fn try_get_transaction_details_by_hash(
    data: &Data<ServerContext>,
    tx_hash: &near_indexer_primitives::CryptoHash,
    shard_id: &near_primitives::types::ShardId,
) -> anyhow::Result<readnode_primitives::TransactionDetails> {
    if let Ok(transaction_details_bytes) = &data
        .tx_details_storage
        .retrieve_tx(&tx_hash.to_string(), shard_id)
        .await
    {
        readnode_primitives::TransactionDetails::tx_deserialize(transaction_details_bytes)
    } else if let Some(tx_cache_storage) = data.tx_cache_storage.clone() {
        Ok(tx_cache_storage.get_tx_by_tx_hash(tx_hash).await?)
    } else {
        anyhow::bail!("Transaction not found")
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct FunctionCallOutcome {
    pub balance: near_primitives::types::Balance,
    pub storage_usage: near_primitives::types::StorageUsage,
    pub return_data: Option<Vec<u8>>,
    pub burnt_gas: near_primitives::types::Gas,
    pub used_gas: near_primitives::types::Gas,
    pub compute_usage: near_primitives::types::Compute,
    pub logs: Vec<String>,
    /// Data collected from making a contract call
    pub profile: ProfileData,
    pub aborted: Option<near_jsonrpc::primitives::types::query::RpcQueryError>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct ProfileData {
    /// Gas spent on sending or executing actions.
    pub actions_profile: std::collections::HashMap<String, near_primitives::types::Gas>,
    /// Non-action gas spent outside the WASM VM while executing a contract.
    pub wasm_ext_profile: std::collections::HashMap<String, near_primitives::types::Gas>,
    /// Gas spent on execution inside the WASM VM.
    pub wasm_gas: near_primitives::types::Gas,
}

impl From<near_vm_runner::ProfileDataV3> for ProfileData {
    fn from(profile: near_vm_runner::ProfileDataV3) -> Self {
        ProfileData {
            actions_profile: profile
                .actions_profile
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
            wasm_ext_profile: profile
                .wasm_ext_profile
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
            wasm_gas: profile.wasm_gas,
        }
    }
}

impl From<crate::modules::queries::contract_runner::RunContractResponse> for FunctionCallOutcome {
    fn from(
        contract_response: crate::modules::queries::contract_runner::RunContractResponse,
    ) -> Self {
        FunctionCallOutcome {
            balance: contract_response.result.balance,
            storage_usage: contract_response.result.storage_usage,
            return_data: contract_response.result.return_data.as_value(),
            burnt_gas: contract_response.result.burnt_gas,
            used_gas: contract_response.result.used_gas,
            compute_usage: contract_response.result.compute_usage,
            logs: contract_response.result.logs,
            profile: contract_response.result.profile.into(),
            aborted: contract_response.result.aborted.map(|err| {
                let message = format!("wasm execution failed with error: {:?}", err);
                near_jsonrpc::primitives::types::query::RpcQueryError::ContractExecutionError {
                    vm_error: message,
                    block_height: contract_response.block_height,
                    block_hash: contract_response.block_hash,
                }
            }),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub enum EmulateTransactionResponse {
    FunctionCall(FunctionCallOutcome),
}

impl near_jsonrpc_client::methods::RpcHandlerResponse for EmulateTransactionResponse {}
