use crate::config::ServerContext;
use crate::utils::friendly_memory_size_format;
use actix_web::Responder;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct RPCHealthStatusResponse {
    blocks_in_cache: usize,
    max_blocks_cache_size: String,
    current_blocks_cache_size: String,

    contracts_codes_in_cache: usize,
    max_contracts_codes_cache_size: String,
    current_contracts_codes_cache_size: String,

    compiled_contracts_codes_in_cache: usize,
    max_compiled_contracts_codes_cache_size: String,
    current_compiled_contracts_codes_cache_size: String,

    final_block_height: u64,
}

impl RPCHealthStatusResponse {
    pub async fn new(server_context: &ServerContext) -> Self {
        let blocks_cache = server_context.blocks_cache.read().await;
        let contract_code_cache = server_context.contract_code_cache.read().await;
        let compiled_contract_code_cache = server_context
            .compiled_contract_code_cache
            .local_cache
            .read()
            .await;
        Self {
            blocks_in_cache: blocks_cache.len(),
            max_blocks_cache_size: friendly_memory_size_format(blocks_cache.max_size()),
            current_blocks_cache_size: friendly_memory_size_format(blocks_cache.current_size()),

            contracts_codes_in_cache: contract_code_cache.len(),
            max_contracts_codes_cache_size: friendly_memory_size_format(
                contract_code_cache.max_size(),
            ),
            current_contracts_codes_cache_size: friendly_memory_size_format(
                contract_code_cache.current_size(),
            ),

            compiled_contracts_codes_in_cache: compiled_contract_code_cache.len(),
            max_compiled_contracts_codes_cache_size: friendly_memory_size_format(
                compiled_contract_code_cache.max_size(),
            ),
            current_compiled_contracts_codes_cache_size: friendly_memory_size_format(
                compiled_contract_code_cache.current_size(),
            ),

            final_block_height: server_context
                .final_block_info
                .read()
                .await
                .final_block_cache
                .block_height,
        }
    }
}

/// Rpc server status
#[actix_web::get("/health")]
pub(crate) async fn get_health_status() -> impl Responder {
    actix_web::web::Json(serde_json::json!({"status": "ok"}))
}
