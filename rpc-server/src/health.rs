use crate::config::ServerContext;
use crate::utils::friendly_memory_size_format;
use actix_web::Responder;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct RPCHealthStatusResponse {
    blocks_in_cache: usize,
    max_blocks_cache_size: String,
    current_blocks_cache_size: String,

    chunks_in_cache: usize,
    max_chunks_cache_size: String,
    current_chunks_cache_size: String,

    contracts_codes_in_cache: usize,
    max_contracts_codes_cache_size: String,
    current_contracts_codes_cache_size: String,

    compiled_codes_in_cache: usize,
    max_compiled_codes_cache_size: String,
    current_compiled_codes_cache_size: String,

    final_block_height: u64,
}

impl RPCHealthStatusResponse {
    pub async fn new(server_context: &ServerContext) -> Self {
        Self {
            blocks_in_cache: server_context.blocks_cache.len().await,
            max_blocks_cache_size: friendly_memory_size_format(
                server_context.blocks_cache.max_size().await,
            ),
            current_blocks_cache_size: friendly_memory_size_format(
                server_context.blocks_cache.current_size().await,
            ),

            chunks_in_cache: server_context.chunks_cache.len().await,
            max_chunks_cache_size: friendly_memory_size_format(
                server_context.chunks_cache.max_size().await,
            ),
            current_chunks_cache_size: friendly_memory_size_format(
                server_context.chunks_cache.current_size().await,
            ),

            contracts_codes_in_cache: server_context.contract_code_cache.len().await,
            max_contracts_codes_cache_size: friendly_memory_size_format(
                server_context.contract_code_cache.max_size().await,
            ),
            current_contracts_codes_cache_size: friendly_memory_size_format(
                server_context.contract_code_cache.current_size().await,
            ),

            compiled_codes_in_cache: server_context
                .compiled_contract_code_cache
                .local_cache
                .len()
                .await,
            max_compiled_codes_cache_size: friendly_memory_size_format(
                server_context
                    .compiled_contract_code_cache
                    .local_cache
                    .max_size()
                    .await,
            ),
            current_compiled_codes_cache_size: friendly_memory_size_format(
                server_context
                    .compiled_contract_code_cache
                    .local_cache
                    .current_size()
                    .await,
            ),

            final_block_height: server_context
                .blocks_info_by_finality
                .final_block_view()
                .await
                .header
                .height,
        }
    }
}

/// Rpc server status
#[actix_web::get("/health")]
pub(crate) async fn get_health_status() -> impl Responder {
    actix_web::web::Json(serde_json::json!({"status": "ok"}))
}
