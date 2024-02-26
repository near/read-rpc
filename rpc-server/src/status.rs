use crate::config::ServerContext;
use actix_web::Responder;
use sysinfo::{System, SystemExt};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct RPCHealthStatusResponse {
    total_memory: String,
    used_memory: String,
    available_memory: String,

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

pub fn friendly_memory_size_format(memory_size_bytes: usize) -> String {
    if memory_size_bytes < 1024 {
        format!("{:.2} B", memory_size_bytes)
    } else if memory_size_bytes < 1024 * 1024 {
        format!("{:.2} KB", memory_size_bytes as f64 / 1024.0)
    } else if memory_size_bytes < 1024 * 1024 * 1024 {
        format!("{:.2} MB", memory_size_bytes as f64 / 1024.0 / 1024.0)
    } else {
        format!(
            "{:.2} GB",
            memory_size_bytes as f64 / 1024.0 / 1024.0 / 1024.0
        )
    }
}

/// Rpc server status
#[actix_web::get("/health")]
pub(crate) async fn get_health_status(data: actix_web::web::Data<ServerContext>) -> impl Responder {
    let sys = System::new_all();
    let total_memory = sys.total_memory();
    let used_memory = sys.used_memory();
    let blocks_cache = data.blocks_cache.read().await;
    let contract_code_cache = data.contract_code_cache.read().await;
    let compiled_contract_code_cache = data.compiled_contract_code_cache.local_cache.read().await;
    let status = RPCHealthStatusResponse {
        total_memory: friendly_memory_size_format(total_memory as usize),
        used_memory: friendly_memory_size_format(used_memory as usize),
        available_memory: friendly_memory_size_format((total_memory - used_memory) as usize),

        blocks_in_cache: blocks_cache.len(),
        max_blocks_cache_size: friendly_memory_size_format(blocks_cache.max_size()),
        current_blocks_cache_size: friendly_memory_size_format(blocks_cache.current_size()),

        contracts_codes_in_cache: contract_code_cache.len(),
        max_contracts_codes_cache_size: friendly_memory_size_format(contract_code_cache.max_size()),
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

        final_block_height: data
            .final_block_info
            .read()
            .await
            .final_block_cache
            .block_height,
    };
    actix_web::web::Json(status)
}
