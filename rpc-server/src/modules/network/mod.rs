pub mod methods;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct StatusResponse {
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
