use crate::configs::deserialize_data_or_env;
use serde_derive::Deserialize;

#[derive(Deserialize, Debug, Clone, Default)]
pub struct GeneralConfig {
    #[serde(deserialize_with = "deserialize_data_or_env")]
    pub chain_id: ChainId,
    #[serde(deserialize_with = "deserialize_data_or_env")]
    pub near_rpc_url: String,
    #[serde(deserialize_with = "deserialize_data_or_env", default)]
    pub near_archival_rpc_url: Option<String>,
    #[serde(default)]
    pub rpc_server: GeneralRpcServerConfig,
    #[serde(default)]
    pub tx_indexer: GeneralTxIndexerConfig,
    #[serde(default)]
    pub state_indexer: GeneralStateIndexerConfig,
    #[serde(default)]
    pub epoch_indexer: GeneralEpochIndexerConfig,
}

#[derive(Deserialize, PartialEq, Debug, Clone, Default)]
#[serde(rename_all = "lowercase")]
pub enum ChainId {
    #[default]
    Mainnet,
    Testnet,
}

#[derive(Deserialize, Debug, Clone)]
pub struct GeneralRpcServerConfig {
    #[serde(
        deserialize_with = "deserialize_data_or_env",
        default = "GeneralRpcServerConfig::default_referer_header_value"
    )]
    pub referer_header_value: String,
    #[serde(
        deserialize_with = "deserialize_data_or_env",
        default = "GeneralRpcServerConfig::default_server_port"
    )]
    pub server_port: u16,
    #[serde(
        deserialize_with = "deserialize_data_or_env",
        default = "GeneralRpcServerConfig::default_max_gas_burnt"
    )]
    pub max_gas_burnt: u64,
    #[serde(deserialize_with = "deserialize_data_or_env", default)]
    pub limit_memory_cache: Option<f64>,
    #[serde(
        deserialize_with = "deserialize_data_or_env",
        default = "GeneralRpcServerConfig::default_reserved_memory"
    )]
    pub reserved_memory: f64,
    #[serde(
        deserialize_with = "deserialize_data_or_env",
        default = "GeneralRpcServerConfig::default_block_cache_size"
    )]
    pub block_cache_size: f64,
}

impl GeneralRpcServerConfig {
    pub fn default_referer_header_value() -> String {
        "read-rpc".to_string()
    }

    pub fn default_server_port() -> u16 {
        8080
    }

    pub fn default_max_gas_burnt() -> u64 {
        300_000_000_000_000
    }

    pub fn default_reserved_memory() -> f64 {
        0.25
    }

    pub fn default_block_cache_size() -> f64 {
        0.125
    }
}

impl Default for GeneralRpcServerConfig {
    fn default() -> Self {
        Self {
            referer_header_value: Self::default_referer_header_value(),
            server_port: Self::default_server_port(),
            max_gas_burnt: Self::default_max_gas_burnt(),
            limit_memory_cache: Default::default(),
            reserved_memory: Self::default_reserved_memory(),
            block_cache_size: Self::default_block_cache_size(),
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct GeneralTxIndexerConfig {
    #[serde(
        deserialize_with = "deserialize_data_or_env",
        default = "GeneralTxIndexerConfig::default_indexer_id"
    )]
    pub indexer_id: String,
    #[serde(
        deserialize_with = "deserialize_data_or_env",
        default = "GeneralTxIndexerConfig::default_metrics_server_port"
    )]
    pub metrics_server_port: u16,
    #[serde(
        deserialize_with = "deserialize_data_or_env",
        default = "GeneralTxIndexerConfig::default_cache_restore_blocks_range"
    )]
    pub cache_restore_blocks_range: u64,
}

impl GeneralTxIndexerConfig {
    pub fn default_indexer_id() -> String {
        "tx-indexer".to_string()
    }

    pub fn default_metrics_server_port() -> u16 {
        8080
    }

    pub fn default_cache_restore_blocks_range() -> u64 {
        1000
    }
}

impl Default for GeneralTxIndexerConfig {
    fn default() -> Self {
        Self {
            indexer_id: Self::default_indexer_id(),
            metrics_server_port: Self::default_metrics_server_port(),
            cache_restore_blocks_range: Self::default_cache_restore_blocks_range(),
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct GeneralStateIndexerConfig {
    #[serde(
        deserialize_with = "deserialize_data_or_env",
        default = "GeneralStateIndexerConfig::default_indexer_id"
    )]
    pub indexer_id: String,
    #[serde(
        deserialize_with = "deserialize_data_or_env",
        default = "GeneralStateIndexerConfig::default_metrics_server_port"
    )]
    pub metrics_server_port: u16,
    #[serde(
        deserialize_with = "deserialize_data_or_env",
        default = "GeneralStateIndexerConfig::default_concurrency"
    )]
    pub concurrency: usize,
}

impl GeneralStateIndexerConfig {
    pub fn default_indexer_id() -> String {
        "state-indexer".to_string()
    }

    pub fn default_metrics_server_port() -> u16 {
        8081
    }

    pub fn default_concurrency() -> usize {
        1
    }
}

impl Default for GeneralStateIndexerConfig {
    fn default() -> Self {
        Self {
            indexer_id: Self::default_indexer_id(),
            metrics_server_port: Self::default_metrics_server_port(),
            concurrency: Self::default_concurrency(),
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct GeneralEpochIndexerConfig {
    #[serde(
        deserialize_with = "deserialize_data_or_env",
        default = "GeneralEpochIndexerConfig::default_indexer_id"
    )]
    pub indexer_id: String,
}

impl GeneralEpochIndexerConfig {
    pub fn default_indexer_id() -> String {
        "epoch-indexer".to_string()
    }
}

impl Default for GeneralEpochIndexerConfig {
    fn default() -> Self {
        Self {
            indexer_id: Self::default_indexer_id(),
        }
    }
}
