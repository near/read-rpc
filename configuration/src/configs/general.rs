use std::str::FromStr;

use serde_derive::Deserialize;

use crate::configs::{
    deserialize_data_or_env, deserialize_optional_data_or_env, required_value_or_panic,
};

#[derive(Debug, Clone)]
pub struct GeneralRpcServerConfig {
    pub chain_id: ChainId,
    pub near_rpc_url: String,
    pub near_archival_rpc_url: Option<String>,
    pub referer_header_value: String,
    pub server_port: u16,
    pub max_gas_burnt: u64,
    pub limit_memory_cache: Option<f64>,
    pub reserved_memory: f64,
    pub block_cache_size: f64,
    pub shadow_data_consistency_rate: f64,
}

#[derive(Debug, Clone)]
pub struct GeneralTxIndexerConfig {
    pub chain_id: ChainId,
    pub near_rpc_url: String,
    pub near_archival_rpc_url: Option<String>,
    pub indexer_id: String,
    pub metrics_server_port: u16,
    pub cache_restore_blocks_range: u64,
}

#[derive(Debug, Clone)]
pub struct GeneralStateIndexerConfig {
    pub chain_id: ChainId,
    pub near_rpc_url: String,
    pub near_archival_rpc_url: Option<String>,
    pub indexer_id: String,
    pub metrics_server_port: u16,
    pub concurrency: usize,
}

#[derive(Debug, Clone)]
pub struct GeneralNearStateIndexerConfig {
    pub chain_id: ChainId,
    pub indexer_id: String,
    pub metrics_server_port: u16,
    pub concurrency: usize,
}

#[derive(Debug, Clone)]
pub struct GeneralEpochIndexerConfig {
    pub chain_id: ChainId,
    pub near_rpc_url: String,
    pub near_archival_rpc_url: Option<String>,
    pub indexer_id: String,
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct CommonGeneralConfig {
    #[serde(deserialize_with = "deserialize_data_or_env")]
    pub chain_id: ChainId,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub near_rpc_url: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub near_archival_rpc_url: Option<String>,
    #[serde(default)]
    pub rpc_server: CommonGeneralRpcServerConfig,
    #[serde(default)]
    pub tx_indexer: CommonGeneralTxIndexerConfig,
    #[serde(default)]
    pub state_indexer: CommonGeneralStateIndexerConfig,
    #[serde(default)]
    pub near_state_indexer: CommonGeneralStateIndexerConfig,
    #[serde(default)]
    pub epoch_indexer: CommonGeneralEpochIndexerConfig,
}

#[derive(Deserialize, PartialEq, Debug, Clone, Default)]
#[serde(rename_all = "lowercase")]
pub enum ChainId {
    #[default]
    Mainnet,
    Testnet,
    Betanet,
    Localnet,
}

impl FromStr for ChainId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "mainnet" => Ok(ChainId::Mainnet),
            "testnet" => Ok(ChainId::Testnet),
            "localnet" => Ok(ChainId::Localnet),
            "betanet" => Ok(ChainId::Betanet),
            _ => Err(anyhow::anyhow!("Invalid chain id")),
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct CommonGeneralRpcServerConfig {
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub referer_header_value: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub server_port: Option<u16>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub max_gas_burnt: Option<u64>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub limit_memory_cache: Option<f64>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub reserved_memory: Option<f64>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub block_cache_size: Option<f64>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub shadow_data_consistency_rate: Option<f64>,
}

impl CommonGeneralRpcServerConfig {
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

    pub fn default_shadow_data_consistency_rate() -> f64 {
        100.0
    }
}

impl Default for CommonGeneralRpcServerConfig {
    fn default() -> Self {
        Self {
            referer_header_value: Some(Self::default_referer_header_value()),
            server_port: Some(Self::default_server_port()),
            max_gas_burnt: Some(Self::default_max_gas_burnt()),
            limit_memory_cache: Default::default(),
            reserved_memory: Some(Self::default_reserved_memory()),
            block_cache_size: Some(Self::default_block_cache_size()),
            shadow_data_consistency_rate: Some(Self::default_shadow_data_consistency_rate()),
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct CommonGeneralTxIndexerConfig {
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub indexer_id: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub metrics_server_port: Option<u16>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub cache_restore_blocks_range: Option<u64>,
}

impl CommonGeneralTxIndexerConfig {
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

impl Default for CommonGeneralTxIndexerConfig {
    fn default() -> Self {
        Self {
            indexer_id: Some(Self::default_indexer_id()),
            metrics_server_port: Some(Self::default_metrics_server_port()),
            cache_restore_blocks_range: Some(Self::default_cache_restore_blocks_range()),
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct CommonGeneralStateIndexerConfig {
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub indexer_id: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub metrics_server_port: Option<u16>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub concurrency: Option<usize>,
}

impl CommonGeneralStateIndexerConfig {
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

impl Default for CommonGeneralStateIndexerConfig {
    fn default() -> Self {
        Self {
            indexer_id: Some(Self::default_indexer_id()),
            metrics_server_port: Some(Self::default_metrics_server_port()),
            concurrency: Some(Self::default_concurrency()),
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct CommonGeneralNearStateIndexerConfig {
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub indexer_id: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub metrics_server_port: Option<u16>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub concurrency: Option<usize>,
}

impl CommonGeneralNearStateIndexerConfig {
    pub fn default_indexer_id() -> String {
        "near-state-indexer".to_string()
    }

    pub fn default_metrics_server_port() -> u16 {
        8082
    }

    pub fn default_concurrency() -> usize {
        1
    }
}

impl Default for CommonGeneralNearStateIndexerConfig {
    fn default() -> Self {
        Self {
            indexer_id: Some(Self::default_indexer_id()),
            metrics_server_port: Some(Self::default_metrics_server_port()),
            concurrency: Some(Self::default_concurrency()),
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct CommonGeneralEpochIndexerConfig {
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub indexer_id: Option<String>,
}

impl CommonGeneralEpochIndexerConfig {
    pub fn default_indexer_id() -> String {
        "epoch-indexer".to_string()
    }
}

impl Default for CommonGeneralEpochIndexerConfig {
    fn default() -> Self {
        Self {
            indexer_id: Some(Self::default_indexer_id()),
        }
    }
}

impl From<CommonGeneralConfig> for GeneralRpcServerConfig {
    fn from(common_config: CommonGeneralConfig) -> Self {
        Self {
            chain_id: common_config.chain_id,
            near_rpc_url: required_value_or_panic("near_rpc_url", common_config.near_rpc_url),
            near_archival_rpc_url: common_config.near_archival_rpc_url,
            referer_header_value: common_config
                .rpc_server
                .referer_header_value
                .unwrap_or_else(CommonGeneralRpcServerConfig::default_referer_header_value),
            server_port: common_config
                .rpc_server
                .server_port
                .unwrap_or_else(CommonGeneralRpcServerConfig::default_server_port),
            max_gas_burnt: common_config
                .rpc_server
                .max_gas_burnt
                .unwrap_or_else(CommonGeneralRpcServerConfig::default_max_gas_burnt),
            limit_memory_cache: common_config.rpc_server.limit_memory_cache,
            reserved_memory: common_config
                .rpc_server
                .reserved_memory
                .unwrap_or_else(CommonGeneralRpcServerConfig::default_reserved_memory),
            block_cache_size: common_config
                .rpc_server
                .block_cache_size
                .unwrap_or_else(CommonGeneralRpcServerConfig::default_block_cache_size),
            shadow_data_consistency_rate: common_config
                .rpc_server
                .shadow_data_consistency_rate
                .unwrap_or_else(CommonGeneralRpcServerConfig::default_shadow_data_consistency_rate),
        }
    }
}

impl From<CommonGeneralConfig> for GeneralTxIndexerConfig {
    fn from(common_config: CommonGeneralConfig) -> Self {
        Self {
            chain_id: common_config.chain_id,
            near_rpc_url: required_value_or_panic("near_rpc_url", common_config.near_rpc_url),
            near_archival_rpc_url: common_config.near_archival_rpc_url,
            indexer_id: common_config
                .tx_indexer
                .indexer_id
                .unwrap_or_else(CommonGeneralTxIndexerConfig::default_indexer_id),
            metrics_server_port: common_config
                .tx_indexer
                .metrics_server_port
                .unwrap_or_else(CommonGeneralTxIndexerConfig::default_metrics_server_port),
            cache_restore_blocks_range: common_config
                .tx_indexer
                .cache_restore_blocks_range
                .unwrap_or_else(CommonGeneralTxIndexerConfig::default_cache_restore_blocks_range),
        }
    }
}

impl From<CommonGeneralConfig> for GeneralStateIndexerConfig {
    fn from(common_config: CommonGeneralConfig) -> Self {
        Self {
            chain_id: common_config.chain_id,
            near_rpc_url: required_value_or_panic("near_rpc_url", common_config.near_rpc_url),
            near_archival_rpc_url: common_config.near_archival_rpc_url,
            indexer_id: common_config
                .state_indexer
                .indexer_id
                .unwrap_or_else(CommonGeneralStateIndexerConfig::default_indexer_id),
            metrics_server_port: common_config
                .state_indexer
                .metrics_server_port
                .unwrap_or_else(CommonGeneralStateIndexerConfig::default_metrics_server_port),
            concurrency: common_config
                .state_indexer
                .concurrency
                .unwrap_or_else(CommonGeneralStateIndexerConfig::default_concurrency),
        }
    }
}

impl From<CommonGeneralConfig> for GeneralNearStateIndexerConfig {
    fn from(common_config: CommonGeneralConfig) -> Self {
        Self {
            chain_id: common_config.chain_id,
            indexer_id: common_config
                .near_state_indexer
                .indexer_id
                .unwrap_or_else(CommonGeneralStateIndexerConfig::default_indexer_id),
            metrics_server_port: common_config
                .near_state_indexer
                .metrics_server_port
                .unwrap_or_else(CommonGeneralStateIndexerConfig::default_metrics_server_port),
            concurrency: common_config
                .near_state_indexer
                .concurrency
                .unwrap_or_else(CommonGeneralStateIndexerConfig::default_concurrency),
        }
    }
}

impl From<CommonGeneralConfig> for GeneralEpochIndexerConfig {
    fn from(common_config: CommonGeneralConfig) -> Self {
        Self {
            chain_id: common_config.chain_id,
            near_rpc_url: required_value_or_panic("near_rpc_url", common_config.near_rpc_url),
            near_archival_rpc_url: common_config.near_archival_rpc_url,
            indexer_id: common_config
                .epoch_indexer
                .indexer_id
                .unwrap_or_else(CommonGeneralEpochIndexerConfig::default_indexer_id),
        }
    }
}
