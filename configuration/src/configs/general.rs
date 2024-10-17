use std::str::FromStr;

use serde_derive::Deserialize;
use validator::Validate;

use crate::configs::{
    deserialize_data_or_env, deserialize_optional_data_or_env, required_value_or_panic,
};

#[derive(Debug, Clone)]
pub struct GeneralRpcServerConfig {
    pub chain_id: ChainId,
    pub near_rpc_url: String,
    pub near_archival_rpc_url: Option<String>,
    pub redis_url: url::Url,
    pub referer_header_value: String,
    pub server_port: u16,
    pub max_gas_burnt: u64,
    pub contract_code_cache_size: f64,
    pub block_cache_size: f64,
    pub shadow_data_consistency_rate: f64,
    pub prefetch_state_size_limit: u64,
}

#[derive(Debug, Clone)]
pub struct GeneralTxIndexerConfig {
    pub chain_id: ChainId,
    pub near_rpc_url: String,
    pub near_archival_rpc_url: Option<String>,
    pub redis_url: url::Url,
    pub indexer_id: String,
    pub metrics_server_port: u16,
}

#[derive(Debug, Clone)]
pub struct GeneralStateIndexerConfig {
    pub chain_id: ChainId,
    pub near_rpc_url: String,
    pub near_archival_rpc_url: Option<String>,
    pub referer_header_value: String,
    pub indexer_id: String,
    pub metrics_server_port: u16,
    pub concurrency: usize,
}

#[derive(Debug, Clone)]
pub struct GeneralNearStateIndexerConfig {
    pub chain_id: ChainId,
    pub redis_url: url::Url,
    pub concurrency: usize,
}

#[derive(Validate, Deserialize, Debug, Clone, Default)]
pub struct CommonGeneralConfig {
    #[serde(deserialize_with = "deserialize_data_or_env")]
    pub chain_id: ChainId,
    #[validate(url(message = "Invalid NEAR RPC URL"))]
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub near_rpc_url: Option<String>,
    #[validate(url(message = "Invalid NEAR Archival RPC URL"))]
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub near_archival_rpc_url: Option<String>,
    #[validate(url(message = "Invalid referer header value"))]
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub referer_header_value: Option<String>,
    #[validate(url(message = "Invalid Redis URL"))]
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub redis_url: Option<String>,
    #[validate(nested)]
    #[serde(default)]
    pub rpc_server: CommonGeneralRpcServerConfig,
    #[serde(default)]
    pub tx_indexer: CommonGeneralTxIndexerConfig,
    #[serde(default)]
    pub state_indexer: CommonGeneralStateIndexerConfig,
    #[serde(default)]
    pub near_state_indexer: CommonGeneralNearStateIndexerConfig,
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

#[derive(Validate, Deserialize, Debug, Clone)]
pub struct CommonGeneralRpcServerConfig {
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub server_port: Option<u16>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub max_gas_burnt: Option<u64>,
    #[validate(range(
        min = 0.0,
        message = "Contract code cache size must be greater than or equal to 0"
    ))]
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub contract_code_cache_size: Option<f64>,
    #[validate(range(
        min = 0.0,
        message = "Block cache size must be greater than or equal to 0"
    ))]
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub block_cache_size: Option<f64>,
    #[validate(range(
        min = 0.0,
        max = 100.0,
        message = "Shadow data consistency rate must be between 0 and 100"
    ))]
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub shadow_data_consistency_rate: Option<f64>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub prefetch_state_size_limit: Option<u64>,
}

impl CommonGeneralRpcServerConfig {
    pub fn default_server_port() -> u16 {
        8080
    }

    pub fn default_max_gas_burnt() -> u64 {
        300_000_000_000_000
    }

    pub fn default_contract_code_cache_size() -> f64 {
        0.25
    }

    pub fn default_block_cache_size() -> f64 {
        0.125
    }

    pub fn default_shadow_data_consistency_rate() -> f64 {
        100.0
    }

    pub fn default_prefetch_state_size_limit() -> u64 {
        100_000
    }
}

impl Default for CommonGeneralRpcServerConfig {
    fn default() -> Self {
        Self {
            server_port: Some(Self::default_server_port()),
            max_gas_burnt: Some(Self::default_max_gas_burnt()),
            contract_code_cache_size: Some(Self::default_contract_code_cache_size()),
            block_cache_size: Some(Self::default_block_cache_size()),
            shadow_data_consistency_rate: Some(Self::default_shadow_data_consistency_rate()),
            prefetch_state_size_limit: Some(Self::default_prefetch_state_size_limit()),
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct CommonGeneralTxIndexerConfig {
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub indexer_id: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub metrics_server_port: Option<u16>,
}

impl CommonGeneralTxIndexerConfig {
    pub fn default_indexer_id() -> String {
        "tx-indexer".to_string()
    }

    pub fn default_metrics_server_port() -> u16 {
        8080
    }
}

impl Default for CommonGeneralTxIndexerConfig {
    fn default() -> Self {
        Self {
            indexer_id: Some(Self::default_indexer_id()),
            metrics_server_port: Some(Self::default_metrics_server_port()),
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
    pub concurrency: Option<usize>,
}

impl CommonGeneralNearStateIndexerConfig {
    pub fn default_concurrency() -> usize {
        1
    }
}

impl Default for CommonGeneralNearStateIndexerConfig {
    fn default() -> Self {
        Self {
            concurrency: Some(Self::default_concurrency()),
        }
    }
}

impl From<CommonGeneralConfig> for GeneralRpcServerConfig {
    fn from(common_config: CommonGeneralConfig) -> Self {
        Self {
            chain_id: common_config.chain_id,
            near_rpc_url: required_value_or_panic("near_rpc_url", common_config.near_rpc_url),
            near_archival_rpc_url: common_config.near_archival_rpc_url,
            redis_url: url::Url::parse(
                &common_config
                    .redis_url
                    .unwrap_or("redis://127.0.0.1:6379".to_string()),
            )
            .expect("Invalid redis url"),
            referer_header_value: common_config
                .referer_header_value
                .unwrap_or("http://read-rpc.local".to_string()),
            server_port: common_config
                .rpc_server
                .server_port
                .unwrap_or_else(CommonGeneralRpcServerConfig::default_server_port),
            max_gas_burnt: common_config
                .rpc_server
                .max_gas_burnt
                .unwrap_or_else(CommonGeneralRpcServerConfig::default_max_gas_burnt),
            contract_code_cache_size: common_config
                .rpc_server
                .contract_code_cache_size
                .unwrap_or_else(CommonGeneralRpcServerConfig::default_contract_code_cache_size),
            block_cache_size: common_config
                .rpc_server
                .block_cache_size
                .unwrap_or_else(CommonGeneralRpcServerConfig::default_block_cache_size),
            shadow_data_consistency_rate: common_config
                .rpc_server
                .shadow_data_consistency_rate
                .unwrap_or_else(CommonGeneralRpcServerConfig::default_shadow_data_consistency_rate),
            prefetch_state_size_limit: common_config
                .rpc_server
                .prefetch_state_size_limit
                .unwrap_or_else(CommonGeneralRpcServerConfig::default_prefetch_state_size_limit),
        }
    }
}

impl From<CommonGeneralConfig> for GeneralTxIndexerConfig {
    fn from(common_config: CommonGeneralConfig) -> Self {
        Self {
            chain_id: common_config.chain_id,
            near_rpc_url: required_value_or_panic("near_rpc_url", common_config.near_rpc_url),
            near_archival_rpc_url: common_config.near_archival_rpc_url,
            redis_url: url::Url::parse(&required_value_or_panic(
                "redis_url",
                common_config.redis_url,
            ))
            .expect("Invalid redis url"),
            indexer_id: common_config
                .tx_indexer
                .indexer_id
                .unwrap_or_else(CommonGeneralTxIndexerConfig::default_indexer_id),
            metrics_server_port: common_config
                .tx_indexer
                .metrics_server_port
                .unwrap_or_else(CommonGeneralTxIndexerConfig::default_metrics_server_port),
        }
    }
}

impl From<CommonGeneralConfig> for GeneralStateIndexerConfig {
    fn from(common_config: CommonGeneralConfig) -> Self {
        Self {
            chain_id: common_config.chain_id,
            near_rpc_url: required_value_or_panic("near_rpc_url", common_config.near_rpc_url),
            near_archival_rpc_url: common_config.near_archival_rpc_url,
            referer_header_value: common_config
                .referer_header_value
                .unwrap_or("http://read-rpc.local".to_string()),
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
            redis_url: url::Url::parse(&required_value_or_panic(
                "redis_url",
                common_config.redis_url,
            ))
            .expect("Invalid redis url"),
            concurrency: common_config
                .near_state_indexer
                .concurrency
                .unwrap_or_else(CommonGeneralNearStateIndexerConfig::default_concurrency),
        }
    }
}
