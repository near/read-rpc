use serde_derive::Deserialize;

use crate::configs::{deserialize_data_or_env, deserialize_optional_data_or_env};

#[derive(Deserialize, Debug, Clone, Default)]
pub struct DatabaseConfig {
    #[serde(deserialize_with = "deserialize_data_or_env")]
    pub database_url: String,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub database_user: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub database_password: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub database_name: Option<String>,
    #[serde(default)]
    pub rpc_server: DatabaseRpcServerConfig,
    #[serde(default)]
    pub tx_indexer: DatabaseTxIndexerConfig,
    #[serde(default)]
    pub state_indexer: DatabaseStateIndexerConfig,
}

#[derive(Deserialize, Debug, Clone)]
pub struct DatabaseRpcServerConfig {
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub preferred_dc: Option<String>,
    #[serde(
        deserialize_with = "deserialize_data_or_env",
        default = "DatabaseRpcServerConfig::default_max_retry"
    )]
    pub max_retry: u8,
    #[serde(
        deserialize_with = "deserialize_data_or_env",
        default = "DatabaseRpcServerConfig::default_strict_mode"
    )]
    pub strict_mode: bool,
    #[serde(
        deserialize_with = "deserialize_data_or_env",
        default = "DatabaseRpcServerConfig::default_keepalive_interval"
    )]
    pub keepalive_interval: u64,
}

impl DatabaseRpcServerConfig {
    pub fn default_max_retry() -> u8 {
        2
    }

    pub fn default_strict_mode() -> bool {
        false
    }

    pub fn default_keepalive_interval() -> u64 {
        60
    }
}

impl Default for DatabaseRpcServerConfig {
    fn default() -> Self {
        Self {
            preferred_dc: Default::default(),
            max_retry: Self::default_max_retry(),
            strict_mode: Self::default_strict_mode(),
            keepalive_interval: Self::default_keepalive_interval(),
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct DatabaseTxIndexerConfig {
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub preferred_dc: Option<String>,
    #[serde(
        deserialize_with = "deserialize_data_or_env",
        default = "DatabaseTxIndexerConfig::default_max_retry"
    )]
    pub max_retry: u8,
    #[serde(
        deserialize_with = "deserialize_data_or_env",
        default = "DatabaseTxIndexerConfig::default_strict_mode"
    )]
    pub strict_mode: bool,
    #[serde(
        deserialize_with = "deserialize_data_or_env",
        default = "DatabaseTxIndexerConfig::default_max_db_parallel_queries"
    )]
    pub max_db_parallel_queries: i64,
}

impl DatabaseTxIndexerConfig {
    pub fn default_max_retry() -> u8 {
        5
    }

    pub fn default_strict_mode() -> bool {
        true
    }

    pub fn default_max_db_parallel_queries() -> i64 {
        144
    }
}

impl Default for DatabaseTxIndexerConfig {
    fn default() -> Self {
        Self {
            preferred_dc: Default::default(),
            max_retry: Self::default_max_retry(),
            strict_mode: Self::default_strict_mode(),
            max_db_parallel_queries: Self::default_max_db_parallel_queries(),
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct DatabaseStateIndexerConfig {
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub preferred_dc: Option<String>,
    #[serde(
        deserialize_with = "deserialize_data_or_env",
        default = "DatabaseStateIndexerConfig::default_max_retry"
    )]
    pub max_retry: u8,
    #[serde(
        deserialize_with = "deserialize_data_or_env",
        default = "DatabaseStateIndexerConfig::default_strict_mode"
    )]
    pub strict_mode: bool,
}

impl DatabaseStateIndexerConfig {
    pub fn default_max_retry() -> u8 {
        5
    }

    pub fn default_strict_mode() -> bool {
        true
    }
}

impl Default for DatabaseStateIndexerConfig {
    fn default() -> Self {
        Self {
            preferred_dc: Default::default(),
            max_retry: Self::default_max_retry(),
            strict_mode: Self::default_strict_mode(),
        }
    }
}
