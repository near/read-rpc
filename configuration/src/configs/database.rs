use serde::Deserialize;

use crate::configs::{deserialize_data_or_env, deserialize_optional_data_or_env};

#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub database_url: String,
    pub database_user: Option<String>,
    pub database_password: Option<String>,
    pub database_name: Option<String>,
    pub preferred_dc: Option<String>,
    pub max_retry: u8,
    pub strict_mode: bool,
    pub keepalive_interval: Option<u64>,
    pub max_db_parallel_queries: i64,
}

pub struct DatabaseRpcServerConfig {
    pub database_url: String,
    pub database_user: Option<String>,
    pub database_password: Option<String>,
    pub database_name: Option<String>,
    pub preferred_dc: Option<String>,
    pub max_retry: u8,
    pub strict_mode: bool,
    pub keepalive_interval: u64,
}

pub struct DatabaseTxIndexerConfig {
    pub database_url: String,
    pub database_user: Option<String>,
    pub database_password: Option<String>,
    pub database_name: Option<String>,
    pub preferred_dc: Option<String>,
    pub max_retry: u8,
    pub strict_mode: bool,
    pub max_db_parallel_queries: i64,
}

pub struct DatabaseStateIndexerConfig {
    pub database_url: String,
    pub database_user: Option<String>,
    pub database_password: Option<String>,
    pub database_name: Option<String>,
    pub preferred_dc: Option<String>,
    pub max_retry: u8,
    pub strict_mode: bool,
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct CommonDatabaseConfig {
    #[serde(deserialize_with = "deserialize_data_or_env")]
    pub database_url: String,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub database_user: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub database_password: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub database_name: Option<String>,
    #[serde(default)]
    pub rpc_server: CommonDatabaseRpcServerConfig,
    #[serde(default)]
    pub tx_indexer: CommonDatabaseTxIndexerConfig,
    #[serde(default)]
    pub state_indexer: CommonDatabaseStateIndexerConfig,
}

#[derive(Deserialize, Debug, Clone)]
pub struct CommonDatabaseRpcServerConfig {
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub preferred_dc: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub max_retry: Option<u8>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub strict_mode: Option<bool>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub keepalive_interval: Option<u64>,
}

impl CommonDatabaseRpcServerConfig {
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

impl Default for CommonDatabaseRpcServerConfig {
    fn default() -> Self {
        Self {
            preferred_dc: Default::default(),
            max_retry: Some(Self::default_max_retry()),
            strict_mode: Some(Self::default_strict_mode()),
            keepalive_interval: Some(Self::default_keepalive_interval()),
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct CommonDatabaseTxIndexerConfig {
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub preferred_dc: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub max_retry: Option<u8>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub strict_mode: Option<bool>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub max_db_parallel_queries: Option<i64>,
}

impl CommonDatabaseTxIndexerConfig {
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

impl Default for CommonDatabaseTxIndexerConfig {
    fn default() -> Self {
        Self {
            preferred_dc: Default::default(),
            max_retry: Some(Self::default_max_retry()),
            strict_mode: Some(Self::default_strict_mode()),
            max_db_parallel_queries: Some(Self::default_max_db_parallel_queries()),
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct CommonDatabaseStateIndexerConfig {
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub preferred_dc: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub max_retry: Option<u8>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub strict_mode: Option<bool>,
}

impl CommonDatabaseStateIndexerConfig {
    pub fn default_max_retry() -> u8 {
        5
    }

    pub fn default_strict_mode() -> bool {
        true
    }
}

impl Default for CommonDatabaseStateIndexerConfig {
    fn default() -> Self {
        Self {
            preferred_dc: Default::default(),
            max_retry: Some(Self::default_max_retry()),
            strict_mode: Some(Self::default_strict_mode()),
        }
    }
}

impl From<CommonDatabaseConfig> for DatabaseRpcServerConfig {
    fn from(common_config: CommonDatabaseConfig) -> Self {
        Self {
            database_url: common_config.database_url,
            database_user: common_config.database_user,
            database_password: common_config.database_password,
            database_name: common_config.database_name,
            preferred_dc: common_config.rpc_server.preferred_dc,
            max_retry: common_config
                .rpc_server
                .max_retry
                .unwrap_or_else(CommonDatabaseRpcServerConfig::default_max_retry),
            strict_mode: common_config
                .rpc_server
                .strict_mode
                .unwrap_or_else(CommonDatabaseRpcServerConfig::default_strict_mode),
            keepalive_interval: common_config
                .rpc_server
                .keepalive_interval
                .unwrap_or_else(CommonDatabaseRpcServerConfig::default_keepalive_interval),
        }
    }
}

impl From<CommonDatabaseConfig> for DatabaseTxIndexerConfig {
    fn from(common_config: CommonDatabaseConfig) -> Self {
        Self {
            database_url: common_config.database_url,
            database_user: common_config.database_user,
            database_password: common_config.database_password,
            database_name: common_config.database_name,
            preferred_dc: common_config.tx_indexer.preferred_dc,
            max_retry: common_config
                .tx_indexer
                .max_retry
                .unwrap_or_else(CommonDatabaseTxIndexerConfig::default_max_retry),
            strict_mode: common_config
                .tx_indexer
                .strict_mode
                .unwrap_or_else(CommonDatabaseTxIndexerConfig::default_strict_mode),
            max_db_parallel_queries: common_config
                .tx_indexer
                .max_db_parallel_queries
                .unwrap_or_else(CommonDatabaseTxIndexerConfig::default_max_db_parallel_queries),
        }
    }
}

impl From<CommonDatabaseConfig> for DatabaseStateIndexerConfig {
    fn from(common_config: CommonDatabaseConfig) -> Self {
        Self {
            database_url: common_config.database_url,
            database_user: common_config.database_user,
            database_password: common_config.database_password,
            database_name: common_config.database_name,
            preferred_dc: common_config.state_indexer.preferred_dc,
            max_retry: common_config
                .state_indexer
                .max_retry
                .unwrap_or_else(CommonDatabaseStateIndexerConfig::default_max_retry),
            strict_mode: common_config
                .state_indexer
                .strict_mode
                .unwrap_or_else(CommonDatabaseStateIndexerConfig::default_strict_mode),
        }
    }
}

impl From<DatabaseRpcServerConfig> for DatabaseConfig {
    fn from(rpc_server_config: DatabaseRpcServerConfig) -> Self {
        Self {
            database_url: rpc_server_config.database_url,
            database_user: rpc_server_config.database_user,
            database_password: rpc_server_config.database_password,
            database_name: rpc_server_config.database_name,
            preferred_dc: rpc_server_config.preferred_dc,
            max_retry: rpc_server_config.max_retry,
            strict_mode: rpc_server_config.strict_mode,
            keepalive_interval: Some(rpc_server_config.keepalive_interval),
            max_db_parallel_queries: Default::default(),
        }
    }
}

impl From<DatabaseTxIndexerConfig> for DatabaseConfig {
    fn from(tx_indexer_config: DatabaseTxIndexerConfig) -> Self {
        Self {
            database_url: tx_indexer_config.database_url,
            database_user: tx_indexer_config.database_user,
            database_password: tx_indexer_config.database_password,
            database_name: tx_indexer_config.database_name,
            preferred_dc: tx_indexer_config.preferred_dc,
            max_retry: tx_indexer_config.max_retry,
            strict_mode: tx_indexer_config.strict_mode,
            keepalive_interval: Default::default(),
            max_db_parallel_queries: tx_indexer_config.max_db_parallel_queries,
        }
    }
}

impl From<DatabaseStateIndexerConfig> for DatabaseConfig {
    fn from(state_indexer_config: DatabaseStateIndexerConfig) -> Self {
        Self {
            database_url: state_indexer_config.database_url,
            database_user: state_indexer_config.database_user,
            database_password: state_indexer_config.database_password,
            database_name: state_indexer_config.database_name,
            preferred_dc: state_indexer_config.preferred_dc,
            max_retry: state_indexer_config.max_retry,
            strict_mode: state_indexer_config.strict_mode,
            keepalive_interval: Default::default(),
            max_db_parallel_queries: Default::default(),
        }
    }
}
