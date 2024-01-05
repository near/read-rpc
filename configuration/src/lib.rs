use near_indexer_primitives::views::StateChangeValueView;
use serde::Deserialize;
use std::path::Path;

lazy_static::lazy_static! {
    static ref RE_NAME_ENV: regex::Regex = regex::Regex::new(r"\$\{(?<env_name>\w+)}").unwrap();
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct Config {
    pub general: GeneralConfig,
    #[serde(default)]
    pub rightsizing: RightsizingConfig,
    pub lake_config: LakeConfig,
    pub database: DatabaseConfig,
}

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
    pub concurrency: u16,
}

impl GeneralStateIndexerConfig {
    pub fn default_indexer_id() -> String {
        "state-indexer".to_string()
    }

    pub fn default_metrics_server_port() -> u16 {
        8081
    }

    pub fn default_concurrency() -> u16 {
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

#[derive(Deserialize, Debug, Clone, Default)]
pub struct RightsizingConfig {
    #[serde(default)]
    pub tracked_accounts: Vec<String>,
    #[serde(default)]
    pub tracked_changes: Vec<ChangeType>,
}

#[derive(Deserialize, PartialEq, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum ChangeType {
    State,
    AccessKey,
    ContractCode,
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct LakeConfig {
    #[serde(deserialize_with = "deserialize_data_or_env")]
    pub aws_access_key_id: String,
    #[serde(deserialize_with = "deserialize_data_or_env")]
    pub aws_secret_access_key: String,
    #[serde(deserialize_with = "deserialize_data_or_env")]
    pub aws_default_region: String,
    #[serde(deserialize_with = "deserialize_data_or_env")]
    pub aws_bucket_name: String,
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct DatabaseConfig {
    #[serde(deserialize_with = "deserialize_data_or_env")]
    pub database_url: String,
    #[serde(deserialize_with = "deserialize_data_or_env", default)]
    pub database_user: Option<String>,
    #[serde(deserialize_with = "deserialize_data_or_env", default)]
    pub database_password: Option<String>,
    #[serde(deserialize_with = "deserialize_data_or_env", default)]
    pub database_name: Option<String>,
    #[serde(default)]
    pub rpc_server: DatabaseRpcServerConfig,
    #[serde(default)]
    pub tx_indexer: DatabaseTxIndexerConfig,
    #[serde(default)]
    pub state_indexer: DatabaseStateIndexerConfig,
    #[serde(default)]
    pub epoch_indexer: DatabaseEpochIndexerConfig,
}

#[derive(Deserialize, Debug, Clone)]
pub struct DatabaseRpcServerConfig {
    #[serde(deserialize_with = "deserialize_data_or_env", default)]
    pub preferred_dc: Option<String>,
    #[serde(
        deserialize_with = "deserialize_data_or_env",
        default = "DatabaseRpcServerConfig::default_max_retry"
    )]
    pub max_retry: u32,
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
    pub fn default_max_retry() -> u32 {
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
    #[serde(deserialize_with = "deserialize_data_or_env", default)]
    pub preferred_dc: Option<String>,
    #[serde(
        deserialize_with = "deserialize_data_or_env",
        default = "DatabaseTxIndexerConfig::default_max_retry"
    )]
    pub max_retry: u32,
    #[serde(
        deserialize_with = "deserialize_data_or_env",
        default = "DatabaseTxIndexerConfig::default_strict_mode"
    )]
    pub strict_mode: bool,
    #[serde(
        deserialize_with = "deserialize_data_or_env",
        default = "DatabaseTxIndexerConfig::default_max_db_parallel_queries"
    )]
    pub max_db_parallel_queries: u16,
}

impl DatabaseTxIndexerConfig {
    pub fn default_max_retry() -> u32 {
        5
    }

    pub fn default_strict_mode() -> bool {
        true
    }

    pub fn default_max_db_parallel_queries() -> u16 {
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
    #[serde(deserialize_with = "deserialize_data_or_env", default)]
    pub preferred_dc: Option<String>,
    #[serde(
        deserialize_with = "deserialize_data_or_env",
        default = "DatabaseStateIndexerConfig::default_max_retry"
    )]
    pub max_retry: u32,
    #[serde(
        deserialize_with = "deserialize_data_or_env",
        default = "DatabaseStateIndexerConfig::default_strict_mode"
    )]
    pub strict_mode: bool,
}

impl DatabaseStateIndexerConfig {
    pub fn default_max_retry() -> u32 {
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

#[derive(Deserialize, Debug, Clone)]
pub struct DatabaseEpochIndexerConfig {
    #[serde(deserialize_with = "deserialize_data_or_env", default)]
    pub preferred_dc: Option<String>,
    #[serde(
        deserialize_with = "deserialize_data_or_env",
        default = "DatabaseEpochIndexerConfig::default_max_retry"
    )]
    pub max_retry: u32,
    #[serde(
        deserialize_with = "deserialize_data_or_env",
        default = "DatabaseEpochIndexerConfig::default_strict_mode"
    )]
    pub strict_mode: bool,
}

impl DatabaseEpochIndexerConfig {
    pub fn default_max_retry() -> u32 {
        5
    }

    pub fn default_strict_mode() -> bool {
        true
    }
}

impl Default for DatabaseEpochIndexerConfig {
    fn default() -> Self {
        Self {
            preferred_dc: Default::default(),
            max_retry: Self::default_max_retry(),
            strict_mode: Self::default_strict_mode(),
        }
    }
}

impl Config {
    fn contains_account(&self, account_id: &str) -> bool {
        self.rightsizing.is_indexed_account(account_id.to_string())
    }

    fn contains_change(&self, change_type: ChangeType) -> bool {
        self.rightsizing.is_indexed_change(change_type)
    }

    pub fn should_be_indexed(&self, state_change_value: &StateChangeValueView) -> bool {
        match state_change_value {
            StateChangeValueView::DataUpdate { account_id, .. }
            | StateChangeValueView::DataDeletion { account_id, .. } => {
                self.contains_account(account_id) && self.contains_change(ChangeType::State)
            }
            StateChangeValueView::AccessKeyUpdate { account_id, .. }
            | StateChangeValueView::AccessKeyDeletion { account_id, .. } => {
                self.contains_account(account_id) && self.contains_change(ChangeType::AccessKey)
            }
            StateChangeValueView::ContractCodeUpdate { account_id, .. }
            | StateChangeValueView::ContractCodeDeletion { account_id, .. } => {
                self.contains_account(account_id) && self.contains_change(ChangeType::ContractCode)
            }
            StateChangeValueView::AccountUpdate { account_id, .. }
            | StateChangeValueView::AccountDeletion { account_id, .. } => {
                self.contains_account(account_id)
            }
        }
    }
}

impl RightsizingConfig {
    pub fn is_indexed_account(&self, account: String) -> bool {
        if self.tracked_accounts.is_empty() {
            true
        } else {
            self.tracked_accounts.contains(&account)
        }
    }

    pub fn is_indexed_change(&self, change_type: ChangeType) -> bool {
        if self.tracked_changes.is_empty() {
            true
        } else {
            self.tracked_changes.contains(&change_type)
        }
    }
}

fn deserialize_data_or_env<'de, D, T>(data: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::de::DeserializeOwned,
{
    let value = serde_json::Value::deserialize(data)?;
    if let serde_json::Value::String(value) = &value {
        if let Some(caps) = RE_NAME_ENV.captures(value) {
            if let Ok(env_value) = std::env::var(&caps["env_name"]) {
                let value = serde_json::Value::from(env_value);
                return serde_json::from_value(value).map_err(serde::de::Error::custom);
            }
        }
    }
    serde_json::from_value(value).map_err(serde::de::Error::custom)
}

pub async fn read_configuration_from_file(path_file: &str) -> anyhow::Result<Config> {
    let path_file = Path::new(path_file);
    read_toml_file(path_file).await
}

pub async fn read_configuration() -> anyhow::Result<Config> {
    let mut path_root = project_root::get_project_root()?;
    path_root.push("config.toml");
    if path_root.exists() {
        read_toml_file(path_root.as_path()).await
    } else {
        Ok(Config::default())
    }
}

async fn read_toml_file(path_file: &Path) -> anyhow::Result<Config> {
    dotenv::dotenv().ok();
    match std::fs::read_to_string(path_file) {
        Ok(content) => match toml::from_str::<Config>(&content) {
            Ok(config) => Ok(config),
            Err(err) => {
                anyhow::bail!(
                    "Unable to load data from: {:?}.\n Error: {}",
                    path_file.to_str(),
                    err
                );
            }
        },
        Err(err) => {
            anyhow::bail!(
                "Could not read file: {:?}.\n Error: {}",
                path_file.to_str(),
                err
            );
        }
    }
}
