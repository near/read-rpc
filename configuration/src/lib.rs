use near_indexer_primitives::views::StateChangeValueView;
use serde_derive::Deserialize;
use std::path::Path;

#[derive(Deserialize, Debug, Clone, Default)]
pub struct Config {
    pub general: GeneralConfig,
    pub accounts: AccountsConfig,
    pub state_changes: StateConfig,
    pub lake_config: LakeConfig,
    pub database: DatabaseConfig,
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct GeneralConfig {
    pub chain_id: ChainId,
    pub near_rpc_url: String,
    pub near_archival_rpc_url: Option<String>,
    pub rpc_server: GeneralRpcServerConfig,
    pub tx_indexer: GeneralTxIndexerConfig,
    pub state_indexer: GeneralStateIndexerConfig,
    pub epoch_indexer: GeneralEpochIndexerConfig,
}

#[derive(Deserialize, PartialEq, Debug, Clone)]
pub enum ChainId {
    Mainnet,
    Testnet,
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct GeneralRpcServerConfig {
    pub referer_header_value: String,
    pub server_port: u16,
    pub max_gas_burnt: u64,
    pub limit_memory_cache: Option<f64>,
    pub reserved_memory: f64,
    pub block_cache_size: f64,
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct GeneralTxIndexerConfig {
    pub indexer_id: String,
    pub port: u16,
    pub cache_restore_blocks_range: u64,
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct GeneralStateIndexerConfig {
    pub indexer_id: String,
    pub port: u16,
    pub concurrency: u16,
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct GeneralEpochIndexerConfig {
    pub indexer_id: String,
}

impl Default for ChainId {
    fn default() -> Self {
        ChainId::Mainnet
    }
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct AccountsConfig {
    pub tracked_accounts: Vec<String>,
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct StateConfig {
    pub tracked_changes: Vec<ChangeType>,
}

#[derive(Deserialize, PartialEq, Debug, Clone)]
pub enum ChangeType {
    State,
    AccessKey,
    ContractCode,
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct LakeConfig {
    pub aws_access_key_id: String,
    pub aws_secret_access_key: String,
    pub aws_default_region: String,
    pub aws_bucket_name: String,
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct DatabaseConfig {
    pub database_url: String,
    pub database_user: Option<String>,
    pub database_password: Option<String>,
    pub database_name: Option<String>,
    pub rpc_server: DatabaseRpcServerConfig,
    pub tx_indexer: DatabaseTxIndexerConfig,
    pub state_indexer: DatabaseStateIndexerConfig,
    pub epoch_indexer: DatabaseEpochIndexerConfig,
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct DatabaseRpcServerConfig {
    pub preferred_dc: Option<String>,
    pub max_retry: u32,
    pub strict_mode: bool,
    pub keepalive_interval: u64,
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct DatabaseTxIndexerConfig {
    pub preferred_dc: Option<String>,
    pub max_retry: u32,
    pub strict_mode: bool,
    pub max_db_parallel_queries: u16,
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct DatabaseStateIndexerConfig {
    pub preferred_dc: Option<String>,
    pub max_retry: u32,
    pub strict_mode: bool,
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct DatabaseEpochIndexerConfig {
    pub preferred_dc: Option<String>,
    pub max_retry: u32,
    pub strict_mode: bool,
}

impl Config {
    fn contains_account(&self, account_id: &str) -> bool {
        self.accounts.is_indexed_account(account_id.to_string())
    }

    fn contains_change(&self, change_type: ChangeType) -> bool {
        self.state_changes.is_indexed_change(change_type)
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

impl AccountsConfig {
    pub fn is_indexed_account(&self, account: String) -> bool {
        if self.tracked_accounts.is_empty() {
            true
        } else {
            self.tracked_accounts.contains(&account)
        }
    }
}

impl StateConfig {
    pub fn is_indexed_change(&self, change_type: ChangeType) -> bool {
        if self.tracked_changes.is_empty() {
            true
        } else {
            self.tracked_changes.contains(&change_type)
        }
    }
}

async fn read_toml_file(path_file: &Path) -> anyhow::Result<Config> {
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
