use near_indexer_primitives::views::StateChangeValueView;
use serde_derive::Deserialize;
use std::path::Path;

#[derive(Deserialize, Debug, Clone, Default)]
pub struct Config {
    pub state_indexer: StateIndexerConfig,
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct StateIndexerConfig {
    pub accounts: Vec<String>,
    pub changes: Vec<String>,
}

impl StateIndexerConfig {
    fn contains_account(&self, account_id: &str) -> bool {
        if self.accounts.is_empty() {
            true
        } else {
            self.accounts.contains(&account_id.to_string())
        }
    }

    fn contains_change(&self, change_type: &str) -> bool {
        if self.changes.is_empty() {
            true
        } else {
            self.changes.contains(&change_type.to_string())
        }
    }
    pub fn should_be_indexed(&self, state_change_value: &StateChangeValueView) -> bool {
        match state_change_value {
            StateChangeValueView::DataUpdate { account_id, .. }
            | StateChangeValueView::DataDeletion { account_id, .. } => {
                self.contains_account(account_id) && self.contains_change("state")
            }
            StateChangeValueView::AccessKeyUpdate { account_id, .. }
            | StateChangeValueView::AccessKeyDeletion { account_id, .. } => {
                self.contains_account(account_id) && self.contains_change("access_key")
            }
            StateChangeValueView::ContractCodeUpdate { account_id, .. }
            | StateChangeValueView::ContractCodeDeletion { account_id, .. } => {
                self.contains_account(account_id) && self.contains_change("contract_code")
            }
            StateChangeValueView::AccountUpdate { account_id, .. }
            | StateChangeValueView::AccountDeletion { account_id, .. } => {
                self.contains_account(account_id) && self.contains_change("account")
            }
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
