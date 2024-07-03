use std::str::FromStr;

use near_lake_framework::{
    near_indexer_primitives, near_indexer_primitives::views::StateChangeValueView,
};
use serde::Deserialize;

pub(crate) mod database;
pub(crate) mod general;
mod lake;
mod rightsizing;
mod tx_details_storage;

lazy_static::lazy_static! {
    static ref RE_NAME_ENV: regex::Regex = regex::Regex::new(r"\$\{(?<env_name>\w+)}").unwrap();
}

fn required_value_or_panic<T>(config_name: &str, value: Option<T>) -> T {
    if let Some(value) = value {
        value
    } else {
        panic!("Config `{}` is required!", config_name)
    }
}

fn get_env_var<T>(env_var_name: &str) -> anyhow::Result<T>
where
    T: FromStr,
    T::Err: std::fmt::Debug,
{
    let var = dotenv::var(env_var_name).map_err(|err| {
        anyhow::anyhow!(
            "Failed to get env var: {:?}. Error: {:?}",
            env_var_name,
            err
        )
    })?;
    var.parse::<T>().map_err(|err| {
        anyhow::anyhow!(
            "Failed to parse env var: {:?}. Error: {:?}",
            env_var_name,
            err
        )
    })
}

fn deserialize_data_or_env<'de, D, T>(data: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::de::DeserializeOwned + FromStr,
    <T as FromStr>::Err: std::fmt::Debug,
{
    let value = serde_json::Value::deserialize(data)?;
    if let serde_json::Value::String(value) = &value {
        if let Some(caps) = RE_NAME_ENV.captures(value) {
            return get_env_var::<T>(&caps["env_name"]).map_err(serde::de::Error::custom);
        }
    }
    serde_json::from_value::<T>(value).map_err(serde::de::Error::custom)
}

fn deserialize_optional_data_or_env<'de, D, T>(data: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::de::DeserializeOwned + FromStr,
    <T as FromStr>::Err: std::fmt::Debug,
{
    Ok(match deserialize_data_or_env(data) {
        Ok(value) => Some(value),
        Err(err) => {
            tracing::warn!("Failed to deserialize_optional_data_or_env: {:?}", err);
            None
        }
    })
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct CommonConfig {
    pub general: general::CommonGeneralConfig,
    #[serde(default)]
    pub rightsizing: rightsizing::CommonRightsizingConfig,
    pub lake_config: lake::CommonLakeConfig,
    pub database: database::CommonDatabaseConfig,
    pub tx_details_storage: tx_details_storage::CommonTxDetailStorageConfig,
}

pub trait Config {
    fn from_common_config(common_config: CommonConfig) -> Self;
}

#[derive(Debug, Clone)]
pub struct RpcServerConfig {
    pub general: general::GeneralRpcServerConfig,
    pub lake_config: lake::LakeConfig,
    pub database: database::DatabaseConfig,
    pub tx_details_storage: tx_details_storage::TxDetailsStorageConfig,
}

impl Config for RpcServerConfig {
    fn from_common_config(common_config: CommonConfig) -> Self {
        Self {
            general: common_config.general.into(),
            lake_config: common_config.lake_config.into(),
            database: database::DatabaseRpcServerConfig::from(common_config.database).into(),
            tx_details_storage: tx_details_storage::TxDetailsStorageConfig::from(
                common_config.tx_details_storage,
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TxIndexerConfig {
    pub general: general::GeneralTxIndexerConfig,
    pub rightsizing: rightsizing::RightsizingConfig,
    pub lake_config: lake::LakeConfig,
    pub database: database::DatabaseConfig,
    pub tx_details_storage: tx_details_storage::TxDetailsStorageConfig,
}

impl TxIndexerConfig {
    pub fn tx_should_be_indexed(
        &self,
        transaction: &near_indexer_primitives::IndexerTransactionWithOutcome,
    ) -> bool {
        self.rightsizing.tx_should_be_indexed(transaction)
    }
}

impl Config for TxIndexerConfig {
    fn from_common_config(common_config: CommonConfig) -> Self {
        Self {
            general: common_config.general.into(),
            rightsizing: common_config.rightsizing.into(),
            lake_config: common_config.lake_config.into(),
            database: database::DatabaseTxIndexerConfig::from(common_config.database).into(),
            tx_details_storage: tx_details_storage::TxDetailsStorageConfig::from(
                common_config.tx_details_storage,
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StateIndexerConfig {
    pub general: general::GeneralStateIndexerConfig,
    pub rightsizing: rightsizing::RightsizingConfig,
    pub lake_config: lake::LakeConfig,
    pub database: database::DatabaseConfig,
}

impl StateIndexerConfig {
    pub fn state_should_be_indexed(&self, state_change_value: &StateChangeValueView) -> bool {
        self.rightsizing.state_should_be_indexed(state_change_value)
    }
}

impl Config for StateIndexerConfig {
    fn from_common_config(common_config: CommonConfig) -> Self {
        Self {
            general: common_config.general.into(),
            rightsizing: common_config.rightsizing.into(),
            lake_config: common_config.lake_config.into(),
            database: database::DatabaseStateIndexerConfig::from(common_config.database).into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NearStateIndexerConfig {
    pub general: general::GeneralNearStateIndexerConfig,
    pub rightsizing: rightsizing::RightsizingConfig,
    pub database: database::DatabaseConfig,
}

impl NearStateIndexerConfig {
    pub fn state_should_be_indexed(&self, state_change_value: &StateChangeValueView) -> bool {
        self.rightsizing.state_should_be_indexed(state_change_value)
    }
}

impl Config for NearStateIndexerConfig {
    fn from_common_config(common_config: CommonConfig) -> Self {
        Self {
            general: common_config.general.into(),
            rightsizing: common_config.rightsizing.into(),
            database: database::DatabaseStateIndexerConfig::from(common_config.database).into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EpochIndexerConfig {
    pub general: general::GeneralEpochIndexerConfig,
    pub lake_config: lake::LakeConfig,
    pub database: database::DatabaseConfig,
}

impl Config for EpochIndexerConfig {
    fn from_common_config(common_config: CommonConfig) -> Self {
        Self {
            general: common_config.general.into(),
            lake_config: common_config.lake_config.into(),
            database: database::DatabaseStateIndexerConfig::from(common_config.database).into(),
        }
    }
}
