use std::str::FromStr;

use near_lake_framework::{
    near_indexer_primitives, near_indexer_primitives::views::StateChangeValueView,
};
use serde::Deserialize;

pub(crate) mod database;
pub(crate) mod general;
mod lake;
mod rightsizing;

lazy_static::lazy_static! {
    static ref RE_NAME_ENV: regex::Regex = regex::Regex::new(r"\$\{(?<env_name>\w+)}").unwrap();
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
pub struct Config {
    pub general: general::GeneralConfig,
    #[serde(default)]
    pub rightsizing: rightsizing::RightsizingConfig,
    pub lake_config: lake::LakeConfig,
    pub database: database::DatabaseConfig,
}

impl Config {
    pub fn state_should_be_indexed(&self, state_change_value: &StateChangeValueView) -> bool {
        self.rightsizing.state_should_be_indexed(state_change_value)
    }

    pub fn tx_should_be_indexed(
        &self,
        transaction: &near_indexer_primitives::IndexerTransactionWithOutcome,
    ) -> bool {
        self.rightsizing.tx_should_be_indexed(transaction)
    }

    pub async fn to_lake_config(
        &self,
        start_block_height: near_indexer_primitives::types::BlockHeight,
    ) -> anyhow::Result<near_lake_framework::LakeConfig> {
        self.lake_config.lake_config(start_block_height).await
    }

    pub async fn to_s3_client(&self) -> near_lake_framework::s3_fetchers::LakeS3Client {
        let s3_config = self.lake_config.s3_config().await;
        near_lake_framework::s3_fetchers::LakeS3Client::new(aws_sdk_s3::Client::from_conf(
            s3_config,
        ))
    }
}
