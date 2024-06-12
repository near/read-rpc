use serde_derive::Deserialize;

use crate::configs::{deserialize_data_or_env, deserialize_optional_data_or_env};

#[derive(Deserialize, Debug, Clone, Default)]
pub struct ShardDatabaseConfig {
    #[serde(deserialize_with = "deserialize_data_or_env")]
    pub shard_id: u64,
    #[serde(deserialize_with = "deserialize_data_or_env")]
    pub database_url: String,
}

#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub database_url: String,
    pub shards_config: std::collections::HashMap<u64, String>,
}

#[derive(Deserialize, Debug, Clone, Default)]
pub struct CommonDatabaseConfig {
    #[serde(deserialize_with = "deserialize_data_or_env")]
    pub database_url: String,
    #[serde(default)]
    pub shards: Option<Vec<ShardDatabaseConfig>>,
}

impl From<CommonDatabaseConfig> for DatabaseConfig {
    fn from(database_config: CommonDatabaseConfig) -> Self {
        Self {
            database_url: database_config.database_url,
            shards_config: database_config.shards.unwrap_or_default().into_iter().map(|shard| (shard.shard_id, shard.database_url)).collect(),
        }
    }
}
