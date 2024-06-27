use crate::configs::deserialize_data_or_env;
use near_lake_framework::near_indexer_primitives::near_primitives;

#[derive(serde_derive::Deserialize, Debug, Clone, Default)]
pub struct ShardDatabaseConfig {
    #[serde(deserialize_with = "deserialize_data_or_env")]
    pub shard_id: u64,
    #[serde(deserialize_with = "deserialize_data_or_env")]
    pub database_url: String,
}

#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub database_url: String,
    pub shards_config: std::collections::HashMap<near_primitives::types::ShardId, String>,
}

#[derive(serde_derive::Deserialize, Debug, Clone, Default)]
pub struct CommonDatabaseConfig {
    #[serde(deserialize_with = "deserialize_data_or_env")]
    pub database_url: String,
    #[serde(default)]
    pub shards: Vec<ShardDatabaseConfig>,
}

impl From<CommonDatabaseConfig> for DatabaseConfig {
    fn from(database_config: CommonDatabaseConfig) -> Self {
        Self {
            database_url: database_config.database_url,
            shards_config: database_config
                .shards
                .into_iter()
                .map(|shard| (shard.shard_id, shard.database_url))
                .collect(),
        }
    }
}
