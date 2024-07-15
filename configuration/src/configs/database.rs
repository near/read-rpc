use crate::configs::deserialize_data_or_env;
use near_lake_framework::near_indexer_primitives::near_primitives;

// Database connection URL
// Example: "postgres://user:password@localhost:5432/dbname"
type DatabaseConnectUrl = String;

#[derive(serde_derive::Deserialize, Debug, Clone, Default)]
pub struct ShardDatabaseConfig {
    #[serde(deserialize_with = "deserialize_data_or_env")]
    pub shard_id: u64,
    #[serde(deserialize_with = "deserialize_data_or_env")]
    pub database_url: DatabaseConnectUrl,
}

#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub database_url: DatabaseConnectUrl,
    pub shards_config:
        std::collections::HashMap<near_primitives::types::ShardId, DatabaseConnectUrl>,
    // Migrations cannot be applied to read-only replicas
    // We should run rpc-server only on read-only replicas
    pub read_only: bool,
}

impl DatabaseConfig {
    pub fn to_read_only(&self) -> Self {
        Self {
            database_url: self.database_url.clone(),
            shards_config: self.shards_config.clone(),
            read_only: true,
        }
    }
}

#[derive(serde_derive::Deserialize, Debug, Clone, Default)]
pub struct CommonDatabaseConfig {
    #[serde(deserialize_with = "deserialize_data_or_env")]
    pub database_url: DatabaseConnectUrl,
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
            read_only: false,
        }
    }
}
