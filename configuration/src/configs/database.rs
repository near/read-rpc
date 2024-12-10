use near_lake_framework::near_indexer_primitives::near_primitives;
use validator::Validate;

use crate::configs::{deserialize_data_or_env, deserialize_optional_data_or_env};

// Database connection URL
// Example: "postgres://user:password@localhost:5432/dbname"
type DatabaseConnectUrl = String;

#[derive(Validate, serde_derive::Deserialize, Debug, Clone, Default)]
pub struct ShardDatabaseConfig {
    #[serde(deserialize_with = "deserialize_data_or_env")]
    pub shard_id: u64,
    #[validate(url(message = "Invalid database shard URL"))]
    #[serde(deserialize_with = "deserialize_data_or_env")]
    pub database_url: DatabaseConnectUrl,
}

#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub database_url: DatabaseConnectUrl,
    pub shards_config:
        std::collections::HashMap<near_primitives::types::ShardId, DatabaseConnectUrl>,
    pub max_connections: u32,
    // Migrations cannot be applied to read-only replicas
    // We should run rpc-server only on read-only replicas
    pub read_only: bool,
}

impl DatabaseConfig {
    pub fn to_read_only(&self) -> Self {
        Self {
            database_url: self.database_url.clone(),
            shards_config: self.shards_config.clone(),
            max_connections: self.max_connections,
            read_only: true,
        }
    }
}

#[derive(Validate, serde_derive::Deserialize, Debug, Clone, Default)]
pub struct CommonDatabaseConfig {
    #[validate(url(message = "Invalid database URL"))]
    #[serde(deserialize_with = "deserialize_data_or_env")]
    pub database_url: DatabaseConnectUrl,
    #[validate(nested)]
    #[serde(default)]
    pub shards: Vec<ShardDatabaseConfig>,
    #[serde(deserialize_with = "deserialize_optional_data_or_env", default)]
    pub max_connections: Option<u32>,
}

impl CommonDatabaseConfig {
    //The maximum number of connections that this pool should maintain.
    // Be mindful of the connection limits for your database as well
    // as other applications which may want to connect to the same database
    // (or even multiple instances of the same application in high-availability deployments).
    //
    // A production application will want to set a higher limit than this.
    // Start with connections based on 4x the number of CPU cores.
    pub fn default_max_connections() -> u32 {
        10
    }
}

impl From<CommonDatabaseConfig> for DatabaseConfig {
    fn from(database_config: CommonDatabaseConfig) -> Self {
        Self {
            database_url: database_config.database_url,
            shards_config: database_config
                .shards
                .into_iter()
                .map(|shard| (shard.shard_id.into(), shard.database_url))
                .collect(),
            max_connections: database_config
                .max_connections
                .unwrap_or_else(CommonDatabaseConfig::default_max_connections),
            read_only: false,
        }
    }
}
