mod rpc_server;
pub use crate::base::rpc_server::ReaderDbManager;
pub mod state_indexer;
pub use crate::base::state_indexer::StateIndexerDbManager;
pub mod tx_indexer;
pub use crate::base::tx_indexer::TxIndexerDbManager;

/// Additional database options for scylla database
///
/// `preferred_dc: Option<String>`,
/// ScyllaDB preferred DataCenter
/// Accepts the DC name of the ScyllaDB to filter the connection to that DC only (preferrably).
/// If you connect to multi-DC cluter, you might experience big latencies while working with the DB.
/// This is due to the fact that ScyllaDB driver tries to connect to any of the nodes in the cluster
/// disregarding of the location of the DC. This option allows to filter the connection to the DC you need.
/// Example: "DC1" where DC1 is located in the same region as the application.
///
/// `keepalive_interval: Option<u64>`
/// ScyllaDB keepalive interval
///
/// `max_retry: u8`
/// Max retry count for ScyllaDB if `strict_mode` is `false`
///
/// `strict_mode: bool`
/// Attempts to store data in the database should be infinite to ensure no data is missing.
/// Disable it to perform a limited write attempts (`max_retry`)
/// before giving up, and moving to the next piece of data
///
/// `database_name: Option<String>`
/// Postgres database name
#[derive(Debug, Clone)]
pub struct AdditionalDatabaseOptions {
    #[cfg(feature = "scylla_db")]
    pub preferred_dc: Option<String>,
    #[cfg(feature = "scylla_db")]
    pub keepalive_interval: Option<u64>,
    #[cfg(feature = "scylla_db")]
    pub max_retry: u8,
    #[cfg(feature = "scylla_db")]
    pub strict_mode: bool,
    #[cfg(feature = "postgres_db")]
    pub database_name: Option<String>,
}

pub type PageToken = Option<String>;

#[async_trait::async_trait]
pub trait BaseDbManager {
    async fn new(config: &configuration::DatabaseConfig) -> anyhow::Result<Box<Self>>;
}
