mod base;

use crate::base::BaseDbManager;
pub use crate::base::ReaderDbManager;
pub use crate::base::StateIndexerDbManager;
pub use crate::base::TxIndexerDbManager;
#[cfg(feature = "scylla_db")]
mod scylladb;
#[cfg(feature = "scylla_db")]
pub(crate) use crate::scylladb::base::ScyllaStorageManager;

pub mod primitives;

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
/// before skipping giving up and moving to the next piece of data
#[cfg(feature = "scylla_db")]
pub struct AdditionalDatabaseOptions {
    pub preferred_dc: Option<String>,
    pub keepalive_interval: Option<u64>,
    pub max_retry: u8,
    pub strict_mode: bool,
}

/// Additional database options for postgres database(WIP)
#[cfg(not(feature = "scylla_db"))]
pub struct AdditionalDatabaseOptions {}

async fn prepare_db_manager<T>(
    database_url: &str,
    database_user: Option<&str>,
    database_password: Option<&str>,
    database_options: AdditionalDatabaseOptions,
) -> anyhow::Result<T>
where
    T: BaseDbManager + Send + Sync + 'static,
{
    Ok(*T::new(
        database_url,
        database_user,
        database_password,
        database_options,
    )
    .await?)
}

pub async fn prepare_read_rpc_db_manager(
    database_url: &str,
    database_user: Option<&str>,
    database_password: Option<&str>,
    database_options: AdditionalDatabaseOptions,
) -> anyhow::Result<impl ReaderDbManager + Send + Sync + 'static> {
    #[cfg(feature = "scylla_db")]
    prepare_db_manager::<scylladb::rpc_server::ScyllaDBManager>(
        database_url,
        database_user,
        database_password,
        database_options,
    )
    .await
}

pub async fn prepare_stata_indexer_db_manager(
    database_url: &str,
    database_user: Option<&str>,
    database_password: Option<&str>,
    database_options: AdditionalDatabaseOptions,
) -> anyhow::Result<impl StateIndexerDbManager + Send + Sync + 'static> {
    #[cfg(feature = "scylla_db")]
    prepare_db_manager::<scylladb::state_indexer::ScyllaDBManager>(
        database_url,
        database_user,
        database_password,
        database_options,
    )
    .await
}

pub async fn prepare_tx_indexer_db_manager(
    database_url: &str,
    database_user: Option<&str>,
    database_password: Option<&str>,
    database_options: AdditionalDatabaseOptions,
) -> anyhow::Result<impl TxIndexerDbManager + Send + Sync + 'static> {
    #[cfg(feature = "scylla_db")]
    prepare_db_manager::<scylladb::tx_indexer::ScyllaDBManager>(
        database_url,
        database_user,
        database_password,
        database_options,
    )
    .await
}
