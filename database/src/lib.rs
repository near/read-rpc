mod base;

use crate::base::BaseDbManager;
pub use crate::base::ReaderDbManager;
pub use crate::base::StateIndexerDbManager;
pub use crate::base::TxIndexerDbManager;

#[cfg(feature = "scylla_db")]
mod scylladb;
#[cfg(feature = "scylla_db")]
pub use crate::scylladb::AdditionalDatabaseOptions;

#[cfg(not(feature = "scylla_db"))]
pub use crate::base::AdditionalDatabaseOptions;

pub mod primitives;

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

pub async fn prepare_state_indexer_db_manager(
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
