#[cfg(all(feature = "scylla_db", feature = "postgres_db"))]
compile_error!(
    "feature \"scylla_db\" and feature \"postgres_db\" cannot be enabled at the same time"
);

mod base;

use crate::base::BaseDbManager;
pub use crate::base::ReaderDbManager;
pub use crate::base::StateIndexerDbManager;
pub use crate::base::TxIndexerDbManager;

#[cfg(feature = "scylla_db")]
mod scylladb;
#[cfg(feature = "scylla_db")]
pub use crate::scylladb::AdditionalDatabaseOptions;

#[cfg(feature = "postgres_db")]
pub extern crate diesel;
#[cfg(feature = "postgres_db")]
pub mod postgres;
#[cfg(feature = "postgres_db")]
pub use crate::postgres::AdditionalDatabaseOptions;
#[cfg(feature = "postgres_db")]
pub use postgres::{models, schema};

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
    let db_manager = prepare_db_manager::<scylladb::rpc_server::ScyllaDBManager>(
        database_url,
        database_user,
        database_password,
        database_options,
    )
    .await?;

    #[cfg(feature = "postgres_db")]
    let db_manager = prepare_db_manager::<postgres::rpc_server::PostgresDBManager>(
        database_url,
        database_user,
        database_password,
        database_options,
    )
    .await?;

    Ok(db_manager)
}

pub async fn prepare_state_indexer_db_manager(
    database_url: &str,
    database_user: Option<&str>,
    database_password: Option<&str>,
    database_options: AdditionalDatabaseOptions,
) -> anyhow::Result<impl StateIndexerDbManager + Send + Sync + 'static> {
    #[cfg(feature = "scylla_db")]
    let db_manager = prepare_db_manager::<scylladb::state_indexer::ScyllaDBManager>(
        database_url,
        database_user,
        database_password,
        database_options,
    )
    .await?;

    #[cfg(feature = "postgres_db")]
    let db_manager = prepare_db_manager::<postgres::state_indexer::PostgresDBManager>(
        database_url,
        database_user,
        database_password,
        database_options,
    )
    .await?;

    Ok(db_manager)
}

pub async fn prepare_tx_indexer_db_manager(
    database_url: &str,
    database_user: Option<&str>,
    database_password: Option<&str>,
    database_options: AdditionalDatabaseOptions,
) -> anyhow::Result<impl TxIndexerDbManager + Send + Sync + 'static> {
    #[cfg(feature = "scylla_db")]
    let db_manager = prepare_db_manager::<scylladb::tx_indexer::ScyllaDBManager>(
        database_url,
        database_user,
        database_password,
        database_options,
    )
    .await?;

    #[cfg(feature = "postgres_db")]
    let db_manager = prepare_db_manager::<postgres::tx_indexer::PostgresDBManager>(
        database_url,
        database_user,
        database_password,
        database_options,
    )
    .await?;

    Ok(db_manager)
}
