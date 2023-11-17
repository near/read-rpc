mod base;

pub use crate::base::AdditionalDatabaseOptions;
use crate::base::BaseDbManager;
pub use crate::base::ReaderDbManager;
pub use crate::base::StateIndexerDbManager;
pub use crate::base::TxIndexerDbManager;

#[cfg(feature = "scylla_db")]
pub mod scylladb;

#[cfg(feature = "postgres_db")]
pub extern crate diesel;
#[cfg(feature = "postgres_db")]
pub mod postgres;
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

#[cfg(feature = "scylla_db")]
pub async fn prepare_read_rpc_scylla_db_manager(
    database_url: &str,
    database_user: Option<&str>,
    database_password: Option<&str>,
    database_options: AdditionalDatabaseOptions,
) -> anyhow::Result<impl ReaderDbManager + Send + Sync + 'static> {
    let db_manager = prepare_db_manager::<scylladb::rpc_server::ScyllaDBManager>(
        database_url,
        database_user,
        database_password,
        database_options,
    )
    .await?;
    Ok(db_manager)
}

#[cfg(feature = "postgres_db")]
pub async fn prepare_read_rpc_postgres_db_manager(
    database_url: &str,
    database_user: Option<&str>,
    database_password: Option<&str>,
    database_options: AdditionalDatabaseOptions,
) -> anyhow::Result<impl ReaderDbManager + Send + Sync + 'static> {
    let db_manager = prepare_db_manager::<postgres::rpc_server::PostgresDBManager>(
        database_url,
        database_user,
        database_password,
        database_options,
    )
    .await?;

    Ok(db_manager)
}

#[cfg(feature = "scylla_db")]
pub async fn prepare_state_indexer_scylla_db_manager(
    database_url: &str,
    database_user: Option<&str>,
    database_password: Option<&str>,
    database_options: AdditionalDatabaseOptions,
) -> anyhow::Result<impl StateIndexerDbManager + Send + Sync + 'static> {
    let db_manager = prepare_db_manager::<scylladb::state_indexer::ScyllaDBManager>(
        database_url,
        database_user,
        database_password,
        database_options,
    )
    .await?;
    Ok(db_manager)
}

#[cfg(feature = "postgres_db")]
pub async fn prepare_state_indexer_postgres_db_manager(
    database_url: &str,
    database_user: Option<&str>,
    database_password: Option<&str>,
    database_options: AdditionalDatabaseOptions,
) -> anyhow::Result<impl StateIndexerDbManager + Send + Sync + 'static> {
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

#[cfg(feature = "scylla_db")]
pub async fn prepare_tx_indexer_scylla_db_manager(
    database_url: &str,
    database_user: Option<&str>,
    database_password: Option<&str>,
    database_options: AdditionalDatabaseOptions,
) -> anyhow::Result<impl TxIndexerDbManager + Send + Sync + 'static> {
    let db_manager = prepare_db_manager::<scylladb::tx_indexer::ScyllaDBManager>(
        database_url,
        database_user,
        database_password,
        database_options,
    )
    .await?;
    Ok(db_manager)
}

#[cfg(feature = "postgres_db")]
pub async fn prepare_tx_indexer_postgres_db_manager(
    database_url: &str,
    database_user: Option<&str>,
    database_password: Option<&str>,
    database_options: AdditionalDatabaseOptions,
) -> anyhow::Result<impl TxIndexerDbManager + Send + Sync + 'static> {
    let db_manager = prepare_db_manager::<postgres::tx_indexer::PostgresDBManager>(
        database_url,
        database_user,
        database_password,
        database_options,
    )
    .await?;

    Ok(db_manager)
}
