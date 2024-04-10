mod base;

use crate::base::BaseDbManager;
pub use crate::base::PageToken;
pub use crate::base::ReaderDbManager;
pub use crate::base::StateIndexerDbManager;
pub use crate::base::TxIndexerDbManager;

#[cfg(all(feature = "scylla_db", not(feature = "postgres_db"), not(feature = "mock_db")))]
pub mod scylladb;

#[cfg(all(feature = "postgres_db", not(feature = "mock_db")))]
pub extern crate diesel;
#[cfg(all(feature = "postgres_db", not(feature = "mock_db")))]
pub mod postgres;
#[cfg(all(feature = "postgres_db", not(feature = "mock_db")))]
pub use postgres::{models, schema};

#[cfg(feature = "mock_db")]
pub mod mockdb;

pub mod primitives;

#[cfg(all(feature = "scylla_db", not(feature = "postgres_db"), not(feature = "mock_db")))]
pub type ReaderDBManager = scylladb::rpc_server::ScyllaDBManager;
#[cfg(all(feature = "postgres_db", not(feature = "mock_db")))]
pub type ReaderDBManager = postgres::rpc_server::PostgresDBManager;
#[cfg(feature = "mock_db")]
pub type ReaderDBManager = mockdb::rpc_server::MockDBManager;

#[cfg(all(feature = "scylla_db", not(feature = "postgres_db"), not(feature = "mock_db")))]
pub type StateIndexerDBManager = scylladb::state_indexer::ScyllaDBManager;
#[cfg(all(feature = "postgres_db", not(feature = "mock_db")))]
pub type StateIndexerDBManager = postgres::state_indexer::PostgresDBManager;
#[cfg(feature = "mock_db")]
pub type StateIndexerDBManager = mockdb::state_indexer::MockDBManager;

#[cfg(all(feature = "scylla_db", not(feature = "postgres_db"), not(feature = "mock_db")))]
pub type TxIndexerDBManager = scylladb::tx_indexer::ScyllaDBManager;
#[cfg(all(feature = "postgres_db", not(feature = "mock_db")))]
pub type TxIndexerDBManager = postgres::tx_indexer::PostgresDBManager;
#[cfg(feature = "mock_db")]
pub type TxIndexerDBManager = mockdb::tx_indexer::MockDBManager;

pub async fn prepare_db_manager<T>(config: &configuration::DatabaseConfig) -> anyhow::Result<T>
where
    T: BaseDbManager + Send + Sync + 'static,
{
    Ok(*T::new(config).await?)
}
