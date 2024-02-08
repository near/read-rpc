mod base;

use crate::base::BaseDbManager;
pub use crate::base::PageToken;
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

pub async fn prepare_db_manager<T>(config: &configuration::DatabaseConfig) -> anyhow::Result<T>
where
    T: BaseDbManager + Send + Sync + 'static,
{
    Ok(*T::new(config).await?)
}
