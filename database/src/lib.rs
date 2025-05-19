#[macro_use]
extern crate lazy_static;

mod base;

use crate::base::BaseDbManager;
pub use crate::base::PageToken;
pub use crate::base::ReaderDbManager;
pub use crate::base::StateIndexerDbManager;
pub use crate::base::TxIndexerDbManager;

mod metrics;
mod postgres;
pub mod primitives;

pub use crate::postgres::PostgresDBManager;

pub async fn prepare_db_manager<T>(config: &configuration::DatabaseConfig) -> anyhow::Result<T>
where
    T: BaseDbManager + Send + Sync + 'static,
{
    Ok(*T::new(config).await?)
}
