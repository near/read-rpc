mod rpc_server;
pub use crate::base::rpc_server::ReaderDbManager;
pub mod state_indexer;
pub use crate::base::state_indexer::StateIndexerDbManager;
pub mod tx_indexer;
pub use crate::base::tx_indexer::TxIndexerDbManager;

#[async_trait::async_trait]
pub trait BaseDbManager {
    async fn new(
        database_url: &str,
        database_user: Option<&str>,
        database_password: Option<&str>,
        database_options: crate::AdditionalDatabaseOptions,
    ) -> anyhow::Result<Box<Self>>;
}
