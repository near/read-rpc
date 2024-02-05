mod rpc_server;
pub use crate::base::rpc_server::ReaderDbManager;
pub mod state_indexer;
pub use crate::base::state_indexer::StateIndexerDbManager;
pub mod tx_indexer;
pub use crate::base::tx_indexer::TxIndexerDbManager;

pub type PageToken = Option<String>;

#[async_trait::async_trait]
pub trait BaseDbManager {
    async fn new(config: &configuration::DatabaseConfig) -> anyhow::Result<Box<Self>>;
}
