pub use crate::base::rpc_server::ReaderDbManager;
pub use crate::base::state_indexer::StateIndexerDbManager;
pub use crate::base::tx_indexer::TxIndexerDbManager;

mod rpc_server;
pub mod state_indexer;
pub mod tx_indexer;

pub type PageToken = Option<String>;

#[async_trait::async_trait]
pub trait BaseDbManager {
    async fn new(config: &configuration::DatabaseConfig) -> anyhow::Result<Box<Self>>;
}
