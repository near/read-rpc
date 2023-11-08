pub mod rpc_server;
pub mod tx_indexer;
pub mod state_indexer;

pub struct AdditionalDatabaseOptions {}

#[async_trait::async_trait]
pub trait PostgresStorageManager {

}
