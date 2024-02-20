use crate::modules::blocks::{CacheBlock, FinalBlockInfo};
use futures::executor::block_on;

#[derive(Debug)]
pub struct GenesisInfo {
    pub genesis_config: near_chain_configs::GenesisConfig,
    pub genesis_block_cache: CacheBlock,
}

impl GenesisInfo {
    pub async fn get(near_rpc_client: &crate::utils::JsonRpcClient) -> Self {
        tracing::info!("Get genesis config...");
        let genesis_config = near_rpc_client
            .call(
                near_jsonrpc_client::methods::EXPERIMENTAL_genesis_config::RpcGenesisConfigRequest,
            )
            .await
            .expect("Error to get genesis config");
        let genesis_block = near_rpc_client
            .archival_call(near_jsonrpc_client::methods::block::RpcBlockRequest {
                block_reference: near_primitives::types::BlockReference::BlockId(
                    near_primitives::types::BlockId::Height(genesis_config.genesis_height),
                ),
            })
            .await
            .expect("Error to get genesis block");
        Self {
            genesis_config,
            genesis_block_cache: genesis_block.into(),
        }
    }
}

pub struct ServerContext {
    pub s3_client: near_lake_framework::s3_fetchers::LakeS3Client,
    pub db_manager: std::sync::Arc<Box<dyn database::ReaderDbManager + Sync + Send + 'static>>,
    pub near_rpc_client: crate::utils::JsonRpcClient,
    pub s3_bucket_name: String,
    pub genesis_info: GenesisInfo,
    pub blocks_cache:
        std::sync::Arc<futures_locks::RwLock<crate::cache::LruMemoryCache<u64, CacheBlock>>>,
    pub final_block_info: std::sync::Arc<futures_locks::RwLock<FinalBlockInfo>>,
    pub compiled_contract_code_cache: std::sync::Arc<CompiledCodeCache>,
    pub contract_code_cache: std::sync::Arc<
        futures_locks::RwLock<
            crate::cache::LruMemoryCache<near_primitives::hash::CryptoHash, Vec<u8>>,
        >,
    >,
    pub max_gas_burnt: near_primitives::types::Gas,
    pub shadow_data_consistency_rate: f64,
}

impl ServerContext {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        s3_client: near_lake_framework::s3_fetchers::LakeS3Client,
        db_manager: impl database::ReaderDbManager + Sync + Send + 'static,
        near_rpc_client: crate::utils::JsonRpcClient,
        s3_bucket_name: String,
        genesis_info: GenesisInfo,
        blocks_cache: std::sync::Arc<
            futures_locks::RwLock<crate::cache::LruMemoryCache<u64, CacheBlock>>,
        >,
        final_block_info: std::sync::Arc<futures_locks::RwLock<FinalBlockInfo>>,
        compiled_contract_code_cache: std::sync::Arc<CompiledCodeCache>,
        contract_code_cache: std::sync::Arc<
            futures_locks::RwLock<
                crate::cache::LruMemoryCache<near_primitives::hash::CryptoHash, Vec<u8>>,
            >,
        >,
        max_gas_burnt: near_primitives::types::Gas,
        shadow_data_consistency_rate: f64,
    ) -> Self {
        Self {
            s3_client,
            db_manager: std::sync::Arc::new(Box::new(db_manager)),
            near_rpc_client,
            s3_bucket_name,
            genesis_info,
            blocks_cache,
            final_block_info,
            compiled_contract_code_cache,
            contract_code_cache,
            max_gas_burnt,
            shadow_data_consistency_rate,
        }
    }
}
pub struct CompiledCodeCache {
    pub local_cache: std::sync::Arc<
        futures_locks::RwLock<
            crate::cache::LruMemoryCache<
                near_primitives::hash::CryptoHash,
                near_vm_runner::logic::CompiledContract,
            >,
        >,
    >,
}

impl near_vm_runner::logic::CompiledContractCache for CompiledCodeCache {
    fn put(
        &self,
        key: &near_primitives::hash::CryptoHash,
        value: near_vm_runner::logic::CompiledContract,
    ) -> std::io::Result<()> {
        block_on(self.local_cache.write()).put(*key, value);
        Ok(())
    }

    fn get(
        &self,
        key: &near_primitives::hash::CryptoHash,
    ) -> std::io::Result<Option<near_vm_runner::logic::CompiledContract>> {
        Ok(block_on(self.local_cache.write()).get(key).cloned())
    }

    fn has(&self, key: &near_primitives::hash::CryptoHash) -> std::io::Result<bool> {
        Ok(block_on(self.local_cache.write()).contains(key))
    }
}
