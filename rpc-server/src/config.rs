use crate::modules::blocks::{CacheBlock, FinalBlockInfo};
use futures::executor::block_on;

#[derive(Debug, Clone)]
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

#[derive(Clone)]
pub struct ServerContext {
    pub s3_client: near_lake_framework::s3_fetchers::LakeS3Client,
    pub db_manager: std::sync::Arc<Box<dyn database::ReaderDbManager + Sync + Send + 'static>>,
    pub genesis_info: GenesisInfo,
    pub near_rpc_client: crate::utils::JsonRpcClient,
    pub s3_bucket_name: String,
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
    pub async fn init(
        rpc_server_config: configuration::RpcServerConfig,
        near_rpc_client: crate::utils::JsonRpcClient,
    ) -> anyhow::Result<Self> {
        let limit_memory_cache_in_bytes =
            if let Some(limit_memory_cache) = rpc_server_config.general.limit_memory_cache {
                Some(crate::utils::gigabytes_to_bytes(limit_memory_cache).await)
            } else {
                None
            };
        let reserved_memory_in_bytes =
            crate::utils::gigabytes_to_bytes(rpc_server_config.general.reserved_memory).await;
        let block_cache_size_in_bytes =
            crate::utils::gigabytes_to_bytes(rpc_server_config.general.block_cache_size).await;

        let contract_code_cache_size = crate::utils::calculate_contract_code_cache_sizes(
            reserved_memory_in_bytes,
            block_cache_size_in_bytes,
            limit_memory_cache_in_bytes,
        )
        .await;
        let blocks_cache = std::sync::Arc::new(futures_locks::RwLock::new(
            crate::cache::LruMemoryCache::new(block_cache_size_in_bytes),
        ));

        let contract_code_cache = std::sync::Arc::new(futures_locks::RwLock::new(
            crate::cache::LruMemoryCache::new(contract_code_cache_size),
        ));

        let compiled_contract_code_cache = std::sync::Arc::new(CompiledCodeCache {
            local_cache: std::sync::Arc::new(futures_locks::RwLock::new(
                crate::cache::LruMemoryCache::new(contract_code_cache_size),
            )),
        });

        let final_block_info = std::sync::Arc::new(futures_locks::RwLock::new(
            FinalBlockInfo::new(&near_rpc_client, &blocks_cache).await,
        ));

        #[cfg(feature = "scylla_db")]
        let db_manager = database::prepare_db_manager::<
            database::scylladb::rpc_server::ScyllaDBManager,
        >(&rpc_server_config.database)
        .await?;

        #[cfg(all(feature = "postgres_db", not(feature = "scylla_db")))]
        let db_manager = database::prepare_db_manager::<
            database::postgres::rpc_server::PostgresDBManager,
        >(&rpc_server_config.database)
        .await?;

        // let genesis_info = GenesisInfo::get(&near_rpc_client).await;

        Ok(Self {
            s3_client: rpc_server_config.lake_config.lake_s3_client().await,
            db_manager: std::sync::Arc::new(Box::new(db_manager)),
            genesis_info: GenesisInfo::get(&near_rpc_client).await,
            near_rpc_client,
            s3_bucket_name: rpc_server_config.lake_config.aws_bucket_name.clone(),
            blocks_cache,
            final_block_info,
            compiled_contract_code_cache,
            contract_code_cache,
            max_gas_burnt: rpc_server_config.general.max_gas_burnt,
            shadow_data_consistency_rate: rpc_server_config.general.shadow_data_consistency_rate,
        })
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
