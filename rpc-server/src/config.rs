use std::string::ToString;

use futures::executor::block_on;

use crate::modules::blocks::{BlocksInfoByFinality, CacheBlock};

static NEARD_VERSION: &str = env!("CARGO_PKG_VERSION");
static NEARD_BUILD: &str = env!("BUILD_VERSION");
static RUSTC_VERSION: &str = env!("RUSTC_VERSION");

// Struct to store genesis_config and genesis_block in the server context
// Fetch once genesis info on start of the server and put it in the context
#[derive(Debug, Clone)]
pub struct GenesisInfo {
    pub genesis_config: near_chain_configs::GenesisConfig,
    pub genesis_block_cache: CacheBlock,
}

impl GenesisInfo {
    pub async fn get(
        near_rpc_client: &crate::utils::JsonRpcClient,
        s3_client: &near_lake_framework::s3_fetchers::LakeS3Client,
        s3_bucket_name: &str,
    ) -> Self {
        tracing::info!("Get genesis config...");
        let genesis_config = near_rpc_client
            .call(
                near_jsonrpc_client::methods::EXPERIMENTAL_genesis_config::RpcGenesisConfigRequest,
            )
            .await
            .expect("Error to get genesis config");

        let genesis_block = near_lake_framework::s3_fetchers::fetch_block(
            s3_client,
            s3_bucket_name,
            genesis_config.genesis_height,
        )
        .await
        .expect("Error to get genesis block");

        Self {
            genesis_config,
            genesis_block_cache: CacheBlock::from(&genesis_block),
        }
    }
}

#[derive(Clone)]
pub struct ServerContext {
    /// Lake s3 client
    pub s3_client: near_lake_framework::s3_fetchers::LakeS3Client,
    /// Database manager
    pub db_manager: std::sync::Arc<Box<dyn database::ReaderDbManager + Sync + Send + 'static>>,
    /// Genesis info include genesis_config and genesis_block
    pub genesis_info: GenesisInfo,
    /// Near rpc client
    pub near_rpc_client: crate::utils::JsonRpcClient,
    /// AWS s3 lake bucket name
    pub s3_bucket_name: String,
    /// Blocks cache
    pub blocks_cache: std::sync::Arc<crate::cache::RwLockLruMemoryCache<u64, CacheBlock>>,
    /// Final block info include final_block_cache and current_validators_info
    pub blocks_info_by_finality: std::sync::Arc<BlocksInfoByFinality>,
    /// Cache to store compiled contract codes
    pub compiled_contract_code_cache: std::sync::Arc<CompiledCodeCache>,
    /// Cache to store contract codes
    pub contract_code_cache: std::sync::Arc<
        crate::cache::RwLockLruMemoryCache<near_primitives::hash::CryptoHash, Vec<u8>>,
    >,
    /// Max gas burnt for contract function call
    pub max_gas_burnt: near_primitives::types::Gas,
    /// How many requests we should check for data consistency
    pub shadow_data_consistency_rate: f64,
    /// Port of the server.
    pub server_port: u16,
    /// Timestamp of starting server.
    pub boot_time_seconds: i64,
    /// Binary version.
    pub version: near_primitives::version::Version,
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
        let blocks_cache = std::sync::Arc::new(crate::cache::RwLockLruMemoryCache::new(
            block_cache_size_in_bytes,
        ));

        let contract_code_cache = std::sync::Arc::new(crate::cache::RwLockLruMemoryCache::new(
            contract_code_cache_size,
        ));

        let blocks_info_by_finality =
            std::sync::Arc::new(BlocksInfoByFinality::new(&near_rpc_client, &blocks_cache).await);

        let s3_client = rpc_server_config.lake_config.lake_s3_client().await;

        let db_manager =
            database::prepare_db_manager::<database::ReaderDBManager>(&rpc_server_config.database)
                .await?;

        let genesis_info = GenesisInfo::get(
            &near_rpc_client,
            &s3_client,
            &rpc_server_config.lake_config.aws_bucket_name,
        )
        .await;

        Ok(Self {
            s3_client,
            db_manager: std::sync::Arc::new(Box::new(db_manager)),
            genesis_info,
            near_rpc_client,
            s3_bucket_name: rpc_server_config.lake_config.aws_bucket_name.clone(),
            blocks_cache,
            blocks_info_by_finality,
            compiled_contract_code_cache: std::sync::Arc::new(CompiledCodeCache::new(
                contract_code_cache_size,
            )),
            contract_code_cache,
            max_gas_burnt: rpc_server_config.general.max_gas_burnt,
            shadow_data_consistency_rate: rpc_server_config.general.shadow_data_consistency_rate,
            server_port: rpc_server_config.general.server_port,
            boot_time_seconds: chrono::Utc::now().timestamp(),
            version: near_primitives::version::Version {
                version: NEARD_VERSION.to_string(),
                build: NEARD_BUILD.to_string(),
                rustc_version: RUSTC_VERSION.to_string(),
            },
        })
    }
}

#[derive(Clone)]
pub struct CompiledCodeCache {
    pub local_cache: std::sync::Arc<
        crate::cache::RwLockLruMemoryCache<
            near_primitives::hash::CryptoHash,
            near_vm_runner::CompiledContractInfo,
        >,
    >,
}

impl CompiledCodeCache {
    pub fn new(contract_code_cache_size: usize) -> Self {
        Self {
            local_cache: std::sync::Arc::new(crate::cache::RwLockLruMemoryCache::new(
                contract_code_cache_size,
            )),
        }
    }
}

impl near_vm_runner::ContractRuntimeCache for CompiledCodeCache {
    fn handle(&self) -> Box<dyn near_vm_runner::ContractRuntimeCache> {
        Box::new(self.clone())
    }

    fn put(
        &self,
        key: &near_primitives::hash::CryptoHash,
        value: near_vm_runner::CompiledContractInfo,
    ) -> std::io::Result<()> {
        block_on(self.local_cache.put(*key, value));
        Ok(())
    }

    fn get(
        &self,
        key: &near_primitives::hash::CryptoHash,
    ) -> std::io::Result<Option<near_vm_runner::CompiledContractInfo>> {
        Ok(block_on(self.local_cache.get(key)))
    }

    fn has(&self, key: &near_primitives::hash::CryptoHash) -> std::io::Result<bool> {
        Ok(block_on(self.local_cache.contains(key)))
    }
}
