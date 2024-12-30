use std::string::ToString;

use futures::executor::block_on;
use near_indexer_primitives::IndexerShard;
use near_primitives::epoch_manager::{AllEpochConfig, EpochConfig};

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
        fastnear_client: &near_lake_framework::FastNearClient,
    ) -> Self {
        tracing::info!("Get genesis config...");
        let genesis_config = near_rpc_client
            .call(
                near_jsonrpc_client::methods::EXPERIMENTAL_genesis_config::RpcGenesisConfigRequest,
                None,
            )
            .await
            .expect("Error to get genesis config");

        let genesis_block =
            near_lake_framework::fastnear::fetchers::fetch_first_block(fastnear_client).await;

        Self {
            genesis_config,
            genesis_block_cache: CacheBlock::from(&genesis_block.block),
        }
    }
}

#[derive(Clone)]
pub struct ServerContext {
    /// Fastnear client
    pub fastnear_client: near_lake_framework::FastNearClient,
    /// Database manager
    pub db_manager: std::sync::Arc<Box<dyn database::ReaderDbManager + Sync + Send + 'static>>,
    /// TransactionDetails storage
    pub tx_details_storage: std::sync::Arc<tx_details_storage::TxDetailsStorage>,
    /// Connection to cache storage with transactions in process
    pub tx_cache_storage: Option<cache_storage::TxIndexerCache>,
    /// Genesis info include genesis_config and genesis_block
    pub genesis_info: GenesisInfo,
    /// Near rpc client
    pub near_rpc_client: crate::utils::JsonRpcClient,
    /// Blocks cache
    pub blocks_cache: std::sync::Arc<crate::cache::RwLockLruMemoryCache<u64, CacheBlock>>,
    /// Chunks cache
    pub chunks_cache: std::sync::Arc<crate::cache::RwLockLruMemoryCache<u64, Vec<IndexerShard>>>,
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
    #[cfg(feature = "shadow-data-consistency")]
    pub shadow_data_consistency_rate: f64,
    /// Max size for state prefetch during a view_call
    pub prefetch_state_size_limit: u64,
    /// Port of the server.
    pub server_port: u16,
    /// Timestamp of starting server.
    pub boot_time_seconds: i64,
    /// Binary version.
    pub version: near_primitives::version::Version,
}

impl ServerContext {
    pub async fn init(rpc_server_config: configuration::RpcServerConfig) -> anyhow::Result<Self> {
        let contract_code_cache_size_in_bytes =
            crate::utils::gigabytes_to_bytes(rpc_server_config.general.contract_code_cache_size)
                .await;
        let contract_code_cache = std::sync::Arc::new(crate::cache::RwLockLruMemoryCache::new(
            contract_code_cache_size_in_bytes,
        ));

        // let block_cache_size_in_bytes =
        //     crate::utils::gigabytes_to_bytes(rpc_server_config.general.block_cache_size).await;

        // For chunk block we use 5GB. make it configurable. temporary hardcoded
        let block_cache_size_in_bytes = crate::utils::gigabytes_to_bytes(5.0).await;
        let blocks_cache = std::sync::Arc::new(crate::cache::RwLockLruMemoryCache::new(
            block_cache_size_in_bytes,
        ));

        // For chunk cache we use 5GB. make it configurable. temporary hardcoded
        let chunk_cache_size_in_bytes = crate::utils::gigabytes_to_bytes(5.0).await;
        let chunks_cache = std::sync::Arc::new(crate::cache::RwLockLruMemoryCache::new(
            chunk_cache_size_in_bytes,
        ));
        let near_rpc_client = crate::utils::JsonRpcClient::new(
            rpc_server_config.general.near_rpc_url.clone(),
            rpc_server_config.general.near_archival_rpc_url.clone(),
        );
        // We want to set a custom referer to let NEAR JSON RPC nodes know that we are a read-rpc instance
        let near_rpc_client = near_rpc_client.header(
            "Referer".to_string(),
            rpc_server_config.general.referer_header_value.clone(),
        )?;

        let fastnear_client = rpc_server_config
            .lake_config
            .lake_client(rpc_server_config.general.chain_id)
            .await?;

        let blocks_info_by_finality = std::sync::Arc::new(
            BlocksInfoByFinality::new(&near_rpc_client, &fastnear_client).await,
        );

        let tx_details_storage = tx_details_storage::TxDetailsStorage::new(
            rpc_server_config.tx_details_storage.storage_client().await,
            rpc_server_config.tx_details_storage.bucket_name.clone(),
        );

        let tx_cache_storage =
            cache_storage::TxIndexerCache::new(rpc_server_config.general.redis_url.to_string())
                .await
                .map_err(|err| {
                    tracing::warn!("Failed to connect to Redis: {:?}", err);
                })
                .ok();

        let genesis_info = GenesisInfo::get(&near_rpc_client, &fastnear_client).await;

        let default_epoch_config = EpochConfig::from(&genesis_info.genesis_config);
        let all_epoch_config = AllEpochConfig::new(
            true,
            genesis_info.genesis_config.protocol_version,
            default_epoch_config,
            &genesis_info.genesis_config.chain_id,
        );
        let epoch_config = all_epoch_config.for_protocol_version(
            blocks_info_by_finality
                .final_cache_block()
                .await
                .latest_protocol_version,
        );

        let db_manager = database::prepare_db_manager::<database::PostgresDBManager>(
            &rpc_server_config.database,
            epoch_config.shard_layout,
        )
        .await?;

        let compiled_contract_code_cache =
            std::sync::Arc::new(CompiledCodeCache::new(contract_code_cache_size_in_bytes));

        crate::metrics::CARGO_PKG_VERSION
            .with_label_values(&[NEARD_VERSION])
            .inc();

        Ok(Self {
            fastnear_client,
            db_manager: std::sync::Arc::new(Box::new(db_manager)),
            tx_details_storage: std::sync::Arc::new(tx_details_storage),
            tx_cache_storage,
            genesis_info,
            near_rpc_client,
            blocks_cache,
            chunks_cache,
            blocks_info_by_finality,
            compiled_contract_code_cache,
            contract_code_cache,
            max_gas_burnt: rpc_server_config.general.max_gas_burnt,
            #[cfg(feature = "shadow-data-consistency")]
            shadow_data_consistency_rate: rpc_server_config.general.shadow_data_consistency_rate,
            prefetch_state_size_limit: rpc_server_config.general.prefetch_state_size_limit,
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

impl crate::cache::ResidentSize for near_vm_runner::CompiledContractInfo {
    fn resident_size(&self) -> usize {
        borsh::to_vec(self).unwrap_or_default().len()
    }
}

impl crate::cache::ResidentSize for Vec<u8> {
    fn resident_size(&self) -> usize {
        self.len()
    }
}

impl crate::cache::ResidentSize for Vec<IndexerShard> {
    fn resident_size(&self) -> usize {
        serde_json::to_vec(self).unwrap_or_default().len()
    }
}

impl crate::cache::ResidentSize for CacheBlock {
    fn resident_size(&self) -> usize {
        serde_json::to_vec(self).unwrap_or_default().len()
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
