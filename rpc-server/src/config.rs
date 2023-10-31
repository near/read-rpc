use crate::modules::blocks::CacheBlock;
use clap::Parser;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
pub struct Opts {
    // near network rpc url
    #[clap(long, env = "NEAR_RPC_URL")]
    pub rpc_url: http::Uri,

    // Indexer bucket name
    #[clap(long, env = "AWS_BUCKET_NAME")]
    pub s3_bucket_name: String,

    // DB url
    #[clap(long, env)]
    pub database_url: String,

    /// DB user(login)
    #[clap(long, env)]
    pub database_user: Option<String>,

    ///DB password
    #[clap(long, env)]
    pub database_password: Option<String>,

    // AWS access key id
    #[clap(long, env = "AWS_ACCESS_KEY_ID")]
    pub access_key_id: String,

    // AWS secret access key
    #[clap(long, env = "AWS_SECRET_ACCESS_KEY")]
    pub secret_access_key: String,

    // AWS default region
    #[clap(long, env = "AWS_DEFAULT_REGION")]
    pub region: String,

    // AWS default region
    #[clap(long, env, default_value = "8000")]
    pub server_port: u16,

    /// Max gas burnt for contract function call
    /// Default value is 300_000_000_000_000
    #[clap(long, env, default_value = "300000000000000")]
    pub max_gas_burnt: near_primitives_core::types::Gas,

    /// Max available memory for `block_cache` and `contract_code_cache` in gigabytes
    /// By default we use all available memory
    #[clap(long, env)]
    pub limit_memory_cache: Option<f64>,

    /// Reserved memory for running the application in gigabytes
    /// By default we use 0.25 gigabyte (256MB or 268_435_456 bytes)
    #[clap(long, env, default_value = "0.25")]
    pub reserved_memory: f64,

    /// Block cache size in gigabytes
    /// By default we use 0.125 gigabyte (128MB or 134_217_728 bytes)
    /// One cache_block size is ≈ 96 bytes
    /// In 128MB we can put 1_398_101 cache_blocks
    #[clap(long, env, default_value = "0.125")]
    pub block_cache_size: f64,
}

impl Opts {
    pub async fn to_s3_config(&self) -> aws_sdk_s3::Config {
        let credentials = aws_credential_types::Credentials::new(
            &self.access_key_id,
            &self.secret_access_key,
            None,
            None,
            "",
        );
        aws_sdk_s3::Config::builder()
            .credentials_provider(credentials)
            .region(aws_sdk_s3::Region::new(self.region.clone()))
            .build()
    }

    pub async fn to_lake_config(
        &self,
        start_block_height: near_primitives_core::types::BlockHeight,
    ) -> anyhow::Result<near_lake_framework::LakeConfig> {
        let config_builder = near_lake_framework::LakeConfigBuilder::default();
        Ok(config_builder
            .s3_config(self.to_s3_config().await)
            .s3_region_name(&self.region)
            .s3_bucket_name(&self.s3_bucket_name)
            .start_block_height(start_block_height)
            .build()
            .expect("Failed to build LakeConfig"))
    }
}

pub struct ServerContext {
    pub s3_client: near_lake_framework::s3_fetchers::LakeS3Client,
    pub db_manager: std::sync::Arc<Box<dyn database::ReaderDbManager + Sync + Send + 'static>>,
    pub near_rpc_client: near_jsonrpc_client::JsonRpcClient,
    pub s3_bucket_name: String,
    pub genesis_config: near_chain_configs::GenesisConfig,
    pub blocks_cache:
        std::sync::Arc<std::sync::RwLock<crate::cache::LruMemoryCache<u64, CacheBlock>>>,
    pub final_block_height: std::sync::Arc<std::sync::atomic::AtomicU64>,
    pub compiled_contract_code_cache: std::sync::Arc<CompiledCodeCache>,
    pub contract_code_cache: std::sync::Arc<
        std::sync::RwLock<crate::cache::LruMemoryCache<near_primitives::hash::CryptoHash, Vec<u8>>>,
    >,
    pub max_gas_burnt: near_primitives_core::types::Gas,
}

impl ServerContext {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        s3_client: near_lake_framework::s3_fetchers::LakeS3Client,
        db_manager: impl database::ReaderDbManager + Sync + Send + 'static,
        near_rpc_client: near_jsonrpc_client::JsonRpcClient,
        s3_bucket_name: String,
        genesis_config: near_chain_configs::GenesisConfig,
        blocks_cache: std::sync::Arc<
            std::sync::RwLock<crate::cache::LruMemoryCache<u64, CacheBlock>>,
        >,
        final_block_height: std::sync::Arc<std::sync::atomic::AtomicU64>,
        compiled_contract_code_cache: std::sync::Arc<CompiledCodeCache>,
        contract_code_cache: std::sync::Arc<
            std::sync::RwLock<
                crate::cache::LruMemoryCache<near_primitives::hash::CryptoHash, Vec<u8>>,
            >,
        >,
        max_gas_burnt: near_primitives_core::types::Gas,
    ) -> Self {
        Self {
            s3_client,
            db_manager: std::sync::Arc::new(Box::new(db_manager)),
            near_rpc_client,
            s3_bucket_name,
            genesis_config,
            blocks_cache,
            final_block_height,
            compiled_contract_code_cache,
            contract_code_cache,
            max_gas_burnt,
        }
    }
}
pub struct CompiledCodeCache {
    pub local_cache: std::sync::Arc<
        std::sync::RwLock<
            crate::cache::LruMemoryCache<
                near_primitives::hash::CryptoHash,
                near_primitives::types::CompiledContract,
            >,
        >,
    >,
}

impl near_primitives::types::CompiledContractCache for CompiledCodeCache {
    fn put(
        &self,
        key: &near_primitives::hash::CryptoHash,
        value: near_primitives::types::CompiledContract,
    ) -> std::io::Result<()> {
        self.local_cache.write().unwrap().put(*key, value);
        Ok(())
    }

    fn get(
        &self,
        key: &near_primitives::hash::CryptoHash,
    ) -> std::io::Result<Option<near_primitives::types::CompiledContract>> {
        Ok(self.local_cache.write().unwrap().get(key).cloned())
    }

    fn has(&self, key: &near_primitives::hash::CryptoHash) -> std::io::Result<bool> {
        Ok(self.local_cache.write().unwrap().contains(key))
    }
}
