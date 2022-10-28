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

    // Indexer database url
    #[clap(long, env)]
    pub database_url: String,

    // State indexer redis url
    #[clap(long, env)]
    pub redis_url: String,

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
}

pub struct ServerContext {
    pub s3_client: aws_sdk_s3::Client,
    pub db_client: sqlx::PgPool,
    pub redis_client: redis::aio::ConnectionManager,
    pub near_rpc_client: near_jsonrpc_client::JsonRpcClient,
    pub s3_bucket_name: String,
    pub blocks_cache: std::sync::Arc<shared_lru::LruCache<u64, CacheBlock>>,
    pub final_block_height: std::sync::Arc<std::sync::atomic::AtomicU64>,
}
