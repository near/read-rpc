pub use clap::{Parser, Subcommand};

use scylla::{Session, SessionBuilder};

use near_jsonrpc_client::{methods, JsonRpcClient};
use near_indexer_primitives::types::{BlockReference, Finality};
use crate::storage;

/// NEAR Indexer for Explorer
/// Watches for stream of blocks from the chain
#[derive(Parser, Debug)]
#[clap(
version,
author,
about,
setting(clap::AppSettings::DisableHelpSubcommand),
setting(clap::AppSettings::PropagateVersion),
setting(clap::AppSettings::NextLineHelp)
)]
pub(crate) struct Opts {
    /// Connection string to connect to the Redis instance for cache. Default: "redis://127.0.0.1"
    #[clap(long, default_value = "redis://127.0.0.1", env)]
    pub redis_connection_string: String,
    /// Indexer ID to handle meta data about the instance
    #[clap(long, env)]
    pub indexer_id: String,
    /// ScyllaDB connection string
    #[clap(long, default_value = "127.0.0.1:9042", env)]
    pub scylla_url: String,
    /// ScyllaDB keyspace
    #[clap(long, default_value = "tx_indexer", env)]
    pub scylla_keyspace: String,
    /// ScyllaDB user(login)
    #[clap(long, env)]
    pub scylla_user: Option<String>,
    /// ScyllaDB password
    #[clap(long, env)]
    pub scylla_password: Option<String>,
    // AWS Access Key with the rights to read from AWS S3
    #[clap(long, env)]
    pub lake_aws_access_key: String,
    /// AWS Secret Access Key with the rights to read from AWS S3
    #[clap(long, env)]
    pub lake_aws_secret_access_key: String,
    /// Chain ID: testnet or mainnet
    #[clap(subcommand)]
    pub chain_id: ChainId,

}

#[derive(Subcommand, Debug, Clone)]
pub enum ChainId {
    #[clap(subcommand)]
    Mainnet(StartOptions),
    #[clap(subcommand)]
    Testnet(StartOptions),
}

#[allow(clippy::enum_variant_names)]
#[derive(Subcommand, Debug, Clone)]
pub enum StartOptions {
    FromBlock { height: u64 },
    FromInterruption,
    FromLatest,
}

impl Opts {
    /// Returns [StartOptions] for current [Opts]
    pub fn start_options(&self) -> &StartOptions {
        match &self.chain_id {
            ChainId::Mainnet(start_options) | ChainId::Testnet(start_options) => start_options,
        }
    }

    pub fn rpc_url(&self) -> &str {
        match self.chain_id {
            ChainId::Mainnet(_) => "https://rpc.mainnet.near.org",
            ChainId::Testnet(_) => "https://rpc.testnet.near.org",
        }
    }

    // Creates AWS Credentials for NEAR Lake
    fn lake_credentials(&self) -> aws_credential_types::provider::SharedCredentialsProvider {
        let provider = aws_sdk_s3::Credentials::new(
            self.lake_aws_access_key.clone(),
            self.lake_aws_secret_access_key.clone(),
            None,
            None,
            "tx_indexer",
        );
        aws_credential_types::provider::SharedCredentialsProvider::new(provider)
    }

    pub fn lake_aws_sdk_config(&self) -> aws_types::sdk_config::SdkConfig {
        aws_types::sdk_config::SdkConfig::builder()
            .credentials_provider(self.lake_credentials())
            .region(aws_types::region::Region::new("eu-central-1"))
            .build()
    }
}

impl Opts {
    pub async fn to_lake_config(&self) -> anyhow::Result<near_lake_framework::LakeConfig> {

        let s3_config = aws_sdk_s3::config::Builder::from(&self.lake_aws_sdk_config()).build();

        let config_builder = near_lake_framework::LakeConfigBuilder::default().s3_config(s3_config);

        Ok(match &self.chain_id {
            ChainId::Mainnet(_) => config_builder
                .mainnet()
                .start_block_height(get_start_block_height(self).await),
            ChainId::Testnet(_) => config_builder
                .testnet()
                .start_block_height(get_start_block_height(self).await),
        }
            .build()
            .expect("Failed to build LakeConfig"))
    }
}

// TODO: refactor to read from Redis once `storage` is extracted to a separate crate
async fn get_start_block_height(opts: &Opts) -> u64 {
    match opts.start_options() {
        StartOptions::FromBlock { height } => {
            *height
        },
        StartOptions::FromInterruption => {
            let redis_connection_manager = match storage::connect(&opts.redis_connection_string)
                .await
            {
                Ok(connection_manager) => connection_manager,
                Err(err) => {
                    tracing::warn!(
                        target: "tx_indexer",
                        "Failed to connect to Redis to get last synced block, failing to the latest...\n{:#?}",
                        err,
                    );
                    return final_block_height(opts).await;
                }
            };
            match storage::get_last_indexed_block(&redis_connection_manager).await {
                Ok(last_indexed_block) => last_indexed_block,
                Err(err) => {
                    tracing::warn!(
                        target: "tx_indexer",
                        "Failed to get last indexer block from Redis. Failing to the latest one...\n{:#?}",
                        err
                    );
                    final_block_height(opts).await
                }
            }
        }
        StartOptions::FromLatest => final_block_height(opts).await,
    }
}

async fn final_block_height(opts: &Opts) -> u64 {
    let client = JsonRpcClient::connect(opts.rpc_url().to_string());
    let request = methods::block::RpcBlockRequest {
        block_reference: BlockReference::Finality(Finality::Final),
    };

    let latest_block = client.call(request).await.unwrap();

    latest_block.header.height
}

use tracing_subscriber::EnvFilter;

pub fn init_tracing() {
    let mut env_filter = EnvFilter::new("near_lake_framework=info,tx_indexer=info,stats=info");

    if let Ok(rust_log) = std::env::var("RUST_LOG") {
        if !rust_log.is_empty() {
            for directive in rust_log.split(',').filter_map(|s| match s.parse() {
                Ok(directive) => Some(directive),
                Err(err) => {
                    eprintln!("Ignoring directive `{}`: {}", s, err);
                    None
                }
            }) {
                env_filter = env_filter.add_directive(directive);
            }
        }
    }

    tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .init();
}
