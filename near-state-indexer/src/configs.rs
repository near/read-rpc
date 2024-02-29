pub use clap::{Parser, Subcommand};
use near_jsonrpc_client::{methods, JsonRpcClient};

/// Watches for stream of blocks from the chain
#[derive(Parser, Clone, Debug)]
#[clap(
    version,
    author,
    about,
    disable_help_subcommand(true),
    propagate_version(true),
    next_line_help(true)
)]
pub(crate) struct Opts {
    /// Sets a custom config dir. Defaults to ~/.near/
    #[clap(short, long)]
    pub home: Option<std::path::PathBuf>,
    #[clap(subcommand)]
    pub subcmd: SubCommand,
}

#[derive(Parser, Clone, Debug)]
pub(crate) enum SubCommand {
    /// Run NEAR Indexer Example. Start observe the network
    Run(RunArgs),
    /// Initialize necessary configs
    Init(InitConfigArgs),
}

#[derive(Parser, Debug, Clone)]
pub(crate) struct RunArgs {
    /// Force streaming while node is syncing
    #[clap(long)]
    pub stream_while_syncing: bool,
    /// Sets the starting point for indexing
    #[clap(subcommand)]
    pub start_options: StartOptions,
}

impl RunArgs {
    pub(crate) fn to_indexer_config(
        &self,
        home_dir: std::path::PathBuf,
    ) -> near_indexer::IndexerConfig {
        near_indexer::IndexerConfig {
            home_dir,
            sync_mode: self.start_options.clone().into(),
            await_for_node_synced: if self.stream_while_syncing {
                near_indexer::AwaitForNodeSyncedEnum::StreamWhileSyncing
            } else {
                near_indexer::AwaitForNodeSyncedEnum::WaitForFullSync
            },
            validate_genesis: false,
        }
    }
}

#[allow(clippy::enum_variant_names)]
#[derive(Subcommand, Debug, Clone)]
pub enum StartOptions {
    FromBlock {
        height: u64,
    },
    FromInterruption {
        /// Fallback start block height if interruption block is not found
        height: Option<u64>,
    },
    FromLatest,
}

impl From<StartOptions> for near_indexer::SyncModeEnum {
    fn from(start_options: StartOptions) -> Self {
        match start_options {
            StartOptions::FromBlock { height } => Self::BlockHeight(height),
            StartOptions::FromInterruption { height } => {
                if let Some(height) = height {
                    Self::BlockHeight(height)
                } else {
                    Self::FromInterruption
                }
            }
            StartOptions::FromLatest => Self::LatestSynced,
        }
    }
}

#[derive(Parser, Clone, Debug)]
pub(crate) struct InitConfigArgs {
    /// chain/network id (localnet, testnet, devnet, betanet)
    #[clap(short, long)]
    pub chain_id: Option<String>,
    /// Account ID for the validator key
    #[clap(long)]
    pub account_id: Option<String>,
    /// Specify private key generated from seed (TESTING ONLY)
    #[clap(long)]
    pub test_seed: Option<String>,
    /// Number of shards to initialize the chain with
    #[clap(short, long, default_value = "1")]
    pub num_shards: u64,
    /// Makes block production fast (TESTING ONLY)
    #[clap(short, long)]
    pub fast: bool,
    /// Genesis file to use when initialize testnet (including downloading)
    #[clap(short, long)]
    pub genesis: Option<String>,
    #[clap(short, long)]
    /// Download the verified NEAR config file automatically.
    #[clap(long)]
    pub download_config: bool,
    #[clap(long)]
    pub download_config_url: Option<String>,
    /// Download the verified NEAR genesis file automatically.
    #[clap(long)]
    pub download_genesis: bool,
    /// Specify a custom download URL for the genesis-file.
    #[clap(long)]
    pub download_genesis_url: Option<String>,
    /// Customize max_gas_burnt_view runtime limit.  If not specified, value
    /// from genesis configuration will be taken.
    #[clap(long)]
    pub max_gas_burnt_view: Option<u64>,
    /// Initialize boots nodes in <node_key>@<ip_addr> format seperated by commas
    /// to bootstrap the network and store them in config.json
    #[clap(long)]
    pub boot_nodes: Option<String>,
}
