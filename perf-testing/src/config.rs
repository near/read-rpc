use clap::Parser;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
pub struct Opts {
    #[clap(long, env = "NEAR_RPC_URL")]
    pub near_rpc_url: http::Uri,
    #[clap(long, env = "READ_RPC_URL")]
    pub read_rpc_url: http::Uri,
    #[clap(long, env, default_value = "30")]
    pub queries_count_per_command: usize,
}
