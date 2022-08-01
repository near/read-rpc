use clap::Parser;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
pub struct Cli {
    /// rpc_url for near network
    #[clap(long, value_parser)]
    pub rpc_url: http::Uri
}