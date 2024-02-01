use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
pub struct Opts {
    #[clap(subcommand)]
    pub start_options: StartOptions,
}

#[allow(clippy::enum_variant_names)]
#[derive(Subcommand, Debug, Clone)]
pub enum StartOptions {
    FromGenesis,
    FromInterruption,
}
