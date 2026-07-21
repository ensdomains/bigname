use std::net::SocketAddr;

use anyhow::Result;
use bigname_execution::ChainRpcUrls;
use bigname_storage::DatabaseConfig;
use clap::{Args, Parser, Subcommand};

use crate::ApiBoundsConfig;

#[derive(Parser, Debug)]
#[command(name = "bigname-api", about = "Read API process for bigname")]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Subcommand, Debug)]
pub(crate) enum Command {
    Serve(ServeArgs),
    PrintOpenapi,
}

#[derive(Args, Debug)]
pub(crate) struct ServeArgs {
    #[arg(long, env = "BIGNAME_API_BIND_ADDR", default_value = "127.0.0.1:3000")]
    pub(crate) bind_addr: SocketAddr,
    #[arg(
        long = "chain-rpc-url",
        env = "BIGNAME_API_CHAIN_RPC_URLS",
        value_delimiter = ','
    )]
    pub(crate) chain_rpc_urls: Vec<String>,
    #[command(flatten)]
    pub(crate) bounds: ApiBoundsConfig,
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
}

impl ServeArgs {
    pub(crate) fn effective_chain_rpc_urls(&self) -> Result<ChainRpcUrls> {
        ChainRpcUrls::from_entries(&self.chain_rpc_urls)
    }
}
