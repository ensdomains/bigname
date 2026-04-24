use std::net::SocketAddr;

use bigname_storage::DatabaseConfig;
use clap::{Args, Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "bigname-api", about = "Bootstrap API process for bigname")]
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
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
}
