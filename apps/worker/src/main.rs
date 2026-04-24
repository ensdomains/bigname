mod address_names;
mod children;
mod cli;
mod commands;
mod execution;
mod inspect;
mod manifest_drift;
mod name_current;
mod permissions;
mod primary_name;
mod record_inventory;
mod replay;
mod resolver;
mod runtime;

#[cfg(test)]
mod main_tests;

use anyhow::Result;
use clap::Parser;

use crate::cli::Cli;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    runtime::init_tracing("bigname-worker", cli.writes_machine_json());
    commands::dispatch(cli.command).await
}
