mod address_names;
mod automatic_projection_replay;
mod children;
mod cli;
mod commands;
mod execution;
mod healthcheck;
mod inspect;
mod manifest_drift;
mod name_current;
mod permissions;
mod primary_name;
mod projection_apply;
mod projection_json;
mod raw_facts;
mod record_inventory;
mod replay;
mod resolver;
mod runtime;

#[cfg(test)]
mod main_tests;

use anyhow::Result;
use clap::Parser;

use crate::cli::{Cli, Command};

pub(crate) const SOFTWARE_VERSION: &str = env!("CARGO_PKG_VERSION");
pub(crate) const BUILD_SHA: &str = match option_env!("BIGNAME_BUILD_SHA") {
    Some(build_sha) => build_sha,
    None => "unknown",
};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    runtime::init_tracing("bigname-worker", cli.writes_machine_json());
    if let Command::Run(args) = &cli.command {
        let metrics_bind_addr = args.metrics_bind_addr;
        let metrics_server = runtime::bind_metrics(metrics_bind_addr).await?;
        tracing::info!(
            service = "worker",
            %metrics_bind_addr,
            "metrics listener bound"
        );
        let _metrics_task = tokio::spawn(async move {
            if let Err(error) = metrics_server.serve().await {
                tracing::error!(
                    service = "worker",
                    error = %format!("{error:#}"),
                    "metrics listener exited"
                );
            }
        });
    }
    commands::dispatch(cli.command).await
}
