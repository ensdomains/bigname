use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use phase_runner::{
    capacity::CapacityGuard,
    cli::{Cli, ResolvedCommand},
    database::RunnerDatabase,
    phase::PhaseSet,
    runner::PhaseRunner,
};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .json()
        .init();
    let command = Cli::parse().resolve()?;
    let cancellation = CancellationToken::new();
    let signal_cancellation = cancellation.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            signal_cancellation.cancel();
        }
    });

    match command {
        ResolvedCommand::Run {
            database_url,
            runtime,
        } => {
            let connections = u32::try_from(runtime.chains.len())
                .unwrap_or(u32::MAX)
                .saturating_mul(2)
                .max(4);
            let database = RunnerDatabase::connect(&database_url, connections).await?;
            let runner = Arc::new(PhaseRunner::new(
                database,
                PhaseSet::loopback(),
                CapacityGuard::system(runtime.capacity.clone()),
                runtime.instance_id.clone(),
                runtime.timing.clone(),
            )?);
            runner.run(&runtime, cancellation).await?;
        }
        ResolvedCommand::Redo {
            database_url,
            instance_id,
            chain,
            capacity,
            timing,
            phase,
            range,
        } => {
            let database = RunnerDatabase::connect(&database_url, 4).await?;
            let runner = PhaseRunner::new(
                database,
                PhaseSet::loopback(),
                CapacityGuard::system(capacity),
                instance_id,
                timing,
            )?;
            runner.redo(&chain, phase, range, cancellation).await?;
        }
    }
    Ok(())
}
