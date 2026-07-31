use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use phase_runner::{
    capacity::CapacityGuard,
    cli::{Cli, ResolvedCommand},
    database::RunnerDatabase,
    ingest_phase::IngestPhase,
    phase::PhaseSet,
    runner::{PhaseRunner, SupervisorReport},
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
            let phases =
                PhaseSet::with_ingest(Arc::new(IngestPhase::new(database.pool().clone())))?;
            let runner = Arc::new(PhaseRunner::new(
                database,
                phases,
                CapacityGuard::system(runtime.capacity.clone()),
                runtime.instance_id.clone(),
                runtime.timing.clone(),
            )?);
            let report = runner.run(&runtime, cancellation).await?;
            require_clean_supervisor_exit(report)?;
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
            let phases =
                PhaseSet::with_ingest(Arc::new(IngestPhase::new(database.pool().clone())))?;
            let runner = PhaseRunner::new(
                database,
                phases,
                CapacityGuard::system(capacity),
                instance_id,
                timing,
            )?;
            runner.redo(&chain, phase, range, cancellation).await?;
        }
    }
    Ok(())
}

fn require_clean_supervisor_exit(report: SupervisorReport) -> Result<()> {
    if report.stopped_chains.is_empty() {
        return Ok(());
    }
    let failures = report
        .stopped_chains
        .iter()
        .map(|(chain_id, error)| format!("{chain_id} ({:?}): {error}", error.kind()))
        .collect::<Vec<_>>()
        .join("; ");
    anyhow::bail!(
        "{} chain supervisor(s) stopped on terminal errors: {failures}",
        report.stopped_chains.len()
    )
}

#[cfg(test)]
mod tests {
    use phase_runner::error::RunnerError;

    use super::*;

    #[test]
    fn terminal_chain_report_makes_run_command_fail() {
        let report = SupervisorReport {
            stopped_chains: vec![(
                "broken-chain".to_owned(),
                RunnerError::data_integrity("bad lineage"),
            )],
        };

        let error = require_clean_supervisor_exit(report)
            .expect_err("a terminal chain failure must produce a nonzero main result");
        assert!(error.to_string().contains("broken-chain"));
        assert!(error.to_string().contains("DataIntegrity"));
    }
}
