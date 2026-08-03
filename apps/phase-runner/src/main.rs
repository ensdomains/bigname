use std::sync::Arc;

use anyhow::{Context, Result, bail, ensure};
use clap::Parser;
use phase_runner::{
    capacity::CapacityGuard,
    cli::{Cli, ResolvedCommand},
    database::RunnerDatabase,
    ingest_phase::IngestPhase,
    interpret_phase::InterpretPhase,
    phase::PhaseSet,
    project_phase::ProjectPhase,
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
        ResolvedCommand::InitSchema { database_url } => {
            let database = RunnerDatabase::connect(&database_url, 1).await?;
            phase_runner::schema::initialize_schema_v2(database.pool()).await?;
            tracing::info!(
                schema = phase_runner::schema::PHASE_SCHEMA_NAME,
                "schema-v2 database is ready"
            );
        }
        ResolvedCommand::Run {
            database_url,
            manifests_root,
            runtime,
        } => {
            let connections = u32::try_from(runtime.chains.len())
                .unwrap_or(u32::MAX)
                .saturating_mul(2)
                .max(4);
            let database = RunnerDatabase::connect(&database_url, connections).await?;
            sync_manifests(database.pool(), &manifests_root).await?;
            let phases = PhaseSet::with_ingest_interpret_and_project(
                Arc::new(IngestPhase::new(database.pool().clone())),
                Arc::new(InterpretPhase::new(database.pool().clone())),
                Arc::new(ProjectPhase::new(database.pool().clone())),
            )?;
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
            manifests_root,
            instance_id,
            chain,
            capacity,
            timing,
            phase,
            range,
        } => {
            if phase == phase_runner::runner::RedoPhase::RecomputeFlags {
                bail!(bigname_interpret::RECOMPUTE_FLAGS_UNAVAILABLE_REASON);
            }
            let database = RunnerDatabase::connect(&database_url, 4).await?;
            sync_manifests(database.pool(), &manifests_root).await?;
            let phases = PhaseSet::with_ingest_interpret_and_project(
                Arc::new(IngestPhase::new(database.pool().clone())),
                Arc::new(InterpretPhase::new(database.pool().clone())),
                Arc::new(ProjectPhase::new(database.pool().clone())),
            )?;
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

async fn sync_manifests(pool: &sqlx::PgPool, root: &std::path::Path) -> Result<()> {
    let (repository, profile) = load_hashed_manifest_repository(root)?;
    let summary = bigname_manifests::sync_schema_v2_repository(pool, &repository).await?;
    tracing::info!(
        manifests_root = %root.display(),
        manifest_profile = profile,
        manifest_count = summary.manifest_count,
        declaration_count = summary.declaration_count,
        discovery_rule_count = summary.discovery_rule_count,
        proxy_edge_count = summary.proxy_edge_count,
        "schema-v2 manifests synchronized"
    );
    Ok(())
}

fn load_hashed_manifest_repository(
    root: &std::path::Path,
) -> Result<(bigname_manifests::ManifestRepository, &'static str)> {
    let before = bigname_content_hash::manifest_profile_hash(root)
        .with_context(|| format!("failed to fingerprint manifest profile {}", root.display()))?;
    let Some((profile, _)) = bigname_content_hash::HASHED_MANIFEST_PROFILES
        .iter()
        .find(|(_, expected)| *expected == before)
    else {
        bail!(
            "runtime manifest profile {} has fingerprint {before}, which is not covered by this binary's interpreter content hash {}",
            root.display(),
            bigname_content_hash::INTERPRETER_CONTENT_HASH
        );
    };

    let repository = bigname_manifests::load_repository(root)?;
    let after = bigname_content_hash::manifest_profile_hash(root).with_context(|| {
        format!(
            "failed to re-fingerprint manifest profile {}",
            root.display()
        )
    })?;
    ensure!(
        before == after,
        "runtime manifest profile {} changed while it was being loaded",
        root.display()
    );
    Ok((repository, profile))
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
    fn init_schema_cli_is_available() {
        let command = Cli::try_parse_from([
            "phase-runner",
            "init-schema",
            "--database-url",
            "postgres://phase-runner.invalid/fresh",
        ])
        .expect("init-schema command must parse")
        .resolve()
        .expect("init-schema command must resolve");
        assert!(matches!(command, ResolvedCommand::InitSchema { .. }));
    }

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

    #[test]
    fn checked_in_manifest_profile_is_bound_to_the_binary() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join("manifests/mainnet");
        let (repository, profile) = load_hashed_manifest_repository(&root)
            .expect("mainnet manifest profile must be covered");

        assert_eq!(profile, "mainnet");
        assert!(!repository.manifests().is_empty());
    }

    #[test]
    fn partial_runtime_manifest_tree_is_rejected_by_the_hash_gate() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join("manifests/mainnet/base");
        let error = load_hashed_manifest_repository(&root)
            .expect_err("an arbitrary runtime manifest subset must be rejected");

        assert!(error.to_string().contains("not covered"));
        assert!(error.to_string().contains("interpreter content hash"));
    }
}
