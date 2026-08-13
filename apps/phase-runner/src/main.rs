use std::sync::Arc;

use anyhow::{Context, Result, bail, ensure};
use clap::Parser;
use phase_runner::{
    capacity::CapacityGuard,
    cli::{
        Cli, RedoChains, ResolvedCommand, resolve_all_redo_chains, validate_redo_attestation_chains,
    },
    database::{RunnerDatabase, VerificationDatabase},
    ingest_phase::IngestPhase,
    interpret_phase::InterpretPhase,
    live_phase::LivePhase,
    phase::PhaseSet,
    project_phase::ProjectPhase,
    runner::{PhaseRunner, SupervisorReport},
    verify_phase::VerifyPhase,
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
            verification_database_url,
            metrics_bind_addr,
            heartbeat_stale_after_secs,
            manifests_root,
            runtime,
            hydration_rpc_urls,
        } => {
            let connections = u32::try_from(runtime.chains.len())
                .unwrap_or(u32::MAX)
                .saturating_mul(2)
                .max(4);
            let database = RunnerDatabase::connect(&database_url, connections).await?;
            sync_manifests(database.pool(), &manifests_root).await?;
            let loop_heartbeat = phase_runner::metrics::RunnerLoopHeartbeat::default();
            for chain in runtime.chains.iter() {
                loop_heartbeat.record_progress(&chain.chain_id);
            }
            let bound_metrics_addr = phase_runner::metrics::start(
                metrics_bind_addr,
                database.pool().clone(),
                cancellation.clone(),
                heartbeat_stale_after_secs,
                loop_heartbeat.clone(),
            )
            .await?;
            tracing::info!(
                service = "phase-runner",
                metrics_bind_addr = %bound_metrics_addr,
                version = phase_runner::SOFTWARE_VERSION,
                build_sha = phase_runner::BUILD_SHA,
                interpreter_content_hash = phase_runner::INTERPRETER_CONTENT_HASH,
                "phase-runner metrics listener started"
            );
            let verification_database = VerificationDatabase::connect(
                &verification_database_url,
                &database,
                u32::try_from(runtime.chains.len())
                    .unwrap_or(u32::MAX)
                    .max(1),
            )
            .await?;
            let ingest_engine = Arc::new(bigname_ingest::Engine::new(database.pool().clone()));
            let phases = PhaseSet::with_ingest_interpret_project_and_live(
                Arc::new(IngestPhase::with_engine(Arc::clone(&ingest_engine))),
                Arc::new(InterpretPhase::with_state_cache_capacity(
                    database.pool().clone(),
                    runtime.capacity.interpreter_state_cache_entries,
                )),
                Arc::new(ProjectPhase::with_hydration(
                    database.pool().clone(),
                    hydration_rpc_urls,
                )),
                Arc::new(VerifyPhase::new(verification_database)),
                Arc::new(LivePhase::with_engine(ingest_engine)),
            )?;
            let runner = Arc::new(
                PhaseRunner::new(
                    database,
                    phases,
                    CapacityGuard::system(runtime.capacity.clone()),
                    runtime.instance_id.clone(),
                    runtime.timing.clone(),
                )?
                .with_loop_heartbeat(loop_heartbeat),
            );
            let report = runner.run(&runtime, cancellation).await?;
            require_clean_supervisor_exit(report)?;
        }
        ResolvedCommand::Redo {
            database_url,
            verification_database_url,
            manifests_root,
            instance_id,
            chains,
            capacity,
            timing,
            phase,
            range,
            watch_set_coverage_attestations,
            hydration_rpc_urls,
        } => {
            let database = RunnerDatabase::connect(&database_url, 4).await?;
            sync_manifests(database.pool(), &manifests_root).await?;
            let chains = match chains {
                RedoChains::Explicit(chains) => chains,
                RedoChains::All { sources } => {
                    resolve_all_redo_chains(database.pool(), sources, phase.requires_ingest())
                        .await?
                }
            };
            validate_redo_attestation_chains(&watch_set_coverage_attestations, &chains)?;
            let ingest_engine = Arc::new(bigname_ingest::Engine::new(database.pool().clone()));
            let ingest = Arc::new(IngestPhase::with_engine(ingest_engine));
            let interpret = Arc::new(InterpretPhase::with_state_cache_capacity(
                database.pool().clone(),
                capacity.interpreter_state_cache_entries,
            ));
            let project = Arc::new(ProjectPhase::with_hydration(
                database.pool().clone(),
                hydration_rpc_urls,
            ));
            let phases = if phase.requires_verify() {
                let verification_database_url =
                    verification_database_url.as_deref().ok_or_else(|| {
                        anyhow::anyhow!(
                            "verify redo requires a SELECT-only verification database URL"
                        )
                    })?;
                let verification_database =
                    VerificationDatabase::connect(verification_database_url, &database, 1).await?;
                PhaseSet::with_ingest_interpret_project_and_verify(
                    ingest,
                    interpret,
                    project,
                    Arc::new(VerifyPhase::new(verification_database)),
                )?
            } else {
                PhaseSet::with_ingest_interpret_and_project(ingest, interpret, project)?
            };
            let runner = PhaseRunner::new(
                database,
                phases,
                CapacityGuard::system(capacity),
                instance_id,
                timing,
            )?
            .with_watch_set_coverage_attestations(watch_set_coverage_attestations);
            let report = runner
                .redo_chains(&chains, phase, range, cancellation)
                .await?;
            require_clean_supervisor_exit(report)?;
        }
        ResolvedCommand::Rewind {
            database_url,
            chain_id,
            ancestor,
        } => {
            let database = RunnerDatabase::connect(&database_url, 2).await?;
            let outcome =
                phase_runner::rewind::rewind_to_ancestor(&database, &chain_id, ancestor).await?;
            tracing::info!(
                chain_id,
                previous_block = outcome.previous.number,
                previous_hash = outcome.previous.hash,
                ancestor_block = outcome.ancestor.number,
                ancestor_hash = outcome.ancestor.hash,
                "chain head rewound; affected downstream phases are stamped for redo"
            );
        }
        ResolvedCommand::Inspect {
            database_url,
            request,
        } => phase_runner::inspect::run(&database_url, request).await?,
        ResolvedCommand::LabelPreimagesImportEnsRainbow {
            database_url,
            batch_size,
            limit,
        } => {
            phase_runner::label_preimages::import_ens_rainbow(&database_url, batch_size, limit)
                .await?;
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
    fn rewind_cli_requires_an_exact_ancestor() {
        let command = Cli::try_parse_from([
            "phase-runner",
            "rewind",
            "--database-url",
            "postgres://phase-runner.invalid/fresh",
            "--chain",
            "base-mainnet",
            "--ancestor-block",
            "42",
            "--ancestor-hash",
            "0x42",
        ])
        .expect("rewind command must parse")
        .resolve()
        .expect("rewind command must resolve");

        match command {
            ResolvedCommand::Rewind {
                chain_id, ancestor, ..
            } => {
                assert_eq!(chain_id, "base-mainnet");
                assert_eq!(ancestor.number, 42);
                assert_eq!(ancestor.hash, "0x42");
            }
            _ => panic!("expected rewind command"),
        }
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
