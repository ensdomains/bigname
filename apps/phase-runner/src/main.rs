use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
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

#[path = "main_manifests.rs"]
mod manifests;
use manifests::{bind_runtime_manifests, hash_manifests_off_runtime};
#[cfg(test)]
use manifests::{load_hashed_manifest_repository, prepare_runtime_manifests};
#[cfg(test)]
use phase_runner::config::{COMPILED_CHAIN_NAMESPACES, validate_deployment_table_set};
use phase_runner::manifest_startup::sync_loaded_manifests;

#[tokio::main]
async fn main() -> Result<()> {
    phase_runner::logging::init();
    let command = Cli::parse().resolve()?;

    match command {
        ResolvedCommand::SourceTransport {
            database_url,
            old,
            new,
        } => {
            let database = RunnerDatabase::connect(&database_url, 2).await?;
            let receipt = phase_runner::source_transport::transition(&database, &old, &new).await?;
            println!("{}", serde_json::to_string(&receipt)?);
        }
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
            mut runtime,
            hydration_rpc_urls,
            project_families,
        } => {
            // Only the supervised run and an explicit redo poll the token; the
            // one-shot commands keep the default SIGTERM disposition.
            let cancellation = CancellationToken::new();
            phase_runner::shutdown::cancel_on_signal(&cancellation)
                .context("register the stop signals before starting")?;
            let startup = async {
                let (manifest_repository, manifest_profile) =
                    hash_manifests_off_runtime(manifests_root.clone()).await??;
                bind_runtime_manifests(
                    &manifest_repository,
                    manifest_profile,
                    Arc::make_mut(&mut runtime.chains),
                )?;
                let connections = u32::try_from(runtime.chains.len())
                    .unwrap_or(u32::MAX)
                    .saturating_mul(2)
                    .max(4);
                let database = RunnerDatabase::connect(&database_url, connections).await?;
                sync_loaded_manifests(
                    database.pool(),
                    &manifests_root,
                    &manifest_repository,
                    manifest_profile,
                )
                .await?;
                let (loop_heartbeat, phase_progress, metrics_feed) = start_metrics(
                    metrics_bind_addr,
                    &database,
                    &cancellation,
                    heartbeat_stale_after_secs,
                    runtime.chains.iter().map(|chain| chain.chain_id.as_str()),
                    true,
                )
                .await?;
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
                    Arc::new(InterpretPhase::from_capacity(
                        database.pool().clone(),
                        &runtime.capacity,
                    )),
                    Arc::new(
                        ProjectPhase::with_hydration(database.pool().clone(), hydration_rpc_urls)
                            .with_families(project_families)
                            .with_metrics_feed(metrics_feed.clone())
                            .with_step_observer(Arc::new(metrics_feed.clone())),
                    ),
                    Arc::new(VerifyPhase::new(verification_database)),
                    Arc::new(LivePhase::with_engine(ingest_engine)),
                )?;
                anyhow::Ok(Arc::new(
                    PhaseRunner::new(
                        database,
                        phases,
                        CapacityGuard::system(runtime.capacity.clone()),
                        runtime.instance_id.clone(),
                        runtime.timing.clone(),
                    )?
                    .with_loop_heartbeat(loop_heartbeat)
                    .with_phase_progress(phase_progress)
                    .with_metrics_feed(metrics_feed),
                ))
            };
            let Some(runner) =
                phase_runner::shutdown::until_cancelled(&cancellation, startup).await?
            else {
                tracing::info!("stop requested during start-up; the runner never started");
                return Ok(());
            };
            let report = runner.run(&runtime, cancellation).await?;
            require_clean_supervisor_exit(report)?;
        }
        ResolvedCommand::Redo {
            database_url,
            verification_database_url,
            metrics_bind_addr,
            heartbeat_stale_after_secs,
            manifests_root,
            instance_id,
            chains,
            capacity,
            timing,
            phase,
            range,
            watch_set_coverage_attestations,
            hydration_rpc_urls,
            project_families,
        } => {
            let cancellation = CancellationToken::new();
            phase_runner::shutdown::cancel_on_signal(&cancellation)
                .context("register the stop signals before starting")?;
            let startup = async {
                let database = RunnerDatabase::connect(&database_url, 4).await?;
                let mut chains = match chains {
                    RedoChains::Explicit(chains) => chains,
                    RedoChains::All { sources } => {
                        resolve_all_redo_chains(
                            database.pool(),
                            sources,
                            phase.requires_intake_sources(),
                        )
                        .await?
                    }
                };
                let (manifest_repository, manifest_profile) =
                    hash_manifests_off_runtime(manifests_root.clone()).await??;
                bind_runtime_manifests(&manifest_repository, manifest_profile, &mut chains)?;
                anyhow::Ok((database, chains, manifest_repository, manifest_profile))
            };
            // Nothing durable happens before this point, so a stop here is a redo
            // that never started. Manifest synchronization is the first commit: a
            // changed manifest can retire hashes or install required Ingest work,
            // so a stop from here on is reported as something to rerun, never as a
            // no-op, whether or not the commit made it.
            let Some((database, chains, manifest_repository, manifest_profile)) =
                phase_runner::shutdown::until_cancelled(&cancellation, startup).await?
            else {
                tracing::info!("stop requested during start-up; the redo never started");
                return Ok(());
            };
            let synchronized = phase_runner::shutdown::until_cancelled(
                &cancellation,
                sync_loaded_manifests(
                    database.pool(),
                    &manifests_root,
                    &manifest_repository,
                    manifest_profile,
                ),
            )
            .await?;
            if synchronized.is_none() {
                bail!(
                    "stop requested during manifest synchronization; whether the manifest change \
                     committed is not known, so this redo must be run again"
                );
            }
            let startup = async {
                validate_redo_attestation_chains(&watch_set_coverage_attestations, &chains)?;
                let (loop_heartbeat, phase_progress, metrics_feed) = start_metrics(
                    metrics_bind_addr,
                    &database,
                    &cancellation,
                    heartbeat_stale_after_secs,
                    chains.iter().map(|chain| chain.chain_id.as_str()),
                    false,
                )
                .await?;
                let ingest_engine = Arc::new(bigname_ingest::Engine::new(database.pool().clone()));
                let ingest = Arc::new(IngestPhase::with_engine(ingest_engine));
                let interpret = Arc::new(InterpretPhase::from_capacity(
                    database.pool().clone(),
                    &capacity,
                ));
                let project = Arc::new(
                    ProjectPhase::with_hydration(database.pool().clone(), hydration_rpc_urls)
                        .with_families(project_families)
                        .with_metrics_feed(metrics_feed.clone())
                        .with_step_observer(Arc::new(metrics_feed.clone())),
                );
                let phases = if phase.requires_verify() {
                    let verification_database_url =
                        verification_database_url.as_deref().ok_or_else(|| {
                            anyhow::anyhow!(
                                "verify redo requires a SELECT-only verification database URL"
                            )
                        })?;
                    let verification_database =
                        VerificationDatabase::connect(verification_database_url, &database, 1)
                            .await?;
                    PhaseSet::with_ingest_interpret_project_and_verify(
                        ingest,
                        interpret,
                        project,
                        Arc::new(VerifyPhase::new(verification_database)),
                    )?
                } else {
                    PhaseSet::with_ingest_interpret_and_project(ingest, interpret, project)?
                };
                anyhow::Ok(
                    PhaseRunner::new(
                        database,
                        phases,
                        CapacityGuard::system(capacity),
                        instance_id,
                        timing,
                    )?
                    .with_watch_set_coverage_attestations(watch_set_coverage_attestations)
                    .with_loop_heartbeat(loop_heartbeat)
                    .with_phase_progress(phase_progress)
                    .with_metrics_feed(metrics_feed),
                )
            };
            let Some(runner) =
                phase_runner::shutdown::until_cancelled(&cancellation, startup).await?
            else {
                bail!(
                    "stop requested after the manifests were synchronized and before the redo \
                     started; required work that synchronization installed is durable, so this \
                     redo must be run again"
                );
            };
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

async fn start_metrics<'a>(
    bind_addr: SocketAddr,
    database: &RunnerDatabase,
    cancellation: &CancellationToken,
    heartbeat_stale_after_secs: i64,
    chain_ids: impl IntoIterator<Item = &'a str>,
    seed_loop_heartbeats: bool,
) -> Result<(
    phase_runner::metrics::RunnerLoopHeartbeat,
    phase_runner::RunnerPhaseProgress,
    phase_runner::metrics::RunnerMetricsFeed,
)> {
    let loop_heartbeat = phase_runner::metrics::RunnerLoopHeartbeat::default();
    let phase_progress = phase_runner::RunnerPhaseProgress::new(Duration::from_secs(
        heartbeat_stale_after_secs
            .try_into()
            .expect("validated threshold"),
    ));
    let metrics_feed = phase_runner::metrics::RunnerMetricsFeed::default();
    for chain_id in chain_ids {
        if seed_loop_heartbeats {
            loop_heartbeat.record_progress(chain_id);
        }
        phase_progress.seed_chain(chain_id);
        metrics_feed.seed_chain(chain_id);
    }
    let bound_addr = phase_runner::metrics::start(
        bind_addr,
        database.pool().clone(),
        cancellation.clone(),
        heartbeat_stale_after_secs,
        loop_heartbeat.clone(),
        phase_progress.clone(),
        metrics_feed.clone(),
    )
    .await?;
    tracing::info!(
        service = "phase-runner",
        metrics_bind_addr = %bound_addr,
        version = phase_runner::SOFTWARE_VERSION,
        build_sha = phase_runner::BUILD_SHA,
        interpreter_content_hash = phase_runner::INTERPRETER_CONTENT_HASH,
        "phase-runner metrics listener started"
    );
    Ok((loop_heartbeat, phase_progress, metrics_feed))
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
#[path = "main/startup_tests.rs"]
mod startup_tests;

#[cfg(test)]
mod tests {
    use phase_runner::{
        config::{ChainConfig, SeedBasis, SourceConfig},
        error::RunnerError,
    };

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
    fn checked_in_profiles_identify_two_configured_ens_chains() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join("manifests/mainnet");
        load_hashed_manifest_repository(&root).expect("mainnet manifest profile must be covered");
        let chains = [
            configured_chain("ethereum-mainnet"),
            configured_chain("ethereum-sepolia"),
        ];

        validate_deployment_table_set(&chains, COMPILED_CHAIN_NAMESPACES.iter().copied())
            .expect_err("the checked-in profiles must identify both configured ENS chains");
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

    fn configured_chain(chain_id: &str) -> ChainConfig {
        ChainConfig::new(
            chain_id,
            vec![
                SourceConfig::new(
                    chain_id,
                    "rpc",
                    "rpc",
                    SeedBasis::BaseSeam,
                    0,
                    "http://rpc.invalid",
                )
                .unwrap(),
            ],
            false,
        )
        .unwrap()
    }
}
