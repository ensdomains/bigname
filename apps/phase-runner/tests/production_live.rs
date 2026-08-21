#[allow(dead_code)]
mod support;

use std::{
    collections::BTreeMap,
    fs,
    io::{self, Write},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use alloy_primitives::Bytes;
use alloy_sol_types::SolValue;
use anyhow::{Context, Result};
use axum::{Json, Router, extract::State, routing::post};
use bigname_ingest::{Engine, LiveBatchRequest, Marker, SourceDescriptor, load_watch_filter};
use bigname_lookup::ChainRpcUrls;
use bigname_manifests::{load_repository, sync_schema_v2_repository};
use bigname_project::Hydrator;
use bigname_storage::{
    PrimaryNameClaimStatus, load_primary_name_current, load_primary_name_current_snapshot,
};
use phase_runner::{
    INTERPRETER_CONTENT_HASH,
    capacity::CapacityGuard,
    config::{CapacityConfig, ChainConfig, SeedBasis, SourceConfig, TimingConfig},
    error::RunnerError,
    heads::{BlockMarker, HeadMarkers, publish_heads},
    ingest_phase::IngestPhase,
    interpret_phase::InterpretPhase,
    live_phase::LivePhase,
    phase::{
        BlockRange, LoopbackPhase, Phase, PhaseBatchOutcome, PhaseContext, PhaseFuture, PhaseName,
        PhaseProgress, PhaseResume, PhaseSet, RunMode,
    },
    project_phase::ProjectPhase,
    rewind::rewind_to_ancestor,
    runner::{PhaseRunner, RedoPhase},
    state::PhaseStore,
};
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::{net::TcpListener, sync::RwLock};
use tokio_util::sync::CancellationToken;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::fmt::MakeWriter;
use uuid::Uuid;

use support::ScratchDatabase;

const ETHEREUM: &str = "ethereum-mainnet";
const REVERSE_RESOLVER: &str = "0xa2c122be93b0074270ebee7f6b7292c7deb45047";
const ADDRESS: &str = "0x00000000000000000000000000000000000000a1";
const ADDRESS_TWO: &str = "0x00000000000000000000000000000000000000a2";
const REVERSE_NODE: &str = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const REVERSE_NODE_TWO: &str = "0x1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const NAMEHASH: &str = "0xabcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
const FAILED_MULTICALL: &str = "__fixture_multicall_failed__";
const FAILED_MULTICALL_BATCH: &str = "__fixture_multicall_batch_failed__";
const MULTICALL_RESULTS_PREFIX: &str = "__fixture_multicall_results__:";
const WATCH_ADDRESS_A: &str = "0x00000000000000000000000000000000000000a1";
const WATCH_ADDRESS_B: &str = "0x00000000000000000000000000000000000000b2";
const TRANSFER_TOPIC: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const TEST_AUTHORITY_FINGERPRINT: &str =
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const HEAD_ADVANCE_DEADLINE: Duration = Duration::from_secs(30);

#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

struct CapturedLogWriter(Arc<Mutex<Vec<u8>>>);

impl CapturedLogs {
    fn text(&self) -> String {
        String::from_utf8(
            self.0
                .lock()
                .expect("captured log lock must not be poisoned")
                .clone(),
        )
        .expect("structured logs must be UTF-8")
    }
}

impl<'a> MakeWriter<'a> for CapturedLogs {
    type Writer = CapturedLogWriter;

    fn make_writer(&'a self) -> Self::Writer {
        CapturedLogWriter(Arc::clone(&self.0))
    }
}

impl Write for CapturedLogWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .expect("captured log lock must not be poisoned")
            .extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct FailingInterpretPhase;

impl Phase for FailingInterpretPhase {
    fn name(&self) -> PhaseName {
        PhaseName::Interpret
    }

    fn run_batch(&self, _context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async { Err(RunnerError::data_integrity("forced required-redo failure")) })
    }
}

struct PanickingInterpretPhase;

impl Phase for PanickingInterpretPhase {
    fn name(&self) -> PhaseName {
        PhaseName::Interpret
    }

    fn run_batch(&self, _context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async { panic!("forced second crash after audited redo restart") })
    }
}

struct ProgressThenFailInterpretPhase {
    attempts: AtomicUsize,
}

impl ProgressThenFailInterpretPhase {
    const fn new() -> Self {
        Self {
            attempts: AtomicUsize::new(0),
        }
    }
}

impl Phase for ProgressThenFailInterpretPhase {
    fn name(&self) -> PhaseName {
        PhaseName::Interpret
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if attempt > 0 {
                return Err(RunnerError::data_integrity(
                    "forced interruption after interpreted redo progress",
                ));
            }
            let range = context.mode.range().expect("fixture requires redo mode");
            let current_number = range
                .from
                .checked_add(1)
                .expect("fixture block number must not overflow");
            let current = BlockMarker::new(current_number, block_hash(1, current_number))?;
            let target = context
                .available_heads
                .expect("fixture requires readable redo heads")
                .latest;
            Ok(PhaseBatchOutcome::Continue(PhaseProgress {
                current: Some(current),
                target: Some(target),
                ..PhaseProgress::default()
            }))
        })
    }
}

struct PanicAfterObservingRedoResume {
    observed: Arc<Mutex<Vec<Option<i64>>>>,
}

impl Phase for PanicAfterObservingRedoResume {
    fn name(&self) -> PhaseName {
        PhaseName::Interpret
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        self.observed
            .lock()
            .expect("resume observation lock must not be poisoned")
            .push(context.resume.current.map(|marker| marker.number));
        Box::pin(async { panic!("forced crash during interpreter content hash rotation restart") })
    }
}

struct ObserveRedoResumeInterpretPhase {
    observed: Arc<Mutex<Vec<Option<i64>>>>,
    loopback: LoopbackPhase,
}

impl ObserveRedoResumeInterpretPhase {
    fn new(observed: Arc<Mutex<Vec<Option<i64>>>>) -> Self {
        Self {
            observed,
            loopback: LoopbackPhase::new(PhaseName::Interpret),
        }
    }
}

impl Phase for ObserveRedoResumeInterpretPhase {
    fn name(&self) -> PhaseName {
        PhaseName::Interpret
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        self.observed
            .lock()
            .expect("resume observation lock must not be poisoned")
            .push(context.resume.current.as_ref().map(|marker| marker.number));
        self.loopback.run_batch(context)
    }
}

struct TransientOnceInterpretPhase {
    attempts: Arc<AtomicUsize>,
    loopback: LoopbackPhase,
}

impl TransientOnceInterpretPhase {
    fn new(attempts: Arc<AtomicUsize>) -> Self {
        Self {
            attempts,
            loopback: LoopbackPhase::new(PhaseName::Interpret),
        }
    }
}

impl Phase for TransientOnceInterpretPhase {
    fn name(&self) -> PhaseName {
        PhaseName::Interpret
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            Box::pin(async { Err(RunnerError::transient("forced attested-redo retry")) })
        } else {
            self.loopback.run_batch(context)
        }
    }
}

struct FailingIngestPhase;

impl Phase for FailingIngestPhase {
    fn name(&self) -> PhaseName {
        PhaseName::Ingest
    }

    fn run_batch(&self, _context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async {
            Err(RunnerError::data_integrity(
                "invalid attestation reached Ingest",
            ))
        })
    }
}

struct FailingProjectPhase;

impl Phase for FailingProjectPhase {
    fn name(&self) -> PhaseName {
        PhaseName::Project
    }

    fn run_batch(&self, _context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async {
            Err(RunnerError::data_integrity(
                "forced project redo interruption",
            ))
        })
    }
}

struct FailingVerifyPreflightPhase;

impl Phase for FailingVerifyPreflightPhase {
    fn name(&self) -> PhaseName {
        PhaseName::Verify
    }

    fn preflight(
        &self,
        _chain_id: &str,
        _sources: &[SourceConfig],
        _mode: &RunMode,
    ) -> phase_runner::error::RunnerResult<()> {
        Err(RunnerError::new(
            phase_runner::error::ErrorKind::Configuration,
            "fixture verify preflight failed",
        ))
    }

    fn run_batch(&self, _context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async { panic!("failed verify preflight must prevent phase execution") })
    }
}

#[tokio::test]
async fn live_head_walk_advances_published_markers() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_head_walk").await?;
    let chain = "live-head-walk";
    seed_branch(scratch.pool(), chain, 1, 0, None).await?;
    publish(scratch.pool(), chain, 1, 0, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 0, &block_hash(1, 0)).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;
    let fixture = RpcFixture::spawn(1, 4).await?;

    let ingest_engine = Arc::new(Engine::new(scratch.pool().clone()));
    let phases = PhaseSet::with_ingest_interpret_project_and_live(
        Arc::new(IngestPhase::with_engine(Arc::clone(&ingest_engine))),
        Arc::new(LoopbackPhase::new(PhaseName::Interpret)),
        Arc::new(LoopbackPhase::new(PhaseName::Project)),
        Arc::new(LoopbackPhase::new(PhaseName::Verify)),
        Arc::new(LivePhase::with_engine(ingest_engine)),
    )?;
    let runner = Arc::new(PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "production-live-head-walk",
        TimingConfig {
            initial_backoff: Duration::from_millis(1),
            maximum_backoff: Duration::from_millis(4),
            live_poll_interval: Duration::from_millis(1),
        },
    )?);
    let configured_chain = ChainConfig::new(
        chain,
        vec![SourceConfig::new(
            chain,
            "rpc",
            "rpc",
            SeedBasis::NewSignatureRange,
            0,
            fixture.endpoint.clone(),
        )?],
        false,
    )?;
    let cancellation = CancellationToken::new();
    let run_cancellation = cancellation.clone();
    let mut task =
        tokio::spawn(async move { runner.run_chain(&configured_chain, run_cancellation).await });
    {
        let head_advanced = tokio::time::timeout(HEAD_ADVANCE_DEADLINE, async {
            loop {
                let latest: i64 = sqlx::query_scalar(
                    "SELECT latest_block_number FROM chain_heads WHERE chain_id = $1",
                )
                .bind(chain)
                .fetch_one(scratch.pool())
                .await?;
                if latest == 4 {
                    return Result::<()>::Ok(());
                }
                tokio::task::yield_now().await;
            }
        });
        tokio::pin!(head_advanced);
        tokio::select! {
            result = &mut task => {
                let result = result?;
                anyhow::bail!("production live runner stopped before advancing the head: {result:?}");
            }
            result = &mut head_advanced => match result {
                Ok(result) => result?,
                Err(error) => {
                    let states: Vec<(String, String, Option<String>)> = sqlx::query_as(
                        "SELECT phase_name, phase_status, last_error
                         FROM chain_phase_state WHERE chain_id = $1 ORDER BY phase_name",
                    )
                    .bind(chain)
                    .fetch_all(scratch.pool())
                    .await?;
                    cancellation.cancel();
                    task.abort();
                    anyhow::bail!("{error}; phase states: {states:?}");
                }
            },
        }
    }
    cancellation.cancel();
    task.await??;

    let stored: (i64, String) = sqlx::query_as(
        "SELECT latest_block_number, latest_block_hash
         FROM chain_heads WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(stored, (4, block_hash(1, 4)));
    fixture.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn live_reorg_above_the_ingest_handoff_replays_through_the_live_head() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_reorg_above_handoff").await?;
    let chain = "live-reorg-above-handoff";
    seed_branch(scratch.pool(), chain, 1, 0, None).await?;
    publish(scratch.pool(), chain, 1, 0, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 0, &block_hash(1, 0)).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;
    let fixture = RpcFixture::spawn(1, 4).await?;
    let engine = Arc::new(Engine::new(scratch.pool().clone()));
    let runner = production_runner(&scratch, engine, chain)?;
    let configured_chain = live_chain(chain, &fixture.endpoint)?;
    let cancellation = CancellationToken::new();
    let run_cancellation = cancellation.clone();
    let mut task =
        tokio::spawn(async move { runner.run_chain(&configured_chain, run_cancellation).await });

    wait_for_rederived_or_runner_stop(scratch.pool(), chain, 4, &block_hash(1, 4), &mut task)
        .await?;
    let ingest_cursor: (i64, Option<i64>) = sqlx::query_as(
        "SELECT next_block_number, target_block_number
         FROM ingest_cursors
         WHERE chain_id = $1 AND source_key = 'rpc'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        ingest_cursor,
        (1, Some(0)),
        "live follow must not rewrite the finite ingest source extent"
    );
    let interpret_hash: Option<String> = sqlx::query_scalar(
        "SELECT input_content_hash
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        interpret_hash.as_deref(),
        Some(INTERPRETER_CONTENT_HASH),
        "without manifest invalidation, the live suffix remains eligible for lineage coverage"
    );

    fixture.reorg(2, 2, 4).await;
    wait_for_rederived_or_runner_stop(scratch.pool(), chain, 4, &block_hash(2, 4), &mut task)
        .await?;
    cancellation.cancel();
    task.await??;

    fixture.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn required_ingest_recovery_uses_the_published_head_without_a_finite_handoff() -> Result<()> {
    let scratch =
        ScratchDatabase::create("production_live_required_ingest_without_handoff").await?;
    let chain = "required-ingest-without-handoff";
    let manifests = WatchManifestFixture::new(chain)?;
    manifests.write(false)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&manifests.root)?).await?;
    seed_branch(scratch.pool(), chain, 1, 1, None).await?;
    publish(scratch.pool(), chain, 1, 1, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 1, &block_hash(1, 1)).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'idle', current_block_number = NULL, current_block_hash = NULL,
             target_block_number = NULL, target_block_hash = NULL, input_content_hash = NULL,
             started_at = NULL, finished_at = NULL
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'running', live_handoff_block_number = NULL,
             live_handoff_block_hash = NULL, target_block_number = 3,
             target_block_hash = $2, finished_at = NULL
         WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(chain)
    .bind(block_hash(1, 3))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE ingest_cursors
         SET next_block_number = 2, target_block_number = 3,
             last_processed_block_number = 1, last_processed_block_hash = $2
         WHERE chain_id = $1 AND source_key = 'rpc'",
    )
    .bind(chain)
    .bind(block_hash(1, 1))
    .execute(scratch.pool())
    .await?;

    let fixture = RpcFixture::spawn(1, 3).await?;
    let runner = production_runner(
        &scratch,
        Arc::new(Engine::new(scratch.pool().clone())),
        chain,
    )?;
    manifests.write(true)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&manifests.root)?).await?;
    fixture.reorg(2, 0, 3).await;
    rewind_to_ancestor(
        &scratch.runner(),
        chain,
        BlockMarker::new(0, block_hash(1, 0))?,
    )
    .await?;

    let pending = tokio::time::timeout(
        Duration::from_secs(10),
        runner.run_chain(
            &live_chain(chain, &fixture.endpoint)?,
            CancellationToken::new(),
        ),
    )
    .await
    .context("Live did not recover the unreadable required Ingest end")?
    .expect_err("normal supervision must leave the historical fetch to the operator");
    assert!(pending.to_string().contains("--phase ingest"));
    assert!(pending.to_string().contains("--from-block 0 --to-block 1"));
    let republished: (i64, String) = sqlx::query_as(
        "SELECT latest_block_number, latest_block_hash FROM chain_heads WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(republished, (3, block_hash(2, 3)));
    let handoff: Option<i64> = sqlx::query_scalar(
        "SELECT live_handoff_block_number FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        handoff, None,
        "Live recovery must not forge a finite handoff"
    );

    runner
        .redo(
            &live_chain(chain, &fixture.endpoint)?,
            RedoPhase::Phase(PhaseName::Ingest),
            BlockRange::new(0, 1)?,
            CancellationToken::new(),
        )
        .await?;
    let still_required: bool = sqlx::query_scalar(
        "SELECT redo_in_progress FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert!(!still_required);

    fixture.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn manifest_authority_change_rejects_live_suffix_lineage_coverage() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_manifest_authority_fence").await?;
    let chain = "manifest-authority-live-suffix";
    let manifests = WatchManifestFixture::new(chain)?;
    manifests.write(false)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&manifests.root)?).await?;
    let initial_watch_plan = load_watch_filter(scratch.pool(), chain, 0, 3).await?;
    assert!(initial_watch_plan.includes(WATCH_ADDRESS_A, TRANSFER_TOPIC, 2));
    assert!(!initial_watch_plan.includes(WATCH_ADDRESS_B, TRANSFER_TOPIC, 2));

    seed_branch(scratch.pool(), chain, 1, 0, None).await?;
    publish(scratch.pool(), chain, 1, 0, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 0, &block_hash(1, 0)).await?;
    let fixture = RpcFixture::spawn_with_b_fact(1, 3, 2).await?;
    let engine = Arc::new(Engine::new(scratch.pool().clone()));
    let phases = PhaseSet::with_ingest_interpret_project_and_live(
        Arc::new(IngestPhase::with_engine(Arc::clone(&engine))),
        Arc::new(LoopbackPhase::new(PhaseName::Interpret)),
        Arc::new(LoopbackPhase::new(PhaseName::Project)),
        Arc::new(LoopbackPhase::new(PhaseName::Verify)),
        Arc::new(LivePhase::with_engine(engine)),
    )?;
    let runner = Arc::new(PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "production-live-manifest-authority-fence",
        fast_timing(),
    )?);
    let configured_chain = live_chain(chain, &fixture.endpoint)?;
    let cancellation = CancellationToken::new();
    let run_cancellation = cancellation.clone();
    let live_runner = Arc::clone(&runner);
    let mut task = tokio::spawn(async move {
        live_runner
            .run_chain(&configured_chain, run_cancellation)
            .await
    });

    wait_for_rederived_or_runner_stop(scratch.pool(), chain, 3, &block_hash(1, 3), &mut task)
        .await?;
    cancellation.cancel();
    task.await??;
    recover_stopped_live_after_exit(runner.as_ref(), scratch.pool(), chain, &fixture.endpoint)
        .await?;
    let b_facts: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM raw_logs WHERE chain_id = $1 AND lower(emitting_address) = $2",
    )
    .bind(chain)
    .bind(WATCH_ADDRESS_B)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        b_facts, 0,
        "Live loaded the suffix under watch plan A, so the B-fact must not be present"
    );

    let interrupted_phases = PhaseSet::with_ingest_interpret_project_and_live(
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        Arc::new(FailingInterpretPhase),
        Arc::new(LoopbackPhase::new(PhaseName::Project)),
        Arc::new(LoopbackPhase::new(PhaseName::Verify)),
        Arc::new(LoopbackPhase::new(PhaseName::Live)),
    )?;
    let interrupted_runner = PhaseRunner::new(
        scratch.runner(),
        interrupted_phases,
        CapacityGuard::system(CapacityConfig::default()),
        "production-live-interrupted-before-manifest-authority",
        fast_timing(),
    )?;
    let interrupted_error = interrupted_runner
        .redo(
            &live_chain(chain, &fixture.endpoint)?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(0, 3)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("the fixture must retain an interrupted redo before manifest sync");
    assert!(
        interrupted_error
            .to_string()
            .contains("forced required-redo failure"),
        "unexpected interrupted-redo fixture failure: {interrupted_error}"
    );
    let interrupted_redo: bool = sqlx::query_scalar(
        "SELECT redo_in_progress
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert!(interrupted_redo);

    manifests.write(true)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&manifests.root)?).await?;
    let widened_watch_plan = load_watch_filter(scratch.pool(), chain, 0, 3).await?;
    assert!(
        widened_watch_plan.includes(WATCH_ADDRESS_B, TRANSFER_TOPIC, 2),
        "manifest B must newly select the retained-missing B-fact in the Live suffix"
    );
    let recorded_hash: Option<String> = sqlx::query_scalar(
        "SELECT input_content_hash
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert!(
        recorded_hash
            .as_deref()
            .is_some_and(|hash| hash.starts_with("manifest-authority:")),
        "manifest sync must stamp the pre-adoption authority marker"
    );
    let first_b_marker = recorded_hash.expect("authority marker was asserted above");
    let (first_b_fingerprint, first_b_generation) = manifest_authority_parts(&first_b_marker)?;
    let first_b_fingerprint = first_b_fingerprint.to_owned();
    let first_b_generation = first_b_generation.to_owned();
    let redo_runner = loopback_runner(&scratch, "production-live-manifest-authority-redo")?;

    let required_ingest: (i64, i64) = sqlx::query_as(
        "SELECT redo_from_block_number, redo_to_block_number
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'ingest' AND redo_in_progress",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(required_ingest, (0, 3));

    let premature_attested_runner = loopback_runner(
        &scratch,
        "production-live-premature-attested-interpret-redo",
    )?
    .with_watch_set_coverage_attestation(chain, &first_b_generation);
    for (phase, runner, range) in [
        (
            PhaseName::Interpret,
            &premature_attested_runner,
            BlockRange::new(0, 3)?,
        ),
        (PhaseName::Project, &redo_runner, BlockRange::new(0, 4)?),
    ] {
        let error = runner
            .redo(
                &live_chain(chain, &fixture.endpoint)?,
                RedoPhase::Phase(phase),
                range,
                CancellationToken::new(),
            )
            .await
            .expect_err("derived redo must wait for the required Ingest redo");
        assert_eq!(
            error.to_string(),
            "manifest watch plan widened over already-ingested blocks for chain \
manifest-authority-live-suffix; automatic re-ingest is disabled because historical fetch cost is \
an operator decision; rerun `phase-runner redo --chain manifest-authority-live-suffix --phase \
ingest --from-block 0 --to-block 3` with the configured sources before derivation"
        );
    }

    fixture.state.write().await.logs.clear();
    fixture.reorg(2, 1, 3).await;
    let sibling_error = runner
        .redo(
            &live_chain(chain, &fixture.endpoint)?,
            RedoPhase::Phase(PhaseName::Ingest),
            BlockRange::new(0, 3)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("the required Ingest redo must not certify a provider sibling fork");
    for expected in ["loaded boundary hash", "readable boundary hash", "rerun"] {
        assert!(
            sibling_error.to_string().contains(expected),
            "missing {expected:?} in sibling-fork refusal: {sibling_error}"
        );
    }

    rewind_to_ancestor(
        &scratch.runner(),
        chain,
        BlockMarker::new(1, block_hash(1, 1))?,
    )
    .await
    .context("operator rewind must preserve a recovery path for the pending Ingest redo")?;
    let pending = tokio::time::timeout(
        Duration::from_secs(10),
        runner.run_chain(
            &live_chain(chain, &fixture.endpoint)?,
            CancellationToken::new(),
        ),
    )
    .await
    .context("Live did not republish the winning suffix for the pending Ingest redo")?
    .expect_err("normal supervision must still leave the historical fetch to the operator");
    assert_eq!(
        pending.to_string(),
        "manifest watch plan widened over already-ingested blocks for chain \
         manifest-authority-live-suffix; automatic re-ingest is disabled because historical \
         fetch cost is an operator decision; rerun `phase-runner redo --chain \
         manifest-authority-live-suffix --phase ingest --from-block 0 --to-block 3` with the \
         configured sources before derivation"
    );
    let republished: (i64, String) = sqlx::query_as(
        "SELECT latest_block_number, latest_block_hash FROM chain_heads WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(republished, (3, block_hash(2, 3)));

    runner
        .redo(
            &live_chain(chain, &fixture.endpoint)?,
            RedoPhase::Phase(PhaseName::Ingest),
            BlockRange::new(0, 3)?,
            CancellationToken::new(),
        )
        .await
        .context("required Ingest redo must complete against the republished winning fork")?;

    // After the required Ingest obligation is discharged, Interpret still needs the separate
    // authority-transition attestation before adopting the new manifest.
    let error = redo_runner
        .redo(
            &live_chain(chain, &fixture.endpoint)?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(0, 3)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("manifest-authority adoption must not use lineage for the live suffix");
    assert_eq!(
        error.kind(),
        phase_runner::error::ErrorKind::DataIntegrity,
        "unexpected redo failure: {error}"
    );
    assert_eq!(
        error.to_string(),
        format!(
            "raw-data presence check failed for interpret redo on chain \
manifest-authority-live-suffix: the manifest authority changed since blocks 0..=3 were loaded; \
invalidation token {first_b_generation}; \
complete any required Ingest redo stamped for this authority transition (docs/manifests.md § \
mandatory historical fetch after watch-plan widening), then re-run with \
--attest-watch-set-coverage {first_b_generation} (or \
--attest-watch-set-coverage manifest-authority-live-suffix={first_b_generation} in a multi-chain \
redo)"
        )
    );

    // The operator reviewed the first transition to watch plan B. A competing sync changes the
    // current authority before redo begins, so that review must not authorize the replacement.
    manifests.write(false)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&manifests.root)?).await?;
    let a_marker: String = sqlx::query_scalar(
        "SELECT input_content_hash
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    let (_, a_generation) = manifest_authority_parts(&a_marker)?;
    let a_generation = a_generation.to_owned();
    let stale_review_runner = loopback_runner(&scratch, "production-live-stale-review-token")?
        .with_watch_set_coverage_attestation(chain, &first_b_generation);
    let swap_error = stale_review_runner
        .redo(
            &live_chain(chain, &fixture.endpoint)?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(0, 3)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("a sync in the review window must invalidate the operator's old token");
    assert_eq!(
        swap_error.kind(),
        phase_runner::error::ErrorKind::Configuration
    );
    assert!(swap_error.to_string().contains(&first_b_generation));
    assert!(swap_error.to_string().contains(&a_generation));

    loopback_runner(&scratch, "production-live-authority-a-discharge")?
        .with_watch_set_coverage_attestation(chain, &a_generation)
        .redo(
            &live_chain(chain, &fixture.endpoint)?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(0, 3)?,
            CancellationToken::new(),
        )
        .await?;

    // Returning to the identical B authority is a new invalidation generation. This pins the ABA
    // interleaving: a stalled command carrying the first B token cannot discharge the second B
    // marker even though its deterministic authority fingerprint is the same.
    manifests.write(true)?;
    sync_schema_v2_repository(scratch.pool(), &load_repository(&manifests.root)?).await?;
    let second_b_marker: String = sqlx::query_scalar(
        "SELECT input_content_hash
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    let (second_b_fingerprint, second_b_generation) = manifest_authority_parts(&second_b_marker)?;
    let second_b_generation = second_b_generation.to_owned();
    assert_eq!(second_b_fingerprint, first_b_fingerprint);
    assert_ne!(second_b_generation, first_b_generation);
    redo_runner
        .redo(
            &live_chain(chain, &fixture.endpoint)?,
            RedoPhase::Phase(PhaseName::Ingest),
            BlockRange::new(0, 3)?,
            CancellationToken::new(),
        )
        .await?;
    let stale_aba_runner = loopback_runner(&scratch, "production-live-stale-aba-token")?
        .with_watch_set_coverage_attestation(chain, &first_b_generation);
    let aba_error = stale_aba_runner
        .redo(
            &live_chain(chain, &fixture.endpoint)?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(0, 3)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("the first B token must not discharge the later B invalidation");
    assert_eq!(
        aba_error.kind(),
        phase_runner::error::ErrorKind::Configuration
    );
    assert!(aba_error.to_string().contains(&first_b_generation));
    assert!(aba_error.to_string().contains(&second_b_generation));

    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .without_time()
        .with_writer(logs.clone())
        .finish();
    let attempts = Arc::new(AtomicUsize::new(0));
    let attested_phases = PhaseSet::with_ingest_interpret_project_and_live(
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        Arc::new(TransientOnceInterpretPhase::new(Arc::clone(&attempts))),
        Arc::new(LoopbackPhase::new(PhaseName::Project)),
        Arc::new(LoopbackPhase::new(PhaseName::Verify)),
        Arc::new(LoopbackPhase::new(PhaseName::Live)),
    )?;
    let attested_runner = PhaseRunner::new(
        scratch.runner(),
        attested_phases,
        CapacityGuard::system(CapacityConfig::default()),
        "production-live-manifest-authority-attested-redo",
        fast_timing(),
    )?
    .with_watch_set_coverage_attestation(chain, &second_b_generation);
    attested_runner
        .redo(
            &live_chain(chain, &fixture.endpoint)?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(0, 0)?,
            CancellationToken::new(),
        )
        .with_subscriber(subscriber)
        .await?;
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    let logs = logs.text();
    assert_eq!(
        logs.matches("\"event\":\"manifest_authority_watch_set_coverage_attested\"")
            .count(),
        2,
        "the first begin and the validated in-process retry must both emit from the durable row"
    );
    assert!(logs.contains("\"chain_id\":\"manifest-authority-live-suffix\""));
    assert!(logs.contains("\"phase\":\"interpret\""));
    assert!(logs.contains("\"redo_from_block\":0"));
    assert!(logs.contains("\"redo_to_block\":3"));
    assert!(logs.contains(&format!(
        "\"authority_fingerprint\":\"{first_b_fingerprint}\""
    )));
    assert!(logs.contains(&format!("\"generation_token\":\"{second_b_generation}\"")));
    assert!(logs.contains("\"attested_by\":\"production-live-manifest-authority-attested-redo\""));
    assert_eq!(logs.matches("\"replayed\":false").count(), 1);
    assert_eq!(logs.matches("\"replayed\":true").count(), 1);
    let durable_audit: (String, String, String, i64, i64, String) = sqlx::query_as(
        "SELECT authority_fingerprint, generation_token, phase_name,
                redo_from_block_number, redo_to_block_number, attested_by
         FROM manifest_authority_attestations
         WHERE chain_id = $1 AND generation_token = $2",
    )
    .bind(chain)
    .bind(&second_b_generation)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        durable_audit,
        (
            first_b_fingerprint,
            second_b_generation,
            "interpret".to_owned(),
            0,
            3,
            "production-live-manifest-authority-attested-redo".to_owned(),
        )
    );

    fixture.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn manifest_authority_change_without_a_live_suffix_requires_attestation() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_authority_finite_coverage").await?;
    let chain = "manifest-authority-finite-coverage";
    seed_branch(scratch.pool(), chain, 1, 0, None).await?;
    publish(scratch.pool(), chain, 1, 0, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 0, &block_hash(1, 0)).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;
    let generation = "finite-coverage-generation";
    sqlx::query(
        "UPDATE chain_phase_state
         SET input_content_hash = $2
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain)
    .bind(manifest_authority_marker(generation))
    .execute(scratch.pool())
    .await?;
    let unattested_runner = loopback_runner(
        &scratch,
        "production-manifest-authority-finite-coverage-unattested",
    )?;
    // Before the uniform manifest-authority fence, full finite-cursor coverage let this redo
    // adopt the new authority without either a historical fetch or an operator attestation.
    let error = unattested_runner
        .redo(
            &live_chain(chain, "http://unused.invalid")?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(0, 0)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("every manifest-authority discharge must require an attestation");
    assert_eq!(error.kind(), phase_runner::error::ErrorKind::DataIntegrity);
    assert_eq!(
        error.to_string(),
        format!(
            "raw-data presence check failed for interpret redo on chain \
manifest-authority-finite-coverage: the manifest authority changed since blocks 0..=0 were \
loaded; invalidation token {generation}; complete any required Ingest redo stamped for this \
authority transition (docs/manifests.md § mandatory historical fetch after watch-plan widening), \
then re-run with --attest-watch-set-coverage {generation} (or \
--attest-watch-set-coverage manifest-authority-finite-coverage={generation} in a multi-chain \
redo)"
        )
    );

    let attested_runner = loopback_runner(
        &scratch,
        "production-manifest-authority-finite-coverage-attested",
    )?
    .with_watch_set_coverage_attestation(chain, generation);
    attested_runner
        .redo(
            &live_chain(chain, "http://unused.invalid")?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(0, 0)?,
            CancellationToken::new(),
        )
        .await?;

    let adopted: Option<String> = sqlx::query_scalar(
        "SELECT input_content_hash
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(adopted.as_deref(), Some(INTERPRETER_CONTENT_HASH));
    let audit_rows: i64 = sqlx::query_scalar(
        "SELECT count(*)
         FROM manifest_authority_attestations
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(audit_rows, 1, "an attested discharge must be durable");
    scratch.cleanup().await
}

#[tokio::test]
async fn attestation_audit_survives_a_crash_before_telemetry_and_reemits_on_restart() -> Result<()>
{
    let scratch = ScratchDatabase::create("production_manifest_attestation_crash_audit").await?;
    let chain = "manifest-attestation-crash-audit";
    let generation = "crash-window-generation";
    seed_branch(scratch.pool(), chain, 1, 0, None).await?;
    publish(scratch.pool(), chain, 1, 0, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 0, &block_hash(1, 0)).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET input_content_hash = $2
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain)
    .bind(manifest_authority_marker(generation))
    .execute(scratch.pool())
    .await?;

    let crash_runner = loopback_runner(&scratch, "production-attestation-crash")?
        .with_watch_set_coverage_attestation(chain, generation)
        .with_manifest_authority_audit_before_emit(|| {
            panic!("forced process crash before attestation telemetry emission")
        });
    let crash_chain = live_chain(chain, "http://unused.invalid")?;
    let crashed = tokio::spawn(async move {
        crash_runner
            .redo(
                &crash_chain,
                RedoPhase::Phase(PhaseName::Interpret),
                BlockRange::new(0, 0).expect("valid crash-window range"),
                CancellationToken::new(),
            )
            .await
    })
    .await
    .expect_err("the fixture must crash after the redo-begin transaction commits");
    assert!(crashed.is_panic());

    let durable: (i64, bool, Option<String>) = sqlx::query_as(
        "SELECT (
             SELECT count(*)
             FROM manifest_authority_attestations
             WHERE chain_id = $1 AND phase_name = 'interpret' AND generation_token = $2
         ), redo_in_progress, input_content_hash
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain)
    .bind(generation)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(durable.0, 1, "the audit row must commit before telemetry");
    assert!(durable.1, "the committed redo must remain resumable");
    assert_eq!(durable.2.as_deref(), Some(INTERPRETER_CONTENT_HASH));

    let tokenless_logs = CapturedLogs::default();
    let tokenless_subscriber = tracing_subscriber::fmt()
        .json()
        .without_time()
        .with_writer(tokenless_logs.clone())
        .finish();
    let tokenless_error = loopback_runner(&scratch, "production-attestation-tokenless-restart")?
        .redo(
            &live_chain(chain, "http://unused.invalid")?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(0, 0)?,
            CancellationToken::new(),
        )
        .with_subscriber(tokenless_subscriber)
        .await
        .expect_err("an interrupted audited redo must require its generation token");
    assert!(tokenless_error.to_string().contains(generation));
    assert!(
        !tokenless_logs.text().contains("\"replayed\":true"),
        "a rejected tokenless command must not emit replay telemetry"
    );

    let remaining_chain = "manifest-attestation-crash-audit-remaining";
    let remaining_generation = "remaining-chain-generation";
    seed_branch(scratch.pool(), remaining_chain, 1, 0, None).await?;
    publish(scratch.pool(), remaining_chain, 1, 0, 0, 0).await?;
    seed_completed_spine(scratch.pool(), remaining_chain, 0, &block_hash(1, 0)).await?;
    seed_empty_watch_manifest(scratch.pool(), remaining_chain).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET input_content_hash = $2
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(remaining_chain)
    .bind(manifest_authority_marker(remaining_generation))
    .execute(scratch.pool())
    .await?;

    let stale_logs = CapturedLogs::default();
    let stale_subscriber = tracing_subscriber::fmt()
        .json()
        .without_time()
        .with_writer(stale_logs.clone())
        .finish();
    let stale_resume_error = loopback_runner(&scratch, "production-attestation-stale-restart")?
        .with_watch_set_coverage_attestation(chain, "stale-restart-generation")
        .redo(
            &live_chain(chain, "http://unused.invalid")?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(0, 0)?,
            CancellationToken::new(),
        )
        .with_subscriber(stale_subscriber)
        .await
        .expect_err("an interrupted audited redo must reject a different generation");
    assert!(
        stale_resume_error
            .to_string()
            .contains("stale-restart-generation")
    );
    assert!(stale_resume_error.to_string().contains(generation));
    assert!(
        !stale_logs.text().contains("\"replayed\":true"),
        "a rejected stale-token command must not emit replay telemetry"
    );

    let second_crash_phases = PhaseSet::with_ingest_interpret_project_and_live(
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        Arc::new(PanickingInterpretPhase),
        Arc::new(LoopbackPhase::new(PhaseName::Project)),
        Arc::new(LoopbackPhase::new(PhaseName::Verify)),
        Arc::new(LoopbackPhase::new(PhaseName::Live)),
    )?;
    let second_crash_runner = PhaseRunner::new(
        scratch.runner(),
        second_crash_phases,
        CapacityGuard::system(CapacityConfig::default()),
        "production-attestation-second-crash",
        fast_timing(),
    )?
    .with_watch_set_coverage_attestation(chain, generation);
    let second_crash_chain = live_chain(chain, "http://unused.invalid")?;
    let second_crash = tokio::spawn(async move {
        second_crash_runner
            .redo(
                &second_crash_chain,
                RedoPhase::Phase(PhaseName::Interpret),
                BlockRange::new(0, 0).expect("valid second-crash range"),
                CancellationToken::new(),
            )
            .await
    })
    .await
    .expect_err("the resumed phase must crash after its locked begin");
    assert!(second_crash.is_panic());
    let audit_still_matches_active_redo: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
             FROM manifest_authority_attestations audit
             JOIN chain_phase_state phase
               ON phase.chain_id = audit.chain_id
              AND phase.phase_name = audit.phase_name
              AND phase.redo_in_progress
              AND phase.redo_from_block_number = audit.redo_from_block_number
              AND phase.redo_to_block_number = audit.redo_to_block_number
              AND phase.started_at = audit.attested_at
             WHERE audit.chain_id = $1 AND audit.generation_token = $2
         )",
    )
    .bind(chain)
    .bind(generation)
    .fetch_one(scratch.pool())
    .await?;
    assert!(
        audit_still_matches_active_redo,
        "a second crash must preserve the durable audit association"
    );

    let replayed_logs = CapturedLogs::default();
    let replay_subscriber = tracing_subscriber::fmt()
        .json()
        .without_time()
        .with_writer(replayed_logs.clone())
        .finish();
    let restart_report = loopback_runner(&scratch, "production-attestation-crash-restart")?
        .with_watch_set_coverage_attestation(chain, generation)
        .with_watch_set_coverage_attestation(remaining_chain, remaining_generation)
        .redo_chains(
            &[
                live_chain(chain, "http://unused.invalid")?,
                live_chain(remaining_chain, "http://unused.invalid")?,
            ],
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(0, 0)?,
            CancellationToken::new(),
        )
        .with_subscriber(replay_subscriber)
        .await?;
    assert!(
        restart_report.stopped_chains.is_empty(),
        "the audited chain must resume without blocking a later chain: {:?}",
        restart_report.stopped_chains
    );
    let replayed_logs = replayed_logs.text();
    assert_eq!(
        replayed_logs.matches("\"replayed\":true").count(),
        1,
        "restart must re-emit the durable row once: {replayed_logs}"
    );
    assert!(replayed_logs.contains(&format!("\"generation_token\":\"{generation}\"")));
    assert!(replayed_logs.contains("\"replayed\":true"));
    let audit_rows: i64 = sqlx::query_scalar(
        "SELECT count(*)
         FROM manifest_authority_attestations
         WHERE chain_id = $1 AND phase_name = 'interpret' AND generation_token = $2",
    )
    .bind(chain)
    .bind(generation)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        audit_rows, 1,
        "restart must not duplicate the durable audit"
    );
    let remaining_audit_rows: i64 = sqlx::query_scalar(
        "SELECT count(*)
         FROM manifest_authority_attestations
         WHERE chain_id = $1 AND phase_name = 'interpret' AND generation_token = $2",
    )
    .bind(remaining_chain)
    .bind(remaining_generation)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        remaining_audit_rows, 1,
        "the remaining chain must discharge its current generation"
    );
    let completed_token_error =
        loopback_runner(&scratch, "production-attestation-completed-token-reuse")?
            .with_watch_set_coverage_attestation(chain, generation)
            .redo(
                &live_chain(chain, "http://unused.invalid")?,
                RedoPhase::Phase(PhaseName::Interpret),
                BlockRange::new(0, 0)?,
                CancellationToken::new(),
            )
            .await
            .expect_err("a completed discharge must make its token invalid");
    assert!(
        completed_token_error
            .to_string()
            .contains("is not discharging a manifest-authority marker")
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn failed_reorg_required_attested_interpret_redo_resumes_its_audit() -> Result<()> {
    let scratch = ScratchDatabase::create("production_reorg_attested_redo_retry").await?;
    let chain = "reorg-attested-redo-retry";
    let generation = "reorg-attested-redo-retry-generation";
    seed_branch(scratch.pool(), chain, 1, 3, None).await?;
    publish(scratch.pool(), chain, 1, 3, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 3, &block_hash(1, 3)).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;

    seed_branch(scratch.pool(), chain, 2, 3, Some((0, block_hash(1, 0)))).await?;
    publish(scratch.pool(), chain, 2, 3, 0, 0).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET input_content_hash = $2
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain)
    .bind(manifest_authority_marker(generation))
    .execute(scratch.pool())
    .await?;

    let first_error = runner_with_interpret_phase(
        &scratch,
        "production-reorg-attested-redo-first-attempt",
        Arc::new(ProgressThenFailInterpretPhase::new()),
    )?
    .with_watch_set_coverage_attestation(chain, generation)
    .redo(
        &live_chain(chain, "http://unused.invalid")?,
        RedoPhase::Phase(PhaseName::Interpret),
        BlockRange::new(0, 3)?,
        CancellationToken::new(),
    )
    .await
    .expect_err("the first attested attempt must fail after recording mid-range progress");
    assert!(
        first_error
            .to_string()
            .contains("forced interruption after interpreted redo progress")
    );
    let interrupted: (Option<i64>, String, bool, i64, i64) = sqlx::query_as(
        "SELECT phase.redo_current_block_number, phase.last_error,
                phase.started_at = audit.attested_at,
                audit.redo_from_block_number, audit.redo_to_block_number
         FROM chain_phase_state phase
         JOIN manifest_authority_attestations audit
           ON audit.chain_id = phase.chain_id
          AND audit.phase_name = phase.phase_name
          AND audit.generation_token = $2
         WHERE phase.chain_id = $1 AND phase.phase_name = 'interpret'",
    )
    .bind(chain)
    .bind(generation)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(interrupted.0, Some(1));
    assert!(interrupted.1.starts_with("required downstream redo: "));
    assert!(
        interrupted.2,
        "failure must preserve the audited start time"
    );
    assert_eq!((interrupted.3, interrupted.4), (0, 3));

    let resumed_from = Arc::new(Mutex::new(Vec::new()));
    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .without_time()
        .with_writer(logs.clone())
        .finish();
    runner_with_interpret_phase(
        &scratch,
        "production-reorg-attested-redo-retry",
        Arc::new(ObserveRedoResumeInterpretPhase::new(Arc::clone(
            &resumed_from,
        ))),
    )?
    .with_watch_set_coverage_attestation(chain, generation)
    .redo(
        &live_chain(chain, "http://unused.invalid")?,
        RedoPhase::Phase(PhaseName::Interpret),
        BlockRange::new(0, 3)?,
        CancellationToken::new(),
    )
    .with_subscriber(subscriber)
    .await?;
    assert_eq!(
        *resumed_from
            .lock()
            .expect("resume observation lock must not be poisoned"),
        vec![Some(1)]
    );
    assert!(logs.text().contains("\"replayed\":true"));
    let completed: (bool, i64) = sqlx::query_as(
        "SELECT phase.redo_in_progress,
                (SELECT count(*) FROM manifest_authority_attestations audit
                 WHERE audit.chain_id = $1 AND audit.generation_token = $2)
         FROM chain_phase_state phase
         WHERE phase.chain_id = $1 AND phase.phase_name = 'interpret'",
    )
    .bind(chain)
    .bind(generation)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(completed, (false, 1));

    scratch.cleanup().await
}

#[tokio::test]
async fn attested_redo_restart_resets_progress_after_interpreter_hash_rotation() -> Result<()> {
    let scratch = ScratchDatabase::create("production_attested_redo_cross_hash_restart").await?;
    let chain = "attested-redo-interpreter-content-hash-restart";
    let generation = "interpreter-content-hash-restart-generation";
    seed_branch(scratch.pool(), chain, 1, 2, None).await?;
    publish(scratch.pool(), chain, 1, 2, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 2, &block_hash(1, 2)).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET current_block_number = 1, current_block_hash = $2,
             target_block_number = 1, target_block_hash = $2,
             live_handoff_block_number = 1, live_handoff_block_hash = $2
         WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(chain)
    .bind(block_hash(1, 1))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE ingest_cursors
         SET next_block_number = 2, target_block_number = 1,
             last_processed_block_number = 1, last_processed_block_hash = $2
         WHERE chain_id = $1 AND source_key = 'rpc'",
    )
    .bind(chain)
    .bind(block_hash(1, 1))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET input_content_hash = $2
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain)
    .bind(manifest_authority_marker(generation))
    .execute(scratch.pool())
    .await?;

    // The operator requests the finite-ingest range 0..=1. Interpret extends the effective
    // audited range through the readable Live lineage and recorded Interpret head at block 2.
    let interrupted = runner_with_interpret_phase(
        &scratch,
        "production-attested-redo-before-hash-rotation",
        Arc::new(ProgressThenFailInterpretPhase::new()),
    )?
    .with_watch_set_coverage_attestation(chain, generation)
    .redo(
        &live_chain(chain, "http://unused.invalid")?,
        RedoPhase::Phase(PhaseName::Interpret),
        BlockRange::new(0, 1)?,
        CancellationToken::new(),
    )
    .await
    .expect_err("the fixture must interrupt after committing redo progress");
    assert!(
        interrupted
            .to_string()
            .contains("forced interruption after interpreted redo progress")
    );
    let h1_progress: (Option<i64>, Option<String>, bool) = sqlx::query_as(
        "SELECT redo_current_block_number, input_content_hash, redo_in_progress
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(h1_progress.0, Some(1));
    assert_eq!(h1_progress.1.as_deref(), Some(INTERPRETER_CONTENT_HASH));
    assert!(h1_progress.2);

    // Model an interpreter content hash rotation (docs/glossary.md#interpreter-content-hash)
    // from H1 to this binary's H2 after the audited redo committed a prefix. The durable audit
    // has no hash field, while phase state records the prior interpreter content hash.
    let simulated_h1 = "keccak256:simulated-interpreter-h1";
    sqlx::query(
        "UPDATE chain_phase_state
         SET input_content_hash = $2
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')",
    )
    .bind(chain)
    .bind(simulated_h1)
    .execute(scratch.pool())
    .await?;

    let tokenless_error = loopback_runner(&scratch, "production-content-hash-tokenless")?
        .redo(
            &live_chain(chain, "http://unused.invalid")?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(0, 2)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("a restart after an interpreter content hash rotation must require its token");
    assert!(tokenless_error.to_string().contains(generation));

    let wrong_generation = "wrong-interpreter-content-hash-generation";
    let wrong_token_error = loopback_runner(&scratch, "production-content-hash-wrong-token")?
        .with_watch_set_coverage_attestation(chain, wrong_generation)
        .redo(
            &live_chain(chain, "http://unused.invalid")?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(0, 2)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("an interpreter content hash rotation restart must reject another generation");
    assert!(wrong_token_error.to_string().contains(wrong_generation));
    assert!(wrong_token_error.to_string().contains(generation));

    let changed_range_error = loopback_runner(&scratch, "production-content-hash-wrong-range")?
        .with_watch_set_coverage_attestation(chain, generation)
        .redo(
            &live_chain(chain, "http://unused.invalid")?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(1, 2)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("an interpreter content hash rotation restart must reject a changed range");
    assert_eq!(
        changed_range_error.kind(),
        phase_runner::error::ErrorKind::ContentHashMismatch
    );
    assert!(changed_range_error.to_string().contains("full range 0..=2"));

    let reset_resume = Arc::new(Mutex::new(Vec::new()));
    let crashing_restart = runner_with_interpret_phase(
        &scratch,
        "production-attested-redo-content-hash-crash",
        Arc::new(PanicAfterObservingRedoResume {
            observed: Arc::clone(&reset_resume),
        }),
    )?
    .with_watch_set_coverage_attestation(chain, generation);
    let restart_chain = live_chain(chain, "http://unused.invalid")?;
    let crashed = tokio::spawn(async move {
        crashing_restart
            .redo(
                &restart_chain,
                RedoPhase::Phase(PhaseName::Interpret),
                BlockRange::new(0, 2).expect("valid audited restart range"),
                CancellationToken::new(),
            )
            .await
    })
    .await
    .expect_err("the matching interpreter content hash restart must reach the crashing phase");
    assert!(crashed.is_panic());
    assert_eq!(
        *reset_resume
            .lock()
            .expect("resume observation lock must not be poisoned"),
        vec![None],
        "the new hash must restart the audited range without the H1 redo cursor"
    );
    let reset_state: (Option<i64>, Option<i64>, Option<String>, bool) = sqlx::query_as(
        "SELECT phase.redo_current_block_number, phase.redo_target_block_number,
                phase.input_content_hash, phase.started_at = audit.attested_at
         FROM chain_phase_state phase
         JOIN manifest_authority_attestations audit
           ON audit.chain_id = phase.chain_id
          AND audit.phase_name = phase.phase_name
          AND audit.generation_token = $2
         WHERE phase.chain_id = $1 AND phase.phase_name = 'interpret'",
    )
    .bind(chain)
    .bind(generation)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        reset_state,
        (None, None, Some(INTERPRETER_CONTENT_HASH.to_owned()), true),
        "interpreter content hash rotation must clear redo progress, adopt H2, and retain the audit join"
    );

    let completion_resume = Arc::new(Mutex::new(Vec::new()));
    runner_with_interpret_phase(
        &scratch,
        "production-attested-redo-content-hash-complete",
        Arc::new(ObserveRedoResumeInterpretPhase::new(Arc::clone(
            &completion_resume,
        ))),
    )?
    .with_watch_set_coverage_attestation(chain, generation)
    .redo(
        &live_chain(chain, "http://unused.invalid")?,
        RedoPhase::Phase(PhaseName::Interpret),
        BlockRange::new(0, 2)?,
        CancellationToken::new(),
    )
    .await?;
    assert_eq!(
        *completion_resume
            .lock()
            .expect("resume observation lock must not be poisoned"),
        vec![None],
        "a crash before H2 progress must still restart from the audited range beginning"
    );
    let completed: (Option<String>, Option<String>, bool, bool, i64, i64, i64) = sqlx::query_as(
        "SELECT interpret.input_content_hash, project.input_content_hash,
                interpret.redo_in_progress, project.redo_in_progress,
                audit.redo_from_block_number, audit.redo_to_block_number,
                (SELECT count(*)
                 FROM manifest_authority_attestations counted
                 WHERE counted.chain_id = $1 AND counted.generation_token = $2)
         FROM chain_phase_state interpret
         JOIN chain_phase_state project
           ON project.chain_id = interpret.chain_id
          AND project.phase_name = 'project'
         JOIN manifest_authority_attestations audit
           ON audit.chain_id = interpret.chain_id
          AND audit.phase_name = interpret.phase_name
          AND audit.generation_token = $2
         WHERE interpret.chain_id = $1 AND interpret.phase_name = 'interpret'",
    )
    .bind(chain)
    .bind(generation)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        completed,
        (
            Some(INTERPRETER_CONTENT_HASH.to_owned()),
            Some(INTERPRETER_CONTENT_HASH.to_owned()),
            false,
            false,
            0,
            2,
            1,
        ),
        "the completed full walk must stamp H2 and the full audited range exactly once"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn audited_restart_rejects_a_changed_effective_range_before_replay() -> Result<()> {
    let scratch = ScratchDatabase::create("production_attestation_exact_restart_range").await?;
    let chain = "manifest-attestation-exact-restart-range";
    let generation = "exact-restart-range-generation";
    seed_branch(scratch.pool(), chain, 1, 0, None).await?;
    publish(scratch.pool(), chain, 1, 0, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 0, &block_hash(1, 0)).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET input_content_hash = $2
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain)
    .bind(manifest_authority_marker(generation))
    .execute(scratch.pool())
    .await?;

    let interrupted_phases = PhaseSet::with_ingest_interpret_project_and_live(
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        Arc::new(FailingInterpretPhase),
        Arc::new(LoopbackPhase::new(PhaseName::Project)),
        Arc::new(LoopbackPhase::new(PhaseName::Verify)),
        Arc::new(LoopbackPhase::new(PhaseName::Live)),
    )?;
    PhaseRunner::new(
        scratch.runner(),
        interrupted_phases,
        CapacityGuard::system(CapacityConfig::default()),
        "production-attestation-exact-range-begin",
        fast_timing(),
    )?
    .with_watch_set_coverage_attestation(chain, generation)
    .redo(
        &live_chain(chain, "http://unused.invalid")?,
        RedoPhase::Phase(PhaseName::Interpret),
        BlockRange::new(0, 0)?,
        CancellationToken::new(),
    )
    .await
    .expect_err("the fixture must leave the attested redo interrupted");

    // Model the recorded Interpret head moving after the audited begin. The token belongs to the
    // durable effective range 0..=0 and must not silently authorize a restart widened to 0..=1.
    seed_branch(scratch.pool(), chain, 1, 1, Some((0, block_hash(1, 0)))).await?;
    publish(scratch.pool(), chain, 1, 1, 0, 0).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET current_block_number = 1, current_block_hash = $2,
             target_block_number = 1, target_block_hash = $2
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain)
    .bind(block_hash(1, 1))
    .execute(scratch.pool())
    .await?;

    let rejected_logs = CapturedLogs::default();
    let rejected_subscriber = tracing_subscriber::fmt()
        .json()
        .without_time()
        .with_writer(rejected_logs.clone())
        .finish();
    let error = loopback_runner(&scratch, "production-attestation-exact-range-reject")?
        .with_watch_set_coverage_attestation(chain, generation)
        .redo(
            &live_chain(chain, "http://unused.invalid")?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(0, 0)?,
            CancellationToken::new(),
        )
        .with_subscriber(rejected_subscriber)
        .await
        .expect_err("the generation token must not authorize a different effective range");
    assert!(
        error
            .to_string()
            .contains("active audited Interpret redo range 0..=0")
    );
    assert!(error.to_string().contains("resolves to 0..=1"));
    assert!(
        !rejected_logs.text().contains("\"replayed\":true"),
        "a rejected range change must not emit replay telemetry"
    );
    let persisted_range: (Option<i64>, Option<i64>, bool) = sqlx::query_as(
        "SELECT phase.redo_from_block_number, phase.redo_to_block_number,
                phase.started_at = audit.attested_at
         FROM chain_phase_state phase
         JOIN manifest_authority_attestations audit
           ON audit.chain_id = phase.chain_id
          AND audit.phase_name = phase.phase_name
          AND audit.generation_token = $2
         WHERE phase.chain_id = $1 AND phase.phase_name = 'interpret'",
    )
    .bind(chain)
    .bind(generation)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(persisted_range, (Some(0), Some(0), true));

    scratch.cleanup().await
}

#[tokio::test]
async fn locked_begin_rejects_an_audit_created_after_tokenless_preflight() -> Result<()> {
    let scratch = ScratchDatabase::create("production_attestation_locked_tokenless_race").await?;
    let chain = "manifest-attestation-locked-tokenless-race";
    let generation = "locked-tokenless-race-generation";
    seed_branch(scratch.pool(), chain, 1, 0, None).await?;
    publish(scratch.pool(), chain, 1, 0, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 0, &block_hash(1, 0)).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;

    let mut blocker = scratch.pool().begin().await?;
    sqlx::query(
        "SELECT phase_name
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'interpret'
         FOR UPDATE",
    )
    .bind(chain)
    .fetch_one(&mut *blocker)
    .await?;

    let rejected_logs = CapturedLogs::default();
    let rejected_subscriber = tracing_subscriber::fmt()
        .json()
        .without_time()
        .with_writer(rejected_logs.clone())
        .finish();
    let racing_runner = loopback_runner(&scratch, "production-attestation-tokenless-racer")?;
    let racing_chain = live_chain(chain, "http://unused.invalid")?;
    let racing = tokio::spawn(async move {
        racing_runner
            .redo(
                &racing_chain,
                RedoPhase::Phase(PhaseName::Interpret),
                BlockRange::new(0, 0).expect("valid raced range"),
                CancellationToken::new(),
            )
            .with_subscriber(rejected_subscriber)
            .await
    });

    let mut waiting_on_locked_begin = false;
    // PostgreSQL may truncate the tracked query before its trailing FOR UPDATE as
    // the locked state-row projection grows, so identify this wait by its SELECT.
    for _ in 0..200 {
        waiting_on_locked_begin = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1
                 FROM pg_stat_activity
                 WHERE datname = current_database()
                   AND wait_event_type = 'Lock'
                   AND query LIKE '%SELECT phase_name%'
                   AND query LIKE '%FROM chain_phase_state%'
             )",
        )
        .fetch_one(scratch.pool())
        .await?;
        if waiting_on_locked_begin {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        waiting_on_locked_begin,
        "the tokenless command must preflight before waiting on the locked begin"
    );

    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'running', redo_in_progress = true, redo_mode = 'redo',
             redo_previous_phase_status = phase_status,
             redo_previous_last_error = last_error,
             redo_previous_started_at = started_at,
             redo_previous_finished_at = finished_at,
             redo_from_block_number = 0, redo_to_block_number = 0,
             started_at = now(), finished_at = NULL
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain)
    .execute(&mut *blocker)
    .await?;
    sqlx::query(
        "INSERT INTO manifest_authority_attestations (
             chain_id, phase_name, generation_token, authority_fingerprint,
             redo_from_block_number, redo_to_block_number, attested_by, attested_at
         )
         SELECT chain_id, phase_name, $2, $3, 0, 0, 'racing-attested-runner', started_at
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain)
    .bind(generation)
    .bind(TEST_AUTHORITY_FINGERPRINT)
    .execute(&mut *blocker)
    .await?;
    blocker.commit().await?;

    let error = racing
        .await?
        .expect_err("locked begin must reject an audit that appeared after tokenless preflight");
    assert!(error.to_string().contains(generation));
    assert!(
        !rejected_logs.text().contains("\"replayed\":true"),
        "the raced tokenless command must not emit replay telemetry"
    );
    let preserved: (bool, Option<i64>, Option<i64>, bool) = sqlx::query_as(
        "SELECT phase.redo_in_progress, phase.redo_from_block_number,
                phase.redo_to_block_number, phase.started_at = audit.attested_at
         FROM chain_phase_state phase
         JOIN manifest_authority_attestations audit
           ON audit.chain_id = phase.chain_id
          AND audit.phase_name = phase.phase_name
          AND audit.generation_token = $2
         WHERE phase.chain_id = $1 AND phase.phase_name = 'interpret'",
    )
    .bind(chain)
    .bind(generation)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(preserved, (true, Some(0), Some(0), true));

    scratch.cleanup().await
}

#[tokio::test]
async fn manifest_authority_fence_applies_when_all_sources_start_after_redo() -> Result<()> {
    let scratch = ScratchDatabase::create("production_manifest_authority_skipped_sources").await?;
    let chain = "manifest-authority-skipped-sources";
    seed_branch(scratch.pool(), chain, 1, 0, None).await?;
    publish(scratch.pool(), chain, 1, 0, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 0, &block_hash(1, 0)).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;
    let generation = "skipped-sources-generation";
    sqlx::query(
        "UPDATE chain_phase_state
         SET input_content_hash = $2
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain)
    .bind(manifest_authority_marker(generation))
    .execute(scratch.pool())
    .await?;
    let future_source_chain = ChainConfig::new(
        chain,
        vec![SourceConfig::new(
            chain,
            "future-rpc",
            "rpc",
            SeedBasis::NewSignatureRange,
            1,
            "http://unused.invalid",
        )?],
        false,
    )?;
    let runner = loopback_runner(&scratch, "production-manifest-authority-skipped-sources")?;

    // Every configured source is outside 0..=0. Before the uniform fence, the source loop skipped
    // them all and the marker was silently adopted because no Live suffix had been recorded.
    let error = runner
        .redo(
            &future_source_chain,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(0, 0)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("a manifest-authority marker must fence even when every source is skipped");
    assert_eq!(error.kind(), phase_runner::error::ErrorKind::DataIntegrity);
    assert_eq!(
        error.to_string(),
        format!(
            "raw-data presence check failed for interpret redo on chain \
manifest-authority-skipped-sources: the manifest authority changed since blocks 0..=0 were \
loaded; invalidation token {generation}; complete any required Ingest redo stamped for this \
authority transition (docs/manifests.md § mandatory historical fetch after watch-plan widening), \
then re-run with --attest-watch-set-coverage {generation} (or \
--attest-watch-set-coverage manifest-authority-skipped-sources={generation} in a multi-chain \
redo)"
        )
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn watch_set_coverage_attestation_without_an_authority_marker_is_rejected() -> Result<()> {
    let scratch =
        ScratchDatabase::create("production_attestation_without_authority_marker").await?;
    let chain = "attestation-without-authority-marker";
    seed_branch(scratch.pool(), chain, 1, 0, None).await?;
    publish(scratch.pool(), chain, 1, 0, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 0, &block_hash(1, 0)).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;
    let runner = loopback_runner(&scratch, "production-attestation-without-authority-marker")?
        .with_watch_set_coverage_attestation(chain, "unused-generation");

    let error = runner
        .redo(
            &live_chain(chain, "http://unused.invalid")?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(0, 0)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("an attestation without a manifest-authority fence must fail");
    assert_eq!(error.kind(), phase_runner::error::ErrorKind::Configuration);
    assert_eq!(
        error.to_string(),
        "--attest-watch-set-coverage supplied invalidation token unused-generation for chain \
attestation-without-authority-marker, but its Interpret redo is not discharging a \
manifest-authority marker"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn watch_set_coverage_attestation_requires_a_recorded_interpret_extent() -> Result<()> {
    let scratch =
        ScratchDatabase::create("production_attestation_without_interpret_extent").await?;
    let chain = "attestation-without-interpret-extent";
    PhaseStore::new(scratch.pool().clone())
        .initialize_chain(chain)
        .await?;
    let generation = "no-extent-generation";
    sqlx::query(
        "UPDATE chain_phase_state
         SET input_content_hash = $2
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain)
    .bind(manifest_authority_marker(generation))
    .execute(scratch.pool())
    .await?;
    let runner = loopback_runner(&scratch, "production-attestation-without-interpret-extent")?
        .with_watch_set_coverage_attestation(chain, generation);

    let error = runner
        .redo(
            &live_chain(chain, "http://unused.invalid")?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(0, 0)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("an attestation cannot discharge a marker without an interpreted extent");
    assert_eq!(error.kind(), phase_runner::error::ErrorKind::Configuration);
    assert_eq!(
        error.to_string(),
        "--attest-watch-set-coverage is not valid for interpret redo on chain \
attestation-without-interpret-extent: the manifest-authority marker has no recorded interpreted \
extent to discharge"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn invalid_all_phase_attestation_is_rejected_before_ingest_runs() -> Result<()> {
    let scratch = ScratchDatabase::create("production_all_attestation_preflight").await?;
    let chain = "all-attestation-without-authority-marker";
    seed_branch(scratch.pool(), chain, 1, 0, None).await?;
    publish(scratch.pool(), chain, 1, 0, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 0, &block_hash(1, 0)).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;
    let phases = PhaseSet::with_ingest_interpret_project_and_live(
        Arc::new(FailingIngestPhase),
        Arc::new(LoopbackPhase::new(PhaseName::Interpret)),
        Arc::new(LoopbackPhase::new(PhaseName::Project)),
        Arc::new(LoopbackPhase::new(PhaseName::Verify)),
        Arc::new(LoopbackPhase::new(PhaseName::Live)),
    )?;
    let runner = PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "production-all-attestation-preflight",
        fast_timing(),
    )?
    .with_watch_set_coverage_attestation(chain, "unused-generation");

    let error = runner
        .redo(
            &live_chain(chain, "http://unused.invalid")?,
            RedoPhase::All,
            BlockRange::new(0, 0)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("an invalid all-phase attestation must fail before Ingest runs");
    assert_eq!(error.kind(), phase_runner::error::ErrorKind::Configuration);
    assert_eq!(
        error.to_string(),
        "--attest-watch-set-coverage supplied invalidation token unused-generation for chain \
all-attestation-without-authority-marker, but its Interpret redo is not discharging a \
manifest-authority marker"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn hash_rotation_replays_the_stamped_project_range_through_the_live_head() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_hash_rotation").await?;
    let chain = "live-hash-rotation";
    seed_branch(scratch.pool(), chain, 1, 0, None).await?;
    publish(scratch.pool(), chain, 1, 0, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 0, &block_hash(1, 0)).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;
    let fixture = RpcFixture::spawn(1, 3).await?;
    let engine = Arc::new(Engine::new(scratch.pool().clone()));
    let runner = production_runner(&scratch, engine, chain)?;
    let configured_chain = live_chain(chain, &fixture.endpoint)?;
    let cancellation = CancellationToken::new();
    let run_cancellation = cancellation.clone();
    let live_runner = Arc::clone(&runner);
    let mut task = tokio::spawn(async move {
        live_runner
            .run_chain(&configured_chain, run_cancellation)
            .await
    });

    wait_for_rederived_or_runner_stop(scratch.pool(), chain, 3, &block_hash(1, 3), &mut task)
        .await?;
    cancellation.cancel();
    task.await??;
    recover_stopped_live_after_exit(runner.as_ref(), scratch.pool(), chain, &fixture.endpoint)
        .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET input_content_hash = 'keccak256:pre-rotation'
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;

    let interrupted_phases = PhaseSet::with_ingest_interpret_project_and_live(
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        Arc::new(InterpretPhase::new(scratch.pool().clone())),
        Arc::new(FailingProjectPhase),
        Arc::new(LoopbackPhase::new(PhaseName::Verify)),
        Arc::new(LoopbackPhase::new(PhaseName::Live)),
    )?;
    let interrupted_runner = PhaseRunner::new(
        scratch.runner(),
        interrupted_phases,
        CapacityGuard::system(CapacityConfig::default()),
        "production-live-hash-rotation-interrupted",
        fast_timing(),
    )?;
    let error = interrupted_runner
        .redo(
            &live_chain(chain, &fixture.endpoint)?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(0, 0)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("the injected Project failure must retain resumable hash-redo state");
    assert!(
        error
            .to_string()
            .contains("forced project redo interruption"),
        "unexpected interrupted hash-rotation error: {error:?}"
    );
    let interrupted: (Option<String>, bool, Option<i64>, Option<i64>) = sqlx::query_as(
        "SELECT input_content_hash, redo_in_progress,
                redo_from_block_number, redo_to_block_number
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        interrupted,
        (
            Some(INTERPRETER_CONTENT_HASH.to_owned()),
            true,
            Some(0),
            Some(3),
        )
    );

    runner
        .redo(
            &live_chain(chain, &fixture.endpoint)?,
            RedoPhase::Phase(PhaseName::Project),
            BlockRange::new(0, 3)?,
            CancellationToken::new(),
        )
        .await?;

    let adopted: Vec<(String, Option<String>, bool)> = sqlx::query_as(
        "SELECT phase_name, input_content_hash, redo_in_progress
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')
         ORDER BY phase_name",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        adopted,
        vec![
            (
                "interpret".to_owned(),
                Some(INTERPRETER_CONTENT_HASH.to_owned()),
                false,
            ),
            (
                "project".to_owned(),
                Some(INTERPRETER_CONTENT_HASH.to_owned()),
                false,
            ),
        ]
    );

    fixture.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn live_gap_fill_is_bounded_and_contiguous() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_gap_fill").await?;
    let chain = "live-gap-fill";
    seed_branch(scratch.pool(), chain, 1, 0, None).await?;
    publish(scratch.pool(), chain, 1, 0, 0, 0).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;
    let fixture = RpcFixture::spawn(1, 600).await?;
    let engine = Engine::new(scratch.pool().clone());
    let request = || live_request(chain, &fixture.endpoint, 0, block_hash(1, 0));

    let first = engine.run_live_batch(request()).await?;
    assert!(!first.caught_up);
    assert_eq!(first.current.number, 256);
    publish_ingest_heads(scratch.pool(), chain, first.heads).await?;
    let second = engine.run_live_batch(request()).await?;
    assert!(!second.caught_up);
    assert_eq!(second.current.number, 512);
    publish_ingest_heads(scratch.pool(), chain, second.heads).await?;
    let third = engine.run_live_batch(request()).await?;
    assert!(third.caught_up);
    assert_eq!(third.current.number, 600);
    publish_ingest_heads(scratch.pool(), chain, third.heads).await?;

    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM chain_lineage
         WHERE chain_id = $1 AND canonicality_state <> 'orphaned'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(count, 601);
    fixture.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn live_retries_when_the_provider_reorgs_between_ancestry_and_suffix_reads() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_snapshot_race").await?;
    let chain = "live-snapshot-race";
    seed_branch(scratch.pool(), chain, 1, 3, None).await?;
    publish(scratch.pool(), chain, 1, 3, 0, 0).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;
    let fixture = RpcFixture::spawn(1, 4).await?;
    fixture.reorg_after_next_number_batch(2, 1, 4).await;
    let engine = Engine::new(scratch.pool().clone());
    let request = || live_request(chain, &fixture.endpoint, 3, block_hash(1, 3));

    let error = engine
        .run_live_batch(request())
        .await
        .expect_err("a suffix disconnected by a provider snapshot change must retry");
    assert_eq!(error.kind(), bigname_ingest::ErrorKind::Transient);

    let recovered = engine.run_live_batch(request()).await?;
    assert!(recovered.caught_up);
    assert_eq!(recovered.current.hash, block_hash(2, 4));
    publish_ingest_heads(scratch.pool(), chain, recovered.heads).await?;
    fixture.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn restart_after_an_unpublished_live_window_republishes_and_advances_the_spine() -> Result<()>
{
    let scratch = ScratchDatabase::create("production_live_restart_unpublished_window").await?;
    let chain = "live-restart-unpublished-window";
    seed_branch(scratch.pool(), chain, 1, 1, None).await?;
    publish(scratch.pool(), chain, 1, 1, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 0, &block_hash(1, 0)).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;
    let fixture = RpcFixture::spawn(1, 3).await?;
    let engine = Arc::new(Engine::new(scratch.pool().clone()));

    let unpublished = engine
        .run_live_batch(live_request(chain, &fixture.endpoint, 0, block_hash(1, 0)))
        .await?;
    assert_eq!(unpublished.current.number, 3);
    let staged: Vec<(i64, String)> = sqlx::query_as(
        "SELECT block_number, canonicality_state::text
         FROM chain_lineage
         WHERE chain_id = $1 AND block_number > 1
         ORDER BY block_number",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        staged,
        vec![(2, "observed".to_owned()), (3, "observed".to_owned())]
    );

    let runner = production_runner(&scratch, Arc::clone(&engine), chain)?;
    let configured_chain = live_chain(chain, &fixture.endpoint)?;
    let cancellation = CancellationToken::new();
    let run_cancellation = cancellation.clone();
    let mut task =
        tokio::spawn(async move { runner.run_chain(&configured_chain, run_cancellation).await });
    wait_for_rederived_or_runner_stop(scratch.pool(), chain, 3, &block_hash(1, 3), &mut task)
        .await?;
    cancellation.cancel();
    task.await??;

    let recovered: Vec<(i64, String)> = sqlx::query_as(
        "SELECT block_number, canonicality_state::text
         FROM chain_lineage
         WHERE chain_id = $1 AND block_number > 1
         ORDER BY block_number",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        recovered,
        vec![(2, "canonical".to_owned()), (3, "canonical".to_owned())]
    );
    fixture.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn restart_after_an_unpublished_losing_fork_orphans_the_observed_suffix() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_restart_losing_observed").await?;
    let chain = "live-restart-losing-observed";
    seed_branch(scratch.pool(), chain, 1, 1, None).await?;
    publish(scratch.pool(), chain, 1, 1, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 0, &block_hash(1, 0)).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;
    let fixture = RpcFixture::spawn(1, 3).await?;
    let engine = Arc::new(Engine::new(scratch.pool().clone()));

    let unpublished = engine
        .run_live_batch(live_request(chain, &fixture.endpoint, 0, block_hash(1, 0)))
        .await?;
    assert_eq!(unpublished.current.hash, block_hash(1, 3));
    fixture.reorg(2, 1, 3).await;

    let runner = production_runner(&scratch, Arc::clone(&engine), chain)?;
    let configured_chain = live_chain(chain, &fixture.endpoint)?;
    let cancellation = CancellationToken::new();
    let run_cancellation = cancellation.clone();
    let mut task =
        tokio::spawn(async move { runner.run_chain(&configured_chain, run_cancellation).await });
    wait_for_rederived_or_runner_stop(scratch.pool(), chain, 3, &block_hash(2, 3), &mut task)
        .await?;
    cancellation.cancel();
    task.await??;

    let suffix: Vec<(i64, String, String)> = sqlx::query_as(
        "SELECT block_number, block_hash, canonicality_state::text
         FROM chain_lineage
         WHERE chain_id = $1 AND block_number > 1
         ORDER BY block_number, block_hash",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        suffix,
        vec![
            (2, block_hash(1, 2), "orphaned".to_owned()),
            (2, block_hash(2, 2), "canonical".to_owned()),
            (3, block_hash(1, 3), "orphaned".to_owned()),
            (3, block_hash(2, 3), "canonical".to_owned()),
        ]
    );
    fixture.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn live_reorg_walk_loads_the_winning_suffix_and_stamps_cursors() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_reorg_walk").await?;
    let chain = "live-reorg-walk";
    seed_branch(scratch.pool(), chain, 1, 3, None).await?;
    publish(scratch.pool(), chain, 1, 3, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 3, &block_hash(1, 3)).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;
    let fixture = RpcFixture::spawn(1, 3).await?;
    fixture.reorg(2, 1, 3).await;

    let outcome = Engine::new(scratch.pool().clone())
        .run_live_batch(live_request(chain, &fixture.endpoint, 3, block_hash(1, 3)))
        .await?;
    assert!(outcome.caught_up);
    assert_eq!(outcome.current.hash, block_hash(2, 3));
    publish_ingest_heads(scratch.pool(), chain, outcome.heads).await?;

    let lineage: Vec<(i64, String, String)> = sqlx::query_as(
        "SELECT block_number, block_hash, canonicality_state::text
         FROM chain_lineage WHERE chain_id = $1 AND block_number >= 2
         ORDER BY block_number, block_hash",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        lineage,
        vec![
            (2, block_hash(1, 2), "orphaned".into()),
            (2, block_hash(2, 2), "canonical".into()),
            (3, block_hash(1, 3), "orphaned".into()),
            (3, block_hash(2, 3), "canonical".into()),
        ]
    );
    let stamps: Vec<(String, Option<i64>, Option<i64>)> = sqlx::query_as(
        "SELECT phase_name, redo_from_block_number, redo_to_block_number
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')
         ORDER BY phase_name",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        stamps,
        vec![
            ("interpret".into(), Some(2), Some(3)),
            ("project".into(), Some(2), Some(3)),
        ]
    );
    fixture.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn lagging_same_fork_snapshot_does_not_publish_or_stamp() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_lagging_same_fork").await?;
    let chain = "live-lagging-same-fork";
    seed_branch(scratch.pool(), chain, 1, 3, None).await?;
    publish(scratch.pool(), chain, 1, 3, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 3, &block_hash(1, 3)).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;
    let fixture = RpcFixture::spawn(1, 2).await?;

    let outcome = LivePhase::new(scratch.pool().clone())
        .run_batch(live_context(chain, &fixture.endpoint, 3, block_hash(1, 3))?)
        .await?;
    if let Some(heads) = &outcome.progress().heads {
        publish_heads(scratch.pool(), chain, heads).await?;
    }

    assert!(
        outcome.progress().heads.is_none(),
        "a lagging same-fork snapshot must not request head publication"
    );
    let head: (i64, String) = sqlx::query_as(
        "SELECT latest_block_number, latest_block_hash
         FROM chain_heads WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(head, (3, block_hash(1, 3)));
    let suffix_state: String = sqlx::query_scalar(
        "SELECT canonicality_state::text FROM chain_lineage
         WHERE chain_id = $1 AND block_hash = $2",
    )
    .bind(chain)
    .bind(block_hash(1, 3))
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(suffix_state, "canonical");
    let stamps: Vec<(String, bool)> = sqlx::query_as(
        "SELECT phase_name, redo_in_progress
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')
         ORDER BY phase_name",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        stamps,
        vec![
            ("interpret".to_owned(), false),
            ("project".to_owned(), false)
        ]
    );
    fixture.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn equal_head_lag_preserves_unpublished_same_fork_observations() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_equal_head_lag").await?;
    let chain = "live-equal-head-lag";
    seed_branch(scratch.pool(), chain, 1, 1, None).await?;
    publish(scratch.pool(), chain, 1, 1, 0, 0).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;
    let fixture = RpcFixture::spawn(1, 3).await?;
    let engine = Engine::new(scratch.pool().clone());

    let unpublished = engine
        .run_live_batch(live_request(chain, &fixture.endpoint, 1, block_hash(1, 1)))
        .await?;
    assert_eq!(unpublished.current.number, 3);
    fixture.reorg(1, 1, 1).await;
    let lagging = engine
        .run_live_batch(live_request(chain, &fixture.endpoint, 1, block_hash(1, 1)))
        .await?;
    publish_ingest_heads(scratch.pool(), chain, lagging.heads).await?;

    let staged: Vec<(i64, String)> = sqlx::query_as(
        "SELECT block_number, canonicality_state::text
         FROM chain_lineage
         WHERE chain_id = $1 AND block_number > 1
         ORDER BY block_number",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        staged,
        vec![(2, "observed".to_owned()), (3, "observed".to_owned())]
    );
    fixture.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn genuine_lower_fork_snapshot_still_publishes_the_reorg() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_genuine_lower_fork").await?;
    let chain = "live-genuine-lower-fork";
    seed_branch(scratch.pool(), chain, 1, 3, None).await?;
    publish(scratch.pool(), chain, 1, 3, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 3, &block_hash(1, 3)).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;
    let fixture = RpcFixture::spawn(1, 3).await?;
    fixture.reorg(2, 1, 2).await;

    let outcome = LivePhase::new(scratch.pool().clone())
        .run_batch(live_context(chain, &fixture.endpoint, 3, block_hash(1, 3))?)
        .await?;
    let heads = outcome
        .progress()
        .heads
        .as_ref()
        .expect("a genuine lower fork must request head publication");
    publish_heads(scratch.pool(), chain, heads).await?;

    let head: (i64, String) = sqlx::query_as(
        "SELECT latest_block_number, latest_block_hash
         FROM chain_heads WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(head, (2, block_hash(2, 2)));
    let stamps: Vec<(String, Option<i64>, Option<i64>)> = sqlx::query_as(
        "SELECT phase_name, redo_from_block_number, redo_to_block_number
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')
         ORDER BY phase_name",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        stamps,
        vec![
            ("interpret".to_owned(), Some(2), Some(3)),
            ("project".to_owned(), Some(2), Some(3)),
        ]
    );
    fixture.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn live_lower_head_waits_for_the_stamped_range_before_downstream_redo() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_lower_head_reorg").await?;
    let chain = "live-lower-head-reorg";
    seed_branch(scratch.pool(), chain, 1, 3, None).await?;
    publish(scratch.pool(), chain, 1, 3, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 3, &block_hash(1, 3)).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;
    let fixture = RpcFixture::spawn(1, 3).await?;
    fixture.reorg(2, 1, 2).await;

    let ingest_engine = Arc::new(Engine::new(scratch.pool().clone()));
    let phases = PhaseSet::with_ingest_interpret_project_and_live(
        Arc::new(IngestPhase::with_engine(Arc::clone(&ingest_engine))),
        Arc::new(LoopbackPhase::new(PhaseName::Interpret)),
        Arc::new(LoopbackPhase::new(PhaseName::Project)),
        Arc::new(LoopbackPhase::new(PhaseName::Verify)),
        Arc::new(LivePhase::with_engine(ingest_engine)),
    )?;
    let runner = Arc::new(PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "production-live-lower-head-reorg",
        fast_timing(),
    )?);
    let configured_chain = live_chain(chain, &fixture.endpoint)?;
    let cancellation = CancellationToken::new();
    let run_cancellation = cancellation.clone();
    let mut task =
        tokio::spawn(async move { runner.run_chain(&configured_chain, run_cancellation).await });

    wait_for_head(scratch.pool(), chain, 2, &block_hash(2, 2)).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    if task.is_finished() {
        let result = (&mut task).await?;
        fixture.server.abort();
        scratch.cleanup().await?;
        anyhow::bail!(
            "live runner stopped instead of waiting for the stamped redo range: {result:?}"
        );
    }

    fixture.reorg(2, 1, 3).await;
    wait_for_rederived_head(scratch.pool(), chain, 3, &block_hash(2, 3)).await?;
    cancellation.cancel();
    task.await??;

    fixture.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn restart_recovers_live_running_after_head_publication() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_restart_after_publication").await?;
    let chain = "live-restart-after-publication";
    seed_branch(scratch.pool(), chain, 1, 1, None).await?;
    publish(scratch.pool(), chain, 1, 0, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 0, &block_hash(1, 0)).await?;
    publish(scratch.pool(), chain, 1, 1, 0, 0).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'running', current_block_number = 1,
             current_block_hash = $2, target_block_number = 1,
             target_block_hash = $2, started_at = now(), finished_at = NULL
         WHERE chain_id = $1 AND phase_name = 'live'",
    )
    .bind(chain)
    .bind(block_hash(1, 1))
    .execute(scratch.pool())
    .await?;

    let runner = Arc::new(PhaseRunner::new(
        scratch.runner(),
        PhaseSet::loopback(),
        CapacityGuard::system(CapacityConfig::default()),
        "production-live-restart-after-publication",
        fast_timing(),
    )?);
    let configured_chain = live_chain(chain, "http://unused.invalid")?;
    let cancellation = CancellationToken::new();
    let run_cancellation = cancellation.clone();
    let task =
        tokio::spawn(async move { runner.run_chain(&configured_chain, run_cancellation).await });
    wait_for_rederived_head(scratch.pool(), chain, 1, &block_hash(1, 1)).await?;
    cancellation.cancel();
    task.await??;

    let cursors: Vec<(String, Option<String>)> = sqlx::query_as(
        "SELECT phase_name, current_block_hash FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')
         ORDER BY phase_name",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert!(
        cursors
            .iter()
            .all(|(_, hash)| hash.as_deref() == Some(block_hash(1, 1).as_str()))
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn failed_system_redo_keeps_automatic_ownership() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_required_redo_failure").await?;
    let chain = "live-required-redo-failure";
    seed_branch(scratch.pool(), chain, 1, 3, None).await?;
    publish(scratch.pool(), chain, 1, 3, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 3, &block_hash(1, 3)).await?;
    seed_branch(scratch.pool(), chain, 2, 3, Some((1, block_hash(1, 1)))).await?;
    publish(scratch.pool(), chain, 2, 3, 0, 0).await?;

    let phases = PhaseSet::new([
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        Arc::new(FailingInterpretPhase),
        Arc::new(LoopbackPhase::new(PhaseName::Project)),
        Arc::new(LoopbackPhase::new(PhaseName::Verify)),
        Arc::new(LoopbackPhase::new(PhaseName::Live)),
    ])?;
    let error = PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "production-live-required-redo-failure",
        fast_timing(),
    )?
    .run_chain(
        &live_chain(chain, "http://unused.invalid")?,
        CancellationToken::new(),
    )
    .await
    .expect_err("fixture interpret redo must fail");
    assert!(error.to_string().contains("forced required-redo failure"));

    let state: (bool, Option<String>) = sqlx::query_as(
        "SELECT redo_in_progress, last_error FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert!(state.0);
    assert!(
        state
            .1
            .as_deref()
            .is_some_and(|message| message.starts_with("required downstream redo: "))
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn pending_required_redo_stamp_widens_and_keeps_ownership() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_pending_stamp_widening").await?;
    let chain = "live-pending-stamp-widening";
    seed_branch(scratch.pool(), chain, 1, 5, None).await?;
    publish(scratch.pool(), chain, 1, 5, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 5, &block_hash(1, 5)).await?;

    seed_branch(scratch.pool(), chain, 2, 5, Some((2, block_hash(1, 2)))).await?;
    publish(scratch.pool(), chain, 2, 5, 0, 0).await?;
    let pending: Vec<RedoStampSnapshot> = sqlx::query_as(
        "SELECT phase_name, redo_from_block_number, redo_to_block_number, last_error
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')
         ORDER BY phase_name",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert!(pending.iter().all(|(_, from, to, owner)| {
        *from == Some(3)
            && *to == Some(5)
            && owner
                .as_deref()
                .is_some_and(|message| message.starts_with("required downstream redo: "))
    }));

    seed_branch(scratch.pool(), chain, 3, 5, Some((0, block_hash(1, 0)))).await?;
    publish(scratch.pool(), chain, 3, 5, 0, 0).await?;
    let widened: Vec<RedoStampSnapshot> = sqlx::query_as(
        "SELECT phase_name, redo_from_block_number, redo_to_block_number, last_error
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')
         ORDER BY phase_name",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert!(widened.iter().all(|(_, from, to, owner)| {
        *from == Some(1)
            && *to == Some(5)
            && owner
                .as_deref()
                .is_some_and(|message| message.starts_with("required downstream redo: "))
    }));
    scratch.cleanup().await
}

#[tokio::test]
async fn failed_operator_redo_range_widens_without_losing_the_operator_error() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_operator_stamp_widening").await?;
    let chain = "live-operator-stamp-widening";
    seed_branch(scratch.pool(), chain, 1, 5, None).await?;
    publish(scratch.pool(), chain, 1, 5, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 5, &block_hash(1, 5)).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'running', redo_in_progress = true, redo_mode = 'redo',
             redo_previous_phase_status = 'completed',
             redo_previous_started_at = started_at,
             redo_previous_finished_at = finished_at,
             redo_from_block_number = 3, redo_to_block_number = 5,
             redo_current_block_number = 4, redo_current_block_hash = $2,
             redo_target_block_number = 5, redo_target_block_hash = $3,
             last_error = 'operator redo failed: fixture',
             started_at = now(), finished_at = NULL
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain)
    .bind(block_hash(1, 4))
    .bind(block_hash(1, 5))
    .execute(scratch.pool())
    .await?;

    seed_branch(scratch.pool(), chain, 2, 5, Some((0, block_hash(1, 0)))).await?;
    publish(scratch.pool(), chain, 2, 5, 0, 0).await?;
    let state: (Option<i64>, Option<i64>, Option<i64>, Option<String>) = sqlx::query_as(
        "SELECT redo_from_block_number, redo_to_block_number,
                redo_current_block_number, last_error
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        state,
        (
            Some(1),
            Some(5),
            None,
            Some("operator redo failed: fixture".to_owned())
        )
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn active_required_redo_blocks_another_writer_phase() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_required_redo_exclusion").await?;
    let chain = "live-required-redo-exclusion";
    seed_branch(scratch.pool(), chain, 1, 2, None).await?;
    publish(scratch.pool(), chain, 1, 2, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 1, &block_hash(1, 1)).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'running', redo_in_progress = true, redo_mode = 'redo',
             redo_previous_phase_status = 'completed',
             redo_previous_started_at = started_at,
             redo_previous_finished_at = finished_at,
             redo_from_block_number = 1,
             redo_to_block_number = 1,
             last_error = 'required downstream redo active: active fixture',
             started_at = now(), finished_at = NULL
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;

    let store = PhaseStore::new(scratch.pool().clone());
    let error = store
        .start_phase(chain, PhaseName::Interpret, &RunMode::Normal)
        .await
        .expect_err("an active required project redo must exclude interpret writes");
    assert_eq!(
        error.kind(),
        phase_runner::error::ErrorKind::InvalidTransition
    );
    assert!(error.to_string().contains("while phase project is running"));
    scratch.cleanup().await
}

#[tokio::test]
async fn unsupported_live_redo_is_rejected_without_persisting_redo_state() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_unsupported_redo").await?;
    let chain = "live-unsupported-redo";
    seed_branch(scratch.pool(), chain, 1, 1, None).await?;
    publish(scratch.pool(), chain, 1, 1, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 1, &block_hash(1, 1)).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed', current_block_number = 1,
             current_block_hash = $2, target_block_number = 1,
             target_block_hash = $2, started_at = now(), finished_at = now()
         WHERE chain_id = $1 AND phase_name = 'live'",
    )
    .bind(chain)
    .bind(block_hash(1, 1))
    .execute(scratch.pool())
    .await?;
    let engine = Arc::new(Engine::new(scratch.pool().clone()));
    let runner = PhaseRunner::new(
        scratch.runner(),
        PhaseSet::with_ingest_interpret_project_and_live(
            Arc::new(IngestPhase::with_engine(Arc::clone(&engine))),
            Arc::new(LoopbackPhase::new(PhaseName::Interpret)),
            Arc::new(LoopbackPhase::new(PhaseName::Project)),
            Arc::new(LoopbackPhase::new(PhaseName::Verify)),
            Arc::new(LivePhase::with_engine(engine)),
        )?,
        CapacityGuard::system(CapacityConfig::default()),
        "production-live-unsupported-redo",
        fast_timing(),
    )?;

    let error = runner
        .redo(
            &live_chain(chain, "http://unused.invalid")?,
            RedoPhase::Phase(PhaseName::Live),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("live redo is unavailable");
    assert_eq!(error.kind(), phase_runner::error::ErrorKind::Configuration);
    let state: (String, bool, Option<String>) = sqlx::query_as(
        "SELECT phase_status, redo_in_progress, last_error
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'live'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(state, ("completed".to_owned(), false, None));
    scratch.cleanup().await
}

#[tokio::test]
async fn verify_preflight_failure_is_rejected_before_persisting_redo_state() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_verify_preflight_redo").await?;
    let chain = "live-verify-preflight-redo";
    seed_branch(scratch.pool(), chain, 1, 1, None).await?;
    publish(scratch.pool(), chain, 1, 1, 0, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 1, &block_hash(1, 1)).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed', verification_level = 'quick_synced',
             current_block_number = 1, current_block_hash = $2,
             target_block_number = 1, target_block_hash = $2,
             started_at = now(), finished_at = now(), updated_at = now()
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(chain)
    .bind(block_hash(1, 1))
    .execute(scratch.pool())
    .await?;
    let before: (
        String,
        Option<String>,
        bool,
        Option<i64>,
        Option<i64>,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT phase_status, verification_level, redo_in_progress,
                    redo_from_block_number, redo_to_block_number, last_error
             FROM chain_phase_state
             WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;

    let engine = Arc::new(Engine::new(scratch.pool().clone()));
    let runner = PhaseRunner::new(
        scratch.runner(),
        PhaseSet::with_ingest_interpret_project_and_live(
            Arc::new(IngestPhase::with_engine(Arc::clone(&engine))),
            Arc::new(LoopbackPhase::new(PhaseName::Interpret)),
            Arc::new(LoopbackPhase::new(PhaseName::Project)),
            Arc::new(FailingVerifyPreflightPhase),
            Arc::new(LivePhase::with_engine(engine)),
        )?,
        CapacityGuard::system(CapacityConfig::default()),
        "production-live-verify-preflight-redo",
        fast_timing(),
    )?;
    let error = runner
        .redo(
            &live_chain(chain, "http://unused.invalid")?,
            RedoPhase::Phase(PhaseName::Verify),
            BlockRange::new(1, 1)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("verify preflight must refuse redo before creating its marker");
    assert_eq!(error.kind(), phase_runner::error::ErrorKind::Configuration);
    assert!(
        error
            .to_string()
            .contains("fixture verify preflight failed")
    );

    let after: (
        String,
        Option<String>,
        bool,
        Option<i64>,
        Option<i64>,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT phase_status, verification_level, redo_in_progress,
                    redo_from_block_number, redo_to_block_number, last_error
             FROM chain_phase_state
             WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(after, before);
    scratch.cleanup().await
}

#[tokio::test]
async fn rewind_uses_head_publication_and_stamps_downstream_redo() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_rewind").await?;
    let chain = "live-rewind";
    seed_branch(scratch.pool(), chain, 1, 3, None).await?;
    publish(scratch.pool(), chain, 1, 3, 1, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 3, &block_hash(1, 3)).await?;
    seed_empty_watch_manifest(scratch.pool(), chain).await?;

    let outcome = rewind_to_ancestor(
        &scratch.runner(),
        chain,
        BlockMarker::new(1, block_hash(1, 1))?,
    )
    .await?;
    assert_eq!(outcome.previous.number, 3);
    assert_eq!(outcome.ancestor.number, 1);

    let head: (i64, String) = sqlx::query_as(
        "SELECT latest_block_number, latest_block_hash
         FROM chain_heads WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(head, (1, block_hash(1, 1)));
    let states: Vec<(String, bool, Option<i64>, Option<i64>)> = sqlx::query_as(
        "SELECT phase_name, redo_in_progress,
                redo_from_block_number, redo_to_block_number
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')
         ORDER BY phase_name",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        states,
        vec![
            ("interpret".into(), true, Some(2), Some(3)),
            ("project".into(), true, Some(2), Some(3)),
        ]
    );
    let orphaned: Vec<i64> = sqlx::query_scalar(
        "SELECT block_number FROM chain_lineage
         WHERE chain_id = $1 AND canonicality_state = 'orphaned'
         ORDER BY block_number",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(orphaned, vec![2, 3]);
    let fixture = RpcFixture::spawn(1, 3).await?;
    let refilled = Engine::new(scratch.pool().clone())
        .run_live_batch(live_request(chain, &fixture.endpoint, 3, block_hash(1, 3)))
        .await?;
    assert!(refilled.caught_up);
    assert_eq!(refilled.current.hash, block_hash(1, 3));
    fixture.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn rewind_refuses_missing_and_non_readable_ancestors_without_state_change() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_rewind_invalid_ancestor").await?;
    let chain = "live-rewind-invalid-ancestor";
    seed_branch(scratch.pool(), chain, 1, 3, None).await?;
    publish(scratch.pool(), chain, 1, 3, 1, 0).await?;
    seed_completed_spine(scratch.pool(), chain, 3, &block_hash(1, 3)).await?;
    seed_branch(scratch.pool(), chain, 2, 2, Some((0, block_hash(1, 0)))).await?;
    let before = rewind_snapshot(scratch.pool(), chain).await?;

    let missing = rewind_to_ancestor(
        &scratch.runner(),
        chain,
        BlockMarker::new(
            1,
            "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        )?,
    )
    .await
    .expect_err("rewind must refuse an ancestor that is not stored");
    assert_eq!(
        missing.kind(),
        phase_runner::error::ErrorKind::DataIntegrity
    );
    assert!(missing.to_string().contains("is not stored"));
    assert_eq!(rewind_snapshot(scratch.pool(), chain).await?, before);

    let observed = rewind_to_ancestor(
        &scratch.runner(),
        chain,
        BlockMarker::new(1, block_hash(2, 1))?,
    )
    .await
    .expect_err("rewind must refuse an observed ancestor");
    assert_eq!(
        observed.kind(),
        phase_runner::error::ErrorKind::DataIntegrity
    );
    assert!(observed.to_string().contains("not on the readable path"));
    assert_eq!(rewind_snapshot(scratch.pool(), chain).await?, before);
    scratch.cleanup().await
}

#[tokio::test]
async fn rewind_refuses_to_cross_the_safe_head_without_state_change() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_rewind_safe_boundary").await?;
    let chain = "live-rewind-safe-boundary";
    seed_branch(scratch.pool(), chain, 1, 3, None).await?;
    publish(scratch.pool(), chain, 1, 3, 2, 1).await?;
    seed_completed_spine(scratch.pool(), chain, 3, &block_hash(1, 3)).await?;
    let before = rewind_snapshot(scratch.pool(), chain).await?;

    let error = rewind_to_ancestor(
        &scratch.runner(),
        chain,
        BlockMarker::new(1, block_hash(1, 1))?,
    )
    .await
    .expect_err("rewind must not cross the published safe head");
    assert_eq!(error.kind(), phase_runner::error::ErrorKind::DataIntegrity);
    assert!(error.to_string().contains("below safe block 2"));
    assert_eq!(rewind_snapshot(scratch.pool(), chain).await?, before);
    scratch.cleanup().await
}

#[tokio::test]
async fn rewind_refuses_to_overlap_a_downstream_writer() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_rewind_lock").await?;
    let chain = "live-rewind-lock";
    seed_branch(scratch.pool(), chain, 1, 2, None).await?;
    publish(scratch.pool(), chain, 1, 2, 0, 0).await?;
    let database = scratch.runner();
    let mut project_lock = scratch.pool().acquire().await?;
    let lock_name = format!("phase-runner:{chain}:project");
    sqlx::query("SELECT pg_advisory_lock(hashtextextended($1::text, 0::bigint))")
        .bind(&lock_name)
        .execute(&mut *project_lock)
        .await?;

    let error = rewind_to_ancestor(&database, chain, BlockMarker::new(1, block_hash(1, 1))?)
        .await
        .expect_err("rewind must not race an active downstream writer");
    assert_eq!(error.kind(), phase_runner::error::ErrorKind::LockHeld);

    let released: bool =
        sqlx::query_scalar("SELECT pg_advisory_unlock(hashtextextended($1::text, 0::bigint))")
            .bind(lock_name)
            .fetch_one(&mut *project_lock)
            .await?;
    assert!(released);
    scratch.cleanup().await
}

#[tokio::test]
async fn event_silent_reverse_hydration_refreshes_and_follows_a_fork() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_reverse_hydration").await?;
    seed_branch(scratch.pool(), ETHEREUM, 1, 2, None).await?;
    publish(scratch.pool(), ETHEREUM, 1, 1, 0, 0).await?;
    seed_reverse_candidate(scratch.pool()).await?;
    let rpc = HydrationRpc::spawn(BTreeMap::from([
        (block_hash(1, 1), "alice.eth".to_owned()),
        (block_hash(1, 2), "bob.eth".to_owned()),
        (block_hash(2, 2), String::new()),
    ]))
    .await?;
    let hydrator = Hydrator::new(
        scratch.pool().clone(),
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM}={}", rpc.endpoint)])?,
    );

    let first = hydrator.hydrate_canonical_head(ETHEREUM).await?;
    assert_eq!(first.updated_rows, 1);
    assert_primary(
        scratch.pool(),
        "success",
        Some("alice.eth"),
        &block_hash(1, 1),
    )
    .await?;

    publish(scratch.pool(), ETHEREUM, 1, 2, 0, 0).await?;
    hydrator.hydrate_canonical_head(ETHEREUM).await?;
    assert_primary(
        scratch.pool(),
        "success",
        Some("bob.eth"),
        &block_hash(1, 2),
    )
    .await?;

    seed_branch(scratch.pool(), ETHEREUM, 2, 2, Some((1, block_hash(1, 1)))).await?;
    publish(scratch.pool(), ETHEREUM, 2, 2, 0, 0).await?;
    hydrator.hydrate_canonical_head(ETHEREUM).await?;
    assert_primary(scratch.pool(), "not_found", None, &block_hash(2, 2)).await?;

    rpc.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn event_silent_reverse_hydration_bounds_the_rolling_refresh_batch() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_reverse_hydration_bound").await?;
    seed_branch(scratch.pool(), ETHEREUM, 1, 2, None).await?;
    publish(scratch.pool(), ETHEREUM, 1, 2, 0, 0).await?;
    sqlx::query(
        "INSERT INTO primary_names_current (
             address, coin_type, namespace, claim_status,
             unsupported_reason, claim_provenance
         )
         SELECT '0x' || lpad(to_hex(candidate), 40, '0'),
                '60', 'ens', 'unsupported',
                'legacy_resolver_does_not_emit_name',
                jsonb_build_object(
                    'chain_id', $1::text,
                    'reverse_node', '0x' || lpad(to_hex(candidate), 64, '0'),
                    'resolver_address', $2::text,
                    'target_block_number', 1,
                    'target_block_hash', $3::text
                )
         FROM generate_series(1, 251) candidate",
    )
    .bind(ETHEREUM)
    .bind(REVERSE_RESOLVER)
    .bind(block_hash(1, 1))
    .execute(scratch.pool())
    .await?;
    let names = std::iter::repeat_n("alice.eth", 250)
        .collect::<Vec<_>>()
        .join("|");
    let rpc = HydrationRpc::spawn(BTreeMap::from([(
        block_hash(1, 2),
        format!("{MULTICALL_RESULTS_PREFIX}{names}"),
    )]))
    .await?;
    let hydrator = Hydrator::new(
        scratch.pool().clone(),
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM}={}", rpc.endpoint)])?,
    );

    let outcome = hydrator.hydrate_canonical_head(ETHEREUM).await?;
    assert_eq!(outcome.reverse_candidates, 250);
    assert_eq!(outcome.updated_rows, 250);
    let refreshed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM primary_names_current
         WHERE claim_provenance ? 'canonical_head_multicall_hydration'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(refreshed, 250);

    rpc.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn failed_reverse_hydration_page_keeps_its_place_across_heads() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_reverse_hydration_cross_head").await?;
    seed_branch(scratch.pool(), ETHEREUM, 1, 4, None).await?;
    seed_cross_head_reverse_hydration_page(scratch.pool()).await?;
    let rpc = SelectiveFailureHydrationRpc::spawn(1, 251).await?;
    let hydrator = Hydrator::new(
        scratch.pool().clone(),
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM}={}", rpc.endpoint)])?,
    );

    let mut transient_by_head = Vec::new();
    for head in 2..=4 {
        publish(scratch.pool(), ETHEREUM, 1, head, 1, 1).await?;
        transient_by_head.push(match hydrator.hydrate_canonical_head(ETHEREUM).await {
            Ok(outcome) => {
                assert_eq!(outcome.reverse_candidates, 1);
                false
            }
            Err(error) => {
                assert_eq!(error.kind(), bigname_project::ErrorKind::Transient);
                true
            }
        });
    }

    assert_eq!(
        transient_by_head,
        vec![false, true, false],
        "the failed group must not re-lead at every new head"
    );

    let batches = rpc.batches.lock().expect("batch observations").clone();
    assert_eq!(
        batches,
        vec![
            ObservedHydrationBatch {
                poisoned: false,
                call_count: 1,
                contains_last_row: true,
            },
            ObservedHydrationBatch {
                poisoned: true,
                call_count: 250,
                contains_last_row: false,
            },
            ObservedHydrationBatch {
                poisoned: false,
                call_count: 1,
                contains_last_row: true,
            },
        ],
        "a failed group must retain its global round-robin position across heads"
    );

    let failed = load_primary_name_current(
        scratch.pool(),
        "0x0000000000000000000000000000000000000001",
        "ens",
        "60",
    )
    .await?
    .expect("a failed row keeps its event-derived baseline");
    assert_eq!(failed.claim_status, PrimaryNameClaimStatus::Unsupported);
    assert_eq!(failed.raw_claim_name, None);

    let refreshed = load_primary_name_current_snapshot(
        scratch.pool(),
        "0x00000000000000000000000000000000000000fb",
        "ens",
        "60",
    )
    .await?
    .expect("the waiting row remains readable");
    assert_eq!(refreshed.row.claim_status, PrimaryNameClaimStatus::Success);
    assert_eq!(refreshed.row.raw_claim_name.as_deref(), Some("new.eth"));
    assert_eq!(refreshed.normalized_claim_name.as_deref(), Some("new.eth"));

    rpc.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn failed_reverse_hydration_page_does_not_starve_the_next_rolling_row() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_reverse_hydration_fairness").await?;
    seed_branch(scratch.pool(), ETHEREUM, 1, 2, None).await?;
    publish(scratch.pool(), ETHEREUM, 1, 2, 1, 1).await?;
    seed_old_reverse_hydration_page(scratch.pool()).await?;
    let rpc = SelectiveFailureHydrationRpc::spawn(1, 251).await?;
    let hydrator = Hydrator::new(
        scratch.pool().clone(),
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM}={}", rpc.endpoint)])?,
    );

    let error = hydrator
        .hydrate_canonical_head(ETHEREUM)
        .await
        .expect_err("the poisoned first page must remain retryable");
    assert_eq!(error.kind(), bigname_project::ErrorKind::Transient);
    let retry = hydrator
        .hydrate_canonical_head(ETHEREUM)
        .await
        .expect("a same-head retry must advance beyond the failed page");
    assert_eq!(retry.reverse_candidates, 1);

    let batches = rpc.batches.lock().expect("batch observations").clone();
    assert_eq!(
        batches,
        vec![
            ObservedHydrationBatch {
                poisoned: true,
                call_count: 250,
                contains_last_row: false,
            },
            ObservedHydrationBatch {
                poisoned: false,
                call_count: 1,
                contains_last_row: true,
            },
        ]
    );

    let failed = load_primary_name_current(
        scratch.pool(),
        "0x0000000000000000000000000000000000000001",
        "ens",
        "60",
    )
    .await?
    .expect("a failed row keeps its event-derived baseline");
    assert_eq!(failed.claim_status, PrimaryNameClaimStatus::Unsupported);
    assert_eq!(failed.raw_claim_name, None);

    let attempt_state: Vec<ObservedReverseHydrationAttempt> = sqlx::query_as(
        "SELECT address,
                    reverse_hydration_attempted_block_number AS attempted_block_number,
                    reverse_hydration_attempted_block_hash AS attempted_block_hash,
                    reverse_hydration_attempt_ordinal AS attempt_ordinal,
                    claim_provenance ? 'canonical_head_multicall_hydration'
                        AS has_serving_marker
             FROM primary_names_current
             WHERE address IN (
                 '0x0000000000000000000000000000000000000001',
                 '0x00000000000000000000000000000000000000fb'
             )
             ORDER BY address",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(attempt_state.len(), 2);
    assert_eq!(
        attempt_state[0].address,
        "0x0000000000000000000000000000000000000001"
    );
    assert_eq!(attempt_state[0].attempted_block_number, Some(2));
    assert_eq!(
        attempt_state[0].attempted_block_hash.as_deref(),
        Some(block_hash(1, 2).as_str())
    );
    assert!(
        !attempt_state[0].has_serving_marker,
        "a failed call has no serving marker"
    );
    assert_eq!(attempt_state[1].attempted_block_number, Some(2));
    assert_eq!(
        attempt_state[1].attempted_block_hash.as_deref(),
        Some(block_hash(1, 2).as_str())
    );
    assert!(
        attempt_state[1].has_serving_marker,
        "a successful call has a serving marker"
    );
    assert!(
        attempt_state[0].attempt_ordinal < attempt_state[1].attempt_ordinal,
        "the retry gets a later durable attempt order"
    );

    let refreshed = load_primary_name_current_snapshot(
        scratch.pool(),
        "0x00000000000000000000000000000000000000fb",
        "ens",
        "60",
    )
    .await?
    .expect("row 251 remains readable");
    assert_eq!(refreshed.row.claim_status, PrimaryNameClaimStatus::Success);
    assert_eq!(refreshed.row.raw_claim_name.as_deref(), Some("new.eth"));
    assert_eq!(refreshed.normalized_claim_name.as_deref(), Some("new.eth"));

    rpc.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn successful_reverse_hydration_page_reaches_the_next_rolling_row() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_reverse_hydration_fair_control").await?;
    seed_branch(scratch.pool(), ETHEREUM, 1, 2, None).await?;
    publish(scratch.pool(), ETHEREUM, 1, 2, 1, 1).await?;
    seed_old_reverse_hydration_page(scratch.pool()).await?;
    let rpc = SelectiveFailureHydrationRpc::spawn(0, 251).await?;
    let hydrator = Hydrator::new(
        scratch.pool().clone(),
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM}={}", rpc.endpoint)])?,
    );

    let first = hydrator.hydrate_canonical_head(ETHEREUM).await?;
    assert_eq!(first.reverse_candidates, 250);
    hydrator.hydrate_canonical_head(ETHEREUM).await?;

    let batches = rpc.batches.lock().expect("batch observations").clone();
    assert!(
        batches.iter().any(|batch| batch.contains_last_row),
        "a successful first page must reach row 251 on the next tick"
    );
    let refreshed = load_primary_name_current_snapshot(
        scratch.pool(),
        "0x00000000000000000000000000000000000000fb",
        "ens",
        "60",
    )
    .await?
    .expect("row 251 remains readable");
    assert_eq!(refreshed.normalized_claim_name.as_deref(), Some("new.eth"));

    rpc.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn event_silent_reverse_hydration_does_not_serve_or_starve_an_orphaned_batch() -> Result<()> {
    let scratch =
        ScratchDatabase::create("production_live_reverse_hydration_orphaned_batch").await?;
    seed_branch(scratch.pool(), ETHEREUM, 1, 2, None).await?;
    publish(scratch.pool(), ETHEREUM, 1, 2, 1, 1).await?;
    sqlx::query(
        "INSERT INTO primary_names_current (
             address, coin_type, namespace, claim_status, raw_claim_name,
             claim_name_is_normalized, claim_provenance
         )
         SELECT '0x' || lpad(to_hex(candidate), 40, '0'),
                '60', 'ens', 'success', 'old.eth', true,
                jsonb_build_object(
                    'chain_id', $1::text,
                    'reverse_node', '0x' || lpad(to_hex(candidate), 64, '0'),
                    'resolver_address', $2::text,
                    'target_block_number', 1,
                    'target_block_hash', $3::text,
                    'canonical_head_multicall_hydration', jsonb_build_object(
                        'chain_id', $1::text,
                        'block_number', 2,
                        'block_hash', $4::text,
                        'resolver_address', $2::text,
                        'reverse_node', '0x' || lpad(to_hex(candidate), 64, '0'),
                        'baseline', jsonb_build_object(
                            'claim_status', 'unsupported',
                            'raw_claim_name', NULL,
                            'claim_name_is_normalized', false,
                            'unsupported_reason', 'legacy_resolver_does_not_emit_name'
                        )
                    )
                )
         FROM generate_series(1, 251) candidate",
    )
    .bind(ETHEREUM)
    .bind(REVERSE_RESOLVER)
    .bind(block_hash(1, 1))
    .bind(block_hash(1, 2))
    .execute(scratch.pool())
    .await?;

    seed_branch(scratch.pool(), ETHEREUM, 2, 2, Some((1, block_hash(1, 1)))).await?;
    publish(scratch.pool(), ETHEREUM, 2, 2, 1, 1).await?;
    let rpc = SelectiveFailureHydrationRpc::spawn(0, 251).await?;
    let hydrator = Hydrator::new(
        scratch.pool().clone(),
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM}={}", rpc.endpoint)])?,
    );

    let first = hydrator.hydrate_canonical_head(ETHEREUM).await?;
    assert_eq!(first.reverse_candidates, 250);
    let last_address = "0x00000000000000000000000000000000000000fb";
    let last_read = load_primary_name_current(scratch.pool(), last_address, "ens", "60")
        .await?
        .expect("the event-derived baseline remains readable");
    assert_eq!(last_read.claim_status, PrimaryNameClaimStatus::Unsupported);
    assert_eq!(last_read.raw_claim_name, None);

    let second = hydrator.hydrate_canonical_head(ETHEREUM).await?;
    assert_eq!(second.reverse_candidates, 1);
    let last_hydration_hash: String = sqlx::query_scalar(
        "SELECT claim_provenance -> 'canonical_head_multicall_hydration' ->> 'block_hash'
         FROM primary_names_current
         WHERE address = $1 AND coin_type = '60' AND namespace = 'ens'",
    )
    .bind(last_address)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(last_hydration_hash, block_hash(2, 2));

    rpc.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn missing_hydration_rpc_fails_before_retracting_existing_values() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_missing_hydration_rpc").await?;
    seed_branch(scratch.pool(), ETHEREUM, 1, 1, None).await?;
    publish(scratch.pool(), ETHEREUM, 1, 1, 0, 0).await?;
    seed_reverse_candidate(scratch.pool()).await?;
    let resource = seed_text_candidate(scratch.pool()).await?;
    let rpc =
        HydrationRpc::spawn(BTreeMap::from([(block_hash(1, 1), "alice.eth".to_owned())])).await?;
    Hydrator::new(
        scratch.pool().clone(),
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM}={}", rpc.endpoint)])?,
    )
    .hydrate_canonical_head(ETHEREUM)
    .await?;
    let primary_before: (String, Option<String>, Value) = sqlx::query_as(
        "SELECT claim_status, raw_claim_name, claim_provenance
         FROM primary_names_current
         WHERE address = $1 AND coin_type = '60' AND namespace = 'ens'",
    )
    .bind(ADDRESS)
    .fetch_one(scratch.pool())
    .await?;
    let text_before = text_entry(scratch.pool(), resource).await?;

    let error = ProjectPhase::with_hydration(scratch.pool().clone(), ChainRpcUrls::default())
        .run_batch(PhaseContext {
            chain_id: ETHEREUM.to_owned(),
            phase: PhaseName::Project,
            mode: RunMode::Normal,
            redo_attempt: None,
            sources: Arc::from([]),
            available_heads: Some(HeadMarkers {
                latest: BlockMarker::new(1, block_hash(1, 1))?,
                safe: Some(BlockMarker::new(0, block_hash(1, 0))?),
                finalized: Some(BlockMarker::new(0, block_hash(1, 0))?),
            }),
            live_handoff: None,
            resume: PhaseResume::default(),
        })
        .await
        .expect_err("hydration candidates require a configured RPC URL");
    assert_eq!(error.kind(), phase_runner::error::ErrorKind::Configuration);
    let primary_after: (String, Option<String>, Value) = sqlx::query_as(
        "SELECT claim_status, raw_claim_name, claim_provenance
         FROM primary_names_current
         WHERE address = $1 AND coin_type = '60' AND namespace = 'ens'",
    )
    .bind(ADDRESS)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(primary_after, primary_before);
    assert_eq!(text_entry(scratch.pool(), resource).await?, text_before);

    rpc.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn project_redo_behind_the_canonical_head_defers_hydration() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_redo_hydration_boundary").await?;
    seed_branch(scratch.pool(), ETHEREUM, 1, 2, None).await?;
    publish(scratch.pool(), ETHEREUM, 1, 2, 0, 0).await?;
    seed_completed_spine(scratch.pool(), ETHEREUM, 1, &block_hash(1, 1)).await?;
    seed_reverse_candidate(scratch.pool()).await?;
    let rpc = HydrationRpc::spawn(BTreeMap::from([(
        block_hash(1, 2),
        "future-head.eth".to_owned(),
    )]))
    .await?;
    let phase = ProjectPhase::with_hydration(
        scratch.pool().clone(),
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM}={}", rpc.endpoint)])?,
    );

    phase
        .run_batch(PhaseContext {
            chain_id: ETHEREUM.to_owned(),
            phase: PhaseName::Project,
            mode: RunMode::Redo(BlockRange::new(1, 1)?),
            redo_attempt: None,
            sources: Arc::from([]),
            available_heads: Some(HeadMarkers {
                latest: BlockMarker::new(1, block_hash(1, 1))?,
                safe: None,
                finalized: None,
            }),
            live_handoff: None,
            resume: PhaseResume::default(),
        })
        .await?;

    let row: (String, Option<String>, Option<Value>) = sqlx::query_as(
        "SELECT claim_status, raw_claim_name,
                claim_provenance -> 'canonical_head_multicall_hydration'
         FROM primary_names_current
         WHERE address = $1 AND coin_type = '60' AND namespace = 'ens'",
    )
    .bind(ADDRESS)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(row, ("not_found".to_owned(), None, None));
    rpc.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn failed_reverse_hydration_retracts_the_previous_head_value() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_reverse_hydration_failure").await?;
    seed_branch(scratch.pool(), ETHEREUM, 1, 2, None).await?;
    publish(scratch.pool(), ETHEREUM, 1, 1, 0, 0).await?;
    seed_reverse_candidate(scratch.pool()).await?;
    let rpc = HydrationRpc::spawn(BTreeMap::from([
        (block_hash(1, 1), "alice.eth".to_owned()),
        (block_hash(2, 2), FAILED_MULTICALL.to_owned()),
    ]))
    .await?;
    let hydrator = Hydrator::new(
        scratch.pool().clone(),
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM}={}", rpc.endpoint)])?,
    );

    hydrator.hydrate_canonical_head(ETHEREUM).await?;
    seed_branch(scratch.pool(), ETHEREUM, 2, 2, Some((1, block_hash(1, 1)))).await?;
    publish(scratch.pool(), ETHEREUM, 2, 2, 0, 0).await?;
    let error = hydrator
        .hydrate_canonical_head(ETHEREUM)
        .await
        .expect_err("failed fork hydration must remain retryable");
    assert_eq!(error.kind(), bigname_project::ErrorKind::Transient);

    let row: (String, Option<String>, Option<Value>) = sqlx::query_as(
        "SELECT claim_status, raw_claim_name,
                claim_provenance -> 'canonical_head_multicall_hydration'
         FROM primary_names_current
         WHERE address = $1 AND coin_type = '60' AND namespace = 'ens'",
    )
    .bind(ADDRESS)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(row, ("unsupported".to_owned(), None, None));

    rpc.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn reverse_hydration_rpc_failure_retracts_the_previous_head_value() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_reverse_hydration_rpc_failure").await?;
    seed_branch(scratch.pool(), ETHEREUM, 1, 2, None).await?;
    publish(scratch.pool(), ETHEREUM, 1, 1, 0, 0).await?;
    seed_reverse_candidate(scratch.pool()).await?;
    let rpc = HydrationRpc::spawn(BTreeMap::from([
        (block_hash(1, 1), "alice.eth".to_owned()),
        (block_hash(2, 2), FAILED_MULTICALL_BATCH.to_owned()),
    ]))
    .await?;
    let hydrator = Hydrator::new(
        scratch.pool().clone(),
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM}={}", rpc.endpoint)])?,
    );

    hydrator.hydrate_canonical_head(ETHEREUM).await?;
    seed_branch(scratch.pool(), ETHEREUM, 2, 2, Some((1, block_hash(1, 1)))).await?;
    publish(scratch.pool(), ETHEREUM, 2, 2, 0, 0).await?;
    let error = hydrator
        .hydrate_canonical_head(ETHEREUM)
        .await
        .expect_err("RPC failure must retract the prior fork and remain retryable");
    assert_eq!(error.kind(), bigname_project::ErrorKind::Transient);

    let row: (String, Option<String>, Option<Value>) = sqlx::query_as(
        "SELECT claim_status, raw_claim_name,
                claim_provenance -> 'canonical_head_multicall_hydration'
         FROM primary_names_current
         WHERE address = $1 AND coin_type = '60' AND namespace = 'ens'",
    )
    .bind(ADDRESS)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(row, ("unsupported".to_owned(), None, None));

    rpc.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn reverse_hydration_retracts_when_the_legacy_resolver_becomes_ineligible() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_reverse_hydration_ineligible").await?;
    seed_branch(scratch.pool(), ETHEREUM, 1, 2, None).await?;
    publish(scratch.pool(), ETHEREUM, 1, 1, 0, 0).await?;
    seed_reverse_candidate(scratch.pool()).await?;
    let rpc =
        HydrationRpc::spawn(BTreeMap::from([(block_hash(1, 1), "alice.eth".to_owned())])).await?;
    let hydrator = Hydrator::new(
        scratch.pool().clone(),
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM}={}", rpc.endpoint)])?,
    );

    hydrator.hydrate_canonical_head(ETHEREUM).await?;
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, event_kind, source_family,
             manifest_version, chain_id, block_number, block_hash,
             derivation_kind, canonicality_state, after_state
         ) VALUES (
             'reverse-resolver-ineligible', 'ens', 'ResolverChanged',
             'ens_v1_reverse_l1', 1, $1, 2, $2,
             'ens_v1_unwrapped_authority', 'canonical', $3
         )",
    )
    .bind(ETHEREUM)
    .bind(block_hash(1, 2))
    .bind(json!({
        "node": REVERSE_NODE,
        "resolver": "0x0000000000000000000000000000000000000000"
    }))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE primary_names_current
         SET claim_provenance = claim_provenance || jsonb_build_object(
             'resolver_address', '0x0000000000000000000000000000000000000000',
             'target_block_number', 2,
             'target_block_hash', $1::text
         )
         WHERE address = $2 AND coin_type = '60' AND namespace = 'ens'",
    )
    .bind(block_hash(1, 2))
    .bind(ADDRESS)
    .execute(scratch.pool())
    .await?;
    publish(scratch.pool(), ETHEREUM, 1, 2, 0, 0).await?;
    let outcome = hydrator.hydrate_canonical_head(ETHEREUM).await?;
    assert_eq!(outcome.updated_rows, 1);

    let row: (String, Option<String>, Option<Value>) = sqlx::query_as(
        "SELECT claim_status, raw_claim_name,
                claim_provenance -> 'canonical_head_multicall_hydration'
         FROM primary_names_current
         WHERE address = $1 AND coin_type = '60' AND namespace = 'ens'",
    )
    .bind(ADDRESS)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(row, ("unsupported".to_owned(), None, None));

    rpc.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn shortened_reverse_multicall_retracts_every_candidate_baseline() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_reverse_hydration_cardinality").await?;
    seed_branch(scratch.pool(), ETHEREUM, 1, 2, None).await?;
    publish(scratch.pool(), ETHEREUM, 1, 1, 0, 0).await?;
    seed_reverse_candidate(scratch.pool()).await?;
    seed_reverse_candidate_for(scratch.pool(), ADDRESS_TWO, REVERSE_NODE_TWO, "-two").await?;
    let rpc = HydrationRpc::spawn(BTreeMap::from([
        (
            block_hash(1, 1),
            format!("{MULTICALL_RESULTS_PREFIX}alice.eth|bob.eth"),
        ),
        (block_hash(1, 2), "winning.eth".to_owned()),
    ]))
    .await?;
    let hydrator = Hydrator::new(
        scratch.pool().clone(),
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM}={}", rpc.endpoint)])?,
    );

    hydrator.hydrate_canonical_head(ETHEREUM).await?;
    publish(scratch.pool(), ETHEREUM, 1, 2, 0, 0).await?;
    let error = hydrator
        .hydrate_canonical_head(ETHEREUM)
        .await
        .expect_err("a shortened reverse result must remain retryable");
    assert_eq!(error.kind(), bigname_project::ErrorKind::Transient);

    let rows: Vec<(String, String, Option<String>, Option<Value>)> = sqlx::query_as(
        "SELECT address, claim_status, raw_claim_name,
                claim_provenance -> 'canonical_head_multicall_hydration'
         FROM primary_names_current ORDER BY address",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        rows,
        vec![
            (ADDRESS.to_owned(), "unsupported".to_owned(), None, None),
            (ADDRESS_TWO.to_owned(), "unsupported".to_owned(), None, None,),
        ]
    );

    rpc.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn valueless_legacy_text_hydration_is_head_pinned_and_refreshes() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_text_hydration").await?;
    seed_branch(scratch.pool(), ETHEREUM, 1, 2, None).await?;
    publish(scratch.pool(), ETHEREUM, 1, 1, 0, 0).await?;
    let resource = seed_text_candidate(scratch.pool()).await?;
    let rpc = HydrationRpc::spawn(BTreeMap::from([
        (block_hash(1, 1), "https://one.test".to_owned()),
        (block_hash(1, 2), String::new()),
    ]))
    .await?;
    let hydrator = Hydrator::new(
        scratch.pool().clone(),
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM}={}", rpc.endpoint)])?,
    );

    hydrator.hydrate_canonical_head(ETHEREUM).await?;
    let first = text_entry(scratch.pool(), resource).await?;
    assert_eq!(first["status"], "success");
    assert_eq!(first["value"], "https://one.test");
    assert_eq!(
        first["canonical_head_multicall_hydration"]["block_hash"],
        block_hash(1, 1)
    );

    publish(scratch.pool(), ETHEREUM, 1, 2, 0, 0).await?;
    hydrator.hydrate_canonical_head(ETHEREUM).await?;
    let refreshed = text_entry(scratch.pool(), resource).await?;
    assert_eq!(refreshed["status"], "not_found");
    assert!(refreshed.get("value").is_none());
    assert_eq!(
        refreshed["canonical_head_multicall_hydration"]["block_hash"],
        block_hash(1, 2)
    );

    rpc.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn failed_text_hydration_retracts_the_previous_head_value() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_text_hydration_failure").await?;
    seed_branch(scratch.pool(), ETHEREUM, 1, 2, None).await?;
    publish(scratch.pool(), ETHEREUM, 1, 1, 0, 0).await?;
    let resource = seed_text_candidate(scratch.pool()).await?;
    let rpc = HydrationRpc::spawn(BTreeMap::from([
        (block_hash(1, 1), "https://one.test".to_owned()),
        (block_hash(2, 2), FAILED_MULTICALL.to_owned()),
    ]))
    .await?;
    let hydrator = Hydrator::new(
        scratch.pool().clone(),
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM}={}", rpc.endpoint)])?,
    );

    hydrator.hydrate_canonical_head(ETHEREUM).await?;
    seed_branch(scratch.pool(), ETHEREUM, 2, 2, Some((1, block_hash(1, 1)))).await?;
    publish(scratch.pool(), ETHEREUM, 2, 2, 0, 0).await?;
    let error = hydrator
        .hydrate_canonical_head(ETHEREUM)
        .await
        .expect_err("failed fork hydration must remain retryable");
    assert_eq!(error.kind(), bigname_project::ErrorKind::Transient);

    let entry = text_entry(scratch.pool(), resource).await?;
    assert_eq!(entry["status"], "unsupported");
    assert_eq!(
        entry["unsupported_reason"],
        "value_not_retained_in_normalized_events"
    );
    assert!(entry.get("value").is_none());
    assert!(entry.get("canonical_head_multicall_hydration").is_none());

    rpc.server.abort();
    scratch.cleanup().await
}

#[tokio::test]
async fn text_hydration_rpc_failure_retracts_the_previous_head_value() -> Result<()> {
    let scratch = ScratchDatabase::create("production_live_text_hydration_rpc_failure").await?;
    seed_branch(scratch.pool(), ETHEREUM, 1, 2, None).await?;
    publish(scratch.pool(), ETHEREUM, 1, 1, 0, 0).await?;
    let resource = seed_text_candidate(scratch.pool()).await?;
    let rpc = HydrationRpc::spawn(BTreeMap::from([
        (block_hash(1, 1), "https://one.test".to_owned()),
        (block_hash(2, 2), FAILED_MULTICALL_BATCH.to_owned()),
    ]))
    .await?;
    let hydrator = Hydrator::new(
        scratch.pool().clone(),
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM}={}", rpc.endpoint)])?,
    );

    hydrator.hydrate_canonical_head(ETHEREUM).await?;
    seed_branch(scratch.pool(), ETHEREUM, 2, 2, Some((1, block_hash(1, 1)))).await?;
    publish(scratch.pool(), ETHEREUM, 2, 2, 0, 0).await?;
    let error = hydrator
        .hydrate_canonical_head(ETHEREUM)
        .await
        .expect_err("RPC failure must retract the prior fork and remain retryable");
    assert_eq!(error.kind(), bigname_project::ErrorKind::Transient);

    let entry = text_entry(scratch.pool(), resource).await?;
    assert_eq!(entry["status"], "unsupported");
    assert_eq!(
        entry["unsupported_reason"],
        "value_not_retained_in_normalized_events"
    );
    assert!(entry.get("value").is_none());
    assert!(entry.get("canonical_head_multicall_hydration").is_none());

    rpc.server.abort();
    scratch.cleanup().await
}

fn live_request(
    chain: &str,
    endpoint: &str,
    handoff: i64,
    handoff_hash: String,
) -> LiveBatchRequest {
    LiveBatchRequest {
        chain_id: chain.to_owned(),
        sources: vec![SourceDescriptor {
            key: "rpc".to_owned(),
            kind: "rpc".to_owned(),
            start_block: 0,
            endpoint: endpoint.to_owned(),
        }],
        live_handoff: Marker {
            number: handoff,
            hash: handoff_hash,
        },
    }
}

async fn publish_ingest_heads(
    pool: &PgPool,
    chain: &str,
    heads: Option<bigname_ingest::HeadMarkers>,
) -> Result<()> {
    let heads = heads.ok_or_else(|| anyhow::anyhow!("live batch did not request publication"))?;
    publish_heads(
        pool,
        chain,
        &HeadMarkers {
            latest: BlockMarker::new(heads.latest.number, heads.latest.hash)?,
            safe: heads
                .safe
                .map(|marker| BlockMarker::new(marker.number, marker.hash))
                .transpose()?,
            finalized: heads
                .finalized
                .map(|marker| BlockMarker::new(marker.number, marker.hash))
                .transpose()?,
        },
    )
    .await?;
    Ok(())
}

async fn publish(
    pool: &PgPool,
    chain: &str,
    branch: u64,
    latest: i64,
    safe: i64,
    finalized: i64,
) -> Result<()> {
    publish_heads(
        pool,
        chain,
        &HeadMarkers {
            latest: BlockMarker::new(latest, block_hash(branch, latest))?,
            safe: Some(BlockMarker::new(safe, block_hash(1, safe))?),
            finalized: Some(BlockMarker::new(finalized, block_hash(1, finalized))?),
        },
    )
    .await?;
    Ok(())
}

async fn seed_branch(
    pool: &PgPool,
    chain: &str,
    branch: u64,
    through: i64,
    fork: Option<(i64, String)>,
) -> Result<()> {
    let start = fork.as_ref().map_or(0, |(number, _)| number + 1);
    for number in start..=through {
        let parent = if number == 0 {
            None
        } else if number == start {
            fork.as_ref()
                .map(|(_, hash)| hash.clone())
                .or_else(|| Some(block_hash(branch, number - 1)))
        } else {
            Some(block_hash(branch, number - 1))
        };
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number,
                 block_timestamp, canonicality_state
             ) VALUES ($1, $2, $3, $4, to_timestamp($4 + 1), 'observed')",
        )
        .bind(chain)
        .bind(block_hash(branch, number))
        .bind(parent)
        .bind(number)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn seed_completed_spine(pool: &PgPool, chain: &str, number: i64, hash: &str) -> Result<()> {
    let store = PhaseStore::new(pool.clone());
    store.initialize_chain(chain).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed', current_block_number = $2,
             current_block_hash = $3, target_block_number = $2,
             target_block_hash = $3,
             live_handoff_block_number = CASE WHEN phase_name = 'ingest' THEN $2 END,
             live_handoff_block_hash = CASE WHEN phase_name = 'ingest' THEN $3 END,
             input_content_hash = CASE
                 WHEN phase_name IN ('interpret', 'project') THEN $4 END,
             started_at = now(), finished_at = now(), updated_at = now()
         WHERE chain_id = $1 AND phase_name IN ('ingest', 'interpret', 'project')",
    )
    .bind(chain)
    .bind(number)
    .bind(hash)
    .bind(INTERPRETER_CONTENT_HASH)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO ingest_cursors (
             chain_id, source_key, source_kind, seed_basis, start_block_number,
             next_block_number, target_block_number, last_processed_block_number,
             last_processed_block_hash
         ) VALUES ($1, 'rpc', 'rpc', 'new_signature_range', 0,
                   $2, $3, $3, $4)",
    )
    .bind(chain)
    .bind(number.saturating_add(1))
    .bind(number)
    .bind(hash)
    .execute(pool)
    .await?;
    Ok(())
}

fn fast_timing() -> TimingConfig {
    TimingConfig {
        initial_backoff: Duration::from_millis(1),
        maximum_backoff: Duration::from_millis(4),
        live_poll_interval: Duration::from_millis(1),
    }
}

fn live_chain(chain: &str, endpoint: &str) -> phase_runner::error::RunnerResult<ChainConfig> {
    ChainConfig::new(
        chain,
        vec![SourceConfig::new(
            chain,
            "rpc",
            "rpc",
            SeedBasis::NewSignatureRange,
            0,
            endpoint,
        )?],
        false,
    )
}

fn runner_with_interpret_phase(
    scratch: &ScratchDatabase,
    instance_id: &str,
    interpret: Arc<dyn Phase>,
) -> Result<PhaseRunner> {
    let phases = PhaseSet::with_ingest_interpret_project_and_live(
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        interpret,
        Arc::new(LoopbackPhase::new(PhaseName::Project)),
        Arc::new(LoopbackPhase::new(PhaseName::Verify)),
        Arc::new(LoopbackPhase::new(PhaseName::Live)),
    )?;
    Ok(PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        instance_id,
        fast_timing(),
    )?)
}

fn loopback_runner(scratch: &ScratchDatabase, instance_id: &str) -> Result<PhaseRunner> {
    let phases = PhaseSet::with_ingest_interpret_project_and_live(
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        Arc::new(LoopbackPhase::new(PhaseName::Interpret)),
        Arc::new(LoopbackPhase::new(PhaseName::Project)),
        Arc::new(LoopbackPhase::new(PhaseName::Verify)),
        Arc::new(LoopbackPhase::new(PhaseName::Live)),
    )?;
    Ok(PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        instance_id,
        fast_timing(),
    )?)
}

fn manifest_authority_marker(generation_token: &str) -> String {
    format!("manifest-authority:{TEST_AUTHORITY_FINGERPRINT}:{generation_token}")
}

fn manifest_authority_parts(marker: &str) -> Result<(&str, &str)> {
    let encoded = marker
        .strip_prefix("manifest-authority:")
        .ok_or_else(|| anyhow::anyhow!("marker has no manifest-authority prefix: {marker}"))?;
    encoded
        .rsplit_once(':')
        .ok_or_else(|| anyhow::anyhow!("marker has no invalidation generation: {marker}"))
}

fn production_runner(
    scratch: &ScratchDatabase,
    engine: Arc<Engine>,
    chain: &str,
) -> Result<Arc<PhaseRunner>> {
    let phases = PhaseSet::with_ingest_interpret_project_and_live(
        Arc::new(IngestPhase::with_engine(Arc::clone(&engine))),
        Arc::new(InterpretPhase::new(scratch.pool().clone())),
        Arc::new(ProjectPhase::new(scratch.pool().clone())),
        Arc::new(LoopbackPhase::new(PhaseName::Verify)),
        Arc::new(LivePhase::with_engine(engine)),
    )?;
    Ok(Arc::new(PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        format!("production-{chain}"),
        fast_timing(),
    )?))
}

fn live_context(
    chain: &str,
    endpoint: &str,
    current: i64,
    current_hash: String,
) -> phase_runner::error::RunnerResult<PhaseContext> {
    let current_marker = BlockMarker::new(current, current_hash)?;
    Ok(PhaseContext {
        chain_id: chain.to_owned(),
        phase: PhaseName::Live,
        mode: RunMode::Normal,
        redo_attempt: None,
        sources: Arc::from([SourceConfig::new(
            chain,
            "rpc",
            "rpc",
            SeedBasis::NewSignatureRange,
            0,
            endpoint,
        )?]),
        available_heads: Some(HeadMarkers {
            latest: current_marker.clone(),
            safe: Some(BlockMarker::new(0, block_hash(1, 0))?),
            finalized: Some(BlockMarker::new(0, block_hash(1, 0))?),
        }),
        live_handoff: Some(current_marker.clone()),
        resume: PhaseResume {
            current: Some(current_marker.clone()),
            target: Some(current_marker),
            ..PhaseResume::default()
        },
    })
}

async fn wait_for_rederived_or_runner_stop(
    pool: &PgPool,
    chain: &str,
    number: i64,
    hash: &str,
    task: &mut tokio::task::JoinHandle<phase_runner::error::RunnerResult<()>>,
) -> Result<()> {
    let wait = wait_for_rederived_head(pool, chain, number, hash);
    tokio::pin!(wait);
    tokio::select! {
        result = task => match result {
            Ok(Ok(())) => anyhow::bail!("production live runner stopped before recovery completed"),
            Ok(Err(error)) => Err(error.into()),
            Err(error) => Err(error.into()),
        },
        result = &mut wait => result,
    }
}

async fn recover_stopped_live_after_exit(
    runner: &PhaseRunner,
    pool: &PgPool,
    chain: &str,
    endpoint: &str,
) -> Result<()> {
    let status: String = sqlx::query_scalar(
        "SELECT phase_status
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'live'",
    )
    .bind(chain)
    .fetch_one(pool)
    .await?;
    if status == "running" {
        // Cancellation can leave the durable Live row running when it wins between batches.
        // Exercise normal restart recovery before starting a different writer phase.
        let stopped = CancellationToken::new();
        stopped.cancel();
        runner
            .run_chain(&live_chain(chain, endpoint)?, stopped)
            .await?;
    }
    let recovered: String = sqlx::query_scalar(
        "SELECT phase_status
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'live'",
    )
    .bind(chain)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        recovered, "completed",
        "a different writer phase must start only after stopped-Live recovery"
    );
    Ok(())
}

type RewindSnapshot = (
    Option<(
        i64,
        String,
        Option<i64>,
        Option<String>,
        Option<i64>,
        Option<String>,
    )>,
    Vec<(i64, String, String)>,
    Vec<(
        String,
        String,
        bool,
        Option<i64>,
        Option<i64>,
        Option<String>,
    )>,
);
type RedoStampSnapshot = (String, Option<i64>, Option<i64>, Option<String>);

async fn rewind_snapshot(pool: &PgPool, chain: &str) -> Result<RewindSnapshot> {
    let heads = sqlx::query_as(
        "SELECT latest_block_number, latest_block_hash,
                safe_block_number, safe_block_hash,
                finalized_block_number, finalized_block_hash
         FROM chain_heads WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_optional(pool)
    .await?;
    let lineage = sqlx::query_as(
        "SELECT block_number, block_hash, canonicality_state::text
         FROM chain_lineage WHERE chain_id = $1
         ORDER BY block_number, block_hash",
    )
    .bind(chain)
    .fetch_all(pool)
    .await?;
    let phases = sqlx::query_as(
        "SELECT phase_name, phase_status, redo_in_progress,
                redo_from_block_number, redo_to_block_number, last_error
         FROM chain_phase_state WHERE chain_id = $1
         ORDER BY phase_name",
    )
    .bind(chain)
    .fetch_all(pool)
    .await?;
    Ok((heads, lineage, phases))
}

async fn wait_for_head(pool: &PgPool, chain: &str, number: i64, hash: &str) -> Result<()> {
    tokio::time::timeout(HEAD_ADVANCE_DEADLINE, async {
        loop {
            let current: (i64, String) = sqlx::query_as(
                "SELECT latest_block_number, latest_block_hash
                 FROM chain_heads WHERE chain_id = $1",
            )
            .bind(chain)
            .fetch_one(pool)
            .await?;
            if current == (number, hash.to_owned()) {
                return Result::<()>::Ok(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    Ok(())
}

async fn wait_for_rederived_head(
    pool: &PgPool,
    chain: &str,
    number: i64,
    hash: &str,
) -> Result<()> {
    tokio::time::timeout(HEAD_ADVANCE_DEADLINE, async {
        loop {
            let head: (i64, String) = sqlx::query_as(
                "SELECT latest_block_number, latest_block_hash
                 FROM chain_heads WHERE chain_id = $1",
            )
            .bind(chain)
            .fetch_one(pool)
            .await?;
            let phases: Vec<(String, String, bool, Option<String>)> = sqlx::query_as(
                "SELECT phase_name, phase_status, redo_in_progress, current_block_hash
                 FROM chain_phase_state
                 WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')
                 ORDER BY phase_name",
            )
            .bind(chain)
            .fetch_all(pool)
            .await?;
            if head == (number, hash.to_owned())
                && phases.iter().all(|(_, status, redo, current_hash)| {
                    status == "completed" && !redo && current_hash.as_deref() == Some(hash)
                })
            {
                return Result::<()>::Ok(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    Ok(())
}

async fn seed_empty_watch_manifest(pool: &PgPool, chain: &str) -> Result<()> {
    let address = "0x0000000000000000000000000000000000000001";
    let contract = Uuid::new_v4();
    let payload = json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": "ens_v1_registry_l1",
        "chain": chain,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": "ensip15@ens-normalize-0.1.1",
        "capability_flags": {},
        "roots": [],
        "contracts": [{
            "role": "registry",
            "address": address,
            "proxy_kind": "none",
            "start_block": 0
        }],
        "discovery_rules": [],
        "abi": {"events": [{
            "name": "NewTTL",
            "fragment": "event NewTTL(bytes32 indexed node, uint64 ttl)",
            "emitter_roles": ["registry"],
            "normalized_events": []
        }], "calls": []}
    });
    sqlx::query(
        "INSERT INTO contract_instances (
             contract_instance_id, chain_id, contract_kind
         ) VALUES ($1, $2, 'contract')",
    )
    .bind(contract)
    .bind(chain)
    .execute(pool)
    .await?;
    let manifest: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions (
             manifest_version, namespace, source_family, chain_id,
             deployment_label, rollout_status, normalizer_version,
             file_path, manifest_payload
         ) VALUES (1, 'ens', 'ens_v1_registry_l1', $1, 'fixture', 'active',
                   'ensip15@ens-normalize-0.1.1', $2, $3)
         RETURNING manifest_id",
    )
    .bind(chain)
    .bind(format!("tests/{chain}.toml"))
    .bind(payload)
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "INSERT INTO manifest_contract_instances (
             manifest_id, chain_id, declaration_kind, declaration_name,
             contract_instance_id, declared_address, role, proxy_kind,
             start_block_number
         ) VALUES ($1, $2, 'contract', 'registry', $3, $4, 'registry', 'none', 0)",
    )
    .bind(manifest)
    .bind(chain)
    .bind(contract)
    .bind(address)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO contract_instance_addresses (
             contract_instance_id, chain_id, address, active_from_block_number,
             source_manifest_id
         ) VALUES ($1, $2, $3, 0, $4)",
    )
    .bind(contract)
    .bind(chain)
    .bind(address)
    .bind(manifest)
    .execute(pool)
    .await?;
    Ok(())
}

struct WatchManifestFixture {
    root: PathBuf,
    chain: String,
}

impl WatchManifestFixture {
    fn new(chain: &str) -> Result<Self> {
        let root = std::env::temp_dir().join(format!(
            "bigname-live-manifest-authority-{}",
            Uuid::new_v4()
        ));
        fs::create_dir_all(root.join("test/test_events"))?;
        Ok(Self {
            root,
            chain: chain.to_owned(),
        })
    }

    fn write(&self, include_b: bool) -> Result<()> {
        let roots = if include_b {
            format!(
                r#"
[[roots]]
name = "source_b"
address = "{WATCH_ADDRESS_B}"
start_block = 0
"#,
            )
        } else {
            "roots = []\n".to_owned()
        };
        let manifest = format!(
            r#"
manifest_version = 1
namespace = "test"
source_family = "test_events"
chain = "{}"
deployment_epoch = "fixture"
rollout_status = "active"
normalizer_version = "ensip15@ens-normalize-0.1.1"
discovery_rules = []
{roots}

[capability_flags]

[[contracts]]
role = "source_a"
address = "{WATCH_ADDRESS_A}"
proxy_kind = "none"
start_block = 0
[[abi.events]]
name = "Transfer"
fragment = "event Transfer(address indexed from, address indexed to, uint256 value)"
emitter_roles = ["source_a"]
normalized_events = []
status = "supported"
"#,
            self.chain
        );
        fs::write(self.root.join("test/test_events/v1.toml"), manifest)?;
        Ok(())
    }
}

impl Drop for WatchManifestFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

async fn seed_reverse_candidate(pool: &PgPool) -> Result<()> {
    seed_reverse_candidate_for(pool, ADDRESS, REVERSE_NODE, "").await
}

async fn seed_reverse_candidate_for(
    pool: &PgPool,
    address: &str,
    reverse_node: &str,
    identity_suffix: &str,
) -> Result<()> {
    for (identity, kind, after_state, derivation) in [
        (
            format!("reverse-claim{identity_suffix}"),
            "ReverseChanged",
            json!({
                "address": address,
                "coin_type": "60",
                "namespace": "ens",
                "reverse_node": reverse_node
            }),
            "ens_v1_reverse_claim",
        ),
        (
            format!("reverse-resolver{identity_suffix}"),
            "ResolverChanged",
            json!({"node": reverse_node, "resolver": REVERSE_RESOLVER}),
            "ens_v1_unwrapped_authority",
        ),
    ] {
        sqlx::query(
            "INSERT INTO normalized_events (
                 event_identity, namespace, event_kind, source_family,
                 manifest_version, chain_id, block_number, block_hash,
                 derivation_kind, canonicality_state, after_state
             ) VALUES ($1, 'ens', $2, 'ens_v1_reverse_l1', 1, $3, 1, $4,
                       $5, 'canonical', $6)",
        )
        .bind(identity)
        .bind(kind)
        .bind(ETHEREUM)
        .bind(block_hash(1, 1))
        .bind(derivation)
        .bind(after_state)
        .execute(pool)
        .await?;
    }
    sqlx::query(
        "INSERT INTO primary_names_current (
             address, coin_type, namespace, claim_status,
             unsupported_reason, claim_provenance
         ) VALUES ($1, '60', 'ens', 'unsupported',
                   'legacy_resolver_does_not_emit_name', $2)",
    )
    .bind(address)
    .bind(json!({
        "chain_id": ETHEREUM,
        "reverse_node": reverse_node,
        "resolver_address": REVERSE_RESOLVER,
        "target_block_number": 1,
        "target_block_hash": block_hash(1, 1)
    }))
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_old_reverse_hydration_page(pool: &PgPool) -> Result<()> {
    sqlx::query(
        "INSERT INTO primary_names_current (
             address, coin_type, namespace, claim_status, raw_claim_name,
             claim_name_is_normalized, claim_provenance
         )
         SELECT '0x' || lpad(to_hex(candidate), 40, '0'),
                '60', 'ens', 'success', 'old.eth', true,
                jsonb_build_object(
                    'chain_id', $1::text,
                    'reverse_node', '0x' || lpad(to_hex(candidate), 24, '0') || repeat('cafe', 10),
                    'resolver_address', $2::text,
                    'target_block_number', 1,
                    'target_block_hash', $3::text,
                    'canonical_head_multicall_hydration', jsonb_build_object(
                        'chain_id', $1::text,
                        'block_number', 1,
                        'block_hash', $3::text,
                        'resolver_address', $2::text,
                        'reverse_node',
                            '0x' || lpad(to_hex(candidate), 24, '0') || repeat('cafe', 10),
                        'baseline', jsonb_build_object(
                            'claim_status', 'unsupported',
                            'raw_claim_name', NULL,
                            'claim_name_is_normalized', false,
                            'unsupported_reason', 'legacy_resolver_does_not_emit_name'
                        )
                    )
                )
         FROM generate_series(1, 251) candidate",
    )
    .bind(ETHEREUM)
    .bind(REVERSE_RESOLVER)
    .bind(block_hash(1, 1))
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_cross_head_reverse_hydration_page(pool: &PgPool) -> Result<()> {
    sqlx::query(
        "INSERT INTO primary_names_current (
             address, coin_type, namespace, claim_status, raw_claim_name,
             claim_name_is_normalized, unsupported_reason, claim_provenance,
             reverse_hydration_attempted_block_number,
             reverse_hydration_attempted_block_hash,
             reverse_hydration_attempt_ordinal
         )
         SELECT '0x' || lpad(to_hex(candidate), 40, '0'),
                '60', 'ens',
                CASE WHEN candidate = 251 THEN 'success' ELSE 'unsupported' END,
                CASE WHEN candidate = 251 THEN 'old.eth' ELSE NULL END,
                candidate = 251,
                CASE WHEN candidate = 251
                     THEN NULL
                     ELSE 'legacy_resolver_does_not_emit_name'
                END,
                jsonb_build_object(
                    'chain_id', $1::text,
                    'reverse_node', '0x' || lpad(to_hex(candidate), 24, '0') || repeat('cafe', 10),
                    'resolver_address', $2::text,
                    'target_block_number', 1,
                    'target_block_hash', $3::text
                ) || CASE WHEN candidate = 251 THEN jsonb_build_object(
                    'canonical_head_multicall_hydration', jsonb_build_object(
                        'chain_id', $1::text,
                        'block_number', 1,
                        'block_hash', $3::text,
                        'resolver_address', $2::text,
                        'reverse_node',
                            '0x' || lpad(to_hex(candidate), 24, '0') || repeat('cafe', 10),
                        'baseline', jsonb_build_object(
                            'claim_status', 'unsupported',
                            'raw_claim_name', NULL,
                            'claim_name_is_normalized', false,
                            'unsupported_reason', 'legacy_resolver_does_not_emit_name'
                        )
                    )
                ) ELSE '{}'::jsonb END,
                1,
                $3::text,
                CASE WHEN candidate = 251 THEN 1 ELSE 2 END
         FROM generate_series(1, 251) candidate",
    )
    .bind(ETHEREUM)
    .bind(REVERSE_RESOLVER)
    .bind(block_hash(1, 1))
    .execute(pool)
    .await?;
    sqlx::query("SELECT setval('reverse_hydration_attempt_ordinal_seq', 2, true)")
        .execute(pool)
        .await?;
    Ok(())
}

async fn assert_primary(
    pool: &PgPool,
    status: &str,
    name: Option<&str>,
    head_hash: &str,
) -> Result<()> {
    let row: (String, Option<String>, String) = sqlx::query_as(
        "SELECT claim_status, raw_claim_name,
                claim_provenance -> 'canonical_head_multicall_hydration' ->> 'block_hash'
         FROM primary_names_current
         WHERE address = $1 AND coin_type = '60' AND namespace = 'ens'",
    )
    .bind(ADDRESS)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        row,
        (
            status.to_owned(),
            name.map(str::to_owned),
            head_hash.to_owned()
        )
    );
    Ok(())
}

async fn seed_text_candidate(pool: &PgPool) -> Result<Uuid> {
    let resource = Uuid::new_v4();
    let logical_name_id = format!("ens:{NAMEHASH}");
    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 1, 'canonical')",
    )
    .bind(resource)
    .bind(ETHEREUM)
    .bind(block_hash(1, 1))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version,
             visibility_state, chain_id, block_hash, block_number,
             canonicality_state
         ) VALUES ($1, 'ens', 'alice.eth', ARRAY['alice','eth'], '\\x'::bytea,
                   $2, ARRAY['0xalice','0xeth'], 'fixture', 'active',
                   $3, $4, 1, 'canonical')",
    )
    .bind(&logical_name_id)
    .bind(NAMEHASH)
    .bind(ETHEREUM)
    .bind(block_hash(1, 1))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO record_inventory_current (
             resource_id, record_version_boundary_key, entries,
             support_status, provenance, manifest_version
         ) VALUES ($1, 'resolver:fixture', $2, 'supported', $3, 1)",
    )
    .bind(resource)
    .bind(json!([{
        "record_key": "text:url",
        "record_family": "text",
        "selector_key": "url",
        "status": "unsupported",
        "unsupported_reason": "value_not_retained_in_normalized_events"
    }]))
    .bind(json!({
        "chain_id": ETHEREUM,
        "logical_name_id": logical_name_id,
        "resolver_address": REVERSE_RESOLVER
    }))
    .execute(pool)
    .await?;
    Ok(resource)
}

async fn text_entry(pool: &PgPool, resource: Uuid) -> Result<Value> {
    let entries: Value =
        sqlx::query_scalar("SELECT entries FROM record_inventory_current WHERE resource_id = $1")
            .bind(resource)
            .fetch_one(pool)
            .await?;
    Ok(entries
        .as_array()
        .and_then(|entries| entries.first())
        .cloned()
        .expect("fixture text entry"))
}

fn block_hash(branch: u64, number: i64) -> String {
    format!("0x{:064x}", branch * 1_000_000 + number as u64 + 1)
}

#[derive(Clone)]
struct RpcChain {
    canonical: Vec<String>,
    blocks: BTreeMap<String, Value>,
    logs: Vec<Value>,
    reorg_after_number_batch: Option<(u64, i64, i64)>,
}

struct RpcFixture {
    endpoint: String,
    state: Arc<RwLock<RpcChain>>,
    server: tokio::task::JoinHandle<()>,
}

impl RpcFixture {
    async fn spawn(branch: u64, through: i64) -> Result<Self> {
        let state = Arc::new(RwLock::new(rpc_chain(branch, through)));
        let server_state = Arc::clone(&state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/", post(chain_rpc))
                    .with_state(server_state),
            )
            .await
            .expect("live fixture RPC server");
        });
        Ok(Self {
            endpoint: format!("http://{address}/"),
            state,
            server,
        })
    }

    async fn spawn_with_b_fact(branch: u64, through: i64, fact_block: i64) -> Result<Self> {
        let fixture = Self::spawn(branch, through).await?;
        fixture.state.write().await.logs.push(json!({
            "blockHash": block_hash(branch, fact_block),
            "blockNumber": format!("0x{fact_block:x}"),
            "transactionHash": format!("0x{:064x}", 9_000_000 + fact_block),
            "transactionIndex": "0x0",
            "logIndex": "0x0",
            "address": WATCH_ADDRESS_B,
            "topics": [
                TRANSFER_TOPIC,
                format!("0x{}", "00".repeat(32)),
                format!("0x{}", "00".repeat(32))
            ],
            "data": format!("0x{}", "00".repeat(32))
        }));
        Ok(fixture)
    }

    async fn reorg(&self, branch: u64, ancestor: i64, through: i64) {
        let mut state = self.state.write().await;
        apply_fixture_reorg(&mut state, branch, ancestor, through);
    }

    async fn reorg_after_next_number_batch(&self, branch: u64, ancestor: i64, through: i64) {
        self.state.write().await.reorg_after_number_batch = Some((branch, ancestor, through));
    }
}

fn rpc_chain(branch: u64, through: i64) -> RpcChain {
    let mut canonical = Vec::new();
    let mut blocks = BTreeMap::new();
    for number in 0..=through {
        let hash = block_hash(branch, number);
        let parent = if number == 0 {
            format!("0x{:064x}", 0)
        } else {
            block_hash(branch, number - 1)
        };
        blocks.insert(
            hash.clone(),
            json!({
                "hash": hash,
                "parentHash": parent,
                "number": format!("0x{number:x}"),
                "timestamp": format!("0x{:x}", number + 1),
                "logsBloom": "0x",
                "transactions": []
            }),
        );
        canonical.push(hash);
    }
    RpcChain {
        canonical,
        blocks,
        logs: Vec::new(),
        reorg_after_number_batch: None,
    }
}

async fn chain_rpc(
    State(state): State<Arc<RwLock<RpcChain>>>,
    Json(request): Json<Value>,
) -> Json<Value> {
    let mut state = state.write().await;
    let response = if let Some(requests) = request.as_array() {
        Value::Array(
            requests
                .iter()
                .map(|request| chain_rpc_response(&state, request))
                .collect(),
        )
    } else {
        chain_rpc_response(&state, &request)
    };
    let is_number_batch = request.as_array().is_some_and(|requests| {
        requests.iter().all(|request| {
            request["method"] == "eth_getBlockByNumber"
                && request
                    .pointer("/params/0")
                    .and_then(Value::as_str)
                    .is_some_and(|selector| selector.starts_with("0x"))
        })
    });
    if is_number_batch
        && let Some((branch, ancestor, through)) = state.reorg_after_number_batch.take()
    {
        apply_fixture_reorg(&mut state, branch, ancestor, through);
    }
    Json(response)
}

fn apply_fixture_reorg(state: &mut RpcChain, branch: u64, ancestor: i64, through: i64) {
    state.canonical.truncate((ancestor + 1) as usize);
    for number in ancestor + 1..=through {
        let hash = block_hash(branch, number);
        let parent = state
            .canonical
            .last()
            .cloned()
            .expect("fixture reorg retains its ancestor");
        state.blocks.insert(
            hash.clone(),
            json!({
                "hash": hash,
                "parentHash": parent,
                "number": format!("0x{number:x}"),
                "timestamp": format!("0x{:x}", number + 1),
                "logsBloom": "0x",
                "transactions": []
            }),
        );
        state.canonical.push(hash);
    }
}

fn chain_rpc_response(state: &RpcChain, request: &Value) -> Value {
    let id = request.get("id").cloned().unwrap_or(json!(1));
    let params = request["params"].as_array().cloned().unwrap_or_default();
    let result = match request["method"].as_str().unwrap_or_default() {
        "eth_getBlockByNumber" => {
            let selector = params.first().and_then(Value::as_str).unwrap_or_default();
            let index = match selector {
                "latest" => state.canonical.len().checked_sub(1),
                "safe" | "finalized" => Some(0),
                quantity => usize::from_str_radix(quantity.trim_start_matches("0x"), 16).ok(),
            };
            index
                .and_then(|index| state.canonical.get(index))
                .and_then(|hash| state.blocks.get(hash))
                .cloned()
        }
        "eth_getBlockByHash" => params
            .first()
            .and_then(Value::as_str)
            .and_then(|hash| state.blocks.get(hash))
            .cloned(),
        "eth_getLogs" => Some(Value::Array(rpc_logs(&state.logs, params.first()))),
        _ => None,
    };
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn rpc_logs(logs: &[Value], filter: Option<&Value>) -> Vec<Value> {
    let filter = filter.cloned().unwrap_or_default();
    if let Some(block_hash) = filter.get("blockHash").and_then(Value::as_str) {
        return logs
            .iter()
            .filter(|log| log.get("blockHash").and_then(Value::as_str) == Some(block_hash))
            .cloned()
            .collect();
    }
    let from = rpc_quantity(filter.get("fromBlock")).unwrap_or_default();
    let to = rpc_quantity(filter.get("toBlock")).unwrap_or(i64::MAX);
    let addresses = rpc_filter_values(filter.get("address"));
    let topics = rpc_filter_values(filter.pointer("/topics/0"));
    logs.iter()
        .filter(|log| {
            let number = rpc_quantity(log.get("blockNumber")).unwrap_or_default();
            let address = log
                .get("address")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let topic = log
                .pointer("/topics/0")
                .and_then(Value::as_str)
                .unwrap_or_default();
            (from..=to).contains(&number)
                && (addresses.is_empty()
                    || addresses
                        .iter()
                        .any(|expected| expected.eq_ignore_ascii_case(address)))
                && (topics.is_empty()
                    || topics
                        .iter()
                        .any(|expected| expected.eq_ignore_ascii_case(topic)))
        })
        .cloned()
        .collect()
}

fn rpc_filter_values(value: Option<&Value>) -> Vec<String> {
    value.map_or_else(Vec::new, |value| {
        value.as_array().map_or_else(
            || value.as_str().map(str::to_owned).into_iter().collect(),
            |values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            },
        )
    })
}

fn rpc_quantity(value: Option<&Value>) -> Option<i64> {
    i64::from_str_radix(value?.as_str()?.trim_start_matches("0x"), 16).ok()
}

struct HydrationRpc {
    endpoint: String,
    server: tokio::task::JoinHandle<()>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ObservedHydrationBatch {
    poisoned: bool,
    call_count: usize,
    contains_last_row: bool,
}

#[derive(Debug, sqlx::FromRow)]
struct ObservedReverseHydrationAttempt {
    address: String,
    attempted_block_number: Option<i64>,
    attempted_block_hash: Option<String>,
    attempt_ordinal: Option<i64>,
    has_serving_marker: bool,
}

#[derive(Clone)]
struct SelectiveFailureHydrationRpcState {
    poison_node_hex: String,
    last_node_hex: String,
    batches: Arc<Mutex<Vec<ObservedHydrationBatch>>>,
}

struct SelectiveFailureHydrationRpc {
    endpoint: String,
    server: tokio::task::JoinHandle<()>,
    batches: Arc<Mutex<Vec<ObservedHydrationBatch>>>,
}

impl SelectiveFailureHydrationRpc {
    async fn spawn(poison_candidate: i64, last_candidate: i64) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let batches = Arc::new(Mutex::new(Vec::new()));
        let state = SelectiveFailureHydrationRpcState {
            poison_node_hex: reverse_hydration_node_hex(poison_candidate),
            last_node_hex: reverse_hydration_node_hex(last_candidate),
            batches: Arc::clone(&batches),
        };
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/", post(selective_failure_hydration_rpc))
                    .with_state(state),
            )
            .await
            .expect("selective-failure hydration fixture RPC server");
        });
        Ok(Self {
            endpoint: format!("http://{address}/"),
            server,
            batches,
        })
    }
}

fn reverse_hydration_node_hex(candidate: i64) -> String {
    format!("{candidate:024x}{}", "cafe".repeat(10))
}

async fn selective_failure_hydration_rpc(
    State(state): State<SelectiveFailureHydrationRpcState>,
    Json(request): Json<Value>,
) -> Json<Value> {
    let id = request.get("id").cloned().unwrap_or(json!(1));
    let data = request
        .pointer("/params/0/data")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let call_count = data.matches("691f3431").count();
    let poisoned = data.contains(&state.poison_node_hex);
    let contains_last_row = data.contains(&state.last_node_hex);
    state
        .batches
        .lock()
        .expect("batch observations")
        .push(ObservedHydrationBatch {
            poisoned,
            call_count,
            contains_last_row,
        });
    if poisoned {
        return Json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {"code": -32000, "message": "fixture poisoned hydration page"}
        }));
    }
    let names = std::iter::repeat_n("new.eth", call_count).collect::<Vec<_>>();
    Json(json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": multicall_string_results(names.iter().copied())
    }))
}

impl HydrationRpc {
    async fn spawn(values: BTreeMap<String, String>) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let values = Arc::new(values);
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/", post(hydration_rpc))
                    .with_state(values),
            )
            .await
            .expect("hydration fixture RPC server");
        });
        Ok(Self {
            endpoint: format!("http://{address}/"),
            server,
        })
    }
}

async fn hydration_rpc(
    State(values): State<Arc<BTreeMap<String, String>>>,
    Json(request): Json<Value>,
) -> Json<Value> {
    let id = request.get("id").cloned().unwrap_or(json!(1));
    let block_hash = request
        .pointer("/params/1/blockHash")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let value = values.get(block_hash).cloned().unwrap_or_default();
    if value == FAILED_MULTICALL_BATCH {
        return Json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {"code": -32000, "message": "fixture multicall failure"}
        }));
    }
    let result = if let Some(values) = value.strip_prefix(MULTICALL_RESULTS_PREFIX) {
        multicall_string_results(values.split('|'))
    } else if value == FAILED_MULTICALL {
        multicall_failed_result()
    } else {
        multicall_string_result(&value)
    };
    Json(json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result
    }))
}

fn multicall_string_result(value: &str) -> String {
    multicall_string_results([value])
}

fn multicall_string_results<'a>(values: impl IntoIterator<Item = &'a str>) -> String {
    let results = values
        .into_iter()
        .map(|value| {
            let inner = (value.to_owned(),).abi_encode_params();
            (true, Bytes::from(inner))
        })
        .collect::<Vec<_>>();
    let outer = (results,).abi_encode_params();
    format!("0x{}", alloy_primitives::hex::encode(outer))
}

fn multicall_failed_result() -> String {
    let outer = (vec![(false, Bytes::new())],).abi_encode_params();
    format!("0x{}", alloy_primitives::hex::encode(outer))
}
