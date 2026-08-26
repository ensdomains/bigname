#[allow(dead_code)]
mod support;

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::Result;
use phase_runner::{
    capacity::{CapacityFuture, CapacityGuard, CapacityMeasurement, CapacityProbe},
    cli::resolve_all_redo_chains,
    config::{CapacityConfig, ChainConfig, RuntimeConfig, SeedBasis, SourceConfig, TimingConfig},
    error::{ErrorKind, RunnerError, RunnerResult},
    heads::{BlockMarker, HeadMarkers, publish_heads},
    metrics::RunnerLoopHeartbeat,
    phase::{
        BlockRange, LoopbackPhase, Phase, PhaseBatchOutcome, PhaseContext, PhaseFuture, PhaseName,
        PhaseProgress, PhaseSet, RedoAttemptFence, RunMode, SourceProgress, VerificationLevel,
    },
    phase_lock::PhaseLock,
    runner::{PhaseRunner, RedoPhase},
    state::{PhaseStatus, PhaseStore, StartDisposition},
};
use serde_json::json;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use support::{ScratchDatabase, assert_connection_hash_stamp, seed_lineage};

#[tokio::test]
async fn phase_transitions_are_legal_and_persisted() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_transitions").await?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    let chain = "transition-chain";
    store.initialize_chain(chain).await?;

    let error = store
        .start_phase(chain, PhaseName::Interpret, &RunMode::Normal)
        .await
        .expect_err("interpret must wait for ingest");
    assert_eq!(error.kind(), ErrorKind::InvalidTransition);
    let error = complete_phase_with_lock(
        &scratch,
        &store,
        chain,
        PhaseName::Verify,
        &PhaseProgress::default(),
    )
    .await
    .expect_err("an idle phase cannot complete");
    assert_eq!(error.kind(), ErrorKind::InvalidTransition);

    let marker = BlockMarker::new(7, "transition-block-7")?;
    let ingest_progress = PhaseProgress {
        current: Some(marker.clone()),
        target: Some(marker.clone()),
        live_handoff: Some(marker),
        ..PhaseProgress::default()
    };
    assert_eq!(
        store
            .start_phase(chain, PhaseName::Ingest, &RunMode::Normal)
            .await?,
        StartDisposition::Started
    );
    store.pause_phase(chain, PhaseName::Ingest).await?;
    assert_eq!(
        store.status(chain, PhaseName::Ingest).await?,
        PhaseStatus::Paused
    );
    store.resume_phase(chain, PhaseName::Ingest).await?;
    complete_phase_with_lock(&scratch, &store, chain, PhaseName::Ingest, &ingest_progress).await?;

    for phase in [PhaseName::Interpret, PhaseName::Project] {
        store.start_phase(chain, phase, &RunMode::Normal).await?;
        complete_phase_with_lock(&scratch, &store, chain, phase, &PhaseProgress::default()).await?;
    }
    store
        .start_phase(chain, PhaseName::Verify, &RunMode::Normal)
        .await?;
    store
        .start_phase(chain, PhaseName::Live, &RunMode::Normal)
        .await?;
    assert_eq!(
        store.status(chain, PhaseName::Verify).await?,
        PhaseStatus::Running
    );
    assert_eq!(
        store.status(chain, PhaseName::Live).await?,
        PhaseStatus::Running
    );
    complete_phase_with_lock(
        &scratch,
        &store,
        chain,
        PhaseName::Verify,
        &PhaseProgress {
            verification_level: Some(VerificationLevel::QuickSynced),
            ..PhaseProgress::default()
        },
    )
    .await?;
    complete_phase_with_lock(
        &scratch,
        &store,
        chain,
        PhaseName::Live,
        &PhaseProgress::default(),
    )
    .await?;

    let error = store
        .fail_phase(chain, PhaseName::Project, "late generic failure")
        .await
        .expect_err("the general failure path cannot demote a completed Project");
    assert_eq!(error.kind(), ErrorKind::InvalidTransition);

    assert_eq!(
        store
            .start_phase(chain, PhaseName::Project, &RunMode::Normal)
            .await?,
        StartDisposition::AlreadyCompleted
    );
    let error = store
        .start_phase(
            chain,
            PhaseName::Project,
            &RunMode::Redo(BlockRange::new(1, 2)?),
        )
        .await
        .expect_err("redo transitions must use the runner so prior state can be restored");
    assert_eq!(error.kind(), ErrorKind::Configuration);
    scratch.cleanup().await
}

#[tokio::test]
async fn completed_project_cannot_enter_ingest_verify_retained_recovery() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_recovery_marker_collision").await?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    let chain_id = "recovery-marker-collision";
    store.initialize_chain(chain_id).await?;
    mark_completed(scratch.pool(), chain_id, PhaseName::Ingest, None).await?;
    mark_completed(
        scratch.pool(),
        chain_id,
        PhaseName::Interpret,
        Some(phase_runner::INTERPRETER_CONTENT_HASH),
    )
    .await?;
    mark_completed(
        scratch.pool(),
        chain_id,
        PhaseName::Project,
        Some(phase_runner::INTERPRETER_CONTENT_HASH),
    )
    .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'failed',
             last_error = 'completed phase validation failed: ordinary project failure',
             current_block_number = 42, current_block_hash = 'project-block-42',
             target_block_number = 42, target_block_hash = 'project-block-42',
             started_at = now(), finished_at = now(), updated_at = now()
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(chain_id)
    .execute(scratch.pool())
    .await?;

    assert_eq!(
        store
            .start_phase(chain_id, PhaseName::Project, &RunMode::Normal)
            .await?,
        StartDisposition::Started,
        "a completed-looking Project must not enter Ingest/Verify retained-completion recovery"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn failure_prefix_without_structural_evidence_cannot_authorize_ingest_recovery() -> Result<()>
{
    let scratch = ScratchDatabase::create("phase_runner_recovery_prefix_without_evidence").await?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    let chain_id = "recovery-prefix-without-evidence";
    store.initialize_chain(chain_id).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'failed',
             last_error = 'completed phase validation failed: text without retained evidence',
             current_block_number = NULL, current_block_hash = NULL,
             target_block_number = NULL, target_block_hash = NULL,
             live_handoff_block_number = NULL, live_handoff_block_hash = NULL,
             started_at = now(), finished_at = now(), updated_at = now()
         WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(chain_id)
    .execute(scratch.pool())
    .await?;

    assert_eq!(
        store
            .start_phase(chain_id, PhaseName::Ingest, &RunMode::Normal)
            .await?,
        StartDisposition::Started,
        "the completed-validation prefix cannot replace retained Ingest evidence"
    );
    assert_eq!(
        store.status(chain_id, PhaseName::Ingest).await?,
        PhaseStatus::Running,
        "the failed Ingest row must take the ordinary restart path"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn second_runner_fails_loudly_when_phase_lock_is_held() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_lock").await?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain("lock-chain").await?;
    mark_completed(scratch.pool(), "lock-chain", PhaseName::Ingest, None).await?;
    mark_completed(
        scratch.pool(),
        "lock-chain",
        PhaseName::Interpret,
        Some(phase_runner::INTERPRETER_CONTENT_HASH),
    )
    .await?;
    set_phase_extent(scratch.pool(), "lock-chain", PhaseName::Interpret, 0).await?;
    seed_interpret_redo_presence(scratch.pool(), "lock-chain", 0).await?;
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let blocking = Arc::new(BlockingPhase {
        name: PhaseName::Interpret,
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
    });
    let first = runner(
        scratch.runner(),
        phase_set_replacing(PhaseName::Interpret, blocking)?,
        available_capacity(),
        "lock-holder",
    )?;
    let second = runner(
        scratch.runner(),
        PhaseSet::loopback(),
        available_capacity(),
        "lock-contender",
    )?;
    let chain = chain("lock-chain")?;
    let first_chain = chain.clone();
    let first_task = tokio::spawn(async move {
        first
            .redo(
                &first_chain,
                RedoPhase::Phase(PhaseName::Interpret),
                BlockRange::new(0, 0).expect("fixed range"),
                CancellationToken::new(),
            )
            .await
    });
    entered.notified().await;

    let error = second
        .redo(
            &chain,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(0, 0)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("a second runner must not share one phase lock");
    assert_eq!(error.kind(), ErrorKind::LockHeld);
    assert!(error.to_string().contains("refusing a second runner"));

    release.notify_one();
    first_task.await??;
    scratch.cleanup().await
}

#[tokio::test]
async fn derived_write_refuses_a_recorded_content_hash_mismatch() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_hash").await?;
    assert_connection_hash_stamp(&scratch.runner()).await?;
    let chain_id = "hash-chain";
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain_id).await?;
    seed_readable_lineage(scratch.pool(), chain_id, 1).await?;
    sqlx::query(
        "
        UPDATE chain_phase_state
        SET phase_status = 'completed',
            input_content_hash = 'keccak256:older-binary',
            started_at = now(),
            finished_at = now(),
            updated_at = now()
        WHERE chain_id = $1
          AND phase_name = 'interpret'
        ",
    )
    .bind(chain_id)
    .execute(scratch.pool())
    .await?;
    let runner = runner(
        scratch.runner(),
        PhaseSet::loopback(),
        available_capacity(),
        "hash-runner",
    )?;
    let error = runner
        .redo(
            &chain(chain_id)?,
            RedoPhase::Phase(PhaseName::Project),
            BlockRange::new(0, 1)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("a mismatched binary must not start derived writes");
    assert_eq!(error.kind(), ErrorKind::ContentHashMismatch);
    assert!(error.to_string().contains("refusing derived writes"));
    assert!(error.to_string().contains("keccak256:older-binary"));
    assert_eq!(
        store.status(chain_id, PhaseName::Project).await?,
        PhaseStatus::Idle
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn runner_writes_transitions_cursors_heads_and_heartbeats() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_writes").await?;
    let chain_id = "write-chain";
    seed_identified_lineage(scratch.pool(), chain_id, 3).await?;
    let heads = HeadMarkers {
        latest: BlockMarker::new(3, format!("{chain_id}-block-3"))?,
        safe: Some(BlockMarker::new(2, format!("{chain_id}-block-2"))?),
        finalized: Some(BlockMarker::new(1, format!("{chain_id}-block-1"))?),
    };
    let phases = complete_phase_set(Some(heads));
    let loop_heartbeat = RunnerLoopHeartbeat::default();
    loop_heartbeat.record_progress(chain_id);
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert!(
        loop_heartbeat
            .age_seconds(chain_id)
            .is_some_and(|age| age >= 1)
    );
    let runner = runner(
        scratch.runner(),
        phases,
        available_capacity(),
        "write-runner",
    )?
    .with_loop_heartbeat(loop_heartbeat.clone());
    let mut chain = chain(chain_id)?;
    chain.verify_before_live = true;
    runner.run_chain(&chain, CancellationToken::new()).await?;
    assert_eq!(loop_heartbeat.age_seconds(chain_id), Some(0));

    let stored_heads: (i64, String, Option<i64>, Option<i64>) = sqlx::query_as(
        "
        SELECT latest_block_number, latest_block_hash,
               safe_block_number, finalized_block_number
        FROM chain_heads
        WHERE chain_id = $1
        ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        stored_heads,
        (3, format!("{chain_id}-block-3"), Some(2), Some(1))
    );
    let states: Vec<(i64, String)> = sqlx::query_as(
        "
        SELECT block_number, canonicality_state::text
        FROM chain_lineage
        WHERE chain_id = $1
        ORDER BY block_number
        ",
    )
    .bind(chain_id)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        states,
        vec![
            (0, "finalized".to_owned()),
            (1, "finalized".to_owned()),
            (2, "safe".to_owned()),
            (3, "canonical".to_owned()),
        ]
    );
    let cursor: (i64, Option<i64>, Option<String>) = sqlx::query_as(
        "
        SELECT next_block_number, last_processed_block_number,
               last_processed_block_hash
        FROM ingest_cursors
        WHERE chain_id = $1
          AND source_key = 'source'
        ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(cursor, (4, Some(3), Some(format!("{chain_id}-block-3"))));
    let heartbeats: i64 = sqlx::query_scalar(
        "
        SELECT count(*)
        FROM service_heartbeats
        WHERE service_name = 'phase-runner'
          AND instance_id = 'write-runner'
          AND chain_id = $1
        ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(heartbeats, 5);
    let stamped_rows: i64 = sqlx::query_scalar(
        "
        SELECT count(*)
        FROM chain_phase_state
        WHERE chain_id = $1
          AND phase_status = 'completed'
          AND input_content_hash = $2
        ",
    )
    .bind(chain_id)
    .bind(phase_runner::INTERPRETER_CONTENT_HASH)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(stamped_rows, 5);
    scratch.cleanup().await
}

#[tokio::test]
async fn runner_loop_heartbeat_refreshes_across_completed_live_follow_passes() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_live_follow_heartbeat").await?;
    let chain_id = "live-follow-heartbeat-chain";
    seed_identified_lineage(scratch.pool(), chain_id, 1).await?;
    let heads = HeadMarkers {
        latest: BlockMarker::new(1, format!("{chain_id}-block-1"))?,
        safe: None,
        finalized: None,
    };
    let live_calls = Arc::new(AtomicUsize::new(0));
    let phases = continuous_complete_phase_set(heads, Arc::clone(&live_calls))?;
    let loop_heartbeat = RunnerLoopHeartbeat::default();
    loop_heartbeat.record_progress(chain_id);
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert!(
        loop_heartbeat
            .age_seconds(chain_id)
            .is_some_and(|age| age >= 1)
    );

    let mut timing = test_timing();
    timing.live_poll_interval = Duration::from_millis(600);
    let runner = PhaseRunner::new(
        scratch.runner(),
        phases,
        available_capacity(),
        "live-follow-heartbeat-runner",
        timing,
    )?
    .with_loop_heartbeat(loop_heartbeat.clone());
    let mut chain = chain(chain_id)?;
    chain.verify_before_live = true;
    let cancellation = CancellationToken::new();
    let run_cancellation = cancellation.clone();
    let task = tokio::spawn(async move { runner.run_chain(&chain, run_cancellation).await });

    tokio::time::timeout(Duration::from_secs(5), async {
        while live_calls.load(Ordering::SeqCst) < 3 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert_eq!(loop_heartbeat.age_seconds(chain_id), Some(0));

    cancellation.cancel();
    task.await??;
    scratch.cleanup().await
}

#[tokio::test]
async fn capacity_breach_pauses_and_then_resumes_the_phase() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_capacity").await?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain("capacity-chain").await?;
    seed_readable_lineage(scratch.pool(), "capacity-chain", 0).await?;
    mark_completed(
        scratch.pool(),
        "capacity-chain",
        PhaseName::Project,
        Some(phase_runner::INTERPRETER_CONTENT_HASH),
    )
    .await?;
    sqlx::query(
        "
        UPDATE chain_phase_state
        SET current_block_number = 0,
            current_block_hash = 'capacity-chain-block-0',
            target_block_number = 0,
            target_block_hash = 'capacity-chain-block-0'
        WHERE chain_id = 'capacity-chain'
          AND phase_name = 'verify'
        ",
    )
    .execute(scratch.pool())
    .await?;
    let probe = Arc::new(GatedCapacityProbe::default());
    let loop_heartbeat = RunnerLoopHeartbeat::default();
    let capacity = CapacityGuard::new(
        CapacityConfig {
            database_max_bytes: None,
            minimum_free_disk_bytes: 1,
            writable_path: ".".into(),
            poll_interval: Duration::from_millis(1),
            interpreter_state_cache_entries:
                bigname_interpret::DEFAULT_INTERPRETER_STATE_CACHE_ENTRIES,
        },
        probe.clone(),
    );
    let runner = runner(
        scratch.runner(),
        PhaseSet::loopback(),
        capacity,
        "capacity-runner",
    )?
    .with_loop_heartbeat(loop_heartbeat.clone());
    let chain = chain("capacity-chain")?;
    let task = tokio::spawn(async move {
        runner
            .redo(
                &chain,
                RedoPhase::Phase(PhaseName::Verify),
                BlockRange::new(0, 0).expect("fixed range"),
                CancellationToken::new(),
            )
            .await
    });
    probe.breached.notified().await;

    wait_for_phase_status(
        scratch.pool(),
        "capacity-chain",
        PhaseName::Verify,
        "paused",
    )
    .await?;
    let initial_heartbeat: f64 = sqlx::query_scalar(
        "
        SELECT extract(epoch FROM heartbeat_at)::double precision
        FROM service_heartbeats
        WHERE chain_id = 'capacity-chain'
          AND phase_name = 'verify'
        ",
    )
    .fetch_one(scratch.pool())
    .await?;
    tokio::time::sleep(Duration::from_millis(5_200)).await;
    let refreshed_heartbeat: f64 = sqlx::query_scalar(
        "
        SELECT extract(epoch FROM heartbeat_at)::double precision
        FROM service_heartbeats
        WHERE chain_id = 'capacity-chain'
          AND phase_name = 'verify'
        ",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert!(refreshed_heartbeat > initial_heartbeat);
    assert!(
        loop_heartbeat
            .age_seconds("capacity-chain")
            .is_some_and(|age| age <= 1)
    );
    assert_eq!(
        store.status("capacity-chain", PhaseName::Verify).await?,
        PhaseStatus::Paused
    );

    probe.released.store(true, Ordering::SeqCst);
    task.await??;
    let status: String = sqlx::query_scalar(
        "
        SELECT phase_status
        FROM chain_phase_state
        WHERE chain_id = 'capacity-chain'
          AND phase_name = 'verify'
        ",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(status, "idle");
    scratch.cleanup().await
}

#[tokio::test]
async fn transient_phase_error_restarts_with_backoff() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_restart").await?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain("restart-chain").await?;
    seed_readable_lineage(scratch.pool(), "restart-chain", 0).await?;
    mark_completed(
        scratch.pool(),
        "restart-chain",
        PhaseName::Project,
        Some(phase_runner::INTERPRETER_CONTENT_HASH),
    )
    .await?;
    mark_completed(scratch.pool(), "restart-chain", PhaseName::Verify, None).await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let flaky = Arc::new(FunctionPhase {
        name: PhaseName::Verify,
        handler: {
            let calls = Arc::clone(&calls);
            Arc::new(move |_| {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    Err(RunnerError::transient("temporary provider outage"))
                } else {
                    Ok(PhaseBatchOutcome::Complete(PhaseProgress {
                        verification_level: Some(VerificationLevel::QuickSynced),
                        ..PhaseProgress::default()
                    }))
                }
            })
        },
    });
    let runner = runner(
        scratch.runner(),
        phase_set_replacing(PhaseName::Verify, flaky)?,
        available_capacity(),
        "restart-runner",
    )?;
    runner
        .redo(
            &chain("restart-chain")?,
            RedoPhase::Phase(PhaseName::Verify),
            BlockRange::new(0, 0)?,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    scratch.cleanup().await
}

#[tokio::test]
async fn fatal_error_stops_only_its_chain_supervisor() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_isolation").await?;
    seed_identified_lineage(scratch.pool(), "good-chain", 0).await?;
    let good_live = Arc::new(Notify::new());
    let phases = routing_phase_set(Arc::clone(&good_live))?;
    let runner = Arc::new(runner(
        scratch.runner(),
        phases,
        available_capacity(),
        "isolation-runner",
    )?);
    let chains = vec![chain("bad-chain")?, chain("good-chain")?];
    let runtime = RuntimeConfig::new(
        "isolation-runner",
        chains,
        CapacityConfig::default(),
        test_timing(),
    )?;
    let cancellation = CancellationToken::new();
    let run_cancellation = cancellation.clone();
    let task = tokio::spawn(async move { runner.run(&runtime, run_cancellation).await });
    good_live.notified().await;
    cancellation.cancel();
    let report = task.await??;

    assert_eq!(report.stopped_chains.len(), 1);
    assert_eq!(report.stopped_chains[0].0, "bad-chain");
    assert_eq!(report.stopped_chains[0].1.kind(), ErrorKind::DataIntegrity);
    let good_status: String = sqlx::query_scalar(
        "
        SELECT phase_status
        FROM chain_phase_state
        WHERE chain_id = 'good-chain'
          AND phase_name = 'live'
        ",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(good_status, "running");
    scratch.cleanup().await
}

#[tokio::test]
async fn startup_settles_active_phase_for_unconfigured_chain() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_unconfigured_recovery").await?;
    let removed_chain = "removed-chain";
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(removed_chain).await?;
    sqlx::query(
        "
        UPDATE chain_phase_state
        SET phase_status = 'running',
            started_at = now(),
            finished_at = NULL,
            updated_at = now()
        WHERE chain_id = $1
          AND phase_name = 'live'
        ",
    )
    .bind(removed_chain)
    .execute(scratch.pool())
    .await?;

    let runtime = RuntimeConfig::new(
        "unconfigured-recovery-runner",
        vec![chain("retained-chain")?],
        CapacityConfig::default(),
        test_timing(),
    )?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    Arc::new(runner(
        scratch.runner(),
        complete_phase_set(None),
        available_capacity(),
        "unconfigured-recovery-runner",
    )?)
    .run(&runtime, cancellation)
    .await?;

    assert_eq!(
        store.status(removed_chain, PhaseName::Live).await?,
        PhaseStatus::Completed
    );
    let settlement: Option<bool> = sqlx::query_scalar(
        "SELECT settled_while_unconfigured
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'live'",
    )
    .bind(removed_chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(settlement, Some(true));
    scratch.cleanup().await
}

#[tokio::test]
async fn startup_marks_every_phase_it_settles_for_unconfigured_chains() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_all_phase_settlement_markers").await?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    for phase in PhaseName::ALL {
        let chain_id = format!("removed-{phase}-chain");
        store.initialize_chain(&chain_id).await?;
        sqlx::query(
            "UPDATE chain_phase_state
             SET phase_status = 'running', started_at = now(),
                 finished_at = NULL, updated_at = now()
             WHERE chain_id = $1 AND phase_name = $2",
        )
        .bind(&chain_id)
        .bind(phase.as_str())
        .execute(scratch.pool())
        .await?;
    }

    let runtime = RuntimeConfig::new(
        "all-phase-settlement-marker-runner",
        vec![chain("retained-chain")?],
        CapacityConfig::default(),
        test_timing(),
    )?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    Arc::new(runner(
        scratch.runner(),
        complete_phase_set(None),
        available_capacity(),
        "all-phase-settlement-marker-runner",
    )?)
    .run(&runtime, cancellation)
    .await?;

    let settled_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM chain_phase_state
         WHERE chain_id LIKE 'removed-%-chain'
           AND phase_status = 'completed'
           AND settled_while_unconfigured IS TRUE",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(settled_count, PhaseName::ALL.len() as i64);
    scratch.cleanup().await
}

#[tokio::test]
async fn configured_restart_preserves_a_settled_live_marker_until_real_completion() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_preserves_live_settlement_marker").await?;
    let chain_id = "readded-live-chain";
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain_id).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'running', settled_while_unconfigured = TRUE,
             started_at = now(), finished_at = NULL, updated_at = now()
         WHERE chain_id = $1 AND phase_name = 'live'",
    )
    .bind(chain_id)
    .execute(scratch.pool())
    .await?;

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    runner(
        scratch.runner(),
        complete_phase_set(None),
        available_capacity(),
        "readded-live-marker-runner",
    )?
    .run_chain(&chain(chain_id)?, cancellation)
    .await?;

    let recovered: (String, Option<bool>) = sqlx::query_as(
        "SELECT phase_status, settled_while_unconfigured
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'live'",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        recovered,
        ("completed".to_owned(), Some(true)),
        "stopped-phase recovery must preserve settlement provenance until a real Live completion"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn readding_chain_preserves_settled_derived_phase_without_a_canonical_head() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_settled_derived_without_head").await?;
    let store = PhaseStore::new(scratch.runner().pool().clone());

    for phase in [PhaseName::Interpret, PhaseName::Project] {
        let chain_id = format!("settled-{phase}-without-head");
        store.initialize_chain(&chain_id).await?;
        mark_completed(scratch.pool(), &chain_id, PhaseName::Ingest, None).await?;
        mark_completed(
            scratch.pool(),
            &chain_id,
            PhaseName::Interpret,
            Some(phase_runner::INTERPRETER_CONTENT_HASH),
        )
        .await?;
        if phase == PhaseName::Project {
            mark_completed(
                scratch.pool(),
                &chain_id,
                PhaseName::Project,
                Some(phase_runner::INTERPRETER_CONTENT_HASH),
            )
            .await?;
        }
        sqlx::query(
            "UPDATE chain_phase_state SET settled_while_unconfigured = TRUE
             WHERE chain_id = $1 AND phase_name = $2",
        )
        .bind(&chain_id)
        .bind(phase.as_str())
        .execute(scratch.pool())
        .await?;

        let error = store
            .start_phase(&chain_id, phase, &RunMode::Normal)
            .await
            .expect_err("a settled phase cannot be revalidated without a canonical head");
        assert_eq!(error.kind(), ErrorKind::DataIntegrity);
        assert!(
            error.to_string().contains("without a canonical head"),
            "{error}"
        );
        let state: (String, Option<bool>, Option<i64>) = sqlx::query_as(
            "SELECT phase_status, settled_while_unconfigured, current_block_number
             FROM chain_phase_state WHERE chain_id = $1 AND phase_name = $2",
        )
        .bind(&chain_id)
        .bind(phase.as_str())
        .fetch_one(scratch.pool())
        .await?;
        assert_eq!(state, ("completed".to_owned(), Some(true), Some(100)));
    }

    scratch.cleanup().await
}

#[tokio::test]
async fn unconfigured_settlement_stops_without_writing_after_lock_connection_loss() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_unconfigured_settlement_lock_loss").await?;
    let removed_chain = "removed-chain-lock-loss";
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(removed_chain).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'running', started_at = now(), finished_at = NULL, updated_at = now()
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(removed_chain)
    .execute(scratch.pool())
    .await?;

    let mut blocked_row = scratch.pool().begin().await?;
    sqlx::query(
        "SELECT phase_name FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'verify' FOR UPDATE",
    )
    .bind(removed_chain)
    .fetch_one(&mut *blocked_row)
    .await?;
    let runtime = RuntimeConfig::new(
        "unconfigured-settlement-lock-loss-runner",
        vec![chain("retained-chain")?],
        CapacityConfig::default(),
        test_timing(),
    )?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let settlement_runner = Arc::new(runner(
        scratch.runner(),
        complete_phase_set(None),
        available_capacity(),
        "unconfigured-settlement-lock-loss-runner",
    )?);
    let mut run = tokio::spawn(async move { settlement_runner.run(&runtime, cancellation).await });

    let lock_pid = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let pid: Option<i32> = sqlx::query_scalar(
                "SELECT advisory.pid
                 FROM pg_locks advisory
                 WHERE advisory.locktype = 'advisory'
                   AND advisory.granted
                   AND advisory.database = (
                       SELECT oid FROM pg_database WHERE datname = current_database()
                   )
                   AND EXISTS (
                       SELECT 1 FROM pg_stat_activity activity
                       WHERE activity.datname = current_database()
                         AND activity.wait_event_type = 'Lock'
                         AND activity.query LIKE '%UPDATE chain_phase_state%'
                   )
                 LIMIT 1",
            )
            .fetch_optional(scratch.pool())
            .await?;
            if let Some(pid) = pid {
                return Ok::<i32, anyhow::Error>(pid);
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(lock_pid)
        .fetch_one(scratch.pool())
        .await?;
    assert!(terminated);
    blocked_row.rollback().await?;

    let error = tokio::time::timeout(Duration::from_secs(2), &mut run)
        .await??
        .expect_err("losing the settlement lock connection must fail startup");
    assert!(
        error.to_string().contains("settle stopped phase")
            || error.to_string().contains("advisory lock"),
        "{error}"
    );
    let state: (String, Option<bool>) = sqlx::query_as(
        "SELECT phase_status, settled_while_unconfigured
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(removed_chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        state,
        ("running".to_owned(), None),
        "a stale settlement attempt must not change the row after losing its lock"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn unconfigured_settlement_refuses_a_phase_changed_after_discovery() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_unconfigured_settlement_revision").await?;
    let removed_chain = "removed-chain-revision";
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(removed_chain).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'running', started_at = now(), finished_at = NULL, updated_at = now()
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(removed_chain)
    .execute(scratch.pool())
    .await?;

    let mut blocked_row = scratch.pool().begin().await?;
    sqlx::query(
        "SELECT phase_name FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'verify' FOR UPDATE",
    )
    .bind(removed_chain)
    .fetch_one(&mut *blocked_row)
    .await?;
    let runtime = RuntimeConfig::new(
        "unconfigured-settlement-revision-runner",
        vec![chain("retained-chain")?],
        CapacityConfig::default(),
        test_timing(),
    )?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let settlement_runner = Arc::new(runner(
        scratch.runner(),
        complete_phase_set(None),
        available_capacity(),
        "unconfigured-settlement-revision-runner",
    )?);
    let run = tokio::spawn(async move { settlement_runner.run(&runtime, cancellation).await });

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS (
                     SELECT 1 FROM pg_stat_activity
                     WHERE datname = current_database()
                       AND wait_event_type = 'Lock'
                       AND query LIKE '%UPDATE chain_phase_state%'
                 )",
            )
            .fetch_one(scratch.pool())
            .await?;
            if waiting {
                return Ok::<(), anyhow::Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    sqlx::query(
        "UPDATE chain_phase_state
         SET updated_at = updated_at + interval '1 second'
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(removed_chain)
    .execute(&mut *blocked_row)
    .await?;
    blocked_row.commit().await?;
    let error = run
        .await?
        .expect_err("startup must report a phase changed after settlement discovery");
    assert_eq!(error.kind(), ErrorKind::Transient);
    assert!(
        error
            .to_string()
            .contains("changed after startup discovery"),
        "{error}"
    );

    let state: (String, Option<bool>) = sqlx::query_as(
        "SELECT phase_status, settled_while_unconfigured
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(removed_chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        state,
        ("running".to_owned(), None),
        "settlement must refuse a row changed after the startup scan"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn readding_chain_restarts_ingest_settled_before_handoff() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_readd_partial_ingest").await?;
    let chain_id = "readded-ingest-chain";
    let configured_chain = chain(chain_id)?;
    seed_identified_lineage(scratch.pool(), chain_id, 3).await?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain_id).await?;
    store
        .start_phase(chain_id, PhaseName::Ingest, &RunMode::Normal)
        .await?;
    let partial = PhaseProgress {
        current: Some(BlockMarker::new(1, format!("{chain_id}-block-1"))?),
        target: Some(BlockMarker::new(3, format!("{chain_id}-block-3"))?),
        source_progress: vec![SourceProgress {
            source_key: "source".to_owned(),
            current: Some(BlockMarker::new(1, format!("{chain_id}-block-1"))?),
            target: Some(BlockMarker::new(3, format!("{chain_id}-block-3"))?),
            redo_loaded_boundary: None,
        }],
        ..PhaseProgress::default()
    };
    store
        .record_progress(
            chain_id,
            PhaseName::Ingest,
            &RunMode::Normal,
            None,
            &partial,
        )
        .await?;
    store
        .update_ingest_cursors(&configured_chain.sources, &partial)
        .await?;

    settle_active_rows_for_removed_chain(&scratch).await?;
    assert_eq!(
        store.status(chain_id, PhaseName::Ingest).await?,
        PhaseStatus::Completed
    );
    let settled: Option<bool> = sqlx::query_scalar(
        "SELECT settled_while_unconfigured
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(settled, Some(true));

    let calls = Arc::new(Mutex::new(Vec::new()));
    let cancellation = CancellationToken::new();
    let phases = PhaseName::ALL.map(|name| {
        let calls = Arc::clone(&calls);
        let cancellation = cancellation.clone();
        Arc::new(FunctionPhase {
            name,
            handler: Arc::new(move |context| {
                calls.lock().expect("phase calls lock").push(name);
                let progress = match name {
                    PhaseName::Ingest => {
                        assert_eq!(
                            context.resume.current.as_ref().map(|marker| marker.number),
                            Some(1)
                        );
                        assert_eq!(
                            context.resume.target.as_ref().map(|marker| marker.number),
                            Some(3)
                        );
                        assert_eq!(context.resume.ingest_cursors.len(), 1);
                        assert_eq!(context.resume.ingest_cursors[0].next_block_number, 2);
                        let marker = BlockMarker::new(3, format!("{chain_id}-block-3"))?;
                        PhaseProgress {
                            current: Some(marker.clone()),
                            target: Some(marker.clone()),
                            live_handoff: Some(marker.clone()),
                            heads: Some(HeadMarkers {
                                latest: marker.clone(),
                                safe: None,
                                finalized: None,
                            }),
                            source_progress: vec![SourceProgress {
                                source_key: "source".to_owned(),
                                current: Some(marker.clone()),
                                target: Some(marker),
                                redo_loaded_boundary: None,
                            }],
                            ..PhaseProgress::default()
                        }
                    }
                    PhaseName::Verify => PhaseProgress {
                        verification_level: Some(VerificationLevel::QuickSynced),
                        ..PhaseProgress::default()
                    },
                    PhaseName::Live => {
                        cancellation.cancel();
                        return Ok(PhaseBatchOutcome::Idle(PhaseProgress::default()));
                    }
                    PhaseName::Interpret | PhaseName::Project => PhaseProgress::default(),
                };
                Ok(PhaseBatchOutcome::Complete(progress))
            }),
        }) as Arc<dyn Phase>
    });
    let mut configured_chain = configured_chain;
    configured_chain.verify_before_live = true;
    let result = runner(
        scratch.runner(),
        PhaseSet::new(phases)?,
        available_capacity(),
        "readded-ingest-runner",
    )?
    .run_chain(&configured_chain, cancellation)
    .await;
    let observed_calls = calls.lock().expect("phase calls lock").clone();
    assert!(
        result.is_ok(),
        "re-added chain failed after phase calls {observed_calls:?}: {result:?}"
    );
    assert_eq!(
        observed_calls,
        vec![
            PhaseName::Ingest,
            PhaseName::Interpret,
            PhaseName::Project,
            PhaseName::Verify,
            PhaseName::Live,
        ]
    );
    let ingest: (String, Option<i64>, Option<i64>, Option<i64>, Option<bool>) = sqlx::query_as(
        "SELECT phase_status, current_block_number, target_block_number,
                live_handoff_block_number, settled_while_unconfigured
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        ingest,
        ("completed".to_owned(), Some(3), Some(3), Some(3), None)
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn readding_chain_restarts_ingest_settled_with_wrong_handoff() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_readd_wrong_ingest_handoff").await?;
    let chain_id = "readded-wrong-ingest-handoff-chain";
    seed_identified_lineage(scratch.pool(), chain_id, 3).await?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain_id).await?;
    store
        .start_phase(chain_id, PhaseName::Ingest, &RunMode::Normal)
        .await?;
    let target = BlockMarker::new(3, format!("{chain_id}-block-3"))?;
    store
        .record_progress(
            chain_id,
            PhaseName::Ingest,
            &RunMode::Normal,
            None,
            &PhaseProgress {
                current: Some(target.clone()),
                target: Some(target),
                live_handoff: Some(BlockMarker::new(2, format!("{chain_id}-block-2"))?),
                ..PhaseProgress::default()
            },
        )
        .await?;

    settle_active_rows_for_removed_chain(&scratch).await?;
    assert_eq!(
        store
            .start_phase(chain_id, PhaseName::Ingest, &RunMode::Normal)
            .await?,
        StartDisposition::Started,
        "a settled Ingest handoff that differs from its target must resume on re-add"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn readding_chain_restarts_after_legacy_torn_ingest_progress() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_readd_torn_ingest_progress").await?;
    let chain_id = "readded-torn-ingest-chain";
    let configured_chain = chain(chain_id)?;
    seed_identified_lineage(scratch.pool(), chain_id, 3).await?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain_id).await?;
    store
        .start_phase(chain_id, PhaseName::Ingest, &RunMode::Normal)
        .await?;
    let partial = PhaseProgress {
        current: Some(BlockMarker::new(1, format!("{chain_id}-block-1"))?),
        target: Some(BlockMarker::new(3, format!("{chain_id}-block-3"))?),
        source_progress: vec![SourceProgress {
            source_key: "source".to_owned(),
            current: Some(BlockMarker::new(1, format!("{chain_id}-block-1"))?),
            target: Some(BlockMarker::new(3, format!("{chain_id}-block-3"))?),
            redo_loaded_boundary: None,
        }],
        ..PhaseProgress::default()
    };
    store
        .update_ingest_cursors(&configured_chain.sources, &partial)
        .await?;
    let target = BlockMarker::new(3, format!("{chain_id}-block-3"))?;
    store
        .record_progress(
            chain_id,
            PhaseName::Ingest,
            &RunMode::Normal,
            None,
            &PhaseProgress {
                current: Some(target.clone()),
                target: Some(target.clone()),
                live_handoff: Some(target),
                ..PhaseProgress::default()
            },
        )
        .await?;

    settle_active_rows_for_removed_chain(&scratch).await?;
    let handoff: Option<i64> = sqlx::query_scalar(
        "SELECT live_handoff_block_number
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(handoff, None, "settlement must invalidate the handoff");
    assert_eq!(
        store
            .start_phase(chain_id, PhaseName::Ingest, &RunMode::Normal)
            .await?,
        StartDisposition::Started
    );
    let resume = store
        .phase_resume(chain_id, PhaseName::Ingest, &RunMode::Normal)
        .await?;
    assert_eq!(resume.ingest_cursors.len(), 1);
    assert_eq!(resume.ingest_cursors[0].next_block_number, 2);
    scratch.cleanup().await
}

#[tokio::test]
async fn readding_chain_restarts_ingest_settled_without_block_extent() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_readd_ingest_without_extent").await?;
    let chain_id = "readded-ingest-without-extent-chain";
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain_id).await?;
    store
        .start_phase(chain_id, PhaseName::Ingest, &RunMode::Normal)
        .await?;

    settle_active_rows_for_removed_chain(&scratch).await?;
    assert_eq!(
        store
            .start_phase(chain_id, PhaseName::Ingest, &RunMode::Normal)
            .await?,
        StartDisposition::Started,
        "a settled Ingest phase without a live handoff must resume on re-add"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn readding_chain_restarts_verify_settled_without_level() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_readd_partial_verify").await?;
    let chain_id = "readded-verify-chain";
    let configured_chain = chain(chain_id)?;
    seed_identified_lineage(scratch.pool(), chain_id, 0).await?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain_id).await?;
    let marker = BlockMarker::new(0, format!("{chain_id}-block-0"))?;
    let ingest_progress = PhaseProgress {
        current: Some(marker.clone()),
        target: Some(marker.clone()),
        live_handoff: Some(marker.clone()),
        source_progress: vec![SourceProgress {
            source_key: "source".to_owned(),
            current: Some(marker.clone()),
            target: Some(marker.clone()),
            redo_loaded_boundary: None,
        }],
        ..PhaseProgress::default()
    };
    store
        .start_phase(chain_id, PhaseName::Ingest, &RunMode::Normal)
        .await?;
    store
        .update_ingest_cursors(&configured_chain.sources, &ingest_progress)
        .await?;
    complete_phase_with_lock(
        &scratch,
        &store,
        chain_id,
        PhaseName::Ingest,
        &ingest_progress,
    )
    .await?;
    publish_heads(
        scratch.pool(),
        chain_id,
        &HeadMarkers {
            latest: marker.clone(),
            safe: None,
            finalized: None,
        },
    )
    .await?;
    for phase in [PhaseName::Interpret, PhaseName::Project] {
        store.start_phase(chain_id, phase, &RunMode::Normal).await?;
        complete_phase_with_lock(
            &scratch,
            &store,
            chain_id,
            phase,
            &PhaseProgress {
                current: Some(marker.clone()),
                target: Some(marker.clone()),
                ..PhaseProgress::default()
            },
        )
        .await?;
    }
    store
        .start_phase(chain_id, PhaseName::Verify, &RunMode::Normal)
        .await?;

    settle_active_rows_for_removed_chain(&scratch).await?;
    assert_eq!(
        store.status(chain_id, PhaseName::Verify).await?,
        PhaseStatus::Completed
    );

    let calls = Arc::new(Mutex::new(Vec::new()));
    let cancellation = CancellationToken::new();
    let phases = PhaseName::ALL.map(|name| {
        let calls = Arc::clone(&calls);
        let cancellation = cancellation.clone();
        Arc::new(FunctionPhase {
            name,
            handler: Arc::new(move |_| {
                calls.lock().expect("phase calls lock").push(name);
                if name == PhaseName::Live {
                    cancellation.cancel();
                    return Ok(PhaseBatchOutcome::Idle(PhaseProgress::default()));
                }
                let progress = PhaseProgress {
                    verification_level: (name == PhaseName::Verify)
                        .then_some(VerificationLevel::QuickSynced),
                    ..PhaseProgress::default()
                };
                Ok(PhaseBatchOutcome::Complete(progress))
            }),
        }) as Arc<dyn Phase>
    });
    let mut configured_chain = configured_chain;
    configured_chain.verify_before_live = true;
    runner(
        scratch.runner(),
        PhaseSet::new(phases)?,
        available_capacity(),
        "readded-verify-runner",
    )?
    .run_chain(&configured_chain, cancellation)
    .await?;

    let observed_calls = calls.lock().expect("phase calls lock").clone();
    let verification_level: Option<String> = sqlx::query_scalar(
        "SELECT verification_level
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        observed_calls,
        vec![PhaseName::Verify, PhaseName::Live],
        "Live must not start before a fabricated Verify completion is rerun"
    );
    assert_eq!(verification_level.as_deref(), Some("quick_synced"));
    scratch.cleanup().await
}

#[tokio::test]
async fn readding_chain_restarts_verify_settled_without_block_extent() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_readd_verify_without_extent").await?;
    let chain_id = "readded-verify-without-extent-chain";
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain_id).await?;
    for phase in [PhaseName::Ingest, PhaseName::Interpret, PhaseName::Project] {
        mark_completed(scratch.pool(), chain_id, phase, None).await?;
    }
    store
        .start_phase(chain_id, PhaseName::Verify, &RunMode::Normal)
        .await?;
    store
        .record_progress(
            chain_id,
            PhaseName::Verify,
            &RunMode::Normal,
            None,
            &PhaseProgress {
                verification_level: Some(VerificationLevel::QuickSynced),
                ..PhaseProgress::default()
            },
        )
        .await?;

    settle_active_rows_for_removed_chain(&scratch).await?;
    assert_eq!(
        store
            .start_phase(chain_id, PhaseName::Verify, &RunMode::Normal)
            .await?,
        StartDisposition::Started,
        "a settled Verify level without a block extent must resume on re-add"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn readding_chain_restarts_verify_settled_before_target_with_level() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_readd_partial_verify_with_level").await?;
    let chain_id = "readded-partial-verify-chain";
    let configured_chain = chain(chain_id)?;
    seed_identified_lineage(scratch.pool(), chain_id, 3).await?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain_id).await?;
    let target = BlockMarker::new(3, format!("{chain_id}-block-3"))?;
    let ingest_progress = PhaseProgress {
        current: Some(target.clone()),
        target: Some(target.clone()),
        live_handoff: Some(target.clone()),
        source_progress: vec![SourceProgress {
            source_key: "source".to_owned(),
            current: Some(target.clone()),
            target: Some(target.clone()),
            redo_loaded_boundary: None,
        }],
        ..PhaseProgress::default()
    };
    store
        .start_phase(chain_id, PhaseName::Ingest, &RunMode::Normal)
        .await?;
    store
        .update_ingest_cursors(&configured_chain.sources, &ingest_progress)
        .await?;
    complete_phase_with_lock(
        &scratch,
        &store,
        chain_id,
        PhaseName::Ingest,
        &ingest_progress,
    )
    .await?;
    publish_heads(
        scratch.pool(),
        chain_id,
        &HeadMarkers {
            latest: target.clone(),
            safe: None,
            finalized: None,
        },
    )
    .await?;
    for phase in [PhaseName::Interpret, PhaseName::Project] {
        store.start_phase(chain_id, phase, &RunMode::Normal).await?;
        complete_phase_with_lock(
            &scratch,
            &store,
            chain_id,
            phase,
            &PhaseProgress {
                current: Some(target.clone()),
                target: Some(target.clone()),
                ..PhaseProgress::default()
            },
        )
        .await?;
    }
    store
        .start_phase(chain_id, PhaseName::Verify, &RunMode::Normal)
        .await?;
    let partial = PhaseProgress {
        current: Some(BlockMarker::new(1, format!("{chain_id}-block-1"))?),
        target: Some(target),
        verification_level: Some(VerificationLevel::QuickSynced),
        ..PhaseProgress::default()
    };
    store
        .record_progress(
            chain_id,
            PhaseName::Verify,
            &RunMode::Normal,
            None,
            &partial,
        )
        .await?;

    settle_active_rows_for_removed_chain(&scratch).await?;
    assert_eq!(
        store.status(chain_id, PhaseName::Verify).await?,
        PhaseStatus::Completed
    );
    assert_eq!(
        store
            .start_phase(chain_id, PhaseName::Verify, &RunMode::Normal)
            .await?,
        StartDisposition::Started,
        "a settled Verify cursor before its target must resume on re-add"
    );
    let resume = store
        .phase_resume(chain_id, PhaseName::Verify, &RunMode::Normal)
        .await?;
    assert_eq!(resume.current.as_ref().map(|marker| marker.number), Some(1));
    assert_eq!(resume.target.as_ref().map(|marker| marker.number), Some(3));
    assert_eq!(
        resume.verification_level,
        Some(VerificationLevel::QuickSynced)
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn panicking_phase_stops_only_its_chain_supervisor() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_panic_isolation").await?;
    seed_identified_lineage(scratch.pool(), "good-panic-chain", 0).await?;
    let good_live = Arc::new(Notify::new());
    let panic_trigger = Arc::new(Notify::new());
    let phases = panic_routing_phase_set(Arc::clone(&good_live), panic_trigger)?;
    let runner = Arc::new(runner(
        scratch.runner(),
        phases,
        available_capacity(),
        "panic-isolation-runner",
    )?);
    let chains = vec![chain("bad-chain")?, chain("good-panic-chain")?];
    let runtime = RuntimeConfig::new(
        "panic-isolation-runner",
        chains,
        CapacityConfig::default(),
        test_timing(),
    )?;
    let cancellation = CancellationToken::new();
    let run_cancellation = cancellation.clone();
    let task = tokio::spawn(async move { runner.run(&runtime, run_cancellation).await });
    good_live.notified().await;
    cancellation.cancel();
    let report = task.await??;

    assert_eq!(report.stopped_chains.len(), 1);
    assert_eq!(report.stopped_chains[0].0, "bad-chain");
    assert!(report.stopped_chains[0].1.to_string().contains("panicked"));
    let good_status: String = sqlx::query_scalar(
        "
        SELECT phase_status
        FROM chain_phase_state
        WHERE chain_id = 'good-panic-chain'
          AND phase_name = 'live'
        ",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(good_status, "running");
    scratch.cleanup().await
}

#[tokio::test]
async fn cancellation_keeps_partial_phase_progress_restartable() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_cancel_resume").await?;
    let entered = Arc::new(Notify::new());
    let phase = Arc::new(FunctionPhase {
        name: PhaseName::Ingest,
        handler: {
            let entered = Arc::clone(&entered);
            Arc::new(move |_| {
                entered.notify_one();
                let marker = BlockMarker::new(4, "cancel-chain-block-4")?;
                Ok(PhaseBatchOutcome::Continue(PhaseProgress {
                    current: Some(marker.clone()),
                    target: Some(marker.clone()),
                    live_handoff: Some(marker),
                    ..PhaseProgress::default()
                }))
            })
        },
    });
    let first_runner = runner(
        scratch.runner(),
        phase_set_replacing(PhaseName::Ingest, phase)?,
        available_capacity(),
        "cancel-runner",
    )?;
    let cancellation = CancellationToken::new();
    let run_cancellation = cancellation.clone();
    let task = tokio::spawn(async move {
        first_runner
            .run_chain(&chain("cancel-chain")?, run_cancellation)
            .await
    });
    entered.notified().await;
    cancellation.cancel();
    task.await??;

    let state: (String, Option<i64>, Option<String>) = sqlx::query_as(
        "
        SELECT phase_status, current_block_number, current_block_hash
        FROM chain_phase_state
        WHERE chain_id = 'cancel-chain'
          AND phase_name = 'ingest'
        ",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        state,
        (
            "running".to_owned(),
            Some(4),
            Some("cancel-chain-block-4".to_owned())
        )
    );

    let resumed = Arc::new(AtomicUsize::new(0));
    let phase = Arc::new(FunctionPhase {
        name: PhaseName::Ingest,
        handler: {
            let resumed = Arc::clone(&resumed);
            Arc::new(move |context| {
                let current =
                    context.resume.current.as_ref().ok_or_else(|| {
                        RunnerError::data_integrity("missing phase resume position")
                    })?;
                if current.number != 4
                    || context.resume.ingest_cursors.len() != 1
                    || context.resume.ingest_cursors[0].next_block_number != 5
                {
                    return Err(RunnerError::data_integrity(
                        "phase resumed from the wrong ingest position",
                    ));
                }
                resumed.fetch_add(1, Ordering::SeqCst);
                Ok(PhaseBatchOutcome::Complete(PhaseProgress {
                    current: Some(current.clone()),
                    target: Some(current.clone()),
                    live_handoff: Some(current.clone()),
                    ..PhaseProgress::default()
                }))
            })
        },
    });
    runner(
        scratch.runner(),
        phase_set_replacing(PhaseName::Ingest, phase)?,
        available_capacity(),
        "resume-runner",
    )?
    .run_chain(&chain("cancel-chain")?, CancellationToken::new())
    .await?;
    assert_eq!(resumed.load(Ordering::SeqCst), 1);
    scratch.cleanup().await
}

#[tokio::test]
async fn redo_restores_the_full_phase_lifecycle_state() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_redo_state").await?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    let chain_id = "redo-state-chain";
    store.initialize_chain(chain_id).await?;
    seed_interpret_redo_presence(scratch.pool(), chain_id, 3).await?;
    mark_completed(scratch.pool(), chain_id, PhaseName::Ingest, None).await?;
    mark_completed(
        scratch.pool(),
        chain_id,
        PhaseName::Interpret,
        Some(phase_runner::INTERPRETER_CONTENT_HASH),
    )
    .await?;
    mark_completed(
        scratch.pool(),
        chain_id,
        PhaseName::Project,
        Some(phase_runner::INTERPRETER_CONTENT_HASH),
    )
    .await?;
    sqlx::query(
        "
        UPDATE chain_phase_state
        SET current_block_number = 7,
            current_block_hash = 'redo-state-block-7',
            target_block_number = 9,
            target_block_hash = 'redo-state-block-9'
        WHERE chain_id = $1
          AND phase_name = 'project'
        ",
    )
    .bind(chain_id)
    .execute(scratch.pool())
    .await?;

    runner(
        scratch.runner(),
        PhaseSet::loopback(),
        available_capacity(),
        "redo-state-runner",
    )?
    .redo(
        &chain(chain_id)?,
        RedoPhase::Phase(PhaseName::Project),
        BlockRange::new(2, 3)?,
        CancellationToken::new(),
    )
    .await?;

    let state: (
        String,
        Option<i64>,
        Option<String>,
        Option<i64>,
        Option<String>,
    ) = sqlx::query_as(
        "
            SELECT phase_status,
                   current_block_number,
                   current_block_hash,
                   target_block_number,
                   target_block_hash
            FROM chain_phase_state
            WHERE chain_id = $1
              AND phase_name = 'project'
            ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        state,
        (
            "completed".to_owned(),
            Some(7),
            Some("redo-state-block-7".to_owned()),
            Some(9),
            Some("redo-state-block-9".to_owned())
        )
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn all_phase_redo_stops_the_failed_chain_and_continues_remaining_chains() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_redo_all_phases").await?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    let failed_chain = "redo-all-failed-chain";
    let completed_chain = "redo-all-completed-chain";
    for chain_id in [failed_chain, completed_chain] {
        store.initialize_chain(chain_id).await?;
        seed_interpret_redo_presence(scratch.pool(), chain_id, 1).await?;
        for (phase, hash) in [
            (PhaseName::Ingest, None),
            (
                PhaseName::Interpret,
                Some(phase_runner::INTERPRETER_CONTENT_HASH),
            ),
            (
                PhaseName::Project,
                Some(phase_runner::INTERPRETER_CONTENT_HASH),
            ),
            (PhaseName::Verify, None),
        ] {
            mark_completed(scratch.pool(), chain_id, phase, hash).await?;
            set_phase_extent(scratch.pool(), chain_id, phase, 1).await?;
        }
    }

    let calls = Arc::new(Mutex::new(Vec::new()));
    let phases = PhaseName::ALL.map(|name| {
        Arc::new(RecordingRedoPhase {
            name,
            calls: Arc::clone(&calls),
            fail_chain: (name == PhaseName::Interpret).then(|| failed_chain.to_owned()),
        }) as Arc<dyn Phase>
    });
    let phase_runner = runner(
        scratch.runner(),
        PhaseSet::new(phases)?,
        available_capacity(),
        "redo-all-phases-runner",
    )?;
    let mut sources = chain(completed_chain)?.sources.to_vec();
    let mut reference = sources[0].clone();
    reference.source_key = "reference".into();
    reference.role = phase_runner::config::SourceRole::VerificationOnly;
    sources.push(reference);
    let completed_config = ChainConfig::new(completed_chain, sources, false)?;
    let report = phase_runner
        .redo_chains(
            &[chain(failed_chain)?, completed_config],
            RedoPhase::All,
            BlockRange::new(0, 0)?,
            CancellationToken::new(),
        )
        .await?;

    assert_eq!(report.stopped_chains.len(), 1);
    assert_eq!(report.stopped_chains[0].0, failed_chain);
    assert!(
        report.stopped_chains[0]
            .1
            .to_string()
            .contains("fixture failed during all-phase interpret redo")
    );
    assert!(report.stopped_chains[0].1.to_string().contains(
        "phase-runner redo --chain redo-all-failed-chain --phase interpret --from-block 0 \
         --to-block 1"
    ));
    assert!(report.stopped_chains[0].1.to_string().contains(
        "phase-runner redo --chain redo-all-failed-chain --phase all --from-block 0 \
         --to-block 1"
    ));
    assert_eq!(
        *calls.lock().expect("recorded calls lock"),
        [
            (failed_chain.into(), PhaseName::Ingest),
            (failed_chain.into(), PhaseName::Interpret),
            (completed_chain.into(), PhaseName::Ingest),
            (completed_chain.into(), PhaseName::Interpret),
            (completed_chain.into(), PhaseName::Project),
            (completed_chain.into(), PhaseName::Verify),
        ]
    );

    let failed_states: Vec<(String, String, bool)> = sqlx::query_as(
        "SELECT phase_name, phase_status, redo_in_progress
         FROM chain_phase_state
         WHERE chain_id = $1
           AND phase_name IN ('ingest', 'interpret', 'project', 'verify')
         ORDER BY array_position(ARRAY['ingest','interpret','project','verify'], phase_name)",
    )
    .bind(failed_chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        failed_states,
        [
            ("ingest".into(), "completed".into(), false),
            ("interpret".into(), "running".into(), true),
            ("project".into(), "completed".into(), false),
            ("verify".into(), "completed".into(), false),
        ]
    );

    let recovery_calls = Arc::new(Mutex::new(Vec::new()));
    let recovery_phases = PhaseName::ALL.map(|name| {
        Arc::new(RecordingRedoPhase {
            name,
            calls: Arc::clone(&recovery_calls),
            fail_chain: None,
        }) as Arc<dyn Phase>
    });
    let recovery_runner = runner(
        scratch.runner(),
        PhaseSet::new(recovery_phases)?,
        available_capacity(),
        "redo-all-phases-recovery-runner",
    )?;
    let recovery_error = recovery_runner
        .redo(
            &chain(failed_chain)?,
            RedoPhase::All,
            BlockRange::new(0, 0)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("all phases must identify the interrupted phase-specific recovery");
    assert!(recovery_error.to_string().contains(
        "phase-runner redo --chain redo-all-failed-chain --phase interpret --from-block 0 \
         --to-block 1"
    ));
    assert!(recovery_error.to_string().contains(
        "phase-runner redo --chain redo-all-failed-chain --phase all --from-block 0 \
         --to-block 1"
    ));
    assert!(
        recovery_calls
            .lock()
            .expect("recovery calls lock")
            .is_empty()
    );
    recovery_runner
        .redo(
            &chain(failed_chain)?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(0, 1)?,
            CancellationToken::new(),
        )
        .await?;
    recovery_runner
        .redo(
            &chain(failed_chain)?,
            RedoPhase::All,
            BlockRange::new(0, 1)?,
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(
        *recovery_calls.lock().expect("recovery calls lock"),
        [
            (failed_chain.into(), PhaseName::Interpret),
            (failed_chain.into(), PhaseName::Project),
            (failed_chain.into(), PhaseName::Ingest),
            (failed_chain.into(), PhaseName::Interpret),
            (failed_chain.into(), PhaseName::Project),
            (failed_chain.into(), PhaseName::Verify),
        ]
    );

    let remaining_active: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM chain_phase_state
         WHERE chain_id = $1 AND redo_in_progress",
    )
    .bind(completed_chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(remaining_active, 0);
    scratch.cleanup().await
}

#[tokio::test]
async fn all_phase_redo_refuses_to_absorb_a_pending_project_redo() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_redo_all_pending_project").await?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    let chain_id = "redo-all-pending-project-chain";
    store.initialize_chain(chain_id).await?;
    seed_interpret_redo_presence(scratch.pool(), chain_id, 1).await?;
    mark_completed(
        scratch.pool(),
        chain_id,
        PhaseName::Project,
        Some(phase_runner::INTERPRETER_CONTENT_HASH),
    )
    .await?;
    set_phase_extent(scratch.pool(), chain_id, PhaseName::Project, 1).await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'running',
             redo_in_progress = true,
             redo_mode = 'redo',
             redo_previous_phase_status = 'completed',
             redo_previous_last_error = NULL,
             redo_previous_started_at = started_at,
             redo_previous_finished_at = finished_at,
             redo_from_block_number = 0,
             redo_to_block_number = 1,
             last_error = 'operator project redo interrupted',
             started_at = now(),
             finished_at = NULL,
             updated_at = now()
         WHERE chain_id = $1
           AND phase_name = 'project'",
    )
    .bind(chain_id)
    .execute(scratch.pool())
    .await?;

    let before: (
        String,
        bool,
        Option<String>,
        Option<i64>,
        Option<i64>,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT phase_status, redo_in_progress, redo_mode,
                    redo_from_block_number, redo_to_block_number, last_error
             FROM chain_phase_state
             WHERE chain_id = $1
               AND phase_name = 'project'",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    let calls = Arc::new(Mutex::new(Vec::new()));
    let phases = PhaseName::ALL.map(|name| {
        Arc::new(RecordingRedoPhase {
            name,
            calls: Arc::clone(&calls),
            fail_chain: None,
        }) as Arc<dyn Phase>
    });
    let error = runner(
        scratch.runner(),
        PhaseSet::new(phases)?,
        available_capacity(),
        "redo-all-pending-project-runner",
    )?
    .redo(
        &chain(chain_id)?,
        RedoPhase::All,
        BlockRange::new(0, 0)?,
        CancellationToken::new(),
    )
    .await
    .expect_err("all phases must not absorb an existing project redo");

    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(error.to_string().contains("pending project redo"));
    assert!(calls.lock().expect("recorded calls lock").is_empty());
    let after: (
        String,
        bool,
        Option<String>,
        Option<i64>,
        Option<i64>,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT phase_status, redo_in_progress, redo_mode,
                    redo_from_block_number, redo_to_block_number, last_error
             FROM chain_phase_state
             WHERE chain_id = $1
               AND phase_name = 'project'",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(after, before);
    scratch.cleanup().await
}

#[tokio::test]
async fn all_chains_redo_discovers_only_active_manifest_chains_in_stable_order() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_redo_all_chains").await?;
    for (chain_id, rollout_status) in [
        ("redo-chain-b", "active"),
        ("redo-chain-retired", "deprecated"),
        ("redo-chain-a", "active"),
    ] {
        sqlx::query(
            "INSERT INTO manifest_versions (
                 manifest_version, namespace, source_family, chain_id,
                 deployment_label, rollout_status, normalizer_version,
                 file_path, manifest_payload
             ) VALUES (1, 'ens', 'fixture', $1, 'fixture', $2,
                       'fixture-normalizer', $3, '{}'::jsonb)",
        )
        .bind(chain_id)
        .bind(rollout_status)
        .bind(format!("tests/{chain_id}.toml"))
        .execute(scratch.pool())
        .await?;
    }

    let chains = resolve_all_redo_chains(scratch.pool(), Vec::new(), false).await?;
    assert_eq!(
        chains
            .iter()
            .map(|chain| chain.chain_id.as_str())
            .collect::<Vec<_>>(),
        ["redo-chain-a", "redo-chain-b"]
    );
    assert!(chains.iter().all(|chain| chain.sources.is_empty()));
    scratch.cleanup().await
}

#[tokio::test]
async fn redo_after_an_interrupted_phase_requires_normal_resume() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_redo_interrupted").await?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    let chain_id = "redo-interrupted-chain";
    store.initialize_chain(chain_id).await?;
    mark_completed(scratch.pool(), chain_id, PhaseName::Ingest, None).await?;
    sqlx::query(
        "
        UPDATE chain_phase_state
        SET phase_status = 'running',
            current_block_number = 4,
            current_block_hash = 'redo-interrupted-block-4',
            input_content_hash = $2,
            started_at = now(),
            finished_at = NULL,
            updated_at = now()
        WHERE chain_id = $1
          AND phase_name = 'interpret'
        ",
    )
    .bind(chain_id)
    .bind(phase_runner::INTERPRETER_CONTENT_HASH)
    .execute(scratch.pool())
    .await?;
    mark_completed(
        scratch.pool(),
        chain_id,
        PhaseName::Project,
        Some("keccak256:older-binary"),
    )
    .await?;
    seed_interpret_redo_presence(scratch.pool(), chain_id, 4).await?;

    runner(
        scratch.runner(),
        PhaseSet::loopback(),
        available_capacity(),
        "redo-interrupted-runner",
    )?
    .redo(
        &chain(chain_id)?,
        RedoPhase::Phase(PhaseName::Interpret),
        BlockRange::new(2, 3)?,
        CancellationToken::new(),
    )
    .await?;

    assert_eq!(
        store.status(chain_id, PhaseName::Interpret).await?,
        PhaseStatus::Failed
    );
    assert_eq!(
        store
            .start_phase(chain_id, PhaseName::Interpret, &RunMode::Normal)
            .await?,
        StartDisposition::Started
    );
    let resumed = store
        .phase_resume(chain_id, PhaseName::Interpret, &RunMode::Normal)
        .await?;
    assert_eq!(resumed.current.map(|marker| marker.number), Some(4));
    scratch.cleanup().await
}

#[tokio::test]
async fn redo_interpret_can_start_a_new_content_hash_epoch() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_hash_redo").await?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    let chain_id = "hash-redo-chain";
    store.initialize_chain(chain_id).await?;
    mark_completed(scratch.pool(), chain_id, PhaseName::Ingest, None).await?;
    mark_completed(
        scratch.pool(),
        chain_id,
        PhaseName::Interpret,
        Some("keccak256:older-binary"),
    )
    .await?;
    mark_completed(
        scratch.pool(),
        chain_id,
        PhaseName::Project,
        Some("keccak256:older-binary"),
    )
    .await?;
    set_phase_extent(scratch.pool(), chain_id, PhaseName::Interpret, 9).await?;
    set_phase_extent(scratch.pool(), chain_id, PhaseName::Project, 9).await?;
    sqlx::query(
        "
        UPDATE chain_phase_state
        SET live_handoff_block_number = 9,
            live_handoff_block_hash = 'hash-redo-block-9'
        WHERE chain_id = $1
          AND phase_name = 'ingest'
        ",
    )
    .bind(chain_id)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "
        INSERT INTO ingest_cursors (
            chain_id,
            source_key,
            source_kind,
            seed_basis,
            start_block_number,
            next_block_number,
            target_block_number,
            last_processed_block_number,
            last_processed_block_hash
        )
        VALUES ($1, 'source', 'test', 'ethereum_head', 0, 10, 9, 9, 'hash-redo-block-9')
        ",
    )
    .bind(chain_id)
    .execute(scratch.pool())
    .await?;
    seed_interpret_redo_presence(scratch.pool(), chain_id, 9).await?;
    let runner = runner(
        scratch.runner(),
        PhaseSet::loopback(),
        available_capacity(),
        "hash-redo-runner",
    )?;

    let error = runner
        .redo(
            &chain(chain_id)?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(0, 1)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("a partial redo cannot adopt a new content hash");
    assert_eq!(error.kind(), ErrorKind::ContentHashMismatch);
    assert!(error.to_string().contains("full range 0..=9"));

    runner
        .redo(
            &chain(chain_id)?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(0, 9)?,
            CancellationToken::new(),
        )
        .await?;
    runner
        .redo(
            &chain(chain_id)?,
            RedoPhase::Phase(PhaseName::Project),
            BlockRange::new(0, 9)?,
            CancellationToken::new(),
        )
        .await?;

    let hashes: Vec<(String, Option<String>)> = sqlx::query_as(
        "
        SELECT phase_name, input_content_hash
        FROM chain_phase_state
        WHERE chain_id = $1
          AND phase_name IN ('interpret', 'project')
        ORDER BY phase_name
        ",
    )
    .bind(chain_id)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        hashes,
        vec![
            (
                "interpret".to_owned(),
                Some(phase_runner::INTERPRETER_CONTENT_HASH.to_owned())
            ),
            (
                "project".to_owned(),
                Some(phase_runner::INTERPRETER_CONTENT_HASH.to_owned())
            ),
        ]
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn partial_redo_cannot_adopt_hash_after_failed_interpret() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_failed_hash_redo").await?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    let chain_id = "failed-hash-redo-chain";
    store.initialize_chain(chain_id).await?;
    mark_completed(scratch.pool(), chain_id, PhaseName::Ingest, None).await?;
    mark_completed(
        scratch.pool(),
        chain_id,
        PhaseName::Interpret,
        Some("keccak256:older-binary"),
    )
    .await?;
    sqlx::query(
        "
        UPDATE chain_phase_state
        SET phase_status = 'failed',
            current_block_number = 4,
            current_block_hash = 'failed-hash-redo-block-4',
            target_block_number = 9,
            target_block_hash = 'failed-hash-redo-block-9',
            last_error = 'interrupted old interpreter'
        WHERE chain_id = $1
          AND phase_name = 'interpret'
        ",
    )
    .bind(chain_id)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "
        UPDATE chain_phase_state
        SET live_handoff_block_number = 9,
            live_handoff_block_hash = 'failed-hash-redo-block-9'
        WHERE chain_id = $1
          AND phase_name = 'ingest'
        ",
    )
    .bind(chain_id)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "
        INSERT INTO ingest_cursors (
            chain_id,
            source_key,
            source_kind,
            seed_basis,
            start_block_number,
            next_block_number,
            target_block_number,
            last_processed_block_number,
            last_processed_block_hash
        )
        VALUES (
            $1, 'source', 'test', 'ethereum_head', 0, 10, 9, 9,
            'failed-hash-redo-block-9'
        )
        ",
    )
    .bind(chain_id)
    .execute(scratch.pool())
    .await?;
    seed_interpret_redo_presence(scratch.pool(), chain_id, 9).await?;

    let error = runner(
        scratch.runner(),
        PhaseSet::loopback(),
        available_capacity(),
        "failed-hash-redo-runner",
    )?
    .redo(
        &chain(chain_id)?,
        RedoPhase::Phase(PhaseName::Interpret),
        BlockRange::new(5, 9)?,
        CancellationToken::new(),
    )
    .await
    .expect_err("a partial redo cannot replace a failed phase's old content hash");
    assert_eq!(error.kind(), ErrorKind::ContentHashMismatch);
    assert!(error.to_string().contains("full range 0..=9"));

    let stored: (String, Option<String>) = sqlx::query_as(
        "
        SELECT phase_status::text, input_content_hash
        FROM chain_phase_state
        WHERE chain_id = $1
          AND phase_name = 'interpret'
        ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        stored,
        (
            "failed".to_owned(),
            Some("keccak256:older-binary".to_owned())
        )
    );

    let replay_head = Arc::new(AtomicUsize::new(usize::MAX));
    let phase_replay_head = Arc::clone(&replay_head);
    let recovery_phase = Arc::new(FunctionPhase {
        name: PhaseName::Interpret,
        handler: Arc::new(move |context| {
            phase_replay_head.store(
                usize::try_from(
                    context
                        .available_heads
                        .expect("hash redo needs the processed interpret head")
                        .latest
                        .number,
                )
                .expect("test block number is nonnegative"),
                Ordering::SeqCst,
            );
            Ok(PhaseBatchOutcome::Complete(PhaseProgress::default()))
        }),
    });
    runner(
        scratch.runner(),
        phase_set_replacing(PhaseName::Interpret, recovery_phase)?,
        available_capacity(),
        "failed-hash-full-redo-runner",
    )?
    .redo(
        &chain(chain_id)?,
        RedoPhase::Phase(PhaseName::Interpret),
        BlockRange::new(0, 9)?,
        CancellationToken::new(),
    )
    .await?;
    assert_eq!(replay_head.load(Ordering::SeqCst), 4);
    let recovered_hash: Option<String> = sqlx::query_scalar(
        "
        SELECT input_content_hash
        FROM chain_phase_state
        WHERE chain_id = $1
          AND phase_name = 'interpret'
        ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        recovered_hash,
        Some(phase_runner::INTERPRETER_CONTENT_HASH.to_owned())
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn head_publication_atomically_replaces_a_readable_fork() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_head_fork").await?;
    let chain_id = "head-fork-chain";
    seed_lineage(scratch.pool(), chain_id, 2).await?;
    publish_heads(
        scratch.pool(),
        chain_id,
        &HeadMarkers {
            latest: BlockMarker::new(2, format!("{chain_id}-block-2"))?,
            safe: None,
            finalized: None,
        },
    )
    .await?;
    let initial_orphaning_epoch: i64 =
        sqlx::query_scalar("SELECT lineage_orphaning_epoch FROM chain_heads WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(initial_orphaning_epoch, 0);
    for (number, hash, parent) in [
        (1_i64, "head-fork-new-1", format!("{chain_id}-block-0")),
        (2_i64, "head-fork-new-2", "head-fork-new-1".to_owned()),
    ] {
        sqlx::query(
            "
            INSERT INTO chain_lineage (
                chain_id,
                block_hash,
                parent_hash,
                block_number,
                block_timestamp,
                canonicality_state
            )
            VALUES ($1, $2, $3, $4, to_timestamp($4), 'observed')
            ",
        )
        .bind(chain_id)
        .bind(hash)
        .bind(parent)
        .bind(number)
        .execute(scratch.pool())
        .await?;
    }

    publish_heads(
        scratch.pool(),
        chain_id,
        &HeadMarkers {
            latest: BlockMarker::new(2, "head-fork-new-2")?,
            safe: None,
            finalized: None,
        },
    )
    .await?;

    let orphaning_epoch: i64 =
        sqlx::query_scalar("SELECT lineage_orphaning_epoch FROM chain_heads WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(orphaning_epoch, 1);
    publish_heads(
        scratch.pool(),
        chain_id,
        &HeadMarkers {
            latest: BlockMarker::new(2, "head-fork-new-2")?,
            safe: None,
            finalized: None,
        },
    )
    .await?;
    let unchanged_orphaning_epoch: i64 =
        sqlx::query_scalar("SELECT lineage_orphaning_epoch FROM chain_heads WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(unchanged_orphaning_epoch, 1);

    let states: Vec<(String, String)> = sqlx::query_as(
        "
        SELECT block_hash, canonicality_state::text
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_number > 0
        ORDER BY block_hash
        ",
    )
    .bind(chain_id)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        states,
        vec![
            (format!("{chain_id}-block-1"), "orphaned".to_owned()),
            (format!("{chain_id}-block-2"), "orphaned".to_owned()),
            ("head-fork-new-1".to_owned(), "canonical".to_owned()),
            ("head-fork-new-2".to_owned(), "canonical".to_owned()),
        ]
    );
    let latest: String =
        sqlx::query_scalar("SELECT latest_block_hash FROM chain_heads WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(latest, "head-fork-new-2");
    scratch.cleanup().await
}

#[tokio::test]
async fn head_publication_refuses_to_drop_finality_markers() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_head_monotonic").await?;
    let chain_id = "head-monotonic-chain";
    seed_lineage(scratch.pool(), chain_id, 3).await?;
    let original = HeadMarkers {
        latest: BlockMarker::new(3, format!("{chain_id}-block-3"))?,
        safe: Some(BlockMarker::new(2, format!("{chain_id}-block-2"))?),
        finalized: Some(BlockMarker::new(1, format!("{chain_id}-block-1"))?),
    };
    publish_heads(scratch.pool(), chain_id, &original).await?;

    let error = publish_heads(
        scratch.pool(),
        chain_id,
        &HeadMarkers {
            latest: original.latest.clone(),
            safe: None,
            finalized: None,
        },
    )
    .await
    .expect_err("safe and finalized markers cannot disappear");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(error.to_string().contains("safe head marker"));

    let stored: (Option<i64>, Option<i64>) = sqlx::query_as(
        "
        SELECT safe_block_number, finalized_block_number
        FROM chain_heads
        WHERE chain_id = $1
        ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(stored, (Some(2), Some(1)));
    scratch.cleanup().await
}

#[tokio::test]
async fn different_writer_phases_cannot_overlap_on_one_chain() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_cross_phase").await?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    let chain_id = "cross-phase-chain";
    store.initialize_chain(chain_id).await?;
    mark_completed(scratch.pool(), chain_id, PhaseName::Ingest, None).await?;
    mark_completed(
        scratch.pool(),
        chain_id,
        PhaseName::Interpret,
        Some(phase_runner::INTERPRETER_CONTENT_HASH),
    )
    .await?;
    set_phase_extent(scratch.pool(), chain_id, PhaseName::Interpret, 1).await?;
    seed_interpret_redo_presence(scratch.pool(), chain_id, 1).await?;
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let blocking = Arc::new(BlockingPhase {
        name: PhaseName::Interpret,
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
    });
    let first = runner(
        scratch.runner(),
        phase_set_replacing(PhaseName::Interpret, blocking)?,
        available_capacity(),
        "cross-phase-first",
    )?;
    let second = runner(
        scratch.runner(),
        PhaseSet::loopback(),
        available_capacity(),
        "cross-phase-second",
    )?;
    let first_chain = chain(chain_id)?;
    let first_task = tokio::spawn(async move {
        first
            .redo(
                &first_chain,
                RedoPhase::Phase(PhaseName::Interpret),
                BlockRange::new(0, 1).expect("fixed range"),
                CancellationToken::new(),
            )
            .await
    });
    entered.notified().await;

    let error = second
        .redo(
            &chain(chain_id)?,
            RedoPhase::Phase(PhaseName::Project),
            BlockRange::new(0, 1)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("different writer phases must not overlap");
    assert_eq!(error.kind(), ErrorKind::InvalidTransition);

    release.notify_one();
    first_task.await??;
    scratch.cleanup().await
}

#[tokio::test]
async fn redo_progress_refuses_a_range_widened_while_the_phase_is_running() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_redo_concurrent_widening").await?;
    let chain_id = "redo-concurrent-widening-chain";
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain_id).await?;
    seed_interpret_redo_presence(scratch.pool(), chain_id, 2).await?;
    for phase in [PhaseName::Ingest, PhaseName::Interpret, PhaseName::Project] {
        mark_completed(
            scratch.pool(),
            chain_id,
            phase,
            phase
                .writes_derived_data()
                .then_some(phase_runner::INTERPRETER_CONTENT_HASH),
        )
        .await?;
        set_phase_extent(scratch.pool(), chain_id, phase, 2).await?;
    }

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let blocking = Arc::new(BlockingPhase {
        name: PhaseName::Project,
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
    });
    let phase_runner = runner(
        scratch.runner(),
        phase_set_replacing(PhaseName::Project, blocking)?,
        available_capacity(),
        "redo-concurrent-widening-runner",
    )?;
    let configured_chain = chain(chain_id)?;
    let task = tokio::spawn(async move {
        phase_runner
            .redo(
                &configured_chain,
                RedoPhase::Phase(PhaseName::Project),
                BlockRange::new(1, 1).expect("fixed range"),
                CancellationToken::new(),
            )
            .await
    });
    entered.notified().await;

    sqlx::query(
        "UPDATE chain_phase_state
         SET redo_from_block_number = 0,
             redo_to_block_number = 2,
             redo_current_block_number = NULL,
             redo_current_block_hash = NULL,
             redo_target_block_number = NULL,
             redo_target_block_hash = NULL,
             updated_at = now()
         WHERE chain_id = $1 AND phase_name = 'project' AND redo_in_progress",
    )
    .bind(chain_id)
    .execute(scratch.pool())
    .await?;
    release.notify_one();
    let error = task
        .await?
        .expect_err("the batch must not write progress into the widened redo range");
    assert!(
        error
            .to_string()
            .contains("redo attempt superseded; progress not recorded"),
        "{error}"
    );

    let marker: (bool, Option<i64>, Option<i64>) = sqlx::query_as(
        "SELECT redo_in_progress, redo_from_block_number, redo_to_block_number
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(marker, (true, Some(0), Some(2)));
    scratch.cleanup().await
}

#[tokio::test]
async fn ingest_cursor_records_the_distinct_source_target() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_cursor_target").await?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    let configured_chain = chain("cursor-target-chain")?;
    store
        .update_ingest_cursors(
            configured_chain.sources.as_ref(),
            &PhaseProgress {
                current: Some(BlockMarker::new(1, "cursor-target-block-1")?),
                target: Some(BlockMarker::new(3, "cursor-target-block-3")?),
                ..PhaseProgress::default()
            },
        )
        .await?;

    let cursor: (i64, Option<i64>, Option<i64>) = sqlx::query_as(
        "
        SELECT next_block_number,
               target_block_number,
               last_processed_block_number
        FROM ingest_cursors
        WHERE chain_id = 'cursor-target-chain'
          AND source_key = 'source'
        ",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(cursor, (2, Some(3), Some(1)));
    scratch.cleanup().await
}

#[tokio::test]
async fn intermediate_ingest_redo_persists_its_loaded_source_boundary() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_redo_loaded_source_boundary").await?;
    let chain_id = "redo-loaded-source-boundary-chain";
    let configured_chain = chain(chain_id)?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain_id).await?;
    store
        .ensure_ingest_sources(chain_id, &configured_chain.sources)
        .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'running', redo_in_progress = true, redo_mode = 'redo',
             redo_previous_phase_status = 'idle',
             redo_from_block_number = 0, redo_to_block_number = 300,
             redo_manifest_authority_fingerprint = (
                 SELECT encode(
                     public.digest(
                         COALESCE(
                             jsonb_agg(
                                 manifest_payload - 'normalizer_version'
                                 ORDER BY namespace, source_family
                             )::text,
                             '[]'
                         ),
                         'sha256'
                     ),
                     'hex'
                 )
                 FROM manifest_versions
                 WHERE chain_id = $1 AND rollout_status = 'active'
             ),
             started_at = now(), finished_at = NULL, updated_at = now()
         WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(chain_id)
    .execute(scratch.pool())
    .await?;
    let loaded = BlockMarker::new(255, format!("{chain_id}-loaded-block-255"))?;
    let summary = BlockMarker::new(255, format!("{chain_id}-summary-block-255"))?;
    let target = BlockMarker::new(300, format!("{chain_id}-block-300"))?;
    store
        .record_progress(
            chain_id,
            PhaseName::Ingest,
            &RunMode::Redo(BlockRange::new(0, 300)?),
            Some(RedoAttemptFence {
                generation: 0,
                execution_range: BlockRange::new(0, 300)?,
            }),
            &PhaseProgress {
                current: Some(summary),
                target: Some(target.clone()),
                source_progress: vec![SourceProgress {
                    source_key: "source".to_owned(),
                    current: Some(loaded.clone()),
                    target: Some(loaded.clone()),
                    redo_loaded_boundary: Some(loaded.clone()),
                }],
                ..PhaseProgress::default()
            },
        )
        .await?;

    let stored: Option<serde_json::Value> = sqlx::query_scalar(
        "SELECT redo_source_boundary_markers
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        stored,
        Some(json!({
            "source": {"number": loaded.number, "hash": loaded.hash}
        })),
        "the load-derived source boundary must be committed with intermediate redo progress"
    );
    let resume = store
        .phase_resume(
            chain_id,
            PhaseName::Ingest,
            &RunMode::Redo(BlockRange::new(0, 300)?),
        )
        .await?;
    assert_eq!(resume.ingest_cursors[0].redo_loaded_boundary, Some(loaded));
    assert_eq!(resume.target, Some(target));
    scratch.cleanup().await
}

#[tokio::test]
async fn missing_mid_run_ingest_cursor_is_not_recreated_over_durable_data() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_missing_mid_run_cursor").await?;
    let chain_id = "missing-mid-run-cursor-chain";
    let configured_chain = chain(chain_id)?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain_id).await?;
    store
        .ensure_ingest_sources(chain_id, &configured_chain.sources)
        .await?;
    assert_eq!(
        store
            .start_phase(chain_id, PhaseName::Ingest, &RunMode::Normal)
            .await?,
        StartDisposition::Started
    );
    seed_lineage(scratch.pool(), chain_id, 1).await?;
    sqlx::query("DELETE FROM ingest_cursors WHERE chain_id = $1 AND source_key = 'source'")
        .bind(chain_id)
        .execute(scratch.pool())
        .await?;

    let marker = BlockMarker::new(1, format!("{chain_id}-block-1"))?;
    let error = store
        .record_ingest_progress(
            chain_id,
            &configured_chain.sources,
            &PhaseProgress {
                current: Some(marker.clone()),
                target: Some(marker),
                ..PhaseProgress::default()
            },
        )
        .await
        .expect_err("a vanished cursor must not be recreated over durable ingest data");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(error.to_string().contains("durable ingest data"), "{error}");
    let cursor_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM ingest_cursors WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(
        cursor_count, 0,
        "the rejected transaction must stay rolled back"
    );
    let phase_state: (String, Option<i64>, Option<i64>) = sqlx::query_as(
        "SELECT phase_status, current_block_number, target_block_number
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        phase_state,
        ("running".to_owned(), None, None),
        "the folded guard must roll back the production phase-state update with the cursor write"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn ingest_redo_does_not_rewrite_a_cursor_past_its_range() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_redo_cursor_past_range").await?;
    let chain_id = "redo-cursor-past-range-chain";
    let configured_chain = chain(chain_id)?;
    let block_2 = BlockMarker::new(2, format!("{chain_id}-block-2"))?;
    seed_completed_ingest_cursor(
        &scratch,
        &configured_chain,
        block_2.clone(),
        block_2.clone(),
        block_2,
    )
    .await?;

    let redo_boundary = BlockMarker::new(1, format!("{chain_id}-replacement-block-1"))?;
    seed_alternative_lineage_marker(scratch.pool(), chain_id, &redo_boundary).await?;
    let redo_progress = PhaseProgress {
        current: Some(redo_boundary.clone()),
        target: Some(redo_boundary.clone()),
        source_progress: vec![SourceProgress {
            source_key: "source".to_owned(),
            current: Some(redo_boundary.clone()),
            target: Some(redo_boundary),
            redo_loaded_boundary: None,
        }],
        ..PhaseProgress::default()
    };
    runner(
        scratch.runner(),
        phase_set_replacing(
            PhaseName::Ingest,
            Arc::new(FunctionPhase {
                name: PhaseName::Ingest,
                handler: Arc::new(move |_| Ok(PhaseBatchOutcome::Complete(redo_progress.clone()))),
            }),
        )?,
        available_capacity(),
        "redo-cursor-past-range-runner",
    )?
    .redo(
        &configured_chain,
        RedoPhase::Phase(PhaseName::Ingest),
        BlockRange::new(1, 1)?,
        CancellationToken::new(),
    )
    .await?;

    let cursor: (i64, Option<i64>, Option<i64>, Option<String>) = sqlx::query_as(
        "SELECT next_block_number, target_block_number,
                last_processed_block_number, last_processed_block_hash
         FROM ingest_cursors
         WHERE chain_id = $1 AND source_key = 'source'",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        cursor,
        (3, Some(2), Some(2), Some(format!("{chain_id}-block-2")),),
        "redoing block 1 must not pair its replacement hash with the cursor at block 2"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn ingest_redo_does_not_reconcile_incomplete_source_progress() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_redo_incomplete_source_cursor").await?;
    let chain_id = "redo-incomplete-source-cursor-chain";
    let configured_chain = chain(chain_id)?;
    let block_1 = BlockMarker::new(1, format!("{chain_id}-block-1"))?;
    let block_2 = BlockMarker::new(2, format!("{chain_id}-block-2"))?;
    seed_completed_ingest_cursor(
        &scratch,
        &configured_chain,
        block_2.clone(),
        block_1.clone(),
        block_2.clone(),
    )
    .await?;

    let unverified_block_1 =
        BlockMarker::new(1, format!("{chain_id}-unverified-replacement-block-1"))?;
    seed_alternative_lineage_marker(scratch.pool(), chain_id, &unverified_block_1).await?;
    let redo_progress = PhaseProgress {
        current: Some(block_2.clone()),
        target: Some(block_2.clone()),
        source_progress: vec![SourceProgress {
            source_key: "source".to_owned(),
            current: Some(unverified_block_1),
            target: Some(block_2),
            redo_loaded_boundary: None,
        }],
        ..PhaseProgress::default()
    };
    runner(
        scratch.runner(),
        phase_set_replacing(
            PhaseName::Ingest,
            Arc::new(FunctionPhase {
                name: PhaseName::Ingest,
                handler: Arc::new(move |_| Ok(PhaseBatchOutcome::Complete(redo_progress.clone()))),
            }),
        )?,
        available_capacity(),
        "redo-incomplete-source-cursor-runner",
    )?
    .redo(
        &configured_chain,
        RedoPhase::Phase(PhaseName::Ingest),
        BlockRange::new(1, 2)?,
        CancellationToken::new(),
    )
    .await?;

    let cursor: (i64, Option<i64>, Option<i64>, Option<String>) = sqlx::query_as(
        "SELECT next_block_number, target_block_number,
                last_processed_block_number, last_processed_block_hash
         FROM ingest_cursors
         WHERE chain_id = $1 AND source_key = 'source'",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        cursor,
        (2, Some(2), Some(1), Some(format!("{chain_id}-block-1")),),
        "incomplete redo progress must not replace the cursor's proven boundary hash"
    );
    scratch.cleanup().await
}

async fn seed_completed_ingest_cursor(
    scratch: &ScratchDatabase,
    chain: &ChainConfig,
    summary: BlockMarker,
    source_current: BlockMarker,
    source_target: BlockMarker,
) -> Result<()> {
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(&chain.chain_id).await?;
    store
        .ensure_ingest_sources(&chain.chain_id, &chain.sources)
        .await?;
    seed_readable_lineage(scratch.pool(), &chain.chain_id, summary.number).await?;
    store
        .start_phase(&chain.chain_id, PhaseName::Ingest, &RunMode::Normal)
        .await?;
    let progress = PhaseProgress {
        current: Some(summary.clone()),
        target: Some(summary.clone()),
        live_handoff: Some(summary),
        source_progress: vec![SourceProgress {
            source_key: "source".to_owned(),
            current: Some(source_current),
            target: Some(source_target),
            redo_loaded_boundary: None,
        }],
        ..PhaseProgress::default()
    };
    store
        .record_ingest_progress(&chain.chain_id, &chain.sources, &progress)
        .await?;
    complete_phase_with_lock(
        scratch,
        &store,
        &chain.chain_id,
        PhaseName::Ingest,
        &progress,
    )
    .await?;
    Ok(())
}

async fn seed_alternative_lineage_marker(
    pool: &sqlx::PgPool,
    chain_id: &str,
    marker: &BlockMarker,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state
         )
         VALUES ($1, $2, $3, $4, to_timestamp($4), 'observed')",
    )
    .bind(chain_id)
    .bind(&marker.hash)
    .bind(format!("{chain_id}-block-0"))
    .bind(marker.number)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test]
async fn ingest_cursors_record_independent_source_progress() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_source_cursors").await?;
    let chain = ChainConfig::new(
        "source-cursors-chain",
        vec![
            SourceConfig::new(
                "source-cursors-chain",
                "bulk",
                "test-bulk",
                SeedBasis::BaseSeam,
                0,
                "http://bulk.invalid",
            )?,
            SourceConfig::new(
                "source-cursors-chain",
                "rpc",
                "test-rpc",
                SeedBasis::NewSignatureRange,
                0,
                "http://rpc.invalid",
            )?,
        ],
        true,
    )?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store
        .update_ingest_cursors(
            chain.sources.as_ref(),
            &PhaseProgress {
                current: Some(BlockMarker::new(3, "source-cursors-block-3")?),
                target: Some(BlockMarker::new(9, "source-cursors-block-9")?),
                source_progress: vec![
                    SourceProgress {
                        source_key: "bulk".to_owned(),
                        current: Some(BlockMarker::new(1, "source-cursors-block-1")?),
                        target: Some(BlockMarker::new(5, "source-cursors-block-5")?),
                        redo_loaded_boundary: None,
                    },
                    SourceProgress {
                        source_key: "rpc".to_owned(),
                        current: Some(BlockMarker::new(3, "source-cursors-block-3")?),
                        target: Some(BlockMarker::new(9, "source-cursors-block-9")?),
                        redo_loaded_boundary: None,
                    },
                ],
                ..PhaseProgress::default()
            },
        )
        .await?;

    let cursors: Vec<(String, i64, Option<i64>, Option<i64>)> = sqlx::query_as(
        "
        SELECT source_key,
               next_block_number,
               target_block_number,
               last_processed_block_number
        FROM ingest_cursors
        WHERE chain_id = 'source-cursors-chain'
        ORDER BY source_key
        ",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        cursors,
        vec![
            ("bulk".to_owned(), 2, Some(5), Some(1)),
            ("rpc".to_owned(), 4, Some(9), Some(3)),
        ]
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn changed_ingest_seed_configuration_fails_loudly() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_cursor_seed").await?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    let original = SourceConfig::new(
        "cursor-seed-chain",
        "source",
        "test",
        SeedBasis::BaseSeam,
        0,
        "http://source.invalid",
    )?;
    store
        .update_ingest_cursors(&[original], &PhaseProgress::default())
        .await?;
    let changed = SourceConfig::new(
        "cursor-seed-chain",
        "source",
        "test",
        SeedBasis::NewSignatureRange,
        10,
        "http://source.invalid",
    )?;

    let error = store
        .update_ingest_cursors(&[changed], &PhaseProgress::default())
        .await
        .expect_err("a source key cannot silently change its persisted seed");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(error.to_string().contains("seed configuration"));
    scratch.cleanup().await
}

#[tokio::test]
async fn progress_free_ingest_cursor_rejects_source_kind_change() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_cursor_progress_free_kind").await?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    let original = SourceConfig::new(
        "cursor-kind-chain",
        "source",
        "rpc",
        SeedBasis::EthereumHead,
        0,
        "http://source.invalid",
    )?;
    store
        .update_ingest_cursors(&[original], &PhaseProgress::default())
        .await?;
    let changed = SourceConfig::new(
        "cursor-kind-chain",
        "source",
        "drpc",
        SeedBasis::EthereumHead,
        0,
        "http://source.invalid",
    )?;
    let error = store
        .update_ingest_cursors(&[changed], &PhaseProgress::default())
        .await
        .expect_err("a progress-free cursor still carries immutable source provenance");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(error.to_string().contains("explicit reset"), "{error}");

    let row: (String, i64, Option<i64>) = sqlx::query_as(
        "SELECT source_kind, next_block_number, last_processed_block_number
         FROM ingest_cursors
         WHERE chain_id = 'cursor-kind-chain' AND source_key = 'source'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(row, ("rpc".to_owned(), 0, None));
    scratch.cleanup().await
}

#[tokio::test]
async fn interpret_redo_accepts_a_normalized_equivalent_source_kind() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_redo_normalized_source_kind").await?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    let chain_id = "redo-normalized-source-kind-chain";
    store.initialize_chain(chain_id).await?;
    mark_completed(scratch.pool(), chain_id, PhaseName::Ingest, None).await?;
    mark_completed(
        scratch.pool(),
        chain_id,
        PhaseName::Interpret,
        Some(phase_runner::INTERPRETER_CONTENT_HASH),
    )
    .await?;
    set_phase_extent(scratch.pool(), chain_id, PhaseName::Interpret, 1).await?;
    seed_interpret_redo_presence(scratch.pool(), chain_id, 1).await?;
    let configured = ChainConfig::new(
        chain_id,
        vec![SourceConfig::new(
            chain_id,
            "source",
            "TEST",
            SeedBasis::EthereumHead,
            0,
            "http://source.invalid",
        )?],
        false,
    )?;

    runner(
        scratch.runner(),
        PhaseSet::loopback(),
        available_capacity(),
        "redo-normalized-source-kind-runner",
    )?
    .redo(
        &configured,
        RedoPhase::Phase(PhaseName::Interpret),
        BlockRange::new(0, 1)?,
        CancellationToken::new(),
    )
    .await?;

    scratch.cleanup().await
}

#[tokio::test]
async fn project_redo_rejects_an_incomplete_intake_source_set_before_its_marker() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_project_redo_source_identity").await?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    let chain_id = "project-redo-source-identity-chain";
    store.initialize_chain(chain_id).await?;
    for phase in [PhaseName::Ingest, PhaseName::Interpret, PhaseName::Project] {
        mark_completed(
            scratch.pool(),
            chain_id,
            phase,
            phase
                .writes_derived_data()
                .then_some(phase_runner::INTERPRETER_CONTENT_HASH),
        )
        .await?;
    }
    set_phase_extent(scratch.pool(), chain_id, PhaseName::Project, 1).await?;
    seed_interpret_redo_presence(scratch.pool(), chain_id, 1).await?;
    let configured = ChainConfig::new(
        chain_id,
        vec![SourceConfig::new(
            chain_id,
            "replacement-source",
            "test",
            SeedBasis::EthereumHead,
            0,
            "http://replacement.invalid",
        )?],
        false,
    )?;

    let error = runner(
        scratch.runner(),
        PhaseSet::loopback(),
        available_capacity(),
        "project-redo-source-identity-runner",
    )?
    .redo(
        &configured,
        RedoPhase::Phase(PhaseName::Project),
        BlockRange::new(0, 1)?,
        CancellationToken::new(),
    )
    .await
    .expect_err("Project redo must reject an incomplete intake source set");
    let redo_in_progress: bool = sqlx::query_scalar(
        "SELECT redo_in_progress FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;

    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(
        error.to_string().contains("has no matching cursor"),
        "{error}"
    );
    assert!(!redo_in_progress);
    scratch.cleanup().await
}

#[tokio::test]
async fn ingest_summary_rolls_back_when_cursor_persistence_fails() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_atomic_ingest_progress").await?;
    let chain_id = "atomic-ingest-progress-chain";
    let configured_chain = chain(chain_id)?;
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain_id).await?;
    store
        .start_phase(chain_id, PhaseName::Ingest, &RunMode::Normal)
        .await?;
    sqlx::raw_sql(
        "
        CREATE FUNCTION reject_atomic_ingest_cursor()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RAISE EXCEPTION 'forced cursor persistence failure'
                USING ERRCODE = '23514';
        END
        $$;
        CREATE TRIGGER reject_atomic_ingest_cursor
        BEFORE INSERT OR UPDATE ON ingest_cursors
        FOR EACH ROW
        EXECUTE FUNCTION reject_atomic_ingest_cursor();
        ",
    )
    .execute(scratch.pool())
    .await?;
    let current = BlockMarker::new(1, format!("{chain_id}-block-1"))?;
    let target = BlockMarker::new(3, format!("{chain_id}-block-3"))?;
    let progress = PhaseProgress {
        current: Some(current.clone()),
        target: Some(target.clone()),
        source_progress: vec![SourceProgress {
            source_key: "source".to_owned(),
            current: Some(current),
            target: Some(target),
            redo_loaded_boundary: None,
        }],
        ..PhaseProgress::default()
    };
    store
        .record_ingest_progress(chain_id, &configured_chain.sources, &progress)
        .await
        .expect_err("cursor failure must roll back the ingest summary");

    let position: (Option<i64>, Option<i64>, Option<i64>) = sqlx::query_as(
        "SELECT current_block_number, target_block_number, live_handoff_block_number
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'ingest'",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(position, (None, None, None));
    let cursor_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM ingest_cursors WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(cursor_count, 0);
    scratch.cleanup().await
}

#[tokio::test]
async fn verify_phase_records_its_trust_level() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_verify_level").await?;
    let phases = PhaseName::ALL.map(|name| {
        Arc::new(FunctionPhase {
            name,
            handler: Arc::new(move |_| {
                let progress = match name {
                    PhaseName::Ingest => {
                        let marker = BlockMarker::new(0, "verify-level-block-0")?;
                        PhaseProgress {
                            current: Some(marker.clone()),
                            target: Some(marker.clone()),
                            live_handoff: Some(marker),
                            ..PhaseProgress::default()
                        }
                    }
                    PhaseName::Verify => PhaseProgress {
                        verification_level: Some(VerificationLevel::NodeChecked),
                        ..PhaseProgress::default()
                    },
                    _ => PhaseProgress::default(),
                };
                Ok(PhaseBatchOutcome::Complete(progress))
            }),
        }) as Arc<dyn Phase>
    });
    runner(
        scratch.runner(),
        PhaseSet::new(phases)?,
        available_capacity(),
        "verify-level-runner",
    )?
    .run_chain(&chain("verify-level-chain")?, CancellationToken::new())
    .await?;

    let level: Option<String> = sqlx::query_scalar(
        "
        SELECT verification_level
        FROM chain_phase_state
        WHERE chain_id = 'verify-level-chain'
          AND phase_name = 'verify'
        ",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(level.as_deref(), Some("node_checked"));
    scratch.cleanup().await
}

#[tokio::test]
async fn verify_phase_cannot_publish_chain_heads() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_verify_heads").await?;
    let chain_id = "verify-heads-chain";
    seed_identified_lineage(scratch.pool(), chain_id, 2).await?;
    let ingest_heads = HeadMarkers {
        latest: BlockMarker::new(1, format!("{chain_id}-block-1"))?,
        safe: None,
        finalized: None,
    };
    let verify_heads = HeadMarkers {
        latest: BlockMarker::new(2, format!("{chain_id}-block-2"))?,
        safe: None,
        finalized: None,
    };
    let phases = PhaseName::ALL.map(|name| {
        let ingest_heads = ingest_heads.clone();
        let verify_heads = verify_heads.clone();
        Arc::new(FunctionPhase {
            name,
            handler: Arc::new(move |_| {
                let progress = match name {
                    PhaseName::Ingest => PhaseProgress {
                        current: Some(ingest_heads.latest.clone()),
                        target: Some(ingest_heads.latest.clone()),
                        live_handoff: Some(ingest_heads.latest.clone()),
                        heads: Some(ingest_heads.clone()),
                        ..PhaseProgress::default()
                    },
                    PhaseName::Verify => PhaseProgress {
                        heads: Some(verify_heads.clone()),
                        verification_level: Some(VerificationLevel::QuickSynced),
                        ..PhaseProgress::default()
                    },
                    _ => PhaseProgress::default(),
                };
                Ok(PhaseBatchOutcome::Complete(progress))
            }),
        }) as Arc<dyn Phase>
    });

    let error = runner(
        scratch.runner(),
        PhaseSet::new(phases)?,
        available_capacity(),
        "verify-heads-runner",
    )?
    .run_chain(&chain(chain_id)?, CancellationToken::new())
    .await
    .expect_err("verify must not have a chain-head write path");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(error.to_string().contains("cannot publish chain heads"));

    let latest: (i64, String) = sqlx::query_as(
        "
        SELECT latest_block_number, latest_block_hash
        FROM chain_heads
        WHERE chain_id = $1
        ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(latest, (1, format!("{chain_id}-block-1")));
    scratch.cleanup().await
}

fn runner(
    database: phase_runner::database::RunnerDatabase,
    phases: PhaseSet,
    capacity: CapacityGuard,
    instance_id: &str,
) -> RunnerResult<PhaseRunner> {
    PhaseRunner::new(database, phases, capacity, instance_id, test_timing())
}

async fn complete_phase_with_lock(
    scratch: &ScratchDatabase,
    store: &PhaseStore,
    chain_id: &str,
    phase: PhaseName,
    progress: &PhaseProgress,
) -> RunnerResult<()> {
    let mut phase_lock =
        PhaseLock::acquire(scratch.writer_connect_options(), chain_id, phase).await?;
    let result = store
        .complete_phase_with_lock(&mut phase_lock, chain_id, phase, progress)
        .await;
    let release = phase_lock.release().await;
    match (result, release) {
        (Ok(()), Ok(())) => Ok(()),
        (Ok(()), Err(error)) | (Err(error), Ok(())) => Err(error),
        (Err(error), Err(_release_error)) => Err(error),
    }
}

fn test_timing() -> TimingConfig {
    TimingConfig {
        initial_backoff: Duration::from_millis(1),
        maximum_backoff: Duration::from_millis(4),
        live_poll_interval: Duration::from_millis(1),
    }
}

fn chain(chain_id: &str) -> RunnerResult<ChainConfig> {
    ChainConfig::new(
        chain_id,
        vec![SourceConfig::new(
            chain_id,
            "source",
            "test",
            SeedBasis::EthereumHead,
            0,
            "http://source.invalid",
        )?],
        false,
    )
}

async fn seed_identified_lineage(pool: &sqlx::PgPool, chain_id: &str, through: i64) -> Result<()> {
    let configured_chain = chain(chain_id)?;
    PhaseStore::new(pool.clone())
        .ensure_ingest_sources(chain_id, &configured_chain.sources)
        .await?;
    seed_lineage(pool, chain_id, through).await
}

fn available_capacity() -> CapacityGuard {
    CapacityGuard::new(CapacityConfig::default(), Arc::new(AlwaysAvailable))
}

async fn settle_active_rows_for_removed_chain(scratch: &ScratchDatabase) -> Result<()> {
    let runtime = RuntimeConfig::new(
        "removed-chain-settlement-runner",
        vec![chain("retained-chain")?],
        CapacityConfig::default(),
        test_timing(),
    )?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    Arc::new(runner(
        scratch.runner(),
        complete_phase_set(None),
        available_capacity(),
        "removed-chain-settlement-runner",
    )?)
    .run(&runtime, cancellation)
    .await?;
    Ok(())
}

struct AlwaysAvailable;

impl CapacityProbe for AlwaysAvailable {
    fn measure<'a>(
        &'a self,
        _pool: &'a sqlx::PgPool,
        _writable_path: &'a std::path::Path,
    ) -> CapacityFuture<'a> {
        Box::pin(async {
            Ok(CapacityMeasurement {
                database_size_bytes: 0,
                free_disk_bytes: u64::MAX,
            })
        })
    }
}

#[derive(Default)]
struct GatedCapacityProbe {
    calls: AtomicUsize,
    breached: Notify,
    released: AtomicBool,
}

impl CapacityProbe for GatedCapacityProbe {
    fn measure<'a>(
        &'a self,
        _pool: &'a sqlx::PgPool,
        _writable_path: &'a std::path::Path,
    ) -> CapacityFuture<'a> {
        Box::pin(async move {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                self.breached.notify_one();
            }
            let free_disk_bytes = if self.released.load(Ordering::SeqCst) {
                u64::MAX
            } else {
                0
            };
            Ok(CapacityMeasurement {
                database_size_bytes: 0,
                free_disk_bytes,
            })
        })
    }
}

struct FunctionPhase {
    name: PhaseName,
    handler: Arc<dyn Fn(PhaseContext) -> RunnerResult<PhaseBatchOutcome> + Send + Sync>,
}

impl Phase for FunctionPhase {
    fn name(&self) -> PhaseName {
        self.name
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        let result = (self.handler)(context);
        Box::pin(async move { result })
    }
}

struct RecordingRedoPhase {
    name: PhaseName,
    calls: Arc<Mutex<Vec<(String, PhaseName)>>>,
    fail_chain: Option<String>,
}

impl Phase for RecordingRedoPhase {
    fn name(&self) -> PhaseName {
        self.name
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            self.calls
                .lock()
                .expect("recorded calls lock")
                .push((context.chain_id.clone(), self.name));
            if context.chain_id == "redo-all-completed-chain" {
                let count = context.sources.len();
                assert_eq!(count, (self.name == PhaseName::Verify) as usize + 1);
            }
            if self.fail_chain.as_deref() == Some(context.chain_id.as_str()) {
                return Err(RunnerError::data_integrity(
                    "fixture failed during all-phase interpret redo",
                ));
            }
            LoopbackPhase::new(self.name).run_batch(context).await
        })
    }
}

struct BlockingPhase {
    name: PhaseName,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl Phase for BlockingPhase {
    fn name(&self) -> PhaseName {
        self.name
    }

    fn run_batch(&self, _context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            self.entered.notify_one();
            self.release.notified().await;
            Ok(PhaseBatchOutcome::Complete(PhaseProgress::default()))
        })
    }
}

fn phase_set_replacing(name: PhaseName, replacement: Arc<dyn Phase>) -> RunnerResult<PhaseSet> {
    let phases = PhaseName::ALL.map(|phase| {
        if phase == name {
            Arc::clone(&replacement)
        } else {
            Arc::new(FunctionPhase {
                name: phase,
                handler: Arc::new(move |_| {
                    Ok(PhaseBatchOutcome::Complete(PhaseProgress {
                        verification_level: (phase == PhaseName::Verify)
                            .then_some(VerificationLevel::QuickSynced),
                        ..PhaseProgress::default()
                    }))
                }),
            }) as Arc<dyn Phase>
        }
    });
    PhaseSet::new(phases)
}

fn complete_phase_set(ingest_heads: Option<HeadMarkers>) -> PhaseSet {
    let phases = PhaseName::ALL.map(|name| complete_phase(name, ingest_heads.clone()));
    PhaseSet::new(phases).expect("phase names are ordered")
}

fn continuous_complete_phase_set(
    heads: HeadMarkers,
    live_calls: Arc<AtomicUsize>,
) -> RunnerResult<PhaseSet> {
    let live = Arc::new(FunctionPhase {
        name: PhaseName::Live,
        handler: Arc::new(move |context| {
            live_calls.fetch_add(1, Ordering::SeqCst);
            let marker = context
                .available_heads
                .as_ref()
                .map(|heads| heads.latest.clone());
            Ok(PhaseBatchOutcome::Complete(PhaseProgress {
                current: marker.clone(),
                target: marker,
                heads: context.available_heads,
                ..PhaseProgress::default()
            }))
        }),
    });
    PhaseSet::with_ingest_interpret_project_and_live(
        complete_phase(PhaseName::Ingest, Some(heads)),
        complete_phase(PhaseName::Interpret, None),
        complete_phase(PhaseName::Project, None),
        complete_phase(PhaseName::Verify, None),
        live,
    )
}

fn complete_phase(name: PhaseName, heads: Option<HeadMarkers>) -> Arc<dyn Phase> {
    Arc::new(FunctionPhase {
        name,
        handler: Arc::new(move |_| {
            let progress = match name {
                PhaseName::Ingest => {
                    let marker = heads.as_ref().map(|heads| heads.latest.clone());
                    PhaseProgress {
                        current: marker.clone(),
                        target: marker.clone(),
                        live_handoff: marker,
                        heads: heads.clone(),
                        estimated_write_bytes: 0,
                        ..PhaseProgress::default()
                    }
                }
                PhaseName::Verify => PhaseProgress {
                    verification_level: Some(VerificationLevel::QuickSynced),
                    ..PhaseProgress::default()
                },
                _ => PhaseProgress::default(),
            };
            Ok(PhaseBatchOutcome::Complete(progress))
        }),
    })
}

fn routing_phase_set(good_live: Arc<Notify>) -> RunnerResult<PhaseSet> {
    let phases = PhaseName::ALL.map(|name| {
        let good_live = Arc::clone(&good_live);
        Arc::new(FunctionPhase {
            name,
            handler: Arc::new(move |context| {
                if context.chain_id == "bad-chain" && name == PhaseName::Ingest {
                    return Err(RunnerError::data_integrity("bad source count"));
                }
                let marker = BlockMarker {
                    number: 0,
                    hash: format!("{}-block-0", context.chain_id),
                };
                let progress = match name {
                    PhaseName::Ingest => PhaseProgress {
                        current: Some(marker.clone()),
                        target: Some(marker.clone()),
                        live_handoff: Some(marker.clone()),
                        heads: Some(HeadMarkers {
                            latest: marker,
                            safe: None,
                            finalized: None,
                        }),
                        estimated_write_bytes: 0,
                        ..PhaseProgress::default()
                    },
                    PhaseName::Verify => PhaseProgress {
                        verification_level: Some(VerificationLevel::QuickSynced),
                        ..PhaseProgress::default()
                    },
                    _ => PhaseProgress::default(),
                };
                if name == PhaseName::Live {
                    good_live.notify_one();
                    Ok(PhaseBatchOutcome::Idle(progress))
                } else {
                    Ok(PhaseBatchOutcome::Complete(progress))
                }
            }),
        }) as Arc<dyn Phase>
    });
    PhaseSet::new(phases)
}

fn panic_routing_phase_set(
    good_live: Arc<Notify>,
    panic_trigger: Arc<Notify>,
) -> RunnerResult<PhaseSet> {
    let phases = PhaseName::ALL.map(|name| {
        if name == PhaseName::Ingest {
            return Arc::new(PanicAfterGoodLiveIngest {
                panic_trigger: Arc::clone(&panic_trigger),
            }) as Arc<dyn Phase>;
        }
        let good_live = Arc::clone(&good_live);
        let panic_trigger = Arc::clone(&panic_trigger);
        Arc::new(FunctionPhase {
            name,
            handler: Arc::new(move |_| {
                if name == PhaseName::Live {
                    panic_trigger.notify_one();
                    good_live.notify_one();
                    Ok(PhaseBatchOutcome::Idle(PhaseProgress::default()))
                } else if name == PhaseName::Verify {
                    Ok(PhaseBatchOutcome::Complete(PhaseProgress {
                        verification_level: Some(VerificationLevel::QuickSynced),
                        ..PhaseProgress::default()
                    }))
                } else {
                    Ok(PhaseBatchOutcome::Complete(PhaseProgress::default()))
                }
            }),
        }) as Arc<dyn Phase>
    });
    PhaseSet::new(phases)
}

struct PanicAfterGoodLiveIngest {
    panic_trigger: Arc<Notify>,
}

impl Phase for PanicAfterGoodLiveIngest {
    fn name(&self) -> PhaseName {
        PhaseName::Ingest
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            if context.chain_id == "bad-chain" {
                self.panic_trigger.notified().await;
                panic!("bad chain phase panic");
            }
            let marker = BlockMarker::new(0, format!("{}-block-0", context.chain_id))?;
            Ok(PhaseBatchOutcome::Complete(PhaseProgress {
                current: Some(marker.clone()),
                target: Some(marker.clone()),
                live_handoff: Some(marker.clone()),
                heads: Some(HeadMarkers {
                    latest: marker,
                    safe: None,
                    finalized: None,
                }),
                ..PhaseProgress::default()
            }))
        })
    }
}

async fn wait_for_phase_status(
    pool: &sqlx::PgPool,
    chain_id: &str,
    phase: PhaseName,
    expected: &str,
) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let status: Option<String> = sqlx::query_scalar(
                "
                SELECT phase_status
                FROM chain_phase_state
                WHERE chain_id = $1
                  AND phase_name = $2
                ",
            )
            .bind(chain_id)
            .bind(phase.as_str())
            .fetch_optional(pool)
            .await?;
            if status.as_deref() == Some(expected) {
                return Result::<()>::Ok(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    Ok(())
}

async fn mark_completed(
    pool: &sqlx::PgPool,
    chain_id: &str,
    phase: PhaseName,
    content_hash: Option<&str>,
) -> Result<()> {
    sqlx::query(
        "
        UPDATE chain_phase_state
        SET phase_status = 'completed',
            input_content_hash = $3,
            current_block_number = COALESCE(current_block_number, 100),
            current_block_hash = COALESCE(current_block_hash, 'test-extent-block-100'),
            target_block_number = COALESCE(target_block_number, 100),
            target_block_hash = COALESCE(target_block_hash, 'test-extent-block-100'),
            started_at = now(),
            finished_at = now(),
            updated_at = now()
        WHERE chain_id = $1
          AND phase_name = $2
        ",
    )
    .bind(chain_id)
    .bind(phase.as_str())
    .bind(content_hash)
    .execute(pool)
    .await?;
    Ok(())
}

async fn set_phase_extent(
    pool: &sqlx::PgPool,
    chain_id: &str,
    phase: PhaseName,
    through: i64,
) -> Result<()> {
    sqlx::query(
        "
        UPDATE chain_phase_state
        SET current_block_number = $3,
            current_block_hash = $4,
            target_block_number = $3,
            target_block_hash = $4
        WHERE chain_id = $1
          AND phase_name = $2
        ",
    )
    .bind(chain_id)
    .bind(phase.as_str())
    .bind(through)
    .bind(format!("{chain_id}-block-{through}"))
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_interpret_redo_presence(
    pool: &sqlx::PgPool,
    chain_id: &str,
    through: i64,
) -> Result<()> {
    seed_readable_lineage(pool, chain_id, through).await?;
    sqlx::query(
        "
        INSERT INTO ingest_cursors (
            chain_id, source_key, source_kind, seed_basis,
            start_block_number, next_block_number, target_block_number,
            last_processed_block_number, last_processed_block_hash
        )
        VALUES ($1, 'source', 'test', 'ethereum_head', 0, $2 + 1, $2, $2, $3)
        ON CONFLICT (chain_id, source_key) DO UPDATE
        SET source_kind = EXCLUDED.source_kind,
            seed_basis = EXCLUDED.seed_basis,
            start_block_number = EXCLUDED.start_block_number,
            next_block_number = EXCLUDED.next_block_number,
            target_block_number = EXCLUDED.target_block_number,
            last_processed_block_number = EXCLUDED.last_processed_block_number,
            last_processed_block_hash = EXCLUDED.last_processed_block_hash
        ",
    )
    .bind(chain_id)
    .bind(through)
    .bind(format!("{chain_id}-block-{through}"))
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_readable_lineage(pool: &sqlx::PgPool, chain_id: &str, through: i64) -> Result<()> {
    seed_lineage(pool, chain_id, through).await?;
    sqlx::query("UPDATE chain_lineage SET canonicality_state = 'canonical' WHERE chain_id = $1")
        .bind(chain_id)
        .execute(pool)
        .await?;
    Ok(())
}
