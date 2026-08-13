#[allow(dead_code)]
mod support;

use std::{
    future,
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::Result;
use phase_runner::{
    capacity::{CapacityFuture, CapacityGuard, CapacityMeasurement, CapacityProbe},
    config::{CapacityConfig, ChainConfig, RuntimeConfig, SeedBasis, SourceConfig, TimingConfig},
    error::{ErrorKind, RunnerError, RunnerResult},
    heads::{BlockMarker, HeadMarkers, publish_heads},
    phase::{
        BlockRange, Phase, PhaseBatchOutcome, PhaseContext, PhaseFuture, PhaseName, PhaseProgress,
        PhaseSet, RunMode, SourceProgress, VerificationLevel,
    },
    runner::{PhaseRunner, RedoPhase},
    state::{PhaseStore, StartDisposition},
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use support::{ScratchDatabase, seed_lineage};

#[tokio::test]
async fn crash_during_hash_redo_blocks_normal_resume_and_still_demands_full_range() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_crash_hash_redo").await?;
    let chain_id = "crash-hash-redo-chain";
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain_id).await?;
    seed_ingest_extent(scratch.pool(), chain_id, 0, 9).await?;
    mark_phase_with_extent(
        scratch.pool(),
        chain_id,
        PhaseName::Interpret,
        9,
        Some("keccak256:older-binary"),
    )
    .await?;

    let second_batch_entered = Arc::new(Notify::new());
    let crashing_phase = Arc::new(CrashBetweenBatchesPhase {
        name: PhaseName::Interpret,
        calls: AtomicUsize::new(0),
        second_batch_entered: Arc::clone(&second_batch_entered),
        first_progress: progress_at(4, 9, "crash-hash-redo"),
    });
    let first_runner = runner(
        scratch.runner(),
        phase_set_replacing(PhaseName::Interpret, crashing_phase)?,
        "crash-hash-redo-runner",
    )?;
    let first_chain = chain(chain_id, SeedBasis::BaseSeam)?;
    let task = tokio::spawn(async move {
        first_runner
            .redo(
                &first_chain,
                RedoPhase::Phase(PhaseName::Interpret),
                BlockRange::new(0, 9).expect("fixed range"),
                CancellationToken::new(),
            )
            .await
    });
    second_batch_entered.notified().await;
    task.abort();
    assert!(
        task.await
            .expect_err("simulated crash must abort the task")
            .is_cancelled()
    );
    wait_for_no_advisory_locks(scratch.pool()).await?;

    let crashed: (String, Option<String>, bool, Option<i64>) = sqlx::query_as(
        "
        SELECT phase_status,
               input_content_hash,
               redo_in_progress,
               redo_current_block_number
        FROM chain_phase_state
        WHERE chain_id = $1
          AND phase_name = 'interpret'
        ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        crashed,
        (
            "running".to_owned(),
            Some(phase_runner::INTERPRETER_CONTENT_HASH.to_owned()),
            true,
            Some(4)
        )
    );

    sqlx::query(
        "UPDATE chain_phase_state
         SET input_content_hash = 'keccak256:intermediate-binary'
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain_id)
    .execute(scratch.pool())
    .await?;

    let normal_error = store
        .start_phase(chain_id, PhaseName::Interpret, &RunMode::Normal)
        .await
        .expect_err("normal mode must not resume an interrupted redo");
    assert_eq!(normal_error.kind(), ErrorKind::InvalidTransition);
    assert!(normal_error.to_string().contains("phase-runner redo"));
    assert!(normal_error.to_string().contains("--from-block 0"));
    assert!(normal_error.to_string().contains("--to-block 9"));

    let recovery_runner = runner(
        scratch.runner(),
        PhaseSet::loopback(),
        "crash-hash-redo-recovery",
    )?;
    let partial_error = recovery_runner
        .redo(
            &chain(chain_id, SeedBasis::BaseSeam)?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(5, 9)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("a crashed hash redo must still require the full recorded range");
    assert_eq!(partial_error.kind(), ErrorKind::ContentHashMismatch);
    assert!(partial_error.to_string().contains("full range 0..=9"));

    let recovered_from = Arc::new(AtomicI64::new(i64::MIN));
    let phase_recovered_from = Arc::clone(&recovered_from);
    let recovery_phase = Arc::new(FunctionPhase {
        name: PhaseName::Interpret,
        handler: Arc::new(move |context| {
            phase_recovered_from.store(
                context.resume.current.map_or(-1, |marker| marker.number),
                Ordering::SeqCst,
            );
            Ok(PhaseBatchOutcome::Complete(PhaseProgress::default()))
        }),
    });
    runner(
        scratch.runner(),
        phase_set_replacing(PhaseName::Interpret, recovery_phase)?,
        "crash-hash-redo-changed-binary-recovery",
    )?
    .redo(
        &chain(chain_id, SeedBasis::BaseSeam)?,
        RedoPhase::Phase(PhaseName::Interpret),
        BlockRange::new(0, 9)?,
        CancellationToken::new(),
    )
    .await?;
    assert_eq!(recovered_from.load(Ordering::SeqCst), -1);
    let recovered: (String, Option<String>, bool) = sqlx::query_as(
        "
        SELECT phase_status, input_content_hash, redo_in_progress
        FROM chain_phase_state
        WHERE chain_id = $1
          AND phase_name = 'interpret'
        ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        recovered,
        (
            "completed".to_owned(),
            Some(phase_runner::INTERPRETER_CONTENT_HASH.to_owned()),
            false
        )
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn crashed_redo_progress_never_replaces_the_normal_resume_cursor() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_redo_cursor_isolation").await?;
    let chain_id = "redo-cursor-isolation-chain";
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain_id).await?;
    seed_ingest_extent(scratch.pool(), chain_id, 0, 9).await?;
    mark_phase_with_extent(
        scratch.pool(),
        chain_id,
        PhaseName::Interpret,
        9,
        Some(phase_runner::INTERPRETER_CONTENT_HASH),
    )
    .await?;

    let second_batch_entered = Arc::new(Notify::new());
    let crashing_phase = Arc::new(CrashBetweenBatchesPhase {
        name: PhaseName::Interpret,
        calls: AtomicUsize::new(0),
        second_batch_entered: Arc::clone(&second_batch_entered),
        first_progress: progress_at(5, 6, "redo-cursor-isolation"),
    });
    let first_runner = runner(
        scratch.runner(),
        phase_set_replacing(PhaseName::Interpret, crashing_phase)?,
        "redo-cursor-isolation-runner",
    )?;
    let first_chain = chain(chain_id, SeedBasis::BaseSeam)?;
    let task = tokio::spawn(async move {
        first_runner
            .redo(
                &first_chain,
                RedoPhase::Phase(PhaseName::Interpret),
                BlockRange::new(5, 6).expect("fixed range"),
                CancellationToken::new(),
            )
            .await
    });
    second_batch_entered.notified().await;
    task.abort();
    assert!(
        task.await
            .expect_err("simulated crash must abort the task")
            .is_cancelled()
    );
    wait_for_no_advisory_locks(scratch.pool()).await?;

    let during_crash: (Option<i64>, Option<i64>, bool) = sqlx::query_as(
        "
        SELECT current_block_number,
               redo_current_block_number,
               redo_in_progress
        FROM chain_phase_state
        WHERE chain_id = $1
          AND phase_name = 'interpret'
        ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(during_crash, (Some(9), Some(5), true));

    let normal_error = store
        .start_phase(chain_id, PhaseName::Interpret, &RunMode::Normal)
        .await
        .expect_err("the interrupted redo marker must block normal mode");
    assert!(normal_error.to_string().contains("phase-runner redo"));

    let recovered_from = Arc::new(AtomicI64::new(-1));
    let phase_recovered_from = Arc::clone(&recovered_from);
    let recovery_phase = Arc::new(FunctionPhase {
        name: PhaseName::Interpret,
        handler: Arc::new(move |context| {
            phase_recovered_from.store(
                context.resume.current.map_or(-1, |marker| marker.number),
                Ordering::SeqCst,
            );
            Ok(PhaseBatchOutcome::Complete(PhaseProgress::default()))
        }),
    });
    runner(
        scratch.runner(),
        phase_set_replacing(PhaseName::Interpret, recovery_phase)?,
        "redo-cursor-isolation-recovery",
    )?
    .redo(
        &chain(chain_id, SeedBasis::BaseSeam)?,
        RedoPhase::Phase(PhaseName::Interpret),
        BlockRange::new(5, 6)?,
        CancellationToken::new(),
    )
    .await?;
    assert_eq!(recovered_from.load(Ordering::SeqCst), 5);

    assert_eq!(
        store
            .start_phase(chain_id, PhaseName::Interpret, &RunMode::Normal)
            .await?,
        StartDisposition::AlreadyCompleted
    );
    let resume = store
        .phase_resume(chain_id, PhaseName::Interpret, &RunMode::Normal)
        .await?;
    assert_eq!(resume.current.map(|marker| marker.number), Some(9));
    scratch.cleanup().await
}

#[tokio::test]
async fn failed_redo_keeps_its_durable_marker_and_blocks_normal_resume() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_failed_redo_marker").await?;
    let chain_id = "failed-redo-marker-chain";
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain_id).await?;
    seed_ingest_extent(scratch.pool(), chain_id, 0, 9).await?;
    mark_phase_with_extent(
        scratch.pool(),
        chain_id,
        PhaseName::Interpret,
        9,
        Some(phase_runner::INTERPRETER_CONTENT_HASH),
    )
    .await?;

    let calls = Arc::new(AtomicUsize::new(0));
    let phase_calls = Arc::clone(&calls);
    let phase = Arc::new(FunctionPhase {
        name: PhaseName::Interpret,
        handler: Arc::new(move |_| {
            if phase_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(PhaseBatchOutcome::Continue(progress_at(
                    5,
                    6,
                    "failed-redo-marker",
                )))
            } else {
                Err(RunnerError::data_integrity(
                    "simulated terminal redo batch failure",
                ))
            }
        }),
    });
    let error = runner(
        scratch.runner(),
        phase_set_replacing(PhaseName::Interpret, phase)?,
        "failed-redo-marker-runner",
    )?
    .redo(
        &chain(chain_id, SeedBasis::BaseSeam)?,
        RedoPhase::Phase(PhaseName::Interpret),
        BlockRange::new(5, 6)?,
        CancellationToken::new(),
    )
    .await
    .expect_err("the second redo batch must fail");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);

    let state: (String, bool, Option<i64>, Option<i64>, Option<String>) = sqlx::query_as(
        "
        SELECT phase_status,
               redo_in_progress,
               redo_current_block_number,
               current_block_number,
               last_error
        FROM chain_phase_state
        WHERE chain_id = $1
          AND phase_name = 'interpret'
        ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(state.0, "running");
    assert!(state.1);
    assert_eq!(state.2, Some(5));
    assert_eq!(state.3, Some(9));
    assert_eq!(
        state.4.as_deref(),
        Some("simulated terminal redo batch failure")
    );

    let normal_error = store
        .start_phase(chain_id, PhaseName::Interpret, &RunMode::Normal)
        .await
        .expect_err("normal mode must refuse a failed partial redo");
    assert!(normal_error.to_string().contains("phase-runner redo"));

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let cancellation_error = runner(
        scratch.runner(),
        PhaseSet::loopback(),
        "failed-redo-marker-cancelled-retry",
    )?
    .redo(
        &chain(chain_id, SeedBasis::BaseSeam)?,
        RedoPhase::Phase(PhaseName::Interpret),
        BlockRange::new(5, 6)?,
        cancelled,
    )
    .await
    .expect_err("cancelling a retry of a durable redo marker must fail");
    assert!(cancellation_error.to_string().contains("is incomplete"));
    assert!(
        cancellation_error
            .to_string()
            .contains("--from-block 5 --to-block 9")
    );

    runner(
        scratch.runner(),
        PhaseSet::loopback(),
        "failed-redo-marker-recovery",
    )?
    .redo(
        &chain(chain_id, SeedBasis::BaseSeam)?,
        RedoPhase::Phase(PhaseName::Interpret),
        BlockRange::new(5, 6)?,
        CancellationToken::new(),
    )
    .await?;
    let recovered: (bool, Option<String>) = sqlx::query_as(
        "
        SELECT redo_in_progress, last_error
        FROM chain_phase_state
        WHERE chain_id = $1
          AND phase_name = 'interpret'
        ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(recovered, (false, None));
    scratch.cleanup().await
}

#[tokio::test]
async fn cancelled_redo_before_start_is_nonzero_without_a_blocked_phase_claim() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_cancelled_redo_before_start").await?;
    let chain_id = "cancelled-redo-before-start-chain";
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain_id).await?;
    seed_ingest_extent(scratch.pool(), chain_id, 0, 9).await?;
    mark_phase_with_extent(
        scratch.pool(),
        chain_id,
        PhaseName::Interpret,
        9,
        Some(phase_runner::INTERPRETER_CONTENT_HASH),
    )
    .await?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();

    let error = runner(
        scratch.runner(),
        PhaseSet::loopback(),
        "cancelled-redo-before-start-runner",
    )?
    .redo(
        &chain(chain_id, SeedBasis::BaseSeam)?,
        RedoPhase::Phase(PhaseName::Interpret),
        BlockRange::new(5, 6)?,
        cancellation,
    )
    .await
    .expect_err("a cancelled redo command must return a nonzero result");
    assert!(error.to_string().contains("cancelled before it started"));
    assert!(
        error
            .to_string()
            .contains("no unfinished redo was recorded")
    );
    assert!(!error.to_string().contains("remains blocked"));
    scratch.cleanup().await
}

#[tokio::test]
async fn out_of_range_redo_progress_constraint_is_terminal_and_recorded() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_redo_constraint").await?;
    let chain_id = "redo-constraint-chain";
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain_id).await?;
    seed_ingest_extent(scratch.pool(), chain_id, 0, 9).await?;
    mark_phase_with_extent(
        scratch.pool(),
        chain_id,
        PhaseName::Interpret,
        9,
        Some(phase_runner::INTERPRETER_CONTENT_HASH),
    )
    .await?;

    let calls = Arc::new(AtomicUsize::new(0));
    let phase_calls = Arc::clone(&calls);
    let phase = Arc::new(FunctionPhase {
        name: PhaseName::Interpret,
        handler: Arc::new(move |_| {
            phase_calls.fetch_add(1, Ordering::SeqCst);
            Ok(PhaseBatchOutcome::Continue(progress_at(
                4,
                6,
                "redo-constraint",
            )))
        }),
    });
    let error = tokio::time::timeout(
        Duration::from_secs(2),
        runner(
            scratch.runner(),
            phase_set_replacing(PhaseName::Interpret, phase)?,
            "redo-constraint-runner",
        )?
        .redo(
            &chain(chain_id, SeedBasis::BaseSeam)?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(5, 6)?,
            CancellationToken::new(),
        ),
    )
    .await
    .expect("a constraint violation must not enter retry backoff")
    .expect_err("out-of-range redo progress must fail");
    let error_message = error.to_string();
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(!error.is_retryable());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(error_message.contains("failed to record redo progress"));

    let state: (String, bool, Option<String>) = sqlx::query_as(
        "
        SELECT phase_status, redo_in_progress, last_error
        FROM chain_phase_state
        WHERE chain_id = $1
          AND phase_name = 'interpret'
        ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(state.0, "running");
    assert!(state.1);
    assert_eq!(state.2.as_deref(), Some(error_message.as_str()));
    scratch.cleanup().await
}

#[tokio::test]
async fn cancelled_redo_is_incomplete_and_returns_the_required_rerun_command() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_cancelled_redo").await?;
    let chain_id = "cancelled-redo-chain";
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain_id).await?;
    seed_ingest_extent(scratch.pool(), chain_id, 0, 9).await?;
    mark_phase_with_extent(
        scratch.pool(),
        chain_id,
        PhaseName::Interpret,
        9,
        Some(phase_runner::INTERPRETER_CONTENT_HASH),
    )
    .await?;

    let entered = Arc::new(Notify::new());
    let phase_entered = Arc::clone(&entered);
    let phase = Arc::new(FunctionPhase {
        name: PhaseName::Interpret,
        handler: Arc::new(move |_| {
            phase_entered.notify_one();
            Ok(PhaseBatchOutcome::Continue(progress_at(
                5,
                6,
                "cancelled-redo",
            )))
        }),
    });
    let cancellation = CancellationToken::new();
    let run_cancellation = cancellation.clone();
    let configured_chain = chain(chain_id, SeedBasis::BaseSeam)?;
    let redo_runner = runner(
        scratch.runner(),
        phase_set_replacing(PhaseName::Interpret, phase)?,
        "cancelled-redo-runner",
    )?;
    let task = tokio::spawn(async move {
        redo_runner
            .redo(
                &configured_chain,
                RedoPhase::Phase(PhaseName::Interpret),
                BlockRange::new(5, 6).expect("fixed range"),
                run_cancellation,
            )
            .await
    });
    entered.notified().await;
    cancellation.cancel();
    let error = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("cancelled redo did not stop")
        .expect("cancelled redo task panicked")
        .expect_err("a cancelled redo must return a nonzero result");
    let error_message = error.to_string();
    let instruction = "rerun `phase-runner redo --chain cancelled-redo-chain --phase interpret \
                       --from-block 5 --to-block 9`";
    assert_eq!(error.kind(), ErrorKind::InvalidTransition);
    assert!(error_message.contains("is incomplete"));
    assert!(error_message.contains("the phase remains blocked from normal restart"));
    assert!(error_message.contains(instruction));

    let state: (String, bool, Option<String>) = sqlx::query_as(
        "
        SELECT phase_status, redo_in_progress, last_error
        FROM chain_phase_state
        WHERE chain_id = $1
          AND phase_name = 'interpret'
        ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(state.0, "running");
    assert!(state.1);
    assert_eq!(state.2.as_deref(), Some(error_message.as_str()));

    let normal_error = store
        .start_phase(chain_id, PhaseName::Interpret, &RunMode::Normal)
        .await
        .expect_err("normal restart must remain blocked");
    assert!(normal_error.to_string().contains(instruction));
    scratch.cleanup().await
}

#[tokio::test]
async fn cancelled_redo_during_retry_backoff_is_incomplete() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_cancelled_redo_backoff").await?;
    let chain_id = "cancelled-redo-backoff-chain";
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain_id).await?;
    seed_ingest_extent(scratch.pool(), chain_id, 0, 9).await?;
    mark_phase_with_extent(
        scratch.pool(),
        chain_id,
        PhaseName::Interpret,
        9,
        Some(phase_runner::INTERPRETER_CONTENT_HASH),
    )
    .await?;

    let failed_once = Arc::new(Notify::new());
    let phase_failed_once = Arc::clone(&failed_once);
    let phase = Arc::new(FunctionPhase {
        name: PhaseName::Interpret,
        handler: Arc::new(move |_| {
            phase_failed_once.notify_one();
            Err(RunnerError::transient(
                "simulated transient redo failure before backoff",
            ))
        }),
    });
    let cancellation = CancellationToken::new();
    let run_cancellation = cancellation.clone();
    let configured_chain = chain(chain_id, SeedBasis::BaseSeam)?;
    let redo_runner = runner_with_timing(
        scratch.runner(),
        phase_set_replacing(PhaseName::Interpret, phase)?,
        "cancelled-redo-backoff-runner",
        TimingConfig {
            initial_backoff: Duration::from_secs(1),
            maximum_backoff: Duration::from_secs(1),
            live_poll_interval: Duration::from_millis(1),
        },
    )?;
    let task = tokio::spawn(async move {
        redo_runner
            .redo(
                &configured_chain,
                RedoPhase::Phase(PhaseName::Interpret),
                BlockRange::new(5, 6).expect("fixed range"),
                run_cancellation,
            )
            .await
    });
    failed_once.notified().await;
    wait_for_no_advisory_locks(scratch.pool()).await?;
    cancellation.cancel();

    let error = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("redo retry backoff did not observe cancellation")
        .expect("redo retry task panicked")
        .expect_err("cancellation during redo retry backoff must return a nonzero result");
    assert!(error.to_string().contains("is incomplete"));
    assert!(
        error
            .to_string()
            .contains("the phase remains blocked from normal restart")
    );
    assert!(error.to_string().contains("--from-block 5 --to-block 9"));
    scratch.cleanup().await
}

#[tokio::test]
async fn redo_range_must_fit_the_recorded_phase_extent() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_redo_extent").await?;
    let chain_id = "redo-extent-chain";
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain_id).await?;
    seed_ingest_extent(scratch.pool(), chain_id, 2, 10).await?;
    mark_phase_with_extent(
        scratch.pool(),
        chain_id,
        PhaseName::Interpret,
        6,
        Some(phase_runner::INTERPRETER_CONTENT_HASH),
    )
    .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET target_block_number = 10,
             target_block_hash = $2
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain_id)
    .bind(format!("{chain_id}-extent-block-10"))
    .execute(scratch.pool())
    .await?;
    let runner = runner(scratch.runner(), PhaseSet::loopback(), "redo-extent-runner")?;
    for range in [BlockRange::new(1, 3)?, BlockRange::new(2, 7)?] {
        let error = runner
            .redo(
                &chain(chain_id, SeedBasis::BaseSeam)?,
                RedoPhase::Phase(PhaseName::Interpret),
                range,
                CancellationToken::new(),
            )
            .await
            .expect_err("redo outside the recorded phase extent must fail");
        assert_eq!(error.kind(), ErrorKind::DataIntegrity);
        assert!(error.to_string().contains("recorded extent 2..=6"));
    }
    scratch.cleanup().await
}

#[tokio::test]
async fn interpret_redo_stops_at_the_recorded_processed_head() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_redo_processed_head").await?;
    let chain_id = "redo-processed-head-chain";
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain_id).await?;
    seed_ingest_extent(scratch.pool(), chain_id, 0, 9).await?;
    mark_phase_with_extent(
        scratch.pool(),
        chain_id,
        PhaseName::Interpret,
        4,
        Some(phase_runner::INTERPRETER_CONTENT_HASH),
    )
    .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET target_block_number = 9,
             target_block_hash = $2
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain_id)
    .bind(format!("{chain_id}-extent-block-9"))
    .execute(scratch.pool())
    .await?;

    let observed_head = Arc::new(AtomicI64::new(i64::MIN));
    let phase_observed_head = Arc::clone(&observed_head);
    let phase = Arc::new(FunctionPhase {
        name: PhaseName::Interpret,
        handler: Arc::new(move |context| {
            phase_observed_head.store(
                context
                    .available_heads
                    .expect("interpret redo needs its recorded head")
                    .latest
                    .number,
                Ordering::SeqCst,
            );
            Ok(PhaseBatchOutcome::Complete(PhaseProgress::default()))
        }),
    });
    runner(
        scratch.runner(),
        phase_set_replacing(PhaseName::Interpret, phase)?,
        "redo-processed-head-runner",
    )?
    .redo(
        &chain(chain_id, SeedBasis::BaseSeam)?,
        RedoPhase::Phase(PhaseName::Interpret),
        BlockRange::new(2, 3)?,
        CancellationToken::new(),
    )
    .await?;

    assert_eq!(observed_head.load(Ordering::SeqCst), 4);
    scratch.cleanup().await
}

#[tokio::test]
async fn interpret_redo_rejects_a_removed_persisted_ingest_source_before_writes() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_redo_removed_ingest_source").await?;
    let chain_id = "redo-removed-ingest-source-chain";
    let store = PhaseStore::new(scratch.pool().clone());
    store.initialize_chain(chain_id).await?;
    seed_ingest_extent(scratch.pool(), chain_id, 0, 1).await?;
    sqlx::query(
        "INSERT INTO ingest_cursors (
             chain_id, source_key, source_kind, seed_basis,
             start_block_number, next_block_number, target_block_number,
             last_processed_block_number, last_processed_block_hash
         ) VALUES ($1, 'removed', 'test', 'base_seam', 0, 2, 1, 1, $2)",
    )
    .bind(chain_id)
    .bind(format!("{chain_id}-extent-block-1"))
    .execute(scratch.pool())
    .await?;
    mark_phase_with_extent(
        scratch.pool(),
        chain_id,
        PhaseName::Interpret,
        1,
        Some(phase_runner::INTERPRETER_CONTENT_HASH),
    )
    .await?;
    let interpret_calls = Arc::new(AtomicUsize::new(0));
    let observed_calls = Arc::clone(&interpret_calls);
    let interpret = Arc::new(FunctionPhase {
        name: PhaseName::Interpret,
        handler: Arc::new(move |_| {
            observed_calls.fetch_add(1, Ordering::SeqCst);
            Ok(PhaseBatchOutcome::Complete(PhaseProgress::default()))
        }),
    });
    let runner = runner(
        scratch.runner(),
        phase_set_replacing(PhaseName::Interpret, interpret)?,
        "redo-removed-ingest-source-runner",
    )?;

    let result = runner
        .redo(
            &chain(chain_id, SeedBasis::BaseSeam)?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(0, 1)?,
            CancellationToken::new(),
        )
        .await;
    let observed_calls = interpret_calls.load(Ordering::SeqCst);

    drop(runner);
    scratch.cleanup().await?;
    let error = result.expect_err("Interpret redo must reject removal of a persisted source");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(
        error.to_string().contains("persisted ingest source keys"),
        "{error}"
    );
    assert_eq!(observed_calls, 0);
    Ok(())
}

#[tokio::test]
async fn interpret_redo_rejects_start_drift_even_when_runtime_source_starts_after_range()
-> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_redo_ingest_start_drift").await?;
    let chain_id = "redo-ingest-start-drift-chain";
    let store = PhaseStore::new(scratch.pool().clone());
    store.initialize_chain(chain_id).await?;
    seed_ingest_extent(scratch.pool(), chain_id, 0, 1).await?;
    mark_phase_with_extent(
        scratch.pool(),
        chain_id,
        PhaseName::Interpret,
        1,
        Some(phase_runner::INTERPRETER_CONTENT_HASH),
    )
    .await?;
    let interpret_calls = Arc::new(AtomicUsize::new(0));
    let observed_calls = Arc::clone(&interpret_calls);
    let interpret = Arc::new(FunctionPhase {
        name: PhaseName::Interpret,
        handler: Arc::new(move |_| {
            observed_calls.fetch_add(1, Ordering::SeqCst);
            Ok(PhaseBatchOutcome::Complete(PhaseProgress::default()))
        }),
    });
    let runner = runner(
        scratch.runner(),
        phase_set_replacing(PhaseName::Interpret, interpret)?,
        "redo-ingest-start-drift-runner",
    )?;
    let changed = ChainConfig::new(
        chain_id,
        vec![source(chain_id, "source", SeedBasis::BaseSeam, 2)?],
        false,
    )?;

    let result = runner
        .redo(
            &changed,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(0, 1)?,
            CancellationToken::new(),
        )
        .await;
    let observed_calls = interpret_calls.load(Ordering::SeqCst);

    drop(runner);
    scratch.cleanup().await?;
    let error = result.expect_err("Interpret redo must reject persisted source start drift");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(
        error
            .to_string()
            .contains("does not match the configured source"),
        "{error}"
    );
    assert_eq!(observed_calls, 0);
    Ok(())
}

#[tokio::test]
async fn killed_advisory_lock_connection_stops_before_batch_progress_writes() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_lock_liveness").await?;
    let chain_id = "lock-liveness-chain";
    let configured_chain = chain(chain_id, SeedBasis::BaseSeam)?;
    PhaseStore::new(scratch.pool().clone())
        .update_ingest_cursors(&configured_chain.sources, &PhaseProgress::default())
        .await?;
    seed_lineage(scratch.pool(), chain_id, 0).await?;
    sqlx::query("CREATE TABLE lock_liveness_writes (marker text PRIMARY KEY)")
        .execute(scratch.pool())
        .await?;
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let phase = Arc::new(LockKillPhase {
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
        chain_id: chain_id.to_owned(),
        pool: scratch.pool().clone(),
    });
    let runner = runner_with_timing(
        scratch.runner(),
        phase_set_replacing(PhaseName::Ingest, phase)?,
        "lock-liveness-runner",
        TimingConfig {
            initial_backoff: Duration::from_secs(1),
            maximum_backoff: Duration::from_secs(1),
            live_poll_interval: Duration::from_millis(1),
        },
    )?;
    let cancellation = CancellationToken::new();
    let run_cancellation = cancellation.clone();
    let task =
        tokio::spawn(async move { runner.run_chain(&configured_chain, run_cancellation).await });
    entered.notified().await;

    let terminated: Option<bool> = sqlx::query_scalar(
        "
        SELECT pg_terminate_backend(pid)
        FROM pg_locks
        WHERE locktype = 'advisory'
          AND granted
          AND database = (
              SELECT oid
              FROM pg_database
              WHERE datname = current_database()
          )
          AND pid <> pg_backend_pid()
        LIMIT 1
        ",
    )
    .fetch_optional(scratch.pool())
    .await?;
    assert_eq!(terminated, Some(true));
    tokio::time::sleep(Duration::from_millis(25)).await;
    cancellation.cancel();
    release.notify_one();
    task.await??;

    let phase_owned_writes: i64 = sqlx::query_scalar("SELECT count(*) FROM lock_liveness_writes")
        .fetch_one(scratch.pool())
        .await?;
    let heads: i64 = sqlx::query_scalar("SELECT count(*) FROM chain_heads WHERE chain_id = $1")
        .bind(chain_id)
        .fetch_one(scratch.pool())
        .await?;
    let cursor: (i64, i64, Option<i64>, Option<i64>) = sqlx::query_as(
        "SELECT start_block_number, next_block_number,
                target_block_number, last_processed_block_number
         FROM ingest_cursors WHERE chain_id = $1 AND source_key = 'source'",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    let state: (String, Option<i64>) = sqlx::query_as(
        "
        SELECT phase_status, current_block_number
        FROM chain_phase_state
        WHERE chain_id = $1
          AND phase_name = 'ingest'
        ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(phase_owned_writes, 0);
    assert_eq!(heads, 0);
    assert_eq!(cursor, (0, 0, None, None));
    assert_eq!(state, ("running".to_owned(), None));
    scratch.cleanup().await
}

#[tokio::test]
async fn head_path_loading_stops_at_the_previous_finalized_boundary() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_head_floor").await?;
    let chain_id = "head-floor-chain";
    seed_lineage(scratch.pool(), chain_id, 4).await?;
    publish_heads(
        scratch.pool(),
        chain_id,
        &HeadMarkers {
            latest: marker(chain_id, 3)?,
            safe: Some(marker(chain_id, 2)?),
            finalized: Some(marker(chain_id, 2)?),
        },
    )
    .await?;
    sqlx::query("DELETE FROM chain_lineage WHERE chain_id = $1 AND block_number < 2")
        .bind(chain_id)
        .execute(scratch.pool())
        .await?;

    publish_heads(
        scratch.pool(),
        chain_id,
        &HeadMarkers {
            latest: marker(chain_id, 4)?,
            safe: Some(marker(chain_id, 3)?),
            finalized: Some(marker(chain_id, 2)?),
        },
    )
    .await?;
    let latest: i64 =
        sqlx::query_scalar("SELECT latest_block_number FROM chain_heads WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(latest, 4);
    scratch.cleanup().await
}

#[tokio::test]
async fn head_publication_rejects_a_lineage_gap_before_the_previous_boundary() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_head_gap").await?;
    let chain_id = "head-gap-chain";
    seed_lineage(scratch.pool(), chain_id, 2).await?;
    publish_heads(
        scratch.pool(),
        chain_id,
        &HeadMarkers {
            latest: marker(chain_id, 2)?,
            safe: Some(marker(chain_id, 2)?),
            finalized: Some(marker(chain_id, 1)?),
        },
    )
    .await?;
    insert_lineage(
        scratch.pool(),
        chain_id,
        4,
        "head-gap-block-4",
        Some("head-gap-missing-3"),
    )
    .await?;

    let error = publish_heads(
        scratch.pool(),
        chain_id,
        &HeadMarkers {
            latest: BlockMarker::new(4, "head-gap-block-4")?,
            safe: Some(marker(chain_id, 2)?),
            finalized: Some(marker(chain_id, 1)?),
        },
    )
    .await
    .expect_err("a missing parent before the prior finality boundary must fail");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(error.to_string().contains("lineage gap"));
    scratch.cleanup().await
}

#[tokio::test]
async fn safe_head_cannot_move_backward() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_safe_backward").await?;
    let chain_id = "safe-backward-chain";
    seed_lineage(scratch.pool(), chain_id, 3).await?;
    publish_heads(
        scratch.pool(),
        chain_id,
        &HeadMarkers {
            latest: marker(chain_id, 3)?,
            safe: Some(marker(chain_id, 2)?),
            finalized: Some(marker(chain_id, 1)?),
        },
    )
    .await?;

    let error = publish_heads(
        scratch.pool(),
        chain_id,
        &HeadMarkers {
            latest: marker(chain_id, 3)?,
            safe: Some(marker(chain_id, 1)?),
            finalized: Some(marker(chain_id, 1)?),
        },
    )
    .await
    .expect_err("safe finality cannot move backward");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(
        error
            .to_string()
            .contains("safe head marker cannot move backward")
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn safe_head_cannot_change_hash_at_the_same_height() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_safe_hash").await?;
    let chain_id = "safe-hash-chain";
    seed_lineage(scratch.pool(), chain_id, 2).await?;
    publish_heads(
        scratch.pool(),
        chain_id,
        &HeadMarkers {
            latest: marker(chain_id, 2)?,
            safe: Some(marker(chain_id, 1)?),
            finalized: None,
        },
    )
    .await?;
    let canonical_parent = format!("{chain_id}-block-0");
    insert_lineage(
        scratch.pool(),
        chain_id,
        1,
        "safe-hash-fork-1",
        Some(&canonical_parent),
    )
    .await?;
    insert_lineage(
        scratch.pool(),
        chain_id,
        2,
        "safe-hash-fork-2",
        Some("safe-hash-fork-1"),
    )
    .await?;

    let error = publish_heads(
        scratch.pool(),
        chain_id,
        &HeadMarkers {
            latest: BlockMarker::new(2, "safe-hash-fork-2")?,
            safe: Some(BlockMarker::new(1, "safe-hash-fork-1")?),
            finalized: None,
        },
    )
    .await
    .expect_err("safe finality cannot change hash at one height");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(
        error
            .to_string()
            .contains("safe head marker at height 1 cannot change hash")
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn finalized_head_can_be_introduced_below_the_existing_safe_head() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_new_finalized_below_safe").await?;
    let chain_id = "new-finalized-below-safe-chain";
    seed_lineage(scratch.pool(), chain_id, 3).await?;
    publish_heads(
        scratch.pool(),
        chain_id,
        &HeadMarkers {
            latest: marker(chain_id, 3)?,
            safe: Some(marker(chain_id, 2)?),
            finalized: None,
        },
    )
    .await?;

    publish_heads(
        scratch.pool(),
        chain_id,
        &HeadMarkers {
            latest: marker(chain_id, 3)?,
            safe: Some(marker(chain_id, 2)?),
            finalized: Some(marker(chain_id, 1)?),
        },
    )
    .await?;
    let finalized: (Option<i64>, Option<String>) = sqlx::query_as(
        "
        SELECT finalized_block_number, finalized_block_hash
        FROM chain_heads
        WHERE chain_id = $1
        ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(finalized, (Some(1), Some(format!("{chain_id}-block-1"))));
    let finalized_ancestry: Vec<(i64, String)> = sqlx::query_as(
        "
        SELECT block_number, canonicality_state::text
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_number <= 1
        ORDER BY block_number
        ",
    )
    .bind(chain_id)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        finalized_ancestry,
        vec![(0, "finalized".to_owned()), (1, "finalized".to_owned())]
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn verification_mismatch_cancels_live_for_that_chain_but_not_other_chains() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_verify_live_overlap").await?;
    require_live_lock_for_mismatch_failure(scratch.pool()).await?;
    let bad_chain = "overlap-bad-chain";
    let good_chain = "overlap-good-chain";
    seed_identified_lineage(scratch.pool(), bad_chain, 0).await?;
    seed_identified_lineage(scratch.pool(), good_chain, 0).await?;

    let bad_live_entered = Arc::new(Notify::new());
    let release_bad_live = Arc::new(Notify::new());
    let good_live_entered = Arc::new(Notify::new());
    let bad_live_batches = Arc::new(AtomicUsize::new(0));
    let good_live_batches = Arc::new(AtomicUsize::new(0));
    let phases = overlap_phase_set(
        bad_chain,
        Arc::clone(&bad_live_entered),
        Arc::clone(&release_bad_live),
        Arc::clone(&good_live_entered),
        Arc::clone(&bad_live_batches),
        Arc::clone(&good_live_batches),
        Arc::new(Notify::new()),
    )?;
    let runner = Arc::new(runner(
        scratch.runner(),
        phases,
        "verify-live-overlap-runner",
    )?);
    let runtime = RuntimeConfig::new(
        "verify-live-overlap-runner",
        vec![
            chain(bad_chain, SeedBasis::BaseSeam)?,
            chain(good_chain, SeedBasis::BaseSeam)?,
        ],
        CapacityConfig::default(),
        test_timing(),
    )?;
    let cancellation = CancellationToken::new();
    let run_cancellation = cancellation.clone();
    let task = tokio::spawn(async move { runner.run(&runtime, run_cancellation).await });

    good_live_entered.notified().await;
    wait_for_phase_status(scratch.pool(), bad_chain, PhaseName::Verify, "failed").await?;
    wait_for_phase_status(scratch.pool(), bad_chain, PhaseName::Live, "failed").await?;
    let bad_live_state: (String, Option<String>) = sqlx::query_as(
        "
        SELECT phase_status, last_error
        FROM chain_phase_state
        WHERE chain_id = $1
          AND phase_name = 'live'
        ",
    )
    .bind(bad_chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(bad_live_state.0, "failed");
    let bad_live_error = bad_live_state
        .1
        .expect("verification mismatch cancellation must record its cause");
    assert!(bad_live_error.contains("verification mismatch"));
    assert!(bad_live_error.contains("stored verification disagrees with the live source"));
    tokio::time::sleep(Duration::from_millis(20)).await;
    let bad_batches_after_mismatch = bad_live_batches.load(Ordering::SeqCst);
    assert!(bad_batches_after_mismatch > 0);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        bad_live_batches.load(Ordering::SeqCst),
        bad_batches_after_mismatch,
        "the bad chain's live phase continued after verification failed"
    );
    assert!(good_live_batches.load(Ordering::SeqCst) > 0);
    let good_live_status: String = sqlx::query_scalar(
        "
        SELECT phase_status
        FROM chain_phase_state
        WHERE chain_id = $1
          AND phase_name = 'live'
        ",
    )
    .bind(good_chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(good_live_status, "running");

    cancellation.cancel();
    let report = tokio::time::timeout(Duration::from_secs(2), task).await???;
    assert_eq!(report.stopped_chains.len(), 1);
    assert_eq!(report.stopped_chains[0].0, bad_chain);
    assert_eq!(
        report.stopped_chains[0].1.kind(),
        ErrorKind::VerificationMismatch
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn verification_mismatch_replaces_a_live_retry_error_during_backoff() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_verify_live_backoff").await?;
    require_live_lock_for_mismatch_failure(scratch.pool()).await?;
    let chain_id = "verify-live-backoff-chain";
    seed_identified_lineage(scratch.pool(), chain_id, 0).await?;

    let live_failed_once = Arc::new(Notify::new());
    let ingest = Arc::new(FunctionPhase {
        name: PhaseName::Ingest,
        handler: Arc::new(|context| {
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
        }),
    }) as Arc<dyn Phase>;
    let verify = Arc::new(BackoffMismatchVerifyPhase {
        live_failed_once: Arc::clone(&live_failed_once),
    }) as Arc<dyn Phase>;
    let live = Arc::new(FunctionPhase {
        name: PhaseName::Live,
        handler: Arc::new(move |_| {
            live_failed_once.notify_one();
            Err(RunnerError::transient(
                "simulated transient live failure before retry backoff",
            ))
        }),
    }) as Arc<dyn Phase>;
    let phases = PhaseSet::new([
        ingest,
        complete_phase(PhaseName::Interpret),
        complete_phase(PhaseName::Project),
        verify,
        live,
    ])?;
    let error = runner_with_timing(
        scratch.runner(),
        phases,
        "verify-live-backoff-runner",
        TimingConfig {
            initial_backoff: Duration::from_secs(1),
            maximum_backoff: Duration::from_secs(1),
            live_poll_interval: Duration::from_millis(1),
        },
    )?
    .run_chain(
        &chain(chain_id, SeedBasis::BaseSeam)?,
        CancellationToken::new(),
    )
    .await
    .expect_err("verification mismatch must stop the chain");
    assert_eq!(error.kind(), ErrorKind::VerificationMismatch);

    let last_error: Option<String> = sqlx::query_scalar(
        "
        SELECT last_error
        FROM chain_phase_state
        WHERE chain_id = $1
          AND phase_name = 'live'
        ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    let last_error = last_error.expect("live failure must remain durable");
    assert!(last_error.contains("verification mismatch"));
    assert!(last_error.contains("verification failed during live retry backoff"));
    scratch.cleanup().await
}

#[tokio::test]
async fn verification_mismatch_reports_live_failure_persistence_errors() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_verify_live_record_failure").await?;
    reject_live_mismatch_failure(scratch.pool()).await?;
    let chain_id = "verify-live-record-failure-chain";
    seed_identified_lineage(scratch.pool(), chain_id, 0).await?;

    let bad_live_entered = Arc::new(Notify::new());
    let release_bad_live = Arc::new(Notify::new());
    let phases = overlap_phase_set(
        chain_id,
        Arc::clone(&bad_live_entered),
        Arc::clone(&release_bad_live),
        Arc::new(Notify::new()),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(Notify::new()),
    )?;
    let error = runner(
        scratch.runner(),
        phases,
        "verify-live-record-failure-runner",
    )?
    .run_chain(
        &chain(chain_id, SeedBasis::BaseSeam)?,
        CancellationToken::new(),
    )
    .await
    .expect_err("verification mismatch must stop the chain");
    assert_eq!(error.kind(), ErrorKind::VerificationMismatch);
    assert!(
        error
            .to_string()
            .contains("simulated live mismatch persistence failure")
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn verification_mismatch_remains_terminal_when_failure_recording_fails() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_verify_record_failure").await?;
    reject_verify_mismatch_failure(scratch.pool()).await?;
    let chain_id = "verify-record-failure-chain";
    seed_identified_lineage(scratch.pool(), chain_id, 0).await?;

    let mismatch_emitted = Arc::new(Notify::new());
    let phases = overlap_phase_set(
        chain_id,
        Arc::new(Notify::new()),
        Arc::new(Notify::new()),
        Arc::new(Notify::new()),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
        Arc::clone(&mismatch_emitted),
    )?;
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let phase_runner = runner(scratch.runner(), phases, "verify-record-failure-runner")?;
    let chain = chain(chain_id, SeedBasis::BaseSeam)?;
    let mut task =
        tokio::spawn(async move { phase_runner.run_chain(&chain, task_cancellation).await });
    tokio::time::timeout(Duration::from_secs(5), mismatch_emitted.notified())
        .await
        .expect("verification mismatch was not emitted");
    let error = match tokio::time::timeout(Duration::from_secs(2), &mut task).await {
        Ok(result) => result?,
        Err(_) => {
            cancellation.cancel();
            let _ = tokio::time::timeout(Duration::from_secs(2), &mut task).await;
            anyhow::bail!("verification mismatch was retried after failure recording failed");
        }
    }
    .expect_err("verification mismatch must remain terminal");
    assert_eq!(error.kind(), ErrorKind::VerificationMismatch);
    assert!(
        error
            .to_string()
            .contains("simulated verify mismatch persistence failure")
    );
    let live_state: (String, Option<String>) = sqlx::query_as(
        "
        SELECT phase_status, last_error
        FROM chain_phase_state
        WHERE chain_id = $1
          AND phase_name = 'live'
        ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(live_state.0, "failed");
    assert!(
        live_state
            .1
            .as_deref()
            .is_some_and(|error| error.contains("verification mismatch"))
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn multi_source_ingest_requires_per_source_progress() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_multi_source_progress").await?;
    let chain_id = "multi-source-progress-chain";
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain_id).await?;
    let sources = vec![
        source(chain_id, "bulk", SeedBasis::BaseSeam, 3)?,
        source(chain_id, "rpc", SeedBasis::NewSignatureRange, 7)?,
    ];

    let error = store
        .update_ingest_cursors(&sources, &PhaseProgress::default())
        .await
        .expect_err("multi-source fallback must not advance every cursor");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(
        error
            .to_string()
            .contains("reported no per-source progress")
    );

    let cursor_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM ingest_cursors WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(cursor_count, 0);
    scratch.cleanup().await
}

#[tokio::test]
async fn multi_source_ingest_completion_requires_every_configured_source() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_multi_source_completion").await?;
    let chain_id = "multi-source-completion-chain";
    let marker = BlockMarker::new(3, "multi-source-completion-block-3")?;
    let phase = Arc::new(FunctionPhase {
        name: PhaseName::Ingest,
        handler: Arc::new(move |_| {
            Ok(PhaseBatchOutcome::Complete(PhaseProgress {
                current: Some(marker.clone()),
                target: Some(marker.clone()),
                live_handoff: Some(marker.clone()),
                source_progress: vec![SourceProgress {
                    source_key: "bulk".to_owned(),
                    current: Some(marker.clone()),
                    target: Some(marker.clone()),
                }],
                ..PhaseProgress::default()
            }))
        }),
    });
    let configured_chain = ChainConfig::new(
        chain_id,
        vec![
            source(chain_id, "bulk", SeedBasis::BaseSeam, 0)?,
            source(chain_id, "rpc", SeedBasis::NewSignatureRange, 0)?,
        ],
        false,
    )?;
    let error = runner(
        scratch.runner(),
        phase_set_replacing(PhaseName::Ingest, phase)?,
        "multi-source-completion-runner",
    )?
    .run_chain(&configured_chain, CancellationToken::new())
    .await
    .expect_err("ingest cannot complete without reporting every configured source");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(
        error
            .to_string()
            .contains("missing source progress for rpc")
    );

    type EmptyCursor = (String, i64, i64, Option<i64>, Option<i64>);
    let cursors: Vec<EmptyCursor> = sqlx::query_as(
        "SELECT source_key, start_block_number, next_block_number,
                target_block_number, last_processed_block_number
         FROM ingest_cursors WHERE chain_id = $1 ORDER BY source_key",
    )
    .bind(chain_id)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        cursors,
        vec![
            ("bulk".to_owned(), 0, 0, None, None),
            ("rpc".to_owned(), 0, 0, None, None),
        ],
        "invalid completion must leave only the phase-entry identities"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn ingest_completion_requires_each_source_to_reach_its_target() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_ingest_incomplete").await?;
    let chain_id = "ingest-incomplete-chain";
    let current = BlockMarker::new(1, "ingest-incomplete-block-1")?;
    let target = BlockMarker::new(3, "ingest-incomplete-block-3")?;
    let phase = Arc::new(FunctionPhase {
        name: PhaseName::Ingest,
        handler: Arc::new(move |_| {
            Ok(PhaseBatchOutcome::Complete(PhaseProgress {
                current: Some(current.clone()),
                target: Some(target.clone()),
                live_handoff: Some(current.clone()),
                ..PhaseProgress::default()
            }))
        }),
    });
    let error = runner(
        scratch.runner(),
        phase_set_replacing(PhaseName::Ingest, phase)?,
        "ingest-incomplete-runner",
    )?
    .run_chain(
        &chain(chain_id, SeedBasis::BaseSeam)?,
        CancellationToken::new(),
    )
    .await
    .expect_err("ingest cannot complete while its source is behind its target");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(
        error
            .to_string()
            .contains("source source cannot complete at block 1 before target block 3")
    );

    let cursor: (i64, i64, Option<i64>, Option<i64>) = sqlx::query_as(
        "SELECT start_block_number, next_block_number,
                target_block_number, last_processed_block_number
         FROM ingest_cursors WHERE chain_id = $1 AND source_key = 'source'",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        cursor,
        (0, 0, None, None),
        "invalid completion must leave only the phase-entry identity"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn ingest_completion_requires_a_target_and_live_handoff() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_ingest_completion_markers").await?;
    let missing_target_chain = "ingest-missing-target-chain";
    let missing_target = Arc::new(FunctionPhase {
        name: PhaseName::Ingest,
        handler: Arc::new(|_| Ok(PhaseBatchOutcome::Complete(PhaseProgress::default()))),
    });
    let error = runner(
        scratch.runner(),
        phase_set_replacing(PhaseName::Ingest, missing_target)?,
        "ingest-missing-target-runner",
    )?
    .run_chain(
        &chain(missing_target_chain, SeedBasis::BaseSeam)?,
        CancellationToken::new(),
    )
    .await
    .expect_err("ingest cannot complete without a target");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(
        error
            .to_string()
            .contains("cannot complete without a target block")
    );

    let missing_handoff_chain = "ingest-missing-handoff-chain";
    let marker = BlockMarker::new(3, "ingest-missing-handoff-block-3")?;
    let missing_handoff = Arc::new(FunctionPhase {
        name: PhaseName::Ingest,
        handler: Arc::new(move |_| {
            Ok(PhaseBatchOutcome::Complete(PhaseProgress {
                current: Some(marker.clone()),
                target: Some(marker.clone()),
                ..PhaseProgress::default()
            }))
        }),
    });
    let error = runner(
        scratch.runner(),
        phase_set_replacing(PhaseName::Ingest, missing_handoff)?,
        "ingest-missing-handoff-runner",
    )?
    .run_chain(
        &chain(missing_handoff_chain, SeedBasis::BaseSeam)?,
        CancellationToken::new(),
    )
    .await
    .expect_err("ingest cannot complete without its live handoff");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(
        error
            .to_string()
            .contains("ingest cannot complete without a live handoff")
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn non_verify_phase_rejects_a_verification_level() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_verification_guard").await?;
    let chain_id = "verification-guard-chain";
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain_id).await?;

    let error = store
        .record_progress(
            chain_id,
            PhaseName::Project,
            &RunMode::Normal,
            &PhaseProgress {
                verification_level: Some(VerificationLevel::CrossChecked),
                ..PhaseProgress::default()
            },
        )
        .await
        .expect_err("only verify may record a verification level");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(error.to_string().contains("reported a verification level"));
    scratch.cleanup().await
}

#[tokio::test]
async fn ingest_redo_resume_loads_each_persisted_source_cursor() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_ingest_redo_cursors").await?;
    let chain_id = "ingest-redo-cursor-chain";
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain_id).await?;
    let sources = vec![
        source(chain_id, "bulk", SeedBasis::BaseSeam, 10)?,
        source(chain_id, "rpc", SeedBasis::NewSignatureRange, 20)?,
    ];
    store
        .update_ingest_cursors(
            &sources,
            &PhaseProgress {
                current: Some(BlockMarker::new(29, "redo-cursor-block-29")?),
                target: Some(BlockMarker::new(40, "redo-cursor-block-40")?),
                source_progress: vec![
                    SourceProgress {
                        source_key: "bulk".to_owned(),
                        current: Some(BlockMarker::new(19, "redo-cursor-block-19")?),
                        target: Some(BlockMarker::new(19, "redo-cursor-block-19")?),
                    },
                    SourceProgress {
                        source_key: "rpc".to_owned(),
                        current: Some(BlockMarker::new(29, "redo-cursor-block-29")?),
                        target: Some(BlockMarker::new(40, "redo-cursor-block-40")?),
                    },
                ],
                ..PhaseProgress::default()
            },
        )
        .await?;
    sqlx::query(
        "
        UPDATE chain_phase_state
        SET phase_status = 'running',
            redo_in_progress = true,
            redo_mode = 'redo',
            redo_previous_phase_status = 'idle',
            redo_from_block_number = 10,
            redo_to_block_number = 40,
            redo_current_block_number = 15,
            redo_current_block_hash = 'redo-cursor-block-15',
            redo_target_block_number = 40,
            redo_target_block_hash = 'redo-cursor-block-40',
            started_at = now()
        WHERE chain_id = $1
          AND phase_name = 'ingest'
        ",
    )
    .bind(chain_id)
    .execute(scratch.pool())
    .await?;

    let resume = store
        .phase_resume(
            chain_id,
            PhaseName::Ingest,
            &RunMode::Redo(BlockRange::new(10, 40)?),
        )
        .await?;
    assert_eq!(resume.ingest_cursors.len(), 2);
    assert_eq!(resume.ingest_cursors[0].source_key, "bulk");
    assert_eq!(resume.ingest_cursors[0].next_block_number, 20);
    assert_eq!(resume.ingest_cursors[0].target_block_number, Some(19));
    assert_eq!(
        resume.ingest_cursors[0]
            .last_processed
            .as_ref()
            .map(|marker| marker.number),
        Some(19)
    );
    assert_eq!(resume.ingest_cursors[1].source_key, "rpc");
    assert_eq!(resume.ingest_cursors[1].next_block_number, 30);
    assert_eq!(resume.ingest_cursors[1].target_block_number, Some(40));
    scratch.cleanup().await
}

#[tokio::test]
async fn normal_mode_constraint_violations_are_terminal_data_integrity() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_normal_constraint").await?;
    let chain_id = "normal-constraint-chain";
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain_id).await?;
    store
        .start_phase(chain_id, PhaseName::Ingest, &RunMode::Normal)
        .await?;
    sqlx::raw_sql(
        "
        CREATE FUNCTION reject_test_normal_writes()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF NEW.current_block_number = 23 OR NEW.phase_status = 'failed' THEN
                RAISE EXCEPTION 'deterministic test constraint'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END
        $$;
        CREATE TRIGGER reject_test_normal_writes
        BEFORE UPDATE ON chain_phase_state
        FOR EACH ROW
        EXECUTE FUNCTION reject_test_normal_writes();
        ",
    )
    .execute(scratch.pool())
    .await?;

    let progress_error = store
        .record_progress(
            chain_id,
            PhaseName::Ingest,
            &RunMode::Normal,
            &PhaseProgress {
                current: Some(BlockMarker::new(23, "normal-constraint-block-23")?),
                target: Some(BlockMarker::new(24, "normal-constraint-block-24")?),
                ..PhaseProgress::default()
            },
        )
        .await
        .expect_err("class-23 progress violation must be terminal");
    assert_eq!(progress_error.kind(), ErrorKind::DataIntegrity);
    assert!(!progress_error.is_retryable());

    let failure_error = store
        .fail_phase(chain_id, PhaseName::Ingest, "test failure")
        .await
        .expect_err("class-23 failure-recording violation must be terminal");
    assert_eq!(failure_error.kind(), ErrorKind::DataIntegrity);
    assert!(!failure_error.is_retryable());
    scratch.cleanup().await
}

#[tokio::test]
async fn ingest_cursor_constraint_violations_are_terminal_data_integrity() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_ingest_cursor_constraint").await?;
    let chain_id = "ingest-cursor-constraint-chain";
    let store = PhaseStore::new(scratch.runner().pool().clone());
    let update_source = source(chain_id, "reject-update", SeedBasis::BaseSeam, 10)?;
    store
        .update_ingest_cursors(
            std::slice::from_ref(&update_source),
            &PhaseProgress::default(),
        )
        .await?;
    sqlx::raw_sql(
        "
        CREATE FUNCTION reject_test_ingest_cursor_writes()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF (TG_OP = 'INSERT' AND NEW.source_key = 'reject-insert')
                OR (TG_OP = 'UPDATE' AND NEW.source_key = 'reject-update')
            THEN
                RAISE EXCEPTION 'deterministic ingest cursor constraint'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END
        $$;
        CREATE TRIGGER reject_test_ingest_cursor_writes
        BEFORE INSERT OR UPDATE ON ingest_cursors
        FOR EACH ROW
        EXECUTE FUNCTION reject_test_ingest_cursor_writes();
        ",
    )
    .execute(scratch.pool())
    .await?;

    let insert_source = source(chain_id, "reject-insert", SeedBasis::BaseSeam, 10)?;
    let insert_error = store
        .update_ingest_cursors(&[insert_source], &PhaseProgress::default())
        .await
        .expect_err("class-23 cursor insert violation must be terminal");
    assert_eq!(insert_error.kind(), ErrorKind::DataIntegrity);
    assert!(!insert_error.is_retryable());

    let update_error = store
        .update_ingest_cursors(
            std::slice::from_ref(&update_source),
            &PhaseProgress {
                source_progress: vec![SourceProgress {
                    source_key: update_source.source_key.clone(),
                    current: Some(BlockMarker::new(10, "cursor-constraint-block-10")?),
                    target: Some(BlockMarker::new(11, "cursor-constraint-block-11")?),
                }],
                ..PhaseProgress::default()
            },
        )
        .await
        .expect_err("class-23 cursor update violation must be terminal");
    assert_eq!(update_error.kind(), ErrorKind::DataIntegrity);
    assert!(!update_error.is_retryable());
    scratch.cleanup().await
}

#[tokio::test]
async fn continue_batches_skip_the_live_poll_sleep() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_continue_without_sleep").await?;
    let chain_id = "continue-without-sleep-chain";
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain_id).await?;
    seed_ingest_extent(scratch.pool(), chain_id, 0, 1).await?;
    mark_phase_with_extent(
        scratch.pool(),
        chain_id,
        PhaseName::Interpret,
        1,
        Some(phase_runner::INTERPRETER_CONTENT_HASH),
    )
    .await?;

    let calls = Arc::new(AtomicUsize::new(0));
    let phase_calls = Arc::clone(&calls);
    let phase = Arc::new(FunctionPhase {
        name: PhaseName::Interpret,
        handler: Arc::new(move |_| {
            let call = phase_calls.fetch_add(1, Ordering::SeqCst);
            let progress = progress_at(call.min(1) as i64, 1, "continue-without-sleep");
            if call == 0 {
                Ok(PhaseBatchOutcome::Continue(progress))
            } else {
                Ok(PhaseBatchOutcome::Complete(progress))
            }
        }),
    });
    let phase_runner = runner_with_timing(
        scratch.runner(),
        phase_set_replacing(PhaseName::Interpret, phase)?,
        "continue-without-sleep-runner",
        TimingConfig {
            initial_backoff: Duration::from_millis(1),
            maximum_backoff: Duration::from_millis(4),
            live_poll_interval: Duration::from_secs(30),
        },
    )?;

    tokio::time::timeout(
        Duration::from_secs(2),
        phase_runner.redo(
            &chain(chain_id, SeedBasis::BaseSeam)?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(0, 1)?,
            CancellationToken::new(),
        ),
    )
    .await
    .expect("continue must run the next batch without waiting for the poll interval")?;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    scratch.cleanup().await
}

#[tokio::test]
async fn verify_phase_cannot_complete_without_a_verification_level() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_verify_level_required").await?;
    let chain_id = "verify-level-required-chain";
    seed_identified_lineage(scratch.pool(), chain_id, 0).await?;
    let configured_chain = ChainConfig::new(
        chain_id,
        vec![source(chain_id, "source", SeedBasis::BaseSeam, 0)?],
        true,
    )?;
    let marker = marker(chain_id, 0)?;
    let ingest = Arc::new(FunctionPhase {
        name: PhaseName::Ingest,
        handler: Arc::new(move |_| {
            Ok(PhaseBatchOutcome::Complete(PhaseProgress {
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
                    target: Some(marker.clone()),
                }],
                ..PhaseProgress::default()
            }))
        }),
    }) as Arc<dyn Phase>;
    let phases = PhaseSet::new([
        ingest,
        complete_phase(PhaseName::Interpret),
        complete_phase(PhaseName::Project),
        complete_phase(PhaseName::Verify),
        complete_phase(PhaseName::Live),
    ])?;
    let error = runner(scratch.runner(), phases, "verify-level-required-runner")?
        .run_chain(&configured_chain, CancellationToken::new())
        .await
        .expect_err("verify cannot complete without reporting its trust level");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(
        error
            .to_string()
            .contains("verify phase cannot complete without a verification level")
    );

    let status: String = sqlx::query_scalar(
        "
        SELECT phase_status
        FROM chain_phase_state
        WHERE chain_id = $1
          AND phase_name = 'verify'
        ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(status, "failed");
    scratch.cleanup().await
}

#[tokio::test]
async fn verify_redo_persists_the_verification_level_reported_by_the_redo() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_verify_redo_level").await?;
    let chain_id = "verify-redo-level-chain";
    let store = PhaseStore::new(scratch.runner().pool().clone());
    store.initialize_chain(chain_id).await?;
    seed_ingest_extent(scratch.pool(), chain_id, 0, 9).await?;
    mark_phase_with_extent(
        scratch.pool(),
        chain_id,
        PhaseName::Project,
        9,
        Some(phase_runner::INTERPRETER_CONTENT_HASH),
    )
    .await?;
    mark_phase_with_extent(scratch.pool(), chain_id, PhaseName::Verify, 9, None).await?;
    sqlx::query(
        "
        UPDATE chain_phase_state
        SET verification_level = 'quick_synced'
        WHERE chain_id = $1
          AND phase_name = 'verify'
        ",
    )
    .bind(chain_id)
    .execute(scratch.pool())
    .await?;

    let phase = Arc::new(FunctionPhase {
        name: PhaseName::Verify,
        handler: Arc::new(|_| {
            Ok(PhaseBatchOutcome::Complete(PhaseProgress {
                verification_level: Some(VerificationLevel::NodeChecked),
                ..progress_at(6, 6, "verify-redo-level")
            }))
        }),
    });
    runner(
        scratch.runner(),
        phase_set_replacing(PhaseName::Verify, phase)?,
        "verify-redo-level-runner",
    )?
    .redo(
        &chain(chain_id, SeedBasis::BaseSeam)?,
        RedoPhase::Phase(PhaseName::Verify),
        BlockRange::new(5, 6)?,
        CancellationToken::new(),
    )
    .await?;

    let level: Option<String> = sqlx::query_scalar(
        "
        SELECT verification_level
        FROM chain_phase_state
        WHERE chain_id = $1
          AND phase_name = 'verify'
        ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(level.as_deref(), Some("node_checked"));
    scratch.cleanup().await
}

fn runner(
    database: phase_runner::database::RunnerDatabase,
    phases: PhaseSet,
    instance_id: &str,
) -> RunnerResult<PhaseRunner> {
    runner_with_timing(database, phases, instance_id, test_timing())
}

fn runner_with_timing(
    database: phase_runner::database::RunnerDatabase,
    phases: PhaseSet,
    instance_id: &str,
    timing: TimingConfig,
) -> RunnerResult<PhaseRunner> {
    PhaseRunner::new(
        database,
        phases,
        CapacityGuard::new(CapacityConfig::default(), Arc::new(AlwaysAvailable)),
        instance_id,
        timing,
    )
}

fn test_timing() -> TimingConfig {
    TimingConfig {
        initial_backoff: Duration::from_millis(1),
        maximum_backoff: Duration::from_millis(4),
        live_poll_interval: Duration::from_millis(1),
    }
}

fn chain(chain_id: &str, seed_basis: SeedBasis) -> RunnerResult<ChainConfig> {
    ChainConfig::new(
        chain_id,
        vec![source(chain_id, "source", seed_basis, 0)?],
        false,
    )
}

fn source(
    chain_id: &str,
    source_key: &str,
    seed_basis: SeedBasis,
    start_block_number: i64,
) -> RunnerResult<SourceConfig> {
    SourceConfig::new(
        chain_id,
        source_key,
        "test",
        seed_basis,
        start_block_number,
        "http://source.invalid",
    )
}

fn progress_at(current: i64, target: i64, prefix: &str) -> PhaseProgress {
    PhaseProgress {
        current: Some(BlockMarker {
            number: current,
            hash: format!("{prefix}-block-{current}"),
        }),
        target: Some(BlockMarker {
            number: target,
            hash: format!("{prefix}-block-{target}"),
        }),
        ..PhaseProgress::default()
    }
}

fn marker(chain_id: &str, number: i64) -> RunnerResult<BlockMarker> {
    BlockMarker::new(number, format!("{chain_id}-block-{number}"))
}

async fn seed_identified_lineage(pool: &sqlx::PgPool, chain_id: &str, through: i64) -> Result<()> {
    let configured_chain = chain(chain_id, SeedBasis::BaseSeam)?;
    PhaseStore::new(pool.clone())
        .ensure_ingest_sources(chain_id, &configured_chain.sources)
        .await?;
    seed_lineage(pool, chain_id, through).await
}

async fn seed_ingest_extent(pool: &sqlx::PgPool, chain_id: &str, from: i64, to: i64) -> Result<()> {
    seed_lineage(pool, chain_id, to).await?;
    sqlx::query("UPDATE chain_lineage SET canonicality_state = 'canonical' WHERE chain_id = $1")
        .bind(chain_id)
        .execute(pool)
        .await?;
    let hash = format!("{chain_id}-extent-block-{to}");
    sqlx::query(
        "
        UPDATE chain_phase_state
        SET phase_status = 'completed',
            current_block_number = $2,
            current_block_hash = $3,
            target_block_number = $2,
            target_block_hash = $3,
            live_handoff_block_number = $2,
            live_handoff_block_hash = $3,
            started_at = now(),
            finished_at = now(),
            updated_at = now()
        WHERE chain_id = $1
          AND phase_name = 'ingest'
        ",
    )
    .bind(chain_id)
    .bind(to)
    .bind(&hash)
    .execute(pool)
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
        VALUES ($1, 'source', 'test', 'base_seam', $2, $3, $4, $4, $5)
        ",
    )
    .bind(chain_id)
    .bind(from)
    .bind(to + 1)
    .bind(to)
    .bind(hash)
    .execute(pool)
    .await?;
    Ok(())
}

async fn mark_phase_with_extent(
    pool: &sqlx::PgPool,
    chain_id: &str,
    phase: PhaseName,
    through: i64,
    content_hash: Option<&str>,
) -> Result<()> {
    let hash = format!("{chain_id}-extent-block-{through}");
    sqlx::query(
        "
        UPDATE chain_phase_state
        SET phase_status = 'completed',
            current_block_number = $3,
            current_block_hash = $4,
            target_block_number = $3,
            target_block_hash = $4,
            input_content_hash = $5,
            started_at = now(),
            finished_at = now(),
            updated_at = now()
        WHERE chain_id = $1
          AND phase_name = $2
        ",
    )
    .bind(chain_id)
    .bind(phase.as_str())
    .bind(through)
    .bind(hash)
    .bind(content_hash)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_lineage(
    pool: &sqlx::PgPool,
    chain_id: &str,
    number: i64,
    hash: &str,
    parent_hash: Option<&str>,
) -> Result<()> {
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
    .bind(parent_hash)
    .bind(number)
    .execute(pool)
    .await?;
    Ok(())
}

async fn wait_for_no_advisory_locks(pool: &sqlx::PgPool) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let count: i64 = sqlx::query_scalar(
                "
                SELECT count(*)
                FROM pg_locks
                WHERE locktype = 'advisory'
                  AND granted
                  AND database = (
                      SELECT oid
                      FROM pg_database
                      WHERE datname = current_database()
                  )
                ",
            )
            .fetch_one(pool)
            .await?;
            if count == 0 {
                return Result::<()>::Ok(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    Ok(())
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

async fn require_live_lock_for_mismatch_failure(pool: &sqlx::PgPool) -> Result<()> {
    sqlx::raw_sql(
        "
        CREATE FUNCTION require_live_lock_for_mismatch_failure()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
            lock_key bigint;
        BEGIN
            IF NEW.phase_name = 'live'
               AND NEW.phase_status = 'failed'
               AND NEW.last_error LIKE '%verification mismatch%'
            THEN
                lock_key := hashtextextended(
                    format('phase-runner:%s:live', NEW.chain_id),
                    0
                );
                IF pg_try_advisory_lock(lock_key) THEN
                    PERFORM pg_advisory_unlock(lock_key);
                    RAISE EXCEPTION
                        'verification mismatch wrote live failure without its phase lock';
                END IF;
            END IF;
            RETURN NEW;
        END
        $$;

        CREATE TRIGGER require_live_lock_for_mismatch_failure
        BEFORE UPDATE ON chain_phase_state
        FOR EACH ROW
        EXECUTE FUNCTION require_live_lock_for_mismatch_failure();
        ",
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn reject_live_mismatch_failure(pool: &sqlx::PgPool) -> Result<()> {
    sqlx::raw_sql(
        "
        CREATE FUNCTION reject_live_mismatch_failure()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF NEW.phase_name = 'live'
               AND NEW.phase_status = 'failed'
               AND NEW.last_error LIKE '%verification mismatch%'
            THEN
                RAISE EXCEPTION
                    'simulated live mismatch persistence failure';
            END IF;
            RETURN NEW;
        END
        $$;

        CREATE TRIGGER reject_live_mismatch_failure
        BEFORE UPDATE ON chain_phase_state
        FOR EACH ROW
        EXECUTE FUNCTION reject_live_mismatch_failure();
        ",
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn reject_verify_mismatch_failure(pool: &sqlx::PgPool) -> Result<()> {
    sqlx::raw_sql(
        "
        CREATE FUNCTION reject_verify_mismatch_failure()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF NEW.phase_name = 'verify'
               AND NEW.phase_status = 'failed'
               AND NEW.last_error LIKE 'verification mismatch:%'
            THEN
                RAISE EXCEPTION
                    'simulated verify mismatch persistence failure';
            END IF;
            RETURN NEW;
        END
        $$;

        CREATE TRIGGER reject_verify_mismatch_failure
        BEFORE UPDATE ON chain_phase_state
        FOR EACH ROW
        EXECUTE FUNCTION reject_verify_mismatch_failure();
        ",
    )
    .execute(pool)
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

struct CrashBetweenBatchesPhase {
    name: PhaseName,
    calls: AtomicUsize,
    second_batch_entered: Arc<Notify>,
    first_progress: PhaseProgress,
}

impl Phase for CrashBetweenBatchesPhase {
    fn name(&self) -> PhaseName {
        self.name
    }

    fn run_batch(&self, _context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                return Ok(PhaseBatchOutcome::Continue(self.first_progress.clone()));
            }
            self.second_batch_entered.notify_one();
            future::pending::<()>().await;
            unreachable!("the simulated crash aborts this future")
        })
    }
}

struct LockKillPhase {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    chain_id: String,
    pool: sqlx::PgPool,
}

impl Phase for LockKillPhase {
    fn name(&self) -> PhaseName {
        PhaseName::Ingest
    }

    fn run_batch(&self, _context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            self.entered.notify_one();
            self.release.notified().await;
            sqlx::query("INSERT INTO lock_liveness_writes (marker) VALUES ('phase-owned-write')")
                .execute(&self.pool)
                .await
                .map_err(|error| {
                    RunnerError::transient(format!(
                        "failed to record the simulated phase-owned write: {error}"
                    ))
                })?;
            let marker = BlockMarker::new(0, format!("{}-block-0", self.chain_id))?;
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

fn phase_set_replacing(name: PhaseName, replacement: Arc<dyn Phase>) -> RunnerResult<PhaseSet> {
    PhaseSet::new(PhaseName::ALL.map(|phase| {
        if phase == name {
            Arc::clone(&replacement)
        } else {
            Arc::new(FunctionPhase {
                name: phase,
                handler: Arc::new(|_| Ok(PhaseBatchOutcome::Complete(PhaseProgress::default()))),
            }) as Arc<dyn Phase>
        }
    }))
}

fn overlap_phase_set(
    bad_chain: &str,
    bad_live_entered: Arc<Notify>,
    release_bad_live: Arc<Notify>,
    good_live_entered: Arc<Notify>,
    bad_live_batches: Arc<AtomicUsize>,
    good_live_batches: Arc<AtomicUsize>,
    bad_mismatch_emitted: Arc<Notify>,
) -> RunnerResult<PhaseSet> {
    let bad_chain = Arc::<str>::from(bad_chain);
    let ingest = Arc::new(FunctionPhase {
        name: PhaseName::Ingest,
        handler: Arc::new(|context| {
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
        }),
    }) as Arc<dyn Phase>;
    let interpret = complete_phase(PhaseName::Interpret);
    let project = complete_phase(PhaseName::Project);
    let verify = Arc::new(OverlapVerifyPhase {
        bad_chain: Arc::clone(&bad_chain),
        bad_live_entered: Arc::clone(&bad_live_entered),
        release_bad_live: Arc::clone(&release_bad_live),
        bad_mismatch_emitted,
    }) as Arc<dyn Phase>;
    let live = Arc::new(OverlapLivePhase {
        bad_chain,
        bad_live_entered,
        release_bad_live,
        good_live_entered,
        bad_live_batches,
        good_live_batches,
    }) as Arc<dyn Phase>;
    PhaseSet::new([ingest, interpret, project, verify, live])
}

fn complete_phase(name: PhaseName) -> Arc<dyn Phase> {
    Arc::new(FunctionPhase {
        name,
        handler: Arc::new(|_| Ok(PhaseBatchOutcome::Complete(PhaseProgress::default()))),
    })
}

struct OverlapVerifyPhase {
    bad_chain: Arc<str>,
    bad_live_entered: Arc<Notify>,
    release_bad_live: Arc<Notify>,
    bad_mismatch_emitted: Arc<Notify>,
}

impl Phase for OverlapVerifyPhase {
    fn name(&self) -> PhaseName {
        PhaseName::Verify
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            if context.chain_id == self.bad_chain.as_ref() {
                self.bad_live_entered.notified().await;
                self.release_bad_live.notify_one();
                self.bad_mismatch_emitted.notify_one();
                return Err(RunnerError::verification_mismatch(
                    "stored verification disagrees with the live source",
                ));
            }
            Ok(PhaseBatchOutcome::Complete(PhaseProgress {
                verification_level: Some(VerificationLevel::CrossChecked),
                ..PhaseProgress::default()
            }))
        })
    }
}

struct OverlapLivePhase {
    bad_chain: Arc<str>,
    bad_live_entered: Arc<Notify>,
    release_bad_live: Arc<Notify>,
    good_live_entered: Arc<Notify>,
    bad_live_batches: Arc<AtomicUsize>,
    good_live_batches: Arc<AtomicUsize>,
}

struct BackoffMismatchVerifyPhase {
    live_failed_once: Arc<Notify>,
}

impl Phase for BackoffMismatchVerifyPhase {
    fn name(&self) -> PhaseName {
        PhaseName::Verify
    }

    fn run_batch(&self, _context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            self.live_failed_once.notified().await;
            Err(RunnerError::verification_mismatch(
                "verification failed during live retry backoff",
            ))
        })
    }
}

impl Phase for OverlapLivePhase {
    fn name(&self) -> PhaseName {
        PhaseName::Live
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            if context.chain_id == self.bad_chain.as_ref() {
                if self.bad_live_batches.fetch_add(1, Ordering::SeqCst) == 0 {
                    self.bad_live_entered.notify_one();
                    self.release_bad_live.notified().await;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            } else {
                self.good_live_batches.fetch_add(1, Ordering::SeqCst);
                self.good_live_entered.notify_one();
            }
            Ok(PhaseBatchOutcome::Idle(PhaseProgress::default()))
        })
    }
}
