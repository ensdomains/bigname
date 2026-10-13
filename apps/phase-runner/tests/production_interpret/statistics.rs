//! The runner refreshes the planner statistics of `normalized_events` between Interpret
//! batches, through the phase's after-progress hook.
use std::sync::Mutex;

use phase_runner::phase::{AfterProgressFuture, PhaseBatchOutcome};

use super::*;

const REFRESHED: &str = "interpret refreshed normalized_events statistics";

/// The production Interpret phase, one block per batch, with each batch and each refresh
/// written down in the order the runner made them.
struct ObservedInterpret {
    inner: InterpretPhase,
    pool: PgPool,
    steps: Mutex<Vec<String>>,
    /// Raised at the end of a batch that leaves a refresh waiting.
    stop_when_due: Option<CancellationToken>,
}

impl ObservedInterpret {
    fn new(scratch: &ScratchDatabase, stop_when_due: Option<CancellationToken>) -> Arc<Self> {
        let capacity = CapacityConfig {
            interpret_blocks_per_batch: std::num::NonZeroU32::MIN,
            ..CapacityConfig::default()
        };
        Arc::new(Self {
            inner: InterpretPhase::from_capacity(scratch.runner().pool().clone(), &capacity),
            pool: scratch.pool().clone(),
            steps: Mutex::default(),
            stop_when_due,
        })
    }

    fn steps(&self) -> Vec<String> {
        self.steps.lock().expect("steps lock").clone()
    }
}

impl Phase for ObservedInterpret {
    fn name(&self) -> PhaseName {
        PhaseName::Interpret
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            let chain_id = context.chain_id.clone();
            let outcome = self.inner.run_batch(context).await?;
            let through = outcome.progress().current.as_ref().map(|at| at.number);
            let complete = matches!(outcome, PhaseBatchOutcome::Complete(_));
            self.steps
                .lock()
                .expect("steps lock")
                .push(format!("batch through {through:?} complete={complete}"));
            if let Some(stop) = &self.stop_when_due
                && self.inner.has_after_progress_work(&chain_id)
            {
                stop.cancel();
            }
            Ok(outcome)
        })
    }

    fn has_after_progress_work(&self, chain_id: &str) -> bool {
        self.inner.has_after_progress_work(chain_id)
    }

    fn after_progress_recorded(&self, chain_id: &str) -> AfterProgressFuture<'_> {
        let chain_id = chain_id.to_owned();
        Box::pin(async move {
            let recorded: Option<i64> = sqlx::query_scalar(
                "SELECT redo_current_block_number FROM chain_phase_state
                 WHERE chain_id = $1 AND phase_name = 'interpret'",
            )
            .bind(&chain_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|error| RunnerError::new(ErrorKind::Transient, error.to_string()))?;
            let step = self.inner.after_progress_recorded(&chain_id).await?;
            self.steps
                .lock()
                .expect("steps lock")
                .push(format!("hook with progress at {recorded:?}"));
            Ok(step)
        })
    }
}

/// `pg_class.reltuples` of `normalized_events`: -1 until the table is first analyzed.
async fn analyzed_rows(pool: &PgPool) -> Result<f32> {
    Ok(sqlx::query_scalar(
        "SELECT reltuples FROM pg_class
         WHERE oid = to_regclass('normalized_events')",
    )
    .fetch_one(pool)
    .await?)
}

/// Runs an operator Interpret redo of blocks 0 to 2 and returns its result with the log.
async fn redo(
    scratch: &ScratchDatabase,
    chain: &str,
    phase: Arc<ObservedInterpret>,
    cancellation: CancellationToken,
) -> Result<(phase_runner::error::RunnerResult<()>, String)> {
    let phases = PhaseSet::with_ingest_and_interpret(
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        phase,
    )?;
    let runner = PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "interpret-statistics-runner",
        test_timing(),
    )?;
    let logs = support::CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_writer(logs.clone())
        .finish();
    let result = runner
        .redo(
            &chain_config(chain)?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(0, 2)?,
            cancellation,
        )
        .with_subscriber(subscriber)
        .await;
    Ok((result, logs.text()))
}

/// A chain interpreted through block 2 and recorded as completed, ready for a redo.
async fn interpreted(scratch: &ScratchDatabase, chain: &str, ensv2: bool) -> Result<()> {
    if ensv2 {
        seed_v2_lifecycle_fixture(scratch.pool(), chain).await?;
    } else {
        seed_fixture(scratch.pool(), chain, &[(1, "alice"), (2, "bob")]).await?;
    }
    run_engine(scratch.pool(), chain, 0, 2, InterpretRunMode::Normal).await?;
    PhaseStore::new(scratch.runner().pool().clone())
        .initialize_chain(chain)
        .await?;
    seed_completed_phase_extent(scratch.pool(), chain, INTERPRETER_CONTENT_HASH).await
}

/// Block 0 holds no ENSv2 event, so the batch that clears the redo range refreshes nothing.
/// Block 1 holds the chain's first ENSv2 events: the refresh follows that batch's recorded
/// progress and comes before the next batch. Block 2 completes the redo and refreshes again.
#[tokio::test]
async fn redo_refreshes_statistics_after_the_first_ensv2_batch() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_statistics_ensv2").await?;
    let chain = "interpret-statistics-ensv2";
    interpreted(&scratch, chain, true).await?;
    let ensv2_events: Vec<i64> = sqlx::query_scalar(
        "SELECT DISTINCT block_number FROM normalized_events
         WHERE chain_id = $1 AND source_family LIKE 'ens\\_v2\\_%' AND block_number IS NOT NULL
         ORDER BY block_number",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(ensv2_events, [1, 2]);
    assert_eq!(analyzed_rows(scratch.pool()).await?, -1.0);

    let phase = ObservedInterpret::new(&scratch, None);
    let (result, logs) = redo(&scratch, chain, phase.clone(), CancellationToken::new()).await?;
    result?;
    assert_eq!(
        phase.steps(),
        [
            "batch through Some(0) complete=false",
            "batch through Some(1) complete=false",
            "hook with progress at Some(1)",
            "batch through Some(2) complete=true",
            "hook with progress at Some(2)",
        ]
    );
    assert_eq!(logs.matches(REFRESHED).count(), 2, "{logs}");
    assert!(analyzed_rows(scratch.pool()).await? > 0.0);
    drop(phase);
    scratch.cleanup().await
}

/// A chain with no ENSv2 event refreshes once, when its redo completes.
#[tokio::test]
async fn redo_completion_refreshes_statistics() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_statistics_completion").await?;
    let chain = "interpret-statistics-completion";
    interpreted(&scratch, chain, false).await?;
    assert_eq!(analyzed_rows(scratch.pool()).await?, -1.0);

    let phase = ObservedInterpret::new(&scratch, None);
    let (result, logs) = redo(&scratch, chain, phase.clone(), CancellationToken::new()).await?;
    result?;
    assert_eq!(
        phase.steps(),
        [
            "batch through Some(0) complete=false",
            "batch through Some(1) complete=false",
            "batch through Some(2) complete=true",
            "hook with progress at Some(2)",
        ]
    );
    assert_eq!(logs.matches(REFRESHED).count(), 1, "{logs}");
    assert!(logs.contains("mode=\"redo\""), "{logs}");
    assert!(analyzed_rows(scratch.pool()).await? > 0.0);
    drop(phase);
    scratch.cleanup().await
}

/// A stop that is already raised when the completing batch leaves a refresh waiting: the
/// runner does not call the hook and the redo ends as incomplete, with the batch and its
/// progress kept.
async fn stopped_at_completion(scratch: &ScratchDatabase, chain: &str) -> Result<()> {
    interpreted(scratch, chain, false).await?;
    let stop = CancellationToken::new();
    let phase = ObservedInterpret::new(scratch, Some(stop.clone()));
    let (result, logs) = redo(scratch, chain, phase.clone(), stop).await?;
    let error = result.expect_err("a stop with a refresh waiting ends the redo as incomplete");
    assert_eq!(error.kind(), ErrorKind::InvalidTransition, "{error}");
    assert!(error.to_string().contains("is incomplete"), "{error}");
    assert_eq!(
        phase.steps(),
        [
            "batch through Some(0) complete=false",
            "batch through Some(1) complete=false",
            "batch through Some(2) complete=true",
        ]
    );
    assert_eq!(logs.matches(REFRESHED).count(), 0, "{logs}");
    assert_eq!(analyzed_rows(scratch.pool()).await?, -1.0);
    Ok(())
}

#[tokio::test]
async fn stop_with_a_refresh_pending_keeps_the_batch() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_statistics_stop").await?;
    let chain = "interpret-statistics-stop";
    stopped_at_completion(&scratch, chain).await?;
    let kept: (bool, Option<i64>, Option<i64>) = sqlx::query_as(
        "SELECT redo_in_progress, redo_current_block_number, redo_target_block_number
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(kept, (true, Some(2), Some(2)));
    let rewritten: Vec<i64> = sqlx::query_scalar(
        "SELECT block_number FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'RegistrationGranted' ORDER BY block_number",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(rewritten, [1, 2]);
    scratch.cleanup().await
}

/// The restart after that stop: a new process has no counters, finds the redo at its target,
/// completes it with no block left to interpret, and refreshes.
#[tokio::test]
async fn resumed_redo_at_its_target_refreshes_on_completion() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_statistics_resume").await?;
    let chain = "interpret-statistics-resume";
    stopped_at_completion(&scratch, chain).await?;

    let phase = ObservedInterpret::new(&scratch, None);
    let (result, logs) = redo(&scratch, chain, phase.clone(), CancellationToken::new()).await?;
    result?;
    assert_eq!(
        phase.steps(),
        [
            "batch through Some(2) complete=true",
            "hook with progress at Some(2)",
        ]
    );
    assert_eq!(logs.matches(REFRESHED).count(), 1, "{logs}");
    assert!(analyzed_rows(scratch.pool()).await? > 0.0);
    let redo_in_progress: bool = sqlx::query_scalar(
        "SELECT redo_in_progress FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert!(!redo_in_progress);
    drop(phase);
    scratch.cleanup().await
}

/// A batch whose write rolls back feeds nothing: the engine that failed has no refresh
/// waiting, while the engine that wrote the same ENSv2 events has.
#[tokio::test]
async fn failed_batch_does_not_feed_the_schedule() -> Result<()> {
    let scratch = ScratchDatabase::create("production_interpret_statistics_failed").await?;
    let chain = "interpret-statistics-failed";
    let other_chain = "interpret-statistics-failed-other";
    seed_v2_lifecycle_fixture(scratch.pool(), chain).await?;
    let request = || BatchRequest {
        chain_id: chain.to_owned(),
        from_block: 0,
        to_block: 2,
        resume_current: None,
        mode: InterpretRunMode::Normal,
    };
    let written = Engine::new(scratch.pool().clone());
    written.run_batch(request()).await?;
    assert!(written.statistics_refresh_due(chain));

    // The token lineage now belongs to another chain, which the write refuses.
    for table in ["normalized_events", "surface_bindings", "resources"] {
        sqlx::query(&format!("DELETE FROM {table} WHERE chain_id = $1"))
            .bind(chain)
            .execute(scratch.pool())
            .await?;
    }
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, block_number, block_timestamp, canonicality_state
         ) VALUES ($1, $2, 1, to_timestamp(1), 'canonical')",
    )
    .bind(other_chain)
    .bind(block_hash(other_chain, 1))
    .execute(scratch.pool())
    .await?;
    sqlx::query("UPDATE token_lineages SET chain_id = $2, block_hash = $3 WHERE chain_id = $1")
        .bind(chain)
        .bind(other_chain)
        .bind(block_hash(other_chain, 1))
        .execute(scratch.pool())
        .await?;
    let failed = Engine::new(scratch.pool().clone());
    let error = failed
        .run_batch(request())
        .await
        .expect_err("the write must fail");
    assert_eq!(error.kind(), InterpretErrorKind::DataIntegrity);
    assert!(!failed.statistics_refresh_due(chain));
    assert!(!failed.refresh_statistics(chain).await);
    drop((written, failed));
    scratch.cleanup().await
}
