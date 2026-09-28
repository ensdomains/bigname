#[allow(dead_code)]
mod support;

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::Result;
use clap::Parser;
use phase_runner::{
    INTERPRETER_CONTENT_HASH,
    capacity::CapacityGuard,
    cli::{Cli, ResolvedCommand},
    config::{CapacityConfig, ChainConfig, SeedBasis, SourceConfig, TimingConfig},
    phase::{
        AfterProgressFuture, BlockRange, CompletedPhaseFuture, LoopbackPhase, Phase,
        PhaseBatchOutcome, PhaseContext, PhaseFuture, PhaseName, PhaseSet, RunMode,
    },
    project_phase::FamilySettings,
    project_phase::ProjectPhase,
    runner::{PhaseRunner, RedoPhase},
    state::PhaseStore,
};
use tokio_util::sync::CancellationToken;

use support::{ScratchDatabase, seed_lineage};

const CHAIN: &str = "families-runner";

// The family loop runs after the batch's progress is recorded, in its own transactions. A failure
// there fails the Project run with a retryable error once that progress is recorded: the served
// redo's progress stands, the restart loop retries, and the families never block the batch. Here
// every family run fails until the trigger goes, so the command is stopped while it retries; the
// rerun that follows catches the families up.
#[tokio::test]
async fn a_failing_family_run_fails_project_after_its_progress_is_recorded() -> Result<()> {
    let scratch = ready("families_runner_failure").await?;
    refuse_marker_writes(&scratch, "TRUE").await?;

    let (error, failure) = redo_until_family_error(&scratch, default_project(&scratch), 3).await?;
    assert!(failure.contains("injected family failure"), "{failure}");
    assert!(
        failure.contains("stopped at no block") && failure.contains("served marker block 3 ("),
        "{failure}"
    );
    assert_eq!(
        redo_progress(&scratch).await?,
        (true, Some(3)),
        "the served redo recorded its progress before the families failed"
    );
    assert_eq!(marker(&scratch).await?, None, "the family loop was refused");
    assert!(error.to_string().contains("is incomplete"), "{error}");

    sqlx::query("DROP TRIGGER refuse_marker ON project_family_marker")
        .execute(scratch.pool())
        .await?;
    redo_project(&scratch).await?;
    assert_eq!(
        project_state(&scratch).await?,
        ("completed".into(), Some(3), false)
    );
    assert_eq!(marker(&scratch).await?, Some(3), "the rerun caught up");
    scratch.cleanup().await
}

// A schema whose family tables were never migrated: every family run fails, and the failure is
// the Project run's, retried by the restart loop, instead of a count beside a completed batch.
#[tokio::test]
async fn a_missing_family_migration_fails_project_and_is_retried() -> Result<()> {
    let scratch = ready("families_runner_missing").await?;
    sqlx::query("DROP TABLE project_family_marker")
        .execute(scratch.pool())
        .await?;
    let (_, failure) = redo_until_family_error(&scratch, default_project(&scratch), 3).await?;
    assert!(failure.contains("project_family_marker"), "{failure}");
    assert_eq!(redo_progress(&scratch).await?, (true, Some(3)));
    scratch.cleanup().await
}

// A family rebuild that needs more blocks than one run's budget: the batch starts one run after
// another until the families reach its served marker, so the one-shot redo, which no later batch
// follows, returns with the rebuild finished.
#[tokio::test]
async fn a_batch_runs_the_families_to_its_served_marker_across_budgets() -> Result<()> {
    let scratch = ready_through("families_runner_budget", 30).await?;
    seed_thirty_blocks_of_work(&scratch).await?;
    redo_project_through(
        &scratch,
        FamilySettings {
            max_blocks_per_run: 10,
            ..FamilySettings::default()
        },
        30,
    )
    .await?;
    assert_eq!(
        project_state(&scratch).await?,
        ("completed".into(), Some(30), false)
    );
    assert_eq!(
        marker(&scratch).await?,
        Some(30),
        "the family rebuild finished before the command returned"
    );
    assert!(repair_completed_for_current_attempt(&scratch).await?);
    scratch.cleanup().await
}

// A family run that stops short of the served marker, here on a failing block, fails the Project
// run with the block the families reached; the restart loop retries it, and once the block
// applies a rerun of the same redo repairs the families.
#[tokio::test]
async fn a_one_shot_redo_whose_families_stop_short_fails_and_a_rerun_repairs_them() -> Result<()> {
    let scratch = ready_through("families_runner_short", 30).await?;
    seed_thirty_blocks_of_work(&scratch).await?;
    refuse_marker_writes(&scratch, "NEW.current_block_number = 15").await?;
    let families = FamilySettings {
        max_blocks_per_run: 10,
        ..FamilySettings::default()
    };
    let project =
        Arc::new(ProjectPhase::new(scratch.pool().clone()).with_family_settings(families));
    let (_, failure) = redo_until_family_error(&scratch, project, 30).await?;
    assert_eq!(marker(&scratch).await?, Some(14), "block 15 was refused");
    assert!(failure.contains("injected family failure"), "{failure}");
    assert!(
        failure.contains("stopped at block 14 (") && failure.contains("served marker block 30 ("),
        "{failure}"
    );
    assert_eq!(redo_progress(&scratch).await?, (true, Some(30)));

    sqlx::query("DROP TRIGGER refuse_marker ON project_family_marker")
        .execute(scratch.pool())
        .await?;
    let project =
        Arc::new(ProjectPhase::new(scratch.pool().clone()).with_family_settings(families));
    redo_with_phase(&scratch, project, 30).await?;
    assert_eq!(marker(&scratch).await?, Some(30), "the rerun repaired them");
    assert!(repair_completed_for_current_attempt(&scratch).await?);
    scratch.cleanup().await
}

// A failed family run is a failure even when the family marker already stands on the served
// block: a redo that ends where the served marker was keeps its block and hash, so only the error
// shows the families never applied the redo attempt. Here the repair's first marker write fails
// before it commits, so the marker stays on block 30.
#[tokio::test]
async fn a_one_shot_redo_whose_family_run_fails_on_the_served_block_fails() -> Result<()> {
    let scratch = ready_through("families_runner_skip", 30).await?;
    seed_thirty_blocks_of_work(&scratch).await?;
    redo_project_through(&scratch, FamilySettings::default(), 30).await?;
    let before = marker_row(&scratch).await?;
    assert_eq!(before.0, Some(30), "the families stand on the served block");

    refuse_marker_writes(&scratch, "TRUE").await?;
    let (_, failure) = redo_until_family_error(&scratch, default_project(&scratch), 30).await?;
    assert_eq!(
        marker_row(&scratch).await?,
        before,
        "nothing family-side committed"
    );
    assert!(!repair_completed_for_current_attempt(&scratch).await?);
    assert!(
        failure.contains("stopped at block 30 (") && failure.contains("served marker block 30 ("),
        "{failure}"
    );

    sqlx::query("DROP TRIGGER refuse_marker ON project_family_marker")
        .execute(scratch.pool())
        .await?;
    redo_project_through(&scratch, FamilySettings::default(), 30).await?;
    assert!(repair_completed_for_current_attempt(&scratch).await?);
    scratch.cleanup().await
}

/// A trigger that refuses every write of the family marker for which `condition` holds.
async fn refuse_marker_writes(scratch: &ScratchDatabase, condition: &str) -> Result<()> {
    sqlx::raw_sql(&format!(
        "CREATE FUNCTION refuse_marker() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF {condition} THEN RAISE EXCEPTION 'injected family failure'; END IF;
             RETURN NEW;
         END $$;
         CREATE TRIGGER refuse_marker BEFORE INSERT OR UPDATE ON project_family_marker
         FOR EACH ROW EXECUTE FUNCTION refuse_marker();"
    ))
    .execute(scratch.pool())
    .await?;
    Ok(())
}

fn default_project(scratch: &ScratchDatabase) -> Arc<dyn Phase> {
    Arc::new(ProjectPhase::new(scratch.pool().clone()))
}

/// Run a Project redo through `head` until its family run has failed and the failure is recorded
/// on the Project row, then stop the command, which the restart loop would otherwise keep
/// retrying. Returns the command's error and the recorded failure.
async fn redo_until_family_error(
    scratch: &ScratchDatabase,
    project: Arc<dyn Phase>,
    head: i64,
) -> Result<(anyhow::Error, String)> {
    let stop = CancellationToken::new();
    let watch = async {
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            let failure: Option<String> = sqlx::query_scalar(
                "SELECT last_error FROM chain_phase_state
                 WHERE chain_id = $1 AND phase_name = 'project'",
            )
            .bind(CHAIN)
            .fetch_one(scratch.pool())
            .await?;
            if let Some(failure) = failure.filter(|failure| failure.contains("owned key families"))
            {
                stop.cancel();
                return Ok::<_, anyhow::Error>(failure);
            }
            anyhow::ensure!(
                std::time::Instant::now() < deadline,
                "no family failure was recorded"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    let (command, failure) = tokio::join!(
        redo_with_phase_and_stop(scratch, project, head, stop.clone()),
        watch
    );
    let failure = failure?;
    let error = command
        .err()
        .ok_or_else(|| anyhow::anyhow!("the redo completed despite the family failure"))?;
    Ok((error, failure))
}

/// Whether the Project row is in redo, and the redo's recorded progress.
async fn redo_progress(scratch: &ScratchDatabase) -> Result<(bool, Option<i64>)> {
    Ok(sqlx::query_as(
        "SELECT redo_in_progress, redo_current_block_number
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?)
}

// A block of the family run that sees the input revision change fails the Project batch with a
// retryable error. The restart loop runs the redo again and the families reach the served marker
// under the new revision.
// The revision moves by Interpret's redo attempt, once, when family block 15 commits.
#[tokio::test]
async fn a_revision_change_mid_run_fails_the_batch_and_the_restart_catches_up() -> Result<()> {
    let scratch = ready_through("families_runner_revision", 30).await?;
    seed_thirty_blocks_of_work(&scratch).await?;
    sqlx::raw_sql(
        "CREATE TABLE revision_moved (fired boolean NOT NULL);
         INSERT INTO revision_moved VALUES (false);
         CREATE FUNCTION move_revision() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF NOT (SELECT fired FROM revision_moved) THEN
                 UPDATE chain_phase_state
                 SET redo_attempt_generation = redo_attempt_generation + 1
                 WHERE chain_id = NEW.chain_id AND phase_name = 'interpret';
                 UPDATE revision_moved SET fired = true;
             END IF;
             RETURN NEW;
         END $$;
         CREATE TRIGGER move_revision AFTER UPDATE ON project_family_marker
         FOR EACH ROW WHEN (NEW.current_block_number = 15)
         EXECUTE FUNCTION move_revision();",
    )
    .execute(scratch.pool())
    .await?;

    redo_project_through(&scratch, FamilySettings::default(), 30).await?;
    let fired: bool = sqlx::query_scalar("SELECT fired FROM revision_moved")
        .fetch_one(scratch.pool())
        .await?;
    assert!(fired, "the revision moved during the family run");
    assert_eq!(
        project_state(&scratch).await?,
        ("completed".into(), Some(30), false)
    );
    assert_eq!(
        marker(&scratch).await?,
        Some(30),
        "the retried batch brought the families to the served marker"
    );
    assert!(repair_completed_for_current_attempt(&scratch).await?);
    scratch.cleanup().await
}

/// Whether the repair record completed the Project redo attempt now on the served row, on the
/// marker as it stands, at the served block. Marker height alone would not show it.
async fn repair_completed_for_current_attempt(scratch: &ScratchDatabase) -> Result<bool> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM project_repair_record record
             JOIN chain_phase_state project
               ON project.chain_id = record.chain_id AND project.phase_name = 'project'
             JOIN project_family_marker marker ON marker.chain_id = record.chain_id
             WHERE record.chain_id = $1 AND record.state = 'complete'
               AND record.attempt = project.redo_attempt_generation
               AND marker.project_redo_attempt = project.redo_attempt_generation
               AND record.completed_sequence = marker.sequence
               AND record.completed_marker_number = marker.current_block_number
               AND record.completed_marker_hash = marker.current_block_hash
               AND marker.current_block_number = project.current_block_number
               AND marker.current_block_hash = project.current_block_hash)",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?)
}

/// The family marker's block, hash and generation.
async fn marker_row(scratch: &ScratchDatabase) -> Result<(Option<i64>, Option<String>, i64)> {
    Ok(sqlx::query_as(
        "SELECT current_block_number, current_block_hash, sequence
         FROM project_family_marker WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?)
}

// A stop that arrives while the final served batch is being recorded abandons the family run
// before it commits anything, and the run ends as cancelled: the redo stays in progress with the
// served batch's progress recorded, the command fails, and an uncancelled rerun repairs the
// families.
#[tokio::test]
async fn a_stop_during_the_final_served_batch_fails_the_one_shot_redo_and_a_rerun_repairs_it()
-> Result<()> {
    let scratch = ready_through("families_runner_stop", 30).await?;
    seed_thirty_blocks_of_work(&scratch).await?;
    let families = FamilySettings {
        max_blocks_per_run: 10,
        ..FamilySettings::default()
    };
    let stop = CancellationToken::new();
    let (project, _) = ServedBatches::cancelling_at(
        ProjectPhase::new(scratch.pool().clone()).with_family_settings(families),
        1,
        stop.clone(),
    );
    let error = redo_with_phase_and_stop(&scratch, project, 30, stop)
        .await
        .expect_err("the stop abandoned the family run");
    assert_eq!(
        redo_progress(&scratch).await?,
        (true, Some(30)),
        "the served batch is recorded and the redo stays open"
    );
    assert_eq!(
        marker(&scratch).await?,
        None,
        "the family run committed nothing"
    );
    let message = error.to_string();
    assert!(message.contains("is incomplete"), "{message}");

    let project =
        Arc::new(ProjectPhase::new(scratch.pool().clone()).with_family_settings(families));
    redo_with_phase(&scratch, project, 30).await?;
    assert_eq!(marker(&scratch).await?, Some(30), "the rerun repaired them");
    assert!(repair_completed_for_current_attempt(&scratch).await?);
    scratch.cleanup().await
}

// The one-shot `redo` command, configured as the command line resolves it, keeps a family
// data-integrity failure's own kind: the command runs the served redo once, the families stop on
// the refused block, and the command exits with the family error recorded instead of running the
// served redo again forever. The redo stays in progress, so a rerun is admitted.
#[tokio::test]
async fn the_redo_command_exits_on_a_persistent_family_failure_after_one_served_redo() -> Result<()>
{
    let scratch = ready_through("families_runner_redo_exit", 30).await?;
    seed_thirty_blocks_of_work(&scratch).await?;
    refuse_marker_integrity(&scratch, 15).await?;
    let stop = CancellationToken::new();
    // A second served redo would be the retry this test rules out; the stop then ends the command.
    let (project, served) = ServedBatches::cancelling_at(
        ProjectPhase::new(scratch.pool().clone())
            .with_family_settings(redo_command_family_settings()?),
        2,
        stop.clone(),
    );
    let error = tokio::time::timeout(
        Duration::from_secs(60),
        redo_with_phase_and_stop(&scratch, project, 30, stop),
    )
    .await?
    .expect_err("the family failure fails the command");
    assert_eq!(
        served.load(Ordering::SeqCst),
        1,
        "the served redo ran once: {error}"
    );
    let message = error.to_string();
    assert!(
        message.contains("owned key families") && message.contains("injected family integrity"),
        "{message}"
    );
    assert_eq!(marker(&scratch).await?, Some(14), "block 15 was refused");
    assert_eq!(
        redo_progress(&scratch).await?,
        (true, Some(30)),
        "the served redo's progress stands and the redo stays in progress"
    );
    let recorded = last_error(&scratch).await?.unwrap_or_default();
    assert!(recorded.contains("owned key families"), "{recorded}");
    scratch.cleanup().await
}

// Under the supervised runner's settings the same data-integrity family failure is retried: the
// restart loop runs the served redo again and again while the block is refused, and once it
// applies the families catch up to the served marker.
#[tokio::test]
async fn a_family_integrity_failure_is_retried_under_the_supervised_settings_and_catches_up()
-> Result<()> {
    let scratch = ready_through("families_runner_supervised_retry", 30).await?;
    seed_thirty_blocks_of_work(&scratch).await?;
    refuse_marker_integrity(&scratch, 15).await?;
    let stop = CancellationToken::new();
    let (project, served) = ServedBatches::cancelling_at(
        ProjectPhase::new(scratch.pool().clone()).with_family_settings(FamilySettings {
            max_blocks_per_run: 10,
            ..FamilySettings::default()
        }),
        usize::MAX,
        stop.clone(),
    );
    let lift = async {
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        while served.load(Ordering::SeqCst) < 3 {
            anyhow::ensure!(
                std::time::Instant::now() < deadline,
                "the restart loop did not retry"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        sqlx::query("DROP TRIGGER refuse_marker ON project_family_marker")
            .execute(scratch.pool())
            .await?;
        Ok::<_, anyhow::Error>(())
    };
    let (command, lifted) =
        tokio::join!(redo_with_phase_and_stop(&scratch, project, 30, stop), lift);
    lifted?;
    command?;
    assert!(served.load(Ordering::SeqCst) >= 3);
    assert_eq!(
        project_state(&scratch).await?,
        ("completed".into(), Some(30), false)
    );
    assert_eq!(marker(&scratch).await?, Some(30), "the retries caught up");
    assert!(repair_completed_for_current_attempt(&scratch).await?);
    scratch.cleanup().await
}

// A family catch-up of several runs keeps the Project heartbeat fresh between runs, as a batch
// settlement does. The first run's block 10 holds its transaction past the heartbeat interval;
// block 11, the second run's first, reads how old the heartbeat is.
#[tokio::test]
async fn a_family_catch_up_of_several_runs_keeps_the_phase_heartbeat_fresh() -> Result<()> {
    let scratch = ready_through("families_runner_heartbeat", 30).await?;
    seed_thirty_blocks_of_work(&scratch).await?;
    sqlx::raw_sql(
        "CREATE TABLE heartbeat_probe (age_seconds float8 NOT NULL);
         CREATE FUNCTION probe_heartbeat() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF NEW.current_block_number = 10 THEN PERFORM pg_sleep(5.5); END IF;
             IF NEW.current_block_number = 11 THEN
                 INSERT INTO heartbeat_probe
                 SELECT extract(epoch FROM clock_timestamp() - heartbeat_at)
                 FROM service_heartbeats
                 WHERE chain_id = NEW.chain_id AND phase_name = 'project';
             END IF;
             RETURN NEW;
         END $$;
         CREATE TRIGGER probe_heartbeat AFTER INSERT OR UPDATE ON project_family_marker
         FOR EACH ROW EXECUTE FUNCTION probe_heartbeat();",
    )
    .execute(scratch.pool())
    .await?;
    redo_project_through(
        &scratch,
        FamilySettings {
            max_blocks_per_run: 10,
            ..FamilySettings::default()
        },
        30,
    )
    .await?;
    let ages: Vec<f64> = sqlx::query_scalar("SELECT age_seconds FROM heartbeat_probe")
        .fetch_all(scratch.pool())
        .await?;
    assert_eq!(ages.len(), 1, "block 11 read the heartbeat once: {ages:?}");
    assert!(
        ages[0] < 2.0,
        "the heartbeat was recorded after the first run, not before it: {} s old",
        ages[0]
    );
    assert_eq!(marker(&scratch).await?, Some(30));
    scratch.cleanup().await
}

// An input token that fails to read is the family run's failure, raised after the batch's
// progress is recorded, and the restart loop retries it. Here only the token read fails: the
// Project phase reads `chain_phase_state` through a view whose `last_error` column, which only the
// token read selects, raises while armed.
#[tokio::test]
async fn a_failed_input_token_read_fails_project_after_its_progress_and_is_retried() -> Result<()> {
    let scratch = ready("families_runner_token").await?;
    let search_path: String = sqlx::query_scalar("SELECT current_setting('search_path')")
        .fetch_one(scratch.pool())
        .await?;
    sqlx::raw_sql(
        "CREATE SCHEMA token_fault;
         CREATE TABLE token_fault.armed (armed boolean NOT NULL);
         INSERT INTO token_fault.armed VALUES (true);
         CREATE FUNCTION token_fault.guard(value text) RETURNS text LANGUAGE plpgsql STABLE AS $$
         BEGIN
             IF (SELECT armed FROM token_fault.armed) THEN
                 RAISE EXCEPTION 'injected token read failure';
             END IF;
             RETURN value;
         END $$;
         DO $$
         DECLARE columns text;
         BEGIN
             SELECT string_agg(CASE WHEN attname = 'last_error'
                                    THEN 'token_fault.guard(last_error) AS last_error'
                                    ELSE quote_ident(attname) END,
                               ', ' ORDER BY attnum)
             INTO columns
             FROM pg_attribute
             WHERE attrelid = 'chain_phase_state'::regclass AND attnum > 0 AND NOT attisdropped;
             EXECUTE format('CREATE VIEW token_fault.chain_phase_state AS SELECT %s FROM %s',
                            columns, 'chain_phase_state'::regclass);
         END $$;",
    )
    .execute(scratch.pool())
    .await?;
    let search_path = format!("token_fault,{}", search_path.replace(' ', ""));
    let project_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect_with(
            scratch
                .writer_connect_options()
                .options([("search_path", search_path.as_str())]),
        )
        .await?;
    let project: Arc<dyn Phase> = Arc::new(ProjectPhase::new(project_pool.clone()));

    let observed = async {
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(failure) = last_error(&scratch)
                .await?
                .filter(|failure| failure.contains("owned key families"))
            {
                let progress = redo_progress(&scratch).await?;
                let family = marker(&scratch).await?;
                sqlx::query("UPDATE token_fault.armed SET armed = false")
                    .execute(scratch.pool())
                    .await?;
                return Ok::<_, anyhow::Error>((failure, progress, family));
            }
            anyhow::ensure!(
                std::time::Instant::now() < deadline,
                "no token read failure was recorded"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    let (command, observed) = tokio::join!(
        redo_with_phase_and_stop(&scratch, project, 3, CancellationToken::new()),
        observed
    );
    let (failure, progress, family) = observed?;
    command?;
    assert!(
        failure.contains("failed to read the family input token")
            && failure.contains("injected token read failure"),
        "{failure}"
    );
    assert_eq!(
        progress,
        (true, Some(3)),
        "the served redo recorded its progress before the token failure surfaced"
    );
    assert_eq!(family, None, "no family run started without a token");
    assert_eq!(
        project_state(&scratch).await?,
        ("completed".into(), Some(3), false),
        "the retry finished the redo"
    );
    assert_eq!(
        marker(&scratch).await?,
        Some(3),
        "the retry ran the families"
    );
    project_pool.close().await;
    scratch.cleanup().await
}

/// A trigger that refuses the family marker's move to `block` with a data-integrity error.
async fn refuse_marker_integrity(scratch: &ScratchDatabase, block: i64) -> Result<()> {
    sqlx::raw_sql(&format!(
        "CREATE FUNCTION refuse_marker() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             RAISE EXCEPTION 'injected family integrity failure' USING ERRCODE = 'check_violation';
         END $$;
         CREATE TRIGGER refuse_marker BEFORE INSERT OR UPDATE ON project_family_marker
         FOR EACH ROW WHEN (NEW.current_block_number = {block})
         EXECUTE FUNCTION refuse_marker();"
    ))
    .execute(scratch.pool())
    .await?;
    Ok(())
}

/// The family settings the one-shot `redo` command resolves from its command line; they do not
/// depend on the phase redone.
fn redo_command_family_settings() -> Result<FamilySettings> {
    let command = Cli::try_parse_from([
        "phase-runner",
        "redo",
        "--database-url",
        "postgres://phase-runner.invalid/fresh",
        "--all-chains",
        "--phase",
        "recompute-flags",
        "--from-block",
        "0",
        "--to-block",
        "30",
        "--project-families-max-blocks",
        "10",
    ])?
    .resolve()?;
    let ResolvedCommand::Redo {
        project_families, ..
    } = command
    else {
        anyhow::bail!("expected the redo command");
    };
    Ok(project_families)
}

async fn last_error(scratch: &ScratchDatabase) -> Result<Option<String>> {
    Ok(sqlx::query_scalar(
        "SELECT last_error FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?)
}

/// Project, counting the served batches that complete and raising a stop as soon as the
/// `cancel_at`th returns, so the stop is pending while the runner records that batch and wins
/// the select against the family run.
struct ServedBatches {
    inner: ProjectPhase,
    served: Arc<AtomicUsize>,
    cancel_at: usize,
    stop: CancellationToken,
}

impl ServedBatches {
    fn cancelling_at(
        inner: ProjectPhase,
        cancel_at: usize,
        stop: CancellationToken,
    ) -> (Arc<dyn Phase>, Arc<AtomicUsize>) {
        let served = Arc::new(AtomicUsize::new(0));
        let phase = Arc::new(Self {
            inner,
            served: Arc::clone(&served),
            cancel_at,
            stop,
        });
        (phase, served)
    }
}

impl Phase for ServedBatches {
    fn name(&self) -> PhaseName {
        self.inner.name()
    }

    fn preflight(
        &self,
        chain_id: &str,
        sources: &[SourceConfig],
        mode: &RunMode,
    ) -> phase_runner::error::RunnerResult<()> {
        self.inner.preflight(chain_id, sources, mode)
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            let outcome = self.inner.run_batch(context).await;
            if matches!(outcome, Ok(PhaseBatchOutcome::Complete(_)))
                && self.served.fetch_add(1, Ordering::SeqCst) + 1 >= self.cancel_at
            {
                self.stop.cancel();
            }
            outcome
        })
    }

    fn after_progress_recorded(&self, chain_id: &str) -> AfterProgressFuture<'_> {
        self.inner.after_progress_recorded(chain_id)
    }

    fn after_redo(&self, chain_id: &str) -> phase_runner::error::RunnerResult<()> {
        self.inner.after_redo(chain_id)
    }

    fn revalidates_completed(
        &self,
        chain_id: &str,
        sources: &[SourceConfig],
    ) -> phase_runner::error::RunnerResult<bool> {
        self.inner.revalidates_completed(chain_id, sources)
    }

    fn revalidate_completed(&self, context: PhaseContext) -> CompletedPhaseFuture<'_> {
        self.inner.revalidate_completed(context)
    }
}

/// One event per block 1 to 30, so a family rebuild has thirty blocks of work.
async fn seed_thirty_blocks_of_work(scratch: &ScratchDatabase) -> Result<()> {
    sqlx::query(
        "INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
             manifest_version, chain_id, block_number, block_hash, derivation_kind,
             canonicality_state, after_state)
         SELECT 'budget:' || block, 'ens', 'PreimageObserved', 'budget_probe', 1, $1, block,
                $1 || '-block-' || block, 'ens_v2_registry_resource_surface', 'canonical', '{}'
         FROM generate_series(1, 30) block",
    )
    .bind(CHAIN)
    .execute(scratch.pool())
    .await?;
    Ok(())
}

async fn ready(prefix: &str) -> Result<ScratchDatabase> {
    ready_through(prefix, 3).await
}

async fn ready_through(prefix: &str, head: i64) -> Result<ScratchDatabase> {
    let scratch = ScratchDatabase::create(prefix).await?;
    seed_lineage(scratch.pool(), CHAIN, head).await?;
    sqlx::query("UPDATE chain_lineage SET canonicality_state = 'canonical' WHERE chain_id = $1")
        .bind(CHAIN)
        .execute(scratch.pool())
        .await?;
    PhaseStore::new(scratch.pool().clone())
        .initialize_chain(CHAIN)
        .await?;
    seed_completed_extent(scratch.pool(), head).await?;
    Ok(scratch)
}

async fn redo_project(scratch: &ScratchDatabase) -> Result<()> {
    redo_project_with(scratch, FamilySettings::default()).await
}

async fn redo_project_with(scratch: &ScratchDatabase, families: FamilySettings) -> Result<()> {
    redo_project_through(scratch, families, 3).await
}

async fn redo_project_through(
    scratch: &ScratchDatabase,
    families: FamilySettings,
    head: i64,
) -> Result<()> {
    let project =
        Arc::new(ProjectPhase::new(scratch.pool().clone()).with_family_settings(families));
    redo_with_phase(scratch, project, head).await
}

async fn redo_with_phase(
    scratch: &ScratchDatabase,
    project: Arc<ProjectPhase>,
    head: i64,
) -> Result<()> {
    redo_with_phase_and_stop(scratch, project, head, CancellationToken::new()).await
}

async fn redo_with_phase_and_stop(
    scratch: &ScratchDatabase,
    project: Arc<dyn Phase>,
    head: i64,
    stop: CancellationToken,
) -> Result<()> {
    PhaseRunner::new(
        scratch.runner(),
        PhaseSet::with_ingest_interpret_and_project(
            Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
            Arc::new(LoopbackPhase::new(PhaseName::Interpret)),
            project,
        )?,
        CapacityGuard::system(CapacityConfig::default()),
        "families-runner",
        TimingConfig {
            initial_backoff: Duration::from_millis(1),
            maximum_backoff: Duration::from_millis(4),
            live_poll_interval: Duration::from_millis(1),
        },
    )?
    .redo(
        &chain_config()?,
        RedoPhase::Phase(PhaseName::Project),
        BlockRange::new(0, head)?,
        stop,
    )
    .await?;
    Ok(())
}

async fn project_state(scratch: &ScratchDatabase) -> Result<(String, Option<i64>, bool)> {
    Ok(sqlx::query_as(
        "SELECT phase_status, current_block_number, redo_in_progress
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?)
}

async fn marker(scratch: &ScratchDatabase) -> Result<Option<i64>> {
    Ok(sqlx::query_scalar(
        "SELECT current_block_number FROM project_family_marker WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .fetch_optional(scratch.pool())
    .await?
    .flatten())
}

async fn seed_completed_extent(pool: &sqlx::PgPool, head: i64) -> Result<()> {
    let hash = format!("{CHAIN}-block-{head}");
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed',
             current_block_number = $2, current_block_hash = $3,
             target_block_number = $2, target_block_hash = $3,
             live_handoff_block_number = CASE WHEN phase_name = 'ingest' THEN $2 END,
             live_handoff_block_hash = CASE WHEN phase_name = 'ingest' THEN $3 END,
             input_content_hash = CASE
                 WHEN phase_name IN ('interpret', 'project') THEN $4
             END,
             started_at = now(), finished_at = now()
         WHERE chain_id = $1 AND phase_name IN ('ingest', 'interpret', 'project')",
    )
    .bind(CHAIN)
    .bind(head)
    .bind(&hash)
    .bind(INTERPRETER_CONTENT_HASH)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO ingest_cursors (
             chain_id, source_key, source_kind, seed_basis, start_block_number,
             next_block_number, target_block_number, last_processed_block_number,
             last_processed_block_hash
         ) VALUES ($1, 'source', 'test', 'new_signature_range', 0, $2, $3, $3, $4)",
    )
    .bind(CHAIN)
    .bind(head + 1)
    .bind(head)
    .bind(hash)
    .execute(pool)
    .await?;
    Ok(())
}

fn chain_config() -> phase_runner::error::RunnerResult<ChainConfig> {
    ChainConfig::new(
        CHAIN,
        vec![SourceConfig::new(
            CHAIN,
            "source",
            "test",
            SeedBasis::NewSignatureRange,
            0,
            "http://source.invalid",
        )?],
        false,
    )
}
