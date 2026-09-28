#[allow(dead_code)]
mod support;

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::Result;
use clap::Parser;
use phase_runner::{
    INTERPRETER_CONTENT_HASH,
    capacity::{CapacityFuture, CapacityGuard, CapacityMeasurement, CapacityProbe},
    cli::{Cli, ResolvedCommand},
    config::{CapacityConfig, ChainConfig, SeedBasis, SourceConfig, TimingConfig},
    database::RunnerDatabase,
    error::{ErrorKind, RunnerError},
    phase::{
        AfterProgress, AfterProgressFuture, BlockRange, CompletedPhaseFuture, LoopbackPhase, Phase,
        PhaseBatchOutcome, PhaseContext, PhaseFuture, PhaseName, PhaseResume, PhaseSet, RunMode,
    },
    project_phase::FamilySettings,
    project_phase::ProjectPhase,
    runner::{PhaseRunner, RedoPhase},
    state::PhaseStore,
};
use tokio::sync::Notify;
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
/// retrying: these runs take the supervised family settings, which retry every family failure.
/// Returns the command's error and the recorded failure.
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
    let (command, failure) = with_watcher(
        redo_with_phase_and_stop(scratch, project, head, stop.clone()),
        watch,
    )
    .await?;
    let error = command
        .err()
        .ok_or_else(|| anyhow::anyhow!("the redo completed despite the family failure"))?;
    Ok((error, failure))
}

/// Runs a runner command beside a watcher that observes or steers it, the pair bounded by one
/// outer deadline, so a stalled statement on either side fails the test instead of hanging it. A
/// watcher failure ends the pair at once, abandoning the command; the command's own result is
/// returned as data, since some tests expect it to fail.
async fn with_watcher<T>(
    command: impl std::future::Future<Output = Result<()>>,
    watcher: impl std::future::Future<Output = Result<T>>,
) -> Result<(Result<()>, T)> {
    let paired = async {
        tokio::pin!(command);
        tokio::pin!(watcher);
        tokio::select! {
            watched = &mut watcher => {
                let watched = watched?;
                Ok((command.await, watched))
            }
            done = &mut command => Ok((done, watcher.await?)),
        }
    };
    tokio::time::timeout(Duration::from_secs(120), paired)
        .await
        .map_err(|_| anyhow::anyhow!("the command and its watcher did not end within 120 s"))?
}

/// Runs a whole test case, from its setup through its last assertion and its cleanup, under one
/// deadline, so a stalled statement anywhere in it fails the test instead of hanging it.
async fn within_case_deadline<T>(case: impl std::future::Future<Output = Result<T>>) -> Result<T> {
    tokio::time::timeout(Duration::from_secs(120), case)
        .await
        .map_err(|_| anyhow::anyhow!("the test case did not finish within 120 s"))?
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

// A stop that arrives while the final served batch is being recorded means no family run starts,
// and the run ends as cancelled: the redo stays in progress with the served batch's progress
// recorded, the command fails, and an uncancelled rerun repairs the families.
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
        .expect_err("the stop left the redo incomplete");
    assert_eq!(
        redo_progress(&scratch).await?,
        (true, Some(30)),
        "the served batch is recorded and the redo stays open"
    );
    assert_eq!(marker(&scratch).await?, None, "no family run started");
    let message = error.to_string();
    assert!(message.contains("is incomplete"), "{message}");

    let project =
        Arc::new(ProjectPhase::new(scratch.pool().clone()).with_family_settings(families));
    redo_with_phase(&scratch, project, 30).await?;
    assert_eq!(marker(&scratch).await?, Some(30), "the rerun repaired them");
    assert!(repair_completed_for_current_attempt(&scratch).await?);
    scratch.cleanup().await
}

// The one-shot `redo` command, configured as the command line resolves it, does not retry a
// family failure of any kind: the command runs the served redo once, the families stop on the
// refused block, and the command exits with the family error recorded, keeping its own kind,
// instead of running the served redo again forever. The redo stays in progress, so a rerun is
// admitted. A plain `RAISE EXCEPTION` is a transient failure, like Interpret being in redo.
#[tokio::test]
async fn the_redo_command_exits_on_a_transient_family_failure_after_one_served_redo() -> Result<()>
{
    redo_command_exits_after_one_served_redo(
        "families_runner_redo_exit_transient",
        "",
        ErrorKind::Transient,
    )
    .await
}

#[tokio::test]
async fn the_redo_command_exits_on_a_family_integrity_failure_after_one_served_redo() -> Result<()>
{
    redo_command_exits_after_one_served_redo(
        "families_runner_redo_exit_integrity",
        INTEGRITY,
        ErrorKind::DataIntegrity,
    )
    .await
}

async fn redo_command_exits_after_one_served_redo(
    prefix: &str,
    using: &str,
    kind: ErrorKind,
) -> Result<()> {
    let scratch = ready_through(prefix, 30).await?;
    seed_thirty_blocks_of_work(&scratch).await?;
    refuse_marker_at(&scratch, 15, using).await?;
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
        message.contains("owned key families") && message.contains("injected family failure"),
        "{message}"
    );
    let runner_error = error
        .downcast_ref::<RunnerError>()
        .ok_or_else(|| anyhow::anyhow!("not a runner error: {error}"))?;
    assert_eq!(runner_error.kind(), kind, "the failure keeps its own kind");
    assert!(!runner_error.is_retryable());
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
    refuse_marker_at(&scratch, 15, INTEGRITY).await?;
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
    let (command, ()) =
        with_watcher(redo_with_phase_and_stop(&scratch, project, 30, stop), lift).await?;
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
// settlement does. The first run's block 10 reads the heartbeat and then holds its transaction
// past the heartbeat interval, so the throttle is due; block 11, the second run's first, reads it
// again and finds it newer.
#[tokio::test]
async fn a_family_catch_up_of_several_runs_keeps_the_phase_heartbeat_fresh() -> Result<()> {
    let scratch = ready_through("families_runner_heartbeat", 30).await?;
    seed_thirty_blocks_of_work(&scratch).await?;
    sqlx::raw_sql(
        "CREATE TABLE heartbeat_probe (block_number bigint NOT NULL, heartbeat_epoch float8);
         CREATE FUNCTION probe_heartbeat() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF NEW.current_block_number IN (10, 11) THEN
                 INSERT INTO heartbeat_probe
                 SELECT NEW.current_block_number,
                        (SELECT extract(epoch FROM heartbeat_at)::float8 FROM service_heartbeats
                         WHERE chain_id = NEW.chain_id AND phase_name = 'project');
             END IF;
             IF NEW.current_block_number = 10 THEN PERFORM pg_sleep(5.5); END IF;
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
    let reads: Vec<(i64, Option<f64>)> = sqlx::query_as(
        "SELECT block_number, heartbeat_epoch FROM heartbeat_probe ORDER BY block_number",
    )
    .fetch_all(scratch.pool())
    .await?;
    let [(10, Some(at_block_10)), (11, Some(at_block_11))] = reads.as_slice() else {
        anyhow::bail!("blocks 10 and 11 each read the heartbeat once: {reads:?}");
    };
    assert!(
        at_block_11 > at_block_10,
        "the heartbeat was recorded between the runs: {at_block_10} then {at_block_11}"
    );
    assert_eq!(marker(&scratch).await?, Some(30));
    scratch.cleanup().await
}

// A family catch-up of several runs re-enters the capacity guard before each run, as the runner
// does before each batch. The probe reports the database over its ceiling once the first run has
// moved the family marker, so the phase pauses with the marker where that run left it; once the
// probe clears, the phase resumes and the remaining runs finish the rebuild.
#[tokio::test]
async fn a_family_catch_up_pauses_between_runs_while_capacity_is_breached() -> Result<()> {
    within_case_deadline(async {
        capacity_pause_case(
            "families_runner_capacity",
            |_, marker| marker.is_some_and(|block| block < 30),
            "the guard paused the catch-up after a run, short of block 30",
        )
        .await
    })
    .await
}

// The first family run after a served batch is guarded too: the probe admits the batch, then
// reports the database over its ceiling before any family block exists, so the phase pauses with
// no marker and the first run starts only once the probe clears.
#[tokio::test]
async fn the_first_family_run_after_a_batch_waits_while_capacity_is_breached() -> Result<()> {
    within_case_deadline(async {
        capacity_pause_case(
            "families_runner_capacity_first",
            |call, marker| call > 1 && marker.is_none(),
            "the guard paused before the first family run",
        )
        .await
    })
    .await
}

// A family run that starts within the capacity poll interval of the last measurement reuses it
// when it showed room, rather than probing again: here the poll interval is a minute, so the
// batch prelude's probe covers all three family runs of the catch-up.
#[tokio::test]
async fn family_runs_reuse_a_fresh_capacity_measurement() -> Result<()> {
    within_case_deadline(async {
        let scratch = ready_through("families_runner_capacity_reuse", 30).await?;
        seed_thirty_blocks_of_work(&scratch).await?;
        let probe = Arc::new(TrippingProbe::new(|_, _| false));
        let capacity = CapacityGuard::new(
            CapacityConfig {
                database_max_bytes: Some(1 << 40),
                poll_interval: Duration::from_secs(60),
                ..CapacityConfig::default()
            },
            probe.clone(),
        );
        let project = Arc::new(
            ProjectPhase::new(scratch.pool().clone()).with_family_settings(FamilySettings {
                max_blocks_per_run: 10,
                ..FamilySettings::default()
            }),
        );
        redo_with_capacity(
            scratch.runner(),
            project,
            30,
            CancellationToken::new(),
            capacity,
            Measure::ReuseFresh,
        )
        .await?;
        assert_eq!(marker(&scratch).await?, Some(30));
        assert_eq!(
            probe.calls.load(Ordering::SeqCst),
            1,
            "only the batch prelude probed; the family runs reused its measurement"
        );
        scratch.cleanup().await
    })
    .await
}

// With reuse on, a reading that has aged past the poll interval is measured again: the first
// family run's block 10 holds its transaction past the 50 ms interval, so the check before the
// second run probes afresh and sees the breach. The phase stays paused, probing afresh at every
// poll, and resumes once a fresh reading shows room.
#[tokio::test]
async fn an_aged_reading_is_measured_again_and_a_breach_pauses_until_a_fresh_reading_clears()
-> Result<()> {
    within_case_deadline(async {
        let scratch = ready_through("families_runner_capacity_aged", 30).await?;
        seed_thirty_blocks_of_work(&scratch).await?;
        sqlx::raw_sql(
            "CREATE FUNCTION hold_block_10() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 PERFORM pg_sleep(0.2);
                 RETURN NEW;
             END $$;
             CREATE TRIGGER hold_block_10 BEFORE INSERT OR UPDATE ON project_family_marker
             FOR EACH ROW WHEN (NEW.current_block_number = 10)
             EXECUTE FUNCTION hold_block_10();",
        )
        .execute(scratch.pool())
        .await?;
        let probe = Arc::new(TrippingProbe::new(|_, marker| {
            marker.is_some_and(|block| block < 30)
        }));
        let capacity = CapacityGuard::new(
            CapacityConfig {
                database_max_bytes: Some(1 << 40),
                poll_interval: Duration::from_millis(50),
                ..CapacityConfig::default()
            },
            probe.clone(),
        );
        let project = Arc::new(
            ProjectPhase::new(scratch.pool().clone()).with_family_settings(FamilySettings {
                max_blocks_per_run: 10,
                ..FamilySettings::default()
            }),
        );
        let observe = async {
            probe.breached.notified().await;
            let paused_at = marker(&scratch).await?;
            anyhow::ensure!(
                paused_at == Some(10),
                "paused after the first run: {paused_at:?}"
            );
            while project_state(&scratch).await?.0 != "paused" {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let probed = probe.calls.load(Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(300)).await;
            anyhow::ensure!(
                probe.calls.load(Ordering::SeqCst) > probed,
                "the paused phase probed afresh at its polls"
            );
            anyhow::ensure!(marker(&scratch).await? == paused_at, "no run while paused");
            anyhow::ensure!(project_state(&scratch).await?.0 == "paused");
            probe.released.store(true, Ordering::SeqCst);
            Ok(())
        };
        let (command, ()) = with_watcher(
            redo_with_capacity(
                scratch.runner(),
                project,
                30,
                CancellationToken::new(),
                capacity,
                Measure::ReuseFresh,
            ),
            observe,
        )
        .await?;
        command?;
        assert_eq!(
            project_state(&scratch).await?,
            ("completed".into(), Some(30), false)
        );
        assert_eq!(
            marker(&scratch).await?,
            Some(30),
            "the resumed runs finished"
        );
        scratch.cleanup().await
    })
    .await
}

/// Runs a Project redo through block 30 in family runs of ten blocks, under a probe that reports
/// the database over its ceiling while `over(call, marker)` holds, until released. The runner
/// probes at every check, as if each measurement had aged past the poll interval, so the probe is
/// asked before each family run. The watcher
/// waits for the breach, requires the phase paused with the family marker standing still, then
/// releases the probe; the redo must then finish the rebuild.
async fn capacity_pause_case(
    prefix: &str,
    over: fn(usize, Option<i64>) -> bool,
    paused_where: &str,
) -> Result<()> {
    let scratch = ready_through(prefix, 30).await?;
    seed_thirty_blocks_of_work(&scratch).await?;
    let probe = Arc::new(TrippingProbe::new(over));
    let capacity = CapacityGuard::new(
        CapacityConfig {
            database_max_bytes: Some(1 << 40),
            poll_interval: Duration::from_millis(5),
            ..CapacityConfig::default()
        },
        probe.clone(),
    );
    let project = Arc::new(
        ProjectPhase::new(scratch.pool().clone()).with_family_settings(FamilySettings {
            max_blocks_per_run: 10,
            ..FamilySettings::default()
        }),
    );
    let observe = async {
        probe.breached.notified().await;
        let paused_at = marker(&scratch).await?;
        anyhow::ensure!(over(2, paused_at), "{paused_where}: marker {paused_at:?}");
        while project_state(&scratch).await?.0 != "paused" {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        anyhow::ensure!(
            marker(&scratch).await? == paused_at,
            "a family run started while the database was over its ceiling"
        );
        anyhow::ensure!(project_state(&scratch).await?.0 == "paused");
        probe.released.store(true, Ordering::SeqCst);
        Ok(())
    };
    let (command, ()) = with_watcher(
        redo_with_capacity(
            scratch.runner(),
            project,
            30,
            CancellationToken::new(),
            capacity,
            Measure::EveryCheck,
        ),
        observe,
    )
    .await?;
    command?;
    assert_eq!(
        project_state(&scratch).await?,
        ("completed".into(), Some(30), false)
    );
    assert_eq!(
        marker(&scratch).await?,
        Some(30),
        "the resumed runs finished the rebuild"
    );
    scratch.cleanup().await
}

/// Reports the database over any ceiling while `over(call, marker)` holds for the probe's
/// one-based call number and the family marker, until released.
struct TrippingProbe {
    over: fn(usize, Option<i64>) -> bool,
    calls: AtomicUsize,
    breached: Notify,
    released: AtomicBool,
}

impl TrippingProbe {
    fn new(over: fn(usize, Option<i64>) -> bool) -> Self {
        Self {
            over,
            calls: AtomicUsize::new(0),
            breached: Notify::new(),
            released: AtomicBool::new(false),
        }
    }
}

impl CapacityProbe for TrippingProbe {
    fn measure<'a>(
        &'a self,
        pool: &'a sqlx::PgPool,
        _writable_path: &'a std::path::Path,
    ) -> CapacityFuture<'a> {
        Box::pin(async move {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            let marker: Option<i64> = sqlx::query_scalar(
                "SELECT current_block_number FROM project_family_marker WHERE chain_id = $1",
            )
            .bind(CHAIN)
            .fetch_optional(pool)
            .await
            .map_err(|error| RunnerError::transient(format!("probe marker read: {error}")))?
            .flatten();
            let over = !self.released.load(Ordering::SeqCst) && (self.over)(call, marker);
            if over {
                self.breached.notify_one();
            }
            Ok(CapacityMeasurement {
                database_size_bytes: if over { u64::MAX } else { 0 },
                free_disk_bytes: u64::MAX,
            })
        })
    }
}

// A stop raised while the served batch settles is seen by the loop's maintenance step before the
// first hook call, so no family run is even planned after the stop.
#[tokio::test]
async fn a_stop_before_the_after_progress_loop_calls_no_hook() -> Result<()> {
    within_case_deadline(async {
        let scratch = ready("families_runner_hook_precancelled").await?;
        let stop = CancellationToken::new();
        let (phase, probe) = CountingHook::stopping(HookStop::InBatch, stop.clone());
        let error = redo_with_phase_and_stop(&scratch, phase, 3, stop)
            .await
            .expect_err("the stop left the redo incomplete");
        assert!(error.to_string().contains("is incomplete"), "{error}");
        assert_eq!(
            probe.calls.load(Ordering::SeqCst),
            0,
            "no hook call after the stop"
        );
        scratch.cleanup().await
    })
    .await
}

// A stop raised during one family run, which asks for another, is seen by the maintenance step
// (lock check, heartbeat, capacity) that precedes the next hook call, so that call never happens.
#[tokio::test]
async fn a_stop_between_family_runs_prevents_the_next_hook_call() -> Result<()> {
    within_case_deadline(async {
        let scratch = ready("families_runner_hook_between").await?;
        let stop = CancellationToken::new();
        let (phase, probe) = CountingHook::stopping(HookStop::InHook, stop.clone());
        let error = redo_with_phase_and_stop(&scratch, phase, 3, stop)
            .await
            .expect_err("the stop left the redo incomplete");
        assert!(error.to_string().contains("is incomplete"), "{error}");
        assert_eq!(
            probe.calls.load(Ordering::SeqCst),
            1,
            "the hook ran once, before the stop"
        );
        scratch.cleanup().await
    })
    .await
}

// A stop raised after the maintenance step, at the work check right before the next hook call,
// is observed by the loop's cancel-first select: the second hook is never built.
#[tokio::test]
async fn a_stop_at_the_work_check_prevents_building_the_next_hook() -> Result<()> {
    within_case_deadline(async {
        let scratch = ready("families_runner_hook_work_check").await?;
        let stop = CancellationToken::new();
        let (phase, probe) = CountingHook::stopping(HookStop::AtWorkCheck, stop.clone());
        let error = redo_with_phase_and_stop(&scratch, phase, 3, stop)
            .await
            .expect_err("the stop left the redo incomplete");
        assert!(error.to_string().contains("is incomplete"), "{error}");
        assert!(
            probe.checkpoint.load(Ordering::SeqCst),
            "the work check after the first run was reached"
        );
        assert_eq!(
            probe.calls.load(Ordering::SeqCst),
            1,
            "only the first hook was built"
        );
        scratch.cleanup().await
    })
    .await
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum HookStop {
    /// As the served batch returns.
    InBatch,
    /// Inside the first hook call.
    InHook,
    /// At the work check that follows the first hook, which returns `More`.
    AtWorkCheck,
}

/// What a [`CountingHook`] observed: its hook calls, counted when the hook is called rather than
/// when its future is polled, and whether the work check after the first call was reached.
#[derive(Default)]
struct HookProbe {
    calls: AtomicUsize,
    checkpoint: AtomicBool,
}

/// A loopback Project whose after-progress hook asks for one more run after the first and reports
/// work waiting until its second call. The stop is raised where `stop_at` says.
struct CountingHook {
    inner: LoopbackPhase,
    probe: Arc<HookProbe>,
    stop_at: HookStop,
    stop: CancellationToken,
}

impl CountingHook {
    fn stopping(stop_at: HookStop, stop: CancellationToken) -> (Arc<dyn Phase>, Arc<HookProbe>) {
        let probe = Arc::new(HookProbe::default());
        let phase = Arc::new(Self {
            inner: LoopbackPhase::new(PhaseName::Project),
            probe: Arc::clone(&probe),
            stop_at,
            stop,
        });
        (phase, probe)
    }
}

impl Phase for CountingHook {
    fn name(&self) -> PhaseName {
        PhaseName::Project
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            let outcome = self.inner.run_batch(context).await;
            if self.stop_at == HookStop::InBatch {
                self.stop.cancel();
            }
            outcome
        })
    }

    fn has_after_progress_work(&self, _chain_id: &str) -> bool {
        let calls = self.probe.calls.load(Ordering::SeqCst);
        if self.stop_at == HookStop::AtWorkCheck && calls == 1 {
            self.probe.checkpoint.store(true, Ordering::SeqCst);
            self.stop.cancel();
        }
        calls < 2
    }

    fn after_progress_recorded(&self, _chain_id: &str) -> AfterProgressFuture<'_> {
        let call = self.probe.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if self.stop_at == HookStop::InHook {
            self.stop.cancel();
        }
        let step = if call == 1 {
            AfterProgress::More
        } else {
            AfterProgress::Done
        };
        Box::pin(std::future::ready(Ok(step)))
    }
}

// A stop raised while a family block's transaction is open abandons the run under way: the
// transaction rolls back, so the marker stays on the block before, and the Project run ends as
// cancelled. Block 15, in the second run, waits on an advisory lock the test holds; the stop is
// raised once it waits.
#[tokio::test]
async fn a_stop_during_a_family_block_rolls_that_block_back() -> Result<()> {
    within_case_deadline(async {
        const LOCK: i64 = 964_015;
        let scratch = ready_through("families_runner_stop_mid_block", 30).await?;
        seed_thirty_blocks_of_work(&scratch).await?;
        sqlx::raw_sql(&format!(
            "CREATE FUNCTION wait_at_block() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 PERFORM pg_advisory_xact_lock({LOCK});
                 RETURN NEW;
             END $$;
             CREATE TRIGGER wait_at_block BEFORE INSERT OR UPDATE ON project_family_marker
             FOR EACH ROW WHEN (NEW.current_block_number = 15)
             EXECUTE FUNCTION wait_at_block();"
        ))
        .execute(scratch.pool())
        .await?;
        let mut holder = scratch.pool().acquire().await?;
        let holder_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *holder)
            .await?;
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(LOCK)
            .execute(&mut *holder)
            .await?;
        let families = FamilySettings {
            max_blocks_per_run: 10,
            ..FamilySettings::default()
        };
        let project =
            Arc::new(ProjectPhase::new(scratch.pool().clone()).with_family_settings(families));
        let stop = CancellationToken::new();
        let watch = async {
            loop {
                // Only this test's block: a session waiting in this database on the full bigint
                // key (high half in classid, low half in objid, objsubid 1), blocked by the holder.
                let waiting: bool = sqlx::query_scalar(
                    "SELECT EXISTS (
                         SELECT 1 FROM pg_locks waiter
                         WHERE waiter.locktype = 'advisory'
                           AND waiter.database =
                               (SELECT oid FROM pg_database WHERE datname = current_database())
                           AND waiter.objsubid = 1
                           AND ((waiter.classid::bigint << 32) | waiter.objid::bigint) = $1
                           AND NOT waiter.granted
                           AND $2 = ANY (pg_blocking_pids(waiter.pid)))",
                )
                .bind(LOCK)
                .bind(holder_pid)
                .fetch_one(scratch.pool())
                .await?;
                if waiting {
                    stop.cancel();
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        };
        let (command, ()) = with_watcher(
            redo_with_phase_and_stop(&scratch, project, 30, stop.clone()),
            watch,
        )
        .await?;
        let error = command.expect_err("the stop left the redo incomplete");
        assert!(error.to_string().contains("is incomplete"), "{error}");
        assert_eq!(
            marker(&scratch).await?,
            Some(14),
            "block 15's transaction did not commit"
        );
        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(LOCK)
            .execute(&mut *holder)
            .await?;
        drop(holder);
        sqlx::query("DROP TRIGGER wait_at_block ON project_family_marker")
            .execute(scratch.pool())
            .await?;
        assert_eq!(
            marker(&scratch).await?,
            Some(14),
            "block 15 did not commit once its lock was free; the rerun below proves the repair"
        );
        assert_eq!(redo_progress(&scratch).await?, (true, Some(30)));

        let project =
            Arc::new(ProjectPhase::new(scratch.pool().clone()).with_family_settings(families));
        redo_with_phase(&scratch, project, 30).await?;
        assert_eq!(marker(&scratch).await?, Some(30), "the rerun repaired them");
        scratch.cleanup().await
    })
    .await
}

// A family run planned by one batch never outlives a later batch that plans none: here a stop
// leaves a planned run waiting, and a batch that finds no readable head, and so plans nothing,
// clears it rather than leaving it for the loop that follows.
#[tokio::test]
async fn a_batch_that_plans_no_family_run_leaves_none_waiting() -> Result<()> {
    within_case_deadline(async {
        let scratch = ready("families_runner_stale_pending").await?;
        let stop = CancellationToken::new();
        let (project, _) = ServedBatches::cancelling_at(
            ProjectPhase::new(scratch.pool().clone()),
            1,
            stop.clone(),
        );
        redo_with_phase_and_stop(&scratch, Arc::clone(&project), 3, stop)
            .await
            .expect_err("the stop left the redo incomplete");
        assert!(
            project.has_after_progress_work(CHAIN),
            "the stop left the planned run waiting"
        );
        let outcome = project
            .run_batch(PhaseContext {
                chain_id: CHAIN.to_owned(),
                phase: PhaseName::Project,
                mode: RunMode::Normal,
                redo_attempt: None,
                sources: chain_config()?.sources.clone(),
                available_heads: None,
                live_handoff: None,
                resume: PhaseResume::default(),
            })
            .await?;
        assert!(matches!(outcome, PhaseBatchOutcome::Complete(_)));
        assert!(
            !project.has_after_progress_work(CHAIN),
            "the batch that planned nothing cleared the earlier run"
        );
        scratch.cleanup().await
    })
    .await
}

// An input token that fails to read is the family run's failure, raised after the batch's
// progress is recorded, and the restart loop retries it. Here only the token read fails: the
// Project phase reads `chain_phase_state` through a view whose `last_error` column, which only the
// token read selects, raises while armed.
#[tokio::test]
async fn a_failed_input_token_read_fails_project_after_its_progress_and_is_retried() -> Result<()> {
    within_case_deadline(async {
        let scratch = ready("families_runner_token").await?;
        let project_pool =
            shadow_token_read(&scratch, "RAISE EXCEPTION 'injected token read failure';").await?;
        let (failure, progress, family) =
            token_failure_is_retried(&scratch, &project_pool, FamilySettings::default()).await?;
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
        project_pool.close().await;
        scratch.cleanup().await
    })
    .await
}

// A token read that outlasts its bound is a family failure too, never a skip: the batch's
// progress stands, the family marker does not move, the Project run fails and the restart loop
// retries, and once the read is fast again the retry runs the families. Here the read sleeps
// while armed, well past a 200 ms bound.
#[tokio::test]
async fn a_token_read_past_its_bound_fails_project_after_its_progress_and_is_retried() -> Result<()>
{
    within_case_deadline(async {
        let scratch = ready("families_runner_token_slow").await?;
        let project_pool = shadow_token_read(&scratch, "PERFORM pg_sleep(2);").await?;
        let families = FamilySettings {
            token_budget: Duration::from_millis(200),
            ..FamilySettings::default()
        };
        let (failure, progress, family) =
            token_failure_is_retried(&scratch, &project_pool, families).await?;
        assert!(
            failure.contains("the family input token did not read within 200ms"),
            "{failure}"
        );
        assert_eq!(
            progress,
            (true, Some(3)),
            "the served redo recorded its progress before the late read surfaced"
        );
        assert_eq!(family, None, "no family run started without a token");
        project_pool.close().await;
        scratch.cleanup().await
    })
    .await
}

/// A pool for the Project phase whose `chain_phase_state` is a view in schema `token_fault`: its
/// `last_error` column, which only the family input token read selects, runs `armed_body` while
/// `token_fault.armed` holds.
async fn shadow_token_read(scratch: &ScratchDatabase, armed_body: &str) -> Result<sqlx::PgPool> {
    let search_path: String = sqlx::query_scalar("SELECT current_setting('search_path')")
        .fetch_one(scratch.pool())
        .await?;
    sqlx::raw_sql(&format!(
        "CREATE SCHEMA token_fault;
         CREATE TABLE token_fault.armed (armed boolean NOT NULL);
         INSERT INTO token_fault.armed VALUES (true);
         CREATE FUNCTION token_fault.guard(value text) RETURNS text LANGUAGE plpgsql STABLE AS $$
         BEGIN
             IF (SELECT armed FROM token_fault.armed) THEN
                 {armed_body}
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
         END $$;"
    ))
    .execute(scratch.pool())
    .await?;
    let search_path = format!("token_fault,{}", search_path.replace(' ', ""));
    Ok(sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect_with(
            scratch
                .writer_connect_options()
                .options([("search_path", search_path.as_str())]),
        )
        .await?)
}

/// Runs a supervised-settings Project redo through block 3 on `project_pool` until a family
/// failure is recorded, observes the redo progress and family marker at that point, disarms the
/// token fault and lets the restart loop finish. Returns the recorded failure, that progress and
/// that marker; requires the retry to have completed the redo and run the families.
async fn token_failure_is_retried(
    scratch: &ScratchDatabase,
    project_pool: &sqlx::PgPool,
    families: FamilySettings,
) -> Result<(String, (bool, Option<i64>), Option<i64>)> {
    let project: Arc<dyn Phase> =
        Arc::new(ProjectPhase::new(project_pool.clone()).with_family_settings(families));
    let observed = async {
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(failure) = last_error(scratch)
                .await?
                .filter(|failure| failure.contains("owned key families"))
            {
                let progress = redo_progress(scratch).await?;
                let family = marker(scratch).await?;
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
    let (command, observed) = with_watcher(
        redo_with_phase_and_stop(scratch, project, 3, CancellationToken::new()),
        observed,
    )
    .await?;
    command?;
    assert_eq!(
        project_state(scratch).await?,
        ("completed".into(), Some(3), false),
        "the retry finished the redo"
    );
    assert_eq!(
        marker(scratch).await?,
        Some(3),
        "the retry ran the families"
    );
    Ok(observed)
}

/// The `RAISE` clause that makes an injected failure a data-integrity one.
const INTEGRITY: &str = " USING ERRCODE = 'check_violation'";

/// A trigger that refuses the family marker's move to `block`, raising with the `using` clause:
/// empty for a plain `RAISE EXCEPTION` (P0001, transient) or [`INTEGRITY`].
async fn refuse_marker_at(scratch: &ScratchDatabase, block: i64, using: &str) -> Result<()> {
    sqlx::raw_sql(&format!(
        "CREATE FUNCTION refuse_marker() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             RAISE EXCEPTION 'injected family failure'{using};
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
/// `cancel_at`th returns, so the stop is pending while the runner records that batch and no
/// family run starts after it.
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

    fn has_after_progress_work(&self, chain_id: &str) -> bool {
        self.inner.has_after_progress_work(chain_id)
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
    let capacity = CapacityGuard::system(CapacityConfig::default());
    redo_with_capacity(
        scratch.runner(),
        project,
        head,
        stop,
        capacity,
        Measure::ReuseFresh,
    )
    .await
}

/// A Project redo through `head` under `capacity`. `Measure::EveryCheck` turns off the runner's
/// reuse of a fresh measurement, as if every earlier one had aged past the poll interval, for tests
/// whose probe must be asked before each family run.
async fn redo_with_capacity(
    runner_store: RunnerDatabase,
    project: Arc<dyn Phase>,
    head: i64,
    stop: CancellationToken,
    capacity: CapacityGuard,
    measure: Measure,
) -> Result<()> {
    let runner = PhaseRunner::new(
        runner_store,
        PhaseSet::with_ingest_interpret_and_project(
            Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
            Arc::new(LoopbackPhase::new(PhaseName::Interpret)),
            project,
        )?,
        capacity,
        "families-runner",
        TimingConfig {
            initial_backoff: Duration::from_millis(1),
            maximum_backoff: Duration::from_millis(4),
            live_poll_interval: Duration::from_millis(1),
        },
    )?;
    let runner = match measure {
        Measure::ReuseFresh => runner,
        Measure::EveryCheck => runner.without_capacity_reuse(),
    };
    runner
        .redo(
            &chain_config()?,
            RedoPhase::Phase(PhaseName::Project),
            BlockRange::new(0, head)?,
            stop,
        )
        .await?;
    Ok(())
}

#[derive(Clone, Copy)]
enum Measure {
    ReuseFresh,
    EveryCheck,
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
