//! The permanent Project batch publishes through the family loop. These cases retain
//! progress, bounded work, cancellation, retry and redo behavior at the actual runner boundary.
#[allow(dead_code)]
mod support;

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Result, ensure};
use phase_runner::{
    INTERPRETER_CONTENT_HASH,
    capacity::CapacityGuard,
    config::{CapacityConfig, ChainConfig, SeedBasis, SourceConfig, TimingConfig},
    heads::{BlockMarker, HeadMarkers},
    phase::{
        BlockRange, LoopbackPhase, Phase, PhaseBatchOutcome, PhaseContext, PhaseFuture, PhaseName,
        PhaseResume, PhaseSet, RunMode,
    },
    project_phase::{FamilySettings, ProjectPhase},
    runner::{PhaseRunner, RedoPhase},
    state::PhaseStore,
};
use tokio_util::sync::CancellationToken;

use support::{ScratchDatabase, seed_lineage};

const CHAIN: &str = "families-runner";
const HEAD: i64 = 30;

// A normal batch: the families rebuild to the head in one run, and the batch completes with the
// family marker as its progress.
#[tokio::test]
async fn a_project_batch_advances_the_family_marker_to_the_head() -> Result<()> {
    let scratch = ready("families_batch").await?;
    let head = head_marker(&scratch, HEAD).await?;
    let project = ProjectPhase::new(scratch.pool().clone());

    let outcome = project.run_batch(context(&head, None)).await?;
    let PhaseBatchOutcome::Complete(progress) = outcome else {
        anyhow::bail!("the family batch did not complete: {outcome:?}");
    };
    ensure!(
        progress.current.as_ref() == Some(&head) && progress.target.as_ref() == Some(&head),
        "the progress is the family marker at the head: {progress:?}"
    );
    ensure!(
        marker_row(&scratch).await? == (Some(HEAD), Some(head.hash.clone()), "live".to_owned()),
        "the family marker is live at the head"
    );
    ensure!(
        !project.has_after_progress_work(CHAIN),
        "nothing follows the recorded progress"
    );

    // The next batch follows from the recorded marker in normal mode.
    let next = head_marker(&scratch, HEAD).await?;
    let outcome = project.run_batch(context(&next, Some(&head))).await?;
    ensure!(
        matches!(outcome, PhaseBatchOutcome::Complete(_)),
        "{outcome:?}"
    );
    scratch.cleanup().await
}

// A family rebuild longer than one run's budget: each batch spends its budget and answers
// Continue with the marker it reached, and the next batch resumes the rebuild rather than
// starting it again, until the families reach the head.
#[tokio::test]
async fn a_budgeted_family_batch_continues_the_rebuild_from_its_marker() -> Result<()> {
    let scratch = ready("families_budget").await?;
    let head = head_marker(&scratch, HEAD).await?;
    let project = ProjectPhase::new(scratch.pool().clone()).with_family_settings(FamilySettings {
        max_blocks_per_run: 10,
        ..FamilySettings::default()
    });
    let mut resume: Option<BlockMarker> = None;
    let mut continued = 0;
    loop {
        let outcome = project.run_batch(context(&head, resume.as_ref())).await?;
        let progress = outcome.progress().clone();
        ensure!(progress.target.as_ref() == Some(&head), "{progress:?}");
        resume = progress.current.clone();
        match outcome {
            PhaseBatchOutcome::Continue(_) => {
                continued += 1;
                ensure!(continued < 10, "the rebuild never finished");
                ensure!(
                    marker(&scratch).await? == resume.as_ref().map(|marker| marker.number),
                    "the progress is the family marker"
                );
            }
            PhaseBatchOutcome::Complete(_) => break,
            PhaseBatchOutcome::Idle(_) => anyhow::bail!("a family batch is never idle"),
        }
    }
    ensure!(
        continued >= 2,
        "the rebuild spanned several budgets: {continued}"
    );
    ensure!(resume.as_ref() == Some(&head));
    ensure!(marker(&scratch).await? == Some(HEAD));
    scratch.cleanup().await
}

// The one-shot redo through the runner, with a budget shorter than the redo: the redo's batches
// continue until the families reach the Project row's block, and the redo completes there.
#[tokio::test]
async fn a_one_shot_redo_replays_the_families_to_the_project_block() -> Result<()> {
    let scratch = ready("families_redo").await?;
    let settings = FamilySettings {
        max_blocks_per_run: 10,
        retry_family_failures: false,
        ..FamilySettings::default()
    };
    redo(&scratch, settings).await?;
    ensure!(
        project_state(&scratch).await? == ("completed".into(), Some(HEAD), false),
        "the redo completed at the head"
    );
    ensure!(marker(&scratch).await? == Some(HEAD));
    ensure!(
        repair_completed(&scratch).await?,
        "the repair record completed"
    );
    // A second redo undoes and replays from the live families.
    redo(&scratch, settings).await?;
    ensure!(marker(&scratch).await? == Some(HEAD));
    ensure!(repair_completed(&scratch).await?);
    scratch.cleanup().await
}

// A full-history Project redo stopped part-way through its family rebuild (a graceful stop after
// one batch) and rerun with the same range under the same binary resumes the rebuild from its
// family marker: the reset the first attempt recorded is the only one.
#[tokio::test]
async fn a_stopped_full_history_project_redo_resumes_its_rebuild() -> Result<()> {
    let scratch = ready("families_redo_resume").await?;
    let range = BlockRange::new(0, HEAD)?;
    let reset = stop_after_one_batch(&scratch, range).await?;

    redo_range(&scratch, budgeted(), range).await?;
    ensure!(
        project_state(&scratch).await? == ("completed".into(), Some(HEAD), false),
        "the rerun completed the redo at the head"
    );
    ensure!(marker(&scratch).await? == Some(HEAD));
    ensure!(repair_completed(&scratch).await?);
    let (state, after) = rebuild_record(&scratch).await?;
    ensure!(
        state == "complete" && after == reset,
        "the rerun reset the rebuild again instead of resuming it: reset generation {reset:?} \
         became {after:?}"
    );
    scratch.cleanup().await
}

// A rerun over a different range starts the rebuild again, even though the first attempt left one.
#[tokio::test]
async fn a_project_redo_rerun_over_another_range_rebuilds_from_the_start() -> Result<()> {
    let scratch = ready("families_redo_other_range").await?;
    let reset = stop_after_one_batch(&scratch, BlockRange::new(0, 20)?).await?;

    redo_range(&scratch, budgeted(), BlockRange::new(0, HEAD)?).await?;
    ensure!(project_state(&scratch).await? == ("completed".into(), Some(HEAD), false));
    let (state, after) = rebuild_record(&scratch).await?;
    ensure!(
        state == "complete" && after.is_some() && after != reset,
        "a different range resets: {reset:?} then {after:?}"
    );
    scratch.cleanup().await
}

// A rerun after the Interpret input revision moved (a recompute-flags pass queued behind the
// stopped Project redo) starts the rebuild again: the rebuilt prefix read another input.
#[tokio::test]
async fn a_project_redo_rerun_after_an_interpret_revision_change_rebuilds() -> Result<()> {
    let scratch = ready("families_redo_revision").await?;
    let range = BlockRange::new(0, HEAD)?;
    let reset = stop_after_one_batch(&scratch, range).await?;
    sqlx::query(
        "UPDATE chain_phase_state SET redo_attempt_generation = redo_attempt_generation + 1
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(CHAIN)
    .execute(scratch.pool())
    .await?;

    redo_range(&scratch, budgeted(), range).await?;
    ensure!(project_state(&scratch).await? == ("completed".into(), Some(HEAD), false));
    let (state, after) = rebuild_record(&scratch).await?;
    ensure!(
        state == "complete" && after.is_some() && after != reset,
        "a changed input revision resets: {reset:?} then {after:?}"
    );
    scratch.cleanup().await
}

fn budgeted() -> FamilySettings {
    FamilySettings {
        max_blocks_per_run: 10,
        retry_family_failures: false,
        ..FamilySettings::default()
    }
}

/// Run the Project redo over `range` and stop it after its first family batch, the way a
/// graceful `docker stop` lands between two batches. Returns the reset generation of the
/// rebuild the stopped attempt left.
async fn stop_after_one_batch(scratch: &ScratchDatabase, range: BlockRange) -> Result<Option<i64>> {
    let stop = CancellationToken::new();
    let project: Arc<dyn Phase> = Arc::new(StopAfterBatches {
        inner: ProjectPhase::new(scratch.pool().clone()).with_family_settings(budgeted()),
        stop: stop.clone(),
        left: AtomicUsize::new(1),
    });
    let stopped = run_redo(scratch, project, range, stop).await?;
    ensure!(
        stopped.is_err(),
        "the stopped redo reports itself incomplete"
    );
    let (status, _, in_redo) = project_state(scratch).await?;
    ensure!(
        status == "running" && in_redo,
        "the stopped redo stays active"
    );
    let reached = marker(scratch).await?;
    ensure!(
        reached.is_some_and(|number| number < range.to),
        "the rebuild stopped part-way: {reached:?}"
    );
    let (state, reset) = rebuild_record(scratch).await?;
    ensure!(
        state == "rebuilding" && reset.is_some(),
        "{state} {reset:?}"
    );
    Ok(reset)
}

/// Project, stopping the redo once `left` batches have run.
struct StopAfterBatches {
    inner: ProjectPhase,
    stop: CancellationToken,
    left: AtomicUsize,
}

impl Phase for StopAfterBatches {
    fn name(&self) -> PhaseName {
        PhaseName::Project
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            let outcome = self.inner.run_batch(context).await;
            if self.left.fetch_sub(1, Ordering::SeqCst) == 1 {
                self.stop.cancel();
            }
            outcome
        })
    }
}

/// The repair record's state and the family generation its rebuild's reset wrote.
async fn rebuild_record(scratch: &ScratchDatabase) -> Result<(String, Option<i64>)> {
    Ok(sqlx::query_as(
        "SELECT state, reset_sequence FROM project_repair_record WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?)
}

async fn redo(scratch: &ScratchDatabase, settings: FamilySettings) -> Result<()> {
    redo_range(scratch, settings, BlockRange::new(0, HEAD)?).await
}

async fn redo_range(
    scratch: &ScratchDatabase,
    settings: FamilySettings,
    range: BlockRange,
) -> Result<()> {
    let project: Arc<dyn Phase> =
        Arc::new(ProjectPhase::new(scratch.pool().clone()).with_family_settings(settings));
    let stop = CancellationToken::new();
    run_redo(scratch, project, range, stop).await??;
    Ok(())
}

/// Run the one-shot Project redo over `range` with `project`, within two minutes; the inner
/// result is the redo command's.
async fn run_redo(
    scratch: &ScratchDatabase,
    project: Arc<dyn Phase>,
    range: BlockRange,
    stop: CancellationToken,
) -> Result<phase_runner::error::RunnerResult<()>> {
    // A failing batch is retried as a transient failure, so the redo is bounded.
    let timed_out = Arc::new(AtomicBool::new(false));
    let deadline = {
        let stop = stop.clone();
        let timed_out = Arc::clone(&timed_out);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(120)).await;
            timed_out.store(true, Ordering::SeqCst);
            stop.cancel();
        })
    };
    let result = PhaseRunner::new(
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
        range,
        stop.clone(),
    )
    .await;
    deadline.abort();
    ensure!(
        !timed_out.load(Ordering::SeqCst),
        "the redo did not finish within two minutes"
    );
    Ok(result)
}

fn context(head: &BlockMarker, resume: Option<&BlockMarker>) -> PhaseContext {
    PhaseContext {
        chain_id: CHAIN.to_owned(),
        phase: PhaseName::Project,
        mode: RunMode::Normal,
        redo_attempt: None,
        sources: Vec::new().into(),
        available_heads: Some(HeadMarkers {
            latest: head.clone(),
            safe: None,
            finalized: None,
        }),
        live_handoff: None,
        resume: PhaseResume {
            current: resume.cloned(),
            ..PhaseResume::default()
        },
    }
}

async fn head_marker(scratch: &ScratchDatabase, number: i64) -> Result<BlockMarker> {
    let hash: String = sqlx::query_scalar(
        "SELECT block_hash FROM chain_lineage WHERE chain_id = $1 AND block_number = $2",
    )
    .bind(CHAIN)
    .bind(number)
    .fetch_one(scratch.pool())
    .await?;
    Ok(BlockMarker::new(number, hash)?)
}

async fn repair_completed(scratch: &ScratchDatabase) -> Result<bool> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM project_repair_record record
             JOIN chain_phase_state project
               ON project.chain_id = record.chain_id AND project.phase_name = 'project'
             JOIN project_family_marker marker ON marker.chain_id = record.chain_id
             WHERE record.chain_id = $1 AND record.state = 'complete'
               AND record.attempt = project.redo_attempt_generation
               AND record.completed_sequence = marker.sequence
               AND marker.current_block_number = project.current_block_number
               AND marker.current_block_hash = project.current_block_hash)",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?)
}

async fn marker_row(scratch: &ScratchDatabase) -> Result<(Option<i64>, Option<String>, String)> {
    Ok(sqlx::query_as(
        "SELECT current_block_number, current_block_hash, state
         FROM project_family_marker WHERE chain_id = $1",
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

async fn project_state(scratch: &ScratchDatabase) -> Result<(String, Option<i64>, bool)> {
    Ok(sqlx::query_as(
        "SELECT phase_status, current_block_number, redo_in_progress
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?)
}

/// Lineage, the other phases complete through the head, Project at the head, and one event per
/// block so a family rebuild has thirty blocks of work.
async fn ready(prefix: &str) -> Result<ScratchDatabase> {
    let scratch = ScratchDatabase::create(prefix).await?;
    seed_lineage(scratch.pool(), CHAIN, HEAD).await?;
    sqlx::query("UPDATE chain_lineage SET canonicality_state = 'canonical' WHERE chain_id = $1")
        .bind(CHAIN)
        .execute(scratch.pool())
        .await?;
    PhaseStore::new(scratch.pool().clone())
        .initialize_chain(CHAIN)
        .await?;
    let hash = format!("{CHAIN}-block-{HEAD}");
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
    .bind(HEAD)
    .bind(&hash)
    .bind(INTERPRETER_CONTENT_HASH)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO ingest_cursors (
             chain_id, source_key, source_kind, seed_basis, start_block_number,
             next_block_number, target_block_number, last_processed_block_number,
             last_processed_block_hash
         ) VALUES ($1, 'source', 'test', 'new_signature_range', 0, $2, $3, $3, $4)",
    )
    .bind(CHAIN)
    .bind(HEAD + 1)
    .bind(HEAD)
    .bind(hash)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
             manifest_version, chain_id, block_number, block_hash, derivation_kind,
             canonicality_state, after_state)
         SELECT 'budget:' || block, 'ens', 'PreimageObserved', 'budget_probe', 1, $1, block,
                $1 || '-block-' || block, 'ens_v2_registry_resource_surface', 'canonical', '{}'
         FROM generate_series(1, $2) block",
    )
    .bind(CHAIN)
    .bind(HEAD)
    .execute(scratch.pool())
    .await?;
    Ok(scratch)
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
