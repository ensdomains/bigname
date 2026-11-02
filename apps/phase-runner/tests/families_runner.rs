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
    INTERPRETER_CONTENT_HASH, PROJECTION_CONTENT_HASH,
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
    redo(&scratch, settings.clone()).await?;
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

// The runner hands its catalogue build settings to the family loop: a full-history redo that
// rebuilds in ranges builds the address history catalogue under the block spans, key partitions
// and `work_mem` it was given, as each build transaction sees them.
#[tokio::test]
async fn a_ranged_redo_builds_the_catalogue_under_the_runner_settings() -> Result<()> {
    let scratch = ready("families_build_settings").await?;
    sqlx::raw_sql(
        "CREATE TABLE build_seen (cursor text NOT NULL, work_mem text NOT NULL);
         CREATE FUNCTION build_seen() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             INSERT INTO build_seen VALUES (NEW.compose_cursor, current_setting('work_mem'));
             RETURN NEW;
         END $$;
         CREATE TRIGGER build_seen AFTER UPDATE ON project_repair_record
         FOR EACH ROW WHEN (OLD.compose_stream = 3 AND NEW.compose_cursor IS NOT NULL
                            AND NEW.compose_chunks > OLD.compose_chunks)
         EXECUTE FUNCTION build_seen();",
    )
    .execute(scratch.pool())
    .await?;
    let settings = FamilySettings {
        retry_family_failures: false,
        rebuild_ranges: bigname_project::families::RebuildRanges::Through(HEAD),
        compose_block_spans: 3,
        compose_key_partitions: 2,
        compose_work_mem: Some("64MB".to_owned()),
        ..FamilySettings::default()
    };
    redo(&scratch, settings).await?;
    ensure!(marker(&scratch).await? == Some(HEAD));
    let seen: Vec<(String, String)> = sqlx::query_as("SELECT cursor, work_mem FROM build_seen")
        .fetch_all(scratch.pool())
        .await?;
    let split = |cursor: &str| {
        serde_json::from_str::<serde_json::Value>(cursor)
            .map(|cursor| (cursor["spans"].clone(), cursor["partitions"].clone()))
            .unwrap_or_default()
    };
    ensure!(
        seen.len() == 1 + 3 + 5 * 2 + 1
            && seen.iter().all(|(cursor, work_mem)| {
                split(cursor) == (serde_json::json!(3), serde_json::json!(2)) && work_mem == "64MB"
            }),
        "the build ran under the runner's settings: {seen:?}"
    );
    scratch.cleanup().await
}

// The range event cap reaches the rebuild through the runner. With the chain's safe block at the
// head, blocks 1 to 25 rebuild in ranges, one event to a block, and each range is one family
// generation. Ranges right after the reset ask for 1, 2, 4, ... blocks, never more than the budget
// has left and never more events than the cap. At the default cap and above it, a budget of 10
// commits [1], [2, 3], [4 to 7] and [8 to 10]. A cap of 2 cuts them to [1], [2, 3], [4, 5],
// [6, 7], [8, 9] and [10], a cap of 1 to single blocks. The families at the head, two composed
// names among them, are the default run's. The crate's range and composition tests carry the
// equality on full histories.
#[tokio::test]
async fn the_range_event_cap_at_or_above_the_default_ranges_as_the_default() -> Result<()> {
    let expected = ranged_rebuild("families_cap_default", budget_and_cap(10, None)).await?;
    ensure!(
        expected.batches[0] == (Some(10), 4),
        "{:?}",
        expected.batches
    );
    for cap in [bigname_project::families::MAX_RANGE_EVENTS, 100_000] {
        let capped = ranged_rebuild("families_cap_large", budget_and_cap(10, Some(cap))).await?;
        capped.require(&expected, &format!("cap {cap}"))?;
        ensure!(
            capped.batches[0] == (Some(10), 4),
            "cap {cap}: {:?}",
            capped.batches
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_range_event_cap_of_two_cuts_the_rebuild_ranges_to_two_blocks() -> Result<()> {
    let expected = ranged_rebuild("families_cap_two_default", budget_and_cap(10, None)).await?;
    let capped = ranged_rebuild("families_cap_two", budget_and_cap(10, Some(2))).await?;
    capped.require(&expected, "cap 2")?;
    ensure!(capped.batches[0] == (Some(10), 6), "{:?}", capped.batches);
    Ok(())
}

#[tokio::test]
async fn a_range_event_cap_of_one_rebuilds_one_block_to_a_generation() -> Result<()> {
    let expected = ranged_rebuild("families_cap_one_default", budget_and_cap(10, None)).await?;
    let capped = ranged_rebuild("families_cap_one", budget_and_cap(10, Some(1))).await?;
    capped.require(&expected, "cap 1")?;
    ensure!(capped.batches[0] == (Some(10), 10), "{:?}", capped.batches);
    Ok(())
}

// The cap alone, under the default budget: the rebuild completes in one batch. At the default cap
// blocks 1 to 25 go in ranges [1], [2, 3], [4 to 7], [8 to 15] and [16 to 25], at a cap of 2 in
// [1] then twelve ranges of two blocks. Then 26 to 29 and the target go one to a generation. The
// composition pass between them advances no generation.
#[tokio::test]
async fn the_range_event_cap_alone_bounds_the_ranges_of_a_one_batch_rebuild() -> Result<()> {
    let expected = ranged_rebuild("families_cap_alone_default", FamilySettings::default()).await?;
    ensure!(
        expected.batches == vec![(Some(HEAD), 5 + 4 + 1)],
        "{:?}",
        expected.batches
    );
    let capped = ranged_rebuild("families_cap_alone", budget_and_cap(256, Some(2))).await?;
    capped.require(&expected, "cap 2")?;
    ensure!(
        capped.batches == vec![(Some(HEAD), 13 + 4 + 1)],
        "{:?}",
        capped.batches
    );
    Ok(())
}

// Budget 5 and cap 3. The first batch commits [1], [2, 3] and [4, 5], as the budget alone would.
// A resumed batch asks for a range of its whole budget: under the default cap [6 to 10] in one
// generation, under a cap of 3 [6 to 8] and [9, 10]. Both continue to the same families.
#[tokio::test]
async fn the_budget_and_the_range_event_cap_together_bound_each_range() -> Result<()> {
    let expected = ranged_rebuild("families_cap_both_default", budget_and_cap(5, None)).await?;
    ensure!(
        expected.batches[..2] == [(Some(5), 3), (Some(10), 4)],
        "{:?}",
        expected.batches
    );
    let capped = ranged_rebuild("families_cap_both", budget_and_cap(5, Some(3))).await?;
    capped.require(&expected, "budget 5, cap 3")?;
    ensure!(
        capped.batches[..2] == [(Some(5), 3), (Some(10), 5)],
        "{:?}",
        capped.batches
    );
    Ok(())
}

// Live follow never forms a range, so the cap is not read: with a cap of 1 and a budget of 1, each
// batch after the rebuild follows one block in one generation until the head.
#[tokio::test]
async fn live_follow_under_a_range_event_cap_applies_a_block_a_batch() -> Result<()> {
    let scratch = ready("families_cap_follow").await?;
    let rebuilt = head_marker(&scratch, 20).await?;
    let outcome = ProjectPhase::new(scratch.pool().clone())
        .run_batch(context(&rebuilt, None))
        .await?;
    ensure!(
        matches!(outcome, PhaseBatchOutcome::Complete(_)),
        "{outcome:?}"
    );
    let head = head_marker(&scratch, HEAD).await?;
    let project =
        ProjectPhase::new(scratch.pool().clone()).with_family_settings(budget_and_cap(1, Some(1)));
    let mut resume = Some(rebuilt);
    let mut sequence = marker_sequence(&scratch).await?;
    for block in 21..=HEAD {
        let outcome = project.run_batch(context(&head, resume.as_ref())).await?;
        let complete = matches!(outcome, PhaseBatchOutcome::Complete(_));
        ensure!(complete == (block == HEAD), "block {block}: {outcome:?}");
        resume = outcome.progress().current.clone();
        let next = marker_sequence(&scratch).await?;
        ensure!(
            marker(&scratch).await? == Some(block) && next == sequence + 1,
            "block {block}: one block in one generation, {sequence} then {next}"
        );
        sequence = next;
    }
    scratch.cleanup().await
}

// The one-shot redo carries the cap and completes as it does without it.
#[tokio::test]
async fn a_one_shot_redo_under_a_range_event_cap_completes() -> Result<()> {
    let scratch = ready("families_cap_redo").await?;
    let settings = FamilySettings {
        retry_family_failures: false,
        ..budget_and_cap(10, Some(1))
    };
    redo(&scratch, settings).await?;
    ensure!(
        project_state(&scratch).await? == ("completed".into(), Some(HEAD), false),
        "the redo completed at the head"
    );
    ensure!(marker(&scratch).await? == Some(HEAD));
    ensure!(repair_completed(&scratch).await?);
    scratch.cleanup().await
}

/// The catalogue build runs as one span and one key partition, 1 + 1 + 5 + 1 = 8 chunks. Each
/// chunk counts against the budget, and the default 64 spans and 16 partitions would take
/// 1 + 64 + 5 * 16 + 1 = 146, past the 30 batches `ranged_rebuild` allows under a budget of 5.
/// The build's output does not depend on its split.
fn budget_and_cap(budget: u64, cap: Option<u64>) -> FamilySettings {
    let defaults = FamilySettings::default();
    FamilySettings {
        max_blocks_per_run: budget,
        max_range_events: cap.unwrap_or(defaults.max_range_events),
        compose_block_spans: 1,
        compose_key_partitions: 1,
        ..defaults
    }
}

/// A rebuild through the runner: the marker and the generations since the rebuild's reset after
/// each batch, and the families at the head, at least one table of them non-empty.
struct RangedRebuild {
    batches: Vec<(Option<i64>, i64)>,
    families: Vec<(String, String)>,
}

impl RangedRebuild {
    fn require(&self, expected: &Self, label: &str) -> Result<()> {
        for ((table, was), (_, now)) in expected.families.iter().zip(&self.families) {
            ensure!(was == now, "{label}: {table} is {now}, by default {was}");
        }
        Ok(())
    }
}

/// Rebuild a fresh scratch to the head with `settings`, batch after batch as the runner continues
/// one, with the chain's safe block at the head so blocks 1 to 25 rebuild in ranges.
async fn ranged_rebuild(prefix: &str, settings: FamilySettings) -> Result<RangedRebuild> {
    let scratch = ready(prefix).await?;
    // Two names surfaced at block 1, which the composition pass composes, so the families
    // compared below hold rows.
    for name in [0xa_u64, 0xb] {
        sqlx::query(
            "INSERT INTO name_surfaces (logical_name_id, namespace, namehash, labelhashes,
                 normalizer_version, visibility_state, chain_id, block_hash, block_number,
                 canonicality_state)
             VALUES ('ens:' || $1, 'ens', $1, ARRAY[$1], 'fixture', 'active', $2,
                 $2 || '-block-1', 1, 'canonical')",
        )
        .bind(format!("0x{name:064x}"))
        .bind(CHAIN)
        .execute(scratch.pool())
        .await?;
    }
    let head = head_marker(&scratch, HEAD).await?;
    sqlx::query("UPDATE chain_lineage SET canonicality_state = 'safe' WHERE chain_id = $1")
        .bind(CHAIN)
        .execute(scratch.pool())
        .await?;
    sqlx::query(
        "INSERT INTO chain_heads (chain_id, latest_block_hash, latest_block_number,
             safe_block_hash, safe_block_number)
         VALUES ($1, $2, $3, $2, $3)",
    )
    .bind(CHAIN)
    .bind(&head.hash)
    .bind(HEAD)
    .execute(scratch.pool())
    .await?;
    let project = ProjectPhase::new(scratch.pool().clone()).with_family_settings(settings);
    let mut resume: Option<BlockMarker> = None;
    let mut batches = Vec::new();
    loop {
        let outcome = project.run_batch(context(&head, resume.as_ref())).await?;
        resume = outcome.progress().current.clone();
        let (_, reset) = rebuild_record(&scratch).await?;
        let reset = reset.ok_or_else(|| anyhow::anyhow!("the rebuild recorded no reset"))?;
        batches.push((
            marker(&scratch).await?,
            marker_sequence(&scratch).await? - reset,
        ));
        match outcome {
            PhaseBatchOutcome::Continue(_) => ensure!(batches.len() < 30, "{batches:?}"),
            PhaseBatchOutcome::Complete(_) => break,
            PhaseBatchOutcome::Idle(_) => anyhow::bail!("a family batch is never idle"),
        }
    }
    ensure!(marker(&scratch).await? == Some(HEAD));
    let families = families(&scratch).await?;
    ensure!(
        families
            .iter()
            .any(|(table, rows)| table != "project_family_marker" && rows != "[]"),
        "the rebuild left every family table empty, so the comparison covers nothing"
    );
    scratch.cleanup().await?;
    Ok(RangedRebuild { batches, families })
}

/// Every family table and the marker without their generations, as text.
async fn families(scratch: &ScratchDatabase) -> Result<Vec<(String, String)>> {
    let mut tables = Vec::new();
    for table in bigname_project::families::family_tables() {
        let row = if table == "project_history_catalogue_marker" {
            "to_jsonb(t) - 'publication_sequence'"
        } else {
            "to_jsonb(t)"
        };
        let rows: String = sqlx::query_scalar(&format!(
            "SELECT coalesce(jsonb_agg({row} ORDER BY ({row})::text), '[]')::text FROM {table} t"
        ))
        .fetch_one(scratch.pool())
        .await?;
        tables.push((table.to_owned(), rows));
    }
    let marker: String = sqlx::query_scalar(
        "SELECT (to_jsonb(m) - 'sequence')::text FROM project_family_marker m WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?;
    tables.push(("project_family_marker".to_owned(), marker));
    Ok(tables)
}

async fn marker_sequence(scratch: &ScratchDatabase) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT sequence FROM project_family_marker WHERE chain_id = $1")
            .bind(CHAIN)
            .fetch_one(scratch.pool())
            .await?,
    )
}

// The runner hands its composition pass worker count to the family loop: a full-history redo
// that rebuilds in ranges with as many workers as its pool has connections is refused at the
// pass start, naming the workers and the pool, since the pass's coordinator needs one more.
#[tokio::test]
async fn a_ranged_redo_composes_on_the_runner_worker_count() -> Result<()> {
    let scratch = ready("families_compose_workers").await?;
    let connections = scratch.pool().options().get_max_connections();
    let settings = FamilySettings {
        retry_family_failures: false,
        rebuild_ranges: bigname_project::families::RebuildRanges::Through(HEAD),
        compose_workers: u16::try_from(connections)?,
        ..FamilySettings::default()
    };
    let error = redo(&scratch, settings)
        .await
        .expect_err("more workers than the pool can serve are refused");
    let text = format!("{error:#}");
    ensure!(
        text.contains(&format!("runs {connections} workers"))
            && text.contains(&format!("needs {}", connections + 1))
            && text.contains(&format!("has {connections}")),
        "{text}"
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

// The swept-name cap's refusal (crates/project/src/families/derived.rs) on the supervised runner.
// Two names are published through block 10, then an ENSv2 root is admitted with no block. The
// next publication would compose both again, above a cap of one: Project refuses it, and the
// runner reports a data-integrity error that is not retried, retry policy or not, so the chain
// stops once instead of retrying. Under the default cap the publication goes through. The
// refusal is the real one, so a change of its wording fails here (TYR-356).
#[tokio::test]
async fn the_sweep_cap_refusal_is_not_retried_under_supervision() -> Result<()> {
    let scratch = ready("families_sweep_cap").await?;
    for label in ["alpha", "beta"] {
        let namehash = format!(
            "0x{:064x}",
            label.len() * 1_000 + usize::from(label.as_bytes()[0])
        );
        sqlx::query(
            "INSERT INTO name_surfaces (logical_name_id, namespace, namehash, labelhashes,
                 normalizer_version, visibility_state, chain_id, block_hash, block_number,
                 canonicality_state)
             VALUES ('ens:' || $1, 'ens', $1, ARRAY[$1], 'fixture', 'active', $2,
                 $2 || '-block-1', 1, 'canonical')",
        )
        .bind(&namehash)
        .bind(CHAIN)
        .execute(scratch.pool())
        .await?;
    }
    let published = head_marker(&scratch, 10).await?;
    let supervised = ProjectPhase::new(scratch.pool().clone());
    supervised.run_batch(context(&published, None)).await?;
    ensure!(marker(&scratch).await? == Some(10));
    let payload = serde_json::json!({
        "manifest_version": 1, "namespace": "ens", "source_family": "ens_v2_root_l1",
        "chain": CHAIN, "rollout_status": "active",
        "contracts": [{"role": "root_registry", "proxy_kind": "none", "start_block": 0,
            "address": "0x00000000000000000000000000000000000000b4"}],
    });
    let manifest_id: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions (manifest_version, namespace, source_family, chain_id,
             deployment_label, rollout_status, normalizer_version, file_path, manifest_payload)
         VALUES (1, 'ens', 'ens_v2_root_l1', $1, 'fixture', 'active', 'fixture',
             'fixture/root.toml', $2)
         RETURNING manifest_id",
    )
    .bind(CHAIN)
    .bind(&payload)
    .fetch_one(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
             manifest_version, source_manifest_id, chain_id, derivation_kind,
             canonicality_state, after_state)
         VALUES ('fixture:root', 'ens', 'SourceManifestUpdated', 'ens_v2_root_l1', 1, $1, $2,
             'manifest_sync', 'finalized',
             jsonb_build_object('rollout_status', 'active', 'manifest_payload', $3::jsonb))",
    )
    .bind(manifest_id)
    .bind(CHAIN)
    .bind(&payload)
    .execute(scratch.pool())
    .await?;
    let head = head_marker(&scratch, HEAD).await?;
    for retry_family_failures in [true, false] {
        let capped =
            ProjectPhase::new(scratch.pool().clone()).with_family_settings(FamilySettings {
                retry_family_failures,
                max_admission_sweep_names: 1,
                ..FamilySettings::default()
            });
        let error = match capped.run_batch(context(&head, Some(&published))).await {
            Ok(outcome) => anyhow::bail!("the sweep above the cap published: {outcome:?}"),
            Err(error) => error,
        };
        let message = error.to_string();
        ensure!(
            error.kind() == phase_runner::error::ErrorKind::DataIntegrity
                && !error.is_retryable()
                && message.contains("above the cap of 1")
                && message.contains("redo the full range of phase project alone"),
            "retry {retry_family_failures}: {:?} retryable {}: {message}",
            error.kind(),
            error.is_retryable()
        );
        ensure!(
            marker(&scratch).await? == Some(10),
            "the refusal published nothing"
        );
    }
    let outcome = supervised
        .run_batch(context(&head, Some(&published)))
        .await?;
    ensure!(
        matches!(outcome, PhaseBatchOutcome::Complete(_)),
        "under the default cap the sweep publishes: {outcome:?}"
    );
    ensure!(marker(&scratch).await? == Some(HEAD));
    scratch.cleanup().await
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
             input_content_hash = CASE phase_name WHEN 'project' THEN $5
                 WHEN 'interpret' THEN $4 END,
             started_at = now(), finished_at = now()
         WHERE chain_id = $1 AND phase_name IN ('ingest', 'interpret', 'project')",
    )
    .bind(CHAIN)
    .bind(HEAD)
    .bind(&hash)
    .bind(INTERPRETER_CONTENT_HASH)
    .bind(PROJECTION_CONTENT_HASH)
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
