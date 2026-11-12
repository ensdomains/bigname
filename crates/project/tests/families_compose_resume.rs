//! A rebuild's composition pass across run boundaries (docs/projections.md, "Owned key families").
//! At the default split a pass takes more partitions than one run's budget, so a runner resumes it
//! many times. Each case stops a pass at a run boundary inside one stream, then finishes it, and
//! requires every family table to equal block-by-block publication: the leads and event count
//! streams cut after their first partition, between two partitions and before their last. That a
//! pass stopped at every run boundary of every stream resumes to the same rows is
//! `a_pass_longer_than_a_run_continues_from_its_cursor` (families_compose.rs). The event count
//! stream also resumes on fresh sessions, after a partition that failed to commit, and after a
//! reset that clears what it left. A reorg between two runs of one pass either restarts it or lets
//! it continue.
//! The history is `families_support/compose.rs`.
mod families_support;

use anyhow::{Result, bail, ensure};
use bigname_project::{
    Marker,
    families::{self, FamilyMode, FamilyOptions, FamilyOutcome, RebuildRanges},
};
use families_support::{CHAIN, CONTENT_HASH, Fixture, compose::seeded, hash, marker};
use serde_json::Value;
use sqlx::{postgres::PgPoolOptions, raw_sql};

const TARGET: i64 = 300;
const SWITCH: i64 = 200;

/// A ranged rebuild through the switch point, the pass split in 3 block spans and 4 key
/// partitions, under `budget` blocks a run.
fn ranged(budget: u64) -> FamilyOptions {
    FamilyOptions::new(CONTENT_HASH)
        .with_rebuild_ranges(RebuildRanges::Through(SWITCH))
        .with_compose_builder_partitions(3, 4)
        .with_max_blocks_per_run(budget)
}

async fn block_by_block(fixture: &Fixture) -> Result<Vec<(String, String)>> {
    let off = FamilyOptions::new(CONTENT_HASH)
        .with_max_blocks_per_run(u64::MAX)
        .with_rebuild_ranges(RebuildRanges::Off);
    fixture
        .apply_with(TARGET, FamilyMode::Rebuild, &off)
        .await?;
    fixture.exact().await
}

async fn require_equal(
    fixture: &Fixture,
    expected: &[(String, String)],
    label: &str,
) -> Result<()> {
    let now = fixture.exact().await?;
    for ((table, was), (_, rebuilt)) in expected.iter().zip(&now) {
        ensure!(
            was == rebuilt,
            "{label}: {table} is {rebuilt}, block by block it is {was}"
        );
    }
    Ok(())
}

async fn count(fixture: &Fixture, query: &str) -> Result<i64> {
    Ok(sqlx::query_scalar(query).fetch_one(&fixture.pool).await?)
}

async fn work_rows(fixture: &Fixture) -> Result<i64> {
    count(
        fixture,
        "SELECT count(*) FROM project_compose_event_count_work",
    )
    .await
}

/// The repair record's state, stream and cursor.
async fn position(fixture: &Fixture) -> Result<(String, Option<i64>, Value)> {
    let record = fixture.repair_record().await?.unwrap_or_default();
    Ok((
        record["state"].as_str().unwrap_or_default().to_owned(),
        record["compose_stream"].as_i64(),
        record["compose_cursor"].clone(),
    ))
}

/// A step cursor's (step, part).
fn step(cursor: &Value) -> Option<(String, i64)> {
    let cursor: Value = serde_json::from_str(cursor.as_str()?).ok()?;
    Some((
        cursor["step"].as_str()?.to_owned(),
        cursor["part"].as_i64()?,
    ))
}

/// Where a case stops the pass: in `stream` with the step cursor `at`.
#[derive(Clone, Copy, Debug)]
enum Cut {
    Step(i64, &'static str, i64),
}

impl Cut {
    fn holds(self, stream: Option<i64>, cursor: &Value) -> bool {
        match self {
            Self::Step(at, name, part) => {
                stream == Some(at) && step(cursor) == Some((name.to_owned(), part))
            }
        }
    }
}

/// Whether the publication a run just left has a child listing the relation differs from: the
/// stored listing is empty until the pass's children stream, so it differs once the relation
/// lists a child, before that stream finished (`families_support/child_listing.rs`).
async fn before_children(fixture: &Fixture) -> Result<usize> {
    let record = fixture.repair_record().await?.unwrap_or_default();
    let unfinished = match record["state"].as_str() {
        Some("rebuilding") => record["composed_block"].is_null(),
        Some("composing") => matches!(record["compose_stream"].as_i64(), None | Some(0 | 7 | 5)),
        _ => false,
    };
    let differs = unfinished
        && fixture.marker().await?.0.is_some()
        && fixture.child_listing_composes_any(CHAIN).await?;
    Ok(usize::from(differs))
}

/// Rebuild, applying the ranges in the first run, then run one block a run until the record is
/// composing at `cut`. Returns the publications the runs left before the children stream finished.
async fn stop_at(fixture: &Fixture, cut: Cut) -> Result<usize> {
    let work = count(
        fixture,
        &format!(
            "SELECT count(DISTINCT block_number) FROM normalized_events
             WHERE chain_id = '{CHAIN}' AND block_number <= {SWITCH}"
        ),
    )
    .await?;
    fixture
        .apply_with(TARGET, FamilyMode::Rebuild, &ranged(u64::try_from(work)?))
        .await?;
    let mut skips = before_children(fixture).await?;
    for _ in 0..3000 {
        let (state, stream, cursor) = position(fixture).await?;
        if state == "composing" && cut.holds(stream, &cursor) {
            return Ok(skips);
        }
        ensure!(
            state == "composing" || state == "rebuilding",
            "{cut:?}: the pass ended before the cut, {state}"
        );
        fixture
            .apply_with(TARGET, FamilyMode::Normal, &ranged(1))
            .await?;
        skips += before_children(fixture).await?;
    }
    bail!("{cut:?}: the pass never reached the cut")
}

/// Finish the rebuild in one run.
async fn finish(fixture: &Fixture) -> Result<FamilyOutcome> {
    let outcome = fixture
        .apply_with(TARGET, FamilyMode::Normal, &ranged(u64::MAX))
        .await?;
    ensure!(
        fixture.marker().await?.0 == Some(TARGET),
        "the rebuild finished: {outcome:?}"
    );
    Ok(outcome)
}

async fn cuts_resume(prefix: &str, cuts: &[Cut]) -> Result<()> {
    let fixture = seeded(prefix).await?;
    let expected = block_by_block(&fixture).await?;
    let mut skips = 0;
    for &cut in cuts {
        skips += stop_at(&fixture, cut).await?;
        let finished = finish(&fixture).await?;
        ensure!(!finished.reset, "{cut:?}: the pass was resumed, not reset");
        require_equal(&fixture, &expected, &format!("{cut:?}")).await?;
    }
    fixture.expect_listing_skips(skips);
    fixture.cleanup().await
}

// R1. The leads stream, four key partitions: cut after its first partition, between its second
// and third, and before its last.
#[tokio::test]
async fn a_pass_cut_in_the_leads_stream_resumes_to_the_same_rows() -> Result<()> {
    cuts_resume(
        "families_resume_leads",
        &[
            Cut::Step(7, "leads", 0),
            Cut::Step(7, "leads", 1),
            Cut::Step(7, "leads", 2),
        ],
    )
    .await
}

// R2. The event count stream, three block spans then four key partitions: cut after its first
// span, between two spans, between its last span and its first sum, between two sums, and before
// its last.
#[tokio::test]
async fn a_pass_cut_in_the_event_count_stream_resumes_to_the_same_rows() -> Result<()> {
    cuts_resume(
        "families_resume_counts",
        &[
            Cut::Step(6, "spans", 0),
            Cut::Step(6, "spans", 1),
            Cut::Step(6, "spans", 2),
            Cut::Step(6, "sums", 0),
            Cut::Step(6, "sums", 1),
            Cut::Step(6, "sums", 2),
        ],
    )
    .await
}

/// Fail the next chunk that commits in the event count stream, once, inside its transaction.
async fn fail_next_count_chunk(fixture: &Fixture) -> Result<()> {
    raw_sql(
        "CREATE SEQUENCE fail_count_once;
         CREATE FUNCTION fail_count() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF nextval('fail_count_once') = 1 THEN
                 RAISE EXCEPTION 'injected event count partition failure';
             END IF;
             RETURN NEW;
         END $$;
         CREATE TRIGGER fail_count BEFORE UPDATE ON project_repair_record
         FOR EACH ROW WHEN (OLD.compose_stream = 6 AND NEW.compose_chunks > OLD.compose_chunks)
         EXECUTE FUNCTION fail_count();",
    )
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

// R4. The event count stream's partials are rows of a permanent table, committed with the
// partition's cursor on the repair record, so a stopped pass leaves both on the database and
// nothing in the runner. A pass stopped after a span, then a partition that fails to commit, is
// finished on a fresh pool of new sessions, as a restarted runner would, and equals block by
// block. A reset of a pass stopped inside the stream deletes the rows it left before anything
// else runs, and the rebuild after it equals block by block. That a new pass's beginning deletes
// a row an earlier pass left is `the_pass_counts_what_every_block_counts_over_many_runs`
// (families_event_counts.rs).
#[tokio::test]
async fn the_event_count_stream_resumes_in_a_fresh_process_and_a_reset_clears_its_work()
-> Result<()> {
    let fixture = seeded("families_resume_restart").await?;
    let expected = block_by_block(&fixture).await?;
    let persistence: String = sqlx::query_scalar(
        "SELECT relpersistence::text FROM pg_class
         WHERE oid = 'bigname_phase.project_compose_event_count_work'::regclass",
    )
    .fetch_one(&fixture.pool)
    .await?;
    ensure!(persistence == "p", "a permanent table, not {persistence}");

    let mut skips = stop_at(&fixture, Cut::Step(6, "spans", 1)).await?;
    let partials = work_rows(&fixture).await?;
    ensure!(partials > 0, "two spans committed their partials");
    fail_next_count_chunk(&fixture).await?;
    let failed = fixture
        .apply_with(TARGET, FamilyMode::Normal, &ranged(u64::MAX))
        .await;
    ensure!(
        failed.is_err_and(|error| error.to_string().contains("injected event count")),
        "the third span failed to commit"
    );
    ensure!(
        Cut::Step(6, "spans", 1).holds(position(&fixture).await?.1, &position(&fixture).await?.2)
            && work_rows(&fixture).await? == partials,
        "the failed span left the cursor and the partials as the second left them"
    );
    raw_sql("DROP TRIGGER fail_count ON project_repair_record")
        .execute(&fixture.pool)
        .await?;
    let fresh = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(fixture.pool.connect_options().as_ref().clone())
        .await?;
    let token = families::input_token(&fresh, CHAIN).await?;
    let outcome = families::apply(
        &fresh,
        CHAIN,
        &marker(TARGET),
        FamilyMode::Normal,
        &token,
        &ranged(u64::MAX),
    )
    .await?;
    fresh.close().await;
    ensure!(
        !outcome.reset && fixture.marker().await?.0 == Some(TARGET),
        "fresh sessions resumed the pass: {outcome:?}"
    );
    require_equal(&fixture, &expected, "fresh sessions").await?;

    skips += stop_at(&fixture, Cut::Step(6, "spans", 1)).await?;
    ensure!(
        work_rows(&fixture).await? > 0,
        "a stopped pass left partials"
    );
    let reset = fixture
        .apply_with(TARGET, FamilyMode::Rebuild, &ranged(1))
        .await?;
    skips += before_children(&fixture).await?;
    let (state, stream, _) = position(&fixture).await?;
    ensure!(
        reset.reset && state == "rebuilding" && stream.is_none(),
        "the rebuild began again: {reset:?}"
    );
    let left = work_rows(&fixture).await?;
    ensure!(left == 0, "the reset left {left} event count work rows");
    finish(&fixture).await?;
    require_equal(&fixture, &expected, "a reset inside the stream").await?;
    fixture.expect_listing_skips(skips);
    fixture.cleanup().await
}

fn sibling(block: i64) -> String {
    format!("0x{:064x}", block + 4096)
}

/// Orphan every block from `from` and put a canonical sibling branch with no events in their
/// place, to the target.
async fn reorg(fixture: &Fixture, from: i64) -> Result<()> {
    sqlx::query(
        "UPDATE chain_lineage SET canonicality_state = 'orphaned'
         WHERE chain_id = $1 AND block_number >= $2",
    )
    .bind(CHAIN)
    .bind(from)
    .execute(&fixture.pool)
    .await?;
    for block in from..=TARGET {
        let parent = if block == from {
            hash(block - 1)
        } else {
            sibling(block - 1)
        };
        sqlx::query(
            "INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number,
                 block_timestamp, canonicality_state)
             VALUES ($1, $2, $3, $4, to_timestamp(1800000000 + $4 * 12), 'canonical')",
        )
        .bind(CHAIN)
        .bind(sibling(block))
        .bind(parent)
        .bind(block)
        .execute(&fixture.pool)
        .await?;
    }
    Ok(())
}

async fn apply_target(
    fixture: &Fixture,
    mode: FamilyMode,
    options: &FamilyOptions,
) -> Result<FamilyOutcome> {
    let target = Marker {
        number: TARGET,
        hash: sibling(TARGET),
    };
    let token = families::input_token(&fixture.pool, CHAIN).await?;
    Ok(families::apply(&fixture.pool, CHAIN, &target, mode, &token, options).await?)
}

// R5. A reorg between two runs of one pass, stopped inside the leads stream or the event count
// stream. One that replaces blocks above the pass block leaves the pass block canonical: the
// pass continues where it stopped, and the blocks above it follow the new branch. One that
// replaces the pass block itself restarts the rebuild: the next run finds the pass's block
// unreadable and resets. Either way the result equals a block-by-block rebuild of the new branch.
// A new Interpret publication between two runs is
// `a_revision_that_moves_between_two_chunks_stops_the_run` (families_compose.rs).
#[tokio::test]
async fn a_reorg_between_two_runs_of_a_pass_continues_or_restarts_it() -> Result<()> {
    for (cut, from, restarts) in [
        (Cut::Step(7, "leads", 1), SWITCH + 50, false),
        (Cut::Step(6, "spans", 1), SWITCH + 50, false),
        (Cut::Step(7, "leads", 1), SWITCH - 20, true),
        (Cut::Step(6, "sums", 1), SWITCH - 20, true),
    ] {
        let case = format!("{cut:?}, branch from {from}");
        let fixture = seeded("families_resume_reorg").await?;
        let skips = stop_at(&fixture, cut).await?;
        reorg(&fixture, from).await?;
        let outcome = apply_target(&fixture, FamilyMode::Normal, &ranged(u64::MAX)).await?;
        ensure!(
            outcome.reset == restarts,
            "{case}: restarts {restarts}: {outcome:?}"
        );
        ensure!(
            fixture.marker().await?.1.as_deref() == Some(sibling(TARGET).as_str()),
            "{case}: on the new branch"
        );
        // The listing oracles, on the publication the resumed pass left.
        fixture.assert_child_listing(CHAIN).await?;
        fixture.assert_address_listing(CHAIN).await?;
        let resumed = fixture.exact().await?;
        let off = FamilyOptions::new(CONTENT_HASH)
            .with_max_blocks_per_run(u64::MAX)
            .with_rebuild_ranges(RebuildRanges::Off);
        apply_target(&fixture, FamilyMode::Rebuild, &off).await?;
        require_equal(&fixture, &resumed, &case).await?;
        fixture.expect_listing_skips(skips);
        fixture.cleanup().await?;
    }
    Ok(())
}
