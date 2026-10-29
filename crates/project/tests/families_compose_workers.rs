//! The names stream of a rebuild's composition pass on several workers (docs/projections.md,
//! "Owned key families"). Each worker claims the work list's next ids no chunk has composed,
//! skipping those another worker's open chunk holds, and composes and marks them in one
//! transaction. Whatever the worker count, the families equal one worker's and a rebuild block
//! by block. The cases stop one chunk at its commit (`families_support/workers.rs`, `hold`) to
//! fix how chunks interleave: two chunks overlap, a failed chunk beside a live one is composed
//! once more, the build waits for the last worker, two names on one resource compose its
//! inventory once, and a live worker holds the marker's generation. The history is
//! `families_support/compose.rs`.
mod families_support;

use std::time::Duration;

use anyhow::{Result, ensure};
use bigname_project::families::{FamilyMode, FamilyOptions, FamilyOutcome, RebuildRanges};
use families_support::{
    CHAIN, CONTENT_HASH, Fixture,
    compose::{install_probe, probe_chunks, probe_list, seeded, seeded_with},
    workers::{
        FIRST_LOOKED_UP, LAST_LOOKED_UP, composed, held, hold, lock_waits, log_transitions,
        require_composed_once, side, stream, transitions, uncomposed, until,
    },
};
use sqlx::raw_sql;

const TARGET: i64 = 300;
const SWITCH: i64 = 200;
/// The longest a case waits for a chunk it expects while another is held.
const BOUND: u64 = 60;

fn options(workers: u16) -> FamilyOptions {
    FamilyOptions::new(CONTENT_HASH)
        .with_max_blocks_per_run(u64::MAX)
        .with_rebuild_ranges(RebuildRanges::Through(TARGET))
        .with_compose_chunk_keys(1)
        .with_compose_workers(workers)
}

/// Rebuild block by block in one run and return every family table and the marker.
async fn block_by_block(fixture: &Fixture) -> Result<Vec<(String, String)>> {
    let off = FamilyOptions::new(CONTENT_HASH)
        .with_max_blocks_per_run(u64::MAX)
        .with_rebuild_ranges(RebuildRanges::Off);
    fixture.apply_with(TARGET, FamilyMode::Rebuild, &off).await?;
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

/// The catalogue rows and the build's work rows.
async fn catalogue_rows(fixture: &Fixture) -> Result<i64> {
    count(
        fixture,
        "SELECT (SELECT count(*) FROM project_history_source)
              + (SELECT count(*) FROM project_history_source_edge)
              + (SELECT count(*) FROM project_address_history_anchor)
              + (SELECT count(*) FROM project_compose_history_work)",
    )
    .await
}

/// Run a rebuild in ranges under `options` while `control` drives the held chunk, and return
/// both results.
async fn run_with<C, Fut>(
    fixture: &Fixture,
    mode: FamilyMode,
    options: &FamilyOptions,
    control: C,
) -> (bigname_project::Result<FamilyOutcome>, Result<()>)
where
    C: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let run = async {
        tokio::time::timeout(
            Duration::from_secs(600),
            fixture.apply_with(TARGET, mode, options),
        )
        .await
        .unwrap_or_else(|_| {
            Err(bigname_project::ProjectError::transient(
                "the pass did not return in ten minutes",
            ))
        })
    };
    tokio::join!(run, control())
}

// T1, T6, T17, T19. Three workers, one name to a chunk. The chunk of the first of two names one
// resource serves is held at its commit until the second's has committed, so the two overlap.
// The families equal one worker's and block by block, `project_lookup_name` among them: its
// `record_serving_resource_id`, which the names stream leaves null and the lookup inventories
// stream sets from the lookup core's resource, equals the column a block's lookup refresh writes
// from the resource itself. The chunk count is the same and is the names, the node and record id
// pairs, the inventories' names, the catalogue build's 1 + 64 + 5 * 16 + 1 partitions and the
// pass end, no backend waited on a lock other than the hold, and the pass ends with its marker
// row and no work rows.
#[tokio::test]
async fn a_pass_with_three_workers_equals_one_worker_and_block_by_block() -> Result<()> {
    let fixture = seeded_with("families_workers_equal", 4).await?;
    let expected = block_by_block(&fixture).await?;
    let shared: (String, String) = sqlx::query_as(
        "SELECT min(logical_name_id COLLATE \"C\"), max(logical_name_id COLLATE \"C\")
         FROM project_lookup_name WHERE record_serving_resource_id IS NOT NULL
         GROUP BY record_serving_resource_id HAVING count(*) > 1
         ORDER BY 1 LIMIT 1",
    )
    .fetch_one(&fixture.pool)
    .await?;
    let one = fixture
        .apply_with(TARGET, FamilyMode::Rebuild, &options(1))
        .await?;
    require_equal(&fixture, &expected, "one worker").await?;

    install_probe(&fixture).await?;
    let side = side(&fixture).await?;
    let first = format!("NEW.logical_name_id = '{}'", shared.0);
    let held_chunk = hold(&fixture, &side, 71, &first, false).await?;
    let waits = lock_waits(side.clone());
    let second = shared.1.clone();
    let (outcome, overlapped) = run_with(&fixture, FamilyMode::Rebuild, &options(3), || async {
        let formed = until(BOUND, || held(&side)).await?;
        let overlapped = formed
            && until(BOUND, || async {
                Ok(sqlx::query_scalar(
                    "SELECT EXISTS (SELECT 1 FROM project_compose_name_work
                     WHERE logical_name_id = $1 AND composed)",
                )
                .bind(&second)
                .fetch_one(&side)
                .await?)
            })
            .await?;
        held_chunk.release().await?;
        ensure!(
            overlapped,
            "the second name's chunk committed while the first's was held (hold formed {formed})"
        );
        Ok(())
    })
    .await;
    let waits = waits.stop().await?;
    overlapped?;
    let outcome = outcome?;
    require_equal(&fixture, &expected, "three workers").await?;
    ensure!(waits.is_empty(), "lock waits in the pass: {waits:?}");

    let chunks = probe_chunks(&fixture).await?;
    let of = |stream: i16| {
        chunks
            .iter()
            .filter(|(composed, _)| *composed == Some(stream))
            .count() as u64
    };
    let names = u64::try_from(probe_list(&fixture).await?)?;
    let served = u64::try_from(
        count(
            &fixture,
            "SELECT count(*) FROM project_lookup_name
             WHERE record_serving_resource_id IS NOT NULL",
        )
        .await?,
    )?;
    ensure!(
        of(0) == names
            && of(4) == served
            && of(3) == 1 + 64 + 5 * 16 + 1
            && outcome.compose_chunks == of(0) + of(1) + of(2) + of(4) + of(3) + 1
            && outcome.compose_chunks == one.compose_chunks,
        "{} chunks, one worker {}: {names} names, {} node pairs, {} record id pairs, {served} \
         served names, {} build partitions",
        outcome.compose_chunks,
        one.compose_chunks,
        of(1),
        of(2),
        of(3)
    );
    let composed_block: Option<i64> =
        sqlx::query_scalar("SELECT composed_block FROM project_repair_record WHERE chain_id = $1")
            .bind(CHAIN)
            .fetch_one(&fixture.pool)
            .await?;
    let journalled = fixture.journalled_blocks().await?;
    ensure!(
        composed_block.is_some_and(|block| block < TARGET)
            && journalled == vec![composed_block.unwrap_or_default(), TARGET],
        "the pass's marker row and the target: {journalled:?}"
    );
    ensure!(
        count(
            &fixture,
            "SELECT (SELECT count(*) FROM project_compose_name_work)
                  + (SELECT count(*) FROM project_compose_history_work)"
        )
        .await?
            == 0,
        "the pass end drops both work tables"
    );
    fixture.cleanup().await
}

// T13. One worker is the serial pass: it composes the work list's chunks in ascending serial
// order, one name each, and equals block by block, `project_lookup_name` and its serving
// resource, which the lookup inventories stream sets, among the families.
#[tokio::test]
async fn one_worker_is_the_serial_pass() -> Result<()> {
    let fixture = seeded("families_workers_one").await?;
    let expected = block_by_block(&fixture).await?;
    log_transitions(&fixture).await?;
    install_probe(&fixture).await?;
    fixture
        .apply_with(TARGET, FamilyMode::Rebuild, &options(1))
        .await?;
    require_equal(&fixture, &expected, "one worker").await?;
    let serials: Vec<i64> = transitions(&fixture)
        .await?
        .into_iter()
        .map(|(serial, _)| serial)
        .collect();
    let listed = probe_list(&fixture).await?;
    ensure!(
        serials == (1..=listed).collect::<Vec<_>>(),
        "chunks in serial order: {serials:?}"
    );
    fixture.cleanup().await
}

// T18 and the fence the coordinator's counting leaves open. With the first looked-up name's
// chunk held at its commit, other chunks commit and the record counts them, so the workers'
// later fences pass on a record whose chunk count moved. The held chunk then commits and the
// families equal block by block.
#[tokio::test]
async fn chunks_overlap_under_workers() -> Result<()> {
    let fixture = seeded_with("families_workers_overlap", 4).await?;
    let expected = block_by_block(&fixture).await?;
    let side = side(&fixture).await?;
    let held_chunk = hold(&fixture, &side, 72, FIRST_LOOKED_UP, false).await?;
    let (outcome, control) = run_with(&fixture, FamilyMode::Rebuild, &options(3), || async {
        let formed = until(BOUND, || held(&side)).await?;
        let at_hold = composed(&side).await?;
        let counted_at_hold = stream(&side).await?.1;
        let overlapped = formed
            && until(BOUND, || async {
                Ok(composed(&side).await? >= at_hold + 2
                    && stream(&side).await?.1 >= counted_at_hold + 2)
            })
            .await?;
        held_chunk.release().await?;
        ensure!(
            overlapped,
            "two more chunks committed and were counted while one was held (hold formed \
             {formed}, {at_hold} composed at the hold)"
        );
        Ok(())
    })
    .await;
    control?;
    outcome?;
    require_equal(&fixture, &expected, "chunks that overlap").await?;
    fixture.cleanup().await
}

// T15. A live worker holds the marker's generation: while its chunk is held, and once another
// chunk has committed beside it, a transaction that would move the generation cannot take the
// marker, and once the pass ends it can. A reset during the pass leaves no work row.
#[tokio::test]
async fn a_live_worker_holds_the_generation() -> Result<()> {
    let fixture = seeded_with("families_workers_generation", 4).await?;
    let side = side(&fixture).await?;
    let held_chunk = hold(&fixture, &side, 73, FIRST_LOOKED_UP, false).await?;
    let probe = "SELECT 1 FROM project_family_marker WHERE chain_id = $1 FOR UPDATE NOWAIT";
    let (outcome, control) = run_with(&fixture, FamilyMode::Rebuild, &options(3), || async {
        let formed = until(BOUND, || held(&side)).await?;
        let at_hold = composed(&side).await?;
        let overlapped = formed
            && until(BOUND, || async { Ok(composed(&side).await? > at_hold) }).await?;
        let mut transaction = side.begin().await?;
        let locked = sqlx::query(probe)
            .bind(CHAIN)
            .execute(&mut *transaction)
            .await;
        drop(transaction);
        held_chunk.release().await?;
        ensure!(
            overlapped,
            "a chunk committed beside the held one (hold formed {formed})"
        );
        let code = locked
            .as_ref()
            .err()
            .and_then(|error| error.as_database_error())
            .and_then(|error| error.code())
            .map(|code| code.into_owned());
        ensure!(
            code.as_deref() == Some("55P03"),
            "the marker cannot be locked for update under a live worker: {locked:?}"
        );
        Ok(())
    })
    .await;
    control?;
    outcome?;
    let mut transaction = side.begin().await?;
    sqlx::query(probe)
        .bind(CHAIN)
        .execute(&mut *transaction)
        .await?;
    transaction.rollback().await?;
    fixture.cleanup().await
}

// T3. The first looked-up name's chunk is held and fails when let go, after the other workers
// composed every other id. The run returns the error, that id alone is not composed, the
// record stays in the names stream and no catalogue row exists. The next run composes it, every
// id of the list moved to composed exactly once, and the families equal block by block.
#[tokio::test]
async fn a_failed_chunk_beside_a_live_worker_is_composed_once_more() -> Result<()> {
    let fixture = seeded_with("families_workers_failure", 4).await?;
    let expected = block_by_block(&fixture).await?;
    log_transitions(&fixture).await?;
    install_probe(&fixture).await?;
    let side = side(&fixture).await?;
    let held_chunk = hold(&fixture, &side, 74, FIRST_LOOKED_UP, true).await?;
    let (outcome, control) = run_with(&fixture, FamilyMode::Rebuild, &options(3), || async {
        let formed = until(BOUND, || held(&side)).await?;
        let rest = formed && until(BOUND, || async { Ok(uncomposed(&side).await? == 1) }).await?;
        held_chunk.release().await?;
        ensure!(
            rest,
            "the other workers composed every other id (hold formed {formed})"
        );
        Ok(())
    })
    .await;
    control?;
    let error = outcome.expect_err("the held chunk fails");
    ensure!(
        error.to_string().contains("injected held chunk failure"),
        "{error}"
    );
    let left: Vec<String> = sqlx::query_scalar(
        "SELECT logical_name_id FROM project_compose_name_work WHERE NOT composed",
    )
    .fetch_all(&fixture.pool)
    .await?;
    let first: String = sqlx::query_scalar(
        "SELECT logical_name_id FROM project_compose_name_work WHERE reach >= 1
         ORDER BY serial LIMIT 1",
    )
    .fetch_one(&fixture.pool)
    .await?;
    ensure!(left == vec![first.clone()], "only {first} is left: {left:?}");
    ensure!(
        stream(&side).await?.0 == Some(0) && catalogue_rows(&fixture).await? == 0,
        "the record stays in the names stream with no catalogue row"
    );
    let listed = probe_list(&fixture).await?;
    fixture
        .apply_with(TARGET, FamilyMode::Normal, &options(3))
        .await?;
    require_composed_once(&fixture, listed).await?;
    require_equal(&fixture, &expected, "after a failed chunk").await?;
    fixture.cleanup().await
}

// T9. The last looked-up name's chunk is held. The other workers compose every other id and
// return, and while the chunk is held the record stays in the names stream with one id not
// composed and the build has not begun. Let go, the pass goes on to the build and equals block
// by block. Held and failed instead, the run returns the error with the record in the names
// stream, and the next run's names stream composes the id before the build runs.
#[tokio::test]
async fn the_build_waits_for_the_last_worker() -> Result<()> {
    for fail in [false, true] {
        let fixture = seeded_with("families_workers_build_wait", 4).await?;
        let expected = block_by_block(&fixture).await?;
        let side = side(&fixture).await?;
        let key = if fail { 76 } else { 75 };
        let held_chunk = hold(&fixture, &side, key, LAST_LOOKED_UP, fail).await?;
        let (outcome, control) =
            run_with(&fixture, FamilyMode::Rebuild, &options(3), || async {
                let formed = until(BOUND, || held(&side)).await?;
                let rest = formed
                    && until(BOUND, || async { Ok(uncomposed(&side).await? == 1) }).await?;
                // The other workers find nothing left to claim; give the coordinator time to
                // act on that before looking.
                tokio::time::sleep(Duration::from_secs(2)).await;
                let waiting = stream(&side).await?.0 == Some(0)
                    && uncomposed(&side).await? == 1
                    && catalogue_rows(&fixture).await? == 0;
                held_chunk.release().await?;
                ensure!(
                    rest && waiting,
                    "fail {fail}: while the last chunk is held the record stays in the names \
                     stream and the build has not begun (hold formed {formed}, rest {rest})"
                );
                Ok(())
            })
            .await;
        control?;
        if fail {
            let error = outcome.expect_err("the held chunk fails");
            ensure!(
                error.to_string().contains("injected held chunk failure"),
                "{error}"
            );
            ensure!(
                stream(&side).await?.0 == Some(0) && uncomposed(&side).await? == 1,
                "the failed run leaves the record in the names stream"
            );
            fixture
                .apply_with(TARGET, FamilyMode::Normal, &options(3))
                .await?;
        } else {
            outcome?;
        }
        require_equal(&fixture, &expected, &format!("fail {fail}")).await?;
        fixture.cleanup().await?;
    }
    Ok(())
}

// T10. Two names serve one resource, and the first's chunk is held at its commit while the
// second's commits, so the two chunk transactions overlap. No names chunk writes the inventory:
// when the names stream ends no inventory row exists, and the inventories stream composes the
// resource's rows as block by block has them, with no unique violation or deadlock.
#[tokio::test]
async fn two_names_on_one_resource_compose_their_inventory_once() -> Result<()> {
    let fixture = seeded_with("families_workers_inventory", 4).await?;
    let expected = block_by_block(&fixture).await?;
    let (resource, first, second): (uuid::Uuid, String, String) = sqlx::query_as(
        "SELECT record_serving_resource_id, min(logical_name_id COLLATE \"C\"),
                max(logical_name_id COLLATE \"C\")
         FROM project_lookup_name WHERE record_serving_resource_id IS NOT NULL
         GROUP BY 1 HAVING count(*) > 1 ORDER BY 2 LIMIT 1",
    )
    .fetch_one(&fixture.pool)
    .await?;
    raw_sql(
        "CREATE TABLE names_end_inventories (rows bigint NOT NULL);
         CREATE FUNCTION names_end_inventories() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             INSERT INTO names_end_inventories SELECT count(*) FROM project_lookup_inventory;
             RETURN NULL;
         END $$;
         CREATE TRIGGER names_end_inventories AFTER UPDATE ON project_repair_record
         FOR EACH ROW WHEN (OLD.compose_stream = 0 AND NEW.compose_stream = 1)
         EXECUTE FUNCTION names_end_inventories();",
    )
    .execute(&fixture.pool)
    .await?;
    let side = side(&fixture).await?;
    let predicate = format!("NEW.logical_name_id = '{first}'");
    let held_chunk = hold(&fixture, &side, 77, &predicate, false).await?;
    let (outcome, control) = run_with(&fixture, FamilyMode::Rebuild, &options(3), || async {
        let formed = until(BOUND, || held(&side)).await?;
        let overlapped = formed
            && until(BOUND, || async {
                Ok(sqlx::query_scalar(
                    "SELECT EXISTS (SELECT 1 FROM project_compose_name_work
                     WHERE logical_name_id = $1 AND composed)",
                )
                .bind(&second)
                .fetch_one(&side)
                .await?)
            })
            .await?;
        held_chunk.release().await?;
        ensure!(
            overlapped,
            "{second}'s chunk committed while {first}'s was held (hold formed {formed})"
        );
        Ok(())
    })
    .await;
    control?;
    outcome?;
    let at_names_end: Vec<i64> = sqlx::query_scalar("SELECT rows FROM names_end_inventories")
        .fetch_all(&fixture.pool)
        .await?;
    ensure!(
        at_names_end == vec![0],
        "no inventory row when the names stream ends: {at_names_end:?}"
    );
    let serving: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM project_lookup_name WHERE record_serving_resource_id = $1",
    )
    .bind(resource)
    .fetch_one(&fixture.pool)
    .await?;
    ensure!(serving >= 2, "both names serve {resource}: {serving}");
    require_equal(&fixture, &expected, "one inventory of two names").await?;
    fixture.cleanup().await
}

// T12b. Two workers on a pool of one connection are refused at the pass start, naming both
// numbers, before the pass begins or anything is claimed.
#[tokio::test]
async fn a_pool_smaller_than_the_workers_is_refused() -> Result<()> {
    let fixture = seeded("families_workers_pool").await?;
    let run = tokio::time::timeout(
        Duration::from_secs(300),
        fixture.apply_with(TARGET, FamilyMode::Rebuild, &options(2)),
    )
    .await;
    let error = match run {
        Ok(Err(error)) => error.to_string(),
        Ok(Ok(outcome)) => anyhow::bail!("the pass ran on a pool too small: {outcome:?}"),
        Err(_) => anyhow::bail!("the pass stalled on a pool too small"),
    };
    ensure!(
        error.contains("2 workers") && error.contains("needs 3") && error.contains("has 1"),
        "{error}"
    );
    let record = fixture.repair_record().await?.unwrap_or_default();
    ensure!(
        record["state"] == "rebuilding"
            && count(&fixture, "SELECT count(*) FROM project_compose_name_work").await? == 0,
        "nothing begun or claimed: {record}"
    );
    fixture.cleanup().await
}

// T2 and T5, T7. The first run applies the ranges only, then runs of budget four with three,
// one, two, three, ... workers in turn, each a normal run as the runner continues one. Every run
// returns with no chunk open, a run in the names stream composes no block, every id moves to
// composed exactly once whatever the worker count, and the families equal block by block.
#[tokio::test]
async fn the_worker_count_may_change_between_runs() -> Result<()> {
    let fixture = seeded_with("families_workers_counts", 4).await?;
    let expected = block_by_block(&fixture).await?;
    log_transitions(&fixture).await?;
    install_probe(&fixture).await?;
    let side = side(&fixture).await?;
    let pre_pass: i64 = sqlx::query_scalar(
        "SELECT count(DISTINCT block_number) FROM normalized_events
         WHERE chain_id = $1 AND block_number <= $2",
    )
    .bind(CHAIN)
    .bind(SWITCH)
    .fetch_one(&fixture.pool)
    .await?;
    let budgeted = |workers: u16, budget: u64| {
        FamilyOptions::new(CONTENT_HASH)
            .with_rebuild_ranges(RebuildRanges::Through(SWITCH))
            .with_compose_chunk_keys(5)
            .with_compose_builder_partitions(1, 1)
            .with_compose_workers(workers)
            .with_max_blocks_per_run(budget)
    };
    let mut first = budgeted(3, u64::try_from(pre_pass)?);
    first.max_range_events = 25;
    fixture
        .apply_with(TARGET, FamilyMode::Rebuild, &first)
        .await?;
    let mut runs = 0;
    let mut names_runs = 0;
    loop {
        let workers = [3, 1, 2][runs % 3];
        let before = fixture.marker().await?.0;
        let in_names = stream(&side).await?.0 == Some(0);
        let mut options = budgeted(workers, 4);
        options.max_range_events = 25;
        let outcome = fixture
            .apply_with(TARGET, FamilyMode::Normal, &options)
            .await?;
        runs += 1;
        let open: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()
             AND state LIKE 'idle in transaction%'",
        )
        .fetch_one(&side)
        .await?;
        ensure!(open == 0, "run {runs} returned with {open} open transactions");
        if in_names && stream(&side).await?.0 == Some(0) {
            names_runs += 1;
            ensure!(
                outcome.blocks == 0 && fixture.marker().await?.0 == before,
                "run {runs} in the names stream composes no block: {outcome:?}"
            );
        }
        if fixture.marker().await?.0 == Some(TARGET) {
            break;
        }
        ensure!(runs < 2000, "the rebuild does not converge");
    }
    ensure!(names_runs > 3, "the names stream took {names_runs} runs");
    require_composed_once(&fixture, probe_list(&fixture).await?).await?;
    require_equal(&fixture, &expected, "worker counts that change").await?;
    fixture.cleanup().await
}

// The claim is the queue idiom on the partial index of the names not yet composed: a lock of
// the rows the limit returns, over that index. A chunk claims a hundredth of this list, so no
// plan that reads the whole list can be the cheaper one. The runner prepares the claim, so the
// plan is checked as a prepared statement's under a custom and a generic plan.
#[tokio::test]
async fn the_claim_locks_the_rows_its_limit_returns() -> Result<()> {
    let fixture = Fixture::new("families_workers_claim", 0).await?;
    sqlx::query(
        "INSERT INTO project_compose_name_work (chain_id, serial, logical_name_id, reach)
         SELECT $1, i, 'ens:' || i, 2 FROM generate_series(1, 100000) i",
    )
    .bind(CHAIN)
    .execute(&fixture.pool)
    .await?;
    raw_sql("ANALYZE project_compose_name_work")
        .execute(&fixture.pool)
        .await?;
    let mut connection = fixture.pool.acquire().await?;
    raw_sql(
        "PREPARE claim (text, bigint) AS
         SELECT work.serial, work.logical_name_id, work.reach
         FROM project_compose_name_work work
         WHERE work.chain_id = $1 AND NOT work.composed
         ORDER BY work.serial LIMIT $2 FOR UPDATE SKIP LOCKED",
    )
    .execute(&mut *connection)
    .await?;
    for mode in ["force_custom_plan", "force_generic_plan"] {
        raw_sql(&format!("SET plan_cache_mode = {mode}"))
            .execute(&mut *connection)
            .await?;
        let plan: Vec<String> = sqlx::query_scalar(&format!(
            "EXPLAIN EXECUTE claim('{CHAIN}', 1000)"
        ))
        .fetch_all(&mut *connection)
        .await?;
        let text = plan.join("\n");
        let (limit, rows) = (text.find("Limit"), text.find("LockRows"));
        ensure!(
            limit.is_some_and(|limit| rows.is_some_and(|rows| limit < rows))
                && text.contains("project_compose_name_work_uncomposed_idx"),
            "{mode}: {text}"
        );
    }
    drop(connection);
    fixture.cleanup().await
}
