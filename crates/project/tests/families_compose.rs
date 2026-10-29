//! The composition pass of a rebuild in ranges (docs/projections.md, "Owned key families"). The
//! ranges at or below the switch point write the family rows only, and the pass composes what
//! their blocks would each have composed, once, at the last range's block, in chunks that each
//! commit on their own. Every case rebuilds a history in ranges and requires every family table
//! and the marker, apart from its generation, as a rebuild block by block leaves them.
//!
//! The history of most cases is `families_support/compose.rs`: `rebuild_performance/seed.sql` for
//! 60 names with the ENSv2 path history of `families_support/ens_v2_path.rs`, as
//! `families_recorded_rows.rs` uses it, and on top: ENSv1 registry children a NewOwner creates
//! with no name surface, record values by node and by record id, unnamed registrar transfers of a
//! name with no surface in two blocks, and an unnamed resource pointer whose node moves.
mod families_support;

use anyhow::{Result, ensure};
use bigname_project::families::{self, FamilyMode, FamilyOptions, FamilyOutcome, RebuildRanges};
use families_support::{
    CHAIN, CONTENT_HASH, Fixture,
    compose::{
        MOVED_FROM, MOVED_TO, R1, TRANSFERRED, build_order, build_step, install_probe,
        probe_chunks, probe_list, reset_probe, seeded, seeded_with,
    },
    ens_v2_path, hash, uuid,
};
use serde_json::{Value, json};
use sqlx::raw_sql;

const TARGET: i64 = 300;
/// The switch point of the cases with single blocks above it: the work blocks at or below it go
/// in ranges, those above it one to a transaction.
const SWITCH: i64 = 200;

fn options(ranges: RebuildRanges) -> FamilyOptions {
    FamilyOptions::new(CONTENT_HASH)
        .with_max_blocks_per_run(u64::MAX)
        .with_rebuild_ranges(ranges)
}

/// Rebuild block by block in one run and return every family table and the marker.
async fn block_by_block(fixture: &Fixture) -> Result<Vec<(String, String)>> {
    let outcome = fixture
        .apply_with(TARGET, FamilyMode::Rebuild, &options(RebuildRanges::Off))
        .await?;
    ensure!(
        (outcome.ranges, outcome.compose_chunks) == (0, 0),
        "ranges off: no range and no pass, not {outcome:?}"
    );
    fixture.exact().await
}

/// Require every family table and the marker as `expected` holds them.
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

/// One run: a rebuild for the first, then normal runs as the runner continues one.
async fn run(fixture: &Fixture, first: bool, options: &FamilyOptions) -> Result<FamilyOutcome> {
    let mode = if first {
        FamilyMode::Rebuild
    } else {
        FamilyMode::Normal
    };
    Ok(fixture.apply_with(TARGET, mode, options).await?)
}

/// The repair record's pass fields: state, stream, cursor and composed block.
async fn pass_state(fixture: &Fixture) -> Result<(String, Value, Value, Value)> {
    let record = fixture.repair_record().await?.unwrap_or_default();
    Ok((
        record["state"].as_str().unwrap_or_default().to_owned(),
        record["compose_stream"].clone(),
        record["compose_cursor"].clone(),
        record["composed_block"].clone(),
    ))
}

async fn count(fixture: &Fixture, query: &str) -> Result<i64> {
    Ok(sqlx::query_scalar(query).fetch_one(&fixture.pool).await?)
}

/// The work blocks at or below `block`: the blocks with interpreted events.
async fn work_blocks(fixture: &Fixture, block: i64) -> Result<u64> {
    let blocks = count(
        fixture,
        &format!(
            "SELECT count(DISTINCT block_number) FROM normalized_events
             WHERE chain_id = '{CHAIN}' AND block_number <= {block}"
        ),
    )
    .await?;
    Ok(u64::try_from(blocks)?)
}

/// The id of a name an address fold or registry node can carry with no surface.
fn unsurfaced(namehash: &str) -> String {
    format!("ens:{namehash}")
}

/// Whether the lookup has a row for `id` with no composed core, and the catalogue an empty name
/// source.
async fn empty_rows(fixture: &Fixture, id: &str) -> Result<(bool, bool)> {
    let lookup = count(
        fixture,
        &format!(
            "SELECT count(*) FROM project_lookup_name
             WHERE logical_name_id = '{id}' AND core IS NULL"
        ),
    )
    .await?;
    let source = count(
        fixture,
        &format!(
            "SELECT count(*) FROM project_history_source
             WHERE source_kind = 0 AND source_key = '{id}' AND event_mask = 0"
        ),
    )
    .await?;
    Ok((lookup == 1, source == 1))
}

/// The lookup name rows and catalogue name sources of `id`, of any content.
async fn composed_for(fixture: &Fixture, id: &str) -> Result<i64> {
    count(
        fixture,
        &format!(
            "SELECT (SELECT count(*) FROM project_lookup_name WHERE logical_name_id = '{id}')
                  + (SELECT count(*) FROM project_history_source
                     WHERE source_kind = 0 AND source_key = '{id}')"
        ),
    )
    .await
}

// Rebuilt in ranges through the target with one name to a chunk, under event caps of 4,096 (the
// default), 1, 2 and 25, the history equals its block-by-block rebuild in every family table, the
// 12 composed tables among them. The cap bounds the ranges only. The catalogue build does not read
// it, so its chunks are the same under every cap, and the catalogue sources, edges and anchors of
// each cap are byte for byte the default cap's. The comparison covers what it is meant to: lookup
// rows with no composed core and empty name sources of ids with no surface, one inventory served
// by names in different chunks, resource anchors, catalogue sources by node and by record id, and
// both record indexes. The caps of 1 and 25 compose the names stream on three workers, so the
// cap and the worker count cross: the families, the catalogue and the chunk count are the same
// under each pairing.
// Each id with no surface keeps the rows block by block leaves it: a registry child a NewOwner
// created has a lookup row and a name source; the name of a registrar transfer in one block has a
// lookup row only, and the one transferred in two blocks has both, since only a later block's
// rewrite of its address fold names it to the catalogue; and the node an unnamed pointer moved away
// from keeps both, though no row carries that node any more.
#[tokio::test]
async fn a_rebuild_in_ranges_composes_once_what_block_by_block_composes() -> Result<()> {
    let fixture = seeded_with("families_compose_equal", 4).await?;
    let expected = block_by_block(&fixture).await?;
    install_probe(&fixture).await?;
    let mut by_default = None;
    for (events, workers) in [(families::MAX_RANGE_EVENTS, 1), (1, 3), (2, 1), (25, 3)] {
        reset_probe(&fixture).await?;
        let ranged = options(RebuildRanges::Through(TARGET))
            .with_compose_chunk_keys(1)
            .with_range_caps(64, events)
            .with_compose_workers(workers);
        let outcome = fixture
            .apply_with(TARGET, FamilyMode::Rebuild, &ranged)
            .await?;
        require_equal(&fixture, &expected, &format!("in ranges, cap {events}")).await?;
        let mut catalogue = Vec::new();
        for table in [
            "project_history_source",
            "project_history_source_edge",
            "project_address_history_anchor",
        ] {
            let rows = fixture.rows(table).await?;
            ensure!(!rows.is_empty(), "cap {events}: {table} is empty");
            catalogue.push((table, rows));
        }
        match &by_default {
            None => by_default = Some(catalogue),
            Some(by_default) => ensure!(
                by_default == &catalogue,
                "cap {events}: the catalogue differs from the default cap's"
            ),
        }
        let (state, stream, cursor, composed) = pass_state(&fixture).await?;
        ensure!(
            (state.as_str(), &stream, &cursor) == ("complete", &Value::Null, &Value::Null),
            "cap {events}: the record completes with no pass in progress: {state} {stream} \
             {cursor}"
        );
        ensure!(
            composed.as_i64().is_some_and(|block| block < TARGET),
            "cap {events}: the pass composed below the target: {composed}"
        );
        // The pass's chunks: one a name, one a record pair, one a name that serves an inventory,
        // the catalogue build's 1 + 64 + 5 * 16 + 1 partitions at the default split, and its end.
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
            names > 100
                && of(0) == names
                && served > 0
                && of(4) == served
                && of(3) == 1 + 64 + 5 * 16 + 1
                && outcome.compose_chunks == names + of(1) + of(2) + of(4) + of(3) + 1
                && outcome.compose_chunks == chunks.len() as u64 + 1,
            "cap {events}: {} chunks: {names} names, {} node pairs, {} record id pairs, {} \
             serving names, {} build partitions",
            outcome.compose_chunks,
            of(1),
            of(2),
            of(4),
            of(3)
        );
        let journalled = fixture.journalled_blocks().await?;
        ensure!(
            journalled == vec![composed.as_i64().unwrap_or_default(), TARGET],
            "cap {events}: the pass's marker row and the target: {journalled:?}"
        );
    }

    // The comparison covers each shape.
    for (what, query) in [
        (
            "lookup rows with no core of ids with no surface",
            "SELECT count(*) FROM project_lookup_name lookup WHERE lookup.core IS NULL
             AND NOT EXISTS (SELECT 1 FROM name_surfaces surface
                             WHERE surface.logical_name_id = lookup.logical_name_id)",
        ),
        (
            "empty name sources of ids with no surface",
            "SELECT count(*) FROM project_history_source source
             WHERE source.source_kind = 0 AND source.event_mask = 0
               AND NOT EXISTS (SELECT 1 FROM name_surfaces surface
                               WHERE surface.logical_name_id = source.source_key)",
        ),
        (
            "an inventory two names serve from",
            "SELECT count(*) - 1 FROM project_lookup_name
             WHERE core::text LIKE '%00000000-0000-0000-0000-000000000387%'",
        ),
        (
            "resource anchors",
            "SELECT count(*) FROM project_address_history_anchor WHERE anchor_kind = 1",
        ),
        (
            "catalogue sources by node and by record id",
            "SELECT count(DISTINCT source_kind) - 1 FROM project_history_source
             WHERE source_kind IN (1, 2)",
        ),
        (
            "record node index rows",
            "SELECT count(*) FROM project_address_record_node_index",
        ),
        (
            "record id index rows",
            "SELECT count(*) FROM project_address_record_id_index",
        ),
    ] {
        ensure!(
            count(&fixture, query).await? > 0,
            "the history has no {what}, so the comparison does not cover it"
        );
    }
    let child = count(
        &fixture,
        "SELECT count(*) FROM project_registry_node_state state
         WHERE NOT EXISTS (SELECT 1 FROM name_surfaces surface
                           WHERE surface.logical_name_id = 'ens:' || state.node)",
    )
    .await?;
    ensure!(
        child > 0,
        "the history has a registry child with no surface"
    );
    let child: String = sqlx::query_scalar(
        "SELECT 'ens:' || state.node FROM project_registry_node_state state
         WHERE NOT EXISTS (SELECT 1 FROM name_surfaces surface
                           WHERE surface.logical_name_id = 'ens:' || state.node)
         ORDER BY 1 LIMIT 1",
    )
    .fetch_one(&fixture.pool)
    .await?;
    let once: String = sqlx::query_scalar(
        "SELECT fold.logical_name_id FROM project_address_name_fold fold
         WHERE NOT EXISTS (SELECT 1 FROM name_surfaces surface
                           WHERE surface.logical_name_id = fold.logical_name_id)
           AND (SELECT count(DISTINCT transfer.block_number) FROM normalized_events transfer
                WHERE transfer.event_kind = 'TokenControlTransferred'
                  AND transfer.logical_name_id IS NULL
                  AND 'ens:' || lower(transfer.after_state ->> 'namehash')
                      = fold.logical_name_id) = 1
         ORDER BY 1 LIMIT 1",
    )
    .fetch_one(&fixture.pool)
    .await?;
    for (id, rows, what) in [
        (child, (true, true), "a registry child a NewOwner created"),
        (
            once,
            (true, false),
            "a name transferred unnamed in one block",
        ),
        (
            unsurfaced(TRANSFERRED),
            (true, true),
            "a name transferred unnamed in two blocks",
        ),
        (
            unsurfaced(MOVED_FROM),
            (true, true),
            "the node a pointer moved away from",
        ),
        (
            unsurfaced(MOVED_TO),
            (true, true),
            "the node a pointer moved to",
        ),
    ] {
        ensure!(
            empty_rows(&fixture, &id).await? == rows,
            "{what} ({id}): lookup row and name source {:?}, not {rows:?}",
            empty_rows(&fixture, &id).await?
        );
    }
    fixture.cleanup().await
}

/// What one run of a budgeted rebuild did, and the repair record it left.
#[derive(Debug)]
struct Seen {
    chunks: u64,
    refreshes: u64,
    blocks: u64,
    ranges: u64,
    undo_rows: u64,
    marker: Option<i64>,
    state: String,
    stream: Value,
    cursor: Value,
    composed: Value,
    /// The work list's ids marked composed.
    marked: i64,
}

// A pass longer than one run. The first run's budget is the work blocks at or below the switch
// point exactly: it applies them in ranges and stops before the pass, the record still rebuilding
// with no pass recorded. Every later run has a budget of one block: it commits at most one chunk
// of five keys, or the pass's end, or one single block. The names stream continues with the ids
// no chunk composed, five a run, and the other streams from the cursor the previous run left, a
// stream starts from its beginning once the one before it is done, the streams run in the order
// names, nodes, record ids, inventories, catalogue build, and a run
// that finds the catalogue build done only ends the pass, which counts as a step that spent the
// budget. The build, three block spans and two key partitions here, commits its fifteen
// partitions in order, one a run. The composed tables' statistics are refreshed after the pass's 1st, 2nd, 4th, ... chunk,
// counted across runs, so once per power of two however the chunks fall into runs. The single
// blocks above the switch point then compose and journal as block by block does, and the finished
// families equal it.
#[tokio::test]
async fn a_pass_longer_than_a_run_continues_from_its_cursor() -> Result<()> {
    let fixture = seeded("families_compose_resume").await?;
    let expected = block_by_block(&fixture).await?;
    let pre_pass = work_blocks(&fixture, SWITCH).await?;
    let mut budgeted = options(RebuildRanges::Through(SWITCH))
        .with_compose_chunk_keys(5)
        .with_compose_builder_partitions(3, 2)
        .with_max_blocks_per_run(pre_pass);
    budgeted.max_range_events = 25;
    let first = run(&fixture, true, &budgeted).await?;
    let pass_block = fixture.marker().await?.0;
    ensure!(
        first.budget_exhausted && first.compose_chunks == 0 && first.blocks == pre_pass,
        "the first run applies the ranges only: {first:?}"
    );
    ensure!(
        pass_state(&fixture).await?
            == (
                "rebuilding".to_owned(),
                Value::Null,
                Value::Null,
                Value::Null
            ),
        "no pass begun: {:?}",
        pass_state(&fixture).await?
    );
    ensure!(
        fixture.composed_rows().await? == 0,
        "the ranges compose nothing"
    );
    ensure!(
        fixture.journalled_blocks().await?.is_empty(),
        "the ranges journal nothing"
    );

    let one = budgeted.clone().with_max_blocks_per_run(1);
    let mut seen = Vec::new();
    loop {
        let outcome = run(&fixture, false, &one).await?;
        let (state, stream, cursor, composed) = pass_state(&fixture).await?;
        seen.push(Seen {
            chunks: outcome.compose_chunks,
            refreshes: outcome.statistics_refreshes,
            blocks: outcome.blocks,
            ranges: outcome.ranges,
            undo_rows: outcome.undo_rows,
            marker: fixture.marker().await?.0,
            state,
            stream,
            cursor,
            composed,
            marked: count(
                &fixture,
                "SELECT count(*) FROM project_compose_name_work WHERE composed",
            )
            .await?,
        });
        if seen.last().is_some_and(|last| last.marker == Some(TARGET)) {
            break;
        }
        ensure!(seen.len() < 2000, "the rebuild does not converge");
    }
    require_equal(&fixture, &expected, "a pass over runs of one block").await?;
    let ended = seen
        .iter()
        .position(|run| run.composed.is_i64())
        .expect("the pass ends");
    // One refresh per power of two of the pass's chunks, not one per run.
    let chunks: u64 = seen[..ended].iter().map(|run| run.chunks).sum();
    let refreshes: u64 = seen[..=ended].iter().map(|run| run.refreshes).sum();
    let record = fixture.repair_record().await?.unwrap_or_default();
    ensure!(
        chunks > 8
            && record["compose_chunks"] == json!(chunks)
            && refreshes == u64::from(chunks.ilog2()) + 1,
        "{refreshes} refreshes over {chunks} chunks, the record counts {}",
        record["compose_chunks"]
    );

    // Each run commits at most one chunk, the pass's end or one block, and no range.
    for (index, run) in seen.iter().enumerate() {
        ensure!(
            run.chunks + run.blocks <= 1 && run.ranges == 0,
            "run {index} spent more than its budget: {run:?}"
        );
    }
    // The pass runs its streams in order, each from its beginning, until it ends.
    let passing: Vec<&Seen> = seen.iter().filter(|run| run.state == "composing").collect();
    let order = [0, 1, 2, 4, 3];
    let streams: Vec<usize> = passing
        .iter()
        .filter_map(|run| run.stream.as_i64())
        .filter_map(|stream| order.iter().position(|at| *at == stream))
        .collect();
    ensure!(
        streams.len() == passing.len() && streams.windows(2).all(|pair| pair[0] <= pair[1]),
        "the streams run in order: {streams:?}"
    );
    for stream in order {
        let cursors: Vec<&Value> = passing
            .iter()
            .filter(|run| run.stream.as_i64() == Some(stream))
            .map(|run| &run.cursor)
            .collect();
        // The catalogue build took several runs. The names stream has no cursor. The two record
        // streams hold one key each here, so each was entered and done in one run.
        let runs = if stream == 3 { 2 } else { 0 };
        ensure!(
            !cursors.is_empty()
                && cursors.iter().filter(|cursor| !cursor.is_null()).count() >= runs,
            "stream {stream} ran: {cursors:?}"
        );
        let texts: Vec<String> = cursors
            .iter()
            .filter_map(|cursor| cursor.as_str().map(str::to_owned))
            .collect();
        if stream == 0 {
            // The names stream leaves no cursor, and each of its runs composes five more ids.
            let marked: Vec<i64> = passing
                .iter()
                .filter(|run| run.stream.as_i64() == Some(0))
                .map(|run| run.marked)
                .collect();
            ensure!(
                texts.is_empty()
                    && marked.len() > 2
                    && marked.windows(2).all(|pair| pair[1] == pair[0] + 5),
                "the names stream composes five ids a run: {texts:?} {marked:?}"
            );
        } else if stream == 3 {
            // The build's cursor names each partition it committed, in the build's order.
            let steps: Vec<(String, i64)> = texts
                .iter()
                .filter_map(|text| build_step(text))
                .filter(|(_, layout)| *layout == (3, 2))
                .map(|(step, _)| step)
                .collect();
            ensure!(
                steps == build_order(3, 2),
                "the build commits each partition once, in order: {texts:?}"
            );
        } else {
            let mut sorted = texts.clone();
            sorted.sort();
            sorted.dedup();
            ensure!(
                sorted == texts,
                "stream {stream}'s cursor only moves on: {texts:?}"
            );
        }
    }
    // The run that found the build done ended the pass and did nothing else.
    let end = &seen[ended];
    ensure!(
        (end.chunks, end.blocks, end.undo_rows, end.marker) == (1, 0, 1, pass_block)
            && end.composed.as_i64() == pass_block
            && end.state == "rebuilding",
        "the pass's end: {end:?}"
    );
    let before = &seen[ended - 1];
    let done = before
        .cursor
        .as_str()
        .and_then(|text| serde_json::from_str::<Value>(text).ok())
        .unwrap_or_default();
    ensure!(
        before.stream == json!(3) && done["step"] == json!("done"),
        "the build was done before the end: {before:?}"
    );
    // The single blocks above the switch point follow, one a run.
    let after = &seen[ended + 1];
    ensure!(
        after.blocks == 1 && after.marker > pass_block,
        "a single block after the pass: {after:?}"
    );
    let journalled = fixture.journalled_blocks().await?;
    ensure!(
        journalled.first().copied() == pass_block
            && journalled.iter().skip(1).all(|block| *block > SWITCH),
        "the pass's marker row, then the single blocks: {journalled:?}"
    );
    let single_journal = count(
        &fixture,
        &format!(
            "SELECT count(*) FROM project_family_undo WHERE chain_id = '{CHAIN}'
             AND block_number > {SWITCH} AND family <> 'marker'"
        ),
    )
    .await?;
    ensure!(
        single_journal > 0,
        "a single block journals the rows it writes"
    );
    fixture.cleanup().await
}

// The pass's beginning lists every id the pass composes once, numbered from 1, and a later run
// of the pass pages through that list rather than enumerating the families again. A name a base
// row gains after the pass began, which no block can give it while the pass runs, is therefore
// not composed. The pass's end drops the list, and the families equal the block-by-block ones.
#[tokio::test]
async fn a_later_run_of_the_pass_composes_the_list_its_beginning_wrote() -> Result<()> {
    let fixture = seeded("families_compose_list").await?;
    let expected = block_by_block(&fixture).await?;
    let pre_pass = work_blocks(&fixture, SWITCH).await?;
    let mut budgeted = options(RebuildRanges::Through(SWITCH))
        .with_compose_chunk_keys(5)
        .with_max_blocks_per_run(pre_pass + 1);
    budgeted.max_range_events = 25;
    run(&fixture, true, &budgeted).await?;
    let (state, stream, cursor, _) = pass_state(&fixture).await?;
    let marked = count(
        &fixture,
        "SELECT count(*) FROM project_compose_name_work WHERE composed",
    )
    .await?;
    ensure!(
        (state.as_str(), &stream) == ("composing", &json!(0)) && cursor.is_null() && marked == 5,
        "one chunk of names committed: {state} {stream} {cursor} {marked}"
    );
    // Read now, checked after the probe below.
    let listed: sqlx::Result<(i64, Option<i64>, Option<i64>, i64)> = sqlx::query_as(
        "SELECT count(*), min(serial), max(serial), count(DISTINCT logical_name_id)
         FROM project_compose_name_work WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .fetch_one(&fixture.pool)
    .await;
    let late = format!("0x{}", "f".repeat(64));
    sqlx::query(
        "INSERT INTO project_registry_node_state (chain_id, namespace, node, block_number,
             event_identity)
         VALUES ($1, 'ens', $2, 1, 'late-probe')",
    )
    .bind(CHAIN)
    .bind(&late)
    .execute(&fixture.pool)
    .await?;
    run(
        &fixture,
        false,
        &budgeted.clone().with_max_blocks_per_run(u64::MAX),
    )
    .await?;
    ensure!(
        composed_for(&fixture, &unsurfaced(&late)).await? == 0,
        "a name gained after the pass began is not composed"
    );
    let (rows, first, last, distinct) = listed?;
    ensure!(
        rows > 5 && first == Some(1) && last == Some(rows) && distinct == rows,
        "the list numbers every id once from 1: {rows} {first:?} {last:?} {distinct}"
    );
    ensure!(
        count(&fixture, "SELECT count(*) FROM project_compose_name_work").await? == 0
            && count(
                &fixture,
                "SELECT count(*) FROM project_compose_history_work"
            )
            .await?
                == 0,
        "the pass's end drops the list and the build's work table"
    );
    sqlx::query("DELETE FROM project_registry_node_state WHERE event_identity = 'late-probe'")
        .execute(&fixture.pool)
        .await?;
    require_equal(&fixture, &expected, "a pass over its list").await?;
    fixture.cleanup().await
}

// An event the fold never applies, a candidate one that waits for a migration's activation,
// names nothing in the pass either: its ResolverChanged node gets no lookup row or name source,
// as block by block, where no block applies the event.
#[tokio::test]
async fn an_event_the_fold_never_applies_names_nothing_in_the_pass() -> Result<()> {
    let fixture = seeded("families_compose_candidate").await?;
    let candidate = format!("0x{}", "e".repeat(64));
    let event = fixture
        .write(
            170,
            9,
            "ResolverChanged",
            "ens_v2_registry_l1",
            None,
            Some(&uuid(906)),
            json!({"node": candidate, "resolver": R1}),
            R1,
        )
        .await?;
    sqlx::query(
        "UPDATE normalized_events
         SET consumer_visibility = 'candidate', migration_correlation_ids = ARRAY['probe']
         WHERE normalized_event_id = $1",
    )
    .bind(event)
    .execute(&fixture.pool)
    .await?;
    let expected = block_by_block(&fixture).await?;
    ensure!(
        composed_for(&fixture, &unsurfaced(&candidate)).await? == 0,
        "block by block names nothing for the candidate event"
    );
    fixture
        .apply_with(
            TARGET,
            FamilyMode::Rebuild,
            &options(RebuildRanges::Through(TARGET)).with_compose_chunk_keys(5),
        )
        .await?;
    ensure!(
        composed_for(&fixture, &unsurfaced(&candidate)).await? == 0,
        "the pass names nothing for the candidate event"
    );
    require_equal(&fixture, &expected, "with a candidate event").await?;
    fixture.cleanup().await
}

/// A trigger that fails the `nth` work list row a names chunk marks composed, once, inside the
/// chunk's transaction: the sequence survives the rollback the failure causes.
async fn fail_chunk(fixture: &Fixture, nth: i64) -> Result<()> {
    raw_sql(&format!(
        "CREATE SEQUENCE fail_chunk_once;
         CREATE FUNCTION fail_chunk() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF nextval('fail_chunk_once') = {nth} THEN
                 RAISE EXCEPTION 'injected chunk failure';
             END IF;
             RETURN NEW;
         END $$;
         CREATE TRIGGER fail_chunk BEFORE UPDATE ON project_compose_name_work
         FOR EACH ROW WHEN (NEW.composed AND NOT OLD.composed)
         EXECUTE FUNCTION fail_chunk();"
    ))
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

async fn allow_chunks(fixture: &Fixture) -> Result<()> {
    raw_sql(
        "DROP TRIGGER fail_chunk ON project_compose_name_work;
         DROP FUNCTION fail_chunk(); DROP SEQUENCE fail_chunk_once;",
    )
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

/// The work list's ids marked composed.
async fn marked(fixture: &Fixture) -> Result<i64> {
    count(
        fixture,
        "SELECT count(*) FROM project_compose_name_work WHERE composed",
    )
    .await
}

// A run that stops after the pass began and before its first chunk committed leaves the record
// composing at the start of the names stream and no composed row, and the next run composes from
// there. A chunk that fails to commit after two committed leaves every row where the second
// chunk left them, and the next run composes the ids that chunk had claimed. Both rebuilds equal
// the block-by-block one.
#[tokio::test]
async fn a_chunk_that_fails_to_commit_is_composed_again() -> Result<()> {
    let fixture = seeded("families_compose_failure").await?;
    let expected = block_by_block(&fixture).await?;
    let ranged = options(RebuildRanges::Through(TARGET)).with_compose_chunk_keys(5);

    // The first chunk fails: the pass has begun and composed nothing.
    fail_chunk(&fixture, 1).await?;
    let error = run(&fixture, true, &ranged)
        .await
        .expect_err("the first chunk fails");
    ensure!(
        error.to_string().contains("injected chunk failure"),
        "{error}"
    );
    ensure!(
        pass_state(&fixture).await? == ("composing".to_owned(), json!(0), Value::Null, Value::Null),
        "the pass began at the start of the names stream: {:?}",
        pass_state(&fixture).await?
    );
    ensure!(
        fixture.composed_rows().await? == 0 && marked(&fixture).await? == 0,
        "the failed chunk composed nothing"
    );
    allow_chunks(&fixture).await?;
    let resumed = run(&fixture, false, &ranged).await?;
    ensure!(!resumed.reset && resumed.ranges == 0, "{resumed:?}");
    require_equal(&fixture, &expected, "after a failed first chunk").await?;

    // Two chunks commit, the third fails.
    let pre_pass = work_blocks(&fixture, TARGET - 1).await?;
    let two = ranged.clone().with_max_blocks_per_run(pre_pass + 2);
    run(&fixture, true, &two).await?;
    let (state, stream, cursor, _) = pass_state(&fixture).await?;
    ensure!(
        (state.as_str(), &stream) == ("composing", &json!(0))
            && cursor.is_null()
            && marked(&fixture).await? == 10,
        "two chunks of names committed: {state} {stream} {cursor}"
    );
    let committed = fixture.exact().await?;
    fail_chunk(&fixture, 1).await?;
    let error = run(&fixture, false, &ranged)
        .await
        .expect_err("the third chunk fails");
    ensure!(
        error.to_string().contains("injected chunk failure"),
        "{error}"
    );
    ensure!(
        marked(&fixture).await? == 10,
        "the failed chunk marked nothing"
    );
    ensure!(
        fixture.exact().await? == committed,
        "the failed chunk left no row"
    );
    allow_chunks(&fixture).await?;
    run(&fixture, false, &ranged).await?;
    require_equal(&fixture, &expected, "after a failed third chunk").await?;
    fixture.cleanup().await
}

// With every work block above the switch point there is no range, and the pass is empty: the
// record records -1 as its composed block, and every block composes and journals as block by
// block does.
#[tokio::test]
async fn with_no_range_the_pass_is_empty() -> Result<()> {
    let fixture = Fixture::new("families_compose_empty", 20).await?;
    for block in 10..=14 {
        fixture.resolver_changed(block, 1, 1, R1).await?;
    }
    let expected = {
        fixture
            .apply_with(15, FamilyMode::Rebuild, &options(RebuildRanges::Off))
            .await?;
        (fixture.exact().await?, fixture.journalled_blocks().await?)
    };
    let outcome = fixture
        .apply_with(15, FamilyMode::Rebuild, &options(RebuildRanges::Through(5)))
        .await?;
    ensure!(
        (outcome.ranges, outcome.compose_chunks) == (0, 0),
        "{outcome:?}"
    );
    ensure!(
        pass_state(&fixture).await?.3 == json!(-1),
        "the empty pass composed at -1: {:?}",
        pass_state(&fixture).await?
    );
    ensure!(
        count(
            &fixture,
            "SELECT count(*) FROM project_compose_history_work"
        )
        .await?
            == 0,
        "the empty pass builds nothing"
    );
    require_equal(&fixture, &expected.0, "with no range").await?;
    ensure!(
        fixture.journalled_blocks().await? == expected.1,
        "every block journals as block by block"
    );
    fixture.cleanup().await
}

// The switch point rises between two runs: the second run applies blocks above the first run's
// switch point in a range, after the pass. That range composes and journals as one generation, so
// it undoes as one step, and the rebuild equals the block-by-block one. Under an event cap of 2
// the second run applies 17 to 24 as four such ranges of two blocks, each its own generation.
#[tokio::test]
async fn a_range_after_the_pass_composes_and_journals() -> Result<()> {
    range_after_the_pass(families::MAX_RANGE_EVENTS, &[24]).await?;
    range_after_the_pass(2, &[18, 20, 22, 24]).await
}

/// The second run's ranges under an event cap of `events` end at `ends`.
async fn range_after_the_pass(events: u64, ends: &[i64]) -> Result<()> {
    let fixture = Fixture::new("families_compose_risen", 30).await?;
    for block in 10..=24 {
        fixture
            .resolver_changed(block, 1, block.unsigned_abs() % 3, R1)
            .await?;
    }
    fixture
        .apply_with(25, FamilyMode::Rebuild, &options(RebuildRanges::Off))
        .await?;
    let expected = fixture.exact().await?;
    // Ranges [10], [11, 12] and [13, 14], the pass (a chunk of names, the catalogue build's
    // eight partitions in one span and one key partition, and its end), then single blocks 15
    // and 16 before the budget runs out.
    let low = options(RebuildRanges::Through(14))
        .with_compose_builder_partitions(1, 1)
        .with_max_blocks_per_run(5 + (1 + 8 + 1) + 2);
    run_to(&fixture, 25, true, &low).await?;
    let (state, _, _, composed) = pass_state(&fixture).await?;
    ensure!(
        state == "rebuilding" && composed == json!(14),
        "the pass composed at 14: {state} {composed}"
    );
    let stopped = fixture.marker().await?.0;
    ensure!(
        stopped == Some(16),
        "two single blocks after the pass: {stopped:?}"
    );
    let high =
        options(RebuildRanges::Through(24)).with_range_caps(families::MAX_RANGE_BLOCKS, events);
    let second = run_to(&fixture, 25, false, &high).await?;
    ensure!(
        second.ranges == u64::try_from(ends.len())? && second.compose_chunks == 0,
        "cap {events}: the risen switch point ranges 17 to 24 after the pass: {second:?}"
    );
    require_equal(
        &fixture,
        &expected,
        &format!("a range after the pass, cap {events}"),
    )
    .await?;
    let journalled = fixture.journalled_blocks().await?;
    ensure!(
        journalled.first() == Some(&14) && ends.iter().all(|end| journalled.contains(end)),
        "cap {events}: the pass at 14, single blocks, the ranges ending at {ends:?} and the \
         target: {journalled:?}"
    );
    let range_rows = count(
        &fixture,
        &format!(
            "SELECT count(*) FROM project_family_undo WHERE chain_id = '{CHAIN}'
             AND block_number = 24 AND family IN ('project_registry_pointer', 'project_lookup_name')"
        ),
    )
    .await?;
    ensure!(
        range_rows > 0,
        "the range journals its family and composed rows"
    );
    let undone = families::undo_to(&fixture.pool, CHAIN, 16).await?;
    ensure!(
        undone == 1 + u64::try_from(ends.len())?,
        "cap {events}: 25, then each range ending at {ends:?} as one step: {undone}"
    );
    fixture
        .apply_with(25, FamilyMode::Normal, &options(RebuildRanges::Off))
        .await?;
    require_equal(
        &fixture,
        &expected,
        &format!("replayed after the undo, cap {events}"),
    )
    .await?;
    fixture.cleanup().await
}

async fn run_to(
    fixture: &Fixture,
    target: i64,
    first: bool,
    options: &FamilyOptions,
) -> Result<FamilyOutcome> {
    let mode = if first {
        FamilyMode::Rebuild
    } else {
        FamilyMode::Normal
    };
    Ok(fixture.apply_with(target, mode, options).await?)
}

// A rebuild stopped inside its ranges, whether a range was cut by the event cap or by the budget,
// has composed and journalled nothing, and resumes to the block-by-block rows.
#[tokio::test]
async fn ranges_cut_by_the_event_cap_or_the_budget_compose_nothing() -> Result<()> {
    let fixture = Fixture::new("families_compose_cut", 40).await?;
    for block in 10..=29 {
        fixture
            .resolver_changed(block, 1, block.unsigned_abs() % 3, R1)
            .await?;
    }
    fixture.resolver_changed(17, 2, 5, R1).await?;
    fixture
        .apply_with(30, FamilyMode::Rebuild, &options(RebuildRanges::Off))
        .await?;
    let expected = fixture.exact().await?;
    let capped = options(RebuildRanges::Through(30))
        .with_range_caps(1024, 2)
        .with_max_blocks_per_run(9);
    let first = run_to(&fixture, 30, true, &capped).await?;
    ensure!(
        first.ranges > 1 && first.budget_exhausted && first.compose_chunks == 0,
        "ranges cut by the cap of two events and by the budget: {first:?}"
    );
    ensure!(
        (
            fixture.composed_rows().await?,
            fixture.journalled_blocks().await?.len()
        ) == (0, 0),
        "no composed row and no journal row"
    );
    loop {
        let outcome = run_to(&fixture, 30, false, &capped).await?;
        ensure!(!outcome.reset, "{outcome:?}");
        if fixture.marker().await?.0 == Some(30) {
            break;
        }
    }
    require_equal(&fixture, &expected, "resumed after cut ranges").await?;
    fixture.cleanup().await
}

/// Where a budgeted redo's rebuild stopped.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Stop {
    Ranges,
    Pass,
    AfterPass,
}

// A rebuild that stops in its ranges or in its pass, a redo resuming it carries it over: the
// record moves to the rerun's attempt with its stream and cursor, and the ranges or the pass
// continue from where they stopped. One that stops after the pass keeps its composed block, and
// the target follows. A redo that does not resume, or one over a moved input revision, rebuilds
// from the start with the pass fields cleared. Each finished rebuild equals the block-by-block
// one.
#[tokio::test]
async fn a_redo_resumes_or_restarts_a_rebuild_in_its_pass() -> Result<()> {
    for (stop, resumes, moved) in [
        (Stop::Ranges, true, false),
        (Stop::Pass, true, false),
        (Stop::AfterPass, true, false),
        (Stop::Pass, false, false),
        (Stop::AfterPass, false, false),
        (Stop::Pass, true, true),
        (Stop::AfterPass, true, true),
    ] {
        let case = format!("stopped {stop:?}, resumed {resumes}, moved revision {moved}");
        let fixture = Fixture::new("families_compose_redo", 30).await?;
        for block in 11..=25 {
            fixture
                .resolver_changed(block, 1, block.unsigned_abs() % 4, R1)
                .await?;
        }
        fixture
            .project_row(7, Some((0, 30, "operator redo")))
            .await?;
        let redo = FamilyMode::Redo { from: 0, to: 30 };
        let ranged = |budget: u64, resumes: bool| {
            options(RebuildRanges::Through(25))
                .with_compose_chunk_keys(1)
                .with_compose_builder_partitions(2, 2)
                .with_max_blocks_per_run(budget)
                .with_resumed_redo(resumes)
        };
        // Five of the 15 work blocks, or all 15 and one chunk of the pass.
        let budget = if stop == Stop::Ranges { 5 } else { 16 };
        let first = fixture
            .apply_with(30, redo.clone(), &ranged(budget, false))
            .await?;
        ensure!(first.reset && first.budget_exhausted, "{case}: {first:?}");
        if stop == Stop::AfterPass {
            while pass_state(&fixture).await?.3.is_null() {
                fixture
                    .apply_with(30, FamilyMode::Normal, &ranged(1, false))
                    .await?;
            }
        }
        let before = pass_state(&fixture).await?;
        let marked_before = marked(&fixture).await?;
        let marker = fixture.marker().await?.0;
        let stopped = match stop {
            Stop::Ranges => before.0 == "rebuilding" && before.3.is_null() && marker < Some(25),
            Stop::Pass => before.0 == "composing" && before.1 == json!(0) && marked_before == 1,
            Stop::AfterPass => {
                before.0 == "rebuilding" && before.3 == json!(25) && marker == Some(25)
            }
        };
        ensure!(stopped, "{case}: {before:?} at {marker:?}");
        fixture
            .project_row(8, Some((0, 30, "operator redo")))
            .await?;
        if moved {
            fixture.interpret_row(CONTENT_HASH, 3, false).await?;
        }
        let second = fixture
            .apply_with(30, redo.clone(), &ranged(1, resumes))
            .await?;
        let after = pass_state(&fixture).await?;
        if resumes && !moved {
            ensure!(!second.reset, "{case}: carried over: {second:?}");
            let record = fixture.repair_record().await?.unwrap_or_default();
            ensure!(record["attempt"] == json!(8), "{case}: {record}");
            let continued = match stop {
                Stop::Ranges => {
                    second.ranges == 1 && fixture.marker().await?.0 > marker && after.3.is_null()
                }
                Stop::Pass => second.compose_chunks == 1 && marked(&fixture).await? == 2,
                Stop::AfterPass => after.3 == json!(25) && second.blocks == 1,
            };
            ensure!(continued, "{case}: {before:?} then {after:?}, {second:?}");
        } else {
            ensure!(second.reset, "{case}: rebuilt from the start: {second:?}");
            ensure!(
                after.3.is_null() && after.0 == "rebuilding",
                "{case}: the pass fields were cleared: {after:?}"
            );
            // The reset clears the pass's work list, and a budget of one block cannot begin a
            // new pass, so a list a stopped pass left is gone.
            let listed = count(&fixture, "SELECT count(*) FROM project_compose_name_work").await?;
            ensure!(
                listed == 0,
                "{case}: the reset left {listed} work list rows"
            );
        }
        while fixture.marker().await?.0 != Some(30)
            || fixture.repair_record().await?.unwrap_or_default()["state"] != json!("complete")
        {
            fixture
                .apply_with(30, FamilyMode::Normal, &ranged(50, resumes))
                .await?;
        }
        let rebuilt = fixture.exact().await?;
        fixture
            .apply_with(30, FamilyMode::Rebuild, &options(RebuildRanges::Off))
            .await?;
        require_equal(&fixture, &rebuilt, &case).await?;
        fixture.cleanup().await?;
    }
    Ok(())
}

/// A manifest update as manifest sync writes it, with no block: a manifest row and its
/// SourceManifestUpdated event.
async fn sync_manifest(fixture: &Fixture) -> Result<()> {
    let payload = json!({"contracts": [{"role": "universal_resolver",
        "address": "0x00000000000000000000000000000000000000f7", "start_block": 0}]});
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions (manifest_version, namespace, source_family, chain_id,
             deployment_label, rollout_status, normalizer_version, file_path, manifest_payload)
         VALUES (1, 'ens', 'ens_execution', $1, 'fixture', 'active', 'fixture',
             'fixture/ens_execution.toml', $2) RETURNING manifest_id",
    )
    .bind(CHAIN)
    .bind(&payload)
    .fetch_one(&fixture.pool)
    .await?;
    sqlx::query(
        "INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
             manifest_version, source_manifest_id, chain_id, derivation_kind,
             canonicality_state, after_state)
         VALUES ($1, 'ens', 'SourceManifestUpdated', 'ens_execution', 1, $2, $3,
             'manifest_sync', 'finalized',
             jsonb_build_object('rollout_status', 'active', 'manifest_payload', $4::jsonb))",
    )
    .bind(format!("sync:{id}"))
    .bind(id)
    .bind(CHAIN)
    .bind(payload)
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

// A manifest sync lands while a rebuild in ranges is stopped before its pass. Sync writes the
// update with no block and moves the Interpret input hash in the same transaction, so the next
// run sees a moved input revision and rebuilds from the start under the new set, as it does
// today. The pass composes with the set its marker recorded: were the run's set to differ from it
// with the revision unmoved, which sync never writes, the pass refuses it rather than composing a
// mix of the two.
#[tokio::test]
async fn a_manifest_sync_before_the_pass_rebuilds_under_the_new_set() -> Result<()> {
    for moved in [true, false] {
        let fixture = Fixture::new("families_compose_manifest", 30).await?;
        for block in 11..=25 {
            fixture
                .resolver_changed(block, 1, block.unsigned_abs() % 4, R1)
                .await?;
        }
        fixture.interpret_row("h1", 1, false).await?;
        let ranged = options(RebuildRanges::Through(25));
        let first = run_to(
            &fixture,
            30,
            true,
            &ranged.clone().with_max_blocks_per_run(15),
        )
        .await?;
        ensure!(
            first.budget_exhausted && pass_state(&fixture).await?.1.is_null(),
            "stopped after the ranges, before the pass: {first:?}"
        );
        sync_manifest(&fixture).await?;
        if moved {
            fixture.interpret_row("h2", 1, false).await?;
            let next = run_to(&fixture, 30, false, &ranged).await?;
            ensure!(
                next.reset,
                "moved {moved}: rebuilt from the start: {next:?}"
            );
            fixture.assert_rebuild_equal(30).await?;
        } else {
            let error = run_to(&fixture, 30, false, &ranged)
                .await
                .expect_err("the pass refuses a set its marker did not record");
            ensure!(error.to_string().contains("manifest set"), "{error}");
        }
        fixture.cleanup().await?;
    }
    Ok(())
}

// The input revision moves between two chunks of the pass, two chunks of names on three workers
// or the catalogue build's first two partitions: the next chunk's fence refuses it and the run
// stops with an error, the committed chunks stay, and the next run rebuilds from the start under
// the new revision. In the names stream the move lands with the record's count of the first
// chunk, and a worker that read the revision before it commits under it.
#[tokio::test]
async fn a_revision_that_moves_between_two_chunks_stops_the_run() -> Result<()> {
    for (stream, moved_after) in [
        (
            0,
            "NEW.compose_stream = 0 AND NEW.compose_chunks = 1 AND OLD.compose_chunks = 0",
        ),
        (
            3,
            "NEW.compose_stream = 3 AND OLD.compose_stream = 3 AND NEW.compose_cursor IS NOT NULL
             AND OLD.compose_cursor IS NULL",
        ),
    ] {
        let fixture = Fixture::with_connections("families_compose_revision", 30, 4).await?;
        for block in 11..=25 {
            fixture
                .resolver_changed(block, 1, block.unsigned_abs() % 4, R1)
                .await?;
        }
        fixture.interpret_row("h1", 1, false).await?;
        raw_sql(&format!(
            "CREATE FUNCTION move_revision() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 UPDATE chain_phase_state SET input_content_hash = 'h2'
                 WHERE phase_name = 'interpret';
                 RETURN NEW;
             END $$;
             CREATE TRIGGER move_revision AFTER UPDATE ON project_repair_record
             FOR EACH ROW WHEN (NEW.state = 'composing' AND {moved_after})
             EXECUTE FUNCTION move_revision();"
        ))
        .execute(&fixture.pool)
        .await?;
        let ranged = options(RebuildRanges::Through(30))
            .with_compose_chunk_keys(1)
            .with_compose_builder_partitions(2, 2)
            .with_compose_workers(3);
        let error = run_to(&fixture, 30, true, &ranged)
            .await
            .expect_err("the second chunk is refused");
        ensure!(error.to_string().contains("revision"), "{error}");
        let (state, at, cursor, _) = pass_state(&fixture).await?;
        let committed = if stream == 0 {
            cursor.is_null() && marked(&fixture).await? >= 1
        } else {
            cursor.is_string()
        };
        ensure!(
            state == "composing" && at == json!(stream) && committed,
            "stream {stream}: a chunk committed before the move: {state} {at} {cursor}"
        );
        if stream == 3 {
            let step = cursor
                .as_str()
                .and_then(|text| serde_json::from_str::<Value>(text).ok())
                .unwrap_or_default();
            let keys = count(
                &fixture,
                "SELECT count(*) FROM project_compose_history_work",
            )
            .await?;
            ensure!(
                step["step"] == json!("keys") && keys > 0,
                "the build's first partition committed and stays: {cursor} with {keys} keys"
            );
        }
        raw_sql("DROP TRIGGER move_revision ON project_repair_record")
            .execute(&fixture.pool)
            .await?;
        let next = run_to(&fixture, 30, false, &ranged).await?;
        ensure!(
            next.reset,
            "stream {stream}: the next run rebuilds: {next:?}"
        );
        ensure!(
            fixture.marker_revision().await? == (Some("h2".to_owned()), Some(1)),
            "stream {stream}: under the new revision"
        );
        fixture.cleanup().await?;
    }
    Ok(())
}

// After a rebuild in ranges completes, the generations above the pass block undo as they always
// did: the first follow block undoes to the rows before it, and a redo whose undo stays above the
// pass block undoes and replays. A redo reaching at or below the pass block, or a marker left on an
// orphaned block at or below it, rebuilds: the walk stops at the pass's marker row and moves no
// row before the reset.
#[tokio::test]
async fn after_a_rebuild_in_ranges_undo_stops_at_the_pass_block() -> Result<()> {
    let fixture = Fixture::new("families_compose_undo", 30).await?;
    for block in 10..=24 {
        fixture
            .resolver_changed(block, 1, block.unsigned_abs() % 3, R1)
            .await?;
    }
    fixture.resolver_changed(26, 1, 1, R1).await?;
    let ranged = options(RebuildRanges::Through(20));
    fixture.apply_with(25, FamilyMode::Rebuild, &ranged).await?;
    ensure!(pass_state(&fixture).await?.3 == json!(20));
    // The first follow block composes, journals and undoes as always.
    fixture.assert_undo_restores(26).await?;

    // A redo of 23..=26 stays above the pass block: it undoes four blocks and replays them.
    fixture.interpret_row("interpret-hash", 2, false).await?;
    fixture
        .project_row(1, Some((23, 26, "required downstream redo")))
        .await?;
    let redo = fixture
        .apply_with(26, FamilyMode::Redo { from: 23, to: 26 }, &ranged)
        .await?;
    ensure!(
        !redo.reset && redo.undone_blocks == 4 && redo.blocks == 4,
        "a redo above the pass block undoes and replays: {redo:?}"
    );

    // A redo of 18..=26 reaches below the pass block: it rebuilds, and no row moved before the
    // reset.
    fixture
        .project_row(2, Some((18, 26, "required downstream redo")))
        .await?;
    let before = fixture.exact().await?;
    ensure!(
        families::undo_to(&fixture.pool, CHAIN, 17).await? == 0,
        "the walk to 17 stops at the pass's marker row"
    );
    ensure!(fixture.exact().await? == before, "no row moved");
    let deep = fixture
        .apply_with(26, FamilyMode::Redo { from: 18, to: 26 }, &ranged)
        .await?;
    ensure!(
        deep.reset && deep.undone_blocks == 0,
        "a redo below the pass block rebuilds: {deep:?}"
    );
    fixture.assert_rebuild_equal(26).await?;
    fixture.cleanup().await
}

/// One run to `target` on the branch that holds it.
async fn apply_target(
    fixture: &Fixture,
    target: &bigname_project::Marker,
    mode: FamilyMode,
    options: &FamilyOptions,
) -> Result<FamilyOutcome> {
    let token = families::input_token(&fixture.pool, CHAIN).await?;
    Ok(families::apply(&fixture.pool, CHAIN, target, mode, &token, options).await?)
}

fn sibling(block: i64) -> String {
    format!("0x{:064x}", block + 4096)
}

// A reorg after a rebuild in ranges. One that replaces the two newest blocks, above the pass
// block, undoes them and replays the new branch as always. One that replaces a block at or below
// the pass block rebuilds: the walk to its common ancestor stops at the pass's marker row. So does
// one that orphans the pass block while the marker stands on it, after an undo of the target:
// undoing the marker's own block meets the pass's marker row, which restores nothing. Each result
// equals a block-by-block rebuild of the new branch.
#[tokio::test]
async fn a_reorg_at_or_below_the_pass_block_rebuilds() -> Result<()> {
    for (through, orphaned, undo_first, rebuilds) in [
        (20, 24, false, false),
        (24, 18, false, true),
        (24, 24, true, true),
    ] {
        let case = format!("pass at {through}, orphaned from {orphaned}");
        let fixture = Fixture::new("families_compose_orphan", 25).await?;
        for block in 10..=24 {
            fixture
                .resolver_changed(block, 1, block.unsigned_abs() % 3, R1)
                .await?;
        }
        fixture
            .apply_with(
                25,
                FamilyMode::Rebuild,
                &options(RebuildRanges::Through(through)),
            )
            .await?;
        ensure!(
            pass_state(&fixture).await?.3 == json!(through),
            "{case}: {:?}",
            pass_state(&fixture).await?
        );
        if undo_first {
            ensure!(
                families::undo_to(&fixture.pool, CHAIN, 24).await? == 1,
                "{case}"
            );
            ensure!(fixture.marker().await?.0 == Some(24), "{case}");
        }
        sqlx::query(
            "UPDATE chain_lineage SET canonicality_state = 'orphaned'
             WHERE chain_id = $1 AND block_number >= $2",
        )
        .bind(CHAIN)
        .bind(orphaned)
        .execute(&fixture.pool)
        .await?;
        for block in orphaned..=25 {
            let parent = if block == orphaned {
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
        let target = bigname_project::Marker {
            number: 25,
            hash: sibling(25),
        };
        let outcome = apply_target(
            &fixture,
            &target,
            FamilyMode::Normal,
            &options(RebuildRanges::Through(through)),
        )
        .await?;
        ensure!(
            outcome.reset == rebuilds
                && (rebuilds || outcome.undone_blocks == 2)
                && (!rebuilds || outcome.undone_blocks == 0),
            "{case}: {outcome:?}"
        );
        ensure!(
            fixture.marker().await?.1.as_deref() == Some(sibling(25).as_str()),
            "{case}: on the new branch"
        );
        let followed = fixture.exact().await?;
        apply_target(
            &fixture,
            &target,
            FamilyMode::Rebuild,
            &options(RebuildRanges::Off),
        )
        .await?;
        require_equal(&fixture, &followed, &case).await?;
        fixture.cleanup().await?;
    }
    Ok(())
}

// The ENSv2 root admission changes at a block inside the ranges before the pass, here a
// fixture's manifest update positioned at block 6 (production manifest sync writes no block).
// Main's publication sweeps every name for it and refuses a followed sweep above the swept-name
// cap. A range before the pass composes nothing, so it sweeps nothing and is not capped, even with
// a cap of one name: the pass composes every name once with the marker's admission, and the
// rebuild equals the block-by-block one.
#[tokio::test]
async fn an_admission_change_before_the_pass_is_neither_swept_nor_capped() -> Result<()> {
    let fixture = Fixture::new("families_compose_admission", 14).await?;
    ens_v2_path::history(&fixture).await?;
    let positioned = sqlx::query(
        "UPDATE normalized_events SET block_number = 6, block_hash = $2
         WHERE chain_id = $1 AND event_kind = 'SourceManifestUpdated'
           AND source_family = 'ens_v2_root_l1'",
    )
    .bind(CHAIN)
    .bind(hash(6))
    .execute(&fixture.pool)
    .await?;
    ensure!(
        positioned.rows_affected() > 0,
        "the root admission is positioned"
    );
    fixture
        .apply_with(13, FamilyMode::Rebuild, &options(RebuildRanges::Off))
        .await?;
    let expected = fixture.exact().await?;
    let capped = options(RebuildRanges::Through(13)).with_max_admission_sweep_names(1);
    let outcome = fixture.apply_with(13, FamilyMode::Rebuild, &capped).await?;
    ensure!(
        outcome.ranges > 1 && outcome.compose_chunks > 0,
        "ranges across block 6, then the pass: {outcome:?}"
    );
    require_equal(&fixture, &expected, "an admission change before the pass").await?;
    fixture.cleanup().await
}
