//! The composition pass of a rebuild in ranges (docs/projections.md, "Owned key families"). The
//! ranges at or below the switch point write the family rows only, and the pass composes what
//! their blocks would each have composed, once, at the last range's block, in chunks that each
//! commit on their own. Every case rebuilds a history in ranges and requires every family table
//! and the marker, apart from its generation, as a rebuild block by block leaves them.
//!
//! The history of most cases is `rebuild_performance/seed.sql` for 60 names with the ENSv2 path
//! history of `families_support/ens_v2_path.rs`, as `families_recorded_rows.rs` uses it, and on
//! top: ENSv1 registry children a NewOwner creates with no name surface, record values by node and
//! by record id, unnamed registrar transfers of a name with no surface in two blocks, and an
//! unnamed resource pointer whose node moves.
mod families_support;

use anyhow::{Result, ensure};
use bigname_project::families::{self, FamilyMode, FamilyOptions, FamilyOutcome, RebuildRanges};
use families_support::{CHAIN, CONTENT_HASH, Fixture, ens_v2_path, hash, uuid};
use serde_json::{Value, json};
use sqlx::raw_sql;

const SEED: &str = include_str!("rebuild_performance/seed.sql");
const TARGET: i64 = 300;
/// The switch point of the cases with single blocks above it: the work blocks at or below it go
/// in ranges, those above it one to a transaction.
const SWITCH: i64 = 200;
const R1: &str = "0x00000000000000000000000000000000000000a1";
const ALICE: &str = "0x00000000000000000000000000000000000000aa";
const CAROL: &str = "0x00000000000000000000000000000000000000cc";
/// The namehash of a registrar name no surface names, transferred unnamed at blocks 160 and 180.
const TRANSFERRED: &str = "0x00000000000000000000000000000000000000000000000000000000000f01d0";
/// The nodes an unnamed resource pointer names at block 165, then at block 185. Neither has a
/// surface.
const MOVED_FROM: &str = "0x000000000000000000000000000000000000000000000000000000000000a0a0";
const MOVED_TO: &str = "0x000000000000000000000000000000000000000000000000000000000000b0b0";

/// ENSv1 registry children with no name surface, as `families_benchmark.rs` writes them: one per
/// twentieth name, created by a NewOwner under a seed name, every second one transferred 30
/// blocks later.
const REGISTRY_CHILDREN: &str = "
INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state)
SELECT md5('c' || i)::uuid, '__CHAIN__', '0x' || lpad(to_hex(1 + (i % 270)), 64, '0'),
       1 + (i % 270), 'canonical'::canonicality_state
FROM generate_series(1, __NAMES__ / 20) i;
INSERT INTO normalized_events (event_identity, namespace, resource_id, event_kind, source_family,
    manifest_version, chain_id, block_number, block_hash, transaction_hash, transaction_index,
    log_index, derivation_kind, canonicality_state, after_state, raw_fact_ref)
SELECT 'seed:child:' || kind || ':' || i, 'ens', md5('c' || i)::uuid, kind,
       'ens_v1_registry_l1', 1, '__CHAIN__', block, '0x' || lpad(to_hex(block), 64, '0'),
       '0x' || md5('ct' || i || kind), 0, 20, 'ens_v1_unwrapped_authority',
       'canonical'::canonicality_state,
       CASE kind
           WHEN 'SubregistryChanged' THEN jsonb_build_object('source_event', 'NewOwner',
               'node', '0x' || md5('n' || i) || md5('m' || i),
               'child_node', '0x' || md5('cn' || i) || md5('cm' || i),
               'labelhash', '0x' || md5('cl' || i) || md5('ck' || i),
               'owner', '0x' || substr(md5('co' || i), 1, 40),
               'owner_getter', '0x' || substr(md5('co' || i), 1, 40), 'emitter_role', 'registry')
           ELSE jsonb_build_object('source_event', 'Transfer',
               'node', '0x' || md5('cn' || i) || md5('cm' || i),
               'owner', '0x' || substr(md5('cp' || i), 1, 40),
               'owner_getter', '0x' || substr(md5('cp' || i), 1, 40), 'emitter_role', 'registry')
       END,
       '{\"emitting_address\":\"0x00000000000000000000000000000000000000a3\"}'
FROM generate_series(1, __NAMES__ / 20) i
CROSS JOIN LATERAL (VALUES ('SubregistryChanged', 1 + (i % 270)),
    ('AuthorityTransferred', 31 + (i % 270))) kinds(kind, block)
WHERE kind = 'SubregistryChanged' OR i % 2 = 0;
";

fn node(n: u64) -> String {
    format!("0x{n:064x}")
}

/// The history described at the top, on canonical blocks 0 to `TARGET`.
async fn seeded(prefix: &str) -> Result<Fixture> {
    let fixture = Fixture::new(prefix, 0).await?;
    for script in [SEED, REGISTRY_CHILDREN] {
        raw_sql(
            &script
                .replace("__NAMES__", "60")
                .replace("__CHAIN__", CHAIN)
                .replace("1700000000", "1800000000"),
        )
        .execute(&fixture.pool)
        .await?;
    }
    ens_v2_path::history(&fixture).await?;
    let addr = |node: String, value: &str| {
        json!({"node": node, "resolver": R1, "record_key": "addr:60", "record_family": "addr",
               "selector_key": "60", "value": value, "source_event": "AddressChanged"})
    };
    fixture
        .write(
            150,
            1,
            "RecordChanged",
            "ens_v1_resolver_l1",
            None,
            None,
            addr(node(1), ALICE),
            R1,
        )
        .await?;
    fixture
        .write(
            151,
            1,
            "RecordChanged",
            "ens_v2_resolver_l1",
            None,
            None,
            json!({"resolver": R1, "resolver_record_id": "7",
                   "storage_model": "resolver_record_id", "record_key": "addr:60",
                   "record_family": "addr", "selector_key": "60", "value": CAROL,
                   "source_event": "AddressUpdated"}),
            R1,
        )
        .await?;
    let lease = uuid(901);
    for (block, to) in [(160, ALICE), (180, CAROL)] {
        fixture
            .write(
                block,
                7,
                "TokenControlTransferred",
                "ens_v1_registrar_l1",
                None,
                Some(&lease),
                json!({"namehash": TRANSFERRED, "to": to, "source_event": "Transfer",
                       "authority_kind": "registrar"}),
                R1,
            )
            .await?;
    }
    let pointer = uuid(902);
    for (block, at) in [(165, MOVED_FROM), (185, MOVED_TO)] {
        fixture
            .write(
                block,
                7,
                "ResolverChanged",
                "ens_v1_registry_l1",
                None,
                Some(&pointer),
                json!({"node": at, "resolver": R1}),
                R1,
            )
            .await?;
    }
    Ok(fixture)
}

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
async fn require_equal(fixture: &Fixture, expected: &[(String, String)], label: &str) -> Result<()> {
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

// Rebuilt in ranges through the target with one name to a chunk and catalogue windows of at most
// 25 events, the history equals its block-by-block rebuild in every family table, the 12 composed
// tables among them. The comparison covers what it is meant to: lookup rows with no composed core
// and empty name sources of ids with no surface, one inventory served by names in different
// chunks, resource anchors, catalogue sources by node and by record id, and both record indexes.
// Each id with no surface keeps the rows block by block leaves it: a registry child a NewOwner
// created has a lookup row and a name source; the name of a registrar transfer in one block has a
// lookup row only, and the one transferred in two blocks has both, since only a later block's
// rewrite of its address fold names it to the catalogue; and the node an unnamed pointer moved away
// from keeps both, though no row carries that node any more.
#[tokio::test]
async fn a_rebuild_in_ranges_composes_once_what_block_by_block_composes() -> Result<()> {
    let fixture = seeded("families_compose_equal").await?;
    let expected = block_by_block(&fixture).await?;
    let mut ranged = options(RebuildRanges::Through(TARGET))
        .with_compose_chunk_keys(1)
        .with_range_caps(64, u64::MAX);
    ranged.max_range_events = 25;
    let outcome = fixture
        .apply_with(TARGET, FamilyMode::Rebuild, &ranged)
        .await?;
    require_equal(&fixture, &expected, "in ranges").await?;
    let (state, stream, cursor, composed) = pass_state(&fixture).await?;
    ensure!(
        (state.as_str(), &stream, &cursor) == ("complete", &Value::Null, &Value::Null),
        "the record completes with no pass in progress: {state} {stream} {cursor}"
    );
    ensure!(
        composed.as_i64().is_some_and(|block| block < TARGET),
        "the pass composed below the target: {composed}"
    );
    ensure!(
        outcome.compose_chunks > 100,
        "one name a chunk: {} chunks",
        outcome.compose_chunks
    );
    let journalled = fixture.journalled_blocks().await?;
    ensure!(
        journalled == vec![composed.as_i64().unwrap_or_default(), TARGET],
        "the pass's marker row and the target: {journalled:?}"
    );

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
            "SELECT count(*) FROM (SELECT record_serving_resource_id FROM project_lookup_name
             WHERE record_serving_resource_id IS NOT NULL
             GROUP BY 1 HAVING count(*) > 1) shared",
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
    ensure!(child > 0, "the history has a registry child with no surface");
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
        (once, (true, false), "a name transferred unnamed in one block"),
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
    blocks: u64,
    ranges: u64,
    undo_rows: u64,
    marker: Option<i64>,
    state: String,
    stream: Value,
    cursor: Value,
    composed: Value,
}

// A pass longer than one run. The first run's budget is the work blocks at or below the switch
// point exactly: it applies them in ranges and stops before the pass, the record still rebuilding
// with no pass recorded. Every later run has a budget of one block: it commits at most one chunk
// of five keys, or the pass's end, or one single block. The pass continues from the cursor the
// previous run left, a stream starts from its beginning once the one before it is done, and a run
// that finds the last window done only ends the pass. The single blocks above the switch point
// then compose and journal as block by block does, and the finished families equal it.
#[tokio::test]
async fn a_pass_longer_than_a_run_continues_from_its_cursor() -> Result<()> {
    let fixture = seeded("families_compose_resume").await?;
    let expected = block_by_block(&fixture).await?;
    let pre_pass = work_blocks(&fixture, SWITCH).await?;
    let mut budgeted = options(RebuildRanges::Through(SWITCH))
        .with_compose_chunk_keys(5)
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
            == ("rebuilding".to_owned(), Value::Null, Value::Null, Value::Null),
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
            blocks: outcome.blocks,
            ranges: outcome.ranges,
            undo_rows: outcome.undo_rows,
            marker: fixture.marker().await?.0,
            state,
            stream,
            cursor,
            composed,
        });
        if seen.last().is_some_and(|last| last.marker == Some(TARGET)) {
            break;
        }
        ensure!(seen.len() < 2000, "the rebuild does not converge");
    }
    require_equal(&fixture, &expected, "a pass over runs of one block").await?;

    // Each run commits at most one chunk, the pass's end or one block, and no range.
    for (index, run) in seen.iter().enumerate() {
        ensure!(
            run.chunks + run.blocks <= 1 && run.ranges == 0,
            "run {index} spent more than its budget: {run:?}"
        );
    }
    // The pass runs its streams in order, each from its beginning, until it ends.
    let passing: Vec<&Seen> = seen.iter().filter(|run| run.state == "composing").collect();
    let streams: Vec<i64> = passing.iter().filter_map(|run| run.stream.as_i64()).collect();
    ensure!(
        streams.windows(2).all(|pair| pair[0] <= pair[1]),
        "the streams run in order: {streams:?}"
    );
    for stream in 0..=3 {
        let cursors: Vec<&Value> = passing
            .iter()
            .filter(|run| run.stream.as_i64() == Some(stream))
            .map(|run| &run.cursor)
            .collect();
        ensure!(
            cursors.iter().filter(|cursor| !cursor.is_null()).count() > 1,
            "stream {stream} took more than one run: {cursors:?}"
        );
        let texts: Vec<String> = cursors
            .iter()
            .filter_map(|cursor| cursor.as_str().map(str::to_owned))
            .collect();
        if stream != 3 {
            let mut sorted = texts.clone();
            sorted.sort();
            sorted.dedup();
            ensure!(sorted == texts, "stream {stream}'s cursor only moves on: {texts:?}");
        }
    }
    // The run that found the last window done ended the pass and did nothing else.
    let ended = seen
        .iter()
        .position(|run| run.composed.is_i64())
        .expect("the pass ends");
    let end = &seen[ended];
    ensure!(
        (end.chunks, end.blocks, end.undo_rows, end.marker) == (0, 0, 1, pass_block)
            && end.composed.as_i64() == pass_block
            && end.state == "rebuilding",
        "the pass's end: {end:?}"
    );
    let before = &seen[ended - 1];
    ensure!(
        before.stream == json!(3) && before.cursor == json!(pass_block.unwrap_or(-1).to_string()),
        "the last window was done before the end: {before:?}"
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
    let lookup_journal = count(
        &fixture,
        &format!(
            "SELECT count(*) FROM project_family_undo WHERE chain_id = '{CHAIN}'
             AND block_number > {SWITCH} AND family LIKE 'project_lookup_%'"
        ),
    )
    .await?;
    ensure!(
        lookup_journal > 0,
        "a single block journals the lookup rows it composes"
    );
    fixture.cleanup().await
}

/// A trigger that fails the `nth` write of a composition chunk to the repair record, once: the
/// sequence survives the rollback the failure causes.
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
         CREATE TRIGGER fail_chunk BEFORE UPDATE ON project_repair_record
         FOR EACH ROW WHEN (OLD.state = 'composing' AND NEW.state = 'composing')
         EXECUTE FUNCTION fail_chunk();"
    ))
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

async fn allow_chunks(fixture: &Fixture) -> Result<()> {
    raw_sql(
        "DROP TRIGGER fail_chunk ON project_repair_record;
         DROP FUNCTION fail_chunk(); DROP SEQUENCE fail_chunk_once;",
    )
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

// A run that stops after the pass began and before its first chunk committed leaves the record
// composing at the start of the names stream and no composed row, and the next run composes from
// there. A chunk that fails to commit after two committed leaves the cursor and every row where
// the second chunk left them, and the next run composes that chunk again. Both rebuilds equal the
// block-by-block one.
#[tokio::test]
async fn a_chunk_that_fails_to_commit_is_composed_again_from_the_cursor() -> Result<()> {
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
        pass_state(&fixture).await?
            == ("composing".to_owned(), json!(0), Value::Null, Value::Null),
        "the pass began at the start of the names stream: {:?}",
        pass_state(&fixture).await?
    );
    ensure!(
        fixture.composed_rows().await? == 0,
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
        (state.as_str(), &stream) == ("composing", &json!(0)) && cursor.is_string(),
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
        pass_state(&fixture).await?.2 == cursor,
        "the cursor is unchanged"
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
    require_equal(&fixture, &expected.0, "with no range").await?;
    ensure!(
        fixture.journalled_blocks().await? == expected.1,
        "every block journals as block by block"
    );
    fixture.cleanup().await
}

// The switch point rises between two runs: the second run applies blocks above the first run's
// switch point in a range, after the pass. That range composes and journals as one generation, so
// it undoes as one step, and the rebuild equals the block-by-block one.
#[tokio::test]
async fn a_range_after_the_pass_composes_and_journals() -> Result<()> {
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
    // Ranges [10], [11, 12] and [13, 14], the pass (a chunk of names, one window and its end),
    // then single blocks 15 and 16 before the budget runs out.
    let low = options(RebuildRanges::Through(14)).with_max_blocks_per_run(5 + 3 + 2);
    run_to(&fixture, 25, true, &low).await?;
    let (state, _, _, composed) = pass_state(&fixture).await?;
    ensure!(
        state == "rebuilding" && composed == json!(14),
        "the pass composed at 14: {state} {composed}"
    );
    let stopped = fixture.marker().await?.0;
    ensure!(stopped == Some(16), "two single blocks after the pass: {stopped:?}");
    let high = options(RebuildRanges::Through(24));
    let second = run_to(&fixture, 25, false, &high).await?;
    ensure!(
        second.ranges > 0 && second.compose_chunks == 0,
        "the risen switch point ranges 17 to 24 after the pass: {second:?}"
    );
    require_equal(&fixture, &expected, "a range after the pass").await?;
    let journalled = fixture.journalled_blocks().await?;
    ensure!(
        journalled.first() == Some(&14) && journalled.contains(&24),
        "the pass at 14, single blocks, the range ending at 24 and the target: {journalled:?}"
    );
    let range_rows = count(
        &fixture,
        &format!(
            "SELECT count(*) FROM project_family_undo WHERE chain_id = '{CHAIN}'
             AND block_number = 24 AND family IN ('project_registry_pointer', 'project_lookup_name')"
        ),
    )
    .await?;
    ensure!(range_rows > 0, "the range journals its family and composed rows");
    let undone = families::undo_to(&fixture.pool, CHAIN, 16).await?;
    ensure!(undone == 2, "25, then the range ending at 24 as one step: {undone}");
    fixture
        .apply_with(25, FamilyMode::Normal, &options(RebuildRanges::Off))
        .await?;
    require_equal(&fixture, &expected, "replayed after the undo").await?;
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

// A rebuild that stops in the pass, a redo resuming it carries the pass over: the record moves to
// the rerun's attempt with its stream and cursor, and the pass continues from them. One that stops
// after the pass keeps its composed block, and the target follows. A redo that does not resume, or
// one over a moved input revision, rebuilds from the start with the pass fields cleared. Each
// finished rebuild equals the block-by-block one.
#[tokio::test]
async fn a_redo_resumes_or_restarts_a_rebuild_in_its_pass() -> Result<()> {
    for (case, in_pass, resumes, moved) in [
        ("in the pass, resumed", true, true, false),
        ("after the pass, resumed", false, true, false),
        ("in the pass, not resumed", true, false, false),
        ("after the pass, not resumed", false, false, false),
        ("in the pass, moved revision", true, true, true),
        ("after the pass, moved revision", false, true, true),
    ] {
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
                .with_max_blocks_per_run(budget)
                .with_resumed_redo(resumes)
        };
        // The 15 work blocks in ranges and one chunk of the pass.
        let first = fixture
            .apply_with(30, redo.clone(), &ranged(16, false))
            .await?;
        ensure!(first.reset && first.budget_exhausted, "{case}: {first:?}");
        if !in_pass {
            while pass_state(&fixture).await?.3.is_null() {
                fixture
                    .apply_with(30, FamilyMode::Normal, &ranged(1, false))
                    .await?;
            }
        }
        let before = pass_state(&fixture).await?;
        if in_pass {
            ensure!(
                before.0 == "composing" && before.2.is_string(),
                "{case}: stopped in the pass: {before:?}"
            );
        } else {
            ensure!(
                before.0 == "rebuilding"
                    && before.3 == json!(25)
                    && fixture.marker().await?.0 == Some(25),
                "{case}: stopped after the pass: {before:?}"
            );
        }
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
            if in_pass {
                ensure!(
                    second.compose_chunks == 1 && (&after.1, &after.2) != (&before.1, &before.2),
                    "{case}: the pass continued from its cursor: {before:?} then {after:?}"
                );
            } else {
                ensure!(
                    after.3 == json!(25) && second.blocks == 1,
                    "{case}: the composed block is kept and the target applied: {after:?}"
                );
            }
        } else {
            ensure!(second.reset, "{case}: rebuilt from the start: {second:?}");
            ensure!(
                after.3.is_null() && after.0 == "rebuilding",
                "{case}: the pass fields were cleared: {after:?}"
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

// The input revision moves between two chunks of the pass: the next chunk's fence refuses it and
// the run stops with an error, and the next run rebuilds from the start under the new revision.
#[tokio::test]
async fn a_revision_that_moves_between_two_chunks_stops_the_run() -> Result<()> {
    let fixture = Fixture::new("families_compose_revision", 30).await?;
    for block in 11..=25 {
        fixture
            .resolver_changed(block, 1, block.unsigned_abs() % 4, R1)
            .await?;
    }
    fixture.interpret_row("h1", 1, false).await?;
    raw_sql(
        "CREATE FUNCTION move_revision() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             UPDATE chain_phase_state SET input_content_hash = 'h2'
             WHERE phase_name = 'interpret';
             RETURN NEW;
         END $$;
         CREATE TRIGGER move_revision AFTER UPDATE ON project_repair_record
         FOR EACH ROW WHEN (NEW.state = 'composing' AND NEW.compose_cursor IS NOT NULL
                            AND OLD.compose_cursor IS NULL)
         EXECUTE FUNCTION move_revision();",
    )
    .execute(&fixture.pool)
    .await?;
    let ranged = options(RebuildRanges::Through(30)).with_compose_chunk_keys(1);
    let error = run_to(&fixture, 30, true, &ranged)
        .await
        .expect_err("the second chunk is refused");
    ensure!(error.to_string().contains("revision"), "{error}");
    let (state, stream, cursor, _) = pass_state(&fixture).await?;
    ensure!(
        state == "composing" && stream == json!(0) && cursor.is_string(),
        "one chunk committed before the move: {state} {stream} {cursor}"
    );
    raw_sql("DROP TRIGGER move_revision ON project_repair_record")
        .execute(&fixture.pool)
        .await?;
    let next = run_to(&fixture, 30, false, &ranged).await?;
    ensure!(next.reset, "the next run rebuilds: {next:?}");
    ensure!(
        fixture.marker_revision().await? == (Some("h2".to_owned()), Some(1)),
        "under the new revision"
    );
    fixture.cleanup().await
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
    fixture
        .apply_with(25, FamilyMode::Rebuild, &ranged)
        .await?;
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
    for (through, orphaned, undo_first, rebuilds) in
        [(20, 24, false, false), (24, 18, false, true), (24, 24, true, true)]
    {
        let case = format!("pass at {through}, orphaned from {orphaned}");
        let fixture = Fixture::new("families_compose_orphan", 25).await?;
        for block in 10..=24 {
            fixture
                .resolver_changed(block, 1, block.unsigned_abs() % 3, R1)
                .await?;
        }
        fixture
            .apply_with(25, FamilyMode::Rebuild, &options(RebuildRanges::Through(through)))
            .await?;
        ensure!(
            pass_state(&fixture).await?.3 == json!(through),
            "{case}: {:?}",
            pass_state(&fixture).await?
        );
        if undo_first {
            ensure!(families::undo_to(&fixture.pool, CHAIN, 24).await? == 1, "{case}");
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
