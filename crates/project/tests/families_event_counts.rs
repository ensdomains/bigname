//! The stored event counts, family F20 (`docs/projections.md`, "Event counts"). After every
//! publication, block by block, after an undo and its replay, and after a rebuild whose
//! composition pass counts in several windows over several runs, each stored count equals the
//! `total_count` of the event read it serves at the family marker's block: the
//! `contract_address` read per namespace and kind for an emitter, the `resolver` read for the sum
//! of a resolver's three anchor kinds, and the registry overview's read across namespaces. A key
//! no read counts has no row.
#[path = "families_support/mod.rs"]
mod support;

use std::collections::BTreeMap;

use anyhow::{Context, Result, ensure};
use bigname_project::families::{self, FamilyMode, FamilyOptions, RebuildRanges};
use bigname_storage::{
    ChainBlockRange, EventHistoryFilter, EventHistoryResolverFilter, HistoryBlockWindow,
    HistorySummaryMode, load_event_history_page,
};
use serde_json::json;
use support::{CHAIN, CONTENT_HASH, Event, Fixture};

const EMITTER: &str = "0x00000000000000000000000000000000000000e1";
/// The emitter `Event::new` gives every row it writes.
const REGISTRY: &str = "0x00000000000000000000000000000000000000a4";
const RESOLVER_1: &str = "0x00000000000000000000000000000000000000c1";
const RESOLVER_2: &str = "0x00000000000000000000000000000000000000c2";
const ANCHORS: [&str; 4] = [EMITTER, REGISTRY, RESOLVER_1, RESOLVER_2];
const NAMESPACES: [&str; 2] = ["ens", "basenames"];
/// The cap of the event feed's `include=total_count`.
const CAP: u64 = 100_000;
const FAMILY: &str = "test_counts_l1";

type Key = (i16, String, String, String);

async fn stored(fixture: &Fixture) -> Result<BTreeMap<Key, i64>> {
    let rows: Vec<(i16, String, String, String, i64)> = sqlx::query_as(
        "SELECT anchor_kind, anchor, namespace, event_kind, event_count
         FROM project_event_count WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .fetch_all(&fixture.pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(kind, anchor, namespace, event, count)| ((kind, anchor, namespace, event), count))
        .collect())
}

async fn read(fixture: &Fixture, mut filter: EventHistoryFilter, block: i64) -> Result<i64> {
    filter.block_window = Some(HistoryBlockWindow {
        ranges: vec![ChainBlockRange {
            chain_id: CHAIN.to_owned(),
            from_block: None,
            to_block: Some(block),
        }],
    });
    let page = load_event_history_page(
        &fixture.pool,
        filter,
        true,
        None,
        1,
        HistorySummaryMode::CappedCount(CAP),
        false,
    )
    .await?;
    Ok(i64::try_from(
        page.summary.context("a counted page")?.total_count,
    )?)
}

/// The stored counts against the reads at the published block, for every anchor, namespace and
/// event kind of the chain's events; returns the counts.
async fn require_reads(fixture: &Fixture, label: &str) -> Result<BTreeMap<Key, i64>> {
    let block = fixture.marker().await?.0.context("a published block")?;
    let counts = stored(fixture).await?;
    let kinds: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT event_kind FROM normalized_events WHERE chain_id = $1 ORDER BY 1",
    )
    .bind(CHAIN)
    .fetch_all(&fixture.pool)
    .await?;
    let count = |kind: i16, anchor: &str, namespace: &str, event: &str| {
        counts
            .get(&(
                kind,
                anchor.to_owned(),
                namespace.to_owned(),
                event.to_owned(),
            ))
            .copied()
            .unwrap_or(0)
    };
    let mut failures = Vec::new();
    for anchor in ANCHORS {
        for kind in &kinds {
            let mut overview = 0;
            for namespace in NAMESPACES {
                let emitted = count(0, anchor, namespace, kind);
                overview += emitted;
                let feed = EventHistoryFilter {
                    contract_address: Some(anchor.to_owned()),
                    namespace: Some(namespace.to_owned()),
                    event_kinds: vec![kind.clone()],
                    ..EventHistoryFilter::default()
                };
                let walked = read(fixture, feed, block).await?;
                if walked != emitted {
                    failures.push(format!("contract {anchor} {namespace} {kind}: {walked}"));
                }
                let own =
                    emitted + count(1, anchor, namespace, kind) + count(2, anchor, namespace, kind);
                let pointers = EventHistoryFilter {
                    resolver: Some(EventHistoryResolverFilter {
                        chain_id: CHAIN.to_owned(),
                        address: anchor.to_owned(),
                    }),
                    namespace: Some(namespace.to_owned()),
                    event_kinds: vec![kind.clone()],
                    ..EventHistoryFilter::default()
                };
                let walked = read(fixture, pointers, block).await?;
                if walked != own {
                    failures.push(format!("resolver {anchor} {namespace} {kind}: {walked}"));
                }
            }
            let every = EventHistoryFilter {
                contract_address: Some(anchor.to_owned()),
                event_kinds: vec![kind.clone()],
                ..EventHistoryFilter::default()
            };
            let walked = read(fixture, every, block).await?;
            if walked != overview {
                failures.push(format!("overview {anchor} {kind}: {walked}"));
            }
        }
    }
    let foreign: Vec<&Key> = counts
        .keys()
        .filter(|key| !ANCHORS.contains(&key.1.as_str()))
        .collect();
    ensure!(
        failures.is_empty() && foreign.is_empty(),
        "{label} at {block}: stored {counts:?}, reads {failures:#?}, unread keys {foreign:?}"
    );
    Ok(counts)
}

fn expected(rows: &[(i16, &str, &str, &str, i64)]) -> BTreeMap<Key, i64> {
    rows.iter()
        .map(|(kind, anchor, namespace, event, count)| {
            (
                (
                    *kind,
                    (*anchor).to_owned(),
                    (*namespace).to_owned(),
                    (*event).to_owned(),
                ),
                *count,
            )
        })
        .collect()
}

async fn emitted(fixture: &Fixture, identity: &str, block: i64, kind: &str) -> Result<()> {
    fixture
        .event(
            Event::new(identity, block, 0, kind, FAMILY).raw(json!({"emitting_address": EMITTER})),
        )
        .await?;
    Ok(())
}

/// A ResolverChanged of the name numbered `name` from `before` to `after`, emitted by `REGISTRY`.
async fn pointer(
    fixture: &Fixture,
    block: i64,
    log: i64,
    name: u64,
    before: Option<&str>,
    after: &str,
) -> Result<()> {
    let node = format!("0x{name:064x}");
    let logical_name_id = format!("ens:{node}");
    fixture.surface(&logical_name_id, &node).await?;
    let identity = format!("pointer-{block}-{log}");
    let mut event = Event::new(
        &identity,
        block,
        log,
        "ResolverChanged",
        "ens_v1_registry_l1",
    )
    .name(&logical_name_id)
    .after(json!({"resolver": after, "node": node}));
    if let Some(before) = before {
        event = event.before(json!({"resolver": before, "node": node}));
    }
    fixture.event(event).await?;
    Ok(())
}

async fn update(fixture: &Fixture, sql: &str) -> Result<()> {
    sqlx::raw_sql(sql).execute(&fixture.pool).await?;
    Ok(())
}

/// Blocks 1 to 5 one publication each: an emitter's first event, the same emitter in another kind
/// and namespace, a resolver's own event and a pointer set to it, the pointer moved to another
/// resolver beside a candidate row, then an orphaned row and a row without a block. Then an undo of
/// block 4 restores the counts before it, its replay counts it once, block 5 is replaced by other
/// events, and both rebuilds give the same rows.
#[tokio::test]
async fn stored_event_counts_follow_every_publication() -> Result<()> {
    let fixture = Fixture::new("families_event_counts", 5).await?;
    emitted(&fixture, "a-1", 1, "CounterA").await?;
    emitted(&fixture, "b-2", 2, "CounterB").await?;
    emitted(&fixture, "a-2-basenames", 2, "CounterA").await?;
    update(
        &fixture,
        "UPDATE normalized_events SET namespace = 'basenames'
         WHERE event_identity = 'a-2-basenames'",
    )
    .await?;
    emitted(&fixture, "a-3", 3, "CounterA").await?;
    fixture
        .event(
            Event::new("own-3", 3, 1, "CounterA", FAMILY)
                .raw(json!({"emitting_address": RESOLVER_1})),
        )
        .await?;
    pointer(&fixture, 3, 2, 1, None, RESOLVER_1).await?;
    pointer(&fixture, 4, 0, 1, Some(RESOLVER_1), RESOLVER_2).await?;
    emitted(&fixture, "candidate-4", 4, "CounterA").await?;
    update(
        &fixture,
        "UPDATE normalized_events SET consumer_visibility = 'candidate',
             migration_correlation_ids = ARRAY['event-count-test']
         WHERE event_identity = 'candidate-4'",
    )
    .await?;
    emitted(&fixture, "orphaned-5", 5, "CounterA").await?;
    emitted(&fixture, "unpositioned-5", 5, "CounterB").await?;
    update(
        &fixture,
        "UPDATE normalized_events SET canonicality_state = 'orphaned'
         WHERE event_identity = 'orphaned-5';
         UPDATE normalized_events SET block_number = NULL, block_hash = NULL,
             transaction_hash = NULL, transaction_index = NULL, log_index = NULL
         WHERE event_identity = 'unpositioned-5'",
    )
    .await?;

    fixture.apply(1, FamilyMode::Normal).await?;
    let first = require_reads(&fixture, "one event").await?;
    ensure!(
        first == expected(&[(0, EMITTER, "ens", "CounterA", 1)]),
        "{first:?}"
    );
    for block in 2..=3 {
        fixture.apply(block, FamilyMode::Normal).await?;
        require_reads(&fixture, "block by block").await?;
    }
    let before = fixture.exact().await?;
    let third = stored(&fixture).await?;
    fixture.apply(4, FamilyMode::Normal).await?;
    let fourth = require_reads(&fixture, "the moved pointer").await?;
    let moved = expected(&[
        (0, EMITTER, "ens", "CounterA", 2),
        (0, EMITTER, "ens", "CounterB", 1),
        (0, EMITTER, "basenames", "CounterA", 1),
        (0, RESOLVER_1, "ens", "CounterA", 1),
        (0, REGISTRY, "ens", "ResolverChanged", 2),
        (1, RESOLVER_1, "ens", "ResolverChanged", 1),
        (1, RESOLVER_2, "ens", "ResolverChanged", 1),
        (2, RESOLVER_1, "ens", "ResolverChanged", 1),
    ]);
    ensure!(fourth == moved, "{fourth:?}");
    // The undo restores every count to its image before block 4 and drops the keys block 4
    // created; the replay counts block 4 once.
    ensure!(families::undo_to(&fixture.pool, CHAIN, 3).await? == 1);
    ensure!(fixture.exact().await? == before, "the undo left other rows");
    ensure!(
        stored(&fixture).await? == third,
        "the undo left {:?}",
        stored(&fixture).await?
    );
    fixture.apply(4, FamilyMode::Normal).await?;
    ensure!(require_reads(&fixture, "the replay").await? == moved);
    fixture.apply(5, FamilyMode::Normal).await?;
    ensure!(require_reads(&fixture, "uncounted rows").await? == moved);
    // Block 5 is undone and replaced by other events.
    ensure!(families::undo_to(&fixture.pool, CHAIN, 4).await? == 1);
    update(
        &fixture,
        "DELETE FROM normalized_events WHERE block_number = 5 OR block_number IS NULL",
    )
    .await?;
    emitted(&fixture, "b-5", 5, "CounterB").await?;
    fixture.apply(5, FamilyMode::Normal).await?;
    let replaced = require_reads(&fixture, "the replaced block").await?;
    let mut expect = moved.clone();
    *expect
        .get_mut(&(
            0,
            EMITTER.to_owned(),
            "ens".to_owned(),
            "CounterB".to_owned(),
        ))
        .context("a CounterB count")? = 2;
    ensure!(replaced == expect, "{replaced:?}");
    fixture.assert_rebuild_equal(5).await?;
    ensure!(require_reads(&fixture, "the rebuilds").await? == expect);
    fixture.cleanup().await
}

/// A rebuild whose ranges stop below the target and whose composition pass counts one or two
/// blocks a window, over runs of one step each, stores the counts block-by-block publication
/// stores. The pass counts each window's block range on its own: several windows commit, each
/// ending below the target.
#[tokio::test]
async fn a_pass_over_many_windows_and_runs_counts_every_block_once() -> Result<()> {
    let fixture = Fixture::new("families_event_counts_pass", 12).await?;
    for block in 1..=12 {
        emitted(&fixture, &format!("a-{block}"), block, "CounterA").await?;
        if block % 3 == 0 {
            let (before, after) = if block % 2 == 0 {
                (Some(RESOLVER_1), RESOLVER_2)
            } else {
                (Some(RESOLVER_2), RESOLVER_1)
            };
            pointer(&fixture, block, 1, 1, before, after).await?;
        }
    }
    fixture
        .apply_with(
            12,
            FamilyMode::Rebuild,
            &FamilyOptions::new(CONTENT_HASH)
                .with_max_blocks_per_run(u64::MAX)
                .with_rebuild_ranges(RebuildRanges::Off),
        )
        .await?;
    let block_by_block = require_reads(&fixture, "block by block").await?;
    ensure!(
        block_by_block.get(&(
            0,
            EMITTER.to_owned(),
            "ens".to_owned(),
            "CounterA".to_owned()
        )) == Some(&12),
        "{block_by_block:?}"
    );
    let mut budgeted = FamilyOptions::new(CONTENT_HASH)
        .with_rebuild_ranges(RebuildRanges::Through(11))
        .with_compose_chunk_keys(1)
        .with_max_blocks_per_run(1);
    budgeted.max_range_events = 2;
    let mut runs = 0;
    let mut mode = FamilyMode::Rebuild;
    // The pass counts window by window, never the chain in one statement: each committed window
    // leaves its end as the stream's cursor.
    let mut window_ends = std::collections::BTreeSet::new();
    loop {
        fixture.apply_with(12, mode, &budgeted).await?;
        mode = FamilyMode::Normal;
        runs += 1;
        if let Some(record) = fixture.repair_record().await?
            && record["compose_stream"] == 3
            && let Some(end) = record["compose_cursor"].as_str()
        {
            window_ends.insert(end.parse::<i64>()?);
        }
        if fixture.marker().await?.0 == Some(12) {
            break;
        }
        ensure!(runs < 500, "the rebuild does not converge");
    }
    ensure!(runs > 3, "the pass ran in {runs} runs");
    ensure!(
        window_ends.len() >= 3 && window_ends.iter().all(|end| *end <= 11),
        "the pass counted in windows ending at {window_ends:?}"
    );
    ensure!(
        require_reads(&fixture, "the resumed pass").await? == block_by_block,
        "the pass stored other counts"
    );
    fixture.cleanup().await
}
