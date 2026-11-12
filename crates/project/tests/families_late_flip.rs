//! A probe for the stored event counts (F20, `docs/projections.md`, "Event counts") against a
//! late visibility flip: a `normalized_events` row at block B that is `candidate` when B is
//! published and `activated` by the time a later block B+k is published, under live following.
//!
//! No production writer performs such a flip. Interpret decides a row's visibility inside its
//! batch (`crates/adapters/src/schema_v2/migration/activation.rs`) and writes it once; its upsert
//! refuses a rewrite that differs in `consumer_visibility`
//! (`crates/interpret/src/write/normalized.rs`, pinned by
//! `a_visibility_only_rewrite_is_refused_and_leaves_the_stored_row`). The only path that changes a
//! stored row is an Interpret redo, which deletes and re-derives the range and stamps a Project
//! redo over it, so Project undoes and replays those blocks itself.
//!
//! This test flips the row by SQL, standing in for the writer that does not exist, and pins both
//! halves of the mechanism by run: the per-block path, which counts a row in its own block's range
//! only, misses the flipped row for as long as block B stays published (the drift such a writer
//! would cause); and the undo and replay of B, which is what an Interpret redo forces, brings the
//! stored count back to the read. A rebuild agrees with the replayed state. If a writer that flips
//! visibility in place is ever added, the drift half of this test is the one to revisit.
#[path = "families_support/mod.rs"]
mod support;

use anyhow::{Context, Result, ensure};
use bigname_project::families::{self, FamilyMode};
use bigname_storage::{
    ChainBlockRange, EventHistoryFilter, HistoryBlockWindow, HistorySummaryMode,
    load_event_history_page,
};
use serde_json::json;
use support::{CHAIN, Event, Fixture};

const EMITTER: &str = "0x00000000000000000000000000000000000000e1";
const KIND: &str = "AliasChanged";
const FAMILY: &str = "test_late_flip_l1";
/// The cap of the event feed's `include=total_count`.
const CAP: u64 = 100_000;

/// The stored count of the emitter's `ens` `AliasChanged` key, zero when the key has no row.
async fn stored(fixture: &Fixture) -> Result<i64> {
    let count: Option<i64> = sqlx::query_scalar(
        "SELECT event_count FROM project_event_count
         WHERE chain_id = $1 AND anchor_kind = 0 AND anchor = $2
           AND namespace = 'ens' AND event_kind = $3",
    )
    .bind(CHAIN)
    .bind(EMITTER)
    .bind(KIND)
    .fetch_optional(&fixture.pool)
    .await?;
    Ok(count.unwrap_or(0))
}

/// The capped walk the stored count is meant to equal: the `contract_address` read's
/// `total_count` for the same key, bounded by the family marker's block.
async fn walked(fixture: &Fixture) -> Result<i64> {
    let block = fixture.marker().await?.0.context("a published block")?;
    let filter = EventHistoryFilter {
        contract_address: Some(EMITTER.to_owned()),
        namespace: Some("ens".to_owned()),
        event_kinds: vec![KIND.to_owned()],
        block_window: Some(HistoryBlockWindow {
            ranges: vec![ChainBlockRange {
                chain_id: CHAIN.to_owned(),
                from_block: None,
                to_block: Some(block),
            }],
        }),
        ..EventHistoryFilter::default()
    };
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

async fn emitted(fixture: &Fixture, identity: &str, block: i64, log: i64) -> Result<()> {
    fixture
        .event(
            Event::new(identity, block, log, KIND, FAMILY)
                .raw(json!({"emitting_address": EMITTER})),
        )
        .await?;
    Ok(())
}

async fn visibility(fixture: &Fixture, identity: &str, visibility: &str) -> Result<()> {
    sqlx::query(
        "UPDATE normalized_events
         SET consumer_visibility = $2, migration_correlation_ids = ARRAY['late-flip-probe']
         WHERE event_identity = $1",
    )
    .bind(identity)
    .bind(visibility)
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

/// Blocks 1 to 3 published one by one, with a candidate row at block 2 beside an activated one.
/// The candidate is then flipped to activated by SQL and block 4 is published. The per-block path
/// holds the count at four (the flipped row's block was already counted), the read says five: the
/// drift a visibility-flipping writer would cause. An undo to block 1 and a block-by-block replay,
/// the shape an Interpret redo forces on Project, count the row in block 2's range and the stored
/// count equals the read again; an undo of the replayed blocks takes it back out through the
/// journal; a rebuild agrees with the replay.
#[tokio::test]
async fn a_visibility_flip_after_publication_is_healed_by_replay_not_by_the_block_path()
-> Result<()> {
    let fixture = Fixture::new("families_late_flip", 4).await?;
    emitted(&fixture, "a-1", 1, 0).await?;
    emitted(&fixture, "a-2", 2, 0).await?;
    emitted(&fixture, "late-2", 2, 1).await?;
    visibility(&fixture, "late-2", "candidate").await?;
    emitted(&fixture, "a-3", 3, 0).await?;
    emitted(&fixture, "a-4", 4, 0).await?;

    // Live following: the candidate row is in no count and in no read.
    for block in 1..=3 {
        fixture.apply(block, FamilyMode::Normal).await?;
        let (stored, walked) = (stored(&fixture).await?, walked(&fixture).await?);
        ensure!(
            stored == walked && stored == block,
            "block {block}: stored {stored}, read {walked}"
        );
    }

    // The flip no production writer performs: block 2's row becomes activated while block 2 is
    // published, observed only when block 4 applies.
    visibility(&fixture, "late-2", "activated").await?;
    fixture.apply(4, FamilyMode::Normal).await?;
    let (stored_after_flip, walked_after_flip) = (stored(&fixture).await?, walked(&fixture).await?);
    ensure!(
        walked_after_flip == 5,
        "the read at block 4 counts the flipped row: {walked_after_flip}"
    );
    ensure!(
        stored_after_flip == 4,
        "the per-block path counts block 4's range only and keeps block 2's image: \
         stored {stored_after_flip}, read {walked_after_flip}"
    );

    // What an Interpret redo of block 2 forces: Project undoes to below the range and replays it.
    // The undo journal restores block 1's image; the replay counts the activated row in block 2's
    // range.
    ensure!(families::undo_to(&fixture.pool, CHAIN, 1).await? == 3);
    ensure!(
        stored(&fixture).await? == 1,
        "the undo left {}",
        stored(&fixture).await?
    );
    for block in 2..=4 {
        fixture.apply(block, FamilyMode::Normal).await?;
        let (stored, walked) = (stored(&fixture).await?, walked(&fixture).await?);
        ensure!(
            stored == walked && stored == block + 1,
            "replay of block {block}: stored {stored}, read {walked}"
        );
    }

    // A reorg that undoes the replayed blocks takes the row back out through the journal, and
    // the replay counts it once more.
    ensure!(families::undo_to(&fixture.pool, CHAIN, 1).await? == 3);
    ensure!(stored(&fixture).await? == 1);
    for block in 2..=4 {
        fixture.apply(block, FamilyMode::Normal).await?;
    }
    ensure!(stored(&fixture).await? == 5 && walked(&fixture).await? == 5);

    // The composition pass and a block-by-block rebuild store what the replay stored.
    fixture.assert_rebuild_equal(4).await?;
    ensure!(stored(&fixture).await? == 5 && walked(&fixture).await? == 5);
    fixture.cleanup().await
}
