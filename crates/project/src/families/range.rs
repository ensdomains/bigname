//! A rebuild range: several work blocks of a rebuild in one transaction (docs/projections.md,
//! "Owned key families"). The range passes the fences of a single block once, for its first
//! block (the marker generation, the input revision read inside the transaction, the repair
//! record rebuilding under its attempt); reads the lineage rows, events, surface bindings and
//! resolver activations of all its blocks in one statement each, grouped back by block; loads
//! every row the blocks' events name, one statement per table; and then folds the blocks one by
//! one through the same reducers a single block runs, each with its own header, manifest set,
//! manifest change against the block before it, and inputs, so each block sees the families as
//! the blocks before it left them. It then journals the pre-range image of every row it changed
//! and the prior marker under its last block, writes, advances the marker to its last block
//! still in bootstrap, prunes and commits: one generation for the whole range.
mod reads;

use std::time::Instant;

use super::{
    FamilyOptions,
    block::{self, BlockStats, Plan},
    input::{BlockEvent, BlockHeader},
    keys::{self, BlockKeys},
    manifests::ActiveSet,
    marker::FamilyMarker,
    reduce::{self, Context, Prefetched, Preload},
    store::RowSet,
};
use crate::Result;

/// One block of a range with the inputs the range read for it.
struct RangeBlock {
    header: BlockHeader,
    events: Vec<BlockEvent>,
    duplicate_anomalies: u64,
    keys: BlockKeys,
    manifests: ActiveSet,
    prefetched: Prefetched,
}

/// Apply the work blocks `numbers` (ascending, none the target) in one transaction, or a prefix
/// of them: the range ends before the block whose events, counted before duplicates are
/// dropped, would take its total past `options.max_range_events`, and always holds its first
/// block. Returns the marker the range
/// published, the number of blocks it applied and what it wrote.
pub(crate) async fn apply(
    pool: &sqlx::PgPool,
    chain_id: &str,
    numbers: &[i64],
    plan: &Plan<'_>,
    options: &FamilyOptions,
) -> Result<(FamilyMarker, usize, BlockStats)> {
    let started = Instant::now();
    let (&first, rest) = numbers
        .split_first()
        .expect("a range holds at least one block");
    let mut opened = block::open(pool, chain_id, first, plan).await?;
    let mut headers = vec![opened.block.clone()];
    headers.extend(reads::headers(&mut opened.transaction, chain_id, rest).await?);
    // The blocks whose events fit the cap, and the first block whatever it holds; counted
    // before the events are read so a range reads only the blocks it applies.
    let counts = reads::event_counts(&mut opened.transaction, chain_id, &headers).await?;
    let (mut total, mut applied) = (0_u64, 0);
    for header in &headers {
        let count = counts.get(&header.number).copied().unwrap_or(0);
        if applied > 0 && total.saturating_add(count) > options.max_range_events {
            break;
        }
        total = total.saturating_add(count);
        applied += 1;
    }
    headers.truncate(applied);
    let events = reads::events(&mut opened.transaction, chain_id, &headers).await?;
    let mut child_registrations = super::child_registrations::read(
        &mut opened.transaction,
        events.iter().flat_map(|(events, _)| events.iter()),
    )
    .await?;
    let mut bindings = reads::bindings(&mut opened.transaction, chain_id, &headers).await?;
    let mut activated =
        reads::activated(&mut opened.transaction, chain_id, &headers, plan.manifests).await?;

    let blocks: Vec<RangeBlock> = headers
        .into_iter()
        .zip(events)
        .map(|(header, (events, duplicate_anomalies))| RangeBlock {
            keys: keys::derive(&events),
            manifests: plan.manifests.at(header.number),
            prefetched: Prefetched {
                child_registrations: child_registrations
                    .remove(&header.number)
                    .unwrap_or_default(),
                bindings: bindings.remove(&header.number).unwrap_or_default(),
                activated: activated.remove(&header.number).unwrap_or_default(),
            },
            header,
            events,
            duplicate_anomalies,
        })
        .collect();
    let mut preload = Preload::default();
    for block in &blocks {
        reduce::preload(
            chain_id,
            &block.events,
            &block.keys,
            &block.prefetched,
            &mut preload,
        );
    }
    let mut rows = RowSet::default();
    preload.load(&mut opened.transaction, &mut rows).await?;

    let mut previous = opened.prior.admission_manifests.clone();
    let mut duplicate_anomalies = 0;
    for block in &blocks {
        let number = block.header.number;
        if block.duplicate_anomalies > 0 {
            tracing::warn!(
                target: "bigname_project::families",
                chain_id,
                block_number = number,
                duplicate_anomalies = block.duplicate_anomalies,
                "deliveries of one event identity disagreed; the first in the canonical order was kept"
            );
        }
        duplicate_anomalies += block.duplicate_anomalies;
        let context = Context {
            chain_id,
            block: &block.header,
            keys: &block.keys,
            manifests: &block.manifests,
            manifests_changed: previous.as_deref() != Some(block.manifests.key.as_str()),
            prefetched: Some(&block.prefetched),
        };
        reduce::apply(&mut opened.transaction, &context, &block.events, &mut rows)
            .await
            .map_err(reduce::in_family(&format!("block {number}")))?;
        rows.end_block();
        previous = Some(block.manifests.key.clone());
    }

    let last = blocks.last().expect("a range applies its first block");
    opened.block = last.header.clone();
    opened.manifests = last.manifests.clone();
    let (next, mut stats) = block::publish(opened, chain_id, &rows, plan, options).await?;
    stats.duplicate_anomalies = duplicate_anomalies;
    stats.elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    Ok((next, blocks.len(), stats))
}
