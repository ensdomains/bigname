//! Owned current-state families, applied and published atomically for each canonical block.
//! Rebuild and replay use the same reducers and undo journal as live follow. A failed block
//! leaves the marker and every family at the preceding complete publication.
mod addresses;
mod block;
mod child_registrations;
mod classification;
pub use child_registrations::EXCLUDED_CHILD_REGISTRATION_PARENTS;
mod decode;
mod derived;
mod driver;
mod hydrate;
mod identity;
mod input;
mod keys;
mod lifecycle;
mod manifests;
mod marker;
mod permissions;
mod position;
mod range;
mod records;
mod reduce;
mod registry;
mod repair;
mod resolver;
mod reverse;
mod store;
mod tables;
mod topology;
mod undo;
mod wrapper;

pub use input::{InputToken, Revision, input_token};
pub use position::emission_ordinal;

use std::{collections::BTreeMap, time::Instant};

use sqlx::PgPool;

use crate::Marker;

/// Undo rows are kept at least this many blocks below the family marker, and further back to the
/// chain's finalized and safe blocks and an active repair's floor (docs/projections.md, "Owned
/// key families"); the per-block publication tunes it.
pub const RETAINED_UNDO_DEPTH: i64 = 256;

/// The most blocks one run applies or undoes before it stops and reports it spent its budget.
/// Live follow applies a block or two per run; the Project phase runs a rebuild or a long
/// catch-up as a series of runs until the families reach the served marker.
pub const MAX_BLOCKS_PER_RUN: u64 = 256;

/// A rebuild applies the work blocks at or below this many blocks under the chain's safe block
/// in [ranges](RebuildRanges); the blocks above it and the target go one to a transaction, so a
/// reorg near the head undoes single blocks. The switch point follows the safe block, not the
/// finalized one: a safe block is not final, and a reorg whose
/// fork point lies inside a range undoes that whole range and replays from its predecessor.
pub const RANGE_SAFE_MARGIN: i64 = 5;

/// With no safe block published, a rebuild applies the work blocks at or below this many blocks
/// under its target in ranges.
pub const RANGE_TARGET_MARGIN: i64 = RETAINED_UNDO_DEPTH;

/// The most work blocks one rebuild range applies. A range saves the fixed statements of every
/// block after its first (the fences, the marker journal and advance, the retention read and
/// prune, the commit), about fifteen round trips, so past a few hundred blocks the saving no
/// longer shows beside the blocks' own reads. A range also never holds more blocks than its run
/// has left of its budget, so under the default budget, [`MAX_BLOCKS_PER_RUN`], that budget is
/// the effective cap: a resumed run applies up to 256 work blocks as one range when their events
/// fit [`MAX_RANGE_EVENTS`].
/// This cap binds only when a run is given a larger budget, where it bounds how much one failure
/// has to redo.
pub const MAX_RANGE_BLOCKS: u64 = 1024;

/// The most events one rebuild range applies, unless its first block alone holds more. The
/// range's working set keeps every row it loaded, with its pre-range and pre-block images, until
/// it commits, and a resource-bearing event loads at least its resource pointer row. 4,096 is
/// about twice Sepolia's densest block (2,147 events), which keeps the working set to tens of
/// megabytes while leaving dense stretches a few blocks per range.
pub const MAX_RANGE_EVENTS: u64 = 4096;

/// Which work blocks a rebuild applies several to a transaction. A range folds its blocks one
/// by one exactly as single blocks would, then journals, writes and advances the marker once, to
/// its last block; undo takes the whole range back in one step.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RebuildRanges {
    /// Every block in a transaction of its own.
    Off,
    /// The work blocks at or below the chain's safe block minus [`RANGE_SAFE_MARGIN`], read once
    /// per run, or with no safe block the target minus [`RANGE_TARGET_MARGIN`].
    BelowSafe,
    /// The work blocks at or below this block, whatever the chain's heads; for tests and
    /// benchmarks.
    Through(i64),
}

/// Every owned key family table, journalled and derived, for tests that compare the families
/// of two runs.
pub fn family_tables() -> impl Iterator<Item = &'static str> {
    tables::JOURNALLED
        .iter()
        .map(|table| table.name)
        .chain(tables::DERIVED)
}

/// Undo the families one journal generation at a time until their marker is at or below
/// `number`, each generation (one block, or one rebuild range) in its own transaction, under a
/// repair record opened for it (reason operator_redo, the Project row's attempt); the last undo
/// moves the record to replaying, and the next run replays. An undo into a range stops on the
/// range's predecessor. Returns the generations undone; stops early, leaving the families where
/// they stand, when the journal no longer holds the next generation.
pub async fn undo_to(pool: &PgPool, chain_id: &str, number: i64) -> crate::Result<u64> {
    let family = marker::read(pool, chain_id).await?;
    let Some(current) = family.current.clone() else {
        return Ok(0);
    };
    if current.number <= number {
        return Ok(0);
    }
    let Some(base) = undo::undo_target(pool, chain_id, &current, number).await? else {
        return Ok(0);
    };
    let planned = repair::read(pool, chain_id).await?;
    let token = input_token(pool, chain_id).await?;
    let mut transaction = pool
        .begin()
        .await
        .map_err(|error| crate::ProjectError::database("failed to begin an undo", error))?;
    let locked = marker::lock(&mut transaction, chain_id).await?;
    marker::require(chain_id, &locked, Some(&current), family.sequence)?;
    let record = repair::lock(&mut transaction, chain_id).await?;
    repair::require_unchanged(chain_id, record.as_ref(), planned.as_ref())?;
    repair::begin_undo(
        &mut transaction,
        chain_id,
        &repair::NewRepair {
            attempt: token.project_redo_attempt_generation,
            reason: repair::Reason::OperatorRedo,
            trusted_base: Some(&base),
            replay_target: &current,
            pending_undo_target: base.number,
        },
    )
    .await?;
    transaction
        .commit()
        .await
        .map_err(|error| crate::ProjectError::database("failed to commit an undo", error))?;
    let mut family = marker::read(pool, chain_id).await?;
    let mut undone = 0;
    while family
        .current
        .as_ref()
        .is_some_and(|marker| marker.number > base.number)
    {
        match undo::undo_block(
            pool,
            chain_id,
            &family,
            token.project_redo_attempt_generation,
        )
        .await?
        {
            Some(restored) => {
                family = restored;
                undone += 1;
            }
            None => break,
        }
    }
    Ok(undone)
}

/// How the family loop reaches its requested Project target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FamilyMode {
    /// Advance from the family marker toward the requested target.
    Normal,
    /// Reset and rebuild the families from retained inputs.
    Rebuild,
    /// Undo to `from - 1` and replay the required range `from..=to`.
    Redo { from: i64, to: i64 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FamilyOptions {
    /// RPC configuration for mainnet follow-block hydration. Replay and rebuild never call it.
    pub hydration_rpc_urls: Option<bigname_lookup::ChainRpcUrls>,
    /// The interpreter content hash the marker records for every block.
    pub input_content_hash: String,
    /// Blocks of undo rows kept below the marker at least.
    pub retained_undo_depth: i64,
    /// Blocks one run applies or undoes at most.
    pub max_blocks_per_run: u64,
    /// Which work blocks a rebuild applies in ranges.
    pub rebuild_ranges: RebuildRanges,
    /// Work blocks one rebuild range applies at most, and never more than the run's budget has
    /// left; zero counts as one.
    pub max_range_blocks: u64,
    /// Events one rebuild range applies at most, unless its first block alone holds more.
    pub max_range_events: u64,
    /// The redo this run serves resumes an interrupted redo of the same range under the same
    /// interpreter content hash (the runner kept its saved progress). A rebuild the interrupted
    /// attempt left then resumes rather than starting again.
    pub resumes_interrupted_redo: bool,
}

impl FamilyOptions {
    pub fn new(input_content_hash: impl Into<String>) -> Self {
        Self {
            hydration_rpc_urls: None,
            input_content_hash: input_content_hash.into(),
            retained_undo_depth: RETAINED_UNDO_DEPTH,
            max_blocks_per_run: MAX_BLOCKS_PER_RUN,
            rebuild_ranges: RebuildRanges::BelowSafe,
            max_range_blocks: MAX_RANGE_BLOCKS,
            max_range_events: MAX_RANGE_EVENTS,
            resumes_interrupted_redo: false,
        }
    }

    pub fn with_resumed_redo(mut self, resumes: bool) -> Self {
        self.resumes_interrupted_redo = resumes;
        self
    }

    pub fn with_rebuild_ranges(mut self, ranges: RebuildRanges) -> Self {
        self.rebuild_ranges = ranges;
        self
    }

    pub fn with_hydration(mut self, rpc_urls: bigname_lookup::ChainRpcUrls) -> Self {
        self.hydration_rpc_urls = Some(rpc_urls);
        self
    }

    /// Cap a rebuild range at `blocks` work blocks and `events` events, each at least one.
    pub fn with_range_caps(mut self, blocks: u64, events: u64) -> Self {
        self.max_range_blocks = blocks.max(1);
        self.max_range_events = events.max(1);
        self
    }

    pub fn with_max_blocks_per_run(mut self, blocks: u64) -> Self {
        self.max_blocks_per_run = blocks.max(1);
        self
    }

    pub fn with_retained_undo_depth(mut self, depth: i64) -> Self {
        self.retained_undo_depth = depth.max(1);
        self
    }
}

/// What one run of the loop did.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FamilyOutcome {
    /// The served marker the loop followed.
    pub target: Option<Marker>,
    /// The family marker when the loop stopped.
    pub marker: Option<Marker>,
    /// Whether the family marker's hash is readable on the lineage at its height.
    pub marker_readable: bool,
    /// Blocks applied, a range's blocks included.
    pub blocks: u64,
    /// Rebuild ranges committed; their blocks count in `blocks`.
    pub ranges: u64,
    /// Journal generations undone, each one block or one rebuild range.
    pub undone_blocks: u64,
    /// Whether the families were cleared and rebuilt.
    pub reset: bool,
    /// Rows written or removed per family table.
    pub rows: BTreeMap<&'static str, u64>,
    /// Undo rows written, marker rows included.
    pub undo_rows: u64,
    /// Elapsed milliseconds of each block applied in a transaction of its own; a rebuild range
    /// adds none.
    pub block_ms: Vec<u64>,
    /// Whether the run stopped because it spent its block budget; the next run continues.
    pub budget_exhausted: bool,
    /// Whether the run adopted an input revision other than the one the last block recorded.
    pub revision_adopted: bool,
    /// Deliveries of one event identity that disagreed with the kept one and were dropped.
    pub duplicate_anomalies: u64,
    /// Times a rebuild refreshed the planner statistics of the family tables.
    pub statistics_refreshes: u64,
    /// Elapsed milliseconds of the whole run.
    pub elapsed_ms: u64,
}

impl FamilyOutcome {
    /// Blocks between the families and the served marker, zero only when the family marker is the
    /// served block itself. A readable marker below or above the served height counts the blocks
    /// in between. A marker off the served branch (orphaned, or another hash at the served
    /// height) counts from the block below the lower of the two heights, a lower bound because the
    /// branch point can sit deeper.
    pub fn lag_blocks(&self) -> u64 {
        let Some(target) = self.target.as_ref() else {
            return 0;
        };
        let Some(marker) = self.marker.as_ref() else {
            return u64::try_from(target.number + 1).unwrap_or(0);
        };
        let on_branch =
            self.marker_readable && (marker.number != target.number || marker.hash == target.hash);
        if on_branch {
            return (target.number - marker.number).unsigned_abs();
        }
        u64::try_from(target.number - marker.number.min(target.number) + 1).unwrap_or(0)
    }

    fn record(&mut self, stats: block::BlockStats) {
        self.blocks += 1;
        self.block_ms.push(stats.elapsed_ms);
        self.add(stats);
    }

    /// A committed rebuild range of `blocks` blocks; its time is not a block's.
    fn record_range(&mut self, stats: block::BlockStats, blocks: u64) {
        self.blocks += blocks;
        self.ranges += 1;
        self.add(stats);
    }

    fn add(&mut self, stats: block::BlockStats) {
        self.undo_rows += stats.undo_rows;
        self.duplicate_anomalies += stats.duplicate_anomalies;
        for (table, rows) in stats.rows {
            *self.rows.entry(table).or_default() += rows;
        }
    }
}

/// Bring the families to the served `target`, at most `options.max_blocks_per_run` blocks this
/// run; `budget_exhausted` in the outcome says the run stopped on its budget and another run
/// continues. `session` is the input token the Project phase read before recording the batch,
/// while a finished redo's session was still open; it names that redo's reason. Every block reads
/// the token again inside its own transaction. An error, a refused fence included, stops the run
/// at the last complete block and is returned; its kind says whether a retry can succeed. See
/// [`run`] for the outcome of a run that failed.
pub async fn apply(
    pool: &PgPool,
    chain_id: &str,
    target: &Marker,
    mode: FamilyMode,
    session: &InputToken,
    options: &FamilyOptions,
) -> crate::Result<FamilyOutcome> {
    match run(pool, chain_id, target, mode, session, options).await {
        (outcome, None) => Ok(outcome),
        (_, Some(error)) => Err(error),
    }
}

/// [`apply`], handing back the outcome whether or not the run failed: a run that fails keeps
/// what the blocks before the failure committed (their times, rows and anomalies), the marker
/// it left and its own elapsed time, so a caller can report them beside the error.
pub async fn run(
    pool: &PgPool,
    chain_id: &str,
    target: &Marker,
    mode: FamilyMode,
    session: &InputToken,
    options: &FamilyOptions,
) -> (FamilyOutcome, Option<crate::ProjectError>) {
    let started = Instant::now();
    let mut outcome = FamilyOutcome {
        target: Some(target.clone()),
        ..FamilyOutcome::default()
    };
    let result = driver::run(
        pool,
        chain_id,
        target,
        &mode,
        session,
        options,
        &mut outcome,
    )
    .await;
    (outcome.marker, outcome.marker_readable) = current_marker(pool, chain_id).await;
    outcome.elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    let mut sorted = outcome.block_ms.clone();
    sorted.sort_unstable();
    let error = result.err();
    tracing::info!(
        target: "bigname_project::families",
        chain_id,
        target_block = target.number,
        marker_block = outcome.marker.as_ref().map(|marker| marker.number),
        blocks = outcome.blocks,
        ranges = outcome.ranges,
        undone_blocks = outcome.undone_blocks,
        reset = outcome.reset,
        rows = ?outcome.rows,
        undo_rows = outcome.undo_rows,
        block_ms_median = sorted.get(sorted.len() / 2).copied(),
        block_ms_max = sorted.last().copied(),
        elapsed_ms = outcome.elapsed_ms,
        budget_exhausted = outcome.budget_exhausted,
        revision_adopted = outcome.revision_adopted,
        duplicate_anomalies = outcome.duplicate_anomalies,
        error = error.as_ref().map(tracing::field::display),
        "Project families applied"
    );
    (outcome, error)
}

/// Where the families stand against the served `target`, with nothing applied: the outcome a
/// caller reports when no run could start, so the lag still shows.
pub async fn standing(pool: &PgPool, chain_id: &str, target: &Marker) -> FamilyOutcome {
    let (marker, marker_readable) = current_marker(pool, chain_id).await;
    FamilyOutcome {
        target: Some(target.clone()),
        marker,
        marker_readable,
        ..FamilyOutcome::default()
    }
}

/// The family marker and whether its hash is readable at its height; a read that fails reports
/// no marker, as the lag then counts the whole served chain.
async fn current_marker(pool: &PgPool, chain_id: &str) -> (Option<Marker>, bool) {
    let Some(current) = marker::read(pool, chain_id)
        .await
        .ok()
        .and_then(|marker| marker.current)
    else {
        return (None, false);
    };
    let readable = input::readable_hash(pool, chain_id, current.number)
        .await
        .ok()
        .flatten()
        .is_some_and(|hash| hash == current.hash);
    (Some(current), readable)
}

#[cfg(test)]
#[path = "guard_tests.rs"]
mod guard_tests;
#[cfg(test)]
#[path = "undo_tests.rs"]
mod undo_tests;
#[cfg(test)]
#[path = "work_tests.rs"]
mod work_tests;
