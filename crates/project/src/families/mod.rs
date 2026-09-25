//! Owned key families (docs/projections.md, "Owned key families"): per-key current-state tables
//! that step 2 of TYR-36 fills block by block beside the served tables. Nothing reads them yet.
//!
//! The loop runs after a Project batch has committed and its progress is recorded, never inside
//! the served transaction: each family block is its own transaction, so a family failure or a
//! slow block can never delay or roll back a served publication. The families follow the served
//! marker from their own shadow marker, catching up from wherever it stands.
// The reducers land in the commits that follow and use the helpers
// that are unused until then.
mod addresses;
mod block;
mod classification;
mod decode;
mod derived;
mod driver;
mod identity;
mod input;
mod keys;
mod lifecycle;
mod marker;
mod permissions;
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

use std::{collections::BTreeMap, time::Instant};

use sqlx::PgPool;

use crate::Marker;

/// Undo rows are kept at least this many blocks below the family marker, and further back to the
/// chain's finalized and safe blocks and an active repair's floor (docs/projections.md, "Owned
/// key families"); the per-block publication tunes it.
pub const RETAINED_UNDO_DEPTH: i64 = 256;

/// The most blocks one run applies or undoes before it stops and leaves the rest to the next
/// run. Live follow applies a block or two per run; a rebuild or a long catch-up spans many runs.
pub const MAX_BLOCKS_PER_RUN: u64 = 256;

/// Every owned key family table, journalled and derived, for tests that compare the families
/// of two runs.
pub fn family_tables() -> impl Iterator<Item = &'static str> {
    tables::JOURNALLED
        .iter()
        .map(|table| table.name)
        .chain(tables::DERIVED)
}

/// Undo the families block by block until their marker is at or below `number`, each block in
/// its own transaction, under a repair record opened for it (reason operator_redo, the Project
/// row's attempt); the last undo moves the record to replaying, and the next run replays.
/// Returns the blocks undone; stops early, leaving the families where they stand, when the
/// journal no longer holds the next block.
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

/// How the loop runs for one served batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FamilyMode {
    /// Follow the served marker from the family marker.
    Normal,
    /// The served tables were rebuilt from scratch: rebuild the families too.
    Rebuild,
    /// The served batch redid blocks `from..=to`: undo the families to `from - 1` and replay.
    Redo { from: i64, to: i64 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FamilyOptions {
    /// The interpreter content hash the marker records for every block.
    pub input_content_hash: String,
    /// Blocks of undo rows kept below the marker at least.
    pub retained_undo_depth: i64,
    /// Blocks one run applies or undoes at most.
    pub max_blocks_per_run: u64,
}

impl FamilyOptions {
    pub fn new(input_content_hash: impl Into<String>) -> Self {
        Self {
            input_content_hash: input_content_hash.into(),
            retained_undo_depth: RETAINED_UNDO_DEPTH,
            max_blocks_per_run: MAX_BLOCKS_PER_RUN,
        }
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
    /// Blocks applied.
    pub blocks: u64,
    /// Blocks undone.
    pub undone_blocks: u64,
    /// Whether the families were cleared and rebuilt.
    pub reset: bool,
    /// Rows written or removed per family table.
    pub rows: BTreeMap<&'static str, u64>,
    /// Undo rows written, marker rows included.
    pub undo_rows: u64,
    /// Elapsed milliseconds of each applied block.
    pub block_ms: Vec<u64>,
    /// Why the loop stopped early; the families then lag and the next run catches up.
    pub skipped: Option<String>,
    /// Whether the run stopped because it spent its block budget; the next run continues.
    pub budget_exhausted: bool,
    /// Whether the run adopted an input revision other than the one the last block recorded.
    pub revision_adopted: bool,
    /// Deliveries of one event identity that disagreed with the kept one and were dropped.
    pub duplicate_anomalies: u64,
    /// Elapsed milliseconds of the whole run.
    pub elapsed_ms: u64,
}

impl FamilyOutcome {
    /// Served marker minus family marker, zero when the families are current.
    pub fn lag_blocks(&self) -> u64 {
        let target = self.target.as_ref().map_or(0, |marker| marker.number);
        let current = self.marker.as_ref().map_or(-1, |marker| marker.number);
        u64::try_from(target - current).unwrap_or(0)
    }

    fn record(&mut self, stats: block::BlockStats) {
        self.blocks += 1;
        self.undo_rows += stats.undo_rows;
        self.duplicate_anomalies += stats.duplicate_anomalies;
        self.block_ms.push(stats.elapsed_ms);
        for (table, rows) in stats.rows {
            *self.rows.entry(table).or_default() += rows;
        }
    }
}

/// Bring the families to the served `target`, at most `options.max_blocks_per_run` blocks this
/// run. `session` is the input token the Project phase read before recording the batch, while a
/// finished redo's session was still open; it names that redo's reason. Every block reads the
/// token again inside its own transaction. Never fails: an error stops the loop, is logged and
/// returned in `skipped`, and leaves the families at the last complete block.
pub async fn apply(
    pool: &PgPool,
    chain_id: &str,
    target: &Marker,
    mode: FamilyMode,
    session: &InputToken,
    options: &FamilyOptions,
) -> FamilyOutcome {
    let started = Instant::now();
    let mut outcome = FamilyOutcome {
        target: Some(target.clone()),
        ..FamilyOutcome::default()
    };
    if let Err(error) = driver::run(
        pool,
        chain_id,
        target,
        &mode,
        session,
        options,
        &mut outcome,
    )
    .await
    {
        tracing::warn!(
            target: "bigname_project::families",
            chain_id,
            target_block = target.number,
            %error,
            "Project families stopped; they catch up on the next batch"
        );
        outcome.skipped = Some(error.to_string());
    }
    outcome.marker = marker::read(pool, chain_id)
        .await
        .ok()
        .and_then(|marker| marker.current);
    outcome.elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    let mut sorted = outcome.block_ms.clone();
    sorted.sort_unstable();
    tracing::info!(
        target: "bigname_project::families",
        chain_id,
        target_block = target.number,
        marker_block = outcome.marker.as_ref().map(|marker| marker.number),
        blocks = outcome.blocks,
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
        skipped = outcome.skipped.as_deref(),
        "Project families applied"
    );
    outcome
}

/// The outcome of a run that could not start, for example because the input token did not read
/// in time: the families stay where they stand and the skip is reported like any other.
pub async fn skipped(
    pool: &PgPool,
    chain_id: &str,
    target: &Marker,
    reason: String,
) -> FamilyOutcome {
    tracing::warn!(
        target: "bigname_project::families",
        chain_id,
        target_block = target.number,
        reason,
        "Project families skipped this batch; they catch up on the next"
    );
    FamilyOutcome {
        target: Some(target.clone()),
        marker: marker::read(pool, chain_id)
            .await
            .ok()
            .and_then(|marker| marker.current),
        skipped: Some(reason),
        ..FamilyOutcome::default()
    }
}

#[cfg(test)]
#[path = "guard_tests.rs"]
mod guard_tests;
#[cfg(test)]
#[path = "undo_tests.rs"]
mod undo_tests;
