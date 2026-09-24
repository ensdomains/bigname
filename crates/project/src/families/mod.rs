//! Owned key families (docs/projections.md, "Owned key families"): per-key current-state tables
//! that step 2 of TYR-36 fills block by block beside the served tables. Nothing reads them yet.
//!
//! The loop runs after a Project batch has committed and its progress is recorded, never inside
//! the served transaction: each family block is its own transaction, so a family failure or a
//! slow block can never delay or roll back a served publication. The families follow the served
//! marker from their own shadow marker, catching up from wherever it stands.
// The reducers, undo and the input token land in the commits that follow and use the helpers
// that are unused until then.
#![allow(dead_code)]
mod block;
mod input;
mod keys;
mod marker;
mod reduce;
mod store;
mod tables;

use std::{collections::BTreeMap, time::Instant};

use sqlx::PgPool;

use crate::{Marker, ProjectError, Result};

/// Undo rows are kept for this many blocks below the family marker (docs/projections.md,
/// "Owned key families"); the per-block publication tunes it.
pub const RETAINED_UNDO_DEPTH: i64 = 256;

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
    /// Blocks of undo rows kept below the marker.
    pub retained_undo_depth: i64,
}

impl FamilyOptions {
    pub fn new(input_content_hash: impl Into<String>) -> Self {
        Self {
            input_content_hash: input_content_hash.into(),
            retained_undo_depth: RETAINED_UNDO_DEPTH,
        }
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
        self.block_ms.push(stats.elapsed_ms);
        for (table, rows) in stats.rows {
            *self.rows.entry(table).or_default() += rows;
        }
    }
}

/// Bring the families to the served `target`. Never fails: an error stops the loop, is logged
/// and returned in `skipped`, and leaves the families at the last complete block.
pub async fn apply(
    pool: &PgPool,
    chain_id: &str,
    target: &Marker,
    mode: FamilyMode,
    options: &FamilyOptions,
) -> FamilyOutcome {
    let started = Instant::now();
    let mut outcome = FamilyOutcome {
        target: Some(target.clone()),
        ..FamilyOutcome::default()
    };
    if let Err(error) = run(pool, chain_id, target, &mode, options, &mut outcome).await {
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
        skipped = outcome.skipped.as_deref(),
        "Project families applied"
    );
    outcome
}

async fn run(
    pool: &PgPool,
    chain_id: &str,
    target: &Marker,
    mode: &FamilyMode,
    options: &FamilyOptions,
    outcome: &mut FamilyOutcome,
) -> Result<()> {
    let revision = (None, None);
    let current = marker::read(pool, chain_id).await?;
    let aligned = match &current.current {
        Some(marker) => {
            marker.number <= target.number
                && input::readable_hash(pool, chain_id, marker.number)
                    .await?
                    .as_deref()
                    == Some(marker.hash.as_str())
        }
        None => false,
    };
    if *mode == FamilyMode::Rebuild || !aligned {
        let from = if *mode != FamilyMode::Rebuild && aligned && current.bootstrap {
            current.current.clone()
        } else {
            reset(pool, chain_id).await?;
            outcome.reset = true;
            None
        };
        return rebuild(pool, chain_id, target, from, revision, options, outcome).await;
    }
    catch_up(
        pool,
        chain_id,
        current.current,
        target,
        revision,
        options,
        outcome,
    )
    .await
}

/// Apply every block above the family marker up to the target, one transaction each.
async fn catch_up(
    pool: &PgPool,
    chain_id: &str,
    mut current: Option<Marker>,
    target: &Marker,
    revision: (Option<&str>, Option<i64>),
    options: &FamilyOptions,
    outcome: &mut FamilyOutcome,
) -> Result<()> {
    let from = current.as_ref().map_or(0, |marker| marker.number + 1);
    for number in from..=target.number {
        let plan = block::Plan {
            predecessor: current.as_ref(),
            contiguous: true,
            bootstrap: false,
        };
        let (next, stats) = block::apply(pool, chain_id, number, &plan, revision, options)
            .await
            .map_err(|error| at_block(number, error))?;
        outcome.record(stats);
        current = next.current;
    }
    Ok(())
}

/// Populate the families from `from` (a reset leaves `None`) to the target, visiting only the
/// blocks that carry events and then the target itself, so the marker ends at the target.
async fn rebuild(
    pool: &PgPool,
    chain_id: &str,
    target: &Marker,
    mut current: Option<Marker>,
    revision: (Option<&str>, Option<i64>),
    options: &FamilyOptions,
    outcome: &mut FamilyOutcome,
) -> Result<()> {
    let from = current.as_ref().map_or(0, |marker| marker.number + 1);
    let mut blocks = input::event_blocks(pool, chain_id, from, target.number).await?;
    if blocks.last() != Some(&target.number) {
        blocks.push(target.number);
    }
    for number in blocks {
        let plan = block::Plan {
            predecessor: current.as_ref(),
            contiguous: false,
            bootstrap: number != target.number,
        };
        let (next, stats) = block::apply(pool, chain_id, number, &plan, revision, options)
            .await
            .map_err(|error| at_block(number, error))?;
        outcome.record(stats);
        current = next.current;
    }
    Ok(())
}

/// Clear every family row, undo row and the marker of the chain.
async fn reset(pool: &PgPool, chain_id: &str) -> Result<()> {
    let mut transaction = pool
        .begin()
        .await
        .map_err(|error| ProjectError::database("failed to begin a family reset", error))?;
    marker::lock(&mut transaction, chain_id).await?;
    for table in tables::JOURNALLED
        .iter()
        .map(|table| table.name)
        .chain(tables::DERIVED)
        .chain(["project_family_undo"])
    {
        sqlx::query(&format!(
            "/* project:families.reset_{table} */ DELETE FROM {table} WHERE chain_id = $1"
        ))
        .bind(chain_id)
        .execute(&mut *transaction)
        .await
        .map_err(|error| ProjectError::database(format!("failed to reset {table}"), error))?;
    }
    let sequence = marker::read_locked_sequence(&mut transaction, chain_id).await?;
    marker::advance(
        &mut transaction,
        chain_id,
        &marker::FamilyMarker {
            sequence: sequence + 1,
            bootstrap: true,
            ..marker::FamilyMarker::default()
        },
    )
    .await?;
    transaction
        .commit()
        .await
        .map_err(|error| ProjectError::database("failed to commit a family reset", error))
}

fn at_block(number: i64, error: ProjectError) -> ProjectError {
    reduce::in_family(&format!("block {number}"))(error)
}

#[cfg(test)]
#[path = "guard_tests.rs"]
mod guard_tests;
