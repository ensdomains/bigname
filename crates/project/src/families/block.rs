//! One family block in its own transaction: lock the shadow marker, require the block's
//! predecessor, read the block's events, derive its owned keys, reduce, journal the before-images
//! and the prior marker, write, advance the marker and prune the journal.
use std::{collections::BTreeMap, time::Instant};

use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, Transaction};

use super::{
    FamilyOptions, input, keys,
    marker::{self, FamilyMarker},
    reduce, store,
};
use crate::{Marker, ProjectError, Result};

/// What the loop expects before a block.
pub(crate) struct Plan<'a> {
    /// The marker the block must follow; `None` right after a reset.
    pub(crate) predecessor: Option<&'a Marker>,
    /// Whether the block must be the predecessor's child on the readable lineage. A rebuild
    /// visits only event-bearing blocks and skips the rest.
    pub(crate) contiguous: bool,
    /// Whether the marker stays in bootstrap_pending after the block.
    pub(crate) bootstrap: bool,
}

/// What one block wrote.
#[derive(Clone, Debug, Default)]
pub(crate) struct BlockStats {
    pub(crate) rows: BTreeMap<&'static str, u64>,
    pub(crate) undo_rows: u64,
    pub(crate) elapsed_ms: u64,
}

pub(crate) async fn apply(
    pool: &PgPool,
    chain_id: &str,
    number: i64,
    plan: &Plan<'_>,
    revision: (Option<&str>, Option<i64>),
    options: &FamilyOptions,
) -> Result<(FamilyMarker, BlockStats)> {
    let started = Instant::now();
    let (mut transaction, prior, block) = open(pool, chain_id, number, plan).await?;
    let events = input::block_events(&mut transaction, chain_id, &block).await?;
    let keys = keys::derive(&events);
    let mut rows = store::RowSet::default();
    let context = reduce::Context {
        chain_id,
        block: &block,
        keys: &keys,
    };
    reduce::apply(&mut transaction, &context, &events, &mut rows).await?;
    let (next, mut stats) = publish(
        transaction,
        chain_id,
        &block,
        &prior,
        &rows,
        plan,
        revision,
        options,
    )
    .await?;
    stats.elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    Ok((next, stats))
}

/// Begin the block's transaction: lock the marker and the block, and check the predecessor.
pub(crate) async fn open(
    pool: &PgPool,
    chain_id: &str,
    number: i64,
    plan: &Plan<'_>,
) -> Result<(
    Transaction<'static, Postgres>,
    FamilyMarker,
    input::BlockHeader,
)> {
    let mut transaction = pool
        .begin()
        .await
        .map_err(|error| ProjectError::database("failed to begin a family block", error))?;
    let prior = marker::lock(&mut transaction, chain_id).await?;
    let block = input::lock_block(&mut transaction, chain_id, number)
        .await?
        .ok_or_else(|| {
            ProjectError::transient(format!(
                "family block {number} of chain {chain_id} is not readable"
            ))
        })?;
    marker::require_predecessor(chain_id, &prior, plan.predecessor, &block, plan.contiguous)?;
    Ok((transaction, prior, block))
}

/// Journal and write the block's changed rows, journal the prior marker, advance the marker,
/// prune the journal and commit.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn publish(
    mut transaction: Transaction<'static, Postgres>,
    chain_id: &str,
    block: &input::BlockHeader,
    prior: &FamilyMarker,
    rows: &store::RowSet,
    plan: &Plan<'_>,
    revision: (Option<&str>, Option<i64>),
    options: &FamilyOptions,
) -> Result<(FamilyMarker, BlockStats)> {
    let mut stats = write(&mut transaction, chain_id, block, rows).await?;
    journal_marker(&mut transaction, chain_id, block, prior).await?;
    stats.undo_rows += 1;
    let next = FamilyMarker {
        current: Some(Marker {
            number: block.number,
            hash: block.hash.clone(),
        }),
        sequence: prior.sequence + 1,
        timestamp_seconds: Some(block.timestamp_seconds),
        input_content_hash: Some(options.input_content_hash.clone()),
        interpret_input_content_hash: revision.0.map(str::to_owned),
        interpret_redo_attempt: revision.1,
        bootstrap: plan.bootstrap,
    };
    marker::advance(&mut transaction, chain_id, &next).await?;
    prune(
        &mut transaction,
        chain_id,
        block.number,
        options.retained_undo_depth,
    )
    .await?;
    transaction
        .commit()
        .await
        .map_err(|error| ProjectError::database("failed to commit a family block", error))?;
    Ok((next, stats))
}

/// Journal every changed row's pre-block image, then write the changes table by table.
async fn write(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    block: &input::BlockHeader,
    rows: &store::RowSet,
) -> Result<BlockStats> {
    let changes = rows.changes();
    let mut stats = BlockStats::default();
    if changes.is_empty() {
        return Ok(stats);
    }
    let journal = changes
        .iter()
        .map(|change| {
            json!({
                "family": change.table.name,
                "key": change.key,
                "before_image": change.before.cloned().map(Value::Object),
            })
        })
        .collect::<Vec<_>>();
    stats.undo_rows += insert_journal(transaction, chain_id, block, journal).await?;

    let mut by_table: BTreeMap<&'static str, (Vec<Value>, Vec<Value>)> = BTreeMap::new();
    for change in &changes {
        let (keys, inserts) = by_table.entry(change.table.name).or_default();
        keys.push(Value::Object(store::key_object(change.table, change.key)?));
        if let Some(after) = change.after {
            inserts.push(Value::Object(after.clone()));
        }
    }
    for (name, (keys, inserts)) in by_table {
        let written = store::replace(transaction, super::tables::spec(name), keys, inserts).await?;
        stats.rows.insert(name, written);
    }
    let touched = super::derived::touched(transaction, chain_id, block.number).await?;
    super::derived::refresh(transaction, chain_id, &touched).await?;
    Ok(stats)
}

pub(crate) async fn insert_journal(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    block: &input::BlockHeader,
    journal: Vec<Value>,
) -> Result<u64> {
    Ok(sqlx::query(
        "/* project:families.block.insert_journal */ INSERT INTO project_family_undo (
             chain_id, block_number, block_hash, family, key, before_image
         )
         SELECT $1, $2, $3, entry.family, entry.key, NULLIF(entry.before_image, 'null'::jsonb)
         FROM jsonb_to_recordset($4) AS entry(family text, key text, before_image jsonb)",
    )
    .bind(chain_id)
    .bind(block.number)
    .bind(&block.hash)
    .bind(Value::Array(journal))
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to journal family before-images", error))?
    .rows_affected())
}

/// The prior marker, journalled on every block including an empty one.
async fn journal_marker(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    block: &input::BlockHeader,
    prior: &FamilyMarker,
) -> Result<()> {
    insert_journal(
        transaction,
        chain_id,
        block,
        vec![json!({"family": "marker", "key": chain_id, "before_image": prior.journal_image()})],
    )
    .await?;
    Ok(())
}

async fn prune(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    number: i64,
    depth: i64,
) -> Result<()> {
    sqlx::query(
        "/* project:families.block.prune */ DELETE FROM project_family_undo
         WHERE chain_id = $1 AND block_number < $2",
    )
    .bind(chain_id)
    .bind(number.saturating_sub(depth))
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to prune the family undo record", error))?;
    Ok(())
}
