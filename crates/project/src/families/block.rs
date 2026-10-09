//! One family block in its own transaction: lock the shadow marker and require the planned
//! predecessor and generation, read the block's lineage row, read the input token inside the
//! transaction and require the input revision the block must apply under, check the repair
//! record's state for the block's role, take the active manifest set at the block from the run's
//! manifest read, read the block's events, derive its owned keys, reduce, journal the
//! before-images and the prior marker, write, advance the marker with the token and manifest set
//! it used, prune the journal below the retention floor, complete a repair whose target this
//! block publishes, and commit.
use std::{collections::BTreeMap, time::Instant};

use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, Transaction};

use super::{
    FamilyOptions, hydrate, input,
    input::Revision,
    keys, manifests,
    marker::{self, FamilyMarker, RecordedToken},
    reduce, repair, store,
    tables::NAME_SUMMARY,
};
use crate::{Marker, ProjectError, Result};

/// Why a block runs, which decides what it requires of the repair record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Role {
    /// Following the served marker: no repair may be active.
    Follow,
    /// Replaying a repair under `attempt`; `completes` when the block is the replay target.
    Replay { attempt: i64, completes: bool },
    /// Populating a rebuild under `attempt`; `completes` when the block is the target.
    Rebuild { attempt: i64, completes: bool },
}

/// What the loop expects before a block.
pub(crate) struct Plan<'a> {
    /// The marker the block must follow; `None` right after a reset.
    pub(crate) predecessor: Option<&'a Marker>,
    /// The marker generation the block must follow.
    pub(crate) sequence: i64,
    /// Whether the block must be the predecessor's child on the readable lineage. A rebuild
    /// visits only the blocks that carry family work and skips the rest.
    pub(crate) contiguous: bool,
    /// Whether the marker stays in bootstrap_pending after the block.
    pub(crate) bootstrap: bool,
    /// The input revision the block must read inside its transaction.
    pub(crate) revision: &'a Revision,
    pub(crate) role: Role,
    /// The manifest updates the run read once; the block takes its active set from them.
    pub(crate) manifests: &'a manifests::History,
    /// The highest readable block when the run started, on a follow block of a run that may
    /// hydrate. The block hydrates only when it is that block, by number and hash.
    pub(crate) head: Option<&'a Marker>,
}

/// What one block wrote.
#[derive(Clone, Debug, Default)]
pub(crate) struct BlockStats {
    pub(crate) rows: BTreeMap<&'static str, u64>,
    pub(crate) undo_rows: u64,
    pub(crate) elapsed_ms: u64,
    /// Deliveries of one event identity that disagreed and were dropped.
    pub(crate) duplicate_anomalies: u64,
    /// Hydration row writes the block committed, reverse then text.
    pub(crate) hydration: (hydrate::outcome::Writes, hydrate::outcome::Writes),
}

/// A block whose input revision differs from the one it must apply under. The loop stops and
/// reports a skip.
pub(crate) fn revision_error(
    chain_id: &str,
    number: i64,
    read: Option<&Revision>,
    expected: &Revision,
) -> ProjectError {
    match read {
        None => ProjectError::transient(format!(
            "family block {number} of chain {chain_id} waits: Interpret is in redo"
        )),
        Some(read) => ProjectError::transient(format!(
            "family block {number} of chain {chain_id} stopped: the input revision changed \
             from {expected:?} to {read:?}"
        )),
    }
}

pub(crate) async fn apply(
    pool: &PgPool,
    chain_id: &str,
    number: i64,
    plan: &Plan<'_>,
    options: &FamilyOptions,
    hydration: &mut hydrate::outcome::HydrationOutcome,
) -> Result<(FamilyMarker, BlockStats)> {
    let started = Instant::now();
    let prepared = hydrate::prepare(pool, chain_id, number, plan, options, hydration).await?;
    let mut opened = open(pool, chain_id, number, plan).await?;
    if let Some(prepared) = &prepared {
        prepared.require_block(&opened.block)?;
    }
    let (events, duplicate_anomalies) =
        input::block_events(&mut opened.transaction, chain_id, &opened.block).await?;
    if duplicate_anomalies > 0 {
        tracing::warn!(
            target: "bigname_project::families",
            chain_id,
            block_number = number,
            duplicate_anomalies,
            "deliveries of one event identity disagreed; the first in the canonical order was kept"
        );
    }
    let keys = keys::derive(&events);
    let mut rows = store::RowSet::default();
    let context = reduce::Context {
        chain_id,
        block: &opened.block,
        keys: &keys,
        manifests: &opened.manifests,
        manifests_changed: opened.prior.admission_manifests.as_deref()
            != Some(opened.manifests.key.as_str()),
        prefetched: None,
    };
    reduce::apply(&mut opened.transaction, &context, &events, &mut rows).await?;
    let hydrated = match prepared {
        Some(prepared) => {
            prepared
                .apply(
                    &mut opened.transaction,
                    &context,
                    &mut rows,
                    plan.sequence + 1,
                )
                .await?
        }
        None => Default::default(),
    };
    let (next, mut stats) = publish(opened, chain_id, &rows, plan, options).await?;
    stats.hydration = hydrated;
    stats.duplicate_anomalies = duplicate_anomalies;
    stats.elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    Ok((next, stats))
}

/// A block transaction past its fences.
pub(crate) struct Opened {
    pub(crate) transaction: Transaction<'static, Postgres>,
    pub(crate) prior: FamilyMarker,
    pub(crate) block: input::BlockHeader,
    token: input::InputToken,
    record: Option<repair::Record>,
    pub(crate) manifests: manifests::ActiveSet,
}

/// Begin the block's transaction and pass its fences: the marker generation and predecessor,
/// the input revision read inside the transaction, and the repair record for the block's role.
pub(crate) async fn open(
    pool: &PgPool,
    chain_id: &str,
    number: i64,
    plan: &Plan<'_>,
) -> Result<Opened> {
    let mut transaction = pool
        .begin()
        .await
        .map_err(|error| ProjectError::database("failed to begin a family block", error))?;
    let prior = marker::lock(&mut transaction, chain_id).await?;
    let block = input::read_block(&mut transaction, chain_id, number)
        .await?
        .ok_or_else(|| {
            ProjectError::transient(format!(
                "family block {number} of chain {chain_id} is not readable"
            ))
        })?;
    marker::require_predecessor(
        chain_id,
        &prior,
        plan.predecessor,
        plan.sequence,
        &block,
        plan.contiguous,
    )?;
    let token = input::token_in(&mut transaction, chain_id).await?;
    let revision = token.revision();
    if revision.as_ref() != Some(plan.revision) {
        return Err(revision_error(
            chain_id,
            number,
            revision.as_ref(),
            plan.revision,
        ));
    }
    let record = repair::lock(&mut transaction, chain_id).await?;
    match plan.role {
        Role::Follow => repair::require_idle(chain_id, record.as_ref())?,
        Role::Replay { attempt, .. } => {
            let active = repair::require_state(
                chain_id,
                record.as_ref(),
                attempt,
                repair::State::Replaying,
            )?;
            if active.prefix_revision.as_ref() != Some(plan.revision) {
                return Err(revision_error(
                    chain_id,
                    number,
                    Some(plan.revision),
                    active.prefix_revision.as_ref().unwrap_or(plan.revision),
                ));
            }
        }
        Role::Rebuild { attempt, .. } => {
            repair::require_state(
                chain_id,
                record.as_ref(),
                attempt,
                repair::State::Rebuilding,
            )?;
        }
    }
    let manifests = plan.manifests.at(number);
    Ok(Opened {
        transaction,
        prior,
        block,
        token,
        record,
        manifests,
    })
}

/// Journal and write the block's changed rows, journal the prior marker, advance the marker with
/// the token and manifest set the block used, prune the journal, complete a repair whose target this is
/// and commit.
pub(crate) async fn publish(
    opened: Opened,
    chain_id: &str,
    rows: &store::RowSet,
    plan: &Plan<'_>,
    options: &FamilyOptions,
) -> Result<(FamilyMarker, BlockStats)> {
    let Opened {
        mut transaction,
        prior,
        block,
        token,
        record,
        manifests,
    } = opened;
    let after = prior.current.as_ref().map_or(-1, |marker| marker.number);
    let composition = marker::Composition::of(chain_id, &manifests)?;
    let mut stats = write(
        &mut transaction,
        chain_id,
        &block,
        after,
        rows,
        &composition,
    )
    .await?;
    journal_marker(&mut transaction, chain_id, &block, &prior).await?;
    stats.undo_rows += 1;
    let next = FamilyMarker {
        current: Some(Marker {
            number: block.number,
            hash: block.hash.clone(),
        }),
        sequence: prior.sequence + 1,
        timestamp_seconds: Some(block.timestamp_seconds),
        input_content_hash: Some(options.input_content_hash.clone()),
        token: RecordedToken::of(&token),
        admission_manifests: composition.manifests,
        admission: composition.admission,
        bootstrap: plan.bootstrap,
    };
    super::history_catalogue::stamp(&mut transaction, chain_id, &block, &next, &mut stats).await?;
    marker::advance(&mut transaction, chain_id, &next).await?;
    let floor = retention_floor(
        &mut transaction,
        chain_id,
        block.number,
        options.retained_undo_depth,
        record.as_ref(),
    )
    .await?;
    if let Some(floor) = floor {
        prune(&mut transaction, chain_id, floor).await?;
    }
    if let Role::Replay {
        completes: true, ..
    }
    | Role::Rebuild {
        completes: true, ..
    } = plan.role
    {
        let published = next.current.as_ref().expect("a block publishes a marker");
        repair::complete(
            &mut transaction,
            chain_id,
            published,
            next.sequence,
            &options.input_content_hash,
        )
        .await?;
    }
    transaction
        .commit()
        .await
        .map_err(|error| ProjectError::database("failed to commit a family block", error))?;
    Ok((next, stats))
}

/// Journal every changed row's pre-block image, then write the changes table by table. `after`
/// is the block of the family marker the write follows, -1 with none. `composition` is the
/// manifest set and admission the block publishes.
async fn write(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    block: &input::BlockHeader,
    after: i64,
    rows: &store::RowSet,
    composition: &marker::Composition,
) -> Result<BlockStats> {
    let changes = rows.written();
    let mut stats = BlockStats::default();
    if changes.is_empty() {
        // The name summaries follow the block clock too, which moves with no row changing.
        refresh_derived(transaction, chain_id, block, after, composition, &mut stats).await?;
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
    let hydration =
        hydrate::work::Targets::read(transaction, chain_id, &hydrate::work::images(&changes))
            .await?;
    for (name, (keys, inserts)) in by_table {
        let written = store::replace(transaction, super::tables::spec(name), keys, inserts).await?;
        stats.rows.insert(name, written);
    }
    hydration
        .refresh(transaction, chain_id, block.number)
        .await?;
    refresh_derived(transaction, chain_id, block, after, composition, &mut stats).await?;
    Ok(stats)
}

/// Refresh the derived rows of the keys the block touched, and count the name summaries it
/// wrote and journalled like a family table's.
async fn refresh_derived(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    block: &input::BlockHeader,
    after: i64,
    composition: &marker::Composition,
    stats: &mut BlockStats,
) -> Result<()> {
    let root = composition.admission.as_ref();
    let root = root.map(|admission| admission.root_registry.as_str());
    super::resolution_paths::prepare(transaction, chain_id, block, after, root).await?;
    super::lookup::prepare(transaction, chain_id, block, after).await?;
    let touched = super::derived::touched(transaction, chain_id, block.number, Some(after)).await?;
    let summary = super::derived::refresh(transaction, chain_id, &touched, composition).await?;
    if summary.rows > 0 {
        stats.rows.insert(NAME_SUMMARY.name, summary.rows);
    }
    stats.undo_rows += summary.undo_rows;
    super::history_catalogue::refresh(
        transaction,
        chain_id,
        block,
        after,
        &summary.names,
        &summary.current_relations,
        stats,
    )
    .await?;
    let (rows, undo_rows) =
        super::resolution_paths::refresh(transaction, chain_id, block, composition).await?;
    *stats.rows.entry(NAME_SUMMARY.name).or_default() += rows;
    stats.undo_rows += undo_rows;
    super::lookup::refresh(transaction, chain_id, block, composition, stats).await?;
    Ok(())
}

/// Journal each family once. The resolution-path refresh revisits summary and lookup keys the
/// ordinary refresh already wrote in this block. Preserve such a key's first before-image
/// while keeping duplicate checks for other families.
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
         FROM jsonb_to_recordset($4) AS entry(family text, key text, before_image jsonb)
         WHERE entry.family NOT IN ('project_name_summary', 'project_lookup_name',
             'project_lookup_relation', 'project_lookup_inventory', 'project_lookup_record',
             'project_lookup_dependency') OR NOT EXISTS (
             SELECT 1 FROM project_family_undo prior
             WHERE prior.chain_id=$1 AND prior.block_number=$2
               AND prior.family=entry.family AND prior.key=entry.key)",
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

/// The lowest block whose undo rows must stay (docs/projections.md, "Owned key families"): the
/// configured depth below this block, the chain's finalized and safe blocks, and an active
/// repair's floor, whichever is lowest. With no finalized or no safe block published, nothing is
/// pruned: a replacement could reach any depth.
async fn retention_floor(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    number: i64,
    depth: i64,
    record: Option<&repair::Record>,
) -> Result<Option<i64>> {
    let heads: Option<(Option<i64>, Option<i64>)> = sqlx::query_as(
        "/* project:families.block.retention_heads */ SELECT finalized_block_number,
                safe_block_number
         FROM chain_heads WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to read the chain's finality heads", error))?;
    let Some((Some(finalized), Some(safe))) = heads else {
        return Ok(None);
    };
    Ok([
        Some(number.saturating_sub(depth)),
        Some(finalized),
        Some(safe),
        record.and_then(repair::Record::retention_floor),
    ]
    .into_iter()
    .flatten()
    .min())
}

async fn prune(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    floor: i64,
) -> Result<()> {
    sqlx::query(
        "/* project:families.block.prune */ DELETE FROM project_family_undo
         WHERE chain_id = $1 AND block_number < $2",
    )
    .bind(chain_id)
    .bind(floor)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to prune the family undo record", error))?;
    Ok(())
}
