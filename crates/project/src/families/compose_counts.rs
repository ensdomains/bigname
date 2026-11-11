//! The event count stream of a rebuild's composition pass (docs/projections.md, "Event counts"):
//! the stored counts of every event at or below the pass block, built once at the pass block from
//! the statement each block counts its own range with (`block_event_counts_sql`), in steps of
//! partitions like the catalogue build (history_catalogue/build). That statement decides a row
//! from the row itself, its own block's lineage row and rows of the same block (the address pair,
//! the handoff copies), so the counts over `(-1, pass]` are the sums of its counts over any split
//! of that range into block spans. The stream reads the events once:
//!
//! 1. `spans`: per block span, the span's per-key counts into `project_compose_event_count_work`.
//! 2. `sums`: per key partition, the chain's stored counts of the partition deleted and the
//!    span counts summed into them.
//!
//! Each partition deletes what it writes first, so a partition run again writes the same rows.
//! The ranges before the pass write no counts and the rebuild's reset deleted the chain's, so once
//! the stream ended a key no event counts has no row. The last partition clears the work table.
use std::sync::LazyLock;

use bigname_storage::history_count_contract::block_event_counts_sql;
use sqlx::{Postgres, Transaction};

use super::steps::{Parts, Step, Steps};
use crate::{
    ProjectError, Result,
    families::{MAX_COMPOSE_BUILDER_BLOCK_SPANS, tables::EVENT_COUNT},
};

pub(crate) const STEPS: Steps = &[("spans", Parts::Spans), ("sums", Parts::Partitions)];

/// The stream's work table, cleared with the pass's other work tables.
pub(crate) const WORK: &str = "project_compose_event_count_work";

const CLEAR_SPAN: &str = "/* project:families.compose.count_clear_span */
    DELETE FROM project_compose_event_count_work WHERE chain_id = $1 AND part = $2";

/// The per-key counts of the blocks `($2, $3]`, as each block's own statement gives them, keyed
/// into partition `hashtext(anchor) & $5` for the merge. `$4` is the span.
static SPAN: LazyLock<String> = LazyLock::new(|| {
    format!(
        "/* project:families.compose.count_span */
    INSERT INTO project_compose_event_count_work
        (chain_id, part, key_partition, anchor_kind, anchor, namespace, event_kind, delta)
    SELECT $1, $4::smallint, (hashtext(counted.anchor) & $5::int)::smallint,
           counted.anchor_kind, counted.anchor, counted.namespace, counted.event_kind,
           counted.delta
    FROM ({}) counted",
        block_event_counts_sql()
    )
});

const CLEAR_COUNTS: &str = "/* project:families.compose.count_clear */
    DELETE FROM project_event_count
    WHERE chain_id = $1 AND (hashtext(anchor) & $3::int) = $2::int";

const MERGE: &str = "/* project:families.compose.count_merge */
    INSERT INTO project_event_count
        (chain_id, anchor_kind, anchor, namespace, event_kind, event_count)
    SELECT $1, work.anchor_kind, work.anchor, work.namespace, work.event_kind,
           sum(work.delta)::bigint
    FROM project_compose_event_count_work work
    WHERE work.chain_id = $1 AND work.key_partition = $2::smallint
    GROUP BY work.anchor_kind, work.anchor, work.namespace, work.event_kind";

const CLEAR_WORK: &str = "/* project:families.compose.count_clear_work */
    DELETE FROM project_compose_event_count_work WHERE chain_id = $1";

/// Run the partition `step` names at the pass block `pass`. Returns the table it wrote and the
/// rows it wrote there.
pub(crate) async fn run(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    pass: i64,
    step: &Step,
) -> Result<(&'static str, u64)> {
    match STEPS[step.index].1 {
        Parts::Spans => span(transaction, chain, pass, step).await,
        Parts::Partitions => merge(transaction, chain, step).await,
    }
}

async fn span(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    pass: i64,
    step: &Step,
) -> Result<(&'static str, u64)> {
    let part = i16::try_from(step.part)
        .ok()
        .filter(|_| step.part < MAX_COMPOSE_BUILDER_BLOCK_SPANS)
        .ok_or_else(|| {
            ProjectError::data_integrity(format!(
                "event count span {} is past the most spans, {MAX_COMPOSE_BUILDER_BLOCK_SPANS}",
                step.part
            ))
        })?;
    sqlx::query(CLEAR_SPAN)
        .bind(chain)
        .bind(part)
        .execute(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to clear an event count span", error))?;
    let (from, to) = bounds(pass, step.part, step.layout.spans);
    let rows = sqlx::query(SPAN.as_str())
        .bind(chain)
        .bind(from - 1)
        .bind(to - 1)
        .bind(part)
        .bind(step.mask())
        .execute(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to count a block span's events", error))?
        .rows_affected();
    Ok((WORK, rows))
}

async fn merge(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    step: &Step,
) -> Result<(&'static str, u64)> {
    let (part, mask) = (i32::from(step.part), step.mask());
    sqlx::query(CLEAR_COUNTS)
        .bind(chain)
        .bind(part)
        .bind(mask)
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to clear a partition of event counts", error)
        })?;
    let partition = i16::try_from(step.part).map_err(|_| {
        ProjectError::data_integrity(format!("event count partition {}", step.part))
    })?;
    let rows = sqlx::query(MERGE)
        .bind(chain)
        .bind(partition)
        .execute(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to merge the event counts", error))?
        .rows_affected();
    if step.last(STEPS) {
        sqlx::query(CLEAR_WORK)
            .bind(chain)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database("failed to clear the event count work table", error)
            })?;
    }
    Ok((EVENT_COUNT.name, rows))
}

/// The blocks `[from, to)` of span `span` of `spans` over `[0, pass]`, as the catalogue build
/// splits them.
pub(crate) fn bounds(pass: i64, span: u16, spans: u16) -> (i64, i64) {
    let blocks = i128::from(pass) + 1;
    let at = |span: u16| {
        i64::try_from(i128::from(span) * blocks / i128::from(spans.max(1))).unwrap_or(i64::MAX)
    };
    (at(span), at(span + 1))
}
