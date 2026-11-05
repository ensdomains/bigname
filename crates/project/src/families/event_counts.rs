//! F20, the stored event counts (`docs/projections.md`, "Event counts"): per chain, anchor kind,
//! anchor, namespace and event kind, the number of product-visible events at or below the
//! publication's block that the anchor's event read admits. A block adds the counts of the events
//! in its range after the family marker's block, in the block's own transaction, and journals
//! each touched key's before-image, so an undo restores the counts with the other families. The
//! composition pass adds the counts of each catalogue window the same way. A count of zero has no
//! row.
use std::{collections::BTreeMap, sync::LazyLock};

use bigname_storage::history_count_contract::block_event_counts_sql;
use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};

use super::{block::BlockStats, history_catalogue::write, input::BlockHeader, tables::EVENT_COUNT};
use crate::{ProjectError, Result};

/// Above this many touched keys a step logs a warning, so a large undo journal can be traced to
/// its block.
const WARN_KEYS: usize = 10_000;

static BLOCK_COUNTS: LazyLock<String> = LazyLock::new(block_event_counts_sql);

type Key = (i16, String, String, String);

/// Add the counts of `chain`'s events in the block range `(after, block]`.
pub(crate) async fn refresh(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    block: &BlockHeader,
    after: i64,
    stats: &mut BlockStats,
) -> Result<()> {
    if block.number <= after {
        return Ok(());
    }
    let deltas: Vec<(i16, String, String, String, i64)> = sqlx::query_as(BLOCK_COUNTS.as_str())
        .bind(chain)
        .bind(after)
        .bind(block.number)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to count a block's events", error))?;
    if deltas.is_empty() {
        return Ok(());
    }
    if deltas.len() > WARN_KEYS {
        tracing::warn!(
            target: "bigname_project::families",
            chain_id = chain,
            block = block.number,
            after,
            keys = deltas.len(),
            "an event count step touched more than {WARN_KEYS} keys"
        );
    }
    let wanted = Value::Array(
        deltas
            .iter()
            .map(|(anchor_kind, anchor, namespace, event_kind, _)| {
                json!({"anchor_kind": anchor_kind, "anchor": anchor,
                    "namespace": namespace, "event_kind": event_kind})
            })
            .collect(),
    );
    let before: Vec<Value> = sqlx::query_scalar(
        "/* project:event_counts.before */ SELECT to_jsonb(stored)
         FROM jsonb_to_recordset($2)
             wanted(anchor_kind smallint, anchor text, namespace text, event_kind text)
         JOIN project_event_count stored ON stored.chain_id = $1
          AND stored.anchor_kind = wanted.anchor_kind AND stored.anchor = wanted.anchor
          AND stored.namespace = wanted.namespace AND stored.event_kind = wanted.event_kind",
    )
    .bind(chain)
    .bind(&wanted)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to read event count before-images", error))?;
    let mut prior = BTreeMap::new();
    for row in &before {
        let (key, count) = stored(row)?;
        prior.insert(key, count);
    }
    let fresh = deltas
        .into_iter()
        .map(|(anchor_kind, anchor, namespace, event_kind, delta)| {
            let key = (anchor_kind, anchor, namespace, event_kind);
            let count = prior.get(&key).copied().unwrap_or(0) + delta;
            let (anchor_kind, anchor, namespace, event_kind) = key;
            json!({"chain_id": chain, "anchor_kind": anchor_kind, "anchor": anchor,
                "namespace": namespace, "event_kind": event_kind, "event_count": count})
        })
        .collect();
    let (rows, undo) =
        write::replace(transaction, chain, block, &EVENT_COUNT, before, fresh).await?;
    *stats.rows.entry(EVENT_COUNT.name).or_default() += rows;
    stats.undo_rows += undo;
    Ok(())
}

fn stored(row: &Value) -> Result<(Key, i64)> {
    let text = |column: &str| {
        row.get(column)
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| ProjectError::data_integrity(format!("event count row lacks {column}")))
    };
    let number = |column: &str| {
        row.get(column)
            .and_then(Value::as_i64)
            .ok_or_else(|| ProjectError::data_integrity(format!("event count row lacks {column}")))
    };
    let anchor_kind = i16::try_from(number("anchor_kind")?)
        .map_err(|_| ProjectError::data_integrity("event count anchor kind out of range"))?;
    Ok((
        (
            anchor_kind,
            text("anchor")?,
            text("namespace")?,
            text("event_kind")?,
        ),
        number("event_count")?,
    ))
}
