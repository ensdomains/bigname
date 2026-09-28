//! The history heads of a composed name row, `declared_summary.history` (name_current/build.sql,
//! the `surface_history` and `resource_history` laterals), which the binding diagnostics route
//! serves: the name's latest readable event, and its latest on the row's resource while its
//! authority is supported. Both read `normalized_events`, an input table, by the name's key at
//! read, in the served order (block, transaction and log index descending with nulls last, then
//! the generated id), so they are not a family fact. The unnamed registrar rows the staging passes
//! give the name count as the name's own, as they do in the served staging, and are read by their
//! identity.
use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde_json::{Value, json};
use sqlx::{PgConnection, Row};

/// One readable event as the heads compare it.
#[derive(Clone, Debug)]
pub(super) struct HeadEvent {
    logical_name_id: Option<String>,
    event_identity: String,
    resource_id: Option<String>,
    /// The served order, later is greater: `None < Some` as `DESC NULLS LAST` reads it.
    key: (i64, Option<i64>, Option<i64>, i64),
    pointer: Value,
}

/// The latest readable events of `names` per (name, resource), and the events `identities` name,
/// at or below `target`.
pub(super) async fn load_heads(
    conn: &mut PgConnection,
    chain_id: &str,
    target: i64,
    names: &[String],
    identities: &[String],
) -> Result<Vec<HeadEvent>> {
    let rows = sqlx::query(
        "/* storage:families.name.history_heads */
         WITH readable AS (
             SELECT event.logical_name_id, event.event_identity, event.resource_id::text
                        AS resource_id,
                    event.block_number, event.transaction_index, event.log_index,
                    event.normalized_event_id,
                    jsonb_build_object(
                        'normalized_event_id', event.normalized_event_id,
                        'event_kind', event.event_kind,
                        'chain_position', jsonb_strip_nulls(jsonb_build_object(
                            'chain_id', event.chain_id,
                            'block_number', event.block_number,
                            'block_hash', event.block_hash,
                            'timestamp', lineage.block_timestamp
                        ))
                    ) AS pointer
             FROM bigname_phase.normalized_events event
             LEFT JOIN bigname_phase.chain_lineage lineage
               ON lineage.chain_id = event.chain_id
              AND lineage.block_number = event.block_number
              AND lineage.block_hash = event.block_hash
             WHERE event.chain_id = $1
               AND event.consumer_visibility = 'activated'
               AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
               AND ((event.block_number IS NULL AND event.block_hash IS NULL)
                    OR (event.block_number <= $2
                        AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')))
               AND (event.logical_name_id = ANY($3)
                    OR (event.logical_name_id IS NULL AND event.event_identity = ANY($4)))
         )
         SELECT DISTINCT ON (logical_name_id, resource_id,
                             CASE WHEN logical_name_id IS NULL THEN event_identity END)
                logical_name_id, event_identity, resource_id, block_number, transaction_index,
                log_index, normalized_event_id, pointer
         FROM readable
         ORDER BY logical_name_id, resource_id,
                  CASE WHEN logical_name_id IS NULL THEN event_identity END,
                  block_number DESC NULLS LAST, transaction_index DESC NULLS LAST,
                  log_index DESC NULLS LAST, normalized_event_id DESC",
    )
    .bind(chain_id)
    .bind(target)
    .bind(names)
    .bind(identities)
    .fetch_all(&mut *conn)
    .await
    .context("failed to load the name history heads")?;
    rows.into_iter()
        .map(|row| {
            let block: Option<i64> = row.try_get("block_number")?;
            Ok(HeadEvent {
                logical_name_id: row.try_get("logical_name_id")?,
                event_identity: row.try_get("event_identity")?,
                resource_id: row.try_get("resource_id")?,
                key: (
                    block.unwrap_or(i64::MIN),
                    row.try_get("transaction_index")?,
                    row.try_get("log_index")?,
                    row.try_get("normalized_event_id")?,
                ),
                pointer: row.try_get("pointer")?,
            })
        })
        .collect()
}

/// The heads loaded for a batch, by name and by the identity of an unnamed event.
#[derive(Default)]
pub(super) struct Heads {
    named: BTreeMap<String, Vec<HeadEvent>>,
    unnamed: BTreeMap<String, HeadEvent>,
}

impl Heads {
    pub(super) fn new(events: Vec<HeadEvent>) -> Self {
        let mut heads = Self::default();
        for event in events {
            match event.logical_name_id.clone() {
                Some(name) => heads.named.entry(name).or_default().push(event),
                None => {
                    heads.unnamed.insert(event.event_identity.clone(), event);
                }
            }
        }
        heads
    }

    /// `declared_summary.history` of `name`: `staged` names the unnamed events the staging gives
    /// it, `resource` is the row's resource when its authority is supported.
    pub(super) fn history(&self, name: &str, staged: &[&str], resource: Option<&str>) -> Value {
        let own: Vec<&HeadEvent> = self
            .named
            .get(name)
            .into_iter()
            .flatten()
            .chain(
                staged
                    .iter()
                    .filter_map(|identity| self.unnamed.get(*identity)),
            )
            .collect();
        let latest = |events: &mut dyn Iterator<Item = &&HeadEvent>| {
            events
                .max_by_key(|event| event.key)
                .map_or(Value::Null, |event| event.pointer.clone())
        };
        let surface = latest(&mut own.iter());
        let resource_head = match resource {
            Some(resource) => latest(
                &mut own
                    .iter()
                    .filter(|event| event.resource_id.as_deref() == Some(resource)),
            ),
            None => Value::Null,
        };
        json!({"surface_head": surface, "resource_head": resource_head})
    }
}
