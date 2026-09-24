//! The input of one block: its readable lineage row and its activated canonical events in the
//! canonical event order, read straight from `normalized_events` so the loop can run any block,
//! inside or outside the batch that published it.
use serde_json::{Map, Value, json};
use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

/// The canonical event order (docs/projections.md, "Owned key families"): block number,
/// transaction index, log index, then the event identity compared as bytes. A synthesised event
/// has no transaction or log position and sorts before every transaction of its block. The
/// derived ordering compares the fields in this order and puts `None` first.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct Position {
    pub(crate) block_number: i64,
    pub(crate) transaction_index: Option<i64>,
    pub(crate) log_index: Option<i64>,
    pub(crate) event_identity: String,
}

impl Position {
    /// The four position columns every family row carries for its last owning event.
    pub(crate) fn write_columns(&self, row: &mut Map<String, Value>) {
        row.insert("block_number".into(), json!(self.block_number));
        row.insert("transaction_index".into(), json!(self.transaction_index));
        row.insert("log_index".into(), json!(self.log_index));
        row.insert("event_identity".into(), json!(self.event_identity));
    }

    /// A secondary position stored as one JSON object beside the row's own position.
    pub(crate) fn to_json(&self) -> Value {
        json!({
            "block_number": self.block_number,
            "transaction_index": self.transaction_index,
            "log_index": self.log_index,
            "event_identity": self.event_identity,
        })
    }

    /// The row's own position, when the row has one.
    pub(crate) fn of_row(row: &Map<String, Value>) -> Option<Self> {
        Self::from_object(row)
    }

    fn from_object(object: &Map<String, Value>) -> Option<Self> {
        Some(Self {
            block_number: object.get("block_number")?.as_i64()?,
            transaction_index: object.get("transaction_index").and_then(Value::as_i64),
            log_index: object.get("log_index").and_then(Value::as_i64),
            event_identity: object.get("event_identity")?.as_str()?.to_owned(),
        })
    }
}

/// One activated canonical event of the block.
#[derive(Clone, Debug)]
pub(crate) struct BlockEvent {
    pub(crate) normalized_event_id: i64,
    pub(crate) position: Position,
    pub(crate) namespace: String,
    pub(crate) logical_name_id: Option<String>,
    pub(crate) resource_id: Option<String>,
    pub(crate) event_kind: String,
    pub(crate) source_family: String,
    pub(crate) source_manifest_id: Option<i64>,
    pub(crate) transaction_hash: Option<String>,
    pub(crate) before: Value,
    pub(crate) after: Value,
    pub(crate) raw_fact_ref: Value,
}

impl BlockEvent {
    /// A text field of the after state, `None` when absent, null or blank.
    pub(crate) fn after_text(&self, field: &str) -> Option<String> {
        text(&self.after, field)
    }

    /// A text field of the before state, `None` when absent, null or blank.
    pub(crate) fn before_text(&self, field: &str) -> Option<String> {
        text(&self.before, field)
    }

    /// The address that emitted the log, lower-cased.
    pub(crate) fn emitting_address(&self) -> Option<String> {
        text(&self.raw_fact_ref, "emitting_address").map(|address| address.to_ascii_lowercase())
    }

    /// The event's own position plus its normalized event id as attribution.
    pub(crate) fn write_position(&self, row: &mut Map<String, Value>) {
        self.position.write_columns(row);
        row.insert(
            "normalized_event_id".into(),
            json!(self.normalized_event_id),
        );
    }
}

/// A non-blank text field of a JSON object. Numbers are rendered as their decimal text, the
/// shape adapters use for coin types and token ids written as numbers.
pub(crate) fn text(value: &Value, field: &str) -> Option<String> {
    match value.get(field)? {
        Value::String(text) if !text.trim().is_empty() => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

/// The readable lineage row of one block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BlockHeader {
    pub(crate) number: i64,
    pub(crate) hash: String,
    /// The predecessor's hash: the recorded parent, else the readable hash one block below.
    pub(crate) predecessor_hash: Option<String>,
    pub(crate) timestamp_seconds: i64,
    /// The block timestamp as `to_jsonb` renders it, for timestamp columns of family rows.
    pub(crate) timestamp: Value,
}

/// Lock block `number`'s readable lineage row for the transaction and read its predecessor.
pub(crate) async fn lock_block(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    number: i64,
) -> Result<Option<BlockHeader>> {
    let row: Option<(String, Option<String>, i64, Value)> = sqlx::query_as(
        "/* project:families.input.lock_block */ SELECT lineage.block_hash,
                COALESCE(lineage.parent_hash, (
                    SELECT previous.block_hash FROM chain_lineage previous
                    WHERE previous.chain_id = lineage.chain_id
                      AND previous.block_number = lineage.block_number - 1
                      AND previous.canonicality_state IN ('canonical', 'safe', 'finalized')
                )),
                extract(epoch FROM lineage.block_timestamp)::bigint,
                to_jsonb(lineage.block_timestamp)
         FROM chain_lineage lineage
         WHERE lineage.chain_id = $1 AND lineage.block_number = $2
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
         FOR SHARE",
    )
    .bind(chain_id)
    .bind(number)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to lock family block lineage", error))?;
    Ok(row.map(
        |(hash, predecessor_hash, timestamp_seconds, timestamp)| BlockHeader {
            number,
            hash,
            predecessor_hash,
            timestamp_seconds,
            timestamp,
        },
    ))
}

type EventRow = (
    i64,
    String,
    String,
    Option<String>,
    Option<String>,
    String,
    String,
    Option<i64>,
    Option<String>,
    Option<i64>,
    Option<i64>,
    Value,
    Value,
    Value,
);

/// The block's activated canonical events at its readable hash, in the canonical event order,
/// each normalized event once.
pub(crate) async fn block_events(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    block: &BlockHeader,
) -> Result<Vec<BlockEvent>> {
    let rows: Vec<EventRow> = sqlx::query_as(
        "/* project:families.input.block_events */ SELECT event.normalized_event_id,
                event.event_identity, event.namespace, event.logical_name_id,
                event.resource_id::text, event.event_kind, event.source_family,
                event.source_manifest_id, event.transaction_hash, event.transaction_index,
                event.log_index, event.before_state, event.after_state, event.raw_fact_ref
         FROM normalized_events event
         WHERE event.chain_id = $1 AND event.block_number = $2 AND event.block_hash = $3
           AND event.consumer_visibility = 'activated'
           AND event.canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(chain_id)
    .bind(block.number)
    .bind(&block.hash)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to read family block events", error))?;
    let mut events = rows
        .into_iter()
        .map(
            |(
                normalized_event_id,
                event_identity,
                namespace,
                logical_name_id,
                resource_id,
                event_kind,
                source_family,
                source_manifest_id,
                transaction_hash,
                transaction_index,
                log_index,
                before,
                after,
                raw_fact_ref,
            )| BlockEvent {
                normalized_event_id,
                position: Position {
                    block_number: block.number,
                    transaction_index,
                    log_index,
                    event_identity,
                },
                namespace,
                logical_name_id,
                resource_id,
                event_kind,
                source_family,
                source_manifest_id,
                transaction_hash,
                before,
                after,
                raw_fact_ref,
            },
        )
        .collect::<Vec<_>>();
    order(&mut events);
    Ok(events)
}

/// Sort into the canonical order and keep each normalized event once.
pub(crate) fn order(events: &mut Vec<BlockEvent>) {
    events.sort_by(|left, right| {
        left.position
            .cmp(&right.position)
            .then(left.normalized_event_id.cmp(&right.normalized_event_id))
    });
    let mut seen = std::collections::BTreeSet::new();
    events.retain(|event| seen.insert(event.normalized_event_id));
}

/// Blocks in `from..=to` that carry an activated canonical event, ascending. A rebuild visits
/// only these: a block with no event owns no family fact.
pub(crate) async fn event_blocks(
    pool: &sqlx::PgPool,
    chain_id: &str,
    from: i64,
    to: i64,
) -> Result<Vec<i64>> {
    sqlx::query_scalar(
        "/* project:families.input.event_blocks */ SELECT DISTINCT event.block_number
         FROM normalized_events event
         JOIN chain_lineage lineage
           ON lineage.chain_id = event.chain_id
          AND lineage.block_number = event.block_number
          AND lineage.block_hash = event.block_hash
         WHERE event.chain_id = $1 AND event.block_number BETWEEN $2 AND $3
           AND event.consumer_visibility = 'activated'
           AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
         ORDER BY 1",
    )
    .bind(chain_id)
    .bind(from)
    .bind(to)
    .fetch_all(pool)
    .await
    .map_err(|error| ProjectError::database("failed to list family event blocks", error))
}

/// The readable hash of a block, `None` when no readable row exists at that height.
pub(crate) async fn readable_hash(
    pool: &sqlx::PgPool,
    chain_id: &str,
    number: i64,
) -> Result<Option<String>> {
    sqlx::query_scalar(
        "/* project:families.input.readable_hash */ SELECT block_hash FROM chain_lineage
         WHERE chain_id = $1 AND block_number = $2
           AND canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(chain_id)
    .bind(number)
    .fetch_optional(pool)
    .await
    .map_err(|error| ProjectError::database("failed to read a readable family block hash", error))
}

/// The input token: the Interpret row's redo state and content hash and the Project row's redo
/// session fence. The Project phase reads it right after its batch commits, while a redo is still
/// open, and hands it to the loop. Step 2 records it and aborts nothing.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct InputToken {
    pub interpret_input_content_hash: Option<String>,
    pub interpret_redo_attempt_generation: Option<i64>,
    pub interpret_redo_in_progress: bool,
    pub project_redo_attempt_generation: i64,
    pub project_redo_mode: Option<String>,
    pub project_redo_from: Option<i64>,
    pub project_redo_to: Option<i64>,
    pub project_last_error: Option<String>,
}

impl InputToken {
    /// The input revision: the Interpret row's content hash and redo attempt, taken only while
    /// Interpret is not in redo. `None` while it is.
    pub fn revision(&self) -> (Option<&str>, Option<i64>) {
        if self.interpret_redo_in_progress {
            (None, None)
        } else {
            (
                self.interpret_input_content_hash.as_deref(),
                self.interpret_redo_attempt_generation,
            )
        }
    }
}

type TokenRow = (
    Option<String>,
    Option<i64>,
    Option<bool>,
    Option<i64>,
    Option<String>,
    Option<i64>,
    Option<i64>,
    Option<String>,
);

pub async fn input_token(pool: &sqlx::PgPool, chain_id: &str) -> Result<InputToken> {
    let row: TokenRow = sqlx::query_as(
        "/* project:families.input.input_token */ SELECT interpret.input_content_hash,
                interpret.redo_attempt_generation, interpret.redo_in_progress,
                project.redo_attempt_generation, project.redo_mode,
                project.redo_from_block_number, project.redo_to_block_number, project.last_error
         FROM (SELECT 1) anchor
         LEFT JOIN chain_phase_state interpret
           ON interpret.chain_id = $1 AND interpret.phase_name = 'interpret'
         LEFT JOIN chain_phase_state project
           ON project.chain_id = $1 AND project.phase_name = 'project'",
    )
    .bind(chain_id)
    .fetch_one(pool)
    .await
    .map_err(|error| ProjectError::database("failed to read the family input token", error))?;
    let (hash, interpret_attempt, in_redo, project_attempt, mode, from, to, last_error) = row;
    Ok(InputToken {
        interpret_input_content_hash: hash,
        interpret_redo_attempt_generation: interpret_attempt,
        interpret_redo_in_progress: in_redo.unwrap_or(false),
        project_redo_attempt_generation: project_attempt.unwrap_or(0),
        project_redo_mode: mode,
        project_redo_from: from,
        project_redo_to: to,
        project_last_error: last_error,
    })
}
