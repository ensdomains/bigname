//! The shadow marker: how far the owned-key families have been applied, with the generation every
//! family block and every family undo advances. It is not the served marker; the Project row of
//! `chain_phase_state` keeps that role until the families are read.
use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};

use super::input::BlockHeader;
use crate::{Marker, ProjectError, Result};

/// The locked marker row of one chain.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct FamilyMarker {
    pub(crate) current: Option<Marker>,
    pub(crate) sequence: i64,
    pub(crate) timestamp_seconds: Option<i64>,
    pub(crate) input_content_hash: Option<String>,
    pub(crate) interpret_input_content_hash: Option<String>,
    pub(crate) interpret_redo_attempt: Option<i64>,
    /// Whether a rebuild is still populating the families.
    pub(crate) bootstrap: bool,
}

impl FamilyMarker {
    /// The prior marker as the journal stores it under family `marker`.
    pub(crate) fn journal_image(&self) -> Value {
        json!({
            "current_block_number": self.current.as_ref().map(|marker| marker.number),
            "current_block_hash": self.current.as_ref().map(|marker| marker.hash.clone()),
            "block_timestamp_seconds": self.timestamp_seconds,
            "input_content_hash": self.input_content_hash,
            "interpret_input_content_hash": self.interpret_input_content_hash,
            "interpret_redo_attempt": self.interpret_redo_attempt,
            "bootstrap": self.bootstrap,
        })
    }

    pub(crate) fn from_journal_image(image: &Value, sequence: i64) -> Self {
        let number = image.get("current_block_number").and_then(Value::as_i64);
        let hash = image
            .get("current_block_hash")
            .and_then(Value::as_str)
            .map(str::to_owned);
        Self {
            current: number
                .zip(hash)
                .map(|(number, hash)| Marker { number, hash }),
            sequence,
            timestamp_seconds: image.get("block_timestamp_seconds").and_then(Value::as_i64),
            input_content_hash: text(image, "input_content_hash"),
            interpret_input_content_hash: text(image, "interpret_input_content_hash"),
            interpret_redo_attempt: image.get("interpret_redo_attempt").and_then(Value::as_i64),
            bootstrap: image
                .get("bootstrap")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        }
    }
}

fn text(image: &Value, field: &str) -> Option<String> {
    image.get(field).and_then(Value::as_str).map(str::to_owned)
}

type MarkerRow = (
    Option<i64>,
    Option<String>,
    Option<i64>,
    Option<String>,
    i64,
    Option<String>,
    Option<i64>,
    String,
);

/// Read the chain's marker without locking it.
pub(crate) async fn read(pool: &sqlx::PgPool, chain_id: &str) -> Result<FamilyMarker> {
    let row: Option<MarkerRow> = sqlx::query_as(
        "/* project:families.marker.read */ SELECT current_block_number, current_block_hash,
                extract(epoch FROM block_timestamp)::bigint, input_content_hash, sequence,
                interpret_input_content_hash, interpret_redo_attempt, state
         FROM project_family_marker WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| ProjectError::database("failed to read the family marker", error))?;
    Ok(row.map(marker_from_row).unwrap_or_default())
}

/// Create the chain's marker row when it is missing, then lock it for this transaction.
pub(crate) async fn lock(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
) -> Result<FamilyMarker> {
    sqlx::query(
        "/* project:families.marker.create */ INSERT INTO project_family_marker (chain_id, state)
         VALUES ($1, 'live') ON CONFLICT (chain_id) DO NOTHING",
    )
    .bind(chain_id)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to create the family marker", error))?;
    let row: MarkerRow = sqlx::query_as(
        "/* project:families.marker.lock */ SELECT current_block_number, current_block_hash,
                extract(epoch FROM block_timestamp)::bigint, input_content_hash, sequence,
                interpret_input_content_hash, interpret_redo_attempt, state
         FROM project_family_marker WHERE chain_id = $1 FOR UPDATE",
    )
    .bind(chain_id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to lock the family marker", error))?;
    Ok(marker_from_row(row))
}

fn marker_from_row(row: MarkerRow) -> FamilyMarker {
    let (
        number,
        hash,
        timestamp_seconds,
        input_content_hash,
        sequence,
        interpret_hash,
        attempt,
        state,
    ) = row;
    FamilyMarker {
        current: number
            .zip(hash)
            .map(|(number, hash)| Marker { number, hash }),
        sequence,
        timestamp_seconds,
        input_content_hash,
        interpret_input_content_hash: interpret_hash,
        interpret_redo_attempt: attempt,
        bootstrap: state == "bootstrap_pending",
    }
}

/// The compare-and-swap of a block: the locked marker must be the block's predecessor, and the
/// predecessor must be the block's parent on the readable lineage. `expected` is the predecessor
/// the driver planned for; `None` means the families were just reset.
pub(crate) fn require_predecessor(
    chain_id: &str,
    marker: &FamilyMarker,
    expected: Option<&Marker>,
    block: &BlockHeader,
    contiguous: bool,
) -> Result<()> {
    if marker.current.as_ref() != expected {
        return Err(ProjectError::transient(format!(
            "family marker for chain {chain_id} is {:?}, expected {:?} before block {}",
            marker.current, expected, block.number
        )));
    }
    if let (true, Some(expected)) = (contiguous, expected)
        && (expected.number + 1 != block.number
            || block.predecessor_hash.as_deref() != Some(expected.hash.as_str()))
    {
        return Err(ProjectError::transient(format!(
            "family block {} {} of chain {chain_id} does not follow the family marker {} {}",
            block.number, block.hash, expected.number, expected.hash
        )));
    }
    Ok(())
}

/// Write the marker: a block's advance, an undo's restore or a reset, each one sequence up.
pub(crate) async fn advance(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    next: &FamilyMarker,
) -> Result<()> {
    sqlx::query(
        "/* project:families.marker.advance */ UPDATE project_family_marker
         SET current_block_number = $2, current_block_hash = $3,
             block_timestamp = to_timestamp($4), input_content_hash = $5, sequence = $6,
             interpret_input_content_hash = $7, interpret_redo_attempt = $8,
             state = CASE WHEN $9 THEN 'bootstrap_pending' ELSE 'live' END
         WHERE chain_id = $1",
    )
    .bind(chain_id)
    .bind(next.current.as_ref().map(|marker| marker.number))
    .bind(next.current.as_ref().map(|marker| marker.hash.as_str()))
    .bind(next.timestamp_seconds.map(|seconds| seconds as f64))
    .bind(next.input_content_hash.as_deref())
    .bind(next.sequence)
    .bind(next.interpret_input_content_hash.as_deref())
    .bind(next.interpret_redo_attempt)
    .bind(next.bootstrap)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to advance the family marker", error))?;
    Ok(())
}

/// The locked marker's sequence, for a reset that clears everything else.
pub(crate) async fn read_locked_sequence(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
) -> Result<i64> {
    sqlx::query_scalar(
        "/* project:families.marker.read_locked_sequence */ SELECT sequence
         FROM project_family_marker WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to read the family marker sequence", error))
}
