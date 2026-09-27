//! The shadow marker: how far the owned-key families have been applied, with the generation every
//! family block and every family undo advances, and the input token and active manifest set key
//! the last block read inside its own transaction. It is not the served marker; the Project row of
//! `chain_phase_state` keeps that role until the families are read.
use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};

use super::input::{BlockHeader, InputToken, Revision};
use crate::{Marker, ProjectError, Result};

/// The input token as a block recorded it.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct RecordedToken {
    pub(crate) interpret_input_content_hash: Option<String>,
    pub(crate) interpret_redo_attempt: Option<i64>,
    pub(crate) interpret_redo_in_progress: Option<bool>,
    pub(crate) project_redo_attempt: Option<i64>,
    pub(crate) project_redo_mode: Option<String>,
    pub(crate) project_redo_from: Option<i64>,
    pub(crate) project_redo_to: Option<i64>,
}

impl RecordedToken {
    pub(crate) fn of(token: &InputToken) -> Self {
        Self {
            interpret_input_content_hash: token.interpret_input_content_hash.clone(),
            interpret_redo_attempt: token.interpret_redo_attempt_generation,
            interpret_redo_in_progress: Some(token.interpret_redo_in_progress),
            project_redo_attempt: Some(token.project_redo_attempt_generation),
            project_redo_mode: token.project_redo_mode.clone(),
            project_redo_from: token.project_redo_from,
            project_redo_to: token.project_redo_to,
        }
    }

    /// The input revision the block applied under; `None` when no block recorded one (a
    /// reset marker, or a marker written before the token was recorded per block).
    pub(crate) fn revision(&self) -> Option<Revision> {
        self.interpret_redo_in_progress.map(|_| {
            (
                self.interpret_input_content_hash.clone(),
                self.interpret_redo_attempt,
            )
        })
    }
}

/// The locked marker row of one chain.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct FamilyMarker {
    pub(crate) current: Option<Marker>,
    pub(crate) sequence: i64,
    pub(crate) timestamp_seconds: Option<i64>,
    pub(crate) input_content_hash: Option<String>,
    pub(crate) token: RecordedToken,
    pub(crate) admission_manifests: Option<String>,
    /// Whether a rebuild is still populating the families.
    pub(crate) bootstrap: bool,
}

impl FamilyMarker {
    /// The prior marker as the journal stores it under family `marker`.
    pub(crate) fn journal_image(&self) -> Value {
        let token = &self.token;
        json!({
            "current_block_number": self.current.as_ref().map(|marker| marker.number),
            "current_block_hash": self.current.as_ref().map(|marker| marker.hash.clone()),
            "block_timestamp_seconds": self.timestamp_seconds,
            "input_content_hash": self.input_content_hash,
            "interpret_input_content_hash": token.interpret_input_content_hash,
            "interpret_redo_attempt": token.interpret_redo_attempt,
            "interpret_redo_in_progress": token.interpret_redo_in_progress,
            "project_redo_attempt": token.project_redo_attempt,
            "project_redo_mode": token.project_redo_mode,
            "project_redo_from": token.project_redo_from,
            "project_redo_to": token.project_redo_to,
            "admission_manifests": self.admission_manifests,
            "bootstrap": self.bootstrap,
        })
    }

    pub(crate) fn from_journal_image(image: &Value, sequence: i64) -> Self {
        let number = image.get("current_block_number").and_then(Value::as_i64);
        let hash = text(image, "current_block_hash");
        let int = |field: &str| image.get(field).and_then(Value::as_i64);
        Self {
            current: number
                .zip(hash)
                .map(|(number, hash)| Marker { number, hash }),
            sequence,
            timestamp_seconds: int("block_timestamp_seconds"),
            input_content_hash: text(image, "input_content_hash"),
            token: RecordedToken {
                interpret_input_content_hash: text(image, "interpret_input_content_hash"),
                interpret_redo_attempt: int("interpret_redo_attempt"),
                interpret_redo_in_progress: image
                    .get("interpret_redo_in_progress")
                    .and_then(Value::as_bool),
                project_redo_attempt: int("project_redo_attempt"),
                project_redo_mode: text(image, "project_redo_mode"),
                project_redo_from: int("project_redo_from"),
                project_redo_to: int("project_redo_to"),
            },
            admission_manifests: text(image, "admission_manifests"),
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

#[derive(sqlx::FromRow)]
struct MarkerRow {
    current_block_number: Option<i64>,
    current_block_hash: Option<String>,
    timestamp_seconds: Option<i64>,
    input_content_hash: Option<String>,
    sequence: i64,
    interpret_input_content_hash: Option<String>,
    interpret_redo_attempt: Option<i64>,
    interpret_redo_in_progress: Option<bool>,
    project_redo_attempt: Option<i64>,
    project_redo_mode: Option<String>,
    project_redo_from: Option<i64>,
    project_redo_to: Option<i64>,
    admission_manifests: Option<String>,
    state: String,
}

impl From<MarkerRow> for FamilyMarker {
    fn from(row: MarkerRow) -> Self {
        Self {
            current: row
                .current_block_number
                .zip(row.current_block_hash)
                .map(|(number, hash)| Marker { number, hash }),
            sequence: row.sequence,
            timestamp_seconds: row.timestamp_seconds,
            input_content_hash: row.input_content_hash,
            token: RecordedToken {
                interpret_input_content_hash: row.interpret_input_content_hash,
                interpret_redo_attempt: row.interpret_redo_attempt,
                interpret_redo_in_progress: row.interpret_redo_in_progress,
                project_redo_attempt: row.project_redo_attempt,
                project_redo_mode: row.project_redo_mode,
                project_redo_from: row.project_redo_from,
                project_redo_to: row.project_redo_to,
            },
            admission_manifests: row.admission_manifests,
            bootstrap: row.state == "bootstrap_pending",
        }
    }
}

const MARKER_COLUMNS: &str = "current_block_number, current_block_hash,
        extract(epoch FROM block_timestamp)::bigint AS timestamp_seconds, input_content_hash,
        sequence, interpret_input_content_hash, interpret_redo_attempt,
        interpret_redo_in_progress, project_redo_attempt, project_redo_mode, project_redo_from,
        project_redo_to, admission_manifests, state";

/// Read the chain's marker without locking it.
pub(crate) async fn read(pool: &sqlx::PgPool, chain_id: &str) -> Result<FamilyMarker> {
    let row: Option<MarkerRow> = sqlx::query_as(&format!(
        "/* project:families.marker.read */ SELECT {MARKER_COLUMNS}
         FROM project_family_marker WHERE chain_id = $1"
    ))
    .bind(chain_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| ProjectError::database("failed to read the family marker", error))?;
    Ok(row.map(FamilyMarker::from).unwrap_or_default())
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
    let row: MarkerRow = sqlx::query_as(&format!(
        "/* project:families.marker.lock */ SELECT {MARKER_COLUMNS}
         FROM project_family_marker WHERE chain_id = $1 FOR UPDATE"
    ))
    .bind(chain_id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to lock the family marker", error))?;
    Ok(row.into())
}

/// The generation fence of every family transaction: the locked marker must be the one the
/// driver planned from, block and sequence both.
pub(crate) fn require(
    chain_id: &str,
    locked: &FamilyMarker,
    expected: Option<&Marker>,
    sequence: i64,
) -> Result<()> {
    if locked.current.as_ref() != expected || locked.sequence != sequence {
        return Err(ProjectError::transient(format!(
            "family marker for chain {chain_id} is {:?} at sequence {}, expected {:?} at \
             sequence {sequence}",
            locked.current, locked.sequence, expected
        )));
    }
    Ok(())
}

/// The compare-and-swap of a block: the locked marker must be the block's predecessor, and the
/// predecessor must be the block's parent on the readable lineage. `expected` is the predecessor
/// the driver planned for; `None` means the families were just reset.
pub(crate) fn require_predecessor(
    chain_id: &str,
    marker: &FamilyMarker,
    expected: Option<&Marker>,
    sequence: i64,
    block: &BlockHeader,
    contiguous: bool,
) -> Result<()> {
    require(chain_id, marker, expected, sequence)?;
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
    let token = &next.token;
    sqlx::query(
        "/* project:families.marker.advance */ UPDATE project_family_marker
         SET current_block_number = $2, current_block_hash = $3,
             block_timestamp = to_timestamp($4), input_content_hash = $5, sequence = $6,
             interpret_input_content_hash = $7, interpret_redo_attempt = $8,
             interpret_redo_in_progress = $9, project_redo_attempt = $10,
             project_redo_mode = $11, project_redo_from = $12, project_redo_to = $13,
             admission_manifests = $14,
             state = CASE WHEN $15 THEN 'bootstrap_pending' ELSE 'live' END
         WHERE chain_id = $1",
    )
    .bind(chain_id)
    .bind(next.current.as_ref().map(|marker| marker.number))
    .bind(next.current.as_ref().map(|marker| marker.hash.as_str()))
    .bind(next.timestamp_seconds.map(|seconds| seconds as f64))
    .bind(next.input_content_hash.as_deref())
    .bind(next.sequence)
    .bind(token.interpret_input_content_hash.as_deref())
    .bind(token.interpret_redo_attempt)
    .bind(token.interpret_redo_in_progress)
    .bind(token.project_redo_attempt)
    .bind(token.project_redo_mode.as_deref())
    .bind(token.project_redo_from)
    .bind(token.project_redo_to)
    .bind(next.admission_manifests.as_deref())
    .bind(next.bootstrap)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to advance the family marker", error))?;
    Ok(())
}
