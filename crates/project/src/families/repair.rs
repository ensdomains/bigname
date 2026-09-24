//! The repair record: the durable description of the latest family undo-then-replay or rebuild
//! of a chain, written by the family loop and read by nothing outside it yet. Each transition is
//! its own statement, so an interrupted repair is found where it stopped and resumed. Undo never
//! rewrites the record.
use sqlx::PgPool;

use super::{input::InputToken, marker::FamilyMarker};
use crate::{Marker, ProjectError, Result};

/// The owner prefix the runner writes into the Project row's `last_error` when it stamps a
/// required redo (apps/phase-runner/src/redo_stamp.rs, `REQUIRED_REDO_OWNER_PREFIX`).
const REQUIRED_REDO_OWNER_PREFIX: &str = "required downstream redo";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Reason {
    RequiredRedoRange,
    OrphanedLineage,
    ContentHashRebuild,
    OperatorRedo,
}

impl Reason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::RequiredRedoRange => "required_redo_range",
            Self::OrphanedLineage => "orphaned_lineage",
            Self::ContentHashRebuild => "content_hash_rebuild",
            Self::OperatorRedo => "operator_redo",
        }
    }

    /// A redo the runner stamped carries the required-redo owner prefix; any other is an
    /// operator redo.
    pub(crate) fn of_redo(token: &InputToken) -> Self {
        if token
            .project_last_error
            .as_deref()
            .is_some_and(|message| message.starts_with(REQUIRED_REDO_OWNER_PREFIX))
        {
            Self::RequiredRedoRange
        } else {
            Self::OperatorRedo
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum State {
    Undoing,
    Replaying,
    Rebuilding,
    Complete,
}

/// The fields the loop reads back.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Record {
    pub(crate) attempt: i64,
    pub(crate) state: State,
    pub(crate) pending_undo_target: Option<i64>,
    pub(crate) completed_marker: Option<Marker>,
}

pub(crate) async fn read(pool: &PgPool, chain_id: &str) -> Result<Option<Record>> {
    type Row = (i64, String, Option<i64>, Option<i64>, Option<String>);
    let row: Option<Row> = sqlx::query_as(
        "/* project:families.repair.read */ SELECT attempt, state, pending_undo_target,
                completed_marker_number, completed_marker_hash
         FROM project_repair_record WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| ProjectError::database("failed to read the repair record", error))?;
    Ok(row.map(|(attempt, state, pending, number, hash)| Record {
        attempt,
        state: match state.as_str() {
            "undoing" => State::Undoing,
            "replaying" => State::Replaying,
            "rebuilding" => State::Rebuilding,
            _ => State::Complete,
        },
        pending_undo_target: pending,
        completed_marker: number
            .zip(hash)
            .map(|(number, hash)| Marker { number, hash }),
    }))
}

/// Start an undo-then-replay toward `undo_target`, replaying afterwards to `replay_target`.
/// `trusted_base` is the block the undo stops on when the caller knows it (a redo range's
/// predecessor); an orphaned marker finds it by undoing and records it when replay starts.
pub(crate) async fn begin_undo(
    pool: &PgPool,
    chain_id: &str,
    reason: Reason,
    token: &InputToken,
    trusted_base: Option<&Marker>,
    undo_target: i64,
    replay_target: &Marker,
) -> Result<()> {
    sqlx::query(
        "/* project:families.repair.begin_undo */ INSERT INTO project_repair_record (
             chain_id, attempt, reason, trusted_base_number, trusted_base_hash,
             replay_target_number, replay_target_hash, state, pending_undo_target, updated_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, 'undoing', $8, now())
         ON CONFLICT (chain_id) DO UPDATE SET
             attempt = EXCLUDED.attempt, reason = EXCLUDED.reason,
             trusted_base_number = EXCLUDED.trusted_base_number,
             trusted_base_hash = EXCLUDED.trusted_base_hash,
             replay_target_number = EXCLUDED.replay_target_number,
             replay_target_hash = EXCLUDED.replay_target_hash, state = 'undoing',
             prefix_interpret_input_content_hash = NULL, prefix_interpret_redo_attempt = NULL,
             invalidation_from = NULL, pending_undo_target = EXCLUDED.pending_undo_target,
             completed_sequence = NULL, completed_marker_number = NULL,
             completed_marker_hash = NULL, completed_input_hash = NULL, updated_at = now()",
    )
    .bind(chain_id)
    .bind(token.project_redo_attempt_generation)
    .bind(reason.as_str())
    .bind(trusted_base.map(|marker| marker.number))
    .bind(trusted_base.map(|marker| marker.hash.as_str()))
    .bind(replay_target.number)
    .bind(&replay_target.hash)
    .bind(undo_target)
    .execute(pool)
    .await
    .map_err(|error| ProjectError::database("failed to begin the repair record", error))?;
    Ok(())
}

/// Start a rebuild from nothing toward `target`: no trusted base and nothing to undo.
pub(crate) async fn begin_rebuild(
    pool: &PgPool,
    chain_id: &str,
    reason: Reason,
    token: &InputToken,
    target: &Marker,
) -> Result<()> {
    let (hash, attempt) = token.revision();
    sqlx::query(
        "/* project:families.repair.begin_rebuild */ INSERT INTO project_repair_record (
             chain_id, attempt, reason, replay_target_number, replay_target_hash, state,
             prefix_interpret_input_content_hash, prefix_interpret_redo_attempt, updated_at
         ) VALUES ($1, $2, $3, $4, $5, 'rebuilding', $6, $7, now())
         ON CONFLICT (chain_id) DO UPDATE SET
             attempt = EXCLUDED.attempt, reason = EXCLUDED.reason, trusted_base_number = NULL,
             trusted_base_hash = NULL, replay_target_number = EXCLUDED.replay_target_number,
             replay_target_hash = EXCLUDED.replay_target_hash, state = 'rebuilding',
             prefix_interpret_input_content_hash = EXCLUDED.prefix_interpret_input_content_hash,
             prefix_interpret_redo_attempt = EXCLUDED.prefix_interpret_redo_attempt,
             invalidation_from = NULL, pending_undo_target = NULL, completed_sequence = NULL,
             completed_marker_number = NULL, completed_marker_hash = NULL,
             completed_input_hash = NULL, updated_at = now()",
    )
    .bind(chain_id)
    .bind(token.project_redo_attempt_generation)
    .bind(reason.as_str())
    .bind(target.number)
    .bind(&target.hash)
    .bind(hash)
    .bind(attempt)
    .execute(pool)
    .await
    .map_err(|error| ProjectError::database("failed to begin the rebuild record", error))?;
    Ok(())
}

/// The undo reached its base: replay starts from `base` against the input revision of `token`.
pub(crate) async fn start_replay(
    pool: &PgPool,
    chain_id: &str,
    token: &InputToken,
    base: &Marker,
) -> Result<()> {
    let (hash, attempt) = token.revision();
    sqlx::query(
        "/* project:families.repair.start_replay */ UPDATE project_repair_record
         SET state = 'replaying', pending_undo_target = NULL, trusted_base_number = $2,
             trusted_base_hash = $3, prefix_interpret_input_content_hash = $4,
             prefix_interpret_redo_attempt = $5, updated_at = now()
         WHERE chain_id = $1 AND state = 'undoing'",
    )
    .bind(chain_id)
    .bind(base.number)
    .bind(&base.hash)
    .bind(hash)
    .bind(attempt)
    .execute(pool)
    .await
    .map_err(|error| ProjectError::database("failed to start the repair replay", error))?;
    Ok(())
}

/// The replay or rebuild reached the served target: record the completion identity.
pub(crate) async fn complete(
    pool: &PgPool,
    chain_id: &str,
    marker: &FamilyMarker,
    input_content_hash: &str,
) -> Result<()> {
    let Some(current) = &marker.current else {
        return Ok(());
    };
    sqlx::query(
        "/* project:families.repair.complete */ UPDATE project_repair_record
         SET state = 'complete', pending_undo_target = NULL, completed_sequence = $2,
             completed_marker_number = $3, completed_marker_hash = $4,
             completed_input_hash = $5, replay_target_number = $3, replay_target_hash = $4,
             updated_at = now()
         WHERE chain_id = $1 AND state IN ('replaying', 'rebuilding')",
    )
    .bind(chain_id)
    .bind(marker.sequence)
    .bind(current.number)
    .bind(&current.hash)
    .bind(input_content_hash)
    .execute(pool)
    .await
    .map_err(|error| ProjectError::database("failed to complete the repair record", error))?;
    Ok(())
}
