//! The repair record: the durable description of the latest family undo-then-replay or rebuild
//! of a chain, written by the family loop and read by nothing outside it yet. Every transition is
//! made inside the transaction that does the work it describes and is fenced by the attempt and
//! state it expects, each read back from the locked row:
//!
//! - `begin_undo` opens a repair in state undoing, in its own transaction, before the first undo;
//! - the undo that reaches the pending undo target moves the record to replaying in its own
//!   commit, capturing the input revision the replay must keep (`start_replay`);
//! - a replay or rebuild block requires the state, the attempt and, for a replay, that revision;
//! - the block that publishes the target sets the record complete in the same commit;
//! - a rebuild's intent is written in the transaction that clears the families (`begin_rebuild`).
//!
//! Undo never rewrites the record's facts and never touches `chain_phase_state`.
use sqlx::{Postgres, Transaction};

use super::input::{InputToken, Revision};
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

impl State {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Undoing => "undoing",
            Self::Replaying => "replaying",
            Self::Rebuilding => "rebuilding",
            Self::Complete => "complete",
        }
    }
}

/// Every field of the record, read back so each transition can fence on what it planned from.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Record {
    pub(crate) attempt: i64,
    pub(crate) reason: String,
    pub(crate) trusted_base: Option<Marker>,
    pub(crate) replay_target: Marker,
    pub(crate) state: State,
    pub(crate) prefix_revision: Option<Revision>,
    pub(crate) invalidation_from: Option<i64>,
    pub(crate) pending_undo_target: Option<i64>,
    pub(crate) completed_sequence: Option<i64>,
    pub(crate) completed_marker: Option<Marker>,
    pub(crate) completed_input_hash: Option<String>,
}

impl Record {
    pub(crate) fn active(&self) -> bool {
        self.state != State::Complete
    }

    /// The lowest block whose undo rows an active repair may still need: the undo must reach
    /// the pending target, and a replaying prefix can be invalidated back to the trusted base.
    pub(crate) fn retention_floor(&self) -> Option<i64> {
        if !matches!(self.state, State::Undoing | State::Replaying) {
            return None;
        }
        [
            self.trusted_base.as_ref().map(|base| base.number),
            self.pending_undo_target,
        ]
        .into_iter()
        .flatten()
        .min()
        .map(|floor| floor + 1)
    }
}

#[derive(sqlx::FromRow)]
struct RecordRow {
    attempt: i64,
    reason: String,
    trusted_base_number: Option<i64>,
    trusted_base_hash: Option<String>,
    replay_target_number: i64,
    replay_target_hash: String,
    state: String,
    prefix_interpret_input_content_hash: Option<String>,
    prefix_interpret_redo_attempt: Option<i64>,
    prefix_recorded: bool,
    invalidation_from: Option<i64>,
    pending_undo_target: Option<i64>,
    completed_sequence: Option<i64>,
    completed_marker_number: Option<i64>,
    completed_marker_hash: Option<String>,
    completed_input_hash: Option<String>,
}

impl From<RecordRow> for Record {
    fn from(row: RecordRow) -> Self {
        let marker = |number: Option<i64>, hash: Option<String>| {
            number
                .zip(hash)
                .map(|(number, hash)| Marker { number, hash })
        };
        Self {
            attempt: row.attempt,
            reason: row.reason,
            trusted_base: marker(row.trusted_base_number, row.trusted_base_hash),
            replay_target: Marker {
                number: row.replay_target_number,
                hash: row.replay_target_hash,
            },
            state: match row.state.as_str() {
                "undoing" => State::Undoing,
                "replaying" => State::Replaying,
                "rebuilding" => State::Rebuilding,
                _ => State::Complete,
            },
            prefix_revision: row.prefix_recorded.then_some((
                row.prefix_interpret_input_content_hash,
                row.prefix_interpret_redo_attempt,
            )),
            invalidation_from: row.invalidation_from,
            pending_undo_target: row.pending_undo_target,
            completed_sequence: row.completed_sequence,
            completed_marker: marker(row.completed_marker_number, row.completed_marker_hash),
            completed_input_hash: row.completed_input_hash,
        }
    }
}

const RECORD_COLUMNS: &str = "attempt, reason, trusted_base_number, trusted_base_hash,
        replay_target_number, replay_target_hash, state, prefix_interpret_input_content_hash,
        prefix_interpret_redo_attempt, prefix_recorded, invalidation_from, pending_undo_target,
        completed_sequence, completed_marker_number, completed_marker_hash, completed_input_hash";

pub(crate) async fn read(pool: &sqlx::PgPool, chain_id: &str) -> Result<Option<Record>> {
    let row: Option<RecordRow> = sqlx::query_as(&format!(
        "/* project:families.repair.read */ SELECT {RECORD_COLUMNS}
         FROM project_repair_record WHERE chain_id = $1"
    ))
    .bind(chain_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| ProjectError::database("failed to read the repair record", error))?;
    Ok(row.map(Record::from))
}

/// Lock the record for the transaction and read every field back.
pub(crate) async fn lock(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
) -> Result<Option<Record>> {
    let row: Option<RecordRow> = sqlx::query_as(&format!(
        "/* project:families.repair.lock */ SELECT {RECORD_COLUMNS}
         FROM project_repair_record WHERE chain_id = $1 FOR UPDATE"
    ))
    .bind(chain_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to lock the repair record", error))?;
    Ok(row.map(Record::from))
}

fn fence_error(chain_id: &str, what: &str, locked: Option<&Record>) -> ProjectError {
    ProjectError::transient(format!(
        "repair record of chain {chain_id} is not {what}: {:?}",
        locked.map(|record| (record.attempt, record.state))
    ))
}

/// The record the transition planned from must still stand, every field of it.
pub(crate) fn require_unchanged(
    chain_id: &str,
    locked: Option<&Record>,
    planned: Option<&Record>,
) -> Result<()> {
    if locked != planned {
        return Err(fence_error(
            chain_id,
            "the one the loop planned from",
            locked,
        ));
    }
    Ok(())
}

/// The record must be in `state` under `attempt`.
pub(crate) fn require_state<'a>(
    chain_id: &str,
    locked: Option<&'a Record>,
    attempt: i64,
    state: State,
) -> Result<&'a Record> {
    locked
        .filter(|record| record.attempt == attempt && record.state == state)
        .ok_or_else(|| {
            fence_error(
                chain_id,
                &format!("{} under attempt {attempt}", state.as_str()),
                locked,
            )
        })
}

/// A block that follows the served marker requires no active repair.
pub(crate) fn require_idle(chain_id: &str, locked: Option<&Record>) -> Result<()> {
    if locked.is_some_and(Record::active) {
        return Err(fence_error(chain_id, "complete", locked));
    }
    Ok(())
}

/// What a new undo-then-replay records before its first undo.
pub(crate) struct NewRepair<'a> {
    pub(crate) attempt: i64,
    pub(crate) reason: Reason,
    pub(crate) trusted_base: Option<&'a Marker>,
    pub(crate) replay_target: &'a Marker,
    pub(crate) pending_undo_target: i64,
}

/// Open an undo-then-replay in state undoing.
pub(crate) async fn begin_undo(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    repair: &NewRepair<'_>,
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
             prefix_recorded = false, invalidation_from = NULL,
             pending_undo_target = EXCLUDED.pending_undo_target, completed_sequence = NULL,
             completed_marker_number = NULL, completed_marker_hash = NULL,
             completed_input_hash = NULL, updated_at = now()",
    )
    .bind(chain_id)
    .bind(repair.attempt)
    .bind(repair.reason.as_str())
    .bind(repair.trusted_base.map(|marker| marker.number))
    .bind(repair.trusted_base.map(|marker| marker.hash.as_str()))
    .bind(repair.replay_target.number)
    .bind(&repair.replay_target.hash)
    .bind(repair.pending_undo_target)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to begin the repair record", error))?;
    Ok(())
}

/// Lower the pending undo target of an undoing repair; never raises it.
pub(crate) async fn lower_undo_target(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: i64,
) -> Result<()> {
    sqlx::query(
        "/* project:families.repair.lower_undo_target */ UPDATE project_repair_record
         SET pending_undo_target = LEAST(pending_undo_target, $2),
             trusted_base_number = CASE WHEN trusted_base_number > $2 THEN NULL
                                        ELSE trusted_base_number END,
             trusted_base_hash = CASE WHEN trusted_base_number > $2 THEN NULL
                                      ELSE trusted_base_hash END,
             updated_at = now()
         WHERE chain_id = $1 AND state = 'undoing'",
    )
    .bind(chain_id)
    .bind(target)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to lower the undo target", error))?;
    Ok(())
}

/// Send a replaying repair back to undoing: its prefix no longer stands on the readable lineage,
/// or its input revision changed.
pub(crate) async fn reopen_undo(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: i64,
) -> Result<()> {
    sqlx::query(
        "/* project:families.repair.reopen_undo */ UPDATE project_repair_record
         SET state = 'undoing', pending_undo_target = $2,
             prefix_interpret_input_content_hash = NULL, prefix_interpret_redo_attempt = NULL,
             prefix_recorded = false,
             trusted_base_number = CASE WHEN trusted_base_number > $2 THEN NULL
                                        ELSE trusted_base_number END,
             trusted_base_hash = CASE WHEN trusted_base_number > $2 THEN NULL
                                      ELSE trusted_base_hash END,
             updated_at = now()
         WHERE chain_id = $1 AND state = 'replaying'",
    )
    .bind(chain_id)
    .bind(target)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to reopen the repair undo", error))?;
    Ok(())
}

/// The undo reached its target: replay starts from `base` against `revision`.
pub(crate) async fn start_replay(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    base: &Marker,
    revision: &Revision,
) -> Result<()> {
    sqlx::query(
        "/* project:families.repair.start_replay */ UPDATE project_repair_record
         SET state = 'replaying', pending_undo_target = NULL,
             trusted_base_number = COALESCE(trusted_base_number, $2),
             trusted_base_hash = CASE WHEN trusted_base_number IS NULL THEN $3
                                      ELSE trusted_base_hash END,
             prefix_interpret_input_content_hash = $4, prefix_interpret_redo_attempt = $5,
             prefix_recorded = true, updated_at = now()
         WHERE chain_id = $1 AND state = 'undoing'",
    )
    .bind(chain_id)
    .bind(base.number)
    .bind(&base.hash)
    .bind(revision.0.as_deref())
    .bind(revision.1)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to start the repair replay", error))?;
    Ok(())
}

/// Open a rebuild from nothing toward `target` in the transaction that clears the families.
pub(crate) async fn begin_rebuild(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    attempt: i64,
    reason: Reason,
    target: &Marker,
    revision: &Revision,
) -> Result<()> {
    sqlx::query(
        "/* project:families.repair.begin_rebuild */ INSERT INTO project_repair_record (
             chain_id, attempt, reason, replay_target_number, replay_target_hash, state,
             prefix_interpret_input_content_hash, prefix_interpret_redo_attempt,
             prefix_recorded, updated_at
         ) VALUES ($1, $2, $3, $4, $5, 'rebuilding', $6, $7, true, now())
         ON CONFLICT (chain_id) DO UPDATE SET
             attempt = EXCLUDED.attempt, reason = EXCLUDED.reason, trusted_base_number = NULL,
             trusted_base_hash = NULL, replay_target_number = EXCLUDED.replay_target_number,
             replay_target_hash = EXCLUDED.replay_target_hash, state = 'rebuilding',
             prefix_interpret_input_content_hash = EXCLUDED.prefix_interpret_input_content_hash,
             prefix_interpret_redo_attempt = EXCLUDED.prefix_interpret_redo_attempt,
             prefix_recorded = true, invalidation_from = NULL, pending_undo_target = NULL,
             completed_sequence = NULL, completed_marker_number = NULL,
             completed_marker_hash = NULL, completed_input_hash = NULL, updated_at = now()",
    )
    .bind(chain_id)
    .bind(attempt)
    .bind(reason.as_str())
    .bind(target.number)
    .bind(&target.hash)
    .bind(revision.0.as_deref())
    .bind(revision.1)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to begin the rebuild record", error))?;
    Ok(())
}

/// The replay or rebuild published the served target: record the completion identity in the
/// same commit as that publication.
pub(crate) async fn complete(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    marker: &Marker,
    sequence: i64,
    input_content_hash: &str,
) -> Result<()> {
    let updated = sqlx::query(
        "/* project:families.repair.complete */ UPDATE project_repair_record
         SET state = 'complete', pending_undo_target = NULL, completed_sequence = $2,
             completed_marker_number = $3, completed_marker_hash = $4,
             completed_input_hash = $5, replay_target_number = $3, replay_target_hash = $4,
             updated_at = now()
         WHERE chain_id = $1 AND state IN ('replaying', 'rebuilding')",
    )
    .bind(chain_id)
    .bind(sequence)
    .bind(marker.number)
    .bind(&marker.hash)
    .bind(input_content_hash)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to complete the repair record", error))?
    .rows_affected();
    if updated != 1 {
        return Err(fence_error(chain_id, "replaying or rebuilding", None));
    }
    Ok(())
}
