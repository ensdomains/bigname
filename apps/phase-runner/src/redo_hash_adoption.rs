//! The range a phase must redo to adopt this binary's interpreter content hash, and the Project
//! stamp an Interpret redo leaves so that range is what the follow-on Project redo runs.
use sqlx::{Postgres, Transaction};

use crate::{
    error::{ErrorKind, RunnerError, RunnerResult},
    phase::{BlockRange, PhaseName, RunMode},
};

/// The range a hash-adopting redo of `phase` must cover: from the first ingested block to the
/// completed Ingest handoff, extended to the phase's recorded head for Project and for an
/// interrupted redo. `None` when Ingest has no completed bounds.
pub(crate) async fn full_hash_range(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    phase: PhaseName,
    recorded_head: Option<i64>,
    interrupted_redo: bool,
) -> RunnerResult<Option<BlockRange>> {
    let bounds: (Option<i64>, Option<i64>) = sqlx::query_as(
        "
        SELECT
            (SELECT min(start_block_number)
             FROM ingest_cursors
             WHERE chain_id = $1),
            (SELECT live_handoff_block_number
             FROM chain_phase_state
             WHERE chain_id = $1
               AND phase_name = 'ingest'
               AND phase_status = 'completed')
        ",
    )
    .bind(chain_id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!("failed to load full redo bounds for chain {chain_id}"),
            error,
        )
    })?;
    let (Some(from), Some(mut to)) = bounds else {
        return Ok(None);
    };
    if phase == PhaseName::Project || interrupted_redo {
        to = to.max(recorded_head.unwrap_or(to));
    }
    BlockRange::new(from, to).map(Some)
}

pub(crate) async fn require_full_hash_redo(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    phase: PhaseName,
    mode: &RunMode,
    recorded_head: Option<i64>,
    interrupted_redo: bool,
) -> RunnerResult<()> {
    let Some(full) = full_hash_range(
        transaction,
        chain_id,
        phase,
        recorded_head,
        interrupted_redo,
    )
    .await?
    else {
        return Err(RunnerError::new(
            ErrorKind::ContentHashMismatch,
            format!(
                "cannot adopt a new interpretation-input hash for chain {chain_id} phase {phase}: \
                 completed ingest bounds are missing"
            ),
        ));
    };
    let Some(range) = mode.range() else {
        return Err(RunnerError::data_integrity(
            "hash adoption requires an explicit redo range",
        ));
    };
    if range != full {
        return Err(RunnerError::new(
            ErrorKind::ContentHashMismatch,
            format!(
                "cannot adopt a new interpretation-input hash for chain {chain_id} phase {phase} \
                 with range {}..={}; redo the full range {}..={}",
                range.from, range.to, full.from, full.to
            ),
        ));
    }
    Ok(())
}

/// Stamp the Project redo a completed Interpret redo requires. While Project still records
/// another hash, its redo must adopt this one, which needs the full range: that can reach past
/// Project's own head up to the Ingest handoff (Project behind Interpret when the release landed),
/// so the stamp is not clipped to Project's head. Otherwise the stamp covers `range` up to
/// Project's head, as before.
pub(crate) async fn stamp_project_after_interpret(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    range: BlockRange,
) -> RunnerResult<bool> {
    const REASON: &str = "interpret redo completed";
    let project: Option<(Option<i64>, Option<String>)> = sqlx::query_as(
        "SELECT current_block_number, input_content_hash
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'project'
         FOR UPDATE",
    )
    .bind(chain_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!("failed to lock the Project row of chain {chain_id} for its redo stamp"),
            error,
        )
    })?;
    if let Some((Some(head), hash)) = project
        && hash.as_deref() != Some(bigname_content_hash::INTERPRETER_CONTENT_HASH)
        && let Some(full) =
            full_hash_range(transaction, chain_id, PhaseName::Project, Some(head), false).await?
    {
        return crate::redo_stamp::stamp_unclipped_in_transaction(
            transaction,
            chain_id,
            PhaseName::Project,
            full,
            REASON,
        )
        .await;
    }
    crate::redo_stamp::stamp_required_in_transaction(
        transaction,
        chain_id,
        PhaseName::Project,
        range,
        REASON,
    )
    .await
}
