use sqlx::{PgPool, Postgres, Transaction};

use crate::{
    error::{RunnerError, RunnerResult},
    phase::{BlockRange, PhaseName},
};

pub(crate) const REQUIRED_REDO_PREFIX: &str = "required downstream redo: ";
pub(crate) const REQUIRED_REDO_ACTIVE_PREFIX: &str = "required downstream redo active: ";
const REQUIRED_REDO_OWNER_PREFIX: &str = "required downstream redo";

pub(crate) fn owns_required_redo(message: &str) -> bool {
    message.starts_with(REQUIRED_REDO_OWNER_PREFIX)
}

pub(crate) fn required_redo_owner_pattern() -> String {
    format!("{REQUIRED_REDO_OWNER_PREFIX}%")
}

type RedoStampRow = (Option<i64>, bool, Option<i64>, Option<i64>);

pub(crate) async fn required_range(
    pool: &PgPool,
    chain_id: &str,
    phase: PhaseName,
) -> RunnerResult<Option<BlockRange>> {
    let row: Option<(i64, i64)> = sqlx::query_as(
        "SELECT redo_from_block_number, redo_to_block_number
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = $2 AND redo_in_progress
           AND last_error LIKE $3",
    )
    .bind(chain_id)
    .bind(phase.as_str())
    .bind(required_redo_owner_pattern())
    .fetch_optional(pool)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!("failed to load required redo for chain {chain_id} phase {phase}"),
            error,
        )
    })?;
    row.map(|(from, to)| BlockRange::new(from, to)).transpose()
}

pub(crate) async fn stamp_orphaned_suffix(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    from: i64,
) -> RunnerResult<()> {
    let range = BlockRange::new(from, i64::MAX)?;
    for phase in [PhaseName::Interpret, PhaseName::Project] {
        stamp_required_in_transaction(
            transaction,
            chain_id,
            phase,
            range,
            "canonical head publication orphaned previously readable blocks",
        )
        .await?;
    }
    Ok(())
}

pub(crate) async fn stamp_required_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    phase: PhaseName,
    requested: BlockRange,
    reason: &str,
) -> RunnerResult<bool> {
    if !matches!(phase, PhaseName::Interpret | PhaseName::Project) {
        return Err(RunnerError::data_integrity(format!(
            "required downstream redo cannot target phase {phase}"
        )));
    }
    let row: Option<RedoStampRow> = sqlx::query_as(
        "SELECT current_block_number, redo_in_progress,
                    redo_from_block_number, redo_to_block_number
             FROM chain_phase_state
             WHERE chain_id = $1 AND phase_name = $2
             FOR UPDATE",
    )
    .bind(chain_id)
    .bind(phase.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!("failed to lock redo stamp for chain {chain_id} phase {phase}"),
            error,
        )
    })?;
    let Some((Some(current), active, active_from, active_to)) = row else {
        return Ok(false);
    };
    if current < requested.from {
        return Ok(false);
    }
    let through = current.min(requested.to);
    if active {
        extend_active(
            transaction,
            chain_id,
            phase,
            requested.from,
            through,
            active_from,
            active_to,
        )
        .await?;
        return Ok(true);
    }
    create_stamp(
        transaction,
        chain_id,
        phase,
        requested.from,
        through,
        reason,
    )
    .await
}

async fn extend_active(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    phase: PhaseName,
    requested_from: i64,
    through: i64,
    active_from: Option<i64>,
    active_to: Option<i64>,
) -> RunnerResult<()> {
    let from = active_from
        .ok_or_else(|| RunnerError::data_integrity("active redo is missing its start block"))?
        .min(requested_from);
    let to = active_to
        .ok_or_else(|| RunnerError::data_integrity("active redo is missing its end block"))?
        .max(through);
    sqlx::query(
        "UPDATE chain_phase_state
         SET redo_from_block_number = $3, redo_to_block_number = $4,
             redo_current_block_number = NULL, redo_current_block_hash = NULL,
             redo_target_block_number = NULL, redo_target_block_hash = NULL,
             updated_at = now()
         WHERE chain_id = $1 AND phase_name = $2 AND redo_in_progress",
    )
    .bind(chain_id)
    .bind(phase.as_str())
    .bind(from)
    .bind(to)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!("failed to extend redo stamp for chain {chain_id} phase {phase}"),
            error,
        )
    })?;
    Ok(())
}

async fn create_stamp(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    phase: PhaseName,
    from: i64,
    to: i64,
    reason: &str,
) -> RunnerResult<bool> {
    let result = sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'running', redo_in_progress = true, redo_mode = 'redo',
             redo_previous_phase_status = phase_status,
             redo_previous_last_error = last_error,
             redo_previous_started_at = started_at,
             redo_previous_finished_at = finished_at,
             redo_from_block_number = $3, redo_to_block_number = $4,
             redo_current_block_number = NULL, redo_current_block_hash = NULL,
             redo_target_block_number = NULL, redo_target_block_hash = NULL,
             last_error = $5, started_at = now(), finished_at = NULL, updated_at = now()
         WHERE chain_id = $1 AND phase_name = $2
           AND current_block_number >= $3 AND NOT redo_in_progress",
    )
    .bind(chain_id)
    .bind(phase.as_str())
    .bind(from)
    .bind(to)
    .bind(format!("{REQUIRED_REDO_PREFIX}{reason}"))
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!("failed to stamp required redo for chain {chain_id} phase {phase}"),
            error,
        )
    })?;
    Ok(result.rows_affected() == 1)
}
