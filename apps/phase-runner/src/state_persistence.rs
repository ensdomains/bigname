use std::sync::Arc;

use sqlx::{PgPool, Postgres, Transaction};

use crate::{
    error::{RunnerError, RunnerResult},
    heads::BlockMarker,
    phase::{IngestCursor, PhaseName, PhaseProgress, PhaseResume, VerificationLevel},
};

type StoredPhasePosition = (
    Option<i64>,
    Option<String>,
    Option<i64>,
    Option<String>,
    Option<String>,
);

pub(crate) async fn update_progress(
    pool: &PgPool,
    chain_id: &str,
    phase: PhaseName,
    progress: &PhaseProgress,
    status_assignment: &str,
) -> RunnerResult<()> {
    validate_progress(phase, progress, false)?;
    let mut transaction = pool.begin().await.map_err(|error| {
        RunnerError::database(
            format!("failed to begin progress update for chain {chain_id} phase {phase}"),
            error,
        )
    })?;
    update_progress_in_transaction(
        &mut transaction,
        chain_id,
        phase,
        progress,
        status_assignment,
    )
    .await?;
    transaction.commit().await.map_err(|error| {
        RunnerError::database(
            format!("failed to commit progress for chain {chain_id} phase {phase}"),
            error,
        )
    })
}

pub(crate) async fn update_progress_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    phase: PhaseName,
    progress: &PhaseProgress,
    status_assignment: &str,
) -> RunnerResult<()> {
    let query = format!(
        "
        UPDATE chain_phase_state
        SET current_block_number = $3,
            current_block_hash = $4,
            target_block_number = $5,
            target_block_hash = $6,
            live_handoff_block_number = $7,
            live_handoff_block_hash = $8,
            verification_level = CASE WHEN phase_name = 'verify' THEN $9 ELSE NULL END,
            {status_assignment}
        WHERE chain_id = $1
          AND phase_name = $2
        "
    );
    sqlx::query(&query)
        .bind(chain_id)
        .bind(phase.as_str())
        .bind(progress.current.as_ref().map(|marker| marker.number))
        .bind(progress.current.as_ref().map(|marker| marker.hash.as_str()))
        .bind(progress.target.as_ref().map(|marker| marker.number))
        .bind(progress.target.as_ref().map(|marker| marker.hash.as_str()))
        .bind(
            (phase == PhaseName::Ingest)
                .then(|| progress.live_handoff.as_ref().map(|marker| marker.number))
                .flatten(),
        )
        .bind(
            (phase == PhaseName::Ingest)
                .then(|| {
                    progress
                        .live_handoff
                        .as_ref()
                        .map(|marker| marker.hash.as_str())
                })
                .flatten(),
        )
        .bind(progress.verification_level.map(|level| level.as_str()))
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            RunnerError::database(
                format!("failed to record progress for chain {chain_id} phase {phase}"),
                error,
            )
        })?;
    Ok(())
}

pub(crate) async fn update_redo_progress(
    pool: &PgPool,
    chain_id: &str,
    phase: PhaseName,
    progress: &PhaseProgress,
) -> RunnerResult<()> {
    validate_progress(phase, progress, false)?;
    let result = sqlx::query(
        "
        UPDATE chain_phase_state
        SET redo_current_block_number = $3,
            redo_current_block_hash = $4,
            redo_target_block_number = $5,
            redo_target_block_hash = $6,
            updated_at = now()
        WHERE chain_id = $1
          AND phase_name = $2
          AND redo_in_progress
        ",
    )
    .bind(chain_id)
    .bind(phase.as_str())
    .bind(progress.current.as_ref().map(|marker| marker.number))
    .bind(progress.current.as_ref().map(|marker| marker.hash.as_str()))
    .bind(progress.target.as_ref().map(|marker| marker.number))
    .bind(progress.target.as_ref().map(|marker| marker.hash.as_str()))
    .execute(pool)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!("failed to record redo progress for chain {chain_id} phase {phase}"),
            error,
        )
    })?;
    if result.rows_affected() != 1 {
        return Err(RunnerError::data_integrity(format!(
            "redo progress requires an active redo for chain {chain_id} phase {phase}"
        )));
    }
    Ok(())
}

pub(crate) fn validate_progress(
    phase: PhaseName,
    progress: &PhaseProgress,
    completing: bool,
) -> RunnerResult<()> {
    if phase != PhaseName::Verify && progress.verification_level.is_some() {
        return Err(RunnerError::data_integrity(format!(
            "phase {phase} reported a verification level"
        )));
    }
    if phase == PhaseName::Verify && completing && progress.verification_level.is_none() {
        return Err(RunnerError::data_integrity(
            "verify phase cannot complete without a verification level",
        ));
    }
    Ok(())
}

pub(crate) async fn load_redo_marker(
    pool: &PgPool,
    chain_id: &str,
    phase: PhaseName,
) -> RunnerResult<Option<(String, i64, i64)>> {
    sqlx::query_as(
        "
        SELECT redo_mode,
               redo_from_block_number,
               redo_to_block_number
        FROM chain_phase_state
        WHERE chain_id = $1
          AND phase_name = $2
          AND redo_in_progress
        ",
    )
    .bind(chain_id)
    .bind(phase.as_str())
    .fetch_optional(pool)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!("failed to load unfinished redo for chain {chain_id} phase {phase}"),
            error,
        )
    })
}

pub(crate) async fn record_live_verification_mismatch(
    pool: &PgPool,
    chain_id: &str,
    reason: &str,
) -> RunnerResult<bool> {
    let result = sqlx::query(
        "
        UPDATE chain_phase_state
        SET phase_status = 'failed',
            last_error = 'live phase stopped because verify reported a verification mismatch: '
                || $2,
            started_at = COALESCE(started_at, now()),
            finished_at = now(),
            updated_at = now()
        WHERE chain_id = $1
          AND phase_name = 'live'
          AND phase_status IN ('idle', 'running', 'paused', 'completed', 'failed')
          AND NOT redo_in_progress
        ",
    )
    .bind(chain_id)
    .bind(reason)
    .execute(pool)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!("failed to record live verification mismatch for chain {chain_id}"),
            error,
        )
    })?;
    Ok(result.rows_affected() == 1)
}

pub(crate) async fn load_phase_resume(
    pool: &PgPool,
    chain_id: &str,
    phase: PhaseName,
) -> RunnerResult<PhaseResume> {
    let position: Option<StoredPhasePosition> = sqlx::query_as(
        "
            SELECT current_block_number,
                   current_block_hash,
                   target_block_number,
                   target_block_hash,
                   verification_level
            FROM chain_phase_state
            WHERE chain_id = $1
              AND phase_name = $2
        ",
    )
    .bind(chain_id)
    .bind(phase.as_str())
    .fetch_optional(pool)
    .await
    .map_err(|error| {
        RunnerError::transient(format!(
            "failed to load resume position for chain {chain_id} phase {phase}: {error}"
        ))
    })?;
    let (current_number, current_hash, target_number, target_hash, verification_level) =
        position.ok_or_else(|| {
            RunnerError::data_integrity(format!(
                "phase state is missing for chain {chain_id} phase {phase}"
            ))
        })?;
    let current = marker_from_pair(current_number, current_hash);
    let target = marker_from_pair(target_number, target_hash);
    let ingest_cursors = if phase == PhaseName::Ingest {
        load_ingest_cursors(pool, chain_id).await?
    } else {
        Vec::new()
    };
    Ok(PhaseResume {
        current,
        target,
        verification_level: parse_verification_level(verification_level.as_deref())?,
        ingest_cursors: Arc::from(ingest_cursors),
    })
}

pub(crate) async fn load_redo_resume(
    pool: &PgPool,
    chain_id: &str,
    phase: PhaseName,
) -> RunnerResult<PhaseResume> {
    let position: Option<StoredPhasePosition> = sqlx::query_as(
        "
        SELECT redo_current_block_number,
               redo_current_block_hash,
               redo_target_block_number,
               redo_target_block_hash,
               verification_level
        FROM chain_phase_state
        WHERE chain_id = $1
          AND phase_name = $2
          AND redo_in_progress
        ",
    )
    .bind(chain_id)
    .bind(phase.as_str())
    .fetch_optional(pool)
    .await
    .map_err(|error| {
        RunnerError::transient(format!(
            "failed to load redo resume position for chain {chain_id} phase {phase}: {error}"
        ))
    })?;
    let (current_number, current_hash, target_number, target_hash, verification_level) =
        position.ok_or_else(|| {
            RunnerError::data_integrity(format!(
                "active redo state is missing for chain {chain_id} phase {phase}"
            ))
        })?;
    let ingest_cursors = if phase == PhaseName::Ingest {
        load_ingest_cursors(pool, chain_id).await?
    } else {
        Vec::new()
    };
    Ok(PhaseResume {
        current: marker_from_pair(current_number, current_hash),
        target: marker_from_pair(target_number, target_hash),
        verification_level: parse_verification_level(verification_level.as_deref())?,
        ingest_cursors: Arc::from(ingest_cursors),
    })
}

fn parse_verification_level(value: Option<&str>) -> RunnerResult<Option<VerificationLevel>> {
    value
        .map(|value| match value {
            "quick_synced" => Ok(VerificationLevel::QuickSynced),
            "cross_checked" => Ok(VerificationLevel::CrossChecked),
            "node_checked" => Ok(VerificationLevel::NodeChecked),
            value => Err(RunnerError::data_integrity(format!(
                "phase state contains unknown verification level {value:?}"
            ))),
        })
        .transpose()
}

async fn load_ingest_cursors(pool: &PgPool, chain_id: &str) -> RunnerResult<Vec<IngestCursor>> {
    let rows = sqlx::query_as::<_, (String, i64, Option<i64>, Option<i64>, Option<String>)>(
        "
        SELECT source_key,
               next_block_number,
               target_block_number,
               last_processed_block_number,
               last_processed_block_hash
        FROM ingest_cursors
        WHERE chain_id = $1
        ORDER BY source_key
        ",
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await
    .map_err(|error| {
        RunnerError::transient(format!(
            "failed to load ingest cursors for chain {chain_id}: {error}"
        ))
    })?;
    Ok(rows
        .into_iter()
        .map(
            |(source_key, next_block_number, target_block_number, number, hash)| IngestCursor {
                source_key,
                next_block_number,
                target_block_number,
                last_processed: marker_from_pair(number, hash),
            },
        )
        .collect())
}

fn marker_from_pair(number: Option<i64>, hash: Option<String>) -> Option<BlockMarker> {
    number
        .zip(hash)
        .map(|(number, hash)| BlockMarker { number, hash })
}
