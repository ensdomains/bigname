use std::{collections::BTreeMap, sync::Arc};

use sqlx::{PgPool, Postgres, Transaction};

use crate::{
    error::{RunnerError, RunnerResult},
    heads::BlockMarker,
    phase::{
        IngestCursor, PhaseName, PhaseProgress, PhaseResume, RedoAttemptFence, RunMode,
        VerificationLevel,
    },
    state::PhaseStore,
};

type StoredPhasePosition = (
    Option<i64>,
    Option<String>,
    Option<i64>,
    Option<String>,
    Option<String>,
);

type StoredRedoPosition = (
    Option<i64>,
    Option<String>,
    Option<i64>,
    Option<String>,
    Option<String>,
    Option<serde_json::Value>,
);

impl PhaseStore {
    pub async fn record_progress(
        &self,
        chain_id: &str,
        phase: PhaseName,
        mode: &RunMode,
        redo_attempt: Option<RedoAttemptFence>,
        progress: &PhaseProgress,
    ) -> RunnerResult<()> {
        match (mode.is_redo(), redo_attempt) {
            (true, Some(attempt)) => {
                update_redo_progress(self.pool(), chain_id, phase, mode, attempt, progress).await
            }
            (true, None) => Err(RunnerError::data_integrity(format!(
                "redo progress is missing its attempt fence for chain {chain_id} phase {phase}"
            ))),
            (false, None) => {
                update_progress(self.pool(), chain_id, phase, progress, "updated_at = now()").await
            }
            (false, Some(_)) => Err(RunnerError::data_integrity(format!(
                "normal progress carries a redo attempt fence for chain {chain_id} phase {phase}"
            ))),
        }
    }
}

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
    mode: &RunMode,
    attempt: RedoAttemptFence,
    progress: &PhaseProgress,
) -> RunnerResult<()> {
    validate_progress(phase, progress, false)?;
    let loaded_boundaries = loaded_redo_boundaries(phase, progress)?;
    let expected_mode = match mode {
        RunMode::Redo(_) => "redo",
        RunMode::RecomputeFlags(_) => "recompute_flags",
        RunMode::Normal => {
            return Err(RunnerError::data_integrity(
                "normal mode cannot record redo progress",
            ));
        }
    };
    // Fence this pool-backed progress write to the exact redo begin. This closes the
    // redo-progress instance of https://github.com/ensdomains/bigname/issues/452;
    // holistic connection routing remains there.
    let query = format!(
        "
        UPDATE chain_phase_state
        SET redo_current_block_number = $3,
            redo_current_block_hash = $4,
            redo_target_block_number = $5,
            redo_target_block_hash = $6,
            redo_source_boundary_markers = CASE
                WHEN $7::jsonb IS NULL THEN redo_source_boundary_markers
                ELSE COALESCE(redo_source_boundary_markers, '{{}}'::jsonb) || $7::jsonb
            END,
            updated_at = now()
        WHERE chain_id = $1
          AND phase_name = $2
          AND redo_in_progress
          AND redo_attempt_generation = $8
          AND redo_mode = $9
          AND redo_from_block_number = $10
          AND redo_to_block_number = $11
          AND (
              phase_name != 'ingest'
              OR redo_manifest_authority_fingerprint = {}
          )
          AND (
              phase_name != 'ingest'
              OR last_error IS NULL
              OR last_error NOT LIKE $12
              OR last_error LIKE $13
          )
        ",
        crate::redo_manifest_authority::FINGERPRINT_SQL
    );
    let result = sqlx::query(&query)
        .bind(chain_id)
        .bind(phase.as_str())
        .bind(progress.current.as_ref().map(|marker| marker.number))
        .bind(progress.current.as_ref().map(|marker| marker.hash.as_str()))
        .bind(progress.target.as_ref().map(|marker| marker.number))
        .bind(progress.target.as_ref().map(|marker| marker.hash.as_str()))
        .bind(loaded_boundaries)
        .bind(attempt.generation)
        .bind(expected_mode)
        .bind(attempt.execution_range.from)
        .bind(attempt.execution_range.to)
        .bind(crate::redo_stamp::required_redo_owner_pattern())
        .bind(format!(
            "{}%",
            crate::redo_stamp::REQUIRED_REDO_ACTIVE_PREFIX
        ))
        .execute(pool)
        .await
        .map_err(|error| {
            RunnerError::database(
                format!("failed to record redo progress for chain {chain_id} phase {phase}"),
                error,
            )
        })?;
    if result.rows_affected() != 1 {
        return Err(RunnerError::redo_attempt_superseded(format!(
            "redo attempt superseded; progress not recorded for chain {chain_id} phase {phase} \
             generation {} mode {expected_mode} range {}..={}",
            attempt.generation, attempt.execution_range.from, attempt.execution_range.to
        )));
    }
    Ok(())
}

fn loaded_redo_boundaries(
    phase: PhaseName,
    progress: &PhaseProgress,
) -> RunnerResult<Option<serde_json::Value>> {
    let mut boundaries = serde_json::Map::new();
    for source in &progress.source_progress {
        let Some(marker) = &source.redo_loaded_boundary else {
            continue;
        };
        if phase != PhaseName::Ingest {
            return Err(RunnerError::data_integrity(format!(
                "phase {phase} reported an Ingest redo source boundary"
            )));
        }
        boundaries.insert(
            source.source_key.clone(),
            serde_json::json!({"number": marker.number, "hash": marker.hash}),
        );
    }
    Ok((!boundaries.is_empty()).then_some(serde_json::Value::Object(boundaries)))
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
        load_ingest_cursors(pool, chain_id, &BTreeMap::new()).await?
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
    let position: Option<StoredRedoPosition> = sqlx::query_as(
        "
        SELECT redo_current_block_number,
               redo_current_block_hash,
               redo_target_block_number,
               redo_target_block_hash,
               verification_level,
               redo_source_boundary_markers
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
    let (
        current_number,
        current_hash,
        target_number,
        target_hash,
        verification_level,
        redo_source_boundary_markers,
    ) = position.ok_or_else(|| {
        RunnerError::data_integrity(format!(
            "active redo state is missing for chain {chain_id} phase {phase}"
        ))
    })?;
    let redo_source_boundaries = parse_redo_source_boundaries(redo_source_boundary_markers)?;
    let ingest_cursors = if phase == PhaseName::Ingest {
        load_ingest_cursors(pool, chain_id, &redo_source_boundaries).await?
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

fn parse_redo_source_boundaries(
    value: Option<serde_json::Value>,
) -> RunnerResult<BTreeMap<String, BlockMarker>> {
    let Some(value) = value else {
        return Ok(BTreeMap::new());
    };
    let object = value.as_object().ok_or_else(|| {
        RunnerError::data_integrity("redo source boundary markers must be a JSON object")
    })?;
    let mut boundaries = BTreeMap::new();
    for (source_key, value) in object {
        let number = value.get("number").and_then(serde_json::Value::as_i64);
        let hash = value.get("hash").and_then(serde_json::Value::as_str);
        let marker = number.zip(hash).ok_or_else(|| {
            RunnerError::data_integrity(format!(
                "redo source boundary marker for source {source_key} is malformed"
            ))
        })?;
        boundaries.insert(source_key.clone(), BlockMarker::new(marker.0, marker.1)?);
    }
    Ok(boundaries)
}

async fn load_ingest_cursors(
    pool: &PgPool,
    chain_id: &str,
    redo_source_boundaries: &BTreeMap<String, BlockMarker>,
) -> RunnerResult<Vec<IngestCursor>> {
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
            |(source_key, next_block_number, target_block_number, number, hash)| {
                let redo_loaded_boundary = redo_source_boundaries.get(&source_key).cloned();
                IngestCursor {
                    source_key,
                    next_block_number,
                    target_block_number,
                    last_processed: marker_from_pair(number, hash),
                    redo_loaded_boundary,
                }
            },
        )
        .collect())
}

fn marker_from_pair(number: Option<i64>, hash: Option<String>) -> Option<BlockMarker> {
    number
        .zip(hash)
        .map(|(number, hash)| BlockMarker { number, hash })
}
