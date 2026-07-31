use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use sqlx::PgPool;

use crate::{
    config::SourceConfig,
    error::{RunnerError, RunnerResult},
    heads::BlockMarker,
    phase::{IngestCursor, PhaseName, PhaseProgress, PhaseResume},
};

type StoredPhasePosition = (Option<i64>, Option<String>, Option<i64>, Option<String>);

pub(crate) async fn update_progress(
    pool: &PgPool,
    chain_id: &str,
    phase: PhaseName,
    progress: &PhaseProgress,
    status_assignment: &str,
) -> RunnerResult<()> {
    validate_progress(phase, progress, false)?;
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
        .execute(pool)
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
          AND phase_status IN ('idle', 'running', 'paused', 'failed')
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

pub(crate) async fn update_ingest_cursors(
    pool: &PgPool,
    sources: &[SourceConfig],
    progress: &PhaseProgress,
) -> RunnerResult<()> {
    crate::ingest_progress::validate(sources, progress, false)?;
    let sources_by_key = sources
        .iter()
        .map(|source| (source.source_key.as_str(), source))
        .collect::<BTreeMap<_, _>>();
    for source in sources {
        ensure_ingest_cursor(pool, source).await?;
    }
    if progress.source_progress.is_empty() {
        for source in sources {
            upsert_ingest_cursor(
                pool,
                source,
                progress.current.as_ref(),
                progress.target.as_ref(),
            )
            .await?;
        }
        return Ok(());
    }

    let mut seen = BTreeSet::new();
    for source_progress in &progress.source_progress {
        if !seen.insert(source_progress.source_key.as_str()) {
            return Err(RunnerError::data_integrity(format!(
                "ingest phase reported source {} more than once in one batch",
                source_progress.source_key
            )));
        }
        let source = sources_by_key
            .get(source_progress.source_key.as_str())
            .ok_or_else(|| {
                RunnerError::data_integrity(format!(
                    "ingest phase reported unconfigured source {}",
                    source_progress.source_key
                ))
            })?;
        upsert_ingest_cursor(
            pool,
            source,
            source_progress.current.as_ref(),
            source_progress.target.as_ref(),
        )
        .await?;
    }
    Ok(())
}

async fn ensure_ingest_cursor(pool: &PgPool, source: &SourceConfig) -> RunnerResult<()> {
    sqlx::query(
        "
        INSERT INTO ingest_cursors (
            chain_id,
            source_key,
            source_kind,
            seed_basis,
            start_block_number,
            next_block_number
        )
        VALUES ($1, $2, $3, $4, $5, $5)
        ON CONFLICT (chain_id, source_key) DO NOTHING
        ",
    )
    .bind(&source.chain_id)
    .bind(&source.source_key)
    .bind(&source.source_kind)
    .bind(source.seed_basis.as_str())
    .bind(source.start_block_number)
    .execute(pool)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!(
                "failed to initialize ingest cursor {} for chain {}",
                source.source_key, source.chain_id
            ),
            error,
        )
    })?;
    let stored: (String, i64) = sqlx::query_as(
        "
        SELECT seed_basis, start_block_number
        FROM ingest_cursors
        WHERE chain_id = $1
          AND source_key = $2
        ",
    )
    .bind(&source.chain_id)
    .bind(&source.source_key)
    .fetch_one(pool)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!(
                "failed to check ingest cursor {} for chain {}",
                source.source_key, source.chain_id
            ),
            error,
        )
    })?;
    if stored
        != (
            source.seed_basis.as_str().to_owned(),
            source.start_block_number,
        )
    {
        return Err(RunnerError::data_integrity(format!(
            "persisted ingest seed configuration for source {} on chain {} differs from runtime \
             configuration",
            source.source_key, source.chain_id
        )));
    }
    Ok(())
}

pub(crate) async fn upsert_ingest_cursor(
    pool: &PgPool,
    source: &SourceConfig,
    current: Option<&BlockMarker>,
    target: Option<&BlockMarker>,
) -> RunnerResult<()> {
    let processed = current.filter(|marker| marker.number >= source.start_block_number);
    let target = target.filter(|marker| marker.number >= source.start_block_number);
    if processed
        .zip(target)
        .is_some_and(|(current, target)| current.number > target.number)
    {
        return Err(RunnerError::data_integrity(format!(
            "ingest source {} current block is above its target",
            source.source_key
        )));
    }
    let next = processed
        .map(|marker| {
            marker
                .number
                .checked_add(1)
                .ok_or_else(|| RunnerError::data_integrity("ingest cursor block number overflowed"))
        })
        .transpose()?
        .unwrap_or(source.start_block_number);
    sqlx::query(
        "
        INSERT INTO ingest_cursors (
            chain_id,
            source_key,
            source_kind,
            seed_basis,
            start_block_number,
            next_block_number,
            target_block_number,
            last_processed_block_number,
            last_processed_block_hash
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (chain_id, source_key) DO UPDATE
        SET source_kind = EXCLUDED.source_kind,
            next_block_number = GREATEST(
                ingest_cursors.next_block_number,
                EXCLUDED.next_block_number
            ),
            target_block_number = EXCLUDED.target_block_number,
            last_processed_block_number = CASE
                WHEN ingest_cursors.last_processed_block_number IS NULL
                  OR EXCLUDED.last_processed_block_number
                     >= ingest_cursors.last_processed_block_number
                THEN EXCLUDED.last_processed_block_number
                ELSE ingest_cursors.last_processed_block_number
            END,
            last_processed_block_hash = CASE
                WHEN ingest_cursors.last_processed_block_number IS NULL
                  OR EXCLUDED.last_processed_block_number
                     >= ingest_cursors.last_processed_block_number
                THEN EXCLUDED.last_processed_block_hash
                ELSE ingest_cursors.last_processed_block_hash
            END,
            updated_at = now()
        ",
    )
    .bind(&source.chain_id)
    .bind(&source.source_key)
    .bind(&source.source_kind)
    .bind(source.seed_basis.as_str())
    .bind(source.start_block_number)
    .bind(next)
    .bind(target.map(|marker| marker.number))
    .bind(processed.map(|marker| marker.number))
    .bind(processed.map(|marker| marker.hash.as_str()))
    .execute(pool)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!(
                "failed to update ingest cursor {} for chain {}",
                source.source_key, source.chain_id
            ),
            error,
        )
    })?;
    Ok(())
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
                   target_block_hash
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
    let (current_number, current_hash, target_number, target_hash) = position.ok_or_else(|| {
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
               redo_target_block_hash
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
    let (current_number, current_hash, target_number, target_hash) = position.ok_or_else(|| {
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
        ingest_cursors: Arc::from(ingest_cursors),
    })
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
