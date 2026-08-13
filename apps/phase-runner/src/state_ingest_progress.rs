use std::collections::{BTreeMap, BTreeSet};

use sqlx::{PgPool, Postgres, Transaction};

use crate::{
    config::SourceConfig,
    error::{RunnerError, RunnerResult},
    heads::BlockMarker,
    phase::{BlockRange, PhaseName, PhaseProgress},
    state_persistence::{update_progress_in_transaction, validate_progress},
};

pub(crate) async fn update_ingest_progress(
    pool: &PgPool,
    chain_id: &str,
    sources: &[SourceConfig],
    progress: &PhaseProgress,
) -> RunnerResult<()> {
    validate_progress(PhaseName::Ingest, progress, false)?;
    crate::ingest_progress::validate(sources, progress, false)?;
    let mut transaction = pool.begin().await.map_err(|error| {
        RunnerError::database(
            format!("failed to begin ingest progress update for chain {chain_id}"),
            error,
        )
    })?;
    update_progress_in_transaction(
        &mut transaction,
        chain_id,
        PhaseName::Ingest,
        progress,
        "updated_at = now()",
    )
    .await?;
    update_ingest_cursors_in_transaction(&mut transaction, sources, progress).await?;
    transaction.commit().await.map_err(|error| {
        RunnerError::database(
            format!("failed to commit ingest progress for chain {chain_id}"),
            error,
        )
    })
}

pub(crate) async fn update_ingest_cursors(
    pool: &PgPool,
    sources: &[SourceConfig],
    progress: &PhaseProgress,
) -> RunnerResult<()> {
    crate::ingest_progress::validate(sources, progress, false)?;
    let mut transaction = pool
        .begin()
        .await
        .map_err(|error| RunnerError::database("failed to begin ingest cursor update", error))?;
    update_ingest_cursors_in_transaction(&mut transaction, sources, progress).await?;
    transaction
        .commit()
        .await
        .map_err(|error| RunnerError::database("failed to commit ingest cursor update", error))
}

async fn update_ingest_cursors_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    sources: &[SourceConfig],
    progress: &PhaseProgress,
) -> RunnerResult<()> {
    let sources_by_key = sources
        .iter()
        .map(|source| (source.source_key.as_str(), source))
        .collect::<BTreeMap<_, _>>();
    for source in sources {
        ensure_ingest_cursor(transaction, source).await?;
    }
    if progress.source_progress.is_empty() {
        for source in sources {
            upsert_ingest_cursor(
                transaction,
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
            transaction,
            source,
            source_progress.current.as_ref(),
            source_progress.target.as_ref(),
        )
        .await?;
    }
    Ok(())
}

pub(crate) async fn reconcile_redo_boundary_cursors(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    range: BlockRange,
    progress: &PhaseProgress,
) -> RunnerResult<()> {
    for source in &progress.source_progress {
        let (Some(current), Some(target)) = (&source.current, &source.target) else {
            continue;
        };
        if current != target || current.number < range.from || current.number > range.to {
            continue;
        }
        sqlx::query(
            "UPDATE ingest_cursors
             SET last_processed_block_hash = $4,
                 updated_at = now()
             WHERE chain_id = $1
               AND source_key = $2
               AND last_processed_block_number = $3",
        )
        .bind(chain_id)
        .bind(&source.source_key)
        .bind(current.number)
        .bind(&current.hash)
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            RunnerError::database(
                format!(
                    "failed to reconcile ingest cursor {} after redo for chain {chain_id}",
                    source.source_key
                ),
                error,
            )
        })?;
    }
    Ok(())
}

async fn ensure_ingest_cursor(
    transaction: &mut Transaction<'_, Postgres>,
    source: &SourceConfig,
) -> RunnerResult<()> {
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
    .execute(&mut **transaction)
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
    .fetch_one(&mut **transaction)
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

async fn upsert_ingest_cursor(
    transaction: &mut Transaction<'_, Postgres>,
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
    .execute(&mut **transaction)
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
