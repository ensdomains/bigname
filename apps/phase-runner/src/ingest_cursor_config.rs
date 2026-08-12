use sqlx::PgPool;

use crate::{
    config::{SourceConfig, normalized_source_kind},
    error::{RunnerError, RunnerResult},
};

type StoredSourceConfig = (String, String, i64, bool);

pub(crate) async fn validate_existing(pool: &PgPool, sources: &[SourceConfig]) -> RunnerResult<()> {
    for source in sources {
        if let Some(stored) = load(pool, source).await? {
            validate_kind(source, &stored)?;
        }
    }
    Ok(())
}

pub(crate) async fn ensure(pool: &PgPool, source: &SourceConfig) -> RunnerResult<()> {
    sqlx::query(
        "INSERT INTO ingest_cursors (
             chain_id, source_key, source_kind, seed_basis,
             start_block_number, next_block_number
         )
         VALUES ($1, $2, $3, $4, $5, $5)
         ON CONFLICT (chain_id, source_key) DO UPDATE
         SET source_kind = EXCLUDED.source_kind,
             updated_at = now()
         WHERE ingest_cursors.last_processed_block_number IS NULL
           AND ingest_cursors.next_block_number = ingest_cursors.start_block_number
           AND ingest_cursors.seed_basis = EXCLUDED.seed_basis
           AND ingest_cursors.start_block_number = EXCLUDED.start_block_number",
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
    let stored = load(pool, source).await?.ok_or_else(|| {
        RunnerError::data_integrity(format!(
            "initialized ingest cursor {} for chain {} is missing",
            source.source_key, source.chain_id
        ))
    })?;
    validate(source, stored)
}

async fn load(pool: &PgPool, source: &SourceConfig) -> RunnerResult<Option<StoredSourceConfig>> {
    sqlx::query_as(
        "SELECT source_kind, seed_basis, start_block_number,
                last_processed_block_number IS NOT NULL
                    OR next_block_number > start_block_number
         FROM ingest_cursors
         WHERE chain_id = $1 AND source_key = $2",
    )
    .bind(&source.chain_id)
    .bind(&source.source_key)
    .fetch_optional(pool)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!(
                "failed to check ingest cursor {} for chain {}",
                source.source_key, source.chain_id
            ),
            error,
        )
    })
}

fn validate(source: &SourceConfig, stored: StoredSourceConfig) -> RunnerResult<()> {
    let (_, stored_seed, stored_start, _) = &stored;
    if stored_seed != source.seed_basis.as_str() || *stored_start != source.start_block_number {
        return Err(RunnerError::data_integrity(format!(
            "persisted ingest seed configuration for source {} on chain {} differs from runtime \
             configuration",
            source.source_key, source.chain_id
        )));
    }
    validate_kind(source, &stored)
}

fn validate_kind(source: &SourceConfig, stored: &StoredSourceConfig) -> RunnerResult<()> {
    let (stored_kind, _, _, has_progress) = stored;
    if *has_progress
        && normalized_source_kind(stored_kind) != normalized_source_kind(&source.source_kind)
    {
        return Err(RunnerError::data_integrity(format!(
            "persisted ingest source kind for source {} on chain {} differs from runtime \
             configuration after progress was recorded",
            source.source_key, source.chain_id
        )));
    }
    Ok(())
}
