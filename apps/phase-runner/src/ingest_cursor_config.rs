use sqlx::PgPool;

use crate::{
    config::{SourceConfig, normalized_source_kind},
    error::{RunnerError, RunnerResult},
};

type StoredSourceConfig = (String, String, i64);

pub(crate) async fn ensure_all(pool: &PgPool, sources: &[SourceConfig]) -> RunnerResult<()> {
    for source in sources {
        let stored = initialize(pool, source).await?;
        validate(source, stored)?;
    }
    Ok(())
}

pub(crate) async fn validate_existing_kinds(
    pool: &PgPool,
    sources: &[SourceConfig],
) -> RunnerResult<()> {
    for source in sources {
        if let Some(stored) = load(pool, source).await? {
            validate_kind(source, &stored)?;
        }
    }
    Ok(())
}

pub(crate) async fn validate_existing(pool: &PgPool, sources: &[SourceConfig]) -> RunnerResult<()> {
    for source in sources {
        if let Some(stored) = load(pool, source).await? {
            validate(source, stored)?;
        }
    }
    Ok(())
}

pub(crate) async fn ensure(pool: &PgPool, source: &SourceConfig) -> RunnerResult<()> {
    let stored = initialize(pool, source).await?;
    validate(source, stored)
}

async fn initialize(pool: &PgPool, source: &SourceConfig) -> RunnerResult<StoredSourceConfig> {
    if let Some(stored) = load(pool, source).await? {
        return Ok(stored);
    }
    if has_durable_ingest_data(pool, &source.chain_id).await? {
        return Err(RunnerError::data_integrity(format!(
            "cannot initialize ingest source {} on chain {} because durable ingest data already \
             exists without a matching cursor; an explicit reset is required before Ingest can \
             run",
            source.source_key, source.chain_id
        )));
    }
    sqlx::query(
        "INSERT INTO ingest_cursors (
             chain_id, source_key, source_kind, seed_basis,
             start_block_number, next_block_number
         )
         VALUES ($1, $2, $3, $4, $5, $5)
         ON CONFLICT (chain_id, source_key) DO NOTHING",
    )
    .bind(&source.chain_id)
    .bind(&source.source_key)
    .bind(normalized_source_kind(&source.source_kind))
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
    load(pool, source).await?.ok_or_else(|| {
        RunnerError::data_integrity(format!(
            "initialized ingest cursor {} for chain {} is missing",
            source.source_key, source.chain_id
        ))
    })
}

async fn has_durable_ingest_data(pool: &PgPool, chain_id: &str) -> RunnerResult<bool> {
    sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM raw_transactions WHERE chain_id = $1
         ) OR EXISTS (
             SELECT 1 FROM raw_receipts WHERE chain_id = $1
         ) OR EXISTS (
             SELECT 1 FROM raw_logs WHERE chain_id = $1
         )",
    )
    .bind(chain_id)
    .fetch_one(pool)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!("failed to check durable ingest data for chain {chain_id}"),
            error,
        )
    })
}

async fn load(pool: &PgPool, source: &SourceConfig) -> RunnerResult<Option<StoredSourceConfig>> {
    sqlx::query_as(
        "SELECT source_kind, seed_basis, start_block_number
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
    let (_, stored_seed, stored_start) = &stored;
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
    let (stored_kind, _, _) = stored;
    if normalized_source_kind(stored_kind) != normalized_source_kind(&source.source_kind) {
        return Err(RunnerError::data_integrity(format!(
            "persisted ingest source kind for source {} on chain {} differs from runtime \
             configuration; source kind changes require an explicit reset and full source \
             re-walk (docs/glossary.md#re-derivation-boundary)",
            source.source_key, source.chain_id
        )));
    }
    Ok(())
}
