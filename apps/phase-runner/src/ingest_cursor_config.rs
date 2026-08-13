use std::collections::BTreeSet;

use sqlx::PgPool;

use crate::{
    config::{SourceConfig, normalized_source_kind},
    error::{RunnerError, RunnerResult},
};

type StoredSourceConfig = (String, String, i64);

pub(crate) async fn ensure_all(
    pool: &PgPool,
    chain_id: &str,
    sources: &[SourceConfig],
) -> RunnerResult<()> {
    validate_persisted_source_keys(pool, chain_id, sources, true).await?;
    for source in sources {
        let stored = initialize(pool, source).await?;
        validate(source, stored)?;
    }
    validate_persisted_source_keys(pool, chain_id, sources, false).await?;
    Ok(())
}

pub(crate) async fn validate_completed(
    pool: &PgPool,
    chain_id: &str,
    sources: &[SourceConfig],
) -> RunnerResult<()> {
    for source in sources {
        let stored = load(pool, source).await?.ok_or_else(|| {
            RunnerError::data_integrity(format!(
                "completed Ingest source {} on chain {} has no matching cursor; retained ingest \
                 data requires an explicit reset before the source configuration can change",
                source.source_key, source.chain_id
            ))
        })?;
        validate(source, stored)?;
    }
    validate_persisted_source_keys(pool, chain_id, sources, false).await
}

pub(crate) async fn validate_existing(
    pool: &PgPool,
    chain_id: &str,
    sources: &[SourceConfig],
) -> RunnerResult<()> {
    validate_persisted_source_keys(pool, chain_id, sources, true).await?;
    for source in sources {
        if let Some(stored) = load(pool, source).await? {
            validate(source, stored)?;
        }
    }
    Ok(())
}

async fn validate_persisted_source_keys(
    pool: &PgPool,
    chain_id: &str,
    sources: &[SourceConfig],
    allow_initializing_subset: bool,
) -> RunnerResult<()> {
    let persisted: Vec<String> = sqlx::query_scalar(
        "SELECT source_key FROM ingest_cursors WHERE chain_id = $1 ORDER BY source_key",
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!("failed to load persisted ingest source keys for chain {chain_id}"),
            error,
        )
    })?;
    if allow_initializing_subset {
        if persisted.is_empty() {
            return Ok(());
        }
        let configured = sources
            .iter()
            .map(|source| source.source_key.as_str())
            .collect::<BTreeSet<_>>();
        if persisted
            .iter()
            .all(|source_key| configured.contains(source_key.as_str()))
            && !has_durable_ingest_data(pool, chain_id).await?
            && !has_progressed_ingest_cursor(pool, chain_id).await?
        {
            return Ok(());
        }
    }
    validate_source_keys(chain_id, sources, &persisted)
}

async fn has_progressed_ingest_cursor(pool: &PgPool, chain_id: &str) -> RunnerResult<bool> {
    sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM ingest_cursors
             WHERE chain_id = $1
               AND (next_block_number <> start_block_number
                    OR target_block_number IS NOT NULL
                    OR last_processed_block_number IS NOT NULL
                    OR last_processed_block_hash IS NOT NULL)
         )",
    )
    .bind(chain_id)
    .fetch_one(pool)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!("failed to check persisted ingest cursor progress for chain {chain_id}"),
            error,
        )
    })
}

pub(crate) fn validate_source_keys(
    chain_id: &str,
    sources: &[SourceConfig],
    persisted: &[String],
) -> RunnerResult<()> {
    let configured = sources
        .iter()
        .map(|source| source.source_key.as_str())
        .collect::<BTreeSet<_>>();
    let persisted = persisted
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if configured == persisted {
        return Ok(());
    }
    Err(RunnerError::data_integrity(format!(
        "persisted ingest source keys for chain {chain_id} differ from the configured source keys; \
         an explicit reset is required before source configuration can change"
    )))
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
         ) OR EXISTS (
             SELECT 1 FROM chain_lineage WHERE chain_id = $1
         ) OR EXISTS (
             SELECT 1 FROM chain_header_audit WHERE chain_id = $1
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
