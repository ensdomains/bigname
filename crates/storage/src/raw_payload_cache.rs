use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::types::time::OffsetDateTime;
use sqlx::{Executor, PgPool, Postgres, Row, postgres::PgRow};

use crate::CanonicalityState;

/// Persisted metadata for an evictable block-scoped raw payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawPayloadCacheMetadata {
    pub raw_payload_cache_metadata_id: i64,
    pub chain_id: String,
    pub block_hash: String,
    pub payload_kind: String,
    pub digest_algorithm: Option<String>,
    pub retained_digest: Option<String>,
    pub block_number: Option<i64>,
    pub payload_size_bytes: i64,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub cache_metadata: Value,
    pub canonicality_state: CanonicalityState,
    pub first_observed_at: OffsetDateTime,
    pub last_observed_at: OffsetDateTime,
}

/// Insert contract for evictable raw payload-cache metadata. The corresponding
/// payload bytes are intentionally not part of this storage boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawPayloadCacheMetadataUpsert {
    pub chain_id: String,
    pub block_hash: String,
    pub payload_kind: String,
    pub digest_algorithm: Option<String>,
    pub retained_digest: Option<String>,
    pub block_number: Option<i64>,
    pub payload_size_bytes: i64,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub cache_metadata: Value,
    pub canonicality_state: CanonicalityState,
}

/// Candidate digest material for a block-scoped payload cache-fill check.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawPayloadCacheDigestVerification {
    pub chain_id: String,
    pub block_hash: String,
    pub payload_kind: String,
    pub digest_algorithm: String,
    pub candidate_digest: String,
    pub payload_size_bytes: i64,
}

/// Insert missing metadata rows or refresh canonicality for already observed
/// payload identities. Immutable metadata must match the stored row.
pub async fn upsert_raw_payload_cache_metadata(
    pool: &PgPool,
    entries: &[RawPayloadCacheMetadataUpsert],
) -> Result<Vec<RawPayloadCacheMetadata>> {
    if entries.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for raw payload cache metadata upsert")?;

    let mut snapshots = Vec::with_capacity(entries.len());
    for entry in entries {
        let entry = normalize_metadata_upsert(entry)?;
        snapshots.push(upsert_raw_payload_cache_metadata_entry(&mut transaction, &entry).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit raw payload cache metadata upsert")?;

    Ok(snapshots)
}

/// Load one payload-cache metadata row by its hash-first metadata identity.
pub async fn load_raw_payload_cache_metadata(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
    payload_kind: &str,
    digest_algorithm: Option<&str>,
    retained_digest: Option<&str>,
) -> Result<Option<RawPayloadCacheMetadata>> {
    let identity = normalize_metadata_identity(
        chain_id,
        block_hash,
        payload_kind,
        digest_algorithm,
        retained_digest,
    )?;

    load_raw_payload_cache_metadata_internal(
        pool,
        &identity.chain_id,
        &identity.block_hash,
        &identity.payload_kind,
        identity.digest_algorithm.as_deref(),
        identity.retained_digest.as_deref(),
    )
    .await
}

/// List retained payload-cache metadata for one block hash in stable order.
pub async fn list_raw_payload_cache_metadata_by_block_hash(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
) -> Result<Vec<RawPayloadCacheMetadata>> {
    let chain_id = required_text("chain_id", chain_id)?;
    let block_hash = required_text("block_hash", block_hash)?;

    list_raw_payload_cache_metadata_by_block_hash_internal(pool, &chain_id, &block_hash).await
}

/// Verify that a block-scoped payload cache-fill candidate matches retained
/// metadata. This does not read or persist payload bytes; callers compute the
/// candidate digest before asking storage to authorize use.
pub async fn verify_raw_payload_cache_digest(
    pool: &PgPool,
    verification: &RawPayloadCacheDigestVerification,
) -> Result<RawPayloadCacheMetadata> {
    let verification = normalize_digest_verification(verification)?;
    let rows = list_raw_payload_cache_metadata_for_payload_identity(
        pool,
        &verification.chain_id,
        &verification.block_hash,
        &verification.payload_kind,
    )
    .await?;

    if rows.is_empty() {
        bail!(
            "raw payload cache identity mismatch for chain {} block {} payload kind {}",
            verification.chain_id,
            verification.block_hash,
            verification.payload_kind
        );
    }

    if rows.iter().all(|row| row.retained_digest.is_none()) {
        bail!(
            "raw payload cache metadata for chain {} block {} payload kind {} has no retained digest",
            verification.chain_id,
            verification.block_hash,
            verification.payload_kind
        );
    }

    let row = rows
        .into_iter()
        .find(|row| {
            row.digest_algorithm.as_deref() == Some(verification.digest_algorithm.as_str())
                && row.retained_digest.as_deref() == Some(verification.candidate_digest.as_str())
        })
        .with_context(|| {
            format!(
                "raw payload cache digest mismatch for chain {} block {} payload kind {}",
                verification.chain_id, verification.block_hash, verification.payload_kind
            )
        })?;

    if row.payload_size_bytes != verification.payload_size_bytes {
        bail!(
            "raw payload cache payload size mismatch for chain {} block {} payload kind {}",
            verification.chain_id,
            verification.block_hash,
            verification.payload_kind
        );
    }

    Ok(row)
}

async fn upsert_raw_payload_cache_metadata_entry(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    entry: &RawPayloadCacheMetadataUpsert,
) -> Result<RawPayloadCacheMetadata> {
    if let Some(snapshot) = sqlx::query(
        r#"
        INSERT INTO raw_payload_cache_metadata (
            chain_id,
            block_hash,
            payload_kind,
            digest_algorithm,
            retained_digest,
            block_number,
            payload_size_bytes,
            content_type,
            content_encoding,
            cache_metadata,
            canonicality_state
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::canonicality_state)
        ON CONFLICT DO NOTHING
        RETURNING
            raw_payload_cache_metadata_id,
            chain_id,
            block_hash,
            payload_kind,
            digest_algorithm,
            retained_digest,
            block_number,
            payload_size_bytes,
            content_type,
            content_encoding,
            cache_metadata,
            canonicality_state::TEXT AS canonicality_state,
            first_observed_at,
            last_observed_at
        "#,
    )
    .bind(&entry.chain_id)
    .bind(&entry.block_hash)
    .bind(&entry.payload_kind)
    .bind(&entry.digest_algorithm)
    .bind(&entry.retained_digest)
    .bind(entry.block_number)
    .bind(entry.payload_size_bytes)
    .bind(&entry.content_type)
    .bind(&entry.content_encoding)
    .bind(&entry.cache_metadata)
    .bind(entry.canonicality_state.as_str())
    .fetch_optional(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to insert raw payload cache metadata for chain {} block {} payload kind {}",
            entry.chain_id, entry.block_hash, entry.payload_kind
        )
    })? {
        return decode_raw_payload_cache_metadata(snapshot);
    }

    let existing = load_raw_payload_cache_metadata_internal(
        &mut **executor,
        &entry.chain_id,
        &entry.block_hash,
        &entry.payload_kind,
        entry.digest_algorithm.as_deref(),
        entry.retained_digest.as_deref(),
    )
    .await?
    .with_context(|| {
        format!(
            "failed to reload existing raw payload cache metadata for chain {} block {} payload kind {} after insert conflict",
            entry.chain_id, entry.block_hash, entry.payload_kind
        )
    })?;

    ensure_metadata_identity_matches(&existing, entry)?;
    let next_state = merge_canonicality(existing.canonicality_state, entry.canonicality_state);

    let snapshot = sqlx::query(
        r#"
        UPDATE raw_payload_cache_metadata
        SET
            canonicality_state = $6::canonicality_state,
            last_observed_at = now()
        WHERE chain_id = $1
          AND block_hash = $2
          AND payload_kind = $3
          AND digest_algorithm IS NOT DISTINCT FROM $4::TEXT
          AND retained_digest IS NOT DISTINCT FROM $5::TEXT
        RETURNING
            raw_payload_cache_metadata_id,
            chain_id,
            block_hash,
            payload_kind,
            digest_algorithm,
            retained_digest,
            block_number,
            payload_size_bytes,
            content_type,
            content_encoding,
            cache_metadata,
            canonicality_state::TEXT AS canonicality_state,
            first_observed_at,
            last_observed_at
        "#,
    )
    .bind(&entry.chain_id)
    .bind(&entry.block_hash)
    .bind(&entry.payload_kind)
    .bind(&entry.digest_algorithm)
    .bind(&entry.retained_digest)
    .bind(next_state.as_str())
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to refresh raw payload cache metadata for chain {} block {} payload kind {}",
            entry.chain_id, entry.block_hash, entry.payload_kind
        )
    })?;

    decode_raw_payload_cache_metadata(snapshot)
}

async fn load_raw_payload_cache_metadata_internal<'e, E>(
    executor: E,
    chain_id: &str,
    block_hash: &str,
    payload_kind: &str,
    digest_algorithm: Option<&str>,
    retained_digest: Option<&str>,
) -> Result<Option<RawPayloadCacheMetadata>>
where
    E: Executor<'e, Database = Postgres>,
{
    let row = sqlx::query(
        r#"
        SELECT
            raw_payload_cache_metadata_id,
            chain_id,
            block_hash,
            payload_kind,
            digest_algorithm,
            retained_digest,
            block_number,
            payload_size_bytes,
            content_type,
            content_encoding,
            cache_metadata,
            canonicality_state::TEXT AS canonicality_state,
            first_observed_at,
            last_observed_at
        FROM raw_payload_cache_metadata
        WHERE chain_id = $1
          AND block_hash = $2
          AND payload_kind = $3
          AND digest_algorithm IS NOT DISTINCT FROM $4::TEXT
          AND retained_digest IS NOT DISTINCT FROM $5::TEXT
        "#,
    )
    .bind(chain_id)
    .bind(block_hash)
    .bind(payload_kind)
    .bind(digest_algorithm)
    .bind(retained_digest)
    .fetch_optional(executor)
    .await
    .with_context(|| {
        format!(
            "failed to load raw payload cache metadata for chain {chain_id} block {block_hash} payload kind {payload_kind}"
        )
    })?;

    row.map(decode_raw_payload_cache_metadata).transpose()
}

async fn list_raw_payload_cache_metadata_by_block_hash_internal<'e, E>(
    executor: E,
    chain_id: &str,
    block_hash: &str,
) -> Result<Vec<RawPayloadCacheMetadata>>
where
    E: Executor<'e, Database = Postgres>,
{
    let rows = sqlx::query(
        r#"
        SELECT
            raw_payload_cache_metadata_id,
            chain_id,
            block_hash,
            payload_kind,
            digest_algorithm,
            retained_digest,
            block_number,
            payload_size_bytes,
            content_type,
            content_encoding,
            cache_metadata,
            canonicality_state::TEXT AS canonicality_state,
            first_observed_at,
            last_observed_at
        FROM raw_payload_cache_metadata
        WHERE chain_id = $1
          AND block_hash = $2
        ORDER BY
            payload_kind,
            digest_algorithm NULLS FIRST,
            retained_digest NULLS FIRST,
            raw_payload_cache_metadata_id
        "#,
    )
    .bind(chain_id)
    .bind(block_hash)
    .fetch_all(executor)
    .await
    .with_context(|| {
        format!("failed to list raw payload cache metadata for chain {chain_id} block {block_hash}")
    })?;

    rows.into_iter()
        .map(decode_raw_payload_cache_metadata)
        .collect()
}

async fn list_raw_payload_cache_metadata_for_payload_identity<'e, E>(
    executor: E,
    chain_id: &str,
    block_hash: &str,
    payload_kind: &str,
) -> Result<Vec<RawPayloadCacheMetadata>>
where
    E: Executor<'e, Database = Postgres>,
{
    let rows = sqlx::query(
        r#"
        SELECT
            raw_payload_cache_metadata_id,
            chain_id,
            block_hash,
            payload_kind,
            digest_algorithm,
            retained_digest,
            block_number,
            payload_size_bytes,
            content_type,
            content_encoding,
            cache_metadata,
            canonicality_state::TEXT AS canonicality_state,
            first_observed_at,
            last_observed_at
        FROM raw_payload_cache_metadata
        WHERE chain_id = $1
          AND block_hash = $2
          AND payload_kind = $3
        ORDER BY
            digest_algorithm NULLS FIRST,
            retained_digest NULLS FIRST,
            raw_payload_cache_metadata_id
        "#,
    )
    .bind(chain_id)
    .bind(block_hash)
    .bind(payload_kind)
    .fetch_all(executor)
    .await
    .with_context(|| {
        format!(
            "failed to list raw payload cache metadata for chain {chain_id} block {block_hash} payload kind {payload_kind}"
        )
    })?;

    rows.into_iter()
        .map(decode_raw_payload_cache_metadata)
        .collect()
}

fn normalize_metadata_upsert(
    entry: &RawPayloadCacheMetadataUpsert,
) -> Result<RawPayloadCacheMetadataUpsert> {
    if let Some(block_number) = entry.block_number
        && block_number < 0
    {
        bail!(
            "raw payload cache metadata for chain {} block {} payload kind {} has negative block number {}",
            entry.chain_id,
            entry.block_hash,
            entry.payload_kind,
            block_number
        );
    }
    if entry.payload_size_bytes < 0 {
        bail!(
            "raw payload cache metadata for chain {} block {} payload kind {} has negative payload size {}",
            entry.chain_id,
            entry.block_hash,
            entry.payload_kind,
            entry.payload_size_bytes
        );
    }
    if !entry.cache_metadata.is_object() {
        bail!(
            "raw payload cache metadata for chain {} block {} payload kind {} must have object cache_metadata",
            entry.chain_id,
            entry.block_hash,
            entry.payload_kind
        );
    }

    let digest_algorithm = optional_lower_text(entry.digest_algorithm.as_deref());
    let retained_digest = optional_lower_text(entry.retained_digest.as_deref());
    ensure_digest_pair(digest_algorithm.as_deref(), retained_digest.as_deref())?;

    Ok(RawPayloadCacheMetadataUpsert {
        chain_id: required_text("chain_id", &entry.chain_id)?,
        block_hash: required_text("block_hash", &entry.block_hash)?,
        payload_kind: required_text("payload_kind", &entry.payload_kind)?,
        digest_algorithm,
        retained_digest,
        block_number: entry.block_number,
        payload_size_bytes: entry.payload_size_bytes,
        content_type: optional_text(entry.content_type.as_deref()),
        content_encoding: optional_text(entry.content_encoding.as_deref()),
        cache_metadata: entry.cache_metadata.clone(),
        canonicality_state: entry.canonicality_state,
    })
}

fn normalize_metadata_identity(
    chain_id: &str,
    block_hash: &str,
    payload_kind: &str,
    digest_algorithm: Option<&str>,
    retained_digest: Option<&str>,
) -> Result<RawPayloadCacheMetadataIdentity> {
    let digest_algorithm = optional_lower_text(digest_algorithm);
    let retained_digest = optional_lower_text(retained_digest);
    ensure_digest_pair(digest_algorithm.as_deref(), retained_digest.as_deref())?;

    Ok(RawPayloadCacheMetadataIdentity {
        chain_id: required_text("chain_id", chain_id)?,
        block_hash: required_text("block_hash", block_hash)?,
        payload_kind: required_text("payload_kind", payload_kind)?,
        digest_algorithm,
        retained_digest,
    })
}

fn normalize_digest_verification(
    verification: &RawPayloadCacheDigestVerification,
) -> Result<RawPayloadCacheDigestVerification> {
    if verification.payload_size_bytes < 0 {
        bail!(
            "raw payload cache digest verification for chain {} block {} payload kind {} has negative payload size {}",
            verification.chain_id,
            verification.block_hash,
            verification.payload_kind,
            verification.payload_size_bytes
        );
    }

    Ok(RawPayloadCacheDigestVerification {
        chain_id: required_text("chain_id", &verification.chain_id)?,
        block_hash: required_text("block_hash", &verification.block_hash)?,
        payload_kind: required_text("payload_kind", &verification.payload_kind)?,
        digest_algorithm: required_lower_text("digest_algorithm", &verification.digest_algorithm)?,
        candidate_digest: required_lower_text("candidate_digest", &verification.candidate_digest)?,
        payload_size_bytes: verification.payload_size_bytes,
    })
}

fn ensure_digest_pair(digest_algorithm: Option<&str>, retained_digest: Option<&str>) -> Result<()> {
    if digest_algorithm.is_some() != retained_digest.is_some() {
        bail!("raw payload cache metadata must set digest_algorithm and retained_digest together");
    }

    Ok(())
}

fn ensure_metadata_identity_matches(
    existing: &RawPayloadCacheMetadata,
    incoming: &RawPayloadCacheMetadataUpsert,
) -> Result<()> {
    if existing.block_number != incoming.block_number
        || existing.payload_size_bytes != incoming.payload_size_bytes
        || existing.content_type != incoming.content_type
        || existing.content_encoding != incoming.content_encoding
        || existing.cache_metadata != incoming.cache_metadata
    {
        bail!(
            "raw payload cache metadata identity mismatch for chain {} block {} payload kind {}",
            existing.chain_id,
            existing.block_hash,
            existing.payload_kind
        );
    }

    Ok(())
}

fn merge_canonicality(
    current: CanonicalityState,
    incoming: CanonicalityState,
) -> CanonicalityState {
    match incoming {
        CanonicalityState::Orphaned => CanonicalityState::Orphaned,
        CanonicalityState::Observed => {
            if current == CanonicalityState::Orphaned {
                CanonicalityState::Observed
            } else {
                current
            }
        }
        CanonicalityState::Canonical | CanonicalityState::Safe | CanonicalityState::Finalized => {
            if current == CanonicalityState::Orphaned {
                incoming
            } else {
                current.promote_to(incoming)
            }
        }
    }
}

fn required_text(field: &str, value: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty() {
        bail!("{field} must not be empty");
    }
    Ok(value.to_owned())
}

fn required_lower_text(field: &str, value: &str) -> Result<String> {
    Ok(required_text(field, value)?.to_ascii_lowercase())
}

fn optional_text(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn optional_lower_text(value: Option<&str>) -> Option<String> {
    optional_text(value).map(|value| value.to_ascii_lowercase())
}

fn decode_raw_payload_cache_metadata(row: PgRow) -> Result<RawPayloadCacheMetadata> {
    Ok(RawPayloadCacheMetadata {
        raw_payload_cache_metadata_id: row
            .try_get("raw_payload_cache_metadata_id")
            .context("missing raw_payload_cache_metadata_id")?,
        chain_id: row.try_get("chain_id").context("missing chain_id")?,
        block_hash: row.try_get("block_hash").context("missing block_hash")?,
        payload_kind: row
            .try_get("payload_kind")
            .context("missing payload_kind")?,
        digest_algorithm: row
            .try_get("digest_algorithm")
            .context("missing digest_algorithm")?,
        retained_digest: row
            .try_get("retained_digest")
            .context("missing retained_digest")?,
        block_number: row
            .try_get("block_number")
            .context("missing block_number")?,
        payload_size_bytes: row
            .try_get("payload_size_bytes")
            .context("missing payload_size_bytes")?,
        content_type: row
            .try_get("content_type")
            .context("missing content_type")?,
        content_encoding: row
            .try_get("content_encoding")
            .context("missing content_encoding")?,
        cache_metadata: row
            .try_get("cache_metadata")
            .context("missing cache_metadata")?,
        canonicality_state: CanonicalityState::parse(
            &row.try_get::<String, _>("canonicality_state")
                .context("missing canonicality_state")?,
        )?,
        first_observed_at: row
            .try_get("first_observed_at")
            .context("missing first_observed_at")?,
        last_observed_at: row
            .try_get("last_observed_at")
            .context("missing last_observed_at")?,
    })
}

struct RawPayloadCacheMetadataIdentity {
    chain_id: String,
    block_hash: String,
    payload_kind: String,
    digest_algorithm: Option<String>,
    retained_digest: Option<String>,
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use anyhow::Result;
    use serde_json::json;
    use sqlx::{
        PgPool,
        postgres::{PgConnectOptions, PgPoolOptions},
    };
    use uuid::Uuid;

    use super::*;
    use crate::default_database_url;

    struct TestDatabase {
        admin_pool: PgPool,
        pool: PgPool,
        database_name: String,
    }

    impl TestDatabase {
        async fn new() -> Result<Self> {
            let database_url = std::env::var("BIGNAME_DATABASE_URL")
                .or_else(|_| std::env::var("DATABASE_URL"))
                .unwrap_or_else(|_| default_database_url().to_owned());
            let base_options = PgConnectOptions::from_str(&database_url)
                .context("failed to parse database URL for raw payload cache tests")?;
            let database_name = format!(
                "bigname_storage_raw_payload_cache_test_{}_{}",
                std::process::id(),
                Uuid::new_v4().simple()
            );

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for raw payload cache tests")?;

            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(base_options.database(&database_name))
                .await
                .context("failed to connect raw payload cache test pool")?;

            crate::MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for raw payload cache tests")?;

            Ok(Self {
                admin_pool,
                pool,
                database_name,
            })
        }

        fn pool(&self) -> &PgPool {
            &self.pool
        }

        async fn cleanup(self) -> Result<()> {
            self.pool.close().await;
            sqlx::query(&format!(
                r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#,
                self.database_name
            ))
            .execute(&self.admin_pool)
            .await
            .with_context(|| format!("failed to drop test database {}", self.database_name))?;
            self.admin_pool.close().await;
            Ok(())
        }
    }

    fn metadata(state: CanonicalityState) -> RawPayloadCacheMetadataUpsert {
        RawPayloadCacheMetadataUpsert {
            chain_id: "eth-mainnet".to_owned(),
            block_hash: "0xaaa".to_owned(),
            payload_kind: "full_block".to_owned(),
            digest_algorithm: Some("SHA256".to_owned()),
            retained_digest: Some("ABCDEF".to_owned()),
            block_number: Some(101),
            payload_size_bytes: 42,
            content_type: Some("application/json".to_owned()),
            content_encoding: Some("identity".to_owned()),
            cache_metadata: json!({
                "source": "json-rpc",
                "fetch_mode": "block_hash"
            }),
            canonicality_state: state,
        }
    }

    fn verification(retained_digest: &str, size: i64) -> RawPayloadCacheDigestVerification {
        RawPayloadCacheDigestVerification {
            chain_id: "eth-mainnet".to_owned(),
            block_hash: "0xaaa".to_owned(),
            payload_kind: "full_block".to_owned(),
            digest_algorithm: "SHA256".to_owned(),
            candidate_digest: retained_digest.to_owned(),
            payload_size_bytes: size,
        }
    }

    #[tokio::test]
    async fn upserts_loads_and_verifies_payload_cache_metadata() -> Result<()> {
        let database = TestDatabase::new().await?;

        let inserted = upsert_raw_payload_cache_metadata(
            database.pool(),
            &[metadata(CanonicalityState::Canonical)],
        )
        .await?;
        assert_eq!(inserted.len(), 1);
        assert_eq!(inserted[0].digest_algorithm.as_deref(), Some("sha256"));
        assert_eq!(inserted[0].retained_digest.as_deref(), Some("abcdef"));
        assert_eq!(inserted[0].block_number, Some(101));
        assert_eq!(inserted[0].payload_size_bytes, 42);

        let promoted = upsert_raw_payload_cache_metadata(
            database.pool(),
            &[metadata(CanonicalityState::Finalized)],
        )
        .await?;
        assert_eq!(promoted[0].canonicality_state, CanonicalityState::Finalized);
        assert_eq!(
            promoted[0].raw_payload_cache_metadata_id,
            inserted[0].raw_payload_cache_metadata_id
        );
        assert!(promoted[0].last_observed_at >= inserted[0].last_observed_at);

        let loaded = load_raw_payload_cache_metadata(
            database.pool(),
            "eth-mainnet",
            "0xaaa",
            "full_block",
            Some("sha256"),
            Some("abcdef"),
        )
        .await?;
        assert_eq!(loaded, Some(promoted[0].clone()));

        let listed =
            list_raw_payload_cache_metadata_by_block_hash(database.pool(), "eth-mainnet", "0xaaa")
                .await?;
        assert_eq!(listed, vec![promoted[0].clone()]);

        let verified =
            verify_raw_payload_cache_digest(database.pool(), &verification("ABCDEF", 42)).await?;
        assert_eq!(verified, promoted[0]);

        database.cleanup().await
    }

    #[tokio::test]
    async fn rejects_mismatched_payload_cache_metadata_identity() -> Result<()> {
        let database = TestDatabase::new().await?;

        upsert_raw_payload_cache_metadata(
            database.pool(),
            &[metadata(CanonicalityState::Canonical)],
        )
        .await?;

        let mut conflicting = metadata(CanonicalityState::Observed);
        conflicting.payload_size_bytes = 43;
        let error = upsert_raw_payload_cache_metadata(database.pool(), &[conflicting])
            .await
            .expect_err("payload cache metadata identity mismatch must fail");
        assert!(
            error.to_string().contains(
                "raw payload cache metadata identity mismatch for chain eth-mainnet block 0xaaa payload kind full_block"
            ),
            "unexpected error: {error:#}"
        );

        database.cleanup().await
    }

    #[tokio::test]
    async fn digest_verification_fails_closed() -> Result<()> {
        let database = TestDatabase::new().await?;

        let mut without_digest = metadata(CanonicalityState::Canonical);
        without_digest.block_hash = "0xnodigest".to_owned();
        without_digest.digest_algorithm = None;
        without_digest.retained_digest = None;
        upsert_raw_payload_cache_metadata(database.pool(), &[without_digest]).await?;

        let mut missing_digest_check = verification("abcdef", 42);
        missing_digest_check.block_hash = "0xnodigest".to_owned();
        let error = verify_raw_payload_cache_digest(database.pool(), &missing_digest_check)
            .await
            .expect_err("metadata without retained digest must fail closed");
        assert!(
            error.to_string().contains("has no retained digest"),
            "unexpected error: {error:#}"
        );

        upsert_raw_payload_cache_metadata(
            database.pool(),
            &[metadata(CanonicalityState::Canonical)],
        )
        .await?;

        let error = verify_raw_payload_cache_digest(database.pool(), &verification("bbbbbb", 42))
            .await
            .expect_err("mismatched digest must fail closed");
        assert!(
            error
                .to_string()
                .contains("raw payload cache digest mismatch"),
            "unexpected error: {error:#}"
        );

        let error = verify_raw_payload_cache_digest(database.pool(), &verification("abcdef", 41))
            .await
            .expect_err("mismatched payload size must fail closed");
        assert!(
            error
                .to_string()
                .contains("raw payload cache payload size mismatch"),
            "unexpected error: {error:#}"
        );

        let mut wrong_identity = verification("abcdef", 42);
        wrong_identity.block_hash = "0xmissing".to_owned();
        let error = verify_raw_payload_cache_digest(database.pool(), &wrong_identity)
            .await
            .expect_err("mismatched payload identity must fail closed");
        assert!(
            error
                .to_string()
                .contains("raw payload cache identity mismatch"),
            "unexpected error: {error:#}"
        );

        database.cleanup().await
    }
}
