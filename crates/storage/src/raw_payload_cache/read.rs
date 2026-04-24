use anyhow::{Context, Result};
use sqlx::{Executor, PgPool, Postgres};

use super::decode::decode_raw_payload_cache_metadata;
use super::normalization::{normalize_metadata_identity, required_text};
use super::types::RawPayloadCacheMetadata;

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

pub(super) async fn load_raw_payload_cache_metadata_internal<'e, E>(
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

pub(super) async fn list_raw_payload_cache_metadata_for_payload_identity<'e, E>(
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
