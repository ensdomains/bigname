use anyhow::{Context, Result, bail};
use sqlx::{PgPool, Row};

use crate::{
    ChainLineageBlock, RawPayloadCacheMetadata, list_raw_payload_cache_metadata_by_block_hash,
    load_chain_lineage_block,
};

use super::{
    decode::{decode_count, decode_stored_lineage_block},
    types::{
        CanonicalityInspection, CanonicalityInspectionStatus, RawFactAuditCounts,
        RawPayloadCacheAuditMetadata, StoredLineageRangeBlock,
    },
};

/// Inspect one block by hash-first identity without mutating storage.
pub async fn inspect_block_canonicality(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
) -> Result<CanonicalityInspection> {
    validate_block_identity(chain_id, block_hash)?;

    let lineage = load_chain_lineage_block(pool, chain_id, block_hash).await?;
    let raw_fact_counts = load_raw_fact_counts(pool, chain_id, block_hash).await?;
    let normalized_event_count = load_normalized_event_count(pool, chain_id, block_hash).await?;

    Ok(build_inspection(
        chain_id,
        block_hash,
        lineage,
        raw_fact_counts,
        normalized_event_count,
    ))
}

/// List retained payload-cache metadata for audit tooling without dereferencing
/// object-backed cache or re-fetching provider bytes.
pub async fn list_raw_payload_cache_audit_metadata(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
) -> Result<Vec<RawPayloadCacheAuditMetadata>> {
    validate_block_identity(chain_id, block_hash)?;

    let rows = list_raw_payload_cache_metadata_by_block_hash(pool, chain_id, block_hash).await?;
    Ok(rows
        .into_iter()
        .map(raw_payload_cache_audit_metadata)
        .collect())
}

/// Inspect every stored lineage block in a bounded block-number range. Missing
/// heights cannot be inferred without a requested block hash, so this returns
/// only stored lineage identities in range order.
pub async fn inspect_canonicality_range(
    pool: &PgPool,
    chain_id: &str,
    range_start_block_number: i64,
    range_end_block_number: i64,
) -> Result<Vec<CanonicalityInspection>> {
    validate_range(chain_id, range_start_block_number, range_end_block_number)?;

    let rows = sqlx::query(
        r#"
        SELECT block_hash
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_number >= $2
          AND block_number <= $3
        ORDER BY block_number, block_hash
        "#,
    )
    .bind(chain_id)
    .bind(range_start_block_number)
    .bind(range_end_block_number)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load lineage block hashes for chain {chain_id} range {range_start_block_number}..={range_end_block_number}"
        )
    })?;

    let mut inspections = Vec::with_capacity(rows.len());
    for row in rows {
        let block_hash = row
            .try_get::<String, _>("block_hash")
            .context("missing block_hash from canonicality range row")?;
        inspections.push(inspect_block_canonicality(pool, chain_id, &block_hash).await?);
    }

    Ok(inspections)
}

/// List only stored lineage rows in a bounded block-number range. The helper
/// does not infer missing heights, gaps, range completeness, or span-wide
/// canonicality.
pub async fn list_stored_lineage_range(
    pool: &PgPool,
    chain_id: &str,
    range_start_block_number: i64,
    range_end_block_number: i64,
) -> Result<Vec<StoredLineageRangeBlock>> {
    validate_range(chain_id, range_start_block_number, range_end_block_number)?;

    let rows = sqlx::query(
        r#"
        SELECT
            chain_id,
            block_hash,
            parent_hash,
            block_number,
            block_timestamp,
            logs_bloom,
            transactions_root,
            receipts_root,
            state_root,
            canonicality_state::TEXT AS canonicality_state
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_number >= $2
          AND block_number <= $3
        ORDER BY block_number, block_hash
        "#,
    )
    .bind(chain_id)
    .bind(range_start_block_number)
    .bind(range_end_block_number)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to list stored lineage rows for chain {chain_id} range {range_start_block_number}..={range_end_block_number}"
        )
    })?;

    rows.into_iter().map(decode_stored_lineage_block).collect()
}

fn build_inspection(
    chain_id: &str,
    block_hash: &str,
    lineage: Option<ChainLineageBlock>,
    raw_fact_counts: RawFactAuditCounts,
    normalized_event_count: u64,
) -> CanonicalityInspection {
    let status = lineage
        .as_ref()
        .map(|block| CanonicalityInspectionStatus::from(block.canonicality_state))
        .unwrap_or(CanonicalityInspectionStatus::Missing);
    let lineage_state = lineage.as_ref().map(|block| block.canonicality_state);
    let parent_hash = lineage.as_ref().and_then(|block| block.parent_hash.clone());
    let block_number = lineage.as_ref().map(|block| block.block_number);

    CanonicalityInspection {
        chain_id: chain_id.to_owned(),
        block_hash: block_hash.to_owned(),
        status,
        lineage_state,
        parent_hash,
        block_number,
        raw_fact_counts,
        normalized_event_count,
    }
}

fn raw_payload_cache_audit_metadata(row: RawPayloadCacheMetadata) -> RawPayloadCacheAuditMetadata {
    RawPayloadCacheAuditMetadata {
        payload_kind: row.payload_kind,
        digest_algorithm: row.digest_algorithm,
        retained_digest: row.retained_digest,
        block_number: row.block_number,
        payload_size_bytes: row.payload_size_bytes,
        content_type: row.content_type,
        content_encoding: row.content_encoding,
        cache_metadata: row.cache_metadata,
        canonicality_state: row.canonicality_state,
        first_observed_at: row.first_observed_at,
        last_observed_at: row.last_observed_at,
    }
}

async fn load_raw_fact_counts(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
) -> Result<RawFactAuditCounts> {
    let row = sqlx::query(
        r#"
        SELECT
          (SELECT COUNT(*)::BIGINT FROM raw_blocks WHERE chain_id = $1 AND block_hash = $2) AS raw_block_count,
          (SELECT COUNT(*)::BIGINT FROM raw_code_hashes WHERE chain_id = $1 AND block_hash = $2) AS raw_code_hash_count,
          (SELECT COUNT(*)::BIGINT FROM raw_transactions WHERE chain_id = $1 AND block_hash = $2) AS raw_transaction_count,
          (SELECT COUNT(*)::BIGINT FROM raw_receipts WHERE chain_id = $1 AND block_hash = $2) AS raw_receipt_count,
          (SELECT COUNT(*)::BIGINT FROM raw_logs WHERE chain_id = $1 AND block_hash = $2) AS raw_log_count,
          (SELECT COUNT(*)::BIGINT FROM raw_call_snapshots WHERE chain_id = $1 AND block_hash = $2) AS raw_call_snapshot_count
        "#,
    )
    .bind(chain_id)
    .bind(block_hash)
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to load raw fact audit counts for chain {chain_id} block {block_hash}"))?;

    Ok(RawFactAuditCounts {
        raw_block_count: decode_count(&row, "raw_block_count")?,
        raw_code_hash_count: decode_count(&row, "raw_code_hash_count")?,
        raw_transaction_count: decode_count(&row, "raw_transaction_count")?,
        raw_receipt_count: decode_count(&row, "raw_receipt_count")?,
        raw_log_count: decode_count(&row, "raw_log_count")?,
        raw_call_snapshot_count: decode_count(&row, "raw_call_snapshot_count")?,
    })
}

async fn load_normalized_event_count(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
) -> Result<u64> {
    let row = sqlx::query(
        r#"
        SELECT COUNT(*)::BIGINT AS normalized_event_count
        FROM normalized_events
        WHERE chain_id = $1
          AND block_hash = $2
        "#,
    )
    .bind(chain_id)
    .bind(block_hash)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load normalized-event audit count for chain {chain_id} block {block_hash}"
        )
    })?;

    decode_count(&row, "normalized_event_count")
}

fn validate_block_identity(chain_id: &str, block_hash: &str) -> Result<()> {
    if chain_id.trim().is_empty() {
        bail!("chain_id must not be empty");
    }
    if block_hash.trim().is_empty() {
        bail!("block_hash must not be empty");
    }
    Ok(())
}

fn validate_range(chain_id: &str, start: i64, end: i64) -> Result<()> {
    if chain_id.trim().is_empty() {
        bail!("chain_id must not be empty");
    }
    if start < 0 {
        bail!("canonicality inspection range start {start} is negative");
    }
    if end < start {
        bail!("canonicality inspection range end {end} is before start {start}");
    }
    Ok(())
}
