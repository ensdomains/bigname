use anyhow::{Context, Result, bail};
use sqlx::PgPool;

use super::normalization::normalize_digest_verification;
use super::read::list_raw_payload_cache_metadata_for_payload_identity;
use super::types::{RawPayloadCacheDigestVerification, RawPayloadCacheMetadata};

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
