use anyhow::{Context, Result};
use sqlx::{Row, postgres::PgRow};

use crate::CanonicalityState;

use super::types::RawPayloadCacheMetadata;

pub(super) fn decode_raw_payload_cache_metadata(row: PgRow) -> Result<RawPayloadCacheMetadata> {
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
