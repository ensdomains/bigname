use serde_json::Value;
use sqlx::types::time::OffsetDateTime;

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

pub(super) struct RawPayloadCacheMetadataIdentity {
    pub(super) chain_id: String,
    pub(super) block_hash: String,
    pub(super) payload_kind: String,
    pub(super) digest_algorithm: Option<String>,
    pub(super) retained_digest: Option<String>,
}
