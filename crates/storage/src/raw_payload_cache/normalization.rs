use anyhow::{Result, bail};

use super::types::{
    RawPayloadCacheDigestVerification, RawPayloadCacheMetadata, RawPayloadCacheMetadataIdentity,
    RawPayloadCacheMetadataUpsert,
};

pub(super) fn normalize_metadata_upsert(
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

pub(super) fn normalize_metadata_identity(
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

pub(super) fn normalize_digest_verification(
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

pub(super) fn ensure_metadata_identity_matches(
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

pub(super) fn required_text(field: &str, value: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty() {
        bail!("{field} must not be empty");
    }
    Ok(value.to_owned())
}

fn required_lower_text(field: &str, value: &str) -> Result<String> {
    Ok(required_text(field, value)?.to_ascii_lowercase())
}

fn ensure_digest_pair(digest_algorithm: Option<&str>, retained_digest: Option<&str>) -> Result<()> {
    if digest_algorithm.is_some() != retained_digest.is_some() {
        bail!("raw payload cache metadata must set digest_algorithm and retained_digest together");
    }

    Ok(())
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
