//! Verified label-preimage import from an operator-loaded ENS rainbow table.
//!
//! `ens_names` rows are unverified candidates. A row enters `label_preimages`
//! only when its label is one DNS label that re-hashes to the row's labelhash.
//! The import is additive and idempotent: re-runs conflict on the primary key
//! and insert nothing, and an existing verified row is never rewritten.

use alloy_primitives::keccak256;
use anyhow::{Context, Result};
use bigname_domain::normalization::{ENS_NORMALIZER_VERSION, normalize_label_under_suffix};
use serde_json::json;
use sqlx::{PgPool, Row};

pub const ENS_RAINBOW_SOURCE_KIND: &str = "ens_rainbow_import";

// Below the interpreter's chain-observation priority (100), so a later
// chain-observed preimage of identical content takes provenance precedence.
const ENS_RAINBOW_SOURCE_PRIORITY: i32 = 10;
const DEFAULT_BATCH_SIZE: i64 = 10_000;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LabelPreimageImportSummary {
    /// Rainbow rows read from `ens_names`.
    pub scanned_row_count: u64,
    /// Proof-checked rows inserted; a row skipped because a verified preimage
    /// already exists is neither retained nor rejected.
    pub retained_row_count: u64,
    /// Rows rejected by the proof check.
    pub rejected_row_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RainbowPreimage {
    labelhash: String,
    raw_label: Vec<u8>,
    decoded_label: Option<String>,
    normalized_under_version: bool,
    normalization_error: Option<String>,
}

pub async fn import_label_preimages_from_ens_names_table(
    pool: &PgPool,
    batch_size: Option<i64>,
    limit: Option<i64>,
) -> Result<LabelPreimageImportSummary> {
    let batch_size = batch_size.unwrap_or(DEFAULT_BATCH_SIZE).max(1);
    let mut summary = LabelPreimageImportSummary::default();
    let mut last_hash = String::new();

    loop {
        let remaining = limit.map(|limit| limit.saturating_sub(summary.scanned_row_count as i64));
        if remaining == Some(0) {
            break;
        }
        let effective_batch_size = remaining.map_or(batch_size, |left| left.min(batch_size));
        let rows = sqlx::query(
            "SELECT hash, name
             FROM ens_names
             WHERE hash > $1
             ORDER BY hash ASC
             LIMIT $2",
        )
        .bind(&last_hash)
        .bind(effective_batch_size)
        .fetch_all(pool)
        .await
        .context("failed to load rows from the ens_names rainbow table")?;
        if rows.is_empty() {
            break;
        }

        let mut preimages = Vec::new();
        for row in &rows {
            let hash: String = row.try_get("hash")?;
            let name: String = row.try_get("name")?;
            last_hash = hash;
            summary.scanned_row_count += 1;
            match proven_rainbow_preimage(&last_hash, &name) {
                Some(preimage) => preimages.push(preimage),
                None => summary.rejected_row_count += 1,
            }
        }
        summary.retained_row_count += insert_label_preimages(pool, &preimages).await?;
    }

    Ok(summary)
}

fn proven_rainbow_preimage(hash: &str, name: &str) -> Option<RainbowPreimage> {
    // Preimages join per label, so a candidate must be exactly one DNS label.
    if name.is_empty() || name.contains('.') {
        return None;
    }
    // The pinned generator stores hash = keccak256(name)
    // (upstream: .refs/ens_rainbow/src/main.rs:L50 @ ens_rainbow@bc44492); the
    // candidate is accepted only when it re-hashes to the row's labelhash.
    let labelhash = format!("{:#x}", keccak256(name.as_bytes()));
    if labelhash != hash.to_ascii_lowercase() {
        return None;
    }
    let (normalized_under_version, normalization_error) = normalization_verdict(name);
    Some(RainbowPreimage {
        labelhash,
        raw_label: name.as_bytes().to_vec(),
        decoded_label: Some(name.to_owned()),
        normalized_under_version,
        normalization_error,
    })
}

// Same verdict the interpreter stores for a decodable chain-observed label:
// normalized only when the raw bytes are byte-identical to their normalized
// form. A failing verdict does not discard a proof-checked preimage.
fn normalization_verdict(name: &str) -> (bool, Option<String>) {
    match normalize_label_under_suffix(name, &[]) {
        Ok(normalized) if normalized.normalized_name.as_bytes() == name.as_bytes() => (true, None),
        Ok(_) => (
            false,
            Some("raw label is not byte-identical to its normalized form".to_owned()),
        ),
        Err(error) => (false, Some(error.to_string())),
    }
}

async fn insert_label_preimages(pool: &PgPool, preimages: &[RainbowPreimage]) -> Result<u64> {
    if preimages.is_empty() {
        return Ok(0);
    }
    let provenance = serde_json::to_string(&json!({
        "source": "ens_rainbow",
        "table": "ens_names",
    }))
    .context("failed to serialize rainbow preimage provenance")?;
    let labelhashes = preimages
        .iter()
        .map(|preimage| preimage.labelhash.clone())
        .collect::<Vec<_>>();
    let raw_labels = preimages
        .iter()
        .map(|preimage| preimage.raw_label.clone())
        .collect::<Vec<_>>();
    let decoded_labels = preimages
        .iter()
        .map(|preimage| preimage.decoded_label.clone())
        .collect::<Vec<_>>();
    let normalizer_versions = preimages
        .iter()
        .map(|_| ENS_NORMALIZER_VERSION.to_owned())
        .collect::<Vec<_>>();
    let normalized_flags = preimages
        .iter()
        .map(|preimage| preimage.normalized_under_version)
        .collect::<Vec<_>>();
    let normalization_errors = preimages
        .iter()
        .map(|preimage| preimage.normalization_error.clone())
        .collect::<Vec<_>>();
    let source_kinds = preimages
        .iter()
        .map(|_| ENS_RAINBOW_SOURCE_KIND.to_owned())
        .collect::<Vec<_>>();
    let source_priorities = preimages
        .iter()
        .map(|_| ENS_RAINBOW_SOURCE_PRIORITY)
        .collect::<Vec<_>>();
    let provenances = preimages
        .iter()
        .map(|_| provenance.clone())
        .collect::<Vec<_>>();

    let inserted = sqlx::query_scalar::<_, String>(
        r#"
        INSERT INTO label_preimages (
            labelhash, raw_label, decoded_label, normalizer_version,
            normalized_under_version, normalization_error,
            source_kind, source_priority, provenance
        )
        SELECT labelhash, raw_label, decoded_label, normalizer_version,
               normalized_under_version, normalization_error,
               source_kind, source_priority, provenance::JSONB
        FROM unnest(
            $1::TEXT[], $2::BYTEA[], $3::TEXT[], $4::TEXT[], $5::BOOLEAN[],
            $6::TEXT[], $7::TEXT[], $8::INTEGER[], $9::TEXT[]
        ) AS input(
            labelhash, raw_label, decoded_label, normalizer_version,
            normalized_under_version, normalization_error,
            source_kind, source_priority, provenance
        )
        ON CONFLICT (labelhash) DO NOTHING
        RETURNING labelhash
        "#,
    )
    .bind(&labelhashes)
    .bind(&raw_labels)
    .bind(&decoded_labels)
    .bind(&normalizer_versions)
    .bind(&normalized_flags)
    .bind(&normalization_errors)
    .bind(&source_kinds)
    .bind(&source_priorities)
    .bind(&provenances)
    .fetch_all(pool)
    .await
    .context("failed to insert verified rainbow label preimages")?;

    Ok(inserted.len() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_a_candidate_that_rehashes_to_its_labelhash() {
        let preimage = proven_rainbow_preimage(
            "0x9c22ff5f21f0b81b113e63f7db6da94fedef11b2119b4088b89664fb9a3cb658",
            "test",
        )
        .expect("keccak256(\"test\") must prove the recorded rainbow hash");

        assert_eq!(
            preimage.labelhash,
            "0x9c22ff5f21f0b81b113e63f7db6da94fedef11b2119b4088b89664fb9a3cb658"
        );
        assert_eq!(preimage.raw_label, b"test");
        assert_eq!(preimage.decoded_label.as_deref(), Some("test"));
        assert!(preimage.normalized_under_version);
        assert_eq!(preimage.normalization_error, None);
    }

    #[test]
    fn accepts_an_uppercase_rainbow_hash_after_lowercasing() {
        let hash = format!("{:#x}", keccak256(b"alice")).to_ascii_uppercase();
        let preimage =
            proven_rainbow_preimage(&hash, "alice").expect("uppercase hash must still prove");

        assert_eq!(preimage.labelhash, format!("{:#x}", keccak256(b"alice")));
    }

    #[test]
    fn rejects_a_candidate_that_does_not_rehash_to_its_labelhash() {
        let wrong_hash = format!("{:#x}", keccak256(b"alice"));
        assert!(proven_rainbow_preimage(&wrong_hash, "bob").is_none());
    }

    #[test]
    fn rejects_candidates_that_are_not_single_labels() {
        let dotted = format!("{:#x}", keccak256(b"bad.label"));
        assert!(proven_rainbow_preimage(&dotted, "bad.label").is_none());
        let empty = format!("{:#x}", keccak256(b""));
        assert!(proven_rainbow_preimage(&empty, "").is_none());
    }

    #[test]
    fn stores_a_proof_checked_unnormalized_label_with_its_verdict() {
        let hash = format!("{:#x}", keccak256(b"Alice"));
        let preimage =
            proven_rainbow_preimage(&hash, "Alice").expect("proof-checked label must be kept");

        assert!(!preimage.normalized_under_version);
        assert_eq!(
            preimage.normalization_error.as_deref(),
            Some("raw label is not byte-identical to its normalized form")
        );
    }

    #[test]
    fn stores_the_normalizer_error_for_a_rejected_label() {
        let label = "Ni\u{200d}ck";
        let hash = format!("{:#x}", keccak256(label.as_bytes()));
        let preimage =
            proven_rainbow_preimage(&hash, label).expect("proof-checked label must be kept");

        assert!(!preimage.normalized_under_version);
        assert!(
            preimage
                .normalization_error
                .as_deref()
                .is_some_and(|error| !error.trim().is_empty())
        );
    }
}
