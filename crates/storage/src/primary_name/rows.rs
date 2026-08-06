use anyhow::{Context, Result};
use sqlx::postgres::PgRow;

use super::types::{PrimaryNameClaimStatus, PrimaryNameCurrentRow, PrimaryNameCurrentSnapshot};

pub(super) fn decode_primary_name_current_snapshot(
    row: PgRow,
) -> Result<PrimaryNameCurrentSnapshot> {
    let address = crate::sql_row::get::<String>(&row, "address")?.to_ascii_lowercase();
    let namespace = crate::sql_row::get::<String>(&row, "namespace")?;
    let coin_type = crate::sql_row::get::<String>(&row, "coin_type")?;
    let claim_status =
        PrimaryNameClaimStatus::parse(&crate::sql_row::get::<String>(&row, "claim_status")?)?;
    let raw_claim_name = crate::sql_row::get::<Option<String>>(&row, "raw_claim_name")?;
    let claim_name_is_normalized = crate::sql_row::get::<bool>(&row, "claim_name_is_normalized")?;
    Ok(PrimaryNameCurrentSnapshot {
        normalized_claim_name: normalized_claim_name(
            claim_status,
            claim_name_is_normalized,
            raw_claim_name.as_deref(),
        )
        .with_context(|| format!("primary_names_current row {address}:{namespace}:{coin_type}"))?,
        row: PrimaryNameCurrentRow {
            address,
            namespace,
            coin_type,
            claim_status,
            raw_claim_name,
            claim_provenance: crate::sql_row::get(&row, "claim_provenance")?,
        },
        claim_name_is_normalized,
    })
}

/// The projection stores the raw claim spelling plus a marker for whether those bytes were already
/// normalized; it stores no normalized column. When the marker is set the stored bytes are the
/// normalized form and are returned as published, so a later normalizer revision cannot silently
/// restate an already-published name. Otherwise a successful claim is normalizable by construction
/// — the projection classifies anything else `invalid_name` — so derive it once here rather than
/// leaving each reader to repair a null or re-derive the rule.
pub fn normalized_claim_name(
    claim_status: PrimaryNameClaimStatus,
    claim_name_is_normalized: bool,
    raw_claim_name: Option<&str>,
) -> Result<Option<String>> {
    if claim_status != PrimaryNameClaimStatus::Success {
        return Ok(None);
    }
    if claim_name_is_normalized {
        return Ok(raw_claim_name.map(str::to_owned));
    }
    raw_claim_name
        .map(|raw_claim_name| {
            bigname_domain::normalization::normalize_name(raw_claim_name)
                .map(|normalized| normalized.normalized_name)
                .map_err(anyhow::Error::from)
        })
        .transpose()
        .context("successful primary-name claim whose raw name does not normalize")
}
