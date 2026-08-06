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
    Ok(PrimaryNameCurrentSnapshot {
        normalized_claim_name: normalized_claim_name(
            claim_status,
            raw_claim_name.as_deref(),
            &address,
            &namespace,
            &coin_type,
        )?,
        row: PrimaryNameCurrentRow {
            address,
            namespace,
            coin_type,
            claim_status,
            raw_claim_name,
            claim_provenance: crate::sql_row::get(&row, "claim_provenance")?,
        },
        claim_name_is_normalized: crate::sql_row::get(&row, "claim_name_is_normalized")?,
    })
}

/// The projection stores the raw claim spelling plus a marker for whether those bytes were already
/// normalized; it does not store the normalized form. A successful claim is normalizable by
/// construction, so derive it here rather than leaving each caller to repair a null.
fn normalized_claim_name(
    claim_status: PrimaryNameClaimStatus,
    raw_claim_name: Option<&str>,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> Result<Option<String>> {
    if claim_status != PrimaryNameClaimStatus::Success {
        return Ok(None);
    }
    raw_claim_name
        .map(|raw_claim_name| {
            bigname_domain::normalization::normalize_name(raw_claim_name)
                .map(|normalized| normalized.normalized_name)
                .map_err(anyhow::Error::from)
        })
        .transpose()
        .with_context(|| {
            format!(
                "primary_names_current row {address}:{namespace}:{coin_type} has a successful claim whose raw name does not normalize"
            )
        })
}
