use anyhow::{Context, Result};
use sqlx::{Row, postgres::PgRow};

use super::types::{PrimaryNameClaimStatus, PrimaryNameCurrentRow, PrimaryNameCurrentSnapshot};

pub(super) fn decode_primary_name_current_snapshot(
    row: PgRow,
) -> Result<PrimaryNameCurrentSnapshot> {
    Ok(PrimaryNameCurrentSnapshot {
        row: PrimaryNameCurrentRow {
            address: row
                .try_get::<String, _>("address")
                .context("missing address")?
                .to_ascii_lowercase(),
            namespace: row.try_get("namespace").context("missing namespace")?,
            coin_type: row.try_get("coin_type").context("missing coin_type")?,
            claim_status: PrimaryNameClaimStatus::parse(
                &row.try_get::<String, _>("claim_status")
                    .context("missing claim_status")?,
            )?,
            raw_claim_name: row
                .try_get("raw_claim_name")
                .context("missing raw_claim_name")?,
            claim_provenance: row
                .try_get("claim_provenance")
                .context("missing claim_provenance")?,
        },
        normalized_claim_name: row
            .try_get("normalized_claim_name")
            .context("missing normalized_claim_name")?,
    })
}
