use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::{Row, postgres::PgRow};

use crate::CanonicalityState;

use super::{HistoryEvent, address_matches::AddressHistoryAnchor};

pub(super) fn decode_address_history_anchor(row: PgRow) -> Result<AddressHistoryAnchor> {
    Ok(AddressHistoryAnchor {
        logical_name_id: row
            .try_get("logical_name_id")
            .context("missing logical_name_id")?,
        resource_id: row.try_get("resource_id").context("missing resource_id")?,
    })
}

pub(super) fn decode_history_event(row: PgRow) -> Result<HistoryEvent> {
    let provenance: Value = row.try_get("provenance").context("missing provenance")?;
    let coverage: Value = row.try_get("coverage").context("missing coverage")?;
    ensure_json_object(&provenance, "provenance")?;
    ensure_json_object(&coverage, "coverage")?;

    Ok(HistoryEvent {
        normalized_event_id: row
            .try_get("normalized_event_id")
            .context("missing normalized_event_id")?,
        event_identity: row
            .try_get("event_identity")
            .context("missing event_identity")?,
        namespace: row.try_get("namespace").context("missing namespace")?,
        logical_name_id: row
            .try_get("logical_name_id")
            .context("missing logical_name_id")?,
        resource_id: row.try_get("resource_id").context("missing resource_id")?,
        event_kind: row.try_get("event_kind").context("missing event_kind")?,
        source_family: row
            .try_get("source_family")
            .context("missing source_family")?,
        manifest_version: row
            .try_get("manifest_version")
            .context("missing manifest_version")?,
        source_manifest_id: row
            .try_get("source_manifest_id")
            .context("missing source_manifest_id")?,
        chain_id: row.try_get("chain_id").context("missing chain_id")?,
        block_number: row
            .try_get("block_number")
            .context("missing block_number")?,
        block_hash: row.try_get("block_hash").context("missing block_hash")?,
        block_timestamp: row
            .try_get("block_timestamp")
            .context("missing block_timestamp")?,
        transaction_hash: row
            .try_get("transaction_hash")
            .context("missing transaction_hash")?,
        log_index: row.try_get("log_index").context("missing log_index")?,
        raw_fact_ref: row
            .try_get("raw_fact_ref")
            .context("missing raw_fact_ref")?,
        derivation_kind: row
            .try_get("derivation_kind")
            .context("missing derivation_kind")?,
        canonicality_state: CanonicalityState::parse(
            &row.try_get::<String, _>("canonicality_state")
                .context("missing canonicality_state")?,
        )?,
        before_state: row
            .try_get("before_state")
            .context("missing before_state")?,
        after_state: row.try_get("after_state").context("missing after_state")?,
        provenance,
        coverage,
    })
}

fn ensure_json_object(value: &Value, field_name: &str) -> Result<()> {
    if !value.is_object() {
        bail!("history field {field_name} must be a JSON object");
    }

    Ok(())
}
