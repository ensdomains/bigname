use anyhow::{Context, Result};
use sqlx::{Row, postgres::PgRow};

use super::types::DeclaredChildEventSource;

pub(super) fn decode_declared_child_event_source(row: PgRow) -> Result<DeclaredChildEventSource> {
    Ok(DeclaredChildEventSource {
        parent_logical_name_id: row
            .try_get("parent_logical_name_id")
            .context("missing parent_logical_name_id")?,
        child_logical_name_id: row
            .try_get("child_logical_name_id")
            .context("missing child_logical_name_id")?,
        namespace: row.try_get("namespace").context("missing namespace")?,
        canonical_display_name: row
            .try_get("canonical_display_name")
            .context("missing canonical_display_name")?,
        normalized_name: row
            .try_get("normalized_name")
            .context("missing normalized_name")?,
        namehash: row.try_get("namehash").context("missing namehash")?,
        normalized_event_id: row
            .try_get("normalized_event_id")
            .context("missing normalized_event_id")?,
        event_identity: row
            .try_get("event_identity")
            .context("missing event_identity")?,
        source_family: row
            .try_get("source_family")
            .context("missing source_family")?,
        manifest_version: row
            .try_get("manifest_version")
            .context("missing manifest_version")?,
        source_manifest_id: row
            .try_get("source_manifest_id")
            .context("missing source_manifest_id")?,
        chain_id: row
            .try_get::<Option<String>, _>("chain_id")
            .context("missing chain_id")?
            .context("declared child source is missing chain_id")?,
        block_number: row
            .try_get::<Option<i64>, _>("block_number")
            .context("missing block_number")?
            .context("declared child source is missing block_number")?,
        block_hash: row
            .try_get::<Option<String>, _>("block_hash")
            .context("missing block_hash")?
            .context("declared child source is missing block_hash")?,
        transaction_hash: row
            .try_get::<Option<String>, _>("transaction_hash")
            .context("missing transaction_hash")?
            .context("declared child source is missing transaction_hash")?,
        log_index: row
            .try_get::<Option<i64>, _>("log_index")
            .context("missing log_index")?
            .context("declared child source is missing log_index")?,
        raw_fact_ref: row
            .try_get("raw_fact_ref")
            .context("missing raw_fact_ref")?,
        normalized_event_ids: row
            .try_get("normalized_event_ids")
            .context("missing normalized_event_ids")?,
        raw_fact_refs: row
            .try_get("raw_fact_refs")
            .context("missing raw_fact_refs")?,
        manifest_versions: row
            .try_get("manifest_versions")
            .context("missing manifest_versions")?,
    })
}
