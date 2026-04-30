mod details;
mod positions;
#[cfg(test)]
mod selectors;
mod storage;
mod topology;

use anyhow::{Context, Result, bail};
use sqlx::{Postgres, Transaction};

use crate::persistence::PersistEnsExactNameVerifiedResolutionRequest;
use crate::validation::extract_requested_selectors;
use crate::{BASENAMES_NAMESPACE, ENS_NAMESPACE};

use storage::{
    load_name_current_for_revalidation, load_supported_record_inventory_current_for_revalidation,
};
use topology::{
    build_resolution_topology_for_revalidation, ensure_storage_supported_boundary_matches_request,
};

#[cfg(test)]
pub(crate) use positions::build_requested_chain_positions_from_projection;

pub(crate) async fn revalidate_supported_resolution_persistence_from_storage(
    transaction: &mut Transaction<'_, Postgres>,
    request: &PersistEnsExactNameVerifiedResolutionRequest,
) -> Result<()> {
    let requested_selectors = extract_requested_selectors(&request.trace)?;
    let logical_name_id = format!(
        "{}:{}",
        request.trace.namespace, requested_selectors.surface
    );
    let context = match request.trace.namespace.as_str() {
        ENS_NAMESPACE => "ENS verified-resolution storage revalidation",
        BASENAMES_NAMESPACE => "Basenames verified-resolution storage revalidation",
        other => bail!("{other} verified-resolution storage revalidation is unsupported"),
    };

    let row = load_name_current_for_revalidation(transaction, &logical_name_id)
        .await?
        .with_context(|| {
            format!("{context} requires name_current row for logical_name_id {logical_name_id}")
        })?;
    let record_inventory_row = load_supported_record_inventory_current_for_revalidation(
        transaction,
        &row,
        &request.outcome.cache_key.record_version_boundary,
    )
    .await
    .with_context(|| {
        format!(
            "{context} failed to load supported record_inventory_current for logical_name_id {logical_name_id}"
        )
    })?;

    let stored_manifest_versions = positions::normalize_manifest_versions_for_revalidation(
        row.provenance
            .as_object()
            .and_then(|object| object.get("manifest_versions"))
            .with_context(|| {
                format!("{context} name_current provenance must include manifest_versions")
            })?,
        &format!("{context} name_current provenance.manifest_versions"),
    )?;
    let outcome_manifest_versions = positions::normalize_manifest_versions_for_revalidation(
        &request.outcome.cache_key.manifest_versions,
        &format!("{context} cache_key.manifest_versions"),
    )?;
    if stored_manifest_versions != outcome_manifest_versions {
        bail!(
            "{context} cache_key.manifest_versions must match name_current provenance.manifest_versions"
        );
    }

    let outcome_requested_positions = positions::normalize_requested_chain_positions(
        Some(&request.outcome.cache_key.requested_chain_positions),
        &format!("{context} cache_key.requested_chain_positions"),
    )?;
    positions::ensure_requested_positions_are_eligible_for_projection(
        transaction,
        &row,
        &outcome_requested_positions,
        context,
    )
    .await?;
    if let Some(record_inventory_row) = record_inventory_row.as_ref() {
        positions::ensure_requested_positions_are_eligible_for_record_inventory_projection(
            transaction,
            record_inventory_row,
            &outcome_requested_positions,
            request.trace.namespace == BASENAMES_NAMESPACE,
            context,
        )
        .await?;
    }

    let topology = build_resolution_topology_for_revalidation(&row, record_inventory_row.as_ref())?;
    let support_boundary = bigname_storage::try_resolution_verified_support_boundary(
        &row,
        record_inventory_row.as_ref(),
    )?
    .with_context(|| {
        format!(
            "{context} could not re-establish a supported mixed-route topology boundary for logical_name_id {logical_name_id}"
        )
    })?;

    ensure_storage_supported_boundary_matches_request(
        request,
        &requested_selectors,
        &topology,
        &support_boundary,
        context,
    )?;
    Ok(())
}

pub(crate) fn normalize_alias_detail(
    value: Option<&serde_json::Value>,
    namespace: &str,
) -> Result<serde_json::Value> {
    details::normalize_alias_detail(value, namespace)
}

pub(crate) fn normalize_wildcard_detail(
    value: Option<&serde_json::Value>,
    namespace: &str,
) -> Result<serde_json::Value> {
    details::normalize_wildcard_detail(value, namespace)
}

pub(crate) fn normalize_transport_detail(
    value: Option<&serde_json::Value>,
) -> Result<serde_json::Value> {
    details::normalize_transport_detail(value)
}

#[cfg(test)]
pub(crate) fn selector_family_and_key(
    record_key: &str,
    selector: &bigname_storage::SupportedVerifiedResolutionRecordKey,
) -> (String, Option<String>) {
    selectors::selector_family_and_key(record_key, selector)
}
