use std::collections::HashMap;

use anyhow::Result;
use bigname_manifests::{
    ManifestDeclaredContractDriftInput, ManifestDriftActiveManifest, ManifestDriftInputs,
    ManifestRuntimeProgress,
};
use bigname_storage::{CanonicalityState, NormalizedEvent};
use serde_json::json;
use sqlx::PgPool;

use super::constants::{
    DERIVATION_KIND_MANIFEST_SYNC, EVENT_KIND_CAPABILITY_CHANGED,
    EVENT_KIND_PROXY_IMPLEMENTATION_CHANGED, EVENT_KIND_SOURCE_MANIFEST_UPDATED,
};
use super::drift_alerts::{
    build_code_hash_drift_alert_events_with_progress, build_proxy_implementation_alert_event,
};
use super::types::ActiveCapabilityRow;
use super::utils::{event_identity, manifest_version_i64};

const MANIFEST_EVENT_BUILD_PROGRESS_ROWS: usize = 1_000;

pub(super) async fn build_normalized_events(
    pool: &PgPool,
    drift_inputs: &ManifestDriftInputs,
    capabilities: &HashMap<i64, Vec<ActiveCapabilityRow>>,
    contracts: &HashMap<i64, Vec<ManifestDeclaredContractDriftInput>>,
    progress: &mut Option<&mut dyn ManifestRuntimeProgress>,
) -> Result<Vec<NormalizedEvent>> {
    let mut events = Vec::new();

    for manifest in &drift_inputs.active_manifests {
        events.push(build_source_manifest_updated_event(manifest)?);

        if let Some(capability_rows) = capabilities.get(&manifest.manifest_id) {
            for capability in capability_rows {
                events.push(build_capability_changed_event(manifest, capability)?);
            }
        }

        if let Some(contract_rows) = contracts.get(&manifest.manifest_id) {
            for contract in contract_rows {
                events.push(build_proxy_implementation_changed_event(
                    manifest, contract,
                )?);
            }
        }
    }

    events.extend(
        build_code_hash_drift_alert_events_with_progress(pool, drift_inputs, progress).await?,
    );
    for (index, edge) in drift_inputs.proxy_implementation_edges.iter().enumerate() {
        events.push(build_proxy_implementation_alert_event(edge)?);
        if (index + 1).is_multiple_of(MANIFEST_EVENT_BUILD_PROGRESS_ROWS)
            && let Some(progress) = progress.as_deref_mut()
        {
            progress.record(pool).await?;
        }
    }

    Ok(events)
}

fn build_source_manifest_updated_event(
    manifest: &ManifestDriftActiveManifest,
) -> Result<NormalizedEvent> {
    let namespace = manifest.namespace.clone();
    let source_family = manifest.source_family.clone();
    let chain = manifest.chain.clone();
    let deployment_epoch = manifest.deployment_epoch.clone();
    let normalizer_version = manifest.normalizer_version.clone();
    Ok(NormalizedEvent {
        event_identity: event_identity(
            "manifest_sync:source_manifest_updated",
            json!([
                manifest.manifest_id,
                manifest.manifest_version,
                namespace.clone(),
                source_family.clone(),
                chain.clone(),
                deployment_epoch.clone(),
                normalizer_version.clone(),
            ]),
        )?,
        namespace: namespace.clone(),
        logical_name_id: None,
        resource_id: None,
        event_kind: EVENT_KIND_SOURCE_MANIFEST_UPDATED.to_owned(),
        source_family: source_family.clone(),
        manifest_version: manifest_version_i64(manifest.manifest_version)?,
        source_manifest_id: Some(manifest.manifest_id),
        chain_id: Some(chain.clone()),
        block_number: None,
        block_hash: None,
        transaction_hash: None,
        log_index: None,
        raw_fact_ref: json!({
            "manifest_id": manifest.manifest_id,
            "namespace": namespace.clone(),
            "source_family": source_family.clone(),
            "chain": chain.clone(),
            "deployment_epoch": deployment_epoch.clone(),
        }),
        derivation_kind: DERIVATION_KIND_MANIFEST_SYNC.to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state: json!({
            "manifest_version": manifest.manifest_version,
            "normalizer_version": normalizer_version,
        }),
    })
}

fn build_capability_changed_event(
    manifest: &ManifestDriftActiveManifest,
    capability: &ActiveCapabilityRow,
) -> Result<NormalizedEvent> {
    let namespace = manifest.namespace.clone();
    let source_family = manifest.source_family.clone();
    let chain = manifest.chain.clone();
    let capability_name = capability.capability_name.clone();
    let status = capability.status.clone();
    let notes = capability.notes.clone();
    Ok(NormalizedEvent {
        event_identity: event_identity(
            "manifest_sync:capability_changed",
            json!([
                manifest.manifest_id,
                capability_name.clone(),
                status.clone(),
                notes.clone(),
            ]),
        )?,
        namespace,
        logical_name_id: None,
        resource_id: None,
        event_kind: EVENT_KIND_CAPABILITY_CHANGED.to_owned(),
        source_family,
        manifest_version: manifest_version_i64(manifest.manifest_version)?,
        source_manifest_id: Some(manifest.manifest_id),
        chain_id: Some(chain),
        block_number: None,
        block_hash: None,
        transaction_hash: None,
        log_index: None,
        raw_fact_ref: json!({
            "manifest_id": manifest.manifest_id,
            "capability_name": capability_name.clone(),
        }),
        derivation_kind: DERIVATION_KIND_MANIFEST_SYNC.to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state: json!({
            "capability_name": capability_name,
            "status": status,
            "notes": notes,
        }),
    })
}

fn build_proxy_implementation_changed_event(
    manifest: &ManifestDriftActiveManifest,
    contract: &ManifestDeclaredContractDriftInput,
) -> Result<NormalizedEvent> {
    let namespace = manifest.namespace.clone();
    let source_family = manifest.source_family.clone();
    let chain = manifest.chain.clone();
    let role = contract
        .role
        .clone()
        .unwrap_or_else(|| contract.declaration_name.clone());
    let address = contract.declared_address.clone();
    let proxy_kind = contract.proxy_kind.clone().unwrap_or_default();
    let implementation = contract
        .declared_implementation_address
        .clone()
        .unwrap_or_default();
    Ok(NormalizedEvent {
        event_identity: event_identity(
            "manifest_sync:proxy_implementation_changed",
            json!([
                manifest.manifest_id,
                role.clone(),
                address.clone(),
                proxy_kind.clone(),
                implementation.clone(),
            ]),
        )?,
        namespace,
        logical_name_id: None,
        resource_id: None,
        event_kind: EVENT_KIND_PROXY_IMPLEMENTATION_CHANGED.to_owned(),
        source_family,
        manifest_version: manifest_version_i64(manifest.manifest_version)?,
        source_manifest_id: Some(manifest.manifest_id),
        chain_id: Some(chain),
        block_number: None,
        block_hash: None,
        transaction_hash: None,
        log_index: None,
        raw_fact_ref: json!({
            "manifest_id": manifest.manifest_id,
            "role": role.clone(),
            "address": address.clone(),
        }),
        derivation_kind: DERIVATION_KIND_MANIFEST_SYNC.to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state: json!({
            "role": role,
            "address": address,
            "proxy_kind": proxy_kind,
            "implementation": implementation,
        }),
    })
}
