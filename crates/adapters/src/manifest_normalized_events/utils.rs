use std::collections::HashMap;

use anyhow::{Context, Result};
use bigname_manifests::{ManifestDeclaredContractDriftInput, ManifestDriftInputs};
use serde_json::Value;

pub(super) fn active_proxy_contracts_by_manifest(
    drift_inputs: &ManifestDriftInputs,
) -> HashMap<i64, Vec<ManifestDeclaredContractDriftInput>> {
    let mut grouped = HashMap::<i64, Vec<ManifestDeclaredContractDriftInput>>::new();
    for contract in &drift_inputs.declared_contracts {
        if contract.declaration_kind == "contract"
            && contract.implementation_contract_instance_id.is_some()
            && contract.declared_implementation_address.is_some()
        {
            grouped
                .entry(contract.manifest_id)
                .or_default()
                .push(contract.clone());
        }
    }
    for rows in grouped.values_mut() {
        rows.sort_by(|left, right| {
            (
                left.role.as_deref().unwrap_or_default(),
                left.declared_address.as_str(),
                left.declared_implementation_address
                    .as_deref()
                    .unwrap_or_default(),
            )
                .cmp(&(
                    right.role.as_deref().unwrap_or_default(),
                    right.declared_address.as_str(),
                    right
                        .declared_implementation_address
                        .as_deref()
                        .unwrap_or_default(),
                ))
        });
    }
    grouped
}

pub(super) fn event_identity(prefix: &str, key: Value) -> Result<String> {
    Ok(format!(
        "{prefix}:{}",
        serde_json::to_string(&key).context("failed to serialize normalized-event identity")?
    ))
}

pub(super) fn manifest_version_i64(manifest_version: u64) -> Result<i64> {
    i64::try_from(manifest_version).context("manifest_version does not fit in i64")
}
