use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use uuid::Uuid;

use super::contract_resolution::{
    resolve_contract_instance_by_address, resolve_manifest_entry_contract_instance_id,
};
use crate::{
    CONTRACT_KIND_CONTRACT, CONTRACT_KIND_ROOT, DECLARATION_KIND_CONTRACT, DECLARATION_KIND_ROOT,
    LoadedManifest, normalize_address,
    support::{DeclarationKey, PersistedManifestEntry},
};

pub(super) fn declared_start_block_for_entry(
    loaded_manifest: &LoadedManifest,
    key: &DeclarationKey,
) -> Result<Option<i64>> {
    let start_block = match key.declaration_kind.as_str() {
        DECLARATION_KIND_ROOT => loaded_manifest
            .manifest
            .roots
            .iter()
            .find(|root| root.name == key.declaration_name)
            .and_then(|root| root.start_block),
        DECLARATION_KIND_CONTRACT => loaded_manifest
            .manifest
            .contracts
            .iter()
            .find(|contract| contract.role == key.declaration_name)
            .and_then(|contract| contract.start_block),
        _ => None,
    };

    start_block
        .map(|start_block| {
            i64::try_from(start_block).with_context(|| {
                format!(
                    "start_block {start_block} for {} {} in {} does not fit into BIGINT",
                    key.declaration_kind,
                    key.declaration_name,
                    loaded_manifest.path.display()
                )
            })
        })
        .transpose()
}

pub(super) async fn plan_manifest_entries(
    executor: &mut sqlx::postgres::PgConnection,
    manifest_id: i64,
    loaded_manifest: &LoadedManifest,
    existing_entries: &HashMap<DeclarationKey, PersistedManifestEntry>,
) -> Result<Vec<PersistedManifestEntry>> {
    let mut planned_entries = Vec::new();
    let mut planned_contract_instance_ids_by_address = HashMap::<String, Uuid>::new();

    for root in &loaded_manifest.manifest.roots {
        let key = DeclarationKey {
            declaration_kind: DECLARATION_KIND_ROOT.to_owned(),
            declaration_name: root.name.clone(),
        };
        let declared_address = normalize_address(&root.address);
        let contract_instance_id =
            match planned_contract_instance_ids_by_address.get(&declared_address) {
                Some(contract_instance_id) => *contract_instance_id,
                None => {
                    let contract_instance_id = resolve_manifest_entry_contract_instance_id(
                        executor,
                        manifest_id,
                        loaded_manifest,
                        &key,
                        &declared_address,
                        existing_entries.get(&key),
                        CONTRACT_KIND_ROOT,
                    )
                    .await?;
                    planned_contract_instance_ids_by_address
                        .insert(declared_address.clone(), contract_instance_id);
                    contract_instance_id
                }
            };

        planned_entries.push(PersistedManifestEntry {
            key,
            contract_instance_id,
            declared_address,
            code_hash: root.code_hash.clone(),
            abi_ref: root.abi_ref.clone(),
            role: None,
            proxy_kind: None,
            implementation_contract_instance_id: None,
            declared_implementation_address: None,
        });
    }

    for contract in &loaded_manifest.manifest.contracts {
        validate_manifest_contract_proxy_shape(loaded_manifest, contract)?;

        let key = DeclarationKey {
            declaration_kind: DECLARATION_KIND_CONTRACT.to_owned(),
            declaration_name: contract.role.clone(),
        };
        let declared_address = normalize_address(&contract.address);
        let contract_instance_id =
            match planned_contract_instance_ids_by_address.get(&declared_address) {
                Some(contract_instance_id) => *contract_instance_id,
                None => {
                    let contract_instance_id = resolve_manifest_entry_contract_instance_id(
                        executor,
                        manifest_id,
                        loaded_manifest,
                        &key,
                        &declared_address,
                        existing_entries.get(&key),
                        CONTRACT_KIND_CONTRACT,
                    )
                    .await?;
                    planned_contract_instance_ids_by_address
                        .insert(declared_address.clone(), contract_instance_id);
                    contract_instance_id
                }
            };

        let declared_implementation_address = contract
            .implementation
            .as_ref()
            .map(|value| normalize_address(value));
        if declared_implementation_address.as_deref() == Some(declared_address.as_str()) {
            bail!(
                "manifest contract role {} in {} cannot declare the proxy address as its own implementation",
                contract.role,
                loaded_manifest.path.display()
            );
        }
        let implementation_contract_instance_id =
            if let Some(implementation_address) = &declared_implementation_address {
                Some(
                    resolve_contract_instance_by_address(
                        executor,
                        &loaded_manifest.manifest.chain,
                        implementation_address,
                        CONTRACT_KIND_CONTRACT,
                        &serde_json::json!({
                            "source": "manifest_contract_implementation",
                            "manifest_id": manifest_id,
                            "role": contract.role,
                        }),
                    )
                    .await?,
                )
            } else {
                None
            };

        planned_entries.push(PersistedManifestEntry {
            key,
            contract_instance_id,
            declared_address,
            code_hash: None,
            abi_ref: None,
            role: Some(contract.role.clone()),
            proxy_kind: Some(contract.proxy_kind.clone()),
            implementation_contract_instance_id,
            declared_implementation_address,
        });
    }

    Ok(planned_entries)
}

fn validate_manifest_contract_proxy_shape(
    loaded_manifest: &LoadedManifest,
    contract: &crate::ManifestContract,
) -> Result<()> {
    match (
        contract.proxy_kind.as_str(),
        contract.implementation.as_ref(),
    ) {
        ("none", Some(_)) => bail!(
            "manifest contract role {} in {} cannot declare implementation when proxy_kind = \"none\"",
            contract.role,
            loaded_manifest.path.display()
        ),
        ("none", None) => Ok(()),
        (_, Some(_)) => Ok(()),
        (proxy_kind, None) => bail!(
            "manifest contract role {} in {} must declare implementation when proxy_kind = \"{}\"",
            contract.role,
            loaded_manifest.path.display(),
            proxy_kind
        ),
    }
}
