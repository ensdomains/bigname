use anyhow::{Context, bail};

use super::{
    catalog::{Catalog, Selected},
    common::{contract_id, normalize_address, provenance},
    model::{
        AddressAdmissionInput, BatchOutput, ContractAddress, ContractInstance, DiscoveryEdge,
        DiscoveryEdgeClosure, RawLogInput,
    },
    protocol::DiscoveryDraft,
};

const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

pub(super) fn materialize(
    catalog: &mut Catalog,
    selected: &Selected,
    raw: &RawLogInput,
    discoveries: Vec<DiscoveryDraft>,
    output: &mut BatchOutput,
) -> anyhow::Result<()> {
    for discovery in discoveries {
        match discovery {
            DiscoveryDraft::RegistryAnnouncement => {
                let rule = catalog
                    .rule(
                        selected.source.manifest_id,
                        "registry_announcement",
                        selected.emitter_role.as_deref(),
                    )
                    .context(
                        "RegistryCreated is not admitted by a registry_announcement manifest rule",
                    )?;
                let address = normalize_address(&raw.emitting_address)?;
                let instance = selected.contract_instance_id;
                let observation_key = format!("registry-announcement:{address}");
                push_contract(output, selected, raw, instance, &address, "RegistryCreated");
                output.discovery_edges.push(DiscoveryEdge {
                    chain_id: raw.chain_id.clone(),
                    edge_kind: "registry_announcement".to_owned(),
                    from_contract_instance_id: instance,
                    to_contract_instance_id: instance,
                    discovery_source: "RegistryCreated".to_owned(),
                    admission_basis: rule.admission.clone(),
                    source_manifest_id: selected.source.manifest_id,
                    observation_key: observation_key.clone(),
                    active_from_block_number: raw.block_number,
                    active_from_block_hash: raw.block_hash.clone(),
                    canonicality_state: raw.canonicality_state.clone(),
                    provenance: discovery_provenance(
                        raw,
                        "RegistryCreated",
                        selected.source.manifest_id,
                        &observation_key,
                    ),
                });
                catalog.admit(AddressAdmissionInput {
                    address,
                    contract_instance_id: instance,
                    source_manifest_id: Some(selected.source.manifest_id),
                    role: None,
                    discovery_edge_kind: Some("registry_announcement".to_owned()),
                    discovery_from_contract_instance_id: Some(instance),
                    discovery_observation_key: Some(observation_key),
                    active_from_block: Some(raw.block_number),
                    active_to_block: None,
                });
            }
            DiscoveryDraft::Close {
                edge_kind,
                observation_key,
            } => {
                catalog.retire(&edge_kind, selected.contract_instance_id, &observation_key);
                output.discovery_edge_closures.push(DiscoveryEdgeClosure {
                    chain_id: raw.chain_id.clone(),
                    edge_kind,
                    from_contract_instance_id: selected.contract_instance_id,
                    observation_key,
                    except_to_contract_instance_id: None,
                    active_to_block_number: raw.block_number,
                    active_to_block_hash: raw.block_hash.clone(),
                    transaction_index: raw.transaction_index,
                    log_index: raw.log_index,
                });
            }
            DiscoveryDraft::Edge {
                edge_kind,
                to_address,
                admission_basis,
                observation_key,
            } => {
                let address = normalize_address(&to_address)?;
                let rule_basis = if edge_kind == "proxy_implementation" {
                    admission_basis
                } else {
                    catalog
                        .rule(
                            selected.source.manifest_id,
                            &edge_kind,
                            selected.emitter_role.as_deref(),
                        )
                        .with_context(|| {
                            format!(
                                "{} is not admitted by a {edge_kind} manifest rule",
                                selected.event.name
                            )
                        })?
                        .admission
                        .clone()
                };
                output.discovery_edge_closures.push(DiscoveryEdgeClosure {
                    chain_id: raw.chain_id.clone(),
                    edge_kind: edge_kind.clone(),
                    from_contract_instance_id: selected.contract_instance_id,
                    observation_key: observation_key.clone(),
                    except_to_contract_instance_id: (address != ZERO_ADDRESS)
                        .then(|| contract_id(&raw.chain_id, &address)),
                    active_to_block_number: raw.block_number,
                    active_to_block_hash: raw.block_hash.clone(),
                    transaction_index: raw.transaction_index,
                    log_index: raw.log_index,
                });
                catalog.retire(&edge_kind, selected.contract_instance_id, &observation_key);
                if address == ZERO_ADDRESS {
                    continue;
                }
                let target = catalog
                    .contract_instance_for_address(&address, raw.block_number)?
                    .unwrap_or_else(|| contract_id(&raw.chain_id, &address));
                if target == selected.contract_instance_id {
                    bail!(
                        "{} produced a non-announcement self-edge of kind {edge_kind}",
                        selected.event.name
                    );
                }
                push_contract(
                    output,
                    selected,
                    raw,
                    target,
                    &address,
                    &selected.event.name,
                );
                output.discovery_edges.push(DiscoveryEdge {
                    chain_id: raw.chain_id.clone(),
                    edge_kind: edge_kind.clone(),
                    from_contract_instance_id: selected.contract_instance_id,
                    to_contract_instance_id: target,
                    discovery_source: selected.event.name.clone(),
                    admission_basis: rule_basis,
                    source_manifest_id: selected.source.manifest_id,
                    observation_key: observation_key.clone(),
                    active_from_block_number: raw.block_number,
                    active_from_block_hash: raw.block_hash.clone(),
                    canonicality_state: raw.canonicality_state.clone(),
                    provenance: discovery_provenance(
                        raw,
                        &selected.event.name,
                        selected.source.manifest_id,
                        &observation_key,
                    ),
                });
                if edge_kind == "resolver" {
                    catalog.admit(AddressAdmissionInput {
                        address,
                        contract_instance_id: target,
                        source_manifest_id: Some(selected.source.manifest_id),
                        role: None,
                        discovery_edge_kind: Some(edge_kind),
                        discovery_from_contract_instance_id: Some(selected.contract_instance_id),
                        discovery_observation_key: Some(observation_key),
                        active_from_block: Some(raw.block_number),
                        active_to_block: None,
                    });
                }
            }
        }
    }
    Ok(())
}

fn discovery_provenance(
    raw: &RawLogInput,
    source_event: &str,
    manifest_id: i64,
    observation_key: &str,
) -> serde_json::Value {
    let mut value = provenance(raw, source_event, manifest_id);
    value
        .as_object_mut()
        .expect("discovery provenance is an object")
        .insert(
            "observation_key".to_owned(),
            serde_json::Value::String(observation_key.to_owned()),
        );
    value
}

fn push_contract(
    output: &mut BatchOutput,
    selected: &Selected,
    raw: &RawLogInput,
    instance: uuid::Uuid,
    address: &str,
    source_event: &str,
) {
    let source_provenance = provenance(raw, source_event, selected.source.manifest_id);
    output.contract_instances.push(ContractInstance {
        contract_instance_id: instance,
        chain_id: raw.chain_id.clone(),
        contract_kind: "contract".to_owned(),
        provenance: source_provenance.clone(),
    });
    output.contract_addresses.push(ContractAddress {
        contract_instance_id: instance,
        chain_id: raw.chain_id.clone(),
        address: address.to_owned(),
        active_from_block_number: raw.block_number,
        active_from_block_hash: raw.block_hash.clone(),
        source_manifest_id: selected.source.manifest_id,
        provenance: source_provenance,
    });
}
