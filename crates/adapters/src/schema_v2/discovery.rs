use anyhow::Context;

use super::{
    catalog::{Catalog, Selected},
    common::{contract_id, normalize_address, provenance},
    model::{
        AddressAdmissionInput, BatchOutput, ContractAddress, ContractInstance, DiscoveryEdge,
        DiscoveryEdgeClosure, RawBlockInput, RawLogInput,
    },
    protocol::{DiscoveryDraft, EventDraft},
    state::State,
};

const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

/// A discovery pointer that targets its own emitter. Interpret logs it once the block's last pass
/// is known, so a discarded pass logs nothing.
pub(super) struct SelfTarget {
    chain_id: String,
    block_number: i64,
    transaction_hash: String,
    log_index: i64,
    emitting_address: String,
    event: String,
    edge_kind: String,
}

impl SelfTarget {
    pub(super) fn warn(&self) {
        tracing::warn!(
            chain_id = %self.chain_id,
            block_number = self.block_number,
            transaction_hash = %self.transaction_hash,
            log_index = self.log_index,
            emitting_address = %self.emitting_address,
            event = %self.event,
            edge_kind = %self.edge_kind,
            "discovery pointer targets its own emitter; previous edge closed, no edge opened"
        );
    }
}

pub(super) fn materialize(
    catalog: &mut Catalog,
    selected: &Selected,
    raw: &RawLogInput,
    discoveries: Vec<DiscoveryDraft>,
    state: &mut State,
    output: &mut BatchOutput,
    self_targets: &mut Vec<SelfTarget>,
) -> anyhow::Result<()> {
    for discovery in discoveries {
        match discovery {
            DiscoveryDraft::ResolverCreation => {
                let address = normalize_address(&raw.emitting_address)?;
                let instance = selected.contract_instance_id;
                let observation_key = format!("resolver-created:{address}");
                push_contract(
                    output,
                    selected.source.manifest_id,
                    raw,
                    instance,
                    &address,
                    "ResolverCreated",
                );
                output.discovery_edges.push(DiscoveryEdge {
                    chain_id: raw.chain_id.clone(),
                    edge_kind: "resolver".to_owned(),
                    from_contract_instance_id: instance,
                    to_contract_instance_id: instance,
                    discovery_source: "ResolverCreated".to_owned(),
                    admission_basis: "resolver_created".to_owned(),
                    source_manifest_id: selected.source.manifest_id,
                    observation_key: observation_key.clone(),
                    active_from_block_number: raw.block_number,
                    active_from_block_hash: raw.block_hash.clone(),
                    canonicality_state: raw.canonicality_state.clone(),
                    provenance: discovery_provenance(
                        raw,
                        "ResolverCreated",
                        selected.source.manifest_id,
                        &observation_key,
                    ),
                });
                catalog.admit(AddressAdmissionInput {
                    address,
                    contract_instance_id: instance,
                    source_manifest_id: Some(selected.source.manifest_id),
                    role: None,
                    discovery_edge_kind: Some("resolver".to_owned()),
                    discovery_from_contract_instance_id: Some(instance),
                    discovery_observation_key: Some(observation_key),
                    active_from_block: Some(raw.block_number),
                    active_to_block: None,
                });
            }
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
                push_contract(
                    output,
                    selected.source.manifest_id,
                    raw,
                    instance,
                    &address,
                    "RegistryCreated",
                );
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
                // Only announcements may produce self-edges, so a self-target, declared or
                // discovery-admitted, opens no edge and the previous edge closed above stays
                // closed. A registry may point one of its own labels back at itself as subregistry:
                // the ETHRegistrar passes a registrant's chosen subregistry straight to the
                // ETHRegistry, which emits it, and the Sepolia ETHRegistry has emitted itself this
                // way (block 11840453, pinned in
                // tests/fixtures/interpreters/v2-registry-self-subregistry.json)
                // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registrar/ETHRegistrar.sol:L151-L158 @ ens_v2_sepolia_20261001@07e55a05)
                // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L508-L510 @ ens_v2_sepolia_20261001@07e55a05).
                // A name below that label walks back into the parent registry and reads the
                // parent's own entries (x.label.eth reads x's entry), so its subnames alias the
                // parent's children rather than living under a registry of their own; the
                // self-pointer gives the label no canonical registry and no new canonical suffix,
                // and bigname models no alias subtree through it
                // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/universalResolver/libraries/LibResolution.sol:L76-L82 @ ens_v2_sepolia_20261001@07e55a05)
                // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/universalResolver/libraries/LibResolution.sol:L127-L160 @ ens_v2_sepolia_20261001@07e55a05).
                // The normalized event is its record. Any other self-target has nothing to
                // discover; it is logged and, like a subregistry self-pointer, writes no
                // operator diagnostic row.
                if target == selected.contract_instance_id {
                    if edge_kind != "subregistry" {
                        self_targets.push(SelfTarget {
                            chain_id: raw.chain_id.clone(),
                            block_number: raw.block_number,
                            transaction_hash: raw.transaction_hash.clone(),
                            log_index: raw.log_index,
                            emitting_address: raw.emitting_address.clone(),
                            event: selected.event.name.clone(),
                            edge_kind: edge_kind.clone(),
                        });
                    }
                    continue;
                }
                push_contract(
                    output,
                    selected.source.manifest_id,
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
                if edge_kind == "resolver"
                    && !matches!(
                        selected.source.source_family.as_str(),
                        "ens_v2_registry_l1" | "ens_v2_root_l1"
                    )
                {
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
            draft @ (DiscoveryDraft::RegistryResolverEdge { .. }
            | DiscoveryDraft::RegistryResolverClose { .. }) => {
                registry_resolver(catalog, raw, &selected.event.name, draft, output)?;
            }
            DiscoveryDraft::ResolverAnnouncement {
                proxy_address,
                implementation,
            } => {
                let implementation = normalize_address(&implementation)?;
                let Some(authority_source) = catalog
                    .resolver_implementation_authority(
                        &selected.source,
                        &implementation,
                        (selected.event.name == "Upgraded").then_some(raw.block_number),
                    )
                    .cloned()
                else {
                    continue;
                };
                let authority = authority_source.manifest_id;
                let address = normalize_address(&proxy_address)?;
                // A factory announcement is the proxy's implementation observation for the
                // support rule, recorded on the proxy's own `Upgraded` stream (same state scope
                // as its ERC-1967 `Upgraded` log) when the authority declares that history.
                if selected.event.name == "ProxyDeployed"
                    && authority_source.events.iter().any(|event| {
                        event.name == "Upgraded"
                            && event
                                .normalized_events
                                .iter()
                                .any(|kind| kind == "Upgraded")
                    })
                {
                    super::normalized::materialize_for_source(
                        &authority_source,
                        raw,
                        vec![EventDraft {
                            event_kind: "Upgraded".to_owned(),
                            logical_name_id: None,
                            resource_id: None,
                            identity_suffix: format!("Upgraded:announced:{address}"),
                            explicit_before: None,
                            after_state: serde_json::json!({
                                "source_event": "ProxyDeployed",
                                "proxy_address": address,
                                "implementation": implementation,
                            }),
                            state_scope: format!("{address}:-:-:-:Upgraded"),
                        }],
                        state,
                        output,
                    );
                }
                let target = catalog
                    .contract_instance_for_address(&address, raw.block_number)?
                    .unwrap_or_else(|| contract_id(&raw.chain_id, &address));
                // A self-announcing proxy (`Upgraded`) has no announcing contract of its own; the
                // edge runs from the implementation instance its `proxy_implementation` edge just
                // materialized. A factory announcement (`ProxyDeployed`) runs from the factory.
                let from = if selected.event.name == "Upgraded" {
                    catalog
                        .contract_instance_for_address(&implementation, raw.block_number)?
                        .unwrap_or_else(|| contract_id(&raw.chain_id, &implementation))
                } else {
                    selected.contract_instance_id
                };
                let observation_key = format!(
                    "resolver-announcement:{}:{address}",
                    selected.event.name.to_ascii_lowercase()
                );
                push_contract(
                    output,
                    authority,
                    raw,
                    target,
                    &address,
                    &selected.event.name,
                );
                output.discovery_edges.push(DiscoveryEdge {
                    chain_id: raw.chain_id.clone(),
                    edge_kind: "resolver".to_owned(),
                    from_contract_instance_id: from,
                    to_contract_instance_id: target,
                    discovery_source: selected.event.name.clone(),
                    admission_basis: super::catalog::ANNOUNCEMENT_ADMISSION_BASIS.to_owned(),
                    source_manifest_id: authority,
                    observation_key: observation_key.clone(),
                    active_from_block_number: raw.block_number,
                    active_from_block_hash: raw.block_hash.clone(),
                    canonicality_state: raw.canonicality_state.clone(),
                    provenance: discovery_provenance(
                        raw,
                        &selected.event.name,
                        authority,
                        &observation_key,
                    ),
                });
                catalog.admit(AddressAdmissionInput {
                    address,
                    contract_instance_id: target,
                    source_manifest_id: Some(authority),
                    role: None,
                    discovery_edge_kind: Some("resolver".to_owned()),
                    discovery_from_contract_instance_id: Some(from),
                    discovery_observation_key: Some(observation_key),
                    active_from_block: Some(raw.block_number),
                    active_to_block: None,
                });
            }
        }
    }
    Ok(())
}

/// The resolver edges of registries whose reachability a block boundary changed, positioned at
/// the block's start with no transaction. An expiry crossing usually takes reachability away, but
/// it can also give it: a registry whose parent claim points back at an unreachable parent falls
/// back to a reachable mount when the claimed entry expires.
pub(super) fn materialize_boundary(
    catalog: &Catalog,
    block: &RawBlockInput,
    drafts: Vec<DiscoveryDraft>,
    output: &mut BatchOutput,
) -> anyhow::Result<()> {
    for draft in drafts {
        let (DiscoveryDraft::RegistryResolverEdge { registry, .. }
        | DiscoveryDraft::RegistryResolverClose { registry, .. }) = &draft
        else {
            anyhow::bail!("an ENSv2 block boundary produced a non-registry discovery draft");
        };
        let raw = RawLogInput {
            chain_id: block.chain_id.clone(),
            block_hash: block.block_hash.clone(),
            block_number: block.block_number,
            block_timestamp: block.block_timestamp,
            canonicality_state: block.canonicality_state.clone(),
            transaction_hash: format!("block-boundary:{}", block.block_hash),
            transaction_index: -1,
            log_index: -1,
            emitting_address: registry.clone(),
            topics: Vec::new(),
            data: Vec::new(),
        };
        registry_resolver(catalog, &raw, "RegistryPathExpired", draft, output)?;
    }
    Ok(())
}

/// A registry token's resolver edge opened or closed by a change in the registry's
/// reachability. It runs from the instance, manifest and rule the registry's own
/// `ResolverUpdated` would select, with the key that log writes, so the registry's next pointer
/// log closes it. A registry nothing would interpret a `ResolverUpdated` for gets no edge, as
/// before. A registry pointer admits nothing.
fn registry_resolver(
    catalog: &Catalog,
    raw: &RawLogInput,
    source_event: &str,
    draft: DiscoveryDraft,
    output: &mut BatchOutput,
) -> anyhow::Result<()> {
    let (registry, observation_key, to_address) = match draft {
        DiscoveryDraft::RegistryResolverEdge {
            registry,
            observation_key,
            to_address,
        } => (registry, observation_key, Some(to_address)),
        DiscoveryDraft::RegistryResolverClose {
            registry,
            observation_key,
        } => (registry, observation_key, None),
        _ => unreachable!("only registry resolver drafts reach this materializer"),
    };
    let probe = RawLogInput {
        emitting_address: registry.clone(),
        topics: vec![
            crate::schema_v2::protocol::v2_registry::reachability::resolver_updated_topic0(),
        ],
        ..raw.clone()
    };
    let Some(selected) = catalog
        .select(&probe)?
        .filter(|selected| selected.event.name == "ResolverUpdated")
    else {
        return Ok(());
    };
    let from = selected.contract_instance_id;
    let target = match to_address.as_deref().map(normalize_address).transpose()? {
        Some(address) if address != ZERO_ADDRESS => Some((
            catalog
                .contract_instance_for_address(&address, raw.block_number)?
                .unwrap_or_else(|| contract_id(&raw.chain_id, &address)),
            address,
        )),
        _ => None,
    };
    output.discovery_edge_closures.push(DiscoveryEdgeClosure {
        chain_id: raw.chain_id.clone(),
        edge_kind: "resolver".to_owned(),
        from_contract_instance_id: from,
        observation_key: observation_key.clone(),
        except_to_contract_instance_id: target.as_ref().map(|(instance, _)| *instance),
        active_to_block_number: raw.block_number,
        active_to_block_hash: raw.block_hash.clone(),
        transaction_index: raw.transaction_index,
        log_index: raw.log_index,
    });
    let Some((target, address)) = target.filter(|(target, _)| *target != from) else {
        return Ok(());
    };
    let manifest_id = selected.source.manifest_id;
    let admission_basis = catalog
        .rule(manifest_id, "resolver", selected.emitter_role.as_deref())
        .with_context(|| format!("ENSv2 registry {registry} has no resolver manifest rule"))?
        .admission
        .clone();
    push_contract(output, manifest_id, raw, target, &address, source_event);
    let provenance = if raw.log_index < 0 {
        // A block boundary has no log position. The writer orders a raw-block edge before the
        // block's logs, as it does a boundary's binding.
        let mut value = crate::schema_v2::seam::raw_block_provenance();
        let fields = value
            .as_object_mut()
            .expect("raw-block provenance is an object");
        fields.insert("source_event".to_owned(), source_event.into());
        fields.insert("source_manifest_id".to_owned(), manifest_id.into());
        fields.insert("chain_id".to_owned(), raw.chain_id.clone().into());
        fields.insert("block_hash".to_owned(), raw.block_hash.clone().into());
        fields.insert("block_number".to_owned(), raw.block_number.into());
        fields.insert("emitting_address".to_owned(), registry.clone().into());
        fields.insert("observation_key".to_owned(), observation_key.clone().into());
        value
    } else {
        discovery_provenance(raw, source_event, manifest_id, &observation_key)
    };
    output.discovery_edges.push(DiscoveryEdge {
        chain_id: raw.chain_id.clone(),
        edge_kind: "resolver".to_owned(),
        from_contract_instance_id: from,
        to_contract_instance_id: target,
        discovery_source: source_event.to_owned(),
        admission_basis,
        source_manifest_id: manifest_id,
        observation_key,
        active_from_block_number: raw.block_number,
        active_from_block_hash: raw.block_hash.clone(),
        canonicality_state: raw.canonicality_state.clone(),
        provenance,
    });
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
    manifest_id: i64,
    raw: &RawLogInput,
    instance: uuid::Uuid,
    address: &str,
    source_event: &str,
) {
    let source_provenance = provenance(raw, source_event, manifest_id);
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
        source_manifest_id: manifest_id,
        provenance: source_provenance,
    });
}
