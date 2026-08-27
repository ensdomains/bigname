use anyhow::Context;
use serde_json::Value;

use super::support::{
    RegistryGroup, associate_event, correlation_id, event_evidence, logical_edge_identity,
    mark_direct_position, observation_evidence, required_position, same_transaction, value_str,
};
use super::{CANDIDATE, MIGRATION_FAMILY};
use crate::schema_v2::{
    BatchOutput, MigrationDiscoveryAssociation, catalog::Catalog, protocol::MigrationObservation,
};

pub(super) const CORRELATION_KIND: &str = "migration_registry_creation";

pub(super) fn correlate_registry_creation(
    catalog: &Catalog,
    observations: &[&MigrationObservation],
    locked_controller: &str,
    output: &mut BatchOutput,
) -> anyhow::Result<Vec<RegistryGroup>> {
    let mut groups = Vec::new();
    for factory in observations.iter().copied().filter(|observation| {
        proxy_sender(observation)
            .is_some_and(|sender| sender.eq_ignore_ascii_case(locked_controller))
    }) {
        if let Some(group) = admit(catalog, factory, output)? {
            groups.push(group);
        }
    }
    back_associate(&groups, output)?;
    Ok(groups)
}

/// The deployer of a migration-created registry proxy. A locked `.eth` second-level name names the
/// locked controller here; a locked child names its parent's own migration registry, because
/// `WrapperRegistry` inherits the same receiver and deploys from itself.
/// (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L32 @ ens_v2@a971bd64)
/// (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L149 @ ens_v2_sepolia_20260629@ccaeb58)
pub(super) fn proxy_sender(observation: &MigrationObservation) -> Option<&str> {
    (observation.event_name == "ProxyDeployed")
        .then(|| observation.decoded.get("sender").and_then(Value::as_str))
        .flatten()
}

/// Admits one migration-created registry from its factory log and the ordinary `RegistryCreated`
/// announcement that precedes it in the same transaction. The CREATE2 salt is the migrated name's
/// namehash, so the factory log alone names both the registry and the name it belongs to.
/// (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L151 @ ens_v2_sepolia_20260629@ccaeb58)
pub(super) fn admit(
    catalog: &Catalog,
    factory: &MigrationObservation,
    output: &mut BatchOutput,
) -> anyhow::Result<Option<RegistryGroup>> {
    let proxy = value_str(&factory.decoded, "proxy_address")?.to_ascii_lowercase();
    let logical_name_id = format!("ens:{}", value_str(&factory.decoded, "salt")?);
    let Some(registry_event) = output
        .normalized_events
        .iter()
        .find(|event| {
            event.source_family == "ens_v2_registry_l1"
                && event.event_kind == "RegistryCreated"
                && same_transaction(event, &factory.raw)
                && event
                    .log_index
                    .is_some_and(|index| index < factory.raw.log_index)
                && event
                    .after_state
                    .get("registry")
                    .and_then(Value::as_str)
                    .is_some_and(|address| address.eq_ignore_ascii_case(&proxy))
        })
        .cloned()
    else {
        return Ok(None);
    };
    let registry_instance = registry_event
        .after_state
        .get("contract_instance_id")
        .and_then(Value::as_str)
        .context("RegistryCreated event has no contract instance")?
        .parse::<uuid::Uuid>()
        .context("RegistryCreated contract instance is malformed")?;
    let edge = output
        .discovery_edges
        .iter()
        .find(|edge| {
            edge.edge_kind == "registry_announcement"
                && edge.to_contract_instance_id == registry_instance
                && edge.active_from_block_hash == registry_event.block_hash.as_deref().unwrap_or("")
        })
        .cloned()
        .context("RegistryCreated event has no ordinary registry-announcement edge")?;
    let evidence = vec![
        event_evidence(&registry_event),
        observation_evidence(factory),
    ];
    let id = correlation_id(CORRELATION_KIND, Some(&logical_name_id), &evidence);
    mark_direct_position(output, &factory.raw, &id);
    associate_event(
        output,
        &registry_event.event_identity,
        &id,
        CORRELATION_KIND,
        evidence.clone(),
    )?;
    let source = catalog
        .source(edge.source_manifest_id)
        .context("registry-announcement edge has no active source manifest")?;
    output
        .migration_discovery_associations
        .push(MigrationDiscoveryAssociation {
            logical_edge_identity: logical_edge_identity(&edge, source)?,
            migration_correlation_id: id.clone(),
            registry_contract_instance_id: registry_instance,
            registry_address: proxy.clone(),
            source_manifest_id: edge.source_manifest_id,
            evidence_refs: Value::Array(evidence.clone()),
            chain_id: registry_event.chain_id.clone(),
            block_number: required_position(&registry_event)?.0,
            block_hash: registry_event
                .block_hash
                .clone()
                .expect("required position"),
            transaction_hash: registry_event
                .transaction_hash
                .clone()
                .expect("required position"),
            transaction_index: registry_event.transaction_index.expect("required position"),
            log_index: registry_event.log_index.expect("required position"),
            canonicality_state: registry_event.canonicality_state.clone(),
            consumer_visibility: CANDIDATE.to_owned(),
        });
    Ok(Some(RegistryGroup {
        correlation_id: id,
        logical_name_id,
        registry_address: proxy,
        evidence,
        completion_log_index: factory.raw.log_index,
        completion_position: (
            factory.raw.block_number,
            factory.raw.transaction_index,
            factory.raw.log_index,
        ),
    }))
}

/// Attaches the registry's correlation to every same-batch fact the new registry emits, so a proxy
/// event that arrives before the batch ends carries the same provenance a restored batch derives.
pub(super) fn back_associate(
    groups: &[RegistryGroup],
    output: &mut BatchOutput,
) -> anyhow::Result<()> {
    for group in groups {
        let identities = output
            .normalized_events
            .iter()
            .filter(|event| {
                event.source_family != MIGRATION_FAMILY
                    && event
                        .raw_fact_ref
                        .get("emitting_address")
                        .and_then(Value::as_str)
                        .is_some_and(|address| {
                            address.eq_ignore_ascii_case(&group.registry_address)
                        })
            })
            .map(|event| event.event_identity.clone())
            .collect::<Vec<_>>();
        for identity in identities {
            associate_event(
                output,
                &identity,
                &group.correlation_id,
                CORRELATION_KIND,
                group.evidence.clone(),
            )?;
        }
    }
    Ok(())
}
