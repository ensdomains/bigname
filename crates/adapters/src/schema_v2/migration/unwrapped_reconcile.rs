use std::collections::BTreeMap;

use alloy_primitives::{Address, B256, U256};
use alloy_sol_types::{SolEvent, sol};
use serde_json::Value;
use uuid::Uuid;

use super::*;
use crate::schema_v2::{
    common::{namehash, stable_uuid},
    model::{RawBlockInput, RawLogInput},
    state::State,
};

sol! {
    event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
    event Transfer(bytes32 indexed node, address owner);
    event NewResolver(bytes32 indexed node, address resolver);
    event NewTTL(bytes32 indexed node, uint64 ttl);
    event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value);
    event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap);
}

/// A transaction-local proof, never a persisted authority or a weaker writer selector.
pub(in crate::schema_v2) struct UnwrappedReconciliation {
    pub logical_name_id: String,
    pub resource_id: Uuid,
    pub registry_resource_id: Uuid,
    pub chain_id: String,
    pub block_hash: String,
    pub transaction_hash: String,
    pub block_number: i64,
    pub transaction_index: i64,
    pub first_log: i64,
    pub cleanup_log: i64,
}

impl UnwrappedReconciliation {
    pub fn contains(&self, event: &NormalizedEvent) -> bool {
        event.chain_id == self.chain_id
            && event.block_hash.as_deref() == Some(&self.block_hash)
            && event.transaction_hash.as_deref() == Some(&self.transaction_hash)
            && event.block_number == Some(self.block_number)
            && event.transaction_index == Some(self.transaction_index)
            && event
                .log_index
                .is_some_and(|log| self.first_log <= log && log <= self.cleanup_log)
            && (event.logical_name_id.as_deref() == Some(&self.logical_name_id)
                || event
                    .resource_id
                    .is_some_and(|id| id == self.resource_id || id == self.registry_resource_id))
            && matches!(
                event.source_family.as_str(),
                "ens_v1_registry_l1" | V1_REGISTRAR_FAMILY
            )
    }
}

pub(in crate::schema_v2) fn unwrapped_reconciliations(
    catalog: &Catalog,
    observations: &[MigrationObservation],
    raw_logs: &[RawLogInput],
    block: &RawBlockInput,
    committed_state: &State,
    output: &BatchOutput,
) -> anyhow::Result<Vec<UnwrappedReconciliation>> {
    let Some(source) = catalog.source_for_family(MIGRATION_FAMILY) else {
        return Ok(Vec::new());
    };
    let Some(base) = catalog.correlation_address(MIGRATION_FAMILY, "ens_v1_base_registrar") else {
        return Ok(Vec::new());
    };
    let graveyard = declared_address(catalog, "graveyard")?;
    let unlocked = declared_address(catalog, "unlocked_migration_controller")?;
    let locked = declared_address(catalog, "locked_migration_controller")?;
    let Some(wrapper) = catalog.correlation_address(MIGRATION_FAMILY, "ens_v1_name_wrapper") else {
        return Ok(Vec::new());
    };
    let mut transactions = BTreeMap::<(&str, &str), Vec<&MigrationObservation>>::new();
    for observation in observations.iter().filter(|observation| {
        is_v1_registrar_observation(observation) && observation.event_name == "Transfer"
    }) {
        transactions
            .entry((
                &observation.raw.block_hash,
                &observation.raw.transaction_hash,
            ))
            .or_default()
            .push(observation);
    }
    if transactions.is_empty() {
        return Ok(Vec::new());
    }
    // Probe only the existing second-level authority correlator. Whole-batch child/factory
    // correlation still runs once at its original boundary after all blocks have been loaded.
    let mut probe = BatchOutput {
        normalized_events: output.normalized_events.clone(),
        surface_bindings: output.surface_bindings.clone(),
        ..BatchOutput::default()
    };
    let mut boundaries = Vec::new();
    for transaction in transactions.values() {
        correlate_authority_transitions(
            source,
            transaction,
            &graveyard,
            &unlocked,
            &locked,
            catalog,
            base,
            wrapper,
            &[],
            &mut probe,
            &mut boundaries,
        )?;
    }
    insert_boundaries(&mut probe, boundaries);
    activate_complete_groups(&mut probe);
    let mut proofs = Vec::new();
    for transition in &probe.migration_authority_transitions {
        let Some(boundary) = probe
            .normalized_events
            .iter()
            .find(|event| event.event_identity == transition.boundary_event_identity)
        else {
            continue;
        };
        if boundary.after_state["migration_path"] != "unwrapped" {
            continue;
        }
        let matching = probe
            .migration_authority_transitions
            .iter()
            .filter(|other| {
                other.logical_name_id == transition.logical_name_id
                    && other.block_number == transition.block_number
                    && other.transaction_index == transition.transaction_index
            })
            .count();
        if matching != 1 {
            continue;
        }
        let transaction = transactions.get(&(
            boundary.block_hash.as_deref().unwrap_or_default(),
            boundary.transaction_hash.as_deref().unwrap_or_default(),
        ));
        let Some(transaction) = transaction else {
            continue;
        };
        let label = transition.predecessor_selector["resource"]["labelhash"]
            .as_str()
            .unwrap_or_default();
        let transfers = transaction
            .iter()
            .copied()
            .filter(|observation| observation.decoded["labelhash"].as_str() == Some(label))
            .collect::<Vec<_>>();
        let [incoming, cleanup] = transfers.as_slice() else {
            continue;
        };
        let prefix = output
            .normalized_events
            .iter()
            .filter(|event| {
                event
                    .transaction_index
                    .zip(event.log_index)
                    .is_some_and(|position| {
                        position < (incoming.raw.transaction_index, incoming.raw.log_index)
                    })
            })
            .cloned()
            .collect::<Vec<_>>();
        let mut predecessor_state = committed_state.clone();
        predecessor_state.apply_prior_event_delta(crate::schema_v2::seam::fold_prior_events(
            Vec::new(),
            &prefix,
            std::slice::from_ref(block),
        )?);
        predecessor_state.ensure_restore_succeeded()?;
        if let Some(proof) = prove(
            catalog,
            boundary,
            incoming,
            cleanup,
            raw_logs,
            &predecessor_state,
            output,
            base,
            &unlocked,
            &graveyard,
        ) {
            proofs.push(proof);
        }
    }
    Ok(proofs)
}

#[allow(clippy::too_many_arguments)]
fn prove(
    catalog: &Catalog,
    boundary: &NormalizedEvent,
    incoming: &MigrationObservation,
    cleanup: &MigrationObservation,
    raw_logs: &[RawLogInput],
    state: &State,
    output: &BatchOutput,
    base: &str,
    controller: &str,
    graveyard: &str,
) -> Option<UnwrappedReconciliation> {
    let logical = boundary.logical_name_id.as_ref()?;
    let node = logical.strip_prefix("ens:")?;
    let predecessor = state.v1_name("ens", node)?;
    let registrar = state.v1_registrar("ens", node)?;
    if !predecessor.surface_known
        || predecessor.resource_id != registrar.resource_id
        || predecessor.authority_source_family != V1_REGISTRAR_FAMILY
        || registrar.expiry? <= incoming.raw.block_timestamp.unix_timestamp()
    {
        return None;
    }
    let instance = catalog
        .contract_instance_for_address(base, incoming.raw.block_number)
        .ok()??;
    let selector = &boundary.after_state["predecessor_binding"];
    if selector["resource"]["contract_instance_id"]
        .as_str()?
        .parse::<Uuid>()
        .ok()?
        != instance
        || selector["resource"]["labelhash"].as_str() != registrar.labelhash.as_deref()
        || selector["predecessor_cleanup"]["log_index"].as_i64() != Some(cleanup.raw.log_index)
    {
        return None;
    }
    for observation in [incoming, cleanup] {
        if observation.contract_instance_id != instance
            || !observation.raw.emitting_address.eq_ignore_ascii_case(base)
            || observation.raw.chain_id != boundary.chain_id
            || !same_transaction(boundary, &observation.raw)
            || observation.raw.block_number != boundary.block_number?
            || observation.raw.transaction_index != boundary.transaction_index?
            || observation.raw.canonicality_state != boundary.canonicality_state
        {
            return None;
        }
        let transfer = output
            .normalized_events
            .iter()
            .filter(|event| {
                event.source_family == V1_REGISTRAR_FAMILY
                    && event.event_kind == "TokenControlTransferred"
                    && same_position(event, &observation.raw)
            })
            .collect::<Vec<_>>();
        let [transfer] = transfer.as_slice() else {
            return None;
        };
        if transfer.resource_id != Some(registrar.resource_id)
            || transfer.after_state["token_lineage_id"]
                .as_str()?
                .parse::<Uuid>()
                .ok()?
                != registrar.token_lineage_id?
        {
            return None;
        }
    }
    let equals = |value: &Value, address: &str| {
        value
            .as_str()
            .is_some_and(|value| value.eq_ignore_ascii_case(address))
    };
    if !equals(&incoming.decoded["from"], registrar.owner.as_deref()?)
        || !equals(&incoming.decoded["to"], controller)
        || !equals(&cleanup.decoded["from"], controller)
        || !equals(&cleanup.decoded["to"], graveyard)
        || incoming.raw.log_index >= cleanup.raw.log_index
        || cleanup.raw.log_index >= boundary.log_index?
    {
        return None;
    }
    if !complete_successor(boundary, output, raw_logs, controller) {
        return None;
    }
    let registry = catalog.declared_address_for_role("ens_v1_registry_l1", "registry")?;
    let registry_instance = catalog
        .contract_instance_for_address(registry, incoming.raw.block_number)
        .ok()??;
    let registry_source = catalog.source_for_contract_instance(registry_instance)?;
    if registry_source.source_family != "ens_v1_registry_l1"
        || registry_source.chain_id != boundary.chain_id
        || registry_source.namespace != "ens"
    {
        return None;
    }
    let node_hash = node.parse::<B256>().ok()?;
    let label_hash = registrar.labelhash.as_ref()?.parse::<B256>().ok()?;
    let eth_node = namehash(&["eth".to_owned()]).parse::<B256>().ok()?;
    let controller = controller.parse::<Address>().ok()?;
    let graveyard = graveyard.parse::<Address>().ok()?;
    let mut reclaim = Vec::new();
    let mut registry_cleanup = Vec::new();
    let mut resolver = Vec::new();
    let mut ttl = Vec::new();
    // reclaim -> setRecord -> BaseRegistrar cleanup -> injection is the pinned ERC721 path.
    // Resolver and TTL logs occur only when their stored values change.
    // (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L111-L119 @ ens_v2@a971bd64)
    // (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L174-L188 @ ens_v1@91c966f)
    for raw in raw_logs.iter().filter(|raw| {
        raw.chain_id == boundary.chain_id
            && same_transaction(boundary, raw)
            && raw.transaction_index == incoming.raw.transaction_index
            && raw.emitting_address.eq_ignore_ascii_case(registry)
            && incoming.raw.log_index < raw.log_index
            && raw.log_index < cleanup.raw.log_index
    }) {
        if let Some(event) = decode::<NewOwner>(raw).ok()? {
            if event.node == eth_node && event.label == label_hash {
                if event.owner != controller {
                    return None;
                }
                reclaim.push(raw.log_index);
            }
        } else if let Some(event) = decode::<Transfer>(raw).ok()? {
            if event.node == node_hash {
                if event.owner != graveyard {
                    return None;
                }
                registry_cleanup.push(raw.log_index);
            }
        } else if let Some(event) = decode::<NewResolver>(raw).ok()? {
            if event.node == node_hash {
                if event.resolver != Address::ZERO {
                    return None;
                }
                resolver.push(raw.log_index);
            }
        } else if let Some(event) = decode::<NewTTL>(raw).ok()? {
            if event.node == node_hash {
                if event.ttl != 0 {
                    return None;
                }
                ttl.push(raw.log_index);
            }
        }
    }
    let ([reclaim], [registry_cleanup]) = (reclaim.as_slice(), registry_cleanup.as_slice()) else {
        return None;
    };
    if reclaim >= registry_cleanup
        || resolver.len() > 1
        || ttl.len() > 1
        || resolver
            .iter()
            .chain(&ttl)
            .any(|log| log <= registry_cleanup)
        || resolver
            .first()
            .zip(ttl.first())
            .is_some_and(|(resolver, ttl)| resolver >= ttl)
        || (state.v1_resolver("ens", node).is_some() && resolver.is_empty())
    {
        return None;
    }
    Some(UnwrappedReconciliation {
        logical_name_id: logical.clone(),
        resource_id: registrar.resource_id,
        registry_resource_id: stable_uuid(&format!(
            "resource:registry-only:{}:{node}",
            boundary.chain_id
        )),
        chain_id: boundary.chain_id.clone(),
        block_hash: boundary.block_hash.clone()?,
        transaction_hash: boundary.transaction_hash.clone()?,
        block_number: incoming.raw.block_number,
        transaction_index: incoming.raw.transaction_index,
        first_log: incoming.raw.log_index,
        cleanup_log: cleanup.raw.log_index,
    })
}

fn complete_successor(
    boundary: &NormalizedEvent,
    output: &BatchOutput,
    raws: &[RawLogInput],
    controller: &str,
) -> bool {
    let proof = || -> Option<()> {
        let registration = output.normalized_events.iter().find(|event| {
            event.source_family == "ens_v2_registry_l1"
                && event.event_kind == "RegistrationGranted"
                && event.logical_name_id == boundary.logical_name_id
                && event.block_hash == boundary.block_hash
                && event.transaction_hash == boundary.transaction_hash
                && event.log_index == boundary.log_index
        })?;
        let token = registration.after_state["token_id"]
            .as_str()?
            .parse::<U256>()
            .ok()?;
        let owner = registration.after_state["registrant"]
            .as_str()?
            .parse::<Address>()
            .ok()?;
        let controller = controller.parse::<Address>().ok()?;
        let emitter = registration.raw_fact_ref["emitting_address"].as_str()?;
        let resource = boundary.after_state["successor_binding"]["resource_id"]
            .as_str()?
            .parse::<Uuid>()
            .ok()?;
        let mut mints = Vec::new();
        let mut grants = Vec::new();
        for raw in raws.iter().filter(|raw| {
            raw.chain_id == boundary.chain_id
                && same_transaction(boundary, raw)
                && raw.emitting_address.eq_ignore_ascii_case(emitter)
                && Some(raw.log_index) > registration.log_index
        }) {
            if let Some(mint) = decode::<TransferSingle>(raw).ok()? {
                if mint.id == token {
                    if mint.operator != controller
                        || mint.from != Address::ZERO
                        || mint.to != owner
                        || mint.value != U256::from(1)
                    {
                        return None;
                    }
                    mints.push(raw.log_index);
                }
            } else if let Some(grant) = decode::<EACRolesChanged>(raw).ok()? {
                if grant.resource == token {
                    if grant.account != owner
                        || grant.oldRoleBitmap != U256::ZERO
                        || grant.newRoleBitmap == U256::ZERO
                    {
                        return None;
                    }
                    if !output.normalized_events.iter().any(|event| {
                        event.source_family == "ens_v2_registry_l1"
                            && event.event_kind == "PermissionChanged"
                            && event.resource_id == Some(resource)
                            && same_position(event, raw)
                    }) {
                        return None;
                    }
                    grants.push(raw.log_index);
                }
            }
        }
        let ([mint], [grant]) = (mints.as_slice(), grants.as_slice()) else {
            return None;
        };
        let links = output
            .normalized_events
            .iter()
            .filter(|event| {
                event.source_family == "ens_v2_registry_l1"
                    && event.event_kind == "TokenResourceLinked"
                    && event.resource_id == Some(resource)
                    && event.block_hash == boundary.block_hash
                    && event.transaction_hash == boundary.transaction_hash
            })
            .collect::<Vec<_>>();
        let [link] = links.as_slice() else {
            return None;
        };
        (mint < &link.log_index? && &link.log_index? < grant).then_some(())
    };
    proof().is_some()
}

fn decode<T: SolEvent>(raw: &RawLogInput) -> Result<Option<T>, ()> {
    if raw
        .topics
        .first()
        .is_none_or(|topic| topic != &format!("{:#x}", T::SIGNATURE_HASH))
    {
        return Ok(None);
    }
    crate::evm_abi::decode_event_log::<T>(&raw.topics, &raw.data, "unwrapped cleanup proof")
        .map(Some)
        .map_err(|_| ())
}
