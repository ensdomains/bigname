#[path = "reconciliation/bulk.rs"]
mod bulk;
#[path = "reconciliation/cascade.rs"]
mod cascade;
#[path = "reconciliation/chronology.rs"]
mod chronology;
#[path = "reconciliation/existing.rs"]
mod existing;
#[path = "reconciliation/full.rs"]
mod full;
#[path = "reconciliation/scoped.rs"]
mod scoped;
#[path = "reconciliation/streamed.rs"]
mod streamed;
#[path = "reconciliation/support.rs"]
mod support;
#[path = "reconciliation/walk.rs"]
mod walk;

pub use full::{
    ExpectedDiscoveryAdmissionEpoch, FullDiscoveryReconciliationOptions,
    reconcile_discovery_observations,
};
pub use scoped::{
    reconcile_scoped_discovery_observation_transitions, reconcile_scoped_discovery_observations,
};
pub use streamed::{
    DiscoveryObservationPageSource, reconcile_discovery_observations_streamed,
    reconcile_discovery_observations_streamed_with_full_options,
};
#[cfg(test)]
pub(crate) use streamed::{
    StreamedDiscoveryReconciliationOptions, reconcile_discovery_observations_streamed_with_options,
};

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use anyhow::{Result, ensure};
use sqlx::types::Uuid;

use super::admission::DiscoveryAdmissionState;
use super::admission_epoch::{
    bump_discovery_admission_epochs, fence_discovery_admission_epoch_writes,
};
use super::loading::load_scoped_discovery_admission_state_with_excluded_source as load_scoped_admission_state;
use super::provenance::{discovery_edge_propagates_role, is_zero_address, observation_key};
use super::types::{
    AdmittedDiscoveryEdge, DiscoveryObservation, DiscoveryReconciliationSummary,
    ObservationTerminalState, ReconciledDiscoveryEdgeSpec,
};
use crate::{normalize_address, reconcile_active_contract_instance_addresses_for_ids};

use self::bulk::{
    deactivate_reconciled_discovery_edge, insert_pending_contract_instance_seeds,
    insert_reconciled_discovery_edges, reconcile_historical_discovery_edges,
};
use self::chronology::{ScopedDiscoveryChronology, edge_starts_after_terminal};
use self::existing::{
    load_active_reconciled_discovery_edges_by_observation_keys,
    load_unreachable_reconciled_discovery_descendant_edges,
};
use self::support::{lock_discovery_reconciliation, observation_terminal_states};
use self::walk::DiscoveryAdmissionWalk;

fn empty_reconciliation_summary() -> DiscoveryReconciliationSummary {
    DiscoveryReconciliationSummary {
        active_edge_count: 0,
        admitted_edge_count: 0,
        inserted_edge_count: 0,
        deactivated_edge_count: 0,
        admission_epoch_bump_count: 0,
        admitted_edges: Vec::new(),
    }
}

fn safe_deactivation_terminal(
    edge: &super::types::ExistingReconciledDiscoveryEdge,
    mut terminal_state: ObservationTerminalState,
) -> ObservationTerminalState {
    if edge.active_from_block_is_orphaned && edge_starts_after_terminal(edge, &terminal_state) {
        terminal_state.block_number = None;
        terminal_state.block_hash = None;
    }
    terminal_state
}

async fn reconcile_scoped_discovery_observations_in_transaction(
    transaction: &mut sqlx::postgres::PgConnection,
    discovery_source: &str,
    observations: &[DiscoveryObservation],
) -> Result<(DiscoveryReconciliationSummary, BTreeSet<String>)> {
    if observations.is_empty() {
        return Ok((empty_reconciliation_summary(), BTreeSet::new()));
    }

    for observation in observations {
        ensure!(
            observation.discovery_source == discovery_source,
            "scoped discovery observation for {} cannot be reconciled under {}",
            observation.discovery_source,
            discovery_source
        );
    }

    // Scoped transitions only mutate their touched observation keys, so the
    // already-reconciled graph from this source is valid ancestry for the
    // next transition in an ordered replay. Full-source reconciliation still
    // excludes the source and recomputes authority from roots.
    let admission_state =
        load_scoped_admission_state(&mut *transaction, None, observations).await?;
    let direct_terminal_states_by_key = observation_terminal_states(observations)?;
    let observations_by_key = observations
        .iter()
        .map(|observation| Ok((observation_key(observation)?, observation)))
        .collect::<Result<HashMap<_, _>>>()?;
    let mut touched_observation_keys = direct_terminal_states_by_key
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    touched_observation_keys.sort();

    let (desired_edges, admitted_edges) =
        resolve_reconciled_discovery_edge_specs(&admission_state, &mut *transaction, observations)
            .await?;
    let existing_edges = load_active_reconciled_discovery_edges_by_observation_keys(
        &mut *transaction,
        discovery_source,
        &touched_observation_keys,
    )
    .await?;

    let desired_set = desired_edges.iter().cloned().collect::<HashSet<_>>();
    let chronology = ScopedDiscoveryChronology::classify(
        &desired_edges,
        &existing_edges,
        &direct_terminal_states_by_key,
    );
    let mut deactivation_terminal_states_by_edge_id =
        BTreeMap::<i64, (String, ObservationTerminalState)>::new();
    let mut removed_parent_edges = Vec::<(String, Uuid, ObservationTerminalState)>::new();
    let mut affected_contract_instance_ids = HashSet::<Uuid>::new();

    for existing_edge in &existing_edges {
        if desired_set.contains(&existing_edge.spec)
            || chronology.retains_newer_edge(existing_edge.discovery_edge_id)
        {
            continue;
        }
        if !observations_by_key.contains_key(&existing_edge.spec.observation_key) {
            continue;
        }
        let Some(terminal_state) = direct_terminal_states_by_key
            .get(&existing_edge.spec.observation_key)
            .cloned()
        else {
            continue;
        };
        let terminal_state = safe_deactivation_terminal(existing_edge, terminal_state);
        deactivation_terminal_states_by_edge_id.insert(
            existing_edge.discovery_edge_id,
            (existing_edge.spec.chain.clone(), terminal_state.clone()),
        );
        affected_contract_instance_ids.insert(existing_edge.spec.from_contract_instance_id);
        affected_contract_instance_ids.insert(existing_edge.spec.to_contract_instance_id);
        if discovery_edge_propagates_role(&existing_edge.spec.edge_kind) {
            removed_parent_edges.push((
                existing_edge.spec.chain.clone(),
                existing_edge.spec.to_contract_instance_id,
                terminal_state,
            ));
        }
    }

    let mut deactivated_edge_count = 0;
    let mut mutated_chains = BTreeSet::new();
    for (discovery_edge_id, (edge_chain, terminal_state)) in deactivation_terminal_states_by_edge_id
    {
        let deactivated = deactivate_reconciled_discovery_edge(
            &mut *transaction,
            discovery_edge_id,
            Some(&terminal_state),
        )
        .await?;
        if deactivated {
            mutated_chains.insert(edge_chain);
            deactivated_edge_count += 1;
        }
    }

    let new_edges = &chronology.current_new_edges;
    let historical_edges = &chronology.historical_edges;
    for new_edge in new_edges
        .iter()
        .copied()
        .chain(historical_edges.iter().map(|(edge, _)| *edge))
    {
        affected_contract_instance_ids.insert(new_edge.from_contract_instance_id);
        affected_contract_instance_ids.insert(new_edge.to_contract_instance_id);
    }
    let edge_insert = insert_reconciled_discovery_edges(&mut *transaction, new_edges).await?;
    let historical_edge_reconciliation =
        reconcile_historical_discovery_edges(&mut *transaction, historical_edges).await?;
    let inserted_edge_count = edge_insert.inserted_count
        + edge_insert.reactivated_count
        + historical_edge_reconciliation.inserted_count;
    mutated_chains.extend(new_edges.iter().map(|edge| edge.chain.clone()));
    if historical_edge_reconciliation.inserted_count > 0
        || historical_edge_reconciliation.updated_count > 0
    {
        mutated_chains.extend(historical_edges.iter().map(|(edge, _)| edge.chain.clone()));
    }

    // Direct removals and replacement insertions are now visible inside this
    // transaction. Recompute root reachability before cascading so a registry
    // which is still admitted through another active incoming subregistry
    // edge retains its outgoing discovery branch.
    let mut descendant_deactivations =
        BTreeMap::<i64, (String, ObservationTerminalState, Uuid, Uuid)>::new();
    for (chain, parent_contract_instance_id, terminal_state) in removed_parent_edges {
        let descendants = load_unreachable_reconciled_discovery_descendant_edges(
            &mut *transaction,
            discovery_source,
            &chain,
            &[parent_contract_instance_id],
        )
        .await?;
        for descendant in descendants {
            if desired_set.contains(&descendant.spec) {
                continue;
            }
            if !descendant.active_from_block_is_orphaned
                && edge_starts_after_terminal(&descendant, &terminal_state)
            {
                continue;
            }
            let descendant_terminal_state =
                safe_deactivation_terminal(&descendant, terminal_state.clone());
            descendant_deactivations
                .entry(descendant.discovery_edge_id)
                .or_insert_with(|| {
                    (
                        descendant.spec.chain.clone(),
                        descendant_terminal_state,
                        descendant.spec.from_contract_instance_id,
                        descendant.spec.to_contract_instance_id,
                    )
                });
        }
    }
    for (discovery_edge_id, (edge_chain, terminal_state, from_id, to_id)) in
        descendant_deactivations
    {
        let deactivated = deactivate_reconciled_discovery_edge(
            &mut *transaction,
            discovery_edge_id,
            Some(&terminal_state),
        )
        .await?;
        if deactivated {
            affected_contract_instance_ids.insert(from_id);
            affected_contract_instance_ids.insert(to_id);
            mutated_chains.insert(edge_chain);
            deactivated_edge_count += 1;
        }
    }

    if inserted_edge_count > 0
        || historical_edge_reconciliation.updated_count > 0
        || deactivated_edge_count > 0
    {
        reconcile_active_contract_instance_addresses_for_ids(
            &mut *transaction,
            &affected_contract_instance_ids,
        )
        .await?;
    }

    let active_edge_count = existing_edges
        .iter()
        .filter(|edge| {
            desired_set.contains(&edge.spec)
                || chronology.retains_newer_edge(edge.discovery_edge_id)
        })
        .count()
        + new_edges.len();
    Ok((
        DiscoveryReconciliationSummary {
            active_edge_count,
            admitted_edge_count: admitted_edges.len(),
            inserted_edge_count,
            deactivated_edge_count,
            admission_epoch_bump_count: 0,
            admitted_edges,
        },
        mutated_chains,
    ))
}

fn compare_reconciled_discovery_edge_specs(
    left: &ReconciledDiscoveryEdgeSpec,
    right: &ReconciledDiscoveryEdgeSpec,
) -> std::cmp::Ordering {
    (
        left.observation_key.as_str(),
        left.chain.as_str(),
        left.edge_kind.as_str(),
        left.from_contract_instance_id,
        left.to_contract_instance_id,
        left.discovery_source.as_str(),
        left.source_manifest_id,
        left.admission.as_str(),
        left.active_from_block_number,
        left.active_from_block_hash.as_deref(),
        left.active_from_event_position,
        left.provenance_json.as_str(),
    )
        .cmp(&(
            right.observation_key.as_str(),
            right.chain.as_str(),
            right.edge_kind.as_str(),
            right.from_contract_instance_id,
            right.to_contract_instance_id,
            right.discovery_source.as_str(),
            right.source_manifest_id,
            right.admission.as_str(),
            right.active_from_block_number,
            right.active_from_block_hash.as_deref(),
            right.active_from_event_position,
            right.provenance_json.as_str(),
        ))
}

async fn resolve_reconciled_discovery_edge_specs(
    admission_state: &DiscoveryAdmissionState,
    executor: &mut sqlx::postgres::PgConnection,
    observations: &[DiscoveryObservation],
) -> Result<(Vec<ReconciledDiscoveryEdgeSpec>, Vec<AdmittedDiscoveryEdge>)> {
    let mut desired_edges = HashSet::new();
    let mut admitted_edges = HashSet::new();
    let mut walk = DiscoveryAdmissionWalk::new(admission_state);
    let mut observations_by_from_address =
        HashMap::<(String, String), Vec<&DiscoveryObservation>>::new();
    for observation in observations {
        if is_zero_address(&observation.to_address) {
            continue;
        }
        observations_by_from_address
            .entry((
                observation.chain.clone(),
                normalize_address(&observation.from_address),
            ))
            .or_default()
            .push(observation);
    }

    let mut queued_contract_keys = walk
        .contract_address_keys()
        .cloned()
        .collect::<HashSet<_>>();
    let mut pending_contract_keys = queued_contract_keys
        .iter()
        .cloned()
        .collect::<VecDeque<_>>();
    while let Some(contract_key) = pending_contract_keys.pop_front() {
        queued_contract_keys.remove(&contract_key);
        let Some(key_observations) = observations_by_from_address.get(&contract_key) else {
            continue;
        };

        for &observation in key_observations {
            for admitted in walk.admit_observation(
                admission_state,
                &admission_state.known_contract_instances_by_address,
                observation,
            )? {
                desired_edges.insert(admitted.desired_edge);
                admitted_edges.insert(admitted.admitted_edge);
                if let Some(derived_contract_key) = admitted.derived_contract_key
                    && queued_contract_keys.insert(derived_contract_key.clone())
                {
                    pending_contract_keys.push_back(derived_contract_key);
                }
            }
        }
    }

    insert_pending_contract_instance_seeds(
        executor,
        &walk.into_sorted_pending_contract_instance_seeds(),
    )
    .await?;

    let mut desired_edges = desired_edges.into_iter().collect::<Vec<_>>();
    desired_edges.sort_by(compare_reconciled_discovery_edge_specs);
    let mut admitted_edges = admitted_edges.into_iter().collect::<Vec<_>>();
    admitted_edges.sort_by(|left, right| {
        (
            left.source_manifest_id,
            left.chain.as_str(),
            left.from_contract_instance_id,
            left.to_contract_instance_id,
            left.to_address.as_str(),
            left.edge_kind.as_str(),
            left.discovery_source.as_str(),
            left.admission.as_str(),
            left.from_role.as_str(),
        )
            .cmp(&(
                right.source_manifest_id,
                right.chain.as_str(),
                right.from_contract_instance_id,
                right.to_contract_instance_id,
                right.to_address.as_str(),
                right.edge_kind.as_str(),
                right.discovery_source.as_str(),
                right.admission.as_str(),
                right.from_role.as_str(),
            ))
    });

    Ok((desired_edges, admitted_edges))
}
