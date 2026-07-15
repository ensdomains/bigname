use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
};

use super::super::types::{
    ExistingReconciledDiscoveryEdge, ObservationTerminalState, ReconciledDiscoveryEdgeSpec,
};

pub(super) struct ScopedDiscoveryChronology<'a> {
    pub(super) current_new_edges: Vec<&'a ReconciledDiscoveryEdgeSpec>,
    pub(super) historical_edges: Vec<(&'a ReconciledDiscoveryEdgeSpec, ObservationTerminalState)>,
    retained_newer_edge_ids: HashSet<i64>,
}

impl<'a> ScopedDiscoveryChronology<'a> {
    pub(super) fn classify(
        desired_edges: &'a [ReconciledDiscoveryEdgeSpec],
        existing_edges: &[ExistingReconciledDiscoveryEdge],
        terminal_states_by_key: &HashMap<String, ObservationTerminalState>,
    ) -> Self {
        let existing_set = existing_edges
            .iter()
            .map(|edge| edge.spec.clone())
            .collect::<HashSet<_>>();
        let mut historical_terminal_states =
            HashMap::<ReconciledDiscoveryEdgeSpec, ObservationTerminalState>::new();
        let mut retained_newer_edge_ids = HashSet::new();

        // A scoped tombstone may arrive before the assignment it would have
        // closed. Retain assignments which start after that terminal event:
        // closing them would reverse the EVM event chronology even though the
        // persisted block interval would not look negative. Exact event
        // endpoints may still close as one-block inclusive intervals.
        for existing_edge in existing_edges {
            let Some(terminal_state) =
                terminal_states_by_key.get(&existing_edge.spec.observation_key)
            else {
                continue;
            };
            if edge_starts_after_terminal(existing_edge, terminal_state)
                && !existing_edge.active_from_block_is_orphaned
            {
                retained_newer_edge_ids.insert(existing_edge.discovery_edge_id);
            }
        }

        for desired_edge in desired_edges {
            if desired_edge.active_from_block_number.is_none() {
                continue;
            }
            let successor = existing_edges
                .iter()
                .filter(|existing_edge| {
                    !existing_edge.active_from_block_is_orphaned
                        && existing_edge.spec.observation_key == desired_edge.observation_key
                        && existing_edge.spec.chain == desired_edge.chain
                        && existing_edge.spec.edge_kind == desired_edge.edge_kind
                        && existing_edge.spec.from_contract_instance_id
                            == desired_edge.from_contract_instance_id
                        && edge_starts_after_spec(existing_edge, desired_edge)
                })
                .min_by(compare_edge_starts);
            let Some(successor) = successor else {
                continue;
            };
            retained_newer_edge_ids.insert(successor.discovery_edge_id);
            historical_terminal_states.insert(
                desired_edge.clone(),
                ObservationTerminalState {
                    chain: successor.spec.chain.clone(),
                    block_number: successor.spec.active_from_block_number,
                    block_hash: successor.spec.active_from_block_hash.clone(),
                    event_position: successor.spec.active_from_event_position,
                },
            );
        }

        // A later observation which repeats the same endpoint continues the
        // original assignment. Retain one earliest materialized epoch instead
        // of closing it and opening a duplicate at the replay position.
        for desired_edge in desired_edges {
            if let Some(existing_edge) = existing_edges
                .iter()
                .filter(|existing_edge| {
                    !existing_edge.active_from_block_is_orphaned
                        && same_assignment(existing_edge, desired_edge)
                        && assignment_starts_no_later(existing_edge, desired_edge)
                })
                .min_by(compare_edge_starts)
            {
                retained_newer_edge_ids.insert(existing_edge.discovery_edge_id);
            }
        }

        let current_new_edges = desired_edges
            .iter()
            .filter(|edge| {
                !existing_set.contains(*edge)
                    && !historical_terminal_states.contains_key(*edge)
                    && !existing_edges.iter().any(|existing_edge| {
                        !existing_edge.active_from_block_is_orphaned
                            && same_assignment(existing_edge, edge)
                            && assignment_starts_no_later(existing_edge, edge)
                    })
            })
            .collect();
        let historical_edges = desired_edges
            .iter()
            .filter_map(|edge| {
                historical_terminal_states
                    .remove(edge)
                    .map(|terminal_state| (edge, terminal_state))
            })
            .collect();

        Self {
            current_new_edges,
            historical_edges,
            retained_newer_edge_ids,
        }
    }

    pub(super) fn retains_newer_edge(&self, discovery_edge_id: i64) -> bool {
        self.retained_newer_edge_ids.contains(&discovery_edge_id)
    }
}

fn same_assignment(
    existing_edge: &ExistingReconciledDiscoveryEdge,
    desired_edge: &ReconciledDiscoveryEdgeSpec,
) -> bool {
    existing_edge.spec.observation_key == desired_edge.observation_key
        && existing_edge.spec.chain == desired_edge.chain
        && existing_edge.spec.edge_kind == desired_edge.edge_kind
        && existing_edge.spec.from_contract_instance_id == desired_edge.from_contract_instance_id
        && existing_edge.spec.to_contract_instance_id == desired_edge.to_contract_instance_id
        && existing_edge.spec.discovery_source == desired_edge.discovery_source
        && existing_edge.spec.source_manifest_id == desired_edge.source_manifest_id
        && existing_edge.spec.admission == desired_edge.admission
}

fn assignment_starts_no_later(
    existing_edge: &ExistingReconciledDiscoveryEdge,
    desired_edge: &ReconciledDiscoveryEdgeSpec,
) -> bool {
    match (
        existing_edge.spec.active_from_block_number,
        desired_edge.active_from_block_number,
    ) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(existing), Some(desired)) => match existing.cmp(&desired) {
            Ordering::Less => true,
            Ordering::Greater => false,
            Ordering::Equal => match (
                existing_edge.spec.active_from_event_position,
                desired_edge.active_from_event_position,
            ) {
                (Some(existing), Some(desired)) => existing <= desired,
                // Legacy and non-log discovery observations retain the
                // historical block-inclusive comparison when no full event
                // position is available on both sides.
                _ => true,
            },
        },
    }
}

pub(super) fn edge_starts_after_terminal(
    edge: &ExistingReconciledDiscoveryEdge,
    terminal_state: &ObservationTerminalState,
) -> bool {
    starts_after(
        edge.spec.active_from_block_number,
        edge.spec.active_from_event_position,
        terminal_state.block_number,
        terminal_state.event_position,
    )
}

fn edge_starts_after_spec(
    edge: &ExistingReconciledDiscoveryEdge,
    desired_edge: &ReconciledDiscoveryEdgeSpec,
) -> bool {
    starts_after(
        edge.spec.active_from_block_number,
        edge.spec.active_from_event_position,
        desired_edge.active_from_block_number,
        desired_edge.active_from_event_position,
    )
}

fn starts_after(
    left_block_number: Option<i64>,
    left_event_position: Option<super::super::types::EvmEventPosition>,
    right_block_number: Option<i64>,
    right_event_position: Option<super::super::types::EvmEventPosition>,
) -> bool {
    let (Some(left_block_number), Some(right_block_number)) =
        (left_block_number, right_block_number)
    else {
        return false;
    };
    match left_block_number.cmp(&right_block_number) {
        Ordering::Greater => true,
        Ordering::Less => false,
        Ordering::Equal => match (left_event_position, right_event_position) {
            (Some(left), Some(right)) => left > right,
            _ => false,
        },
    }
}

fn compare_edge_starts(
    left: &&ExistingReconciledDiscoveryEdge,
    right: &&ExistingReconciledDiscoveryEdge,
) -> Ordering {
    left.spec
        .active_from_block_number
        .cmp(&right.spec.active_from_block_number)
        .then_with(|| {
            match (
                left.spec.active_from_event_position,
                right.spec.active_from_event_position,
            ) {
                (Some(left), Some(right)) => left.cmp(&right),
                _ => Ordering::Equal,
            }
        })
        .then_with(|| left.discovery_edge_id.cmp(&right.discovery_edge_id))
}
