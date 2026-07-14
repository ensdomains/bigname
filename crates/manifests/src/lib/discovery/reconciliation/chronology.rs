use std::collections::{HashMap, HashSet};

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
    ) -> Self {
        let existing_set = existing_edges
            .iter()
            .map(|edge| edge.spec.clone())
            .collect::<HashSet<_>>();
        let mut historical_terminal_states =
            HashMap::<ReconciledDiscoveryEdgeSpec, ObservationTerminalState>::new();
        let mut retained_newer_edge_ids = HashSet::new();

        for desired_edge in desired_edges {
            let Some(desired_block_number) = desired_edge.active_from_block_number else {
                continue;
            };
            let successor = existing_edges
                .iter()
                .filter(|existing_edge| {
                    existing_edge.spec.observation_key == desired_edge.observation_key
                        && existing_edge.spec.chain == desired_edge.chain
                        && existing_edge.spec.edge_kind == desired_edge.edge_kind
                        && existing_edge.spec.from_contract_instance_id
                            == desired_edge.from_contract_instance_id
                        && existing_edge
                            .spec
                            .active_from_block_number
                            .is_some_and(|block_number| block_number > desired_block_number)
                })
                .min_by_key(|existing_edge| existing_edge.spec.active_from_block_number);
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
                },
            );
        }

        let current_new_edges = desired_edges
            .iter()
            .filter(|edge| {
                !existing_set.contains(*edge) && !historical_terminal_states.contains_key(*edge)
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
