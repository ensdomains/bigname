use std::collections::{HashMap, HashSet};

use anyhow::Result;

use super::super::{
    provenance::is_zero_address,
    types::{
        DiscoveryObservation, ExistingReconciledDiscoveryEdge, ObservationTerminalState,
        ReconciledDiscoveryEdgeSpec,
    },
};
use crate::normalize_address;

pub(super) fn cascade_deactivation_terminal_states(
    existing_edges: &[ExistingReconciledDiscoveryEdge],
    desired_set: &HashSet<ReconciledDiscoveryEdgeSpec>,
    observations_by_key: &HashMap<String, &DiscoveryObservation>,
    direct_terminal_states_by_key: &HashMap<String, ObservationTerminalState>,
) -> Result<HashMap<String, ObservationTerminalState>> {
    let mut terminal_states_by_key = HashMap::<String, ObservationTerminalState>::new();
    let mut removed_parent_addresses = HashMap::<String, ObservationTerminalState>::new();

    for existing_edge in existing_edges
        .iter()
        .filter(|edge| !desired_set.contains(&edge.spec))
    {
        let Some(observation) = observations_by_key.get(&existing_edge.spec.observation_key) else {
            continue;
        };
        let Some(terminal_state) = direct_terminal_states_by_key
            .get(&existing_edge.spec.observation_key)
            .cloned()
        else {
            continue;
        };
        let next_address = normalize_address(&observation.to_address);
        if !is_zero_address(&next_address) && next_address == existing_edge.to_address {
            continue;
        }

        terminal_states_by_key.insert(
            existing_edge.spec.observation_key.clone(),
            terminal_state.clone(),
        );
        removed_parent_addresses.insert(existing_edge.to_address.clone(), terminal_state);
    }

    let mut changed = true;
    while changed {
        changed = false;

        for existing_edge in existing_edges
            .iter()
            .filter(|edge| !desired_set.contains(&edge.spec))
        {
            if terminal_states_by_key.contains_key(&existing_edge.spec.observation_key) {
                continue;
            }
            let Some(observation) = observations_by_key.get(&existing_edge.spec.observation_key)
            else {
                continue;
            };
            let parent_address = normalize_address(&observation.from_address);
            let Some(terminal_state) = removed_parent_addresses.get(&parent_address).cloned()
            else {
                continue;
            };

            terminal_states_by_key.insert(
                existing_edge.spec.observation_key.clone(),
                terminal_state.clone(),
            );
            removed_parent_addresses.insert(existing_edge.to_address.clone(), terminal_state);
            changed = true;
        }
    }

    Ok(terminal_states_by_key)
}
