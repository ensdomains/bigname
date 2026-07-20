use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use sqlx::types::Uuid;

use super::super::admission::DiscoveryAdmissionState;
use super::super::provenance::{
    discovery_edge_propagates_role, discovery_edge_provenance, evm_event_position, observation_key,
};
use super::super::types::{
    AdmittedDiscoveryEdge, DiscoveryObservation, ReconciledDiscoveryEdgeSpec, StoredActiveContract,
};
use super::bulk::PendingContractInstanceSeed;
use crate::{REACHABLE_FROM_ROOT_ADMISSION, normalize_address};

/// One admission produced by [`DiscoveryAdmissionWalk::admit_observation`].
pub(super) struct AdmittedObservationEdge {
    pub(super) admitted_edge: AdmittedDiscoveryEdge,
    pub(super) desired_edge: ReconciledDiscoveryEdgeSpec,
    /// `(chain, normalized address)` whose active-contract set grew through
    /// this admission's role propagation. The caller must (re)walk the
    /// observations emitted from this address to reach the fixed point.
    pub(super) derived_contract_key: Option<(String, String)>,
}

/// Mutable state of the fixed-point discovery admission walk, shared by the
/// in-memory and streamed full-source reconciles so both admit observations
/// through identical logic. Memory here is bounded by the active-contract
/// closure (manifest contracts plus derived registries, i.e. distinct
/// admitted role-propagating targets) and by pending seeds for addresses not
/// yet present in `contract_instance_addresses` — not by the observation
/// count.
pub(super) struct DiscoveryAdmissionWalk {
    active_contracts: HashSet<StoredActiveContract>,
    active_contracts_by_address: HashMap<(String, String), Vec<StoredActiveContract>>,
    pending_contract_instance_seeds: HashMap<(String, String), PendingContractInstanceSeed>,
}

impl DiscoveryAdmissionWalk {
    pub(super) fn new(admission_state: &DiscoveryAdmissionState) -> Self {
        let active_contracts = admission_state
            .active_contracts
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let mut active_contracts_by_address =
            HashMap::<(String, String), Vec<StoredActiveContract>>::new();
        for contract in &active_contracts {
            active_contracts_by_address
                .entry((contract.chain.clone(), contract.address.clone()))
                .or_default()
                .push(contract.clone());
        }

        Self {
            active_contracts,
            active_contracts_by_address,
            pending_contract_instance_seeds: HashMap::new(),
        }
    }

    pub(super) fn contract_address_keys(&self) -> impl Iterator<Item = &(String, String)> {
        self.active_contracts_by_address.keys()
    }

    pub(super) fn has_contract_address(&self, key: &(String, String)) -> bool {
        self.active_contracts_by_address.contains_key(key)
    }

    /// Admit one observation against the current active-contract closure,
    /// resolving discovered target instances through `known_contract_
    /// instances_by_address` first and the pending-seed map second, exactly
    /// like the historical in-memory walk.
    pub(super) fn admit_observation(
        &mut self,
        admission_state: &DiscoveryAdmissionState,
        known_contract_instances_by_address: &HashMap<(String, String), Uuid>,
        observation: &DiscoveryObservation,
    ) -> Result<Vec<AdmittedObservationEdge>> {
        let observation_key = observation_key(observation)?;
        let mut admitted = Vec::new();

        for mut admitted_edge in admission_state
            .admit_candidate_against_contract_lookup_with_known_addresses(
                &self.active_contracts_by_address,
                known_contract_instances_by_address,
                &observation.candidate(),
            )
        {
            let to_contract_instance_id = match admitted_edge.to_contract_instance_id {
                Some(contract_instance_id) => contract_instance_id,
                None => {
                    let resolved_key = (
                        admitted_edge.chain.clone(),
                        normalize_address(&admitted_edge.to_address),
                    );
                    if let Some(seed) = self.pending_contract_instance_seeds.get(&resolved_key) {
                        seed.contract_instance_id
                    } else {
                        let contract_instance_id = Uuid::new_v4();
                        let instance_provenance_json = serde_json::json!({
                            "source": "discovery_observation",
                            "edge_kind": admitted_edge.edge_kind,
                            "discovery_source": admitted_edge.discovery_source,
                        });
                        let address_provenance_json = serde_json::json!({
                            "source": "discovery_observation_seed",
                            "edge_kind": admitted_edge.edge_kind,
                            "discovery_source": admitted_edge.discovery_source,
                        });
                        self.pending_contract_instance_seeds.insert(
                            resolved_key,
                            PendingContractInstanceSeed {
                                contract_instance_id,
                                chain: admitted_edge.chain.clone(),
                                address: normalize_address(&admitted_edge.to_address),
                                source_manifest_id: admitted_edge.source_manifest_id,
                                instance_provenance_json,
                                address_provenance_json,
                            },
                        );
                        contract_instance_id
                    }
                }
            };
            admitted_edge.to_contract_instance_id = Some(to_contract_instance_id);

            let provenance = discovery_edge_provenance(
                &observation.provenance,
                &admitted_edge.edge_kind,
                &admitted_edge.from_role,
            )?;
            let desired_edge = ReconciledDiscoveryEdgeSpec {
                observation_key: observation_key.clone(),
                chain: admitted_edge.chain.clone(),
                edge_kind: admitted_edge.edge_kind.clone(),
                from_contract_instance_id: admitted_edge.from_contract_instance_id,
                to_contract_instance_id,
                discovery_source: admitted_edge.discovery_source.clone(),
                source_manifest_id: admitted_edge.source_manifest_id,
                admission: admitted_edge.admission.clone(),
                active_from_block_number: observation.active_from_block_number,
                active_from_block_hash: observation.active_from_block_hash.clone(),
                active_from_event_position: evm_event_position(&observation.provenance)?,
                provenance_json: serde_json::to_string(&provenance)
                    .context("failed to serialize reconciled discovery-edge provenance")?,
            };

            let mut derived_contract_key = None;
            if admitted_edge.admission == REACHABLE_FROM_ROOT_ADMISSION
                && discovery_edge_propagates_role(&admitted_edge.edge_kind)
            {
                let derived_contract = StoredActiveContract {
                    manifest_id: admitted_edge.source_manifest_id,
                    chain: admitted_edge.chain.clone(),
                    role: admitted_edge.from_role.clone(),
                    contract_instance_id: to_contract_instance_id,
                    address: admitted_edge.to_address.clone(),
                };
                if self.active_contracts.insert(derived_contract.clone()) {
                    let key = (
                        derived_contract.chain.clone(),
                        derived_contract.address.clone(),
                    );
                    self.active_contracts_by_address
                        .entry(key.clone())
                        .or_default()
                        .push(derived_contract);
                    derived_contract_key = Some(key);
                }
            }

            admitted.push(AdmittedObservationEdge {
                admitted_edge,
                desired_edge,
                derived_contract_key,
            });
        }

        Ok(admitted)
    }

    /// Pending seeds for `insert_pending_contract_instance_seeds`, in the
    /// deterministic `(chain, address)` order both reconciles rely on.
    pub(super) fn into_sorted_pending_contract_instance_seeds(
        self,
    ) -> Vec<PendingContractInstanceSeed> {
        let mut pending_contract_instance_seeds = self
            .pending_contract_instance_seeds
            .into_values()
            .collect::<Vec<_>>();
        pending_contract_instance_seeds.sort_by(|left, right| {
            (left.chain.as_str(), left.address.as_str())
                .cmp(&(right.chain.as_str(), right.address.as_str()))
        });
        pending_contract_instance_seeds
    }
}
