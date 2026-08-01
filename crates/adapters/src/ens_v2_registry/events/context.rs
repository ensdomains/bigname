use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use bigname_manifests::DiscoveryObservation;
use bigname_storage::{NormalizedEvent, SurfaceBinding};
use sqlx::types::Uuid;

use crate::ens_v2_registry::{
    names::{RegistryNameKey, RegistryTokenKey},
    types::{CurrentParentClaim, CurrentSubregistryLink, RegistryEntryTopology, RegistryNameState},
};

pub(in crate::ens_v2_registry) struct RegistryObservationContext<'a> {
    pub(in crate::ens_v2_registry) registry_suffix_by_address: &'a mut HashMap<String, String>,
    pub(in crate::ens_v2_registry) root_registry_addresses: &'a HashSet<String>,
    pub(in crate::ens_v2_registry) registry_contract_by_address: &'a mut HashMap<String, Uuid>,
    pub(in crate::ens_v2_registry) current_subregistry_by_parent_label:
        &'a mut HashMap<(String, String), CurrentSubregistryLink>,
    pub(in crate::ens_v2_registry) current_parent_claim_by_registry:
        &'a mut HashMap<String, CurrentParentClaim>,
    pub(in crate::ens_v2_registry) entry_topology_by_registry_token:
        &'a mut HashMap<(String, String), RegistryEntryTopology>,
    pub(in crate::ens_v2_registry) states_by_registry_token:
        &'a mut BTreeMap<(String, String), RegistryNameState>,
    pub(in crate::ens_v2_registry) state_keys_by_registry_namehash:
        &'a mut HashMap<RegistryNameKey, BTreeSet<RegistryTokenKey>>,
    pub(in crate::ens_v2_registry) linked_resource_states:
        &'a mut BTreeMap<Uuid, RegistryNameState>,
    pub(in crate::ens_v2_registry) retired_binding_states:
        &'a mut BTreeMap<Uuid, RegistryNameState>,
    pub(in crate::ens_v2_registry) closed_bindings: &'a mut BTreeMap<Uuid, SurfaceBinding>,
    pub(in crate::ens_v2_registry) token_aliases:
        &'a mut HashMap<RegistryTokenKey, RegistryTokenKey>,
    pub(in crate::ens_v2_registry) current_token_alias_by_canonical_key:
        &'a mut HashMap<RegistryTokenKey, RegistryTokenKey>,
    pub(in crate::ens_v2_registry) observations: &'a mut Vec<DiscoveryObservation>,
    pub(in crate::ens_v2_registry) graph_events: &'a mut Vec<NormalizedEvent>,
}
