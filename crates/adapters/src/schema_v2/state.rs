use imbl::{ordmap::OrdMap, ordset::OrdSet};
use serde_json::Value;
use uuid::Uuid;

use super::state_residency::{StateCacheCapacity, StateResidency};

#[path = "state_topology.rs"]
mod topology;

#[path = "state_incremental.rs"]
mod incremental;

#[cfg(test)]
#[path = "state_tests.rs"]
mod tests;

#[path = "state_wrapper.rs"]
mod wrapper;

#[path = "state_surfaces.rs"]
mod surfaces;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct V1NameState {
    pub logical_name_id: String,
    pub surface_known: bool,
    pub resource_id: Uuid,
    pub token_lineage_id: Option<Uuid>,
    pub authority_source_family: String,
    pub source_manifest_id: Option<i64>,
    pub labelhash: Option<String>,
    pub expiry: Option<i64>,
    pub owner: Option<String>,
    pub authority_key: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct V1WrapperData {
    pub fuses: u32,
    pub expiry: u64,
}

#[path = "state_v2.rs"]
mod v2;

#[path = "state_v2_refresh.rs"]
mod v2_refresh;

pub(super) use self::v2::{V2NameState, V2NameTransition, V2RawNameState, V2TokenState};
#[cfg(test)]
pub(super) use self::v2_refresh::{reset_v2_refresh_visits, v2_refresh_visits};

#[derive(Clone, Debug)]
pub(super) struct V1Release {
    pub namehash: String,
    pub registrar: V1NameState,
    pub release_was_active: bool,
    pub previous_authority: Option<V1NameState>,
    pub next_authority: Option<V1NameState>,
    pub resolver: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct State {
    pub(super) values: StateResidency,
    pub(super) provisional_values: OrdMap<String, Value>,
    v1_names: OrdMap<String, V1NameState>,
    v1_wrapper_data: OrdMap<String, V1WrapperData>,
    v1_registrars: OrdMap<String, V1NameState>,
    v1_expiries: OrdSet<(i64, String)>,
    v1_registry_authorities: OrdMap<String, V1NameState>,
    v1_registry_owners: OrdMap<String, String>,
    v1_resolvers: OrdMap<String, String>,
    v1_migrated_nodes: OrdSet<String>,
    v1_materialized_surfaces: OrdSet<String>,
    known_surfaces: OrdSet<String>,
    restored_surface_sources: OrdMap<String, OrdSet<String>>,
    restored_surface_counts: OrdMap<String, usize>,
    v2_current_surface_counts: OrdMap<String, usize>,
    surface_removal_candidates: OrdSet<String>,
    restoring_state_key: Option<String>,
    active_resources: OrdMap<String, Uuid>,
    v2_tokens: OrdMap<String, V2TokenState>,
    v2_expiries: OrdSet<(u64, String)>,
    v2_entry_by_parent_label: OrdMap<(String, Vec<u8>), String>,
    v2_parent_claims: OrdMap<String, (String, Vec<u8>)>,
    v2_suffix_anchors: OrdMap<String, (String, Vec<String>)>,
    latest_v2_timestamp: Option<i64>,
    v2_resolver_hints: OrdMap<(String, String), (String, Value)>,
    materialized_token_lineages: OrdSet<Uuid>,
}

impl State {
    pub(super) fn materialize_token_lineage(&mut self, token_lineage_id: Uuid) -> bool {
        self.materialized_token_lineages
            .insert(token_lineage_id)
            .is_none()
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn observe_v1_name(
        &mut self,
        namespace: &str,
        namehash: &str,
        logical_name_id: String,
        surface_known: bool,
        resource_id: Uuid,
        token_lineage_id: Option<Uuid>,
        authority_source_family: String,
        expiry: Option<i64>,
        owner: Option<String>,
        authority_key: Option<String>,
    ) {
        let key = v1_key(namespace, namehash);
        if surface_known {
            self.remember_known_surface(logical_name_id.clone());
            self.active_resources
                .insert(logical_name_id.clone(), resource_id);
        }
        if let Some(registry) = self.v1_registry_authorities.get_mut(&key) {
            registry.logical_name_id = logical_name_id.clone();
            registry.surface_known = surface_known;
        }
        self.v1_names.insert(
            key,
            V1NameState {
                logical_name_id,
                surface_known,
                resource_id,
                token_lineage_id,
                authority_source_family,
                source_manifest_id: None,
                labelhash: None,
                expiry,
                owner,
                authority_key,
            },
        );
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn observe_v1_registrar(
        &mut self,
        namespace: &str,
        namehash: &str,
        logical_name_id: String,
        surface_known: bool,
        resource_id: Uuid,
        token_lineage_id: Uuid,
        authority_source_family: String,
        source_manifest_id: Option<i64>,
        labelhash: Option<String>,
        expiry: Option<i64>,
        owner: Option<String>,
        authority_key: Option<String>,
        make_current: bool,
    ) {
        if surface_known {
            self.remember_known_surface(logical_name_id.clone());
        }
        if make_current && surface_known {
            self.active_resources
                .insert(logical_name_id.clone(), resource_id);
        }
        let value = V1NameState {
            logical_name_id,
            surface_known,
            resource_id,
            token_lineage_id: Some(token_lineage_id),
            authority_source_family,
            source_manifest_id,
            labelhash,
            expiry,
            owner,
            authority_key,
        };
        let key = v1_key(namespace, namehash);
        let previous_expiry = self
            .v1_registrars
            .insert(key.clone(), value.clone())
            .and_then(|state| state.expiry);
        self.update_v1_expiry_index(&key, previous_expiry, expiry);
        if let Some(registry) = self.v1_registry_authorities.get_mut(&key) {
            registry.logical_name_id = value.logical_name_id.clone();
            registry.surface_known = surface_known;
            registry.labelhash = value.labelhash.clone();
        }
        if make_current {
            self.v1_names.insert(key, value);
        }
    }

    pub(super) fn v1_name(&self, namespace: &str, namehash: &str) -> Option<V1NameState> {
        self.v1_names.get(&v1_key(namespace, namehash)).cloned()
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn observe_v1_registry(
        &mut self,
        namespace: &str,
        namehash: &str,
        logical_name_id: String,
        surface_known: bool,
        resource_id: Uuid,
        authority_source_family: String,
        owner: Option<String>,
        authority_key: Option<String>,
    ) {
        let key = v1_key(namespace, namehash);
        let authority = V1NameState {
            logical_name_id,
            surface_known,
            resource_id,
            token_lineage_id: None,
            authority_source_family,
            source_manifest_id: None,
            labelhash: None,
            expiry: None,
            owner,
            authority_key,
        };
        self.v1_registry_authorities.insert(key, authority.clone());
        self.activate_v1_authority(namespace, namehash, Some(authority));
    }

    pub(super) fn remember_v1_registry_authority(
        &mut self,
        namespace: &str,
        namehash: &str,
        authority: V1NameState,
    ) {
        self.v1_registry_authorities
            .insert(v1_key(namespace, namehash), authority);
    }

    pub(super) fn v1_registry_authority(
        &self,
        namespace: &str,
        namehash: &str,
    ) -> Option<V1NameState> {
        self.v1_registry_authorities
            .get(&v1_key(namespace, namehash))
            .cloned()
    }

    pub(super) fn set_v1_registry_owner(
        &mut self,
        namespace: &str,
        namehash: &str,
        owner: String,
    ) -> Option<String> {
        self.v1_registry_owners
            .insert(v1_key(namespace, namehash), owner)
    }

    /// Forgets the registry owner of record and any remembered registry-direct authority for
    /// a node whose logged owner word was unmasked: the word names no authenticatable owner,
    /// and the on-chain write ended the previous registry-direct authority with it.
    pub(super) fn forget_v1_registry_owner(&mut self, namespace: &str, namehash: &str) {
        let key = v1_key(namespace, namehash);
        self.v1_registry_owners.remove(&key);
        self.v1_registry_authorities.remove(&key);
    }

    pub(super) fn v1_registry_owner(&self, namespace: &str, namehash: &str) -> Option<String> {
        self.v1_registry_owners
            .get(&v1_key(namespace, namehash))
            .cloned()
    }

    pub(super) fn activate_v1_authority(
        &mut self,
        namespace: &str,
        namehash: &str,
        authority: Option<V1NameState>,
    ) -> Option<V1NameState> {
        let key = v1_key(namespace, namehash);
        let previous = self.v1_names.remove(&key);
        if let Some(previous) = previous.as_ref()
            && self.active_resources.get(&previous.logical_name_id) == Some(&previous.resource_id)
        {
            self.active_resources.remove(&previous.logical_name_id);
        }
        if let Some(authority) = authority {
            if authority.surface_known {
                self.remember_known_surface(authority.logical_name_id.clone());
                self.active_resources
                    .insert(authority.logical_name_id.clone(), authority.resource_id);
            }
            self.v1_names.insert(key, authority);
        }
        previous
    }

    pub(super) fn mark_v1_migrated(&mut self, namespace: &str, namehash: &str) {
        self.v1_migrated_nodes.insert(v1_key(namespace, namehash));
    }

    pub(super) fn v1_is_migrated(&self, namespace: &str, namehash: &str) -> bool {
        self.v1_migrated_nodes
            .contains(&v1_key(namespace, namehash))
    }

    pub(super) fn v1_registrar(&self, namespace: &str, namehash: &str) -> Option<V1NameState> {
        self.v1_registrars
            .get(&v1_key(namespace, namehash))
            .cloned()
    }

    pub(super) fn transfer_v1_registrar_owner(
        &mut self,
        namespace: &str,
        namehash: &str,
        owner: String,
    ) -> Option<(V1NameState, V1NameState)> {
        let key = v1_key(namespace, namehash);
        let registrar = self.v1_registrars.get_mut(&key)?;
        let before = registrar.clone();
        registrar.owner = Some(owner);
        let after = registrar.clone();
        Some((before, after))
    }

    pub(super) fn converge_v1_registrar_transfer(
        &mut self,
        namespace: &str,
        namehash: &str,
        at_unix_timestamp: i64,
    ) -> Option<V1NameState> {
        let current = self.v1_name(namespace, namehash);
        if current
            .as_ref()
            .is_some_and(|authority| authority.authority_source_family == "ens_v1_wrapper_l1")
        {
            return current;
        }
        let registrar = self.v1_registrar(namespace, namehash)?;
        let registry_owner = self.v1_registry_owner(namespace, namehash);
        let registrar_matches_registry = registry_owner.as_deref().is_none_or(|owner| {
            owner.eq_ignore_ascii_case("0x0000000000000000000000000000000000000000")
                || registrar
                    .owner
                    .as_deref()
                    .is_some_and(|registrant| registrant.eq_ignore_ascii_case(owner))
        });
        let next = if registrar_matches_registry
            && v1_registration_is_live(registrar.expiry, at_unix_timestamp)
        {
            Some(registrar)
        } else {
            self.v1_registry_authority(namespace, namehash)
        };
        self.activate_v1_authority(namespace, namehash, next.clone());
        next
    }

    pub(super) fn transfer_v1_wrapper_owner(
        &mut self,
        namespace: &str,
        namehash: &str,
        source_family: &str,
        owner: String,
    ) -> Option<(V1NameState, V1NameState)> {
        let current = self.v1_names.get_mut(&v1_key(namespace, namehash))?;
        if current.authority_source_family != source_family {
            return None;
        }
        let before = current.clone();
        current.owner = Some(owner);
        Some((before, current.clone()))
    }

    pub(super) fn set_v1_resolver(
        &mut self,
        namespace: &str,
        namehash: &str,
        resolver: Option<String>,
    ) -> Option<String> {
        let key = v1_key(namespace, namehash);
        let previous = self.v1_resolvers.remove(&key);
        if let Some(resolver) = resolver {
            self.v1_resolvers.insert(key, resolver);
        }
        previous
    }

    pub(super) fn v1_resolver(&self, namespace: &str, namehash: &str) -> Option<String> {
        self.v1_resolvers.get(&v1_key(namespace, namehash)).cloned()
    }

    pub(super) fn reactivate_v1_registrar(
        &mut self,
        namespace: &str,
        namehash: &str,
        at_unix_timestamp: i64,
    ) -> Option<V1NameState> {
        let key = v1_key(namespace, namehash);
        let registrar = self
            .v1_registrars
            .get(&key)
            .filter(|state| v1_registration_is_live(state.expiry, at_unix_timestamp))?
            .clone();
        self.v1_names.insert(key, registrar.clone());
        if registrar.surface_known {
            self.remember_known_surface(registrar.logical_name_id.clone());
            self.active_resources
                .insert(registrar.logical_name_id.clone(), registrar.resource_id);
        }
        Some(registrar)
    }

    pub(super) fn reactivate_v1_registrar_for_owner(
        &mut self,
        namespace: &str,
        namehash: &str,
        owner: &str,
        at_unix_timestamp: i64,
    ) -> Option<V1NameState> {
        let registrar = self.v1_registrar(namespace, namehash)?;
        if registrar
            .owner
            .as_deref()
            .is_none_or(|registrant| !registrant.eq_ignore_ascii_case(owner))
            || !v1_registration_is_live(registrar.expiry, at_unix_timestamp)
        {
            return None;
        }
        self.v1_names
            .insert(v1_key(namespace, namehash), registrar.clone());
        if registrar.surface_known {
            self.remember_known_surface(registrar.logical_name_id.clone());
            self.active_resources
                .insert(registrar.logical_name_id.clone(), registrar.resource_id);
        }
        Some(registrar)
    }

    pub(super) fn release_v1_name(
        &mut self,
        namespace: &str,
        namehash: &str,
    ) -> Option<V1NameState> {
        let released = self.v1_names.remove(&v1_key(namespace, namehash));
        if let Some(released) = released.as_ref()
            && self.active_resources.get(&released.logical_name_id) == Some(&released.resource_id)
        {
            self.active_resources.remove(&released.logical_name_id);
        }
        released
    }

    pub(super) fn restore_v1_registration_release(&mut self, namespace: &str, namehash: &str) {
        let key = v1_key(namespace, namehash);
        let registrar = self.v1_registrars.remove(&key);
        self.update_v1_expiry_index(
            &key,
            registrar.as_ref().and_then(|state| state.expiry),
            None,
        );
        let should_release_active = self.v1_names.get(&key).is_some_and(|active| {
            registrar
                .as_ref()
                .is_some_and(|registrar| active.logical_name_id == registrar.logical_name_id)
                || matches!(
                    active.authority_source_family.as_str(),
                    "ens_v1_registrar_l1" | "basenames_base_registrar" | "ens_v1_wrapper_l1"
                )
        });
        if should_release_active {
            let next_authority = self
                .v1_registry_owners
                .get(&key)
                .filter(|owner| {
                    !owner.eq_ignore_ascii_case("0x0000000000000000000000000000000000000000")
                })
                .and_then(|_| self.v1_registry_authorities.get(&key))
                .cloned();
            self.activate_v1_authority(namespace, namehash, next_authority);
        }
    }

    pub(super) fn settle_v1_releases(&mut self, at_unix_timestamp: i64) -> Vec<V1Release> {
        let mut due = Vec::new();
        while let Some((expiry, _)) = self.v1_expiries.get_max() {
            if expiry.checked_add(ENS_GRACE_PERIOD_SECS).is_some() {
                break;
            }
            due.push(self.v1_expiries.remove_max().unwrap().1);
        }
        while let Some((expiry, _)) = self.v1_expiries.get_min() {
            if v1_registration_is_live(Some(*expiry), at_unix_timestamp) {
                break;
            }
            due.push(self.v1_expiries.remove_min().unwrap().1);
        }
        // Preserve the prior OrdMap registrar-key order for deterministic, output-identical releases.
        due.sort();
        let mut releases = Vec::new();
        for key in due {
            let Some(registrar) = self.v1_registrars.remove(&key) else {
                continue;
            };
            let previous_authority = self.v1_names.get(&key).cloned();
            let release_is_active = previous_authority.as_ref().is_some_and(|active| {
                active.resource_id == registrar.resource_id
                    || active.authority_source_family == "ens_v1_wrapper_l1"
            });
            let Some((namespace, namehash)) = key.split_once(':') else {
                continue;
            };
            let next_authority = if release_is_active {
                let next = self
                    .v1_registry_owners
                    .get(&key)
                    .filter(|owner| {
                        !owner.eq_ignore_ascii_case("0x0000000000000000000000000000000000000000")
                    })
                    .and_then(|_| self.v1_registry_authorities.get(&key))
                    .cloned();
                self.activate_v1_authority(namespace, namehash, next.clone());
                next
            } else {
                previous_authority.clone()
            };
            releases.push(V1Release {
                namehash: namehash.to_owned(),
                resolver: self.v1_resolvers.get(&key).cloned(),
                registrar,
                release_was_active: release_is_active,
                previous_authority,
                next_authority,
            });
        }
        releases
    }
    fn update_v1_expiry_index(
        &mut self,
        registrar_key: &str,
        previous: Option<i64>,
        current: Option<i64>,
    ) {
        if previous == current {
            return;
        }
        if let Some(previous) = previous {
            self.v1_expiries
                .remove(&(previous, registrar_key.to_owned()));
        }
        if let Some(current) = current {
            self.v1_expiries.insert((current, registrar_key.to_owned()));
        }
    }
}

const ENS_GRACE_PERIOD_SECS: i64 = 90 * 24 * 60 * 60;

fn v1_registration_is_live(expiry: Option<i64>, at_unix_timestamp: i64) -> bool {
    expiry.is_none_or(|expiry| {
        expiry
            .checked_add(ENS_GRACE_PERIOD_SECS)
            .is_some_and(|release| at_unix_timestamp <= release)
    })
}

fn v1_key(namespace: &str, namehash: &str) -> String {
    format!("{namespace}:{}", namehash.to_ascii_lowercase())
}
