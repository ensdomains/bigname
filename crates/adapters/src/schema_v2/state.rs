use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};
use uuid::Uuid;

use super::{model::PriorEventInput, state_key::interpreter_state_key};

#[path = "state_topology.rs"]
mod topology;

#[derive(Clone, Debug)]
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

#[path = "state_v2.rs"]
mod v2;

pub(super) use self::v2::{V2NameState, V2NameTransition, V2TokenState};

#[derive(Clone, Debug)]
pub(super) struct V1Release {
    pub namehash: String,
    pub registrar: V1NameState,
    pub release_was_active: bool,
    pub previous_authority: Option<V1NameState>,
    pub next_authority: Option<V1NameState>,
    pub resolver: Option<String>,
}

pub(super) struct State {
    values: BTreeMap<String, Value>,
    v1_names: BTreeMap<String, V1NameState>,
    v1_registrars: BTreeMap<String, V1NameState>,
    v1_registry_authorities: BTreeMap<String, V1NameState>,
    v1_registry_owners: BTreeMap<String, String>,
    v1_resolvers: BTreeMap<String, String>,
    v1_migrated_nodes: std::collections::BTreeSet<String>,
    known_surfaces: BTreeSet<String>,
    active_resources: BTreeMap<String, Uuid>,
    v2_tokens: BTreeMap<String, V2TokenState>,
    v2_entry_by_parent_label: BTreeMap<(String, String), String>,
    v2_parent_claims: BTreeMap<String, (String, String)>,
    v2_suffix_anchors: BTreeMap<String, (String, Vec<String>)>,
    v2_resolver_hints: BTreeMap<(String, String), (String, Value)>,
}

impl State {
    pub(super) fn new(
        prior: Vec<PriorEventInput>,
        v2_suffix_anchors: Vec<(String, String, Vec<String>)>,
    ) -> Self {
        let mut state = Self {
            values: BTreeMap::new(),
            v1_names: BTreeMap::new(),
            v1_registrars: BTreeMap::new(),
            v1_registry_authorities: BTreeMap::new(),
            v1_registry_owners: BTreeMap::new(),
            v1_resolvers: BTreeMap::new(),
            v1_migrated_nodes: std::collections::BTreeSet::new(),
            known_surfaces: BTreeSet::new(),
            active_resources: BTreeMap::new(),
            v2_tokens: BTreeMap::new(),
            v2_entry_by_parent_label: BTreeMap::new(),
            v2_parent_claims: BTreeMap::new(),
            v2_resolver_hints: BTreeMap::new(),
            v2_suffix_anchors: v2_suffix_anchors
                .into_iter()
                .map(|(address, namespace, suffix)| {
                    (address.to_ascii_lowercase(), (namespace, suffix))
                })
                .collect(),
        };
        let mut latest_timestamp = None;
        for event in prior {
            if matches!(
                event.source_family.as_str(),
                "ens_v2_registry_l1" | "ens_v2_root_l1"
            ) {
                latest_timestamp = latest_timestamp.max(event.block_timestamp);
            }
            let scope = event.state_scope.as_deref().unwrap_or("legacy");
            let key = interpreter_state_key(
                &event.namespace,
                event.logical_name_id.as_deref(),
                event.resource_id,
                &event.event_kind,
                &event.source_family,
                scope,
            );
            state.values.insert(key, event.after_state.clone());
            super::state_restore::v1(&mut state, &event);
            super::state_restore::v2(&mut state, &event);
        }
        if let Some(timestamp) = latest_timestamp {
            // Replaying prior events reconstructs topology before the next raw-log batch. The
            // resulting transitions already exist in normalized_events, so only retain the
            // reconstructed name state here.
            state.refresh_v2_names(timestamp.unix_timestamp());
        }
        state
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn observe_v1_name(
        &mut self,
        namespace: &str,
        namehash: &str,
        logical_name_id: String,
        resource_id: Uuid,
        token_lineage_id: Option<Uuid>,
        authority_source_family: String,
        expiry: Option<i64>,
        owner: Option<String>,
        authority_key: Option<String>,
    ) {
        let key = v1_key(namespace, namehash);
        self.known_surfaces.insert(logical_name_id.clone());
        self.active_resources
            .insert(logical_name_id.clone(), resource_id);
        if let Some(registry) = self.v1_registry_authorities.get_mut(&key) {
            registry.logical_name_id = logical_name_id.clone();
            registry.surface_known = true;
        }
        self.v1_names.insert(
            key,
            V1NameState {
                logical_name_id,
                surface_known: true,
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
        self.known_surfaces.insert(logical_name_id.clone());
        if make_current {
            self.active_resources
                .insert(logical_name_id.clone(), resource_id);
        }
        let value = V1NameState {
            logical_name_id,
            surface_known: true,
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
        self.v1_registrars.insert(key.clone(), value.clone());
        if let Some(registry) = self.v1_registry_authorities.get_mut(&key) {
            registry.logical_name_id = value.logical_name_id.clone();
            registry.surface_known = true;
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
                self.known_surfaces
                    .insert(authority.logical_name_id.clone());
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
        self.known_surfaces
            .insert(registrar.logical_name_id.clone());
        self.active_resources
            .insert(registrar.logical_name_id.clone(), registrar.resource_id);
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
        self.known_surfaces
            .insert(registrar.logical_name_id.clone());
        self.active_resources
            .insert(registrar.logical_name_id.clone(), registrar.resource_id);
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

    pub(super) fn update_v1_expiry(
        &mut self,
        namespace: &str,
        namehash: &str,
        expiry: i64,
    ) -> Option<V1NameState> {
        let state = self.v1_names.get_mut(&v1_key(namespace, namehash))?;
        state.expiry = Some(expiry);
        Some(state.clone())
    }

    pub(super) fn settle_v1_releases(&mut self, at_unix_timestamp: i64) -> Vec<V1Release> {
        let due = self
            .v1_registrars
            .iter()
            .filter_map(|(key, registrar)| {
                let expiry = registrar.expiry?;
                (!v1_registration_is_live(Some(expiry), at_unix_timestamp)).then(|| key.clone())
            })
            .collect::<Vec<_>>();
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

    #[allow(clippy::too_many_arguments)]
    pub(super) fn transition(
        &mut self,
        namespace: &str,
        logical_name_id: Option<&str>,
        resource_id: Option<Uuid>,
        event_kind: &str,
        source_family: &str,
        state_scope: &str,
        explicit_before: Option<Value>,
        after: Value,
    ) -> Value {
        let key = interpreter_state_key(
            namespace,
            logical_name_id,
            resource_id,
            event_kind,
            source_family,
            state_scope,
        );
        let before = explicit_before
            .or_else(|| self.values.get(&key).cloned())
            .unwrap_or_else(|| json!({}));
        self.values.insert(key, after);
        before
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
