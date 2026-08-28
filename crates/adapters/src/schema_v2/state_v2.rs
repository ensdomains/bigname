use std::collections::BTreeSet;

use serde_json::Value;
use uuid::Uuid;

use super::{State, v2_pointers::same_resolver_observation};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::schema_v2) struct V2NameState {
    pub labels: Vec<String>,
    pub namehash: String,
    pub logical_name_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::schema_v2) struct V2RawNameState {
    pub raw_labels: Vec<Vec<u8>>,
    pub namehash: String,
    pub logical_name_id: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(in crate::schema_v2) struct V2TokenState {
    pub registry_contract_instance_id: Option<Uuid>,
    pub namespace: Option<String>,
    pub raw_label: Option<Vec<u8>>,
    pub expiry: Option<u64>,
    pub name: Option<V2NameState>,
    pub shadow_name: Option<V2RawNameState>,
    pub last_logical_name_id: Option<String>,
    pub registration: Option<Value>,
    pub upstream_resource: Option<String>,
    pub resource_id: Option<Uuid>,
    pub token_lineage_id: Option<Uuid>,
    pub resolver: Option<String>,
    pub resolver_discovery_aliases: BTreeSet<String>,
    pub subregistry: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::schema_v2) struct V2NameTransition {
    pub registry: String,
    pub registry_contract_instance_id: Option<Uuid>,
    pub token_id: String,
    pub expiry: Option<u64>,
    pub previous: Option<V2NameState>,
    pub previous_shadow: Option<V2RawNameState>,
    pub current: Option<V2NameState>,
    pub current_shadow: Option<V2RawNameState>,
    pub resource_id: Option<Uuid>,
    pub token_lineage_id: Option<Uuid>,
    pub upstream_resource: Option<String>,
    pub registration: Option<Value>,
    pub resolver: Option<String>,
    pub subregistry: Option<String>,
}

impl State {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::schema_v2) fn replace_v2_registration(
        &mut self,
        emitter: &str,
        token_id: &str,
        registry_contract_instance_id: Uuid,
        namespace: &str,
        raw_label: &[u8],
        expiry: u64,
        registration: Option<Value>,
    ) -> Vec<(String, V2TokenState)> {
        self.install_v2_registration(
            emitter,
            token_id,
            Some(registry_contract_instance_id),
            namespace,
            raw_label,
            expiry,
            registration,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn install_v2_registration(
        &mut self,
        emitter: &str,
        token_id: &str,
        registry_contract_instance_id: Option<Uuid>,
        namespace: &str,
        raw_label: &[u8],
        expiry: u64,
        registration: Option<Value>,
    ) -> Vec<(String, V2TokenState)> {
        let emitter = emitter.to_ascii_lowercase();
        let token_key = v2_key(&emitter, token_id);
        let mut replaced = Vec::new();
        let previous = self.v2_tokens.remove(&token_key);
        let retained_reservation_resource = previous.as_ref().filter(|previous| {
            previous.registration.is_none()
                && registration.is_some()
                && previous.raw_label.as_deref() == Some(raw_label)
        });
        let upstream_resource =
            retained_reservation_resource.and_then(|previous| previous.upstream_resource.clone());
        let resource_id = retained_reservation_resource.and_then(|previous| previous.resource_id);
        let token_lineage_id =
            retained_reservation_resource.and_then(|previous| previous.token_lineage_id);
        if let Some(previous) = previous.as_ref() {
            if previous.resolver.is_some() {
                self.set_v2_resolver_token_index(&emitter, token_id, false);
            }
            self.set_v2_token_resolver_alias_index(&emitter, token_id, previous, false);
            self.replace_v2_token_indexes(&token_key, Some(previous), None);
            self.replace_v2_expiry_index(&token_key, previous.expiry, None);
            self.remove_v2_current_surface(previous);
            if let Some(subregistry) = previous.subregistry.as_deref() {
                self.mark_v2_registry_dirty(subregistry);
            }
        }
        if let Some(previous) = previous.as_ref()
            && let Some(previous_label) = previous.raw_label.as_ref()
        {
            self.v2_entry_by_parent_label
                .remove(&(emitter.clone(), previous_label.clone()));
        }
        if let Some(previous) = previous {
            replaced.push((token_id.to_owned(), previous));
        }
        if let Some(replaced_key) = self
            .v2_entry_by_parent_label
            .insert((emitter.clone(), raw_label.to_vec()), token_key.clone())
            && replaced_key != token_key
            && let Some(displaced) = self.v2_tokens.remove(&replaced_key)
        {
            let displaced_token = replaced_key
                .rsplit_once(':')
                .map(|(_, token)| token.to_owned())
                .unwrap_or_else(|| replaced_key.clone());
            if displaced.resolver.is_some() {
                self.set_v2_resolver_token_index(&emitter, &displaced_token, false);
            }
            self.set_v2_token_resolver_alias_index(&emitter, &displaced_token, &displaced, false);
            self.replace_v2_token_indexes(&replaced_key, Some(&displaced), None);
            self.replace_v2_expiry_index(&replaced_key, displaced.expiry, None);
            self.remove_v2_current_surface(&displaced);
            if let Some(subregistry) = displaced.subregistry.as_deref() {
                self.mark_v2_registry_dirty(subregistry);
            }
            replaced.push((displaced_token, displaced));
        }
        let current = V2TokenState {
            registry_contract_instance_id,
            namespace: Some(namespace.to_owned()),
            raw_label: Some(raw_label.to_vec()),
            expiry: Some(expiry),
            registration,
            upstream_resource,
            resource_id,
            token_lineage_id,
            ..V2TokenState::default()
        };
        self.v2_tokens.insert(token_key.clone(), current.clone());
        self.replace_v2_token_indexes(&token_key, None, Some(&current));
        self.replace_v2_expiry_index(&token_key, None, Some(expiry));
        self.mark_v2_token_component_dirty(&token_key);
        replaced
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::schema_v2) fn restore_v2_registration(
        &mut self,
        emitter: &str,
        token_id: &str,
        registry_contract_instance_id: Option<Uuid>,
        namespace: &str,
        raw_label: &[u8],
        expiry: u64,
        registration: Option<Value>,
    ) {
        let emitter = emitter.to_ascii_lowercase();
        let token_key = v2_key(&emitter, token_id);
        if self.v2_tokens.get(&token_key).is_some_and(|existing| {
            existing
                .raw_label
                .as_deref()
                .is_none_or(|existing_label| existing_label == raw_label)
        }) {
            if let Some(displaced_key) = self
                .v2_entry_by_parent_label
                .insert((emitter.clone(), raw_label.to_vec()), token_key.clone())
                && displaced_key != token_key
            {
                self.remove_v2_displaced_restored_token(&emitter, &displaced_key);
            }
            let previous_expiry;
            {
                let existing = self
                    .v2_tokens
                    .get_mut(&token_key)
                    .expect("the restored ENSv2 token was checked above");
                previous_expiry = existing.expiry;
                existing.registry_contract_instance_id =
                    registry_contract_instance_id.or(existing.registry_contract_instance_id);
                existing.namespace = Some(namespace.to_owned());
                existing.raw_label = Some(raw_label.to_vec());
                existing.expiry = Some(expiry);
                if registration.is_some() {
                    existing.registration = registration;
                }
            }
            self.replace_v2_expiry_index(&token_key, previous_expiry, Some(expiry));
            self.mark_v2_token_component_dirty(&token_key);
            return;
        }
        let replaced = self.install_v2_registration(
            &emitter,
            token_id,
            registry_contract_instance_id,
            namespace,
            raw_label,
            expiry,
            registration,
        );
        // Live interpretation preserves the historical one-shot behavior while producing the
        // batch; restore clears only opaque displaced state after the output boundary.
        for (_, displaced) in &replaced {
            self.remove_v2_active_resource(displaced);
        }
    }

    pub(in crate::schema_v2) fn v2_name_for_registration(
        &self,
        emitter: &str,
        namespace: &str,
        raw_label: &str,
        at_unix_timestamp: i64,
    ) -> Option<V2NameState> {
        let mut labels = self.v2_registry_suffix(emitter, namespace, at_unix_timestamp)?;
        labels.insert(0, raw_label.to_owned());
        let namehash = crate::schema_v2::common::namehash(&labels);
        Some(V2NameState {
            logical_name_id: format!("{namespace}:{namehash}"),
            labels,
            namehash,
        })
    }

    pub(in crate::schema_v2) fn v2_shadow_name_for_parent_claim(
        &self,
        parent: &str,
        namespace: &str,
        raw_label: &[u8],
        at_unix_timestamp: i64,
    ) -> Option<(Vec<Vec<u8>>, String)> {
        let mut labels = self.v2_registry_raw_suffix(parent, namespace, at_unix_timestamp)?;
        labels.insert(0, raw_label.to_vec());
        let namehash = crate::schema_v2::common::namehash_raw(labels.iter().map(Vec::as_slice));
        Some((labels, namehash))
    }

    pub(in crate::schema_v2) fn set_v2_parent_claim(
        &mut self,
        registry: &str,
        parent: Option<String>,
        raw_label: &[u8],
    ) {
        let registry = registry.to_ascii_lowercase();
        match parent {
            Some(parent) => {
                self.v2_parent_claims.insert(
                    registry.clone(),
                    (parent.to_ascii_lowercase(), raw_label.to_vec()),
                );
            }
            None => {
                self.v2_parent_claims.remove(&registry);
            }
        }
        self.mark_v2_registry_dirty(&registry);
    }

    pub(in crate::schema_v2) fn set_v2_expiry(
        &mut self,
        emitter: &str,
        token_id: &str,
        expiry: u64,
    ) {
        let key = v2_key(emitter, token_id);
        let previous_expiry;
        if let Some(entry) = self.v2_tokens.get_mut(&key) {
            previous_expiry = entry.expiry;
            entry.expiry = Some(expiry);
            if let Some(registration) = entry.registration.as_mut()
                && let Some(registration) = registration.as_object_mut()
            {
                registration.insert("expiry".to_owned(), Value::from(expiry));
            }
        } else {
            return;
        }
        self.replace_v2_expiry_index(&key, previous_expiry, Some(expiry));
        self.mark_v2_token_component_dirty(&key);
    }

    pub(in crate::schema_v2) fn transfer_v2_registrant(
        &mut self,
        emitter: &str,
        token_id: &str,
        registrant: String,
    ) -> Option<V2TokenState> {
        let key = v2_key(emitter, token_id);
        let state = {
            let entry = self.v2_tokens.get_mut(&key)?;
            if let Some(registration) = entry.registration.as_mut()
                && let Some(registration) = registration.as_object_mut()
            {
                registration.insert("registrant".to_owned(), Value::String(registrant));
                registration.remove("owner");
            }
            entry.clone()
        };
        self.mark_v2_token_dirty(key);
        Some(state)
    }

    pub(in crate::schema_v2) fn link_v2_resource(
        &mut self,
        emitter: &str,
        token_id: &str,
        upstream_resource: String,
        resource_id: Uuid,
        token_lineage_id: Option<Uuid>,
    ) -> V2TokenState {
        let key = v2_key(emitter, token_id);
        let previous = self.v2_tokens.get(&key).cloned();
        let state = {
            let entry = self.v2_tokens.entry(key.clone()).or_default();
            entry.upstream_resource = Some(upstream_resource);
            entry.resource_id = Some(resource_id);
            entry.token_lineage_id = token_lineage_id;
            entry.clone()
        };
        self.replace_v2_token_indexes(&key, previous.as_ref(), Some(&state));
        self.mark_v2_token_dirty(key);
        if state.registration.is_some()
            && let Some(name) = state.name.as_ref()
        {
            self.known_surfaces.insert(name.logical_name_id.clone());
            self.active_resources
                .insert(name.logical_name_id.clone(), resource_id);
        }
        state
    }

    pub(in crate::schema_v2) fn attach_v2_unbound_resource(
        &mut self,
        emitter: &str,
        token_id: &str,
        upstream_resource: String,
        resource_id: Uuid,
        token_lineage_id: Option<Uuid>,
    ) -> V2TokenState {
        let key = v2_key(emitter, token_id);
        let previous = self.v2_tokens.get(&key).cloned();
        let current = {
            let entry = self.v2_tokens.entry(key.clone()).or_default();
            entry.upstream_resource = Some(upstream_resource);
            entry.resource_id = Some(resource_id);
            entry.token_lineage_id = token_lineage_id;
            entry.clone()
        };
        self.replace_v2_token_indexes(&key, previous.as_ref(), Some(&current));
        self.mark_v2_token_dirty(key);
        current
    }

    pub(in crate::schema_v2) fn restore_v2_unbound_resource(
        &mut self,
        emitter: &str,
        token_id: &str,
        event: &crate::schema_v2::model::PriorEventInput,
    ) {
        if let (Some(resource_id), Some(upstream_resource)) = (
            event.resource_id,
            event
                .after_state
                .get("upstream_resource")
                .and_then(Value::as_str),
        ) {
            self.attach_v2_unbound_resource(
                emitter,
                token_id,
                upstream_resource.to_owned(),
                resource_id,
                event
                    .after_state
                    .get("token_lineage_id")
                    .and_then(Value::as_str)
                    .and_then(|value| Uuid::parse_str(value).ok()),
            );
        }
    }

    pub(in crate::schema_v2) fn v2_token(
        &self,
        emitter: &str,
        token_id: &str,
    ) -> Option<V2TokenState> {
        self.v2_tokens.get(&v2_key(emitter, token_id)).cloned()
    }

    pub(in crate::schema_v2) fn observe_v2_resolver_hint(
        &mut self,
        resolver: &str,
        upstream_resource: &str,
        logical_name_id: String,
        selector: Value,
    ) {
        self.v2_resolver_hints.insert(
            (
                resolver.to_ascii_lowercase(),
                upstream_resource.to_ascii_lowercase(),
            ),
            (logical_name_id, selector),
        );
    }

    pub(in crate::schema_v2) fn v2_resolver_hint(
        &self,
        resolver: &str,
        upstream_resource: &str,
    ) -> Option<(String, Option<Uuid>, Value)> {
        let (logical_name_id, selector) = self.v2_resolver_hints.get(&(
            resolver.to_ascii_lowercase(),
            upstream_resource.to_ascii_lowercase(),
        ))?;
        Some((
            logical_name_id.clone(),
            self.active_resources.get(logical_name_id).copied(),
            selector.clone(),
        ))
    }

    pub(in crate::schema_v2) fn regenerate_v2_token(
        &mut self,
        emitter: &str,
        old_token_id: &str,
        new_token_id: &str,
    ) -> Option<(V2TokenState, Option<V2TokenState>)> {
        let old_key = v2_key(emitter, old_token_id);
        let mut state = self.v2_tokens.remove(&old_key)?;
        if state.resolver.is_some() {
            self.set_v2_resolver_token_index(emitter, old_token_id, false);
        }
        self.set_v2_token_resolver_alias_index(emitter, old_token_id, &state, false);
        self.replace_v2_token_indexes(&old_key, Some(&state), None);
        self.replace_v2_expiry_index(&old_key, state.expiry, None);
        let new_key = v2_key(emitter, new_token_id);
        if state.resolver.is_some() && !same_resolver_observation(old_token_id, new_token_id) {
            state
                .resolver_discovery_aliases
                .insert(old_token_id.to_owned());
        }
        if let Some(label) = state.raw_label.as_ref() {
            self.v2_entry_by_parent_label.insert(
                (emitter.to_ascii_lowercase(), label.clone()),
                new_key.clone(),
            );
        }
        let displaced = self.v2_tokens.insert(new_key.clone(), state.clone());
        if let Some(displaced) = displaced.as_ref() {
            if displaced.resolver.is_some() {
                self.set_v2_resolver_token_index(emitter, new_token_id, false);
            }
            self.set_v2_token_resolver_alias_index(emitter, new_token_id, displaced, false);
            self.replace_v2_token_indexes(&new_key, Some(displaced), None);
            self.replace_v2_expiry_index(&new_key, displaced.expiry, None);
            self.remove_v2_current_name(displaced);
            if let Some(subregistry) = displaced.subregistry.as_deref() {
                self.mark_v2_registry_dirty(subregistry);
            }
            if displaced.raw_label != state.raw_label
                && let Some(label) = displaced.raw_label.as_ref()
            {
                self.v2_entry_by_parent_label
                    .remove(&(emitter.to_ascii_lowercase(), label.clone()));
            }
        }
        self.replace_v2_token_indexes(&new_key, None, Some(&state));
        if state.resolver.is_some() {
            self.set_v2_resolver_token_index(emitter, new_token_id, true);
        }
        self.set_v2_token_resolver_alias_index(emitter, new_token_id, &state, true);
        self.replace_v2_expiry_index(&new_key, None, state.expiry);
        self.mark_v2_token_component_dirty(&new_key);
        Some((state, displaced))
    }

    pub(in crate::schema_v2) fn release_v2_token(
        &mut self,
        emitter: &str,
        token_id: &str,
    ) -> Option<V2TokenState> {
        let emitter = emitter.to_ascii_lowercase();
        let token_key = v2_key(&emitter, token_id);
        let state = self.v2_tokens.remove(&token_key)?;
        if state.resolver.is_some() {
            self.set_v2_resolver_token_index(&emitter, token_id, false);
        }
        self.set_v2_token_resolver_alias_index(&emitter, token_id, &state, false);
        self.replace_v2_token_indexes(&token_key, Some(&state), None);
        self.replace_v2_expiry_index(&token_key, state.expiry, None);
        self.remove_v2_current_name(&state);
        if let Some(subregistry) = state.subregistry.as_deref() {
            self.mark_v2_registry_dirty(subregistry);
        }
        if let Some(label) = state.raw_label.as_ref() {
            self.v2_entry_by_parent_label
                .remove(&(emitter, label.clone()));
        }
        Some(state)
    }

    pub(in crate::schema_v2) fn observe_name_surface(&mut self, logical_name_id: String) {
        self.remember_known_surface(logical_name_id);
    }

    pub(in crate::schema_v2) fn name_link_by_namehash(
        &self,
        namespace: &str,
        namehash: &str,
    ) -> Option<(String, Option<Uuid>)> {
        let logical_name_id = format!("{namespace}:{}", namehash.to_ascii_lowercase());
        self.known_surfaces.contains(&logical_name_id).then(|| {
            let resource_id = self.active_resources.get(&logical_name_id).copied();
            (logical_name_id, resource_id)
        })
    }

    pub(super) fn replace_v2_expiry_index(
        &mut self,
        token_key: &str,
        previous: Option<u64>,
        current: Option<u64>,
    ) {
        if previous == current {
            return;
        }
        if let Some(previous) = previous {
            self.v2_expiries.remove(&(previous, token_key.to_owned()));
        }
        if let Some(current) = current {
            self.v2_expiries.insert((current, token_key.to_owned()));
        }
    }

    pub(super) fn remove_v2_current_name(&mut self, token: &V2TokenState) {
        self.remove_v2_current_surface(token);
        self.remove_v2_active_resource(token);
    }

    fn remove_v2_current_surface(&mut self, token: &V2TokenState) {
        let logical_name_id = token
            .name
            .as_ref()
            .map(|name| name.logical_name_id.as_str());
        if let Some(logical_name_id) = logical_name_id {
            // The departing holder may own the surface's active resource; dirty the surviving
            // co-holders so the next targeted refresh re-elects the winner a full walk would.
            let co_holders = self
                .v2_tokens_by_current_name_index
                .get(logical_name_id)
                .cloned();
            for token_key in co_holders.into_iter().flatten() {
                self.mark_v2_token_dirty(token_key);
            }
        }
        self.replace_v2_current_surface(logical_name_id, None);
    }

    fn remove_v2_active_resource(&mut self, token: &V2TokenState) {
        let logical_name_id = token
            .name
            .as_ref()
            .map(|name| name.logical_name_id.as_str());
        if let (Some(logical_name_id), Some(resource_id)) = (logical_name_id, token.resource_id)
            && self.active_resources.get(logical_name_id) == Some(&resource_id)
        {
            self.active_resources.remove(logical_name_id);
        }
    }
}

pub(super) fn v2_key(emitter: &str, token_id: &str) -> String {
    format!(
        "{}:{}",
        emitter.to_ascii_lowercase(),
        token_id.to_ascii_lowercase()
    )
}
