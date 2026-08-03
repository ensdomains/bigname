use serde_json::Value;
use uuid::Uuid;

use super::State;
use crate::schema_v2::common::surface_labels;

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

#[derive(Clone, Debug, Default)]
pub(in crate::schema_v2) struct V2TokenState {
    pub registry_contract_instance_id: Option<Uuid>,
    pub namespace: Option<String>,
    pub raw_label: Option<Vec<u8>>,
    pub expiry: Option<u64>,
    pub name: Option<V2NameState>,
    pub shadow_name: Option<V2RawNameState>,
    pub registration: Option<Value>,
    pub upstream_resource: Option<String>,
    pub resource_id: Option<Uuid>,
    pub token_lineage_id: Option<Uuid>,
    pub resolver: Option<String>,
    pub subregistry: Option<String>,
}

#[derive(Clone, Debug)]
pub(in crate::schema_v2) struct V2NameTransition {
    pub registry: String,
    pub registry_contract_instance_id: Option<Uuid>,
    pub token_id: String,
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
                .unwrap_or(replaced_key);
            replaced.push((displaced_token, displaced));
        }
        self.v2_tokens.insert(
            token_key,
            V2TokenState {
                registry_contract_instance_id,
                namespace: Some(namespace.to_owned()),
                raw_label: Some(raw_label.to_vec()),
                expiry: Some(expiry),
                registration,
                ..V2TokenState::default()
            },
        );
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
                self.v2_tokens.remove(&displaced_key);
            }
            let existing = self
                .v2_tokens
                .get_mut(&token_key)
                .expect("the restored ENSv2 token was checked above");
            existing.registry_contract_instance_id =
                registry_contract_instance_id.or(existing.registry_contract_instance_id);
            existing.namespace = Some(namespace.to_owned());
            existing.raw_label = Some(raw_label.to_vec());
            existing.expiry = Some(expiry);
            if registration.is_some() {
                existing.registration = registration;
            }
            return;
        }
        let _ = self.install_v2_registration(
            &emitter,
            token_id,
            registry_contract_instance_id,
            namespace,
            raw_label,
            expiry,
            registration,
        );
    }

    pub(in crate::schema_v2) fn refresh_v2_names(
        &mut self,
        at_unix_timestamp: i64,
    ) -> Vec<V2NameTransition> {
        let mut transitions = Vec::new();
        let keys = self.v2_tokens.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            let Some((emitter, token_id)) = key.rsplit_once(':') else {
                continue;
            };
            let Some(token) = self.v2_tokens.get(&key) else {
                continue;
            };
            let (Some(namespace), Some(raw_label)) =
                (token.namespace.as_ref(), token.raw_label.as_ref())
            else {
                continue;
            };
            let raw_name = self
                .v2_registry_raw_suffix(emitter, namespace, at_unix_timestamp)
                .map(|mut suffix| {
                    suffix.insert(0, raw_label.clone());
                    suffix
                });
            let name = raw_name
                .as_ref()
                .and_then(|raw_labels| surface_labels(raw_labels))
                .map(|labels| {
                    let namehash = crate::schema_v2::common::namehash(&labels);
                    V2NameState {
                        logical_name_id: format!("{namespace}:{namehash}"),
                        labels,
                        namehash,
                    }
                });
            let shadow_name = raw_name.filter(|_| name.is_none()).map(|raw_labels| {
                let namehash =
                    crate::schema_v2::common::namehash_raw(raw_labels.iter().map(Vec::as_slice));
                V2RawNameState {
                    logical_name_id: format!("{namespace}:{namehash}"),
                    raw_labels,
                    namehash,
                }
            });
            let previous = token.name.clone();
            let previous_shadow = token.shadow_name.clone();
            if previous != name || previous_shadow != shadow_name {
                if let Some(previous) = previous.as_ref()
                    && token.resource_id.is_some_and(|resource_id| {
                        self.active_resources.get(&previous.logical_name_id) == Some(&resource_id)
                    })
                {
                    self.active_resources.remove(&previous.logical_name_id);
                }
                if let Some(current) = name.as_ref() {
                    self.known_surfaces.insert(current.logical_name_id.clone());
                    if let Some(resource_id) = token.resource_id {
                        self.active_resources
                            .insert(current.logical_name_id.clone(), resource_id);
                    }
                }
                transitions.push(V2NameTransition {
                    registry: emitter.to_owned(),
                    registry_contract_instance_id: token.registry_contract_instance_id,
                    token_id: token_id.to_owned(),
                    previous,
                    previous_shadow,
                    current: name.clone(),
                    current_shadow: shadow_name.clone(),
                    resource_id: token.resource_id,
                    token_lineage_id: token.token_lineage_id,
                    upstream_resource: token.upstream_resource.clone(),
                    registration: token.registration.clone(),
                    resolver: token.resolver.clone(),
                    subregistry: token.subregistry.clone(),
                });
            }
            if let Some(token) = self.v2_tokens.get_mut(&key) {
                token.name = name;
                token.shadow_name = shadow_name;
            }
        }
        transitions
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
                self.v2_parent_claims
                    .insert(registry, (parent.to_ascii_lowercase(), raw_label.to_vec()));
            }
            None => {
                self.v2_parent_claims.remove(&registry);
            }
        }
    }

    pub(in crate::schema_v2) fn set_v2_expiry(
        &mut self,
        emitter: &str,
        token_id: &str,
        expiry: u64,
    ) {
        if let Some(entry) = self.v2_tokens.get_mut(&v2_key(emitter, token_id)) {
            entry.expiry = Some(expiry);
            if let Some(registration) = entry.registration.as_mut()
                && let Some(registration) = registration.as_object_mut()
            {
                registration.insert("expiry".to_owned(), Value::from(expiry));
            }
        }
    }

    pub(in crate::schema_v2) fn transfer_v2_registrant(
        &mut self,
        emitter: &str,
        token_id: &str,
        registrant: String,
    ) -> Option<V2TokenState> {
        let entry = self.v2_tokens.get_mut(&v2_key(emitter, token_id))?;
        if let Some(registration) = entry.registration.as_mut()
            && let Some(registration) = registration.as_object_mut()
        {
            registration.insert("registrant".to_owned(), Value::String(registrant));
            registration.remove("owner");
        }
        Some(entry.clone())
    }

    pub(in crate::schema_v2) fn link_v2_resource(
        &mut self,
        emitter: &str,
        token_id: &str,
        upstream_resource: String,
        resource_id: Uuid,
        token_lineage_id: Option<Uuid>,
    ) -> V2TokenState {
        let state = {
            let entry = self.v2_tokens.entry(v2_key(emitter, token_id)).or_default();
            entry.upstream_resource = Some(upstream_resource);
            entry.resource_id = Some(resource_id);
            entry.token_lineage_id = token_lineage_id;
            entry.clone()
        };
        if let Some(name) = state.name.as_ref() {
            self.known_surfaces.insert(name.logical_name_id.clone());
            self.active_resources
                .insert(name.logical_name_id.clone(), resource_id);
        }
        state
    }

    pub(in crate::schema_v2) fn v2_token(
        &self,
        emitter: &str,
        token_id: &str,
    ) -> Option<V2TokenState> {
        self.v2_tokens.get(&v2_key(emitter, token_id)).cloned()
    }

    pub(in crate::schema_v2) fn v2_token_by_upstream_resource(
        &self,
        emitter: &str,
        upstream_resource: &str,
    ) -> anyhow::Result<Option<V2TokenState>> {
        let emitter = format!("{}:", emitter.to_ascii_lowercase());
        let mut matching = self
            .v2_tokens
            .iter()
            .filter(|(key, state)| {
                key.starts_with(&emitter)
                    && state.upstream_resource.as_deref() == Some(upstream_resource)
            })
            .map(|(_, state)| state.clone());
        let first = matching.next();
        if first.is_some() && matching.next().is_some() {
            anyhow::bail!(
                "ENSv2 upstream resource {upstream_resource} maps to more than one retained token"
            );
        }
        Ok(first)
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

    pub(in crate::schema_v2) fn set_v2_resolver(
        &mut self,
        emitter: &str,
        token_id: &str,
        resolver: Option<String>,
    ) {
        self.v2_tokens
            .entry(v2_key(emitter, token_id))
            .or_default()
            .resolver = resolver;
    }

    pub(in crate::schema_v2) fn set_v2_subregistry(
        &mut self,
        emitter: &str,
        token_id: &str,
        subregistry: Option<String>,
    ) {
        self.v2_tokens
            .entry(v2_key(emitter, token_id))
            .or_default()
            .subregistry = subregistry;
    }

    pub(in crate::schema_v2) fn v2_token_for_logical_name(
        &self,
        token_id: &str,
        logical_name_id: &str,
    ) -> anyhow::Result<Option<V2TokenState>> {
        let suffix = format!(":{}", token_id.to_ascii_lowercase());
        let mut matches = self
            .v2_tokens
            .iter()
            .filter(|(key, state)| {
                key.ends_with(&suffix)
                    && (state.name.as_ref().is_some_and(|name| {
                        name.logical_name_id.eq_ignore_ascii_case(logical_name_id)
                    }) || state.shadow_name.as_ref().is_some_and(|name| {
                        name.logical_name_id.eq_ignore_ascii_case(logical_name_id)
                    }))
            })
            .map(|(_, state)| state.clone());
        let first = matches.next();
        if first.is_some() && matches.next().is_some() {
            anyhow::bail!(
                "ENSv2 registrar token {token_id} and name {logical_name_id} map to more than one retained registry resource"
            );
        }
        Ok(first)
    }

    pub(in crate::schema_v2) fn regenerate_v2_token(
        &mut self,
        emitter: &str,
        old_token_id: &str,
        new_token_id: &str,
    ) -> Option<V2TokenState> {
        let state = self.v2_tokens.remove(&v2_key(emitter, old_token_id))?;
        let new_key = v2_key(emitter, new_token_id);
        if let Some(label) = state.raw_label.as_ref() {
            self.v2_entry_by_parent_label.insert(
                (emitter.to_ascii_lowercase(), label.clone()),
                new_key.clone(),
            );
        }
        self.v2_tokens.insert(new_key, state.clone());
        Some(state)
    }

    pub(in crate::schema_v2) fn release_v2_token(
        &mut self,
        emitter: &str,
        token_id: &str,
    ) -> Option<V2TokenState> {
        let emitter = emitter.to_ascii_lowercase();
        let state = self.v2_tokens.remove(&v2_key(&emitter, token_id))?;
        if let (Some(name), Some(resource_id)) = (state.name.as_ref(), state.resource_id)
            && self.active_resources.get(&name.logical_name_id) == Some(&resource_id)
        {
            self.active_resources.remove(&name.logical_name_id);
        }
        if let Some(label) = state.raw_label.as_ref() {
            self.v2_entry_by_parent_label
                .remove(&(emitter, label.clone()));
        }
        Some(state)
    }

    pub(in crate::schema_v2) fn observe_name_surface(&mut self, logical_name_id: String) {
        self.known_surfaces.insert(logical_name_id);
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
}

fn v2_key(emitter: &str, token_id: &str) -> String {
    format!(
        "{}:{}",
        emitter.to_ascii_lowercase(),
        token_id.to_ascii_lowercase()
    )
}
