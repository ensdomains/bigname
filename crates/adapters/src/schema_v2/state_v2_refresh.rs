use super::{State, V2NameState, V2NameTransition, V2RawNameState, v2::v2_key};
use crate::schema_v2::common::surface_labels;

#[cfg(test)]
std::thread_local! {
    static V2_REFRESH_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(in crate::schema_v2) fn reset_v2_refresh_visits() {
    V2_REFRESH_VISITS.set(0);
}

#[cfg(test)]
pub(in crate::schema_v2) fn v2_refresh_visits() -> usize {
    V2_REFRESH_VISITS.get()
}

impl State {
    pub(in crate::schema_v2) fn remember_v2_logical_name(
        &mut self,
        emitter: &str,
        token_id: &str,
        logical_name_id: &str,
    ) {
        if let Some(token) = self.v2_tokens.get_mut(&v2_key(emitter, token_id)) {
            token.last_logical_name_id = Some(logical_name_id.to_owned());
        }
    }

    #[rustfmt::skip]
    pub(in crate::schema_v2) fn mark_v2_expiry_retirement(&mut self, emitter: &str, token_id: &str, emitted: bool) {
        if let Some(token) = self.v2_tokens.get_mut(&v2_key(emitter, token_id)) { token.expiry_retirement_emitted = emitted; }
    }

    pub(in crate::schema_v2) fn refresh_dirty_v2_names(
        &mut self,
        at_unix_timestamp: i64,
    ) -> Vec<V2NameTransition> {
        let previous_timestamp = self.latest_v2_timestamp;
        let at_unix_timestamp = self.advance_v2_timestamp(at_unix_timestamp);
        let crossed = self.capture_crossed_v2_expiries(previous_timestamp, at_unix_timestamp);
        self.expand_dirty_v2_registries();
        let keys = std::mem::take(&mut self.v2_dirty_tokens)
            .into_iter()
            .collect();
        self.refresh_v2_name_keys(keys, at_unix_timestamp, &crossed)
    }

    pub(super) fn refresh_all_v2_names(&mut self, at_unix_timestamp: i64) -> Vec<V2NameTransition> {
        let at_unix_timestamp = self.advance_v2_timestamp(at_unix_timestamp);
        self.v2_dirty_tokens.clear();
        self.v2_dirty_registries.clear();
        let keys = self.v2_tokens.keys().cloned().collect::<Vec<_>>();
        self.refresh_v2_name_keys(keys, at_unix_timestamp, &imbl::ordset::OrdSet::new())
    }

    pub(super) fn mark_v2_token_dirty(&mut self, token_key: impl Into<String>) {
        self.v2_dirty_tokens.insert(token_key.into());
    }

    pub(super) fn mark_v2_token_component_dirty(&mut self, token_key: &str) {
        self.v2_dirty_tokens.insert(token_key.to_owned());
        if let Some(subregistry) = self
            .v2_tokens
            .get(token_key)
            .and_then(|token| token.subregistry.clone())
        {
            self.v2_dirty_registries
                .insert(subregistry.to_ascii_lowercase());
        }
    }

    pub(super) fn mark_v2_registry_dirty(&mut self, registry: &str) {
        self.v2_dirty_registries
            .insert(registry.to_ascii_lowercase());
    }

    pub(in crate::schema_v2) fn record_v2_terminal_closure_hit(
        &mut self,
        logical_name_id: &str,
        authority_arm: &str,
    ) {
        self.v2_terminal_closure_hits.insert((
            logical_name_id.to_ascii_lowercase(),
            authority_arm.to_owned(),
        ));
    }

    fn advance_v2_timestamp(&mut self, at_unix_timestamp: i64) -> i64 {
        let effective = self
            .latest_v2_timestamp
            .map_or(at_unix_timestamp, |previous| {
                previous.max(at_unix_timestamp)
            });
        self.latest_v2_timestamp = Some(effective);
        effective
    }

    fn capture_crossed_v2_expiries(
        &mut self,
        previous_timestamp: Option<i64>,
        current_timestamp: i64,
    ) -> imbl::ordset::OrdSet<String> {
        let previous_timestamp = previous_timestamp.unwrap_or(-1);
        if current_timestamp <= previous_timestamp || current_timestamp < 0 {
            return imbl::ordset::OrdSet::new();
        }
        let first_expiry = u64::try_from(previous_timestamp.saturating_add(1)).unwrap_or_default();
        let last_expiry = u64::try_from(current_timestamp).expect("non-negative timestamp");
        let crossed = self
            .v2_expiries
            .range((first_expiry, String::new())..)
            .take_while(|(expiry, _)| *expiry <= last_expiry)
            .map(|(_, token_key)| token_key.clone())
            .collect::<imbl::ordset::OrdSet<String>>();
        for token_key in &crossed {
            self.mark_v2_token_component_dirty(token_key);
        }
        crossed
    }

    fn expand_dirty_v2_registries(&mut self) {
        let mut pending = std::mem::take(&mut self.v2_dirty_registries)
            .into_iter()
            .collect::<Vec<_>>();
        let mut visited = imbl::ordset::OrdSet::new();
        while let Some(registry) = pending.pop() {
            if visited.insert(registry.clone()).is_some() {
                continue;
            }
            let prefix = format!("{registry}:");
            let keys = self
                .v2_tokens
                .range(prefix.clone()..)
                .take_while(|(key, _)| key.starts_with(&prefix))
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            for key in keys {
                self.v2_dirty_tokens.insert(key.clone());
                let Some(token) = self.v2_tokens.get(&key) else {
                    continue;
                };
                let (Some(raw_label), Some(subregistry)) =
                    (token.raw_label.as_ref(), token.subregistry.as_ref())
                else {
                    continue;
                };
                if self.v2_parent_claims.get(subregistry)
                    == Some(&(registry.clone(), raw_label.clone()))
                {
                    pending.push(subregistry.clone());
                }
            }
        }
    }

    pub(in crate::schema_v2) fn refresh_v2_name_keys(
        &mut self,
        keys: Vec<String>,
        at_unix_timestamp: i64,
        resource_retirements: &imbl::ordset::OrdSet<String>,
    ) -> Vec<V2NameTransition> {
        let mut transitions = Vec::new();
        let mut terminal_closure_hits = std::mem::take(&mut self.v2_terminal_closure_hits);
        let mut keys = keys.into_iter().collect::<imbl::ordset::OrdSet<String>>();
        for (logical_name_id, authority_arm) in &terminal_closure_hits {
            if authority_arm == "ens_v2"
                && let Some(token_keys) = self.v2_tokens_by_current_name_index.get(logical_name_id)
            {
                keys.extend(token_keys.iter().cloned());
            }
        }
        #[cfg(test)]
        V2_REFRESH_VISITS.set(V2_REFRESH_VISITS.get() + keys.len());
        for key in keys {
            let Some((emitter, token_id)) = key.rsplit_once(':') else {
                continue;
            };
            let Some(token) = self.v2_tokens.get(&key).cloned() else {
                continue;
            };
            let (Some(namespace), Some(raw_label)) =
                (token.namespace.as_ref(), token.raw_label.as_ref())
            else {
                continue;
            };
            let raw_name = super::topology::v2_expiry_is_live(token.expiry, at_unix_timestamp)
                .then(|| {
                    self.v2_registry_raw_suffix(emitter, namespace, at_unix_timestamp)
                        .map(|mut suffix| {
                            suffix.insert(0, raw_label.clone());
                            suffix
                        })
                })
                .flatten();
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
            let current_logical_name_id = name
                .as_ref()
                .map(|name| name.logical_name_id.clone())
                .or_else(|| {
                    shadow_name
                        .as_ref()
                        .map(|name| name.logical_name_id.clone())
                });
            let previous = token.name.clone();
            let previous_shadow = token.shadow_name.clone();
            let current_resource = token
                .registration
                .is_some()
                .then_some(token.resource_id)
                .flatten();
            let changed = previous != name || previous_shadow != shadow_name;
            let expiry_retirement = name.is_none()
                && shadow_name.is_none()
                && (resource_retirements.contains(&key)
                    || (!resource_retirements.is_empty()
                        && (previous.is_some() || previous_shadow.is_some())));
            let resource_retirement = resource_retirements.contains(&key)
                && token.resource_id.is_some()
                && !token.expiry_retirement_emitted
                && previous.is_none()
                && previous_shadow.is_none();
            if changed || resource_retirement {
                if token.registration.is_some()
                    && let Some(previous) = previous.as_ref()
                    && name
                        .as_ref()
                        .is_none_or(|current| current.logical_name_id != previous.logical_name_id)
                {
                    self.record_v2_terminal_closure_hit(&previous.logical_name_id, "ens_v2");
                }
                let previous_surface = previous.as_ref().map(|name| name.logical_name_id.clone());
                let current_surface = name.as_ref().map(|name| name.logical_name_id.clone());
                if let Some(previous) = previous.as_ref()
                    && token.registration.is_some()
                    && token.resource_id.is_some_and(|resource_id| {
                        self.active_resources.get(&previous.logical_name_id) == Some(&resource_id)
                    })
                {
                    self.active_resources.remove(&previous.logical_name_id);
                }
                transitions.push(V2NameTransition {
                    registry: emitter.to_owned(),
                    registry_contract_instance_id: token.registry_contract_instance_id,
                    token_id: token_id.to_owned(),
                    expiry: token.expiry,
                    previous: previous.clone(),
                    previous_shadow: previous_shadow.clone(),
                    current: name.clone(),
                    current_shadow: shadow_name.clone(),
                    resource_id: token.resource_id,
                    token_lineage_id: token.token_lineage_id,
                    upstream_resource: token.upstream_resource.clone(),
                    registration: token.registration.clone(),
                    resolver: token.resolver.clone(),
                    subregistry: token.subregistry.clone(),
                });
                self.replace_v2_current_surface(
                    previous_surface.as_deref(),
                    current_surface.as_deref(),
                );
            }
            if changed {
                let mut current = token.clone();
                current.name = name.clone();
                current.shadow_name = shadow_name.clone();
                if let Some(logical_name_id) = current_logical_name_id.as_ref() {
                    current.last_logical_name_id = Some(logical_name_id.clone());
                }
                self.replace_v2_token_indexes(&key, Some(&token), Some(&current));
            }
            if changed
                && let Some(previous) = previous.as_ref()
                && let Some(resource_id) = self.v2_active_resource_winner(&previous.logical_name_id)
            {
                self.active_resources
                    .insert(previous.logical_name_id.clone(), resource_id);
            }
            if let Some(current) = name.as_ref()
                && current_resource.is_some()
                && let Some(resource_id) = self.v2_active_resource_winner(&current.logical_name_id)
            {
                self.active_resources
                    .insert(current.logical_name_id.clone(), resource_id);
            }
            if let Some(current) = self.v2_tokens.get_mut(&key) {
                current.name = name;
                current.shadow_name = shadow_name;
                if expiry_retirement || current_logical_name_id.is_some() {
                    current.expiry_retirement_emitted = expiry_retirement;
                }
                if let Some(logical_name_id) = current_logical_name_id {
                    current.last_logical_name_id = Some(logical_name_id);
                }
            }
        }
        terminal_closure_hits.extend(std::mem::take(&mut self.v2_terminal_closure_hits));
        for (logical_name_id, authority_arm) in terminal_closure_hits {
            if authority_arm == "ens_v2"
                && let Some(transition) = self.v2_terminal_reassertion(&logical_name_id)
            {
                transitions.push(transition);
            }
        }
        transitions
    }

    fn v2_terminal_reassertion(&self, logical_name_id: &str) -> Option<V2NameTransition> {
        let winner_key = self.v2_active_resource_winner_key(logical_name_id)?;
        let (registry, token_id) = winner_key.rsplit_once(':')?;
        let token = self.v2_tokens.get(&winner_key)?;
        let current = token
            .name
            .as_ref()
            .filter(|name| name.logical_name_id.eq_ignore_ascii_case(logical_name_id))?
            .clone();
        Some(V2NameTransition {
            registry: registry.to_owned(),
            registry_contract_instance_id: token.registry_contract_instance_id,
            token_id: token_id.to_owned(),
            expiry: token.expiry,
            previous: Some(current.clone()),
            previous_shadow: token.shadow_name.clone(),
            current: Some(current),
            current_shadow: token.shadow_name.clone(),
            resource_id: token.resource_id,
            token_lineage_id: token.token_lineage_id,
            upstream_resource: token.upstream_resource.clone(),
            registration: token.registration.clone(),
            resolver: token.resolver.clone(),
            subregistry: token.subregistry.clone(),
        })
    }

    /// The active resource for a surface is the resource of the greatest token key among retained
    /// holders that carry a registration and a linked resource — the winner an unconditional
    /// re-assert produces on a full ascending walk — so a refresh elects the same resource for
    /// any dirty set that closes over the surface's contention.
    fn v2_active_resource_winner(&self, logical_name_id: &str) -> Option<uuid::Uuid> {
        self.v2_active_resource_winner_key(logical_name_id)
            .and_then(|token_key| self.v2_tokens.get(&token_key))
            .and_then(|token| token.resource_id)
    }

    fn v2_active_resource_winner_key(&self, logical_name_id: &str) -> Option<String> {
        self.v2_tokens_by_current_name_index
            .get(logical_name_id)?
            .iter()
            .rev()
            .find(|token_key| {
                self.v2_tokens.get(*token_key).is_some_and(|token| {
                    token.registration.is_some() && token.resource_id.is_some()
                })
            })
            .cloned()
    }
}
