use std::collections::BTreeSet;

use serde_json::Value;

use super::{State, v2::V2TokenState, v2::v2_key};

#[cfg(test)]
std::thread_local! {
    static V2_SUBREGISTRY_LOOKUP_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(in crate::schema_v2) fn reset_v2_subregistry_lookup_visits() {
    V2_SUBREGISTRY_LOOKUP_VISITS.set(0);
}

#[cfg(test)]
pub(in crate::schema_v2) fn v2_subregistry_lookup_visits() -> usize {
    V2_SUBREGISTRY_LOOKUP_VISITS.get()
}

impl State {
    pub(in crate::schema_v2) fn restore_v2_regeneration(
        &mut self,
        emitter: &str,
        old_token_id: &str,
        new_token_id: &str,
        after_state: &Value,
    ) {
        if self
            .regenerate_v2_token(emitter, old_token_id, new_token_id)
            .is_none()
        {
            return;
        }
        let Some(aliases) = after_state
            .get("resolver_discovery_aliases")
            .and_then(Value::as_array)
        else {
            return;
        };
        let aliases = aliases
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect::<BTreeSet<_>>();
        let token_key = v2_key(emitter, new_token_id);
        let previous = self
            .v2_tokens
            .get(&token_key)
            .map(|token| token.resolver_discovery_aliases.clone())
            .unwrap_or_default();
        self.set_v2_resolver_alias_index(emitter, new_token_id, &previous, false);
        if let Some(token) = self.v2_tokens.get_mut(&token_key) {
            token.resolver_discovery_aliases = aliases.clone();
        }
        self.set_v2_resolver_alias_index(emitter, new_token_id, &aliases, true);
    }

    pub(super) fn remove_v2_displaced_restored_token(
        &mut self,
        emitter: &str,
        displaced_key: &str,
    ) {
        let Some(displaced) = self.v2_tokens.remove(displaced_key) else {
            return;
        };
        if let Some((_, displaced_token_id)) = displaced_key.rsplit_once(':') {
            if displaced.resolver.is_some() {
                self.set_v2_resolver_token_index(emitter, displaced_token_id, false);
            }
            self.set_v2_token_resolver_alias_index(emitter, displaced_token_id, &displaced, false);
        }
        self.replace_v2_token_indexes(displaced_key, Some(&displaced), None);
        self.replace_v2_expiry_index(displaced_key, displaced.expiry, None);
        self.remove_v2_current_name(&displaced);
        if let Some(subregistry) = displaced.subregistry.as_deref() {
            self.mark_v2_registry_dirty(subregistry);
        }
    }

    pub(in crate::schema_v2) fn set_v2_subregistry(
        &mut self,
        emitter: &str,
        token_id: &str,
        subregistry: Option<String>,
    ) {
        let key = v2_key(emitter, token_id);
        let previous_state = self.v2_tokens.get(&key).cloned();
        let previous = previous_state
            .as_ref()
            .and_then(|token| token.subregistry.clone());
        let current = {
            let token = self.v2_tokens.entry(key.clone()).or_default();
            token.subregistry = subregistry.clone();
            token.clone()
        };
        self.replace_v2_token_indexes(&key, previous_state.as_ref(), Some(&current));
        self.mark_v2_token_dirty(key);
        for registry in previous.into_iter().chain(subregistry) {
            self.mark_v2_registry_dirty(&registry);
        }
    }

    pub(in crate::schema_v2) fn live_v2_resolver_tokens_sharing(
        &self,
        emitter: &str,
        candidates: &BTreeSet<String>,
    ) -> BTreeSet<String> {
        let emitter = emitter.to_ascii_lowercase();
        let mut sharing = BTreeSet::new();
        for candidate in candidates {
            let key = (emitter.clone(), resolver_observation_id(candidate));
            if let Some(tokens) = self.v2_resolver_tokens_by_observation.get(&key) {
                sharing.extend(tokens.iter().cloned());
            }
            if let Some(aliases) = self.v2_resolver_aliases_by_observation.get(&key) {
                sharing.extend(aliases.iter().map(|(_, alias)| alias.clone()));
            }
        }
        sharing
    }

    pub(in crate::schema_v2) fn v2_subregistry_reassertion_target(
        &self,
        emitter: &str,
        token_id: &str,
    ) -> Option<String> {
        let emitter = emitter.to_ascii_lowercase();
        let tokens = self
            .v2_subregistry_tokens_by_observation
            .get(&(emitter.clone(), resolver_observation_id(token_id)))?;
        record_subregistry_lookup_visits(tokens.len());
        // Fixed-width lowercase token IDs have a stable OrdSet order. Choosing the greatest
        // retained token makes shared-key reassertion deterministic across replay and restore.
        let survivor = tokens.get_max()?;
        self.v2_tokens
            .get(&v2_key(&emitter, survivor))
            .and_then(|token| token.subregistry.clone())
    }

    pub(in crate::schema_v2) fn set_v2_resolver(
        &mut self,
        emitter: &str,
        token_id: &str,
        resolver: Option<String>,
    ) -> BTreeSet<String> {
        let key = v2_key(emitter, token_id);
        let (was_active, is_active, aliases) = {
            let token = self.v2_tokens.entry(key.clone()).or_default();
            let was_active = token.resolver.is_some();
            token.resolver = resolver;
            (
                was_active,
                token.resolver.is_some(),
                std::mem::take(&mut token.resolver_discovery_aliases),
            )
        };
        if was_active != is_active {
            self.set_v2_resolver_token_index(emitter, token_id, is_active);
        }
        self.set_v2_resolver_alias_index(emitter, token_id, &aliases, false);
        self.mark_v2_token_dirty(key);
        aliases
    }

    pub(super) fn set_v2_resolver_token_index(
        &mut self,
        emitter: &str,
        token_id: &str,
        active: bool,
    ) {
        let key = (
            emitter.to_ascii_lowercase(),
            resolver_observation_id(token_id),
        );
        if active {
            self.v2_resolver_tokens_by_observation
                .entry(key)
                .or_default()
                .insert(token_id.to_owned());
        } else {
            let remove_key = self
                .v2_resolver_tokens_by_observation
                .get_mut(&key)
                .is_some_and(|tokens| {
                    tokens.remove(token_id);
                    tokens.is_empty()
                });
            if remove_key {
                self.v2_resolver_tokens_by_observation.remove(&key);
            }
        }
    }

    pub(super) fn set_v2_resolver_alias_index(
        &mut self,
        emitter: &str,
        holder_token_id: &str,
        aliases: &BTreeSet<String>,
        active: bool,
    ) {
        let emitter = emitter.to_ascii_lowercase();
        for alias in aliases {
            let key = (emitter.clone(), resolver_observation_id(alias));
            let entry = (holder_token_id.to_owned(), alias.clone());
            if active {
                self.v2_resolver_aliases_by_observation
                    .entry(key)
                    .or_default()
                    .insert(entry);
            } else {
                let remove_key = self
                    .v2_resolver_aliases_by_observation
                    .get_mut(&key)
                    .is_some_and(|holders| {
                        holders.remove(&entry);
                        holders.is_empty()
                    });
                if remove_key {
                    self.v2_resolver_aliases_by_observation.remove(&key);
                }
            }
        }
    }

    pub(super) fn set_v2_token_resolver_alias_index(
        &mut self,
        emitter: &str,
        holder_token_id: &str,
        token: &V2TokenState,
        active: bool,
    ) {
        if active && token.resolver.is_none() {
            return;
        }
        self.set_v2_resolver_alias_index(
            emitter,
            holder_token_id,
            &token.resolver_discovery_aliases,
            active,
        );
    }
}

pub(super) fn same_resolver_observation(left: &str, right: &str) -> bool {
    resolver_observation_id(left) == resolver_observation_id(right)
}

pub(super) fn resolver_observation_id(token_id: &str) -> String {
    token_id
        .get(..token_id.len().saturating_sub(8))
        .map(|prefix| format!("{prefix}00000000"))
        .unwrap_or_else(|| token_id.to_owned())
        .to_ascii_lowercase()
}

fn record_subregistry_lookup_visits(visits: usize) {
    #[cfg(test)]
    V2_SUBREGISTRY_LOOKUP_VISITS.set(V2_SUBREGISTRY_LOOKUP_VISITS.get() + visits);
    #[cfg(not(test))]
    let _ = visits;
}
