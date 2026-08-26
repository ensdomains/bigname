use std::collections::BTreeSet;

use super::{State, v2::v2_key};

impl State {
    pub(super) fn remove_v2_displaced_restored_token(
        &mut self,
        emitter: &str,
        displaced_key: &str,
    ) {
        let Some(displaced) = self.v2_tokens.remove(displaced_key) else {
            return;
        };
        if displaced.resolver.is_some()
            && let Some((_, displaced_token_id)) = displaced_key.rsplit_once(':')
        {
            self.set_v2_resolver_token_index(emitter, displaced_token_id, false);
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
        let previous = self
            .v2_tokens
            .get(&key)
            .and_then(|token| token.subregistry.clone());
        self.v2_tokens.entry(key.clone()).or_default().subregistry = subregistry.clone();
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
        candidates
            .iter()
            .filter_map(|token| {
                self.v2_resolver_tokens_by_observation
                    .get(&(emitter.clone(), resolver_observation_id(token)))
            })
            .flatten()
            .cloned()
            .collect()
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
