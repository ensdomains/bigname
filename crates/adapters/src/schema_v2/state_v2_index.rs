use imbl::{ordmap::OrdMap, ordset::OrdSet};

use super::{State, V2TokenState, v2_pointers::resolver_observation_id};

#[cfg(test)]
std::thread_local! {
    static V2_LOOKUP_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(in crate::schema_v2) fn reset_v2_lookup_visits() {
    V2_LOOKUP_VISITS.set(0);
}

#[cfg(test)]
pub(in crate::schema_v2) fn v2_lookup_visits() -> usize {
    V2_LOOKUP_VISITS.get()
}

impl State {
    pub(in crate::schema_v2) fn rebuild_v2_token_indexes(&mut self) {
        self.v2_token_by_upstream_resource_index.clear();
        self.v2_token_by_name_index.clear();
        self.v2_tokens_by_current_name_index.clear();
        self.v2_subregistry_tokens_by_observation.clear();
        let tokens = self
            .v2_tokens
            .iter()
            .map(|(key, token)| (key.clone(), token.clone()))
            .collect::<Vec<_>>();
        for (key, token) in tokens {
            self.replace_v2_token_indexes(&key, None, Some(&token));
        }
    }

    pub(in crate::schema_v2) fn v2_token_by_upstream_resource(
        &self,
        emitter: &str,
        upstream_resource: &str,
    ) -> anyhow::Result<Option<V2TokenState>> {
        let identity = (emitter.to_ascii_lowercase(), upstream_resource.to_owned());
        let Some(token_keys) = self.v2_token_by_upstream_resource_index.get(&identity) else {
            return Ok(None);
        };
        record_lookup_visits(token_keys.len());
        if token_keys.len() > 1 {
            anyhow::bail!(
                "ENSv2 upstream resource {upstream_resource} maps to more than one retained token"
            );
        }
        Ok(token_keys
            .get_min()
            .and_then(|token_key| self.v2_tokens.get(token_key))
            .cloned())
    }

    pub(in crate::schema_v2) fn v2_token_for_logical_name(
        &self,
        token_id: &str,
        logical_name_id: &str,
    ) -> anyhow::Result<Option<V2TokenState>> {
        let identity = (
            token_id.to_ascii_lowercase(),
            logical_name_id.to_ascii_lowercase(),
        );
        let Some(token_keys) = self.v2_token_by_name_index.get(&identity) else {
            return Ok(None);
        };
        record_lookup_visits(token_keys.len());
        if token_keys.len() > 1 {
            anyhow::bail!(
                "ENSv2 registrar token {token_id} and name {logical_name_id} map to more than one retained registry resource"
            );
        }
        Ok(token_keys
            .get_min()
            .and_then(|token_key| self.v2_tokens.get(token_key))
            .cloned())
    }

    pub(super) fn replace_v2_token_indexes(
        &mut self,
        token_key: &str,
        previous: Option<&V2TokenState>,
        current: Option<&V2TokenState>,
    ) {
        if previous == current {
            return;
        }
        if let Some(previous) = previous {
            replace_subregistry_index(
                &mut self.v2_subregistry_tokens_by_observation,
                token_key,
                previous,
                false,
            );
            remove_token_indexes(
                &mut self.v2_token_by_upstream_resource_index,
                &mut self.v2_token_by_name_index,
                &mut self.v2_tokens_by_current_name_index,
                token_key,
                previous,
            );
        }
        if let Some(current) = current {
            replace_subregistry_index(
                &mut self.v2_subregistry_tokens_by_observation,
                token_key,
                current,
                true,
            );
            insert_token_indexes(
                &mut self.v2_token_by_upstream_resource_index,
                &mut self.v2_token_by_name_index,
                &mut self.v2_tokens_by_current_name_index,
                token_key,
                current,
            );
        }
    }
}

fn replace_subregistry_index(
    index: &mut OrdMap<(String, String), OrdSet<String>>,
    token_key: &str,
    token: &V2TokenState,
    active: bool,
) {
    if token.subregistry.is_none() {
        return;
    }
    let Some((emitter, token_id)) = token_key.rsplit_once(':') else {
        return;
    };
    let identity = (emitter.to_owned(), resolver_observation_id(token_id));
    if active {
        index
            .entry(identity)
            .or_default()
            .insert(token_id.to_owned());
    } else {
        remove_index_key(index, &identity, token_id);
    }
}

fn insert_token_indexes(
    upstream_index: &mut OrdMap<(String, String), OrdSet<String>>,
    name_index: &mut OrdMap<(String, String), OrdSet<String>>,
    current_name_index: &mut OrdMap<String, OrdSet<String>>,
    token_key: &str,
    token: &V2TokenState,
) {
    let Some((emitter, token_id)) = token_key.rsplit_once(':') else {
        return;
    };
    if let Some(upstream_resource) = token.upstream_resource.as_ref() {
        upstream_index
            .entry((emitter.to_owned(), upstream_resource.clone()))
            .or_default()
            .insert(token_key.to_owned());
    }
    for logical_name_id in token_name_ids(token) {
        name_index
            .entry((token_id.to_owned(), logical_name_id))
            .or_default()
            .insert(token_key.to_owned());
    }
    if let Some(name) = token.name.as_ref() {
        current_name_index
            .entry(name.logical_name_id.clone())
            .or_default()
            .insert(token_key.to_owned());
    }
}

fn remove_token_indexes(
    upstream_index: &mut OrdMap<(String, String), OrdSet<String>>,
    name_index: &mut OrdMap<(String, String), OrdSet<String>>,
    current_name_index: &mut OrdMap<String, OrdSet<String>>,
    token_key: &str,
    token: &V2TokenState,
) {
    let Some((emitter, token_id)) = token_key.rsplit_once(':') else {
        return;
    };
    if let Some(upstream_resource) = token.upstream_resource.as_ref() {
        remove_index_key(
            upstream_index,
            &(emitter.to_owned(), upstream_resource.clone()),
            token_key,
        );
    }
    for logical_name_id in token_name_ids(token) {
        remove_index_key(
            name_index,
            &(token_id.to_owned(), logical_name_id),
            token_key,
        );
    }
    if let Some(name) = token.name.as_ref() {
        remove_index_key(current_name_index, &name.logical_name_id, token_key);
    }
}

pub(super) fn token_name_ids(token: &V2TokenState) -> OrdSet<String> {
    token
        .name
        .as_ref()
        .map(|name| name.logical_name_id.to_ascii_lowercase())
        .into_iter()
        .chain(
            token
                .shadow_name
                .as_ref()
                .map(|name| name.logical_name_id.to_ascii_lowercase()),
        )
        .collect()
}

fn remove_index_key<K: Ord + Clone>(
    index: &mut OrdMap<K, OrdSet<String>>,
    identity: &K,
    token_key: &str,
) {
    let remove_identity = if let Some(token_keys) = index.get_mut(identity) {
        token_keys.remove(token_key);
        token_keys.is_empty()
    } else {
        false
    };
    if remove_identity {
        index.remove(identity);
    }
}

fn record_lookup_visits(visits: usize) {
    #[cfg(test)]
    V2_LOOKUP_VISITS.set(V2_LOOKUP_VISITS.get() + visits);
    #[cfg(not(test))]
    let _ = visits;
}
