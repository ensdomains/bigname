use super::{State, V2NameState, V2NameTransition, V2RawNameState};
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
    pub(in crate::schema_v2) fn refresh_v2_names(
        &mut self,
        at_unix_timestamp: i64,
    ) -> Vec<V2NameTransition> {
        let keys = self.v2_tokens.keys().cloned().collect::<Vec<_>>();
        self.refresh_v2_name_keys(keys, at_unix_timestamp)
    }

    pub(in crate::schema_v2) fn refresh_v2_name_keys(
        &mut self,
        keys: Vec<String>,
        at_unix_timestamp: i64,
    ) -> Vec<V2NameTransition> {
        let mut transitions = Vec::new();
        #[cfg(test)]
        V2_REFRESH_VISITS.set(V2_REFRESH_VISITS.get() + keys.len());
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
                let previous_surface = previous.as_ref().map(|name| name.logical_name_id.clone());
                let current_surface = name.as_ref().map(|name| name.logical_name_id.clone());
                if let Some(previous) = previous.as_ref()
                    && token.resource_id.is_some_and(|resource_id| {
                        self.active_resources.get(&previous.logical_name_id) == Some(&resource_id)
                    })
                {
                    self.active_resources.remove(&previous.logical_name_id);
                }
                if let Some(current) = name.as_ref()
                    && let Some(resource_id) = token.resource_id
                {
                    self.active_resources
                        .insert(current.logical_name_id.clone(), resource_id);
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
                self.replace_v2_current_surface(
                    previous_surface.as_deref(),
                    current_surface.as_deref(),
                );
            }
            if let Some(token) = self.v2_tokens.get_mut(&key) {
                token.name = name;
                token.shadow_name = shadow_name;
            }
        }
        transitions
    }
}
