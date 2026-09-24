use std::sync::Arc;

use super::{State, V1NameState, v1_key};

impl State {
    // Current authority can select the same registrar or registry fallback snapshot.
    // Retain one payload while keeping the maps and later mutations independent.
    pub(super) fn share_v1_authority(&self, key: &str, value: V1NameState) -> Arc<V1NameState> {
        for retained in [
            self.v1_names.get(key),
            self.v1_registrars.get(key),
            self.v1_registry_authorities.get(key),
        ]
        .into_iter()
        .flatten()
        {
            if retained.as_ref() == &value {
                return retained.clone();
            }
        }
        Arc::new(value)
    }

    pub(super) fn promote_known_v1_authority(
        &mut self,
        key: &str,
        authority: &mut V1NameState,
    ) -> bool {
        if self.known_surfaces.contains(&authority.logical_name_id) {
            authority.surface_known = true;
            if let Some(registrar) = self.v1_registrars.get_mut(key)
                && registrar.resource_id == authority.resource_id
                && !registrar.surface_known
            {
                Arc::make_mut(registrar).surface_known = true;
            }
            if let Some(registry) = self.v1_registry_authorities.get_mut(key)
                && registry.resource_id == authority.resource_id
                && !registry.surface_known
            {
                Arc::make_mut(registry).surface_known = true;
            }
        }
        authority.surface_known
    }

    pub(in crate::schema_v2) fn bind_v1_active_surface(&mut self, namespace: &str, namehash: &str) {
        self.observe_v1_active_surface(namespace, namehash);
        let key = v1_key(namespace, namehash);
        let logical_name_id = format!("{namespace}:{namehash}");
        let Some(resource_id) = self.v1_names.get(&key).map(|state| state.resource_id) else {
            return;
        };
        let current = self.v1_names.get_mut(&key).expect("current V1 authority");
        if !current.surface_known {
            Arc::make_mut(current).surface_known = true;
        }
        self.active_resources
            .insert(logical_name_id.clone(), resource_id);
        if let Some(registrar) = self.v1_registrars.get_mut(&key)
            && registrar.resource_id == resource_id
            && !registrar.surface_known
        {
            Arc::make_mut(registrar).surface_known = true;
        }
        if let Some(authority) = self.v1_registry_authorities.get_mut(&key)
            && authority.resource_id == resource_id
            && !authority.surface_known
        {
            Arc::make_mut(authority).surface_known = true;
        }
    }
}

#[cfg(test)]
#[path = "state_sharing_tests.rs"]
mod tests;
