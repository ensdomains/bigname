use super::{State, V1NameState, v1_key};

impl State {
    pub(super) fn promote_known_v1_authority(
        &mut self,
        key: &str,
        authority: &mut V1NameState,
    ) -> bool {
        if self.known_surfaces.contains(&authority.logical_name_id) {
            authority.surface_known = true;
            if let Some(registrar) = self.v1_registrars.get_mut(key)
                && registrar.resource_id == authority.resource_id
            {
                registrar.surface_known = true;
            }
            if let Some(registry) = self.v1_registry_authorities.get_mut(key)
                && registry.resource_id == authority.resource_id
            {
                registry.surface_known = true;
            }
        }
        authority.surface_known
    }

    pub(in crate::schema_v2) fn materialize_v1_surface(
        &mut self,
        namespace: &str,
        namehash: &str,
        active: bool,
        bind: bool,
    ) {
        if active && bind {
            self.bind_v1_active_surface(namespace, namehash);
        } else if active {
            self.observe_v1_active_surface(namespace, namehash);
        } else {
            self.observe_v1_surface(namespace, namehash);
        }
    }

    pub(in crate::schema_v2) fn observe_v1_surface(&mut self, namespace: &str, namehash: &str) {
        self.v1_materialized_surfaces
            .insert(v1_key(namespace, namehash));
    }

    pub(in crate::schema_v2) fn observe_v1_active_surface(
        &mut self,
        namespace: &str,
        namehash: &str,
    ) {
        let key = v1_key(namespace, namehash);
        let logical_name_id = format!("{namespace}:{namehash}");
        self.v1_materialized_surfaces.insert(key.clone());
        self.remember_known_surface(logical_name_id.clone());
        if let Some(anchor) = self.v1_registry_read_anchors.get_mut(&key) {
            anchor.logical_name_id = logical_name_id;
            anchor.surface_known = true;
        }
    }

    pub(in crate::schema_v2) fn bind_v1_active_surface(&mut self, namespace: &str, namehash: &str) {
        self.observe_v1_active_surface(namespace, namehash);
        let key = v1_key(namespace, namehash);
        let logical_name_id = format!("{namespace}:{namehash}");
        let Some(resource_id) = self.v1_names.get(&key).map(|state| state.resource_id) else {
            return;
        };
        self.v1_names
            .get_mut(&key)
            .expect("current V1 authority")
            .surface_known = true;
        self.active_resources
            .insert(logical_name_id.clone(), resource_id);
        if let Some(registrar) = self.v1_registrars.get_mut(&key)
            && registrar.resource_id == resource_id
        {
            registrar.surface_known = true;
        }
        if let Some(authority) = self.v1_registry_authorities.get_mut(&key)
            && authority.resource_id == resource_id
        {
            authority.surface_known = true;
        }
    }

    pub(in crate::schema_v2) fn v1_active_surface_materialized(
        &self,
        namespace: &str,
        namehash: &str,
    ) -> bool {
        self.known_surfaces
            .contains(&format!("{namespace}:{namehash}"))
    }

    pub(in crate::schema_v2) fn v1_surface_materialized(
        &self,
        namespace: &str,
        namehash: &str,
    ) -> bool {
        self.v1_materialized_surfaces
            .contains(&v1_key(namespace, namehash))
    }
}
