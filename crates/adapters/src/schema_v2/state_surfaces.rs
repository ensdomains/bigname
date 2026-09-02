use super::{State, v1_key};

impl State {
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
        let registrar_resource = self.v1_registrars.get_mut(&key).map(|registrar| {
            registrar.logical_name_id = logical_name_id.clone();
            registrar.surface_known = true;
            registrar.resource_id
        });
        if let Some(resource_id) = registrar_resource {
            if let Some(current) = self.v1_names.get_mut(&key)
                && current.resource_id == resource_id
            {
                current.logical_name_id = logical_name_id.clone();
                current.surface_known = true;
                self.active_resources
                    .insert(logical_name_id.clone(), resource_id);
            }
            if let Some(authority) = self.v1_registry_authorities.get_mut(&key)
                && authority.resource_id == resource_id
            {
                authority.logical_name_id = logical_name_id.clone();
                authority.surface_known = true;
            }
        }
        if let Some(anchor) = self.v1_registry_read_anchors.get_mut(&key) {
            anchor.logical_name_id = logical_name_id;
            anchor.surface_known = true;
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
