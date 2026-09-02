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
