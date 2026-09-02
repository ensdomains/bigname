use super::{State, V1NameState, V1RegistryReadAnchor, v1_key};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::schema_v2) enum V1SurfaceMaterialization {
    RegistryAuthority {
        previous: V1NameState,
        promoted: V1NameState,
        resolver: Option<String>,
        source_manifest_id: i64,
    },
    OwnerlessRegistryRead {
        anchor: V1RegistryReadAnchor,
        resolver: Option<String>,
        source_manifest_id: i64,
    },
    AlreadyMaterialized,
}

impl State {
    pub(in crate::schema_v2) fn mark_v1_migrated(&mut self, namespace: &str, namehash: &str) {
        let key = v1_key(namespace, namehash);
        if self.v1_migrated_nodes.insert(key.clone()).is_none() {
            self.v1_resolver_links.remove(&key);
            self.v1_resolvers.remove(&key);
        }
    }

    pub(in crate::schema_v2) fn sync_registry_surface_from_registrar(
        &mut self,
        namespace: &str,
        namehash: &str,
        logical_name_id: &str,
        surface_known: bool,
        labelhash: Option<&str>,
    ) {
        let key = v1_key(namespace, namehash);
        if let Some(registry) = self.v1_registry_authorities.get_mut(&key) {
            registry.logical_name_id = logical_name_id.to_owned();
            registry.surface_known = surface_known;
            registry.labelhash = labelhash.map(str::to_owned);
        }
        if let Some(anchor) = self.v1_registry_read_anchors.get_mut(&key) {
            anchor.logical_name_id = logical_name_id.to_owned();
            anchor.surface_known |= surface_known;
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

    pub(in crate::schema_v2) fn materialize_v1_active_surface(
        &mut self,
        namespace: &str,
        namehash: &str,
        logical_name_id: &str,
        labelhash: &str,
    ) -> anyhow::Result<V1SurfaceMaterialization> {
        let key = v1_key(namespace, namehash);
        self.v1_materialized_surfaces.insert(key.clone());
        self.remember_known_surface(logical_name_id.to_owned());

        if let Some(previous) = self.v1_names.get(&key).cloned()
            && previous.token_lineage_id.is_none()
            && !previous.surface_known
        {
            let source_manifest_id = previous.source_manifest_id.ok_or_else(|| {
                anyhow::anyhow!(
                    "registry authority for {namespace}:{namehash} has no source manifest"
                )
            })?;
            let mut promoted = previous.clone();
            promoted.logical_name_id = logical_name_id.to_owned();
            promoted.labelhash = Some(labelhash.to_owned());
            promoted.surface_known = true;
            self.v1_names.insert(key.clone(), promoted.clone());
            self.v1_registry_authorities
                .insert(key.clone(), promoted.clone());
            self.active_resources
                .insert(logical_name_id.to_owned(), promoted.resource_id);
            if let Some(anchor) = self.v1_registry_read_anchors.get_mut(&key) {
                anchor.logical_name_id = logical_name_id.to_owned();
                anchor.surface_known = true;
            }
            let resolver = self
                .v1_resolver_links
                .get(&key)
                .map(|link| link.resolver_address.clone());
            if let Some(link) = self.v1_resolver_links.get_mut(&key) {
                link.resource_id = Some(promoted.resource_id);
                link.logical_name_id = Some(logical_name_id.to_owned());
            }
            return Ok(V1SurfaceMaterialization::RegistryAuthority {
                previous,
                promoted,
                resolver,
                source_manifest_id,
            });
        }

        let explicitly_ownerless = self.v1_registry_owners.get(&key).is_some_and(|owner| {
            owner.eq_ignore_ascii_case("0x0000000000000000000000000000000000000000")
        });
        if explicitly_ownerless
            && let Some(mut anchor) = self.v1_registry_read_anchors.get(&key).cloned()
            && !anchor.surface_known
        {
            let source_manifest_id = anchor.source_manifest_id.ok_or_else(|| {
                anyhow::anyhow!(
                    "registry read anchor for {namespace}:{namehash} has no source manifest"
                )
            })?;
            anchor.logical_name_id = logical_name_id.to_owned();
            anchor.surface_known = true;
            self.v1_registry_read_anchors
                .insert(key.clone(), anchor.clone());
            let resolver = self
                .v1_resolver_links
                .get(&key)
                .map(|link| link.resolver_address.clone());
            if let Some(link) = self.v1_resolver_links.get_mut(&key) {
                link.resource_id = Some(anchor.resource_id);
                link.logical_name_id = Some(logical_name_id.to_owned());
            }
            return Ok(V1SurfaceMaterialization::OwnerlessRegistryRead {
                anchor,
                resolver,
                source_manifest_id,
            });
        }

        Ok(V1SurfaceMaterialization::AlreadyMaterialized)
    }

    pub(in crate::schema_v2) fn v1_explicit_ownerless_registry_evidence(
        &self,
        namespace: &str,
        namehash: &str,
    ) -> bool {
        let key = v1_key(namespace, namehash);
        self.v1_registry_read_anchors.contains_key(&key)
            && self.v1_registry_owners.get(&key).is_some_and(|owner| {
                owner.eq_ignore_ascii_case("0x0000000000000000000000000000000000000000")
            })
    }

    pub(in crate::schema_v2) fn activate_retained_v1_registry_authority(
        &mut self,
        namespace: &str,
        namehash: &str,
    ) {
        let authority = self
            .v1_registry_authorities
            .get(&v1_key(namespace, namehash))
            .cloned();
        self.activate_v1_authority(namespace, namehash, authority);
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
