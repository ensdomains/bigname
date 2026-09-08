//! Late materialization of an ENSv1 surface: an authority observed before its
//! label was known is bound, and a resolver set in that window replayed, when
//! a label-bearing event names it.

use super::{State, V1NameState, V1RegistryReadAnchor, V1ResolverLink, v1_key};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::schema_v2) enum V1SurfaceMaterialization {
    RegistryAuthority {
        previous: Box<V1NameState>,
        promoted: Box<V1NameState>,
        resolver: Option<V1ResolverLink>,
        source_manifest_id: i64,
    },
    RegistryRead {
        anchor: V1RegistryReadAnchor,
        resolver: Option<V1ResolverLink>,
        source_manifest_id: i64,
    },
    /// A registrar authority observed without its label (the registrar fallback)
    /// that a label-bearing event now names: the surface is bound to it, and a
    /// resolver set before the label arrived is replayed onto that surface.
    RegistrarAuthority {
        authority: Box<V1NameState>,
        resolver: Option<V1ResolverLink>,
        source_manifest_id: i64,
    },
    AlreadyMaterialized,
}

impl State {
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
            self.require_source_manifest(namespace, namehash, source_manifest_id)?;
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
            let resolver = self.v1_resolver_links.get(&key).cloned();
            if let Some(link) = self.v1_resolver_links.get_mut(&key) {
                link.resource_id = Some(promoted.resource_id);
                link.logical_name_id = Some(logical_name_id.to_owned());
            }
            return Ok(V1SurfaceMaterialization::RegistryAuthority {
                previous: Box::new(previous),
                promoted: Box::new(promoted),
                resolver,
                source_manifest_id,
            });
        }

        let explicitly_ownerless = self.v1_registry_owners.get(&key).is_some_and(|owner| {
            owner.eq_ignore_ascii_case("0x0000000000000000000000000000000000000000")
        });
        let wrapper_is_current = self
            .v1_names
            .get(&key)
            .is_some_and(|authority| authority.authority_source_family == "ens_v1_wrapper_l1");
        if (explicitly_ownerless || wrapper_is_current)
            && let Some(mut anchor) = self.v1_registry_read_anchors.get(&key).cloned()
            && !anchor.surface_known
        {
            let source_manifest_id = anchor.source_manifest_id.ok_or_else(|| {
                anyhow::anyhow!(
                    "registry read anchor for {namespace}:{namehash} has no source manifest"
                )
            })?;
            self.require_source_manifest(namespace, namehash, source_manifest_id)?;
            anchor.logical_name_id = logical_name_id.to_owned();
            anchor.surface_known = true;
            self.v1_registry_read_anchors
                .insert(key.clone(), anchor.clone());
            self.sync_registry_surface_from_registrar(
                namespace,
                namehash,
                logical_name_id,
                true,
                Some(labelhash),
            );
            let resolver = self.v1_resolver_links.get(&key).cloned();
            if let Some(link) = self.v1_resolver_links.get_mut(&key) {
                link.resource_id = Some(anchor.resource_id);
                link.logical_name_id = Some(logical_name_id.to_owned());
            }
            return Ok(V1SurfaceMaterialization::RegistryRead {
                anchor,
                resolver,
                source_manifest_id,
            });
        }

        Ok(V1SurfaceMaterialization::AlreadyMaterialized)
    }

    /// The current registrar authority was observed without its label and this
    /// event carries it. Unlike a registry-only authority, nothing promotes the
    /// state here -- the observation that preceded this call already recorded
    /// the label -- but the surface it names has never been bound, and a
    /// resolver set while the label was unknown is linked only to the resource.
    pub(in crate::schema_v2) fn name_v1_registrar_surface(
        &mut self,
        namespace: &str,
        namehash: &str,
        logical_name_id: &str,
    ) -> anyhow::Result<V1SurfaceMaterialization> {
        let key = v1_key(namespace, namehash);
        self.v1_materialized_surfaces.insert(key.clone());
        self.remember_known_surface(logical_name_id.to_owned());
        let authority = self
            .v1_names
            .get(&key)
            .cloned()
            .filter(|authority| authority.token_lineage_id.is_some())
            .ok_or_else(|| {
                anyhow::anyhow!("registrar authority for {namespace}:{namehash} is not current")
            })?;
        let source_manifest_id = authority.source_manifest_id.ok_or_else(|| {
            anyhow::anyhow!("registrar authority for {namespace}:{namehash} has no source manifest")
        })?;
        self.require_source_manifest(namespace, namehash, source_manifest_id)?;
        let resolver = self
            .v1_resolver_links
            .get(&key)
            .cloned()
            .filter(|link| link.logical_name_id.is_none())
            .map(|link| V1ResolverLink {
                resource_id: Some(authority.resource_id),
                logical_name_id: Some(logical_name_id.to_owned()),
                ..link
            });
        if let Some(resolver) = resolver.as_ref() {
            self.v1_resolver_links.insert(key, resolver.clone());
        }
        Ok(V1SurfaceMaterialization::RegistrarAuthority {
            authority: Box::new(authority),
            resolver,
            source_manifest_id,
        })
    }

    pub(in crate::schema_v2) fn materialize_or_sync_v1_active_surface(
        &mut self,
        namespace: &str,
        namehash: &str,
        logical_name_id: &str,
        labelhash: &str,
    ) -> anyhow::Result<V1SurfaceMaterialization> {
        let materialization =
            self.materialize_v1_active_surface(namespace, namehash, logical_name_id, labelhash)?;
        if materialization == V1SurfaceMaterialization::AlreadyMaterialized {
            self.sync_registry_surface_from_registrar(
                namespace,
                namehash,
                logical_name_id,
                true,
                Some(labelhash),
            );
        }
        Ok(materialization)
    }
}
