//! The retained ENSv1 resolver link's write time and the per-resource copies an old-registry
//! resolver selection keeps.
use uuid::Uuid;

use super::{State, V1ResolverLink, v1_key};

impl State {
    pub(in crate::schema_v2) fn set_v1_resolver_link_written_at(
        &mut self,
        namespace: &str,
        namehash: &str,
        written_at: Option<i64>,
    ) {
        if let Some(link) = self.v1_resolver_links.get_mut(&v1_key(namespace, namehash)) {
            link.written_at = written_at;
        }
    }

    pub(in crate::schema_v2) fn remember_v1_resolver_linked_resource(
        &mut self,
        namespace: &str,
        namehash: &str,
        resolver: &str,
        resource_id: Uuid,
        logical_name_id: Option<String>,
    ) {
        let key = v1_key(namespace, namehash);
        if resolver.eq_ignore_ascii_case("0x0000000000000000000000000000000000000000") {
            self.remove_v1_resolver_linked_resource(&key, resource_id);
            return;
        }
        let Some(selected) = self.v1_resolver_links.get(&key) else {
            return;
        };
        if selected.source_role.as_deref() != Some("registry_old")
            || !selected.resolver_address.eq_ignore_ascii_case(resolver)
        {
            return;
        }
        self.v1_resolver_linked_resources
            .entry(key)
            .or_default()
            .insert(
                resource_id,
                V1ResolverLink {
                    resolver_address: selected.resolver_address.clone(),
                    resource_id: Some(resource_id),
                    logical_name_id,
                    source_role: selected.source_role.clone(),
                    written_at: None,
                },
            );
    }

    pub(in crate::schema_v2) fn restore_v1_resolver_linked_resource(
        &mut self,
        namespace: &str,
        namehash: &str,
        resolver: &str,
        resource_id: Uuid,
        logical_name_id: Option<String>,
        source_role: &str,
    ) {
        let key = v1_key(namespace, namehash);
        if resolver.eq_ignore_ascii_case("0x0000000000000000000000000000000000000000") {
            self.remove_v1_resolver_linked_resource(&key, resource_id);
            return;
        }
        if source_role != "registry_old" {
            return;
        }
        self.v1_resolver_linked_resources
            .entry(key)
            .or_default()
            .insert(
                resource_id,
                V1ResolverLink {
                    resolver_address: resolver.to_owned(),
                    resource_id: Some(resource_id),
                    logical_name_id,
                    source_role: Some(source_role.to_owned()),
                    written_at: None,
                },
            );
    }
}
