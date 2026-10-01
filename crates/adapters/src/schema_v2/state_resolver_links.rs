//! Which restored ENSv1 registry resolver write the retained link keeps, and the per-resource
//! copies an old-registry resolver selection keeps.
use std::cmp::Ordering;

use uuid::Uuid;

use super::{State, V1ResolverLink, v1_key};
use crate::schema_v2::model::PriorWritePosition;

/// The raw write behind the last restored registry resolver row a name's link took, clears
/// included, and whether that row was the registry-read copy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::schema_v2) struct V1ResolverWriteMark {
    position: PriorWritePosition,
    registry_read: bool,
}

impl State {
    /// Whether a restored registry resolver row replaces the name's link. Stored rows of one
    /// block come back in no fixed order, and one raw write can leave copies on several
    /// resources. A later raw write, by block, transaction and log position, always replaces the
    /// link and an earlier one never does; among the copies of one write the registry-read copy
    /// wins.
    pub(in crate::schema_v2) fn restored_v1_resolver_write_replaces(
        &self,
        namespace: &str,
        namehash: &str,
        position: Option<PriorWritePosition>,
        registry_read: bool,
        written_at: Option<i64>,
        registry_resource_id: Uuid,
    ) -> bool {
        let key = v1_key(namespace, namehash);
        if let (Some(position), Some(mark)) = (position, self.v1_resolver_write_marks.get(&key)) {
            return match position.cmp(&mark.position) {
                Ordering::Less => false,
                Ordering::Equal => registry_read || !mark.registry_read,
                Ordering::Greater => true,
            };
        }
        // A row without a full raw position, or a link last set by one, can only be ordered by
        // block time: a nonzero registry-read link from the same block time keeps out the
        // other resources' rows.
        let registry_linked_at_same_time =
            self.v1_resolver_link(namespace, namehash)
                .is_some_and(|link| {
                    link.resource_id == Some(registry_resource_id) && link.written_at == written_at
                });
        registry_read || !registry_linked_at_same_time
    }

    /// Record the restored registry resolver write the link just took, clears included.
    pub(in crate::schema_v2) fn record_restored_v1_resolver_write(
        &mut self,
        namespace: &str,
        namehash: &str,
        position: Option<PriorWritePosition>,
        registry_read: bool,
        written_at: Option<i64>,
    ) {
        let key = v1_key(namespace, namehash);
        match position {
            Some(position) => {
                self.v1_resolver_write_marks.insert(
                    key.clone(),
                    V1ResolverWriteMark {
                        position,
                        registry_read,
                    },
                );
            }
            None => {
                self.v1_resolver_write_marks.remove(&key);
            }
        }
        if let Some(link) = self.v1_resolver_links.get_mut(&key) {
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
