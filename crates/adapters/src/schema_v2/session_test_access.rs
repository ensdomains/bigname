//! Read access to a session's retained state for the adapter tests.
use super::AdapterSession;
use crate::schema_v2::state::{V1NameState, V1ResolverLink};

impl AdapterSession {
    pub(in crate::schema_v2) fn has_v1_registry_authority(
        &self,
        namespace: &str,
        namehash: &str,
    ) -> bool {
        self.state.has_v1_registry_authority(namespace, namehash)
    }

    pub(in crate::schema_v2) fn v1_name(
        &self,
        namespace: &str,
        namehash: &str,
    ) -> Option<V1NameState> {
        self.state.v1_name(namespace, namehash)
    }

    pub(in crate::schema_v2) fn v1_resolver_link(
        &self,
        namespace: &str,
        namehash: &str,
    ) -> Option<V1ResolverLink> {
        self.state.v1_resolver_link(namespace, namehash)
    }
}
