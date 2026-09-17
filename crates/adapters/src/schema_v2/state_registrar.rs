use uuid::Uuid;

use super::{State, V1ResolverLink};

const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

pub(in crate::schema_v2) fn v1_key(namespace: &str, namehash: &str) -> String {
    format!("{namespace}:{}", namehash.to_ascii_lowercase())
}

impl State {
    // BaseRegistrar emits the same NameRegistered event for `register` and `registerOnly`, but the
    // latter deliberately skips the ENS registry write. Exact same-transaction registry evidence
    // distinguishes an incoming setup from a retained, legitimately divergent registry owner.
    // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L118-L152 @ ens_v1@91c966f)
    pub(in crate::schema_v2) fn v1_registrar_event_makes_current(
        &self,
        namespace: &str,
        namehash: &str,
        registrar_family: &str,
        registrar_owner: Option<&str>,
        registration: bool,
        transaction_has_registry_setup: bool,
    ) -> bool {
        self.v1_names
            .get(&v1_key(namespace, namehash))
            .is_none_or(|current| {
                current.authority_source_family == registrar_family
                    || (registration
                        && (transaction_has_registry_setup
                            || (current.token_lineage_id.is_none()
                                && current.owner.as_deref().zip(registrar_owner).is_some_and(
                                    |(registry_owner, registrar_owner)| {
                                        registry_owner.eq_ignore_ascii_case(registrar_owner)
                                    },
                                ))))
            })
    }

    /// A resolver set while the node's authority had no surface is linked to the
    /// resource alone. When a label-bearing event names the surface, the link
    /// takes the name and the named authority's resource, and the link as it is
    /// now stored is returned so the caller can replay its resolver onto the
    /// surface. A link that already carries a name, a cleared (zero) resolver,
    /// or no link returns nothing and changes nothing.
    pub(in crate::schema_v2) fn name_v1_resolver_link(
        &mut self,
        namespace: &str,
        namehash: &str,
        logical_name_id: &str,
        resource_id: Uuid,
    ) -> Option<V1ResolverLink> {
        let key = v1_key(namespace, namehash);
        let link = self.v1_resolver_links.get(&key)?.clone();
        if link.logical_name_id.is_some()
            || link.resolver_address.eq_ignore_ascii_case(ZERO_ADDRESS)
        {
            return None;
        }
        let named = V1ResolverLink {
            resource_id: Some(resource_id),
            logical_name_id: Some(logical_name_id.to_owned()),
            ..link
        };
        self.v1_resolver_links.insert(key, named.clone());
        Some(named)
    }
}
