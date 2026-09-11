use super::State;

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
}
