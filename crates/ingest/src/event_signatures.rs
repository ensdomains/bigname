pub use bigname_manifests::{
    BASENAMES_BASE_RESOLVER_SOURCE_FAMILY, ENS_V1_RESOLVER_SOURCE_FAMILY,
    ENS_V2_REGISTRY_SOURCE_FAMILY, ENS_V2_RESOLVER_SOURCE_FAMILY, registry_announcement_topic0,
};

#[cfg(test)]
pub use bigname_manifests::generic_resolver_topic0s;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generic_resolver_topic_set_excludes_shared_standard_events() {
        let topics = generic_resolver_topic0s();

        assert_eq!(topics.len(), 14);
        assert!(!topics.contains(&format!(
            "{}",
            alloy_primitives::keccak256("ApprovalForAll(address,address,bool)".as_bytes())
        )));
    }

    #[test]
    fn generic_resolver_topics_also_exclude_token_and_delegate_approvals() {
        let topics = generic_resolver_topic0s();

        for signature in [
            "Approval(address,address,uint256)",
            "Approved(address,bytes32,address,bool)",
        ] {
            assert!(!topics.contains(&format!(
                "{}",
                alloy_primitives::keccak256(signature.as_bytes())
            )));
        }
        assert_eq!(
            topics.len(),
            14,
            "the existing generic topic set is retained"
        );
    }

    #[test]
    fn registry_announcement_topic_is_the_registry_created_selector() {
        assert_eq!(
            registry_announcement_topic0(),
            format!(
                "{}",
                alloy_primitives::keccak256("RegistryCreated()".as_bytes())
            )
        );
    }
}
