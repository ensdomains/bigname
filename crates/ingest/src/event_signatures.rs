pub const ENS_V1_RESOLVER_SOURCE_FAMILY: &str = "ens_v1_resolver_l1";
pub const ENS_V2_RESOLVER_SOURCE_FAMILY: &str = "ens_v2_resolver_l1";
pub const ENS_V2_REGISTRY_SOURCE_FAMILY: &str = "ens_v2_registry_l1";
pub const BASENAMES_BASE_RESOLVER_SOURCE_FAMILY: &str = "basenames_base_resolver";

const REGISTRY_CREATED_SIGNATURE: &str = "RegistryCreated()";
const ENS_V2_UNIQUE_RESOLVER_EVENT_SIGNATURES: &[&str] = &[
    "AliasChanged(bytes,bytes,bytes,bytes)",
    "NamedResource(uint256,bytes)",
    "NamedTextResource(uint256,bytes,bytes32,string)",
    "NamedAddrResource(uint256,bytes,uint256)",
];
const GENERIC_RESOLVER_EVENT_SIGNATURES: &[&str] = &[
    "ABIChanged(bytes32,uint256)",
    "AddrChanged(bytes32,address)",
    "AddressChanged(bytes32,uint256,bytes)",
    "ContentChanged(bytes32,bytes32)",
    "ContenthashChanged(bytes32,bytes)",
    "DNSRecordChanged(bytes32,bytes,uint16,bytes)",
    "DNSRecordDeleted(bytes32,bytes,uint16)",
    "DNSZonehashChanged(bytes32,bytes,bytes)",
    "DataChanged(bytes32,string,string,bytes)",
    "InterfaceChanged(bytes32,bytes4,address)",
    "NameChanged(bytes32,string)",
    "TextChanged(bytes32,string,string)",
    "TextChanged(bytes32,string,string,string)",
    "VersionChanged(bytes32,uint64)",
];

pub fn generic_resolver_topic0s() -> Vec<String> {
    GENERIC_RESOLVER_EVENT_SIGNATURES
        .iter()
        .map(|signature| format!("{}", alloy_primitives::keccak256(signature.as_bytes())))
        .collect()
}

pub fn registry_announcement_topic0() -> String {
    format!(
        "{}",
        alloy_primitives::keccak256(REGISTRY_CREATED_SIGNATURE.as_bytes())
    )
}

pub fn ens_v2_unique_resolver_topic0s() -> Vec<String> {
    ENS_V2_UNIQUE_RESOLVER_EVENT_SIGNATURES
        .iter()
        .map(|signature| format!("{}", alloy_primitives::keccak256(signature.as_bytes())))
        .collect()
}

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
