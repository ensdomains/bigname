use std::collections::BTreeSet;

pub const ENS_V1_RESOLVER_SOURCE_FAMILY: &str = "ens_v1_resolver_l1";
pub const ENS_V2_RESOLVER_SOURCE_FAMILY: &str = "ens_v2_resolver_l1";
pub const ENS_V2_REGISTRY_SOURCE_FAMILY: &str = "ens_v2_registry_l1";
pub const BASENAMES_BASE_RESOLVER_SOURCE_FAMILY: &str = "basenames_base_resolver";

const REGISTRY_CREATED_SIGNATURE: &str = "RegistryCreated()";
pub const APPROVAL_FOR_ALL_SIGNATURE: &str = "ApprovalForAll(address,address,bool)";
pub const APPROVAL_SIGNATURE: &str = "Approval(address,address,uint256)";
pub const APPROVED_SIGNATURE: &str = "Approved(address,bytes32,address,bool)";
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

pub fn all_emitter_topic0s(source_family: &str, manifest_topic0s: &[String]) -> Vec<String> {
    let manifest_topic0s = manifest_topic0s.iter().cloned().collect::<BTreeSet<_>>();
    let candidates = match source_family {
        ENS_V1_RESOLVER_SOURCE_FAMILY | BASENAMES_BASE_RESOLVER_SOURCE_FAMILY => {
            generic_resolver_topic0s()
        }
        ENS_V2_REGISTRY_SOURCE_FAMILY => vec![registry_announcement_topic0()],
        ENS_V2_RESOLVER_SOURCE_FAMILY => ens_v2_unique_resolver_topic0s(),
        _ => Vec::new(),
    };
    candidates
        .into_iter()
        .filter(|topic| manifest_topic0s.contains(topic))
        .collect()
}

pub fn uses_discovered_emitters(source_family: &str) -> bool {
    matches!(
        source_family,
        ENS_V1_RESOLVER_SOURCE_FAMILY
            | BASENAMES_BASE_RESOLVER_SOURCE_FAMILY
            | ENS_V2_REGISTRY_SOURCE_FAMILY
            | ENS_V2_RESOLVER_SOURCE_FAMILY
    )
}

/// Returns whether an approval [intake-only event](../../../../docs/glossary.md#intake-only-event)
/// uses only the contract roles named by its manifest declaration. Keep this list closed: empty
/// normalized output is not by itself an admission or watch-policy bypass.
pub fn is_address_scoped_approval(source_family: &str, signature: &str) -> bool {
    match source_family {
        "ens_v1_registry_l1" | "basenames_base_registry" => signature == APPROVAL_FOR_ALL_SIGNATURE,
        "ens_v1_registrar_l1" | "basenames_base_registrar" | "ens_v1_wrapper_l1" => {
            matches!(signature, APPROVAL_SIGNATURE | APPROVAL_FOR_ALL_SIGNATURE)
        }
        ENS_V1_RESOLVER_SOURCE_FAMILY | BASENAMES_BASE_RESOLVER_SOURCE_FAMILY => {
            matches!(signature, APPROVAL_FOR_ALL_SIGNATURE | APPROVED_SIGNATURE)
        }
        _ => false,
    }
}

pub fn generic_resolver_topic0s() -> Vec<String> {
    topic0s(GENERIC_RESOLVER_EVENT_SIGNATURES)
}

pub fn registry_announcement_topic0() -> String {
    topic0(REGISTRY_CREATED_SIGNATURE)
}

pub fn ens_v2_unique_resolver_topic0s() -> Vec<String> {
    topic0s(ENS_V2_UNIQUE_RESOLVER_EVENT_SIGNATURES)
}

fn topic0s(signatures: &[&str]) -> Vec<String> {
    signatures
        .iter()
        .map(|signature| topic0(signature))
        .collect()
}

fn topic0(signature: &str) -> String {
    format!("{}", alloy_primitives::keccak256(signature.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v1_and_basenames_select_all_fourteen_generic_topics() {
        let generic = generic_resolver_topic0s();
        assert_eq!(generic.len(), 14);
        assert_eq!(
            all_emitter_topic0s(ENS_V1_RESOLVER_SOURCE_FAMILY, &generic),
            generic
        );
        let generic = generic_resolver_topic0s();
        assert_eq!(
            all_emitter_topic0s(BASENAMES_BASE_RESOLVER_SOURCE_FAMILY, &generic),
            generic
        );
    }

    #[test]
    fn v2_selects_only_its_four_unique_topics() {
        let generic_text = topic0("TextChanged(bytes32,string,string,string)");
        let mut manifest = ens_v2_unique_resolver_topic0s();
        assert_eq!(manifest.len(), 4);
        manifest.push(generic_text.clone());
        assert_eq!(
            all_emitter_topic0s(ENS_V2_RESOLVER_SOURCE_FAMILY, &manifest),
            ens_v2_unique_resolver_topic0s()
        );
        assert!(
            !all_emitter_topic0s(ENS_V2_RESOLVER_SOURCE_FAMILY, &manifest).contains(&generic_text)
        );
    }
}
