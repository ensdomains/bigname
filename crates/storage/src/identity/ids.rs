use alloy_primitives::keccak256;
use uuid::Uuid;

/// Returns the stable bigname identity for an ENSv2 registry EAC resource.
///
/// The seed layout and UUID bit normalization are compatibility-sensitive: adapters and
/// projections persist and join on these exact bytes.
pub fn ens_v2_registry_resource_id(
    chain_id: &str,
    registry_contract_instance_id: Uuid,
    upstream_resource: &str,
) -> Uuid {
    let seed =
        format!("ens-v2-resource:{chain_id}:{registry_contract_instance_id}:{upstream_resource}");
    let digest = keccak256(seed.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ens_v2_registry_resource_id_matches_legacy_golden_vectors() {
        let vectors = [
            (
                "ethereum-sepolia",
                "00000000-0000-0000-0000-000000001234",
                "0x0000000000000000000000000000000000000000000000000000000000000eac",
                "1b3e5fe2-1f00-5c75-97c9-d2a5ccd024e2",
            ),
            (
                "sepolia-dev",
                "22222222-2222-5222-8222-222222222222",
                "0x0000000000000000000000000000000000000000000000000000000000000000",
                "9dc2aecc-e987-52e2-b6c7-823eb71231bc",
            ),
            (
                "ethereum-mainnet",
                "00000000-0000-0000-0000-00000000e201",
                "0x0000000000000000000000000000000000000000000000000000000000000000",
                "882f36f0-b76f-5dd2-9eae-e4d2fe4bb714",
            ),
        ];

        for (chain_id, contract_instance_id, upstream_resource, expected) in vectors {
            assert_eq!(
                ens_v2_registry_resource_id(
                    chain_id,
                    Uuid::parse_str(contract_instance_id).expect("golden UUID should parse"),
                    upstream_resource,
                ),
                Uuid::parse_str(expected).expect("golden UUID should parse")
            );
        }
    }
}
