use std::str::FromStr;

use alloy_primitives::{Address, B256, hex, keccak256};

/// Derive an ENS namehash from raw labels in left-to-right name order.
pub fn ens_namehash_label_bytes(labels: &[&[u8]]) -> B256 {
    labels.iter().rev().fold(B256::ZERO, |parent, label| {
        let labelhash = keccak256(label);
        let mut input = [0_u8; 64];
        input[..32].copy_from_slice(parent.as_slice());
        input[32..].copy_from_slice(labelhash.as_slice());
        keccak256(input)
    })
}

pub fn normalize_evm_address(value: &str) -> String {
    normalize_standard_address(value).unwrap_or_else(|| value.to_ascii_lowercase())
}

pub(crate) fn normalize_optional_evm_address(value: &Option<String>) -> Option<String> {
    value.as_deref().map(normalize_evm_address)
}

pub fn normalize_evm_b256(value: &str) -> String {
    normalize_standard_b256(value).unwrap_or_else(|| normalize_evm_hex_bytes(value))
}

pub(crate) fn normalize_optional_evm_b256(value: &Option<String>) -> Option<String> {
    value.as_deref().map(normalize_evm_b256)
}

pub(crate) fn normalize_evm_hex_bytes(value: &str) -> String {
    normalize_prefixed_hex_bytes(value).unwrap_or_else(|| value.to_ascii_lowercase())
}

fn normalize_standard_address(value: &str) -> Option<String> {
    if !is_prefixed_hex_len(value, 40) {
        return None;
    }

    let address = Address::from_str(value).ok()?;
    Some(format_prefixed_hex(address.as_slice()))
}

fn normalize_standard_b256(value: &str) -> Option<String> {
    if !is_prefixed_hex_len(value, 64) {
        return None;
    }

    let hash = B256::from_str(value).ok()?;
    Some(format_prefixed_hex(hash.as_slice()))
}

fn normalize_prefixed_hex_bytes(value: &str) -> Option<String> {
    let payload = strip_hex_prefix(value)?;
    if payload.len() % 2 != 0 {
        return None;
    }

    let bytes = hex::decode(payload).ok()?;
    Some(format_prefixed_hex(bytes))
}

fn is_prefixed_hex_len(value: &str, payload_len: usize) -> bool {
    strip_hex_prefix(value).is_some_and(|payload| payload.len() == payload_len)
}

fn strip_hex_prefix(value: &str) -> Option<&str> {
    value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
}

fn format_prefixed_hex(bytes: impl AsRef<[u8]>) -> String {
    format!("0x{}", hex::encode(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_standard_evm_primitives_without_rejecting_sentinels() {
        assert_eq!(
            normalize_evm_address("0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E"),
            "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e"
        );
        assert_eq!(
            normalize_evm_b256(
                "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            ),
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        assert_eq!(normalize_evm_hex_bytes("0XDEADBEEF"), "0xdeadbeef");
        assert_eq!(normalize_evm_address("0xABC"), "0xabc");
        assert_eq!(normalize_evm_b256("0xblockAAA"), "0xblockaaa");
        assert_eq!(normalize_evm_hex_bytes("sha256:ABCDEF"), "sha256:abcdef");
    }

    #[test]
    fn derives_ens_namehash_from_labels_in_name_order() {
        assert_eq!(
            ens_namehash_label_bytes(&[
                b"0000000000000000000000000000000000001234",
                b"addr",
                b"reverse",
            ]),
            "0x1378947657d42d9154dde03fb7f77bc334f2644cbeab9b53de179fb457806802"
                .parse::<B256>()
                .expect("fixture must be a 32-byte hash")
        );
    }
}
