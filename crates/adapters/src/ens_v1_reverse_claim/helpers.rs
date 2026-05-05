use alloy_primitives::{hex, keccak256};
use anyhow::{Result, bail};

use crate::evm_abi;

use super::{
    REVERSE_CLAIMED_SIGNATURE, SOURCE_FAMILY_BASENAMES_BASE_PRIMARY,
    SOURCE_FAMILY_ENS_V1_REVERSE_L1,
};

pub(super) fn supports_reverse_claim_source_family(source_family: &str) -> bool {
    matches!(
        source_family,
        SOURCE_FAMILY_ENS_V1_REVERSE_L1 | SOURCE_FAMILY_BASENAMES_BASE_PRIMARY
    )
}

pub(super) fn normalize_address(value: &str) -> Result<String> {
    let normalized = value.to_ascii_lowercase();
    if !normalized.starts_with("0x") || normalized.len() != 42 {
        bail!("expected 20-byte address, got {value}");
    }
    Ok(normalized)
}

pub(super) fn reverse_label_for_address(address: &str) -> Result<String> {
    Ok(normalize_address(address)?
        .trim_start_matches("0x")
        .to_owned())
}

pub(super) fn reverse_node_for_address(address: &str) -> Result<String> {
    let reverse_label = reverse_label_for_address(address)?;
    Ok(namehash_hex(&[
        reverse_label.into_bytes(),
        b"addr".to_vec(),
        b"reverse".to_vec(),
    ]))
}

pub(super) fn normalize_hex_32(value: &str) -> Result<String> {
    evm_abi::normalize_hex_32(value)
}

pub(super) fn normalize_topic_address(value: &str) -> Result<String> {
    evm_abi::topic_address_hex(value)
}

pub(super) fn reverse_claimed_topic0() -> String {
    keccak256_hex(REVERSE_CLAIMED_SIGNATURE.as_bytes())
}

pub(super) fn namehash_hex(labels: &[Vec<u8>]) -> String {
    let mut node = [0u8; 32];
    for label in labels.iter().rev() {
        let mut combined = [0u8; 64];
        combined[..32].copy_from_slice(&node);
        combined[32..].copy_from_slice(keccak256(label).as_slice());
        node.copy_from_slice(keccak256(combined).as_slice());
    }

    hex_string(&node)
}

pub(super) fn keccak256_hex(bytes: &[u8]) -> String {
    hex_string(keccak256(bytes).as_slice())
}

pub(super) fn hex_string(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}
