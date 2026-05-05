use alloy_primitives::{hex, keccak256};

use super::constants::{
    NAME_WRAPPED_SIGNATURE, REGISTRAR_NAME_REGISTERED_SIGNATURE, REGISTRAR_NAME_RENEWED_SIGNATURE,
};
pub(super) use crate::evm_abi::{
    dynamic_bytes as decode_dynamic_bytes, dynamic_string as decode_dynamic_string,
};

pub(super) fn name_wrapped_topic0() -> String {
    keccak256_hex(NAME_WRAPPED_SIGNATURE.as_bytes())
}

pub(super) fn registrar_name_registered_topic0() -> String {
    keccak256_hex(REGISTRAR_NAME_REGISTERED_SIGNATURE.as_bytes())
}

pub(super) fn registrar_name_renewed_topic0() -> String {
    keccak256_hex(REGISTRAR_NAME_RENEWED_SIGNATURE.as_bytes())
}

pub(super) fn keccak_signature_hex(signature: &str) -> String {
    keccak256_hex(signature.as_bytes())
}

pub(super) fn namehash_hex(labels: &[Vec<u8>]) -> String {
    let mut node = [0u8; 32];
    for label in labels.iter().rev() {
        let label_hash = keccak256_bytes(label);
        let mut combined = [0u8; 64];
        combined[..32].copy_from_slice(&node);
        combined[32..].copy_from_slice(&label_hash);
        node = keccak256_bytes(&combined);
    }
    hex_string(&node)
}

fn keccak256_bytes(bytes: &[u8]) -> [u8; 32] {
    let digest = keccak256(bytes);
    let mut output = [0u8; 32];
    output.copy_from_slice(digest.as_slice());
    output
}

pub(super) fn keccak256_hex(bytes: &[u8]) -> String {
    hex_string(&keccak256_bytes(bytes))
}

pub(super) fn hex_string(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

pub(super) fn hex_string_without_prefix(bytes: &[u8]) -> String {
    hex::encode(bytes)
}
