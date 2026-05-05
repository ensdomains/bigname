use alloy_primitives::{hex, keccak256};
use anyhow::{Context, Result};

use crate::evm_abi;

const NEW_OWNER_SIGNATURE: &str = "NewOwner(bytes32,bytes32,address)";
const NEW_RESOLVER_SIGNATURE: &str = "NewResolver(bytes32,address)";
const REGISTRY_TRANSFER_SIGNATURE: &str = "Transfer(bytes32,address)";
const NEW_TTL_SIGNATURE: &str = "NewTTL(bytes32,uint64)";
pub(super) const ZERO_NODE: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000000";
pub(super) const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

pub(super) fn decode_owner_address(data: &[u8]) -> Result<String> {
    evm_abi::address_word_hex(data, 0).context("NewOwner log data must be at least 32 bytes")
}

pub(super) fn child_node(parent_node: &str, labelhash: &str) -> Result<String> {
    let parent_node = decode_hex_32(parent_node)?;
    let labelhash = decode_hex_32(labelhash)?;
    let mut combined = [0u8; 64];
    combined[..32].copy_from_slice(&parent_node);
    combined[32..].copy_from_slice(&labelhash);
    Ok(format!("0x{}", hex_string(keccak256(combined))))
}

pub(super) fn decode_hex_32(value: &str) -> Result<[u8; 32]> {
    evm_abi::hex_32(value)
}

pub(super) fn normalize_hex_32(value: &str) -> Result<String> {
    evm_abi::normalize_hex_32(value)
}

pub(super) fn normalize_address(value: &str) -> String {
    value.to_ascii_lowercase()
}

pub(super) fn new_owner_topic0() -> String {
    keccak_signature_hex(NEW_OWNER_SIGNATURE)
}

pub(super) fn new_resolver_topic0() -> String {
    keccak_signature_hex(NEW_RESOLVER_SIGNATURE)
}

pub(super) fn registry_transfer_topic0() -> String {
    keccak_signature_hex(REGISTRY_TRANSFER_SIGNATURE)
}

pub(super) fn new_ttl_topic0() -> String {
    keccak_signature_hex(NEW_TTL_SIGNATURE)
}

fn keccak_signature_hex(signature: &str) -> String {
    format!("0x{}", hex_string(keccak256(signature.as_bytes())))
}

pub(super) fn null_if_zero_address(address: &str) -> Option<String> {
    if normalize_address(address) == ZERO_ADDRESS {
        None
    } else {
        Some(normalize_address(address))
    }
}

pub(super) fn hex_string(bytes: impl AsRef<[u8]>) -> String {
    hex::encode(bytes)
}
