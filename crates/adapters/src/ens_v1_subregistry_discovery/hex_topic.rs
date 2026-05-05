use anyhow::{Context, Result};

use crate::evm_abi;
pub(super) use crate::evm_abi::hex_string_without_prefix as hex_string;
use crate::evm_abi::keccak_signature_hex;

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
    evm_abi::child_namehash_hex(parent_node, labelhash)
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

pub(super) fn null_if_zero_address(address: &str) -> Option<String> {
    if normalize_address(address) == ZERO_ADDRESS {
        None
    } else {
        Some(normalize_address(address))
    }
}
