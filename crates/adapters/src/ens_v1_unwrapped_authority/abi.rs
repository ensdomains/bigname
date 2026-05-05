use super::*;
use crate::evm_abi;

pub(super) fn decode_first_dynamic_string(data: &[u8]) -> Result<String> {
    decode_nth_dynamic_string(data, 0)
}

pub(super) fn decode_first_dynamic_bytes(data: &[u8]) -> Result<Vec<u8>> {
    decode_nth_dynamic_bytes(data, 0)
}

pub(super) fn decode_nth_dynamic_string(data: &[u8], parameter_index: usize) -> Result<String> {
    evm_abi::dynamic_string(data, parameter_index)
}

pub(super) fn decode_nth_dynamic_bytes(data: &[u8], parameter_index: usize) -> Result<Vec<u8>> {
    evm_abi::dynamic_bytes(data, parameter_index)
}

pub(super) fn abi_word_to_usize(word: &[u8]) -> Result<usize> {
    evm_abi::usize_from_word(word)
}

pub(super) fn abi_word_to_i64(word: &[u8]) -> Result<i64> {
    evm_abi::i64_from_u64_word(word)
}

pub(super) fn normalize_hex_32(value: &str) -> Result<String> {
    evm_abi::normalize_hex_32(value)
}

pub(super) fn decode_owner_address(data: &[u8]) -> Result<String> {
    evm_abi::address_word_hex(data, 0)
        .context("owner address payload is missing the first ABI word")
}

pub(super) fn normalize_topic_address(value: &str) -> Result<String> {
    evm_abi::topic_address_hex(value)
}

pub(super) fn parse_canonicality_state(value: &str) -> Result<CanonicalityState> {
    match value {
        "observed" => Ok(CanonicalityState::Observed),
        "canonical" => Ok(CanonicalityState::Canonical),
        "safe" => Ok(CanonicalityState::Safe),
        "finalized" => Ok(CanonicalityState::Finalized),
        "orphaned" => Ok(CanonicalityState::Orphaned),
        _ => bail!("unknown canonicality_state value {value}"),
    }
}
