use super::*;
use crate::evm_abi;
use alloy_sol_types::sol_data::{Address as SolAddress, Bytes as SolBytes, String as SolString};

pub(super) fn decode_first_dynamic_string(data: &[u8]) -> Result<String> {
    let (value,) = evm_abi::abi_decode_params::<(SolString,)>(
        data,
        "first dynamic string payload is malformed",
    )?;
    Ok(value)
}

pub(super) fn decode_first_dynamic_bytes(data: &[u8]) -> Result<Vec<u8>> {
    let (value,) = evm_abi::abi_decode_params::<(SolBytes,)>(
        data,
        "first dynamic bytes payload is malformed",
    )?;
    Ok(value.to_vec())
}

pub(super) fn decode_nth_dynamic_string(data: &[u8], parameter_index: usize) -> Result<String> {
    match parameter_index {
        0 => decode_first_dynamic_string(data),
        1 => {
            let (_, value) = evm_abi::abi_decode_params::<(SolString, SolString)>(
                data,
                "second dynamic string payload is malformed",
            )?;
            Ok(value)
        }
        _ => evm_abi::dynamic_string(data, parameter_index),
    }
}

pub(super) fn decode_nth_dynamic_bytes(data: &[u8], parameter_index: usize) -> Result<Vec<u8>> {
    match parameter_index {
        0 => decode_first_dynamic_bytes(data),
        _ => evm_abi::dynamic_bytes(data, parameter_index),
    }
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
    let (address,) =
        evm_abi::abi_decode_params::<(SolAddress,)>(data, "owner address payload is malformed")?;
    Ok(evm_abi::address_hex(address))
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
