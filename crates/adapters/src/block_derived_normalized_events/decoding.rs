use super::constants::{
    NAME_WRAPPED_SIGNATURE, REGISTRAR_NAME_REGISTERED_SIGNATURE, REGISTRAR_NAME_RENEWED_SIGNATURE,
};
pub(super) use crate::evm_abi::{
    dynamic_bytes as decode_dynamic_bytes, dynamic_string as decode_dynamic_string, hex_string,
    hex_string_without_prefix, keccak_signature_hex, keccak256_hex, namehash_hex,
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
