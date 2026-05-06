pub(crate) use bigname_storage::{
    normalize_evm_address as normalize_evm_address_or_lowercase,
    normalize_evm_b256 as normalize_evm_b256_or_lowercase,
};

pub(crate) fn normalize_trimmed_evm_address_or_lowercase(value: &str) -> String {
    normalize_evm_address_or_lowercase(value.trim())
}
