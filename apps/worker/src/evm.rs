use alloy_primitives::{Address, B256, hex};

pub(crate) fn normalize_evm_address_or_lowercase(value: &str) -> String {
    normalize_standard_evm_address(value).unwrap_or_else(|| value.to_ascii_lowercase())
}

pub(crate) fn normalize_trimmed_evm_address_or_lowercase(value: &str) -> String {
    normalize_evm_address_or_lowercase(value.trim())
}

pub(crate) fn normalize_evm_b256_or_lowercase(value: &str) -> String {
    normalize_standard_evm_b256(value).unwrap_or_else(|| value.to_ascii_lowercase())
}

fn normalize_standard_evm_address(value: &str) -> Option<String> {
    if value.len() != 42 || (!value.starts_with("0x") && !value.starts_with("0X")) {
        return None;
    }

    let address = format!("0x{}", &value[2..]).parse::<Address>().ok()?;
    Some(format_prefixed_hex(address.as_slice()))
}

fn normalize_standard_evm_b256(value: &str) -> Option<String> {
    if value.len() != 66 || (!value.starts_with("0x") && !value.starts_with("0X")) {
        return None;
    }

    let hash = format!("0x{}", &value[2..]).parse::<B256>().ok()?;
    Some(format_prefixed_hex(hash.as_slice()))
}

fn format_prefixed_hex(bytes: impl AsRef<[u8]>) -> String {
    format!("0x{}", hex::encode(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_address_uses_alloy_for_standard_hex_without_tightening_fallbacks() {
        assert_eq!(
            normalize_evm_address_or_lowercase("0X00000000000C2E074eC69A0dFb2997BA6C7d2E1E"),
            "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e"
        );
        assert_eq!(
            normalize_trimmed_evm_address_or_lowercase(
                " 0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E "
            ),
            "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e"
        );
        assert_eq!(
            normalize_evm_address_or_lowercase("NOT-A-HEX-ADDRESS"),
            "not-a-hex-address"
        );
        assert_eq!(normalize_evm_address_or_lowercase("0xABC"), "0xabc");
    }

    #[test]
    fn normalize_b256_uses_alloy_for_standard_hex_without_tightening_fallbacks() {
        let mixed_case_hash = format!("0x{:0>64}", "AA");
        let normalized_hash = format!("0x{:0>64}", "aa");
        assert_eq!(
            normalize_evm_b256_or_lowercase(&mixed_case_hash),
            normalized_hash
        );
        assert_eq!(normalize_evm_b256_or_lowercase("0xABC"), "0xabc");
        assert_eq!(normalize_evm_b256_or_lowercase("NOT-A-HASH"), "not-a-hash");
    }
}
