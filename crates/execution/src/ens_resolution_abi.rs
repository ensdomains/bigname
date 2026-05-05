use alloy_primitives::{Address, B256, U256, hex, keccak256};
use alloy_sol_types::{SolType, SolValue, sol_data};
use anyhow::{Context, Result, bail};
use bigname_storage::SupportedVerifiedResolutionRecordKey;
pub(crate) const UNIVERSAL_RESOLVER_RESOLVE_SELECTOR: [u8; 4] = [0x90, 0x61, 0xb9, 0x23];
pub(crate) const ADDR_SELECTOR: [u8; 4] = [0x3b, 0x3b, 0x57, 0xde];
pub(crate) const MULTICOIN_ADDR_SELECTOR: [u8; 4] = [0xf1, 0xcb, 0x7e, 0x06];
pub(crate) const TEXT_SELECTOR: [u8; 4] = [0x59, 0xd1, 0xd4, 0x3c];
pub(crate) const CONTENTHASH_SELECTOR: [u8; 4] = [0xbc, 0x1c, 0x58, 0xd1];

pub(crate) fn selector_hex(selector: [u8; 4]) -> String {
    hex_string(&selector)
}

pub(crate) fn dns_encode_name(name: &str) -> Result<Vec<u8>> {
    let normalized = name.trim_matches('.');
    if normalized.is_empty() {
        return Ok(vec![0]);
    }

    let mut encoded = Vec::new();
    for label in normalized.split('.') {
        if label.is_empty() {
            bail!("ENS name {name} contains an empty label");
        }
        let bytes = label.as_bytes();
        if bytes.len() > 63 {
            bail!("ENS name {name} contains a label longer than 63 bytes");
        }
        encoded.push(bytes.len() as u8);
        encoded.extend_from_slice(bytes);
    }
    encoded.push(0);
    Ok(encoded)
}

pub(crate) fn namehash(name: &str) -> Result<[u8; 32]> {
    let normalized = name.trim_matches('.');
    let mut node = [0_u8; 32];
    if normalized.is_empty() {
        return Ok(node);
    }

    for label in normalized.split('.').rev() {
        if label.is_empty() {
            bail!("ENS name {name} contains an empty label");
        }
        let mut combined = [0_u8; 64];
        combined[..32].copy_from_slice(&node);
        combined[32..].copy_from_slice(keccak256(label.as_bytes()).as_slice());
        node.copy_from_slice(keccak256(combined).as_slice());
    }
    Ok(node)
}

pub(crate) fn resolver_calldata(
    selector: &SupportedVerifiedResolutionRecordKey,
    record_key: &str,
    node: [u8; 32],
) -> Result<Vec<u8>> {
    match selector {
        SupportedVerifiedResolutionRecordKey::Addr { coin_type } if coin_type == "60" => {
            let mut calldata = ADDR_SELECTOR.to_vec();
            calldata.extend_from_slice(&(B256::from(node),).abi_encode_params());
            Ok(calldata)
        }
        SupportedVerifiedResolutionRecordKey::Addr { coin_type } => {
            let coin_type = coin_type.parse::<u64>().with_context(|| {
                format!("record selector {record_key} has invalid numeric coin type")
            })?;
            let mut calldata = MULTICOIN_ADDR_SELECTOR.to_vec();
            calldata
                .extend_from_slice(&(B256::from(node), U256::from(coin_type)).abi_encode_params());
            Ok(calldata)
        }
        SupportedVerifiedResolutionRecordKey::Text => {
            let text_key = record_key
                .strip_prefix("text:")
                .filter(|value| !value.is_empty())
                .with_context(|| format!("record selector {record_key} is missing text key"))?;
            text_calldata(node, text_key)
        }
        SupportedVerifiedResolutionRecordKey::Avatar => text_calldata(node, "avatar"),
        SupportedVerifiedResolutionRecordKey::Contenthash => {
            let mut calldata = CONTENTHASH_SELECTOR.to_vec();
            calldata.extend_from_slice(&(B256::from(node),).abi_encode_params());
            Ok(calldata)
        }
    }
}

pub(crate) fn universal_resolver_calldata(dns_name: &[u8], resolver_data: &[u8]) -> Vec<u8> {
    let mut calldata = Vec::with_capacity(4);
    calldata.extend_from_slice(&UNIVERSAL_RESOLVER_RESOLVE_SELECTOR);
    calldata.extend_from_slice(&(dns_name, resolver_data).abi_encode_params());
    calldata
}

pub(crate) fn decode_universal_resolver_result(return_data: &[u8]) -> Result<Vec<u8>> {
    let (result, _) =
        <(sol_data::Bytes, sol_data::Address)>::abi_decode_params_validate(return_data)
            .context("Universal Resolver return data is malformed")?;
    Ok(result.to_vec())
}

pub(crate) fn decode_selector_result(
    selector: &SupportedVerifiedResolutionRecordKey,
    return_data: &[u8],
) -> Result<Option<String>> {
    match selector {
        SupportedVerifiedResolutionRecordKey::Addr { coin_type } if coin_type == "60" => {
            decode_abi_address(return_data)
        }
        SupportedVerifiedResolutionRecordKey::Addr { .. }
        | SupportedVerifiedResolutionRecordKey::Contenthash => {
            let bytes = decode_abi_dynamic_bytes(return_data)?;
            if bytes.is_empty() {
                Ok(None)
            } else {
                Ok(Some(hex_string(&bytes)))
            }
        }
        SupportedVerifiedResolutionRecordKey::Text
        | SupportedVerifiedResolutionRecordKey::Avatar => {
            let text = decode_abi_string(return_data)?;
            if text.is_empty() {
                Ok(None)
            } else {
                Ok(Some(text))
            }
        }
    }
}

pub(crate) fn hex_to_bytes(value: &str) -> Result<Vec<u8>> {
    let payload = value
        .strip_prefix("0x")
        .with_context(|| "hex value must start with 0x".to_owned())?;
    if payload.len() % 2 != 0 {
        bail!("hex value must contain an even number of digits");
    }

    hex::decode(payload).context("hex value contains non-hex digits")
}

pub(crate) fn hex_string(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

pub(crate) fn digest_json(value: &serde_json::Value) -> String {
    let bytes = serde_json::to_vec(value).unwrap_or_else(|_| value.to_string().into_bytes());
    format!("keccak256:{}", hex::encode(keccak256(&bytes)))
}

fn text_calldata(node: [u8; 32], text_key: &str) -> Result<Vec<u8>> {
    if text_key.is_empty() {
        bail!("text record key must not be empty");
    }
    let mut calldata = Vec::with_capacity(4);
    calldata.extend_from_slice(&TEXT_SELECTOR);
    calldata.extend_from_slice(&(B256::from(node), text_key).abi_encode_params());
    Ok(calldata)
}

fn decode_abi_address(return_data: &[u8]) -> Result<Option<String>> {
    let address = sol_data::Address::abi_decode_validate(return_data)
        .context("addr(bytes32) return data is malformed")?;
    if address == Address::ZERO {
        return Ok(None);
    }
    Ok(Some(hex_string(address.as_slice())))
}

fn decode_abi_dynamic_bytes(return_data: &[u8]) -> Result<Vec<u8>> {
    Ok(sol_data::Bytes::abi_decode_validate(return_data)
        .context("dynamic bytes return data is malformed")?
        .to_vec())
}

fn decode_abi_string(return_data: &[u8]) -> Result<String> {
    sol_data::String::abi_decode_validate(return_data)
        .context("string return data is not valid ABI string")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_dns_name() {
        assert_eq!(
            dns_encode_name("alice.eth").expect("name must encode"),
            b"\x05alice\x03eth\0".to_vec()
        );
    }

    #[test]
    fn encodes_universal_resolver_call_selector() {
        let dns_name = dns_encode_name("alice.eth").expect("name must encode");
        let resolver_data = vec![1, 2, 3, 4];
        let calldata = universal_resolver_calldata(&dns_name, &resolver_data);
        assert_eq!(&calldata[0..4], UNIVERSAL_RESOLVER_RESOLVE_SELECTOR);
        assert_eq!(&calldata[4 + 31..4 + 32], &[64]);
    }

    #[test]
    fn resolver_selectors_match_ens_profiles() {
        assert_eq!(selector_hex(ADDR_SELECTOR), "0x3b3b57de");
        assert_eq!(selector_hex(MULTICOIN_ADDR_SELECTOR), "0xf1cb7e06");
        assert_eq!(selector_hex(TEXT_SELECTOR), "0x59d1d43c");
        assert_eq!(selector_hex(CONTENTHASH_SELECTOR), "0xbc1c58d1");
    }

    #[test]
    fn decodes_addr60_zero_as_missing() {
        assert_eq!(
            decode_abi_address(&[0_u8; 32]).expect("address must decode"),
            None
        );
    }
}
