use alloy_primitives::{Address, B256, Bytes, U256, hex, keccak256};
use alloy_sol_types::{SolCall, sol};
use anyhow::{Context, Result, bail};
use bigname_storage::SupportedVerifiedResolutionRecordKey;

mod abi {
    use super::*;

    sol! {
        function resolve(bytes name, bytes data) external view returns (bytes result, address resolver);
        function addr(bytes32 node) external view returns (address);
        function addr(bytes32 node, uint256 coin_type) external view returns (bytes);
        function text(bytes32 node, string key) external view returns (string);
        function contenthash(bytes32 node) external view returns (bytes);
    }
}

pub(crate) const UNIVERSAL_RESOLVER_RESOLVE_SELECTOR: [u8; 4] = abi::resolveCall::SELECTOR;

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
            Ok(abi::addr_0Call {
                node: B256::from(node),
            }
            .abi_encode())
        }
        SupportedVerifiedResolutionRecordKey::Addr { coin_type } => {
            let coin_type = coin_type.parse::<u64>().with_context(|| {
                format!("record selector {record_key} has invalid numeric coin type")
            })?;
            Ok(abi::addr_1Call {
                node: B256::from(node),
                coin_type: U256::from(coin_type),
            }
            .abi_encode())
        }
        SupportedVerifiedResolutionRecordKey::Text => {
            let text_key = record_key
                .strip_prefix("text:")
                .filter(|value| !value.is_empty())
                .with_context(|| format!("record selector {record_key} is missing text key"))?;
            text_calldata(node, text_key)
        }
        SupportedVerifiedResolutionRecordKey::Avatar => text_calldata(node, "avatar"),
        SupportedVerifiedResolutionRecordKey::Contenthash => Ok(abi::contenthashCall {
            node: B256::from(node),
        }
        .abi_encode()),
    }
}

pub(crate) fn universal_resolver_calldata(dns_name: &[u8], resolver_data: &[u8]) -> Vec<u8> {
    abi::resolveCall {
        name: Bytes::copy_from_slice(dns_name),
        data: Bytes::copy_from_slice(resolver_data),
    }
    .abi_encode()
}

pub(crate) fn decode_universal_resolver_result(return_data: &[u8]) -> Result<Vec<u8>> {
    let result = abi::resolveCall::abi_decode_returns_validate(return_data)
        .context("Universal Resolver return data is malformed")?
        .result;
    Ok(result.to_vec())
}

pub(crate) fn decode_selector_result(
    selector: &SupportedVerifiedResolutionRecordKey,
    return_data: &[u8],
) -> Result<Option<String>> {
    match selector {
        SupportedVerifiedResolutionRecordKey::Addr { coin_type } if coin_type == "60" => {
            decode_addr60_result(return_data)
        }
        SupportedVerifiedResolutionRecordKey::Addr { .. } => {
            let bytes = abi::addr_1Call::abi_decode_returns_validate(return_data)
                .context("addr(bytes32,uint256) return data is malformed")?;
            if bytes.is_empty() {
                Ok(None)
            } else {
                Ok(Some(hex_string(&bytes)))
            }
        }
        SupportedVerifiedResolutionRecordKey::Contenthash => {
            let bytes = abi::contenthashCall::abi_decode_returns_validate(return_data)
                .context("contenthash(bytes32) return data is malformed")?;
            if bytes.is_empty() {
                Ok(None)
            } else {
                Ok(Some(hex_string(&bytes)))
            }
        }
        SupportedVerifiedResolutionRecordKey::Text
        | SupportedVerifiedResolutionRecordKey::Avatar => {
            let text = abi::textCall::abi_decode_returns_validate(return_data)
                .context("string return data is not valid ABI string")?;
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
    Ok(abi::textCall {
        node: B256::from(node),
        key: text_key.to_owned(),
    }
    .abi_encode())
}

fn decode_addr60_result(return_data: &[u8]) -> Result<Option<String>> {
    let address = abi::addr_0Call::abi_decode_returns_validate(return_data)
        .context("addr(bytes32) return data is malformed")?;
    if address == Address::ZERO {
        return Ok(None);
    }
    Ok(Some(hex_string(address.as_slice())))
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
        assert_eq!(selector_hex(abi::addr_0Call::SELECTOR), "0x3b3b57de");
        assert_eq!(selector_hex(abi::addr_1Call::SELECTOR), "0xf1cb7e06");
        assert_eq!(selector_hex(abi::textCall::SELECTOR), "0x59d1d43c");
        assert_eq!(selector_hex(abi::contenthashCall::SELECTOR), "0xbc1c58d1");
    }

    #[test]
    fn decodes_addr60_zero_as_missing() {
        assert_eq!(
            decode_addr60_result(&[0_u8; 32]).expect("address must decode"),
            None
        );
    }
}
