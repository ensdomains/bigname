use anyhow::{Context, Result, bail};
use bigname_storage::SupportedVerifiedResolutionRecordKey;
use sha3::{Digest, Keccak256};

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
        let label_hash = keccak256(label.as_bytes());
        let mut combined = [0_u8; 64];
        combined[..32].copy_from_slice(&node);
        combined[32..].copy_from_slice(&label_hash);
        node = keccak256(&combined);
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
            let mut calldata = Vec::with_capacity(4 + 32);
            calldata.extend_from_slice(&ADDR_SELECTOR);
            calldata.extend_from_slice(&node);
            Ok(calldata)
        }
        SupportedVerifiedResolutionRecordKey::Addr { coin_type } => {
            let coin_type = coin_type.parse::<u64>().with_context(|| {
                format!("record selector {record_key} has invalid numeric coin type")
            })?;
            let mut calldata = Vec::with_capacity(4 + 64);
            calldata.extend_from_slice(&MULTICOIN_ADDR_SELECTOR);
            calldata.extend_from_slice(&node);
            calldata.extend_from_slice(&u256_word(coin_type));
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
            let mut calldata = Vec::with_capacity(4 + 32);
            calldata.extend_from_slice(&CONTENTHASH_SELECTOR);
            calldata.extend_from_slice(&node);
            Ok(calldata)
        }
    }
}

pub(crate) fn universal_resolver_calldata(dns_name: &[u8], resolver_data: &[u8]) -> Vec<u8> {
    let mut calldata = Vec::new();
    calldata.extend_from_slice(&UNIVERSAL_RESOLVER_RESOLVE_SELECTOR);
    calldata.extend_from_slice(&u256_word(64));
    let name_tail_len = padded_dynamic_len(dns_name.len());
    calldata.extend_from_slice(&u256_word((64 + name_tail_len) as u64));
    calldata.extend_from_slice(&abi_bytes_tail(dns_name));
    calldata.extend_from_slice(&abi_bytes_tail(resolver_data));
    calldata
}

pub(crate) fn decode_universal_resolver_result(return_data: &[u8]) -> Result<Vec<u8>> {
    if return_data.len() < 64 {
        bail!("Universal Resolver return data is shorter than the ABI tuple head");
    }
    let result_offset = word_to_usize(&return_data[0..32])
        .context("Universal Resolver result offset is invalid")?;
    decode_abi_bytes_at(return_data, result_offset)
        .context("Universal Resolver result bytes are malformed")
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
    let hex = value
        .strip_prefix("0x")
        .with_context(|| "hex value must start with 0x".to_owned())?;
    if hex.len() % 2 != 0 {
        bail!("hex value must contain an even number of digits");
    }

    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for chunk in hex.as_bytes().chunks(2) {
        let text = std::str::from_utf8(chunk).context("hex value contains invalid UTF-8")?;
        bytes.push(u8::from_str_radix(text, 16).context("hex value contains non-hex digits")?);
    }
    Ok(bytes)
}

pub(crate) fn hex_string(bytes: &[u8]) -> String {
    let mut output = String::from("0x");
    for byte in bytes {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}

pub(crate) fn digest_json(value: &serde_json::Value) -> String {
    let bytes = serde_json::to_vec(value).unwrap_or_else(|_| value.to_string().into_bytes());
    format!("keccak256:{}", hex_string_no_prefix(&keccak256(&bytes)))
}

fn text_calldata(node: [u8; 32], text_key: &str) -> Result<Vec<u8>> {
    if text_key.is_empty() {
        bail!("text record key must not be empty");
    }
    let mut calldata = Vec::new();
    calldata.extend_from_slice(&TEXT_SELECTOR);
    calldata.extend_from_slice(&node);
    calldata.extend_from_slice(&u256_word(64));
    calldata.extend_from_slice(&abi_bytes_tail(text_key.as_bytes()));
    Ok(calldata)
}

fn decode_abi_address(return_data: &[u8]) -> Result<Option<String>> {
    if return_data.len() < 32 {
        bail!("addr(bytes32) return data is shorter than one ABI word");
    }
    if return_data[0..12].iter().any(|byte| *byte != 0) {
        bail!("addr(bytes32) return data is not an ABI-encoded address");
    }
    let address = &return_data[12..32];
    if address.iter().all(|byte| *byte == 0) {
        return Ok(None);
    }
    Ok(Some(hex_string(address)))
}

fn decode_abi_dynamic_bytes(return_data: &[u8]) -> Result<Vec<u8>> {
    if return_data.len() < 64 {
        bail!("dynamic bytes return data is shorter than the ABI head");
    }
    let offset = word_to_usize(&return_data[0..32]).context("dynamic bytes offset is invalid")?;
    decode_abi_bytes_at(return_data, offset)
}

fn decode_abi_string(return_data: &[u8]) -> Result<String> {
    String::from_utf8(decode_abi_dynamic_bytes(return_data)?)
        .context("string return data is not valid UTF-8")
}

fn decode_abi_bytes_at(data: &[u8], offset: usize) -> Result<Vec<u8>> {
    if data.len() < offset + 32 {
        bail!("ABI bytes value is missing its length word");
    }
    let len = word_to_usize(&data[offset..offset + 32]).context("ABI bytes length is invalid")?;
    let start = offset + 32;
    let end = start
        .checked_add(len)
        .context("ABI bytes length overflows usize")?;
    if data.len() < end {
        bail!("ABI bytes value is shorter than its declared length");
    }
    Ok(data[start..end].to_vec())
}

fn abi_bytes_tail(bytes: &[u8]) -> Vec<u8> {
    let mut tail = Vec::new();
    tail.extend_from_slice(&u256_word(bytes.len() as u64));
    tail.extend_from_slice(bytes);
    let padding = (32 - (bytes.len() % 32)) % 32;
    tail.extend(std::iter::repeat_n(0_u8, padding));
    tail
}

fn padded_dynamic_len(len: usize) -> usize {
    32 + len + ((32 - (len % 32)) % 32)
}

fn u256_word(value: u64) -> [u8; 32] {
    let mut word = [0_u8; 32];
    word[24..32].copy_from_slice(&value.to_be_bytes());
    word
}

fn word_to_usize(word: &[u8]) -> Result<usize> {
    if word.len() != 32 {
        bail!("ABI word must be 32 bytes");
    }
    if word[..24].iter().any(|byte| *byte != 0) {
        bail!("ABI word exceeds supported usize width");
    }
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&word[24..32]);
    usize::try_from(u64::from_be_bytes(bytes)).context("ABI word does not fit in usize")
}

fn keccak256(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Keccak256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut output = [0_u8; 32];
    output.copy_from_slice(&digest);
    output
}

fn hex_string_no_prefix(bytes: &[u8]) -> String {
    let mut output = String::new();
    for byte in bytes {
        output.push_str(&format!("{byte:02x}"));
    }
    output
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
