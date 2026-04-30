use anyhow::{Context, Result, bail};
use sha3::{Digest, Keccak256};

use super::constants::{
    NAME_WRAPPED_SIGNATURE, REGISTRAR_NAME_REGISTERED_SIGNATURE, REGISTRAR_NAME_RENEWED_SIGNATURE,
};

pub(super) fn decode_dynamic_bytes(data: &[u8], offset_word_index: usize) -> Result<Vec<u8>> {
    if data.len() < 64 {
        bail!("event data is too short to decode a dynamic bytes parameter");
    }

    let offset_word_start = offset_word_index
        .checked_mul(32)
        .context("ABI offset word index overflow")?;
    let offset_word_end = offset_word_start + 32;
    let offset_word = data
        .get(offset_word_start..offset_word_end)
        .with_context(|| format!("event data is missing ABI offset word {offset_word_index}"))?;
    let offset = word_to_usize(offset_word).context("invalid ABI offset for dynamic bytes")?;
    if data.len() < offset + 32 {
        bail!("event data does not contain the dynamic bytes length word");
    }
    let byte_length = word_to_usize(&data[offset..offset + 32])
        .context("invalid ABI length for dynamic bytes")?;
    let bytes_start = offset + 32;
    let bytes_end = bytes_start + byte_length;
    if data.len() < bytes_end {
        bail!("event data does not contain the full dynamic bytes payload");
    }

    Ok(data[bytes_start..bytes_end].to_vec())
}

pub(super) fn decode_dynamic_string(data: &[u8], offset_word_index: usize) -> Result<String> {
    String::from_utf8(decode_dynamic_bytes(data, offset_word_index)?)
        .context("dynamic string payload is not valid UTF-8")
}

fn word_to_usize(word: &[u8]) -> Result<usize> {
    if word.len() != 32 {
        bail!("ABI word must be exactly 32 bytes");
    }
    if word[..24].iter().any(|byte| *byte != 0) {
        bail!("ABI word exceeds supported usize width");
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&word[24..32]);
    usize::try_from(u64::from_be_bytes(bytes)).context("ABI word does not fit in usize")
}

pub(super) fn name_wrapped_topic0() -> String {
    keccak256_hex(NAME_WRAPPED_SIGNATURE.as_bytes())
}

pub(super) fn registrar_name_registered_topic0() -> String {
    keccak256_hex(REGISTRAR_NAME_REGISTERED_SIGNATURE.as_bytes())
}

pub(super) fn registrar_name_renewed_topic0() -> String {
    keccak256_hex(REGISTRAR_NAME_RENEWED_SIGNATURE.as_bytes())
}

pub(super) fn keccak_signature_hex(signature: &str) -> String {
    keccak256_hex(signature.as_bytes())
}

pub(super) fn namehash_hex(labels: &[Vec<u8>]) -> String {
    let mut node = [0u8; 32];
    for label in labels.iter().rev() {
        let label_hash = keccak256_bytes(label);
        let mut combined = [0u8; 64];
        combined[..32].copy_from_slice(&node);
        combined[32..].copy_from_slice(&label_hash);
        node = keccak256_bytes(&combined);
    }
    hex_string(&node)
}

fn keccak256_bytes(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Keccak256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut output = [0u8; 32];
    output.copy_from_slice(&digest);
    output
}

pub(super) fn keccak256_hex(bytes: &[u8]) -> String {
    hex_string(&keccak256_bytes(bytes))
}

pub(super) fn hex_string(bytes: &[u8]) -> String {
    let mut output = String::from("0x");
    for byte in bytes {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}

pub(super) fn hex_string_without_prefix(bytes: &[u8]) -> String {
    let mut output = String::new();
    for byte in bytes {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}
