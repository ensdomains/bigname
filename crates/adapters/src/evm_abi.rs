use alloy_primitives::{Address, U256, hex};
use anyhow::{Context, Result, bail};

const ABI_WORD_BYTES: usize = 32;

pub(crate) fn dynamic_string(data: &[u8], offset_word_index: usize) -> Result<String> {
    String::from_utf8(dynamic_bytes(data, offset_word_index)?)
        .context("dynamic string payload is not valid UTF-8")
}

pub(crate) fn dynamic_bytes(data: &[u8], offset_word_index: usize) -> Result<Vec<u8>> {
    let offset = usize_word(data, offset_word_index).context("invalid ABI dynamic offset")?;
    let length = usize_at(data, offset).context("invalid ABI dynamic length")?;
    let start = offset
        .checked_add(ABI_WORD_BYTES)
        .context("ABI dynamic payload start overflow")?;
    let end = start
        .checked_add(length)
        .context("ABI dynamic payload end overflow")?;
    if data.len() < end {
        bail!("ABI data is shorter than the declared dynamic payload");
    }
    Ok(data[start..end].to_vec())
}

pub(crate) fn address_word_hex(data: &[u8], word_index: usize) -> Result<String> {
    address_hex_from_word(word_at(data, word_index)?)
}

pub(crate) fn address_hex_from_word(word: &[u8]) -> Result<String> {
    let word = exact_word(word)?;
    let address = Address::from_slice(&word[12..]);
    Ok(format!("0x{}", hex::encode(address.as_slice())))
}

pub(crate) fn topic_address_hex(value: &str) -> Result<String> {
    address_hex_from_word(&hex_32(value)?)
}

pub(crate) fn word_hex(word: &[u8]) -> Result<String> {
    Ok(format!("0x{}", hex::encode(exact_word(word)?)))
}

pub(crate) fn u64_word(data: &[u8], word_index: usize) -> Result<i64> {
    i64_from_u64_word(word_at(data, word_index)?)
}

pub(crate) fn u64_topic(value: &str) -> Result<i64> {
    i64_from_u64_word(&hex_32(value)?)
}

pub(crate) fn i64_from_u64_word(word: &[u8]) -> Result<i64> {
    let word = exact_word(word)?;
    if word[..24].iter().any(|byte| *byte != 0) {
        bail!("u64 ABI word exceeds supported width");
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&word[24..]);
    i64::try_from(u64::from_be_bytes(bytes)).context("u64 ABI word does not fit in i64")
}

pub(crate) fn usize_from_word(word: &[u8]) -> Result<usize> {
    let word = exact_word(word)?;
    if word[..24].iter().any(|byte| *byte != 0) {
        bail!("ABI word exceeds supported usize width");
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&word[24..]);
    usize::try_from(u64::from_be_bytes(bytes)).context("ABI word does not fit in usize")
}

pub(crate) fn u256_word_decimal(data: &[u8], word_index: usize) -> Result<String> {
    Ok(U256::from_be_bytes(*word_at(data, word_index)?).to_string())
}

pub(crate) fn u256_topic_decimal(value: &str) -> Result<String> {
    Ok(U256::from_be_bytes(hex_32(value)?).to_string())
}

pub(crate) fn word_at(data: &[u8], word_index: usize) -> Result<&[u8; ABI_WORD_BYTES]> {
    let offset = word_index
        .checked_mul(ABI_WORD_BYTES)
        .context("ABI word index overflow")?;
    word_at_offset(data, offset)
}

pub(crate) fn word_at_offset(data: &[u8], offset: usize) -> Result<&[u8; ABI_WORD_BYTES]> {
    let end = offset
        .checked_add(ABI_WORD_BYTES)
        .context("ABI word offset overflow")?;
    let word = data
        .get(offset..end)
        .with_context(|| format!("ABI data missing word at byte offset {offset}"))?;
    exact_word(word)
}

pub(crate) fn hex_32(value: &str) -> Result<[u8; ABI_WORD_BYTES]> {
    let normalized = normalize_hex_32(value)?;
    let mut output = [0u8; ABI_WORD_BYTES];
    hex::decode_to_slice(&normalized[2..], &mut output)
        .with_context(|| format!("invalid 32-byte hex value {normalized}"))?;
    Ok(output)
}

pub(crate) fn normalize_hex_32(value: &str) -> Result<String> {
    let normalized = value.to_ascii_lowercase();
    let normalized = if normalized.starts_with("0x") {
        normalized
    } else {
        format!("0x{normalized}")
    };
    if normalized.len() != 66 {
        bail!("expected 32-byte hex value, got {normalized}");
    }
    Ok(normalized)
}

fn usize_word(data: &[u8], word_index: usize) -> Result<usize> {
    usize_from_word(word_at(data, word_index)?)
}

fn usize_at(data: &[u8], offset: usize) -> Result<usize> {
    usize_from_word(word_at_offset(data, offset)?)
}

fn exact_word(word: &[u8]) -> Result<&[u8; ABI_WORD_BYTES]> {
    if word.len() != ABI_WORD_BYTES {
        bail!("ABI word must be exactly 32 bytes");
    }
    word.try_into().context("ABI word must be exactly 32 bytes")
}
