use anyhow::{Context, Result, bail};
use sha3::{Digest, Keccak256};

use super::{NAME_REGISTERED_SIGNATURE, NAME_RENEWED_SIGNATURE, raw_logs::RegistrarRawLogRow};

pub(super) enum RegistrarObservation {
    NameRegistered {
        token_id: String,
        label: String,
        owner: String,
        subregistry: String,
        resolver: String,
        duration: i64,
        payment_token: String,
        referrer: String,
        base: String,
        premium: String,
    },
    NameRenewed {
        token_id: String,
        label: String,
        duration: i64,
        new_expiry: i64,
        payment_token: String,
        referrer: String,
        base: String,
    },
}

pub(super) fn build_registrar_observation(
    raw_log: &RegistrarRawLogRow,
) -> Result<Option<RegistrarObservation>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(None);
    };

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(NAME_REGISTERED_SIGNATURE)) {
        let token_id = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("NameRegistered missing tokenId topic")?,
        )?;
        return Ok(Some(RegistrarObservation::NameRegistered {
            token_id,
            label: decode_dynamic_string(&raw_log.data, 0)?,
            owner: decode_address_word(&raw_log.data, 1)?,
            subregistry: decode_address_word(&raw_log.data, 2)?,
            resolver: decode_address_word(&raw_log.data, 3)?,
            duration: decode_u64_word(&raw_log.data, 4)?,
            payment_token: decode_address_word(&raw_log.data, 5)?,
            referrer: format!("0x{}", hex_string(word_at(&raw_log.data, 6)?)),
            base: normalize_word_hex(word_at(&raw_log.data, 7)?),
            premium: normalize_word_hex(word_at(&raw_log.data, 8)?),
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(NAME_RENEWED_SIGNATURE)) {
        let token_id = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("NameRenewed missing tokenId topic")?,
        )?;
        return Ok(Some(RegistrarObservation::NameRenewed {
            token_id,
            label: decode_dynamic_string(&raw_log.data, 0)?,
            duration: decode_u64_word(&raw_log.data, 1)?,
            new_expiry: decode_u64_word(&raw_log.data, 2)?,
            payment_token: decode_address_word(&raw_log.data, 3)?,
            referrer: format!("0x{}", hex_string(word_at(&raw_log.data, 4)?)),
            base: normalize_word_hex(word_at(&raw_log.data, 5)?),
        }));
    }

    Ok(None)
}

fn decode_dynamic_string(data: &[u8], offset_word_index: usize) -> Result<String> {
    let offset = decode_usize_word(data, offset_word_index)?;
    if data.len() < offset + 32 {
        bail!("dynamic string payload is missing length word");
    }
    let length = decode_usize_at(data, offset)?;
    let start = offset + 32;
    let end = start + length;
    if data.len() < end {
        bail!("dynamic string payload is shorter than declared length");
    }
    String::from_utf8(data[start..end].to_vec()).context("dynamic string is not valid UTF-8")
}

fn decode_address_word(data: &[u8], word_index: usize) -> Result<String> {
    let word = word_at(data, word_index)?;
    Ok(format!("0x{}", hex_string(&word[12..32])))
}

fn decode_u64_word(data: &[u8], word_index: usize) -> Result<i64> {
    let word = word_at(data, word_index)?;
    if word[..24].iter().any(|byte| *byte != 0) {
        bail!("u64 ABI word exceeds supported width");
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&word[24..32]);
    i64::try_from(u64::from_be_bytes(bytes)).context("u64 ABI word does not fit in i64")
}

fn decode_usize_word(data: &[u8], word_index: usize) -> Result<usize> {
    decode_usize(word_at(data, word_index)?)
}

fn decode_usize_at(data: &[u8], offset: usize) -> Result<usize> {
    if data.len() < offset + 32 {
        bail!("ABI word offset is outside payload");
    }
    decode_usize(&data[offset..offset + 32])
}

fn decode_usize(word: &[u8]) -> Result<usize> {
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

fn word_at(data: &[u8], word_index: usize) -> Result<&[u8]> {
    let start = word_index
        .checked_mul(32)
        .context("ABI word index overflow")?;
    let end = start + 32;
    data.get(start..end)
        .with_context(|| format!("ABI data missing word {word_index}"))
}

fn normalize_word_hex(word: &[u8]) -> String {
    format!("0x{}", hex_string(word))
}

fn normalize_hex_32(value: &str) -> Result<String> {
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

pub(super) fn normalize_address(value: &str) -> String {
    value.to_ascii_lowercase()
}

fn keccak_signature_hex(signature: &str) -> String {
    format!("0x{}", hex_string(keccak256_bytes(signature.as_bytes())))
}

fn keccak256_bytes(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Keccak256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut output = [0u8; 32];
    output.copy_from_slice(&digest);
    output
}

pub(super) fn hex_string(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
