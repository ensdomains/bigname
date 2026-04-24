use anyhow::{Context, Result, bail};

use super::{
    constants::*,
    types::{RegistryObservation, RegistryRawLogRow},
    util::{hex_string, keccak_signature_hex},
};

pub(super) fn build_registry_observation(
    raw_log: &RegistryRawLogRow,
) -> Result<Option<RegistryObservation>> {
    let Some(topic0) = raw_log.topics.first() else {
        return Ok(None);
    };
    let reference = raw_log.reference();

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(LABEL_REGISTERED_SIGNATURE)) {
        let token_id = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("LabelRegistered missing tokenId topic")?,
        )?;
        let labelhash = normalize_hex_32(
            raw_log
                .topics
                .get(2)
                .context("LabelRegistered missing labelHash topic")?,
        )?;
        let sender = normalize_topic_address(
            raw_log
                .topics
                .get(3)
                .context("LabelRegistered missing sender topic")?,
        )?;
        let label = decode_dynamic_string(&raw_log.data, 0)?;
        let owner = decode_address_word(&raw_log.data, 1)?;
        let expiry = decode_u64_word(&raw_log.data, 2)?;
        return Ok(Some(RegistryObservation::LabelRegistered {
            token_id,
            labelhash,
            label,
            owner,
            expiry,
            sender,
            reference,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(LABEL_RESERVED_SIGNATURE)) {
        let token_id = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("LabelReserved missing tokenId topic")?,
        )?;
        let labelhash = normalize_hex_32(
            raw_log
                .topics
                .get(2)
                .context("LabelReserved missing labelHash topic")?,
        )?;
        let sender = normalize_topic_address(
            raw_log
                .topics
                .get(3)
                .context("LabelReserved missing sender topic")?,
        )?;
        let label = decode_dynamic_string(&raw_log.data, 0)?;
        let expiry = decode_u64_word(&raw_log.data, 1)?;
        return Ok(Some(RegistryObservation::LabelReserved {
            token_id,
            labelhash,
            label,
            expiry,
            sender,
            reference,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(LABEL_UNREGISTERED_SIGNATURE)) {
        let token_id = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("LabelUnregistered missing tokenId topic")?,
        )?;
        let sender = normalize_topic_address(
            raw_log
                .topics
                .get(2)
                .context("LabelUnregistered missing sender topic")?,
        )?;
        return Ok(Some(RegistryObservation::LabelUnregistered {
            token_id,
            sender,
            reference,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(EXPIRY_UPDATED_SIGNATURE)) {
        let token_id = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("ExpiryUpdated missing tokenId topic")?,
        )?;
        let new_expiry = decode_u64_topic(
            raw_log
                .topics
                .get(2)
                .context("ExpiryUpdated missing newExpiry topic")?,
        )?;
        let sender = normalize_topic_address(
            raw_log
                .topics
                .get(3)
                .context("ExpiryUpdated missing sender topic")?,
        )?;
        return Ok(Some(RegistryObservation::ExpiryUpdated {
            token_id,
            new_expiry,
            sender,
            reference,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(SUBREGISTRY_UPDATED_SIGNATURE)) {
        let token_id = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("SubregistryUpdated missing tokenId topic")?,
        )?;
        let subregistry = normalize_topic_address(
            raw_log
                .topics
                .get(2)
                .context("SubregistryUpdated missing subregistry topic")?,
        )?;
        let sender = normalize_topic_address(
            raw_log
                .topics
                .get(3)
                .context("SubregistryUpdated missing sender topic")?,
        )?;
        return Ok(Some(RegistryObservation::SubregistryUpdated {
            token_id,
            subregistry,
            sender,
            reference,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(RESOLVER_UPDATED_SIGNATURE)) {
        let token_id = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("ResolverUpdated missing tokenId topic")?,
        )?;
        let resolver = normalize_topic_address(
            raw_log
                .topics
                .get(2)
                .context("ResolverUpdated missing resolver topic")?,
        )?;
        let sender = normalize_topic_address(
            raw_log
                .topics
                .get(3)
                .context("ResolverUpdated missing sender topic")?,
        )?;
        return Ok(Some(RegistryObservation::ResolverUpdated {
            token_id,
            resolver,
            sender,
            reference,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(TOKEN_RESOURCE_SIGNATURE)) {
        let token_id = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("TokenResource missing tokenId topic")?,
        )?;
        let upstream_resource = normalize_hex_32(
            raw_log
                .topics
                .get(2)
                .context("TokenResource missing resource topic")?,
        )?;
        return Ok(Some(RegistryObservation::TokenResource {
            token_id,
            upstream_resource,
            reference,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(TOKEN_REGENERATED_SIGNATURE)) {
        let old_token_id = normalize_hex_32(
            raw_log
                .topics
                .get(1)
                .context("TokenRegenerated missing oldTokenId topic")?,
        )?;
        let new_token_id = normalize_hex_32(
            raw_log
                .topics
                .get(2)
                .context("TokenRegenerated missing newTokenId topic")?,
        )?;
        return Ok(Some(RegistryObservation::TokenRegenerated {
            old_token_id,
            new_token_id,
            reference,
        }));
    }

    if topic0.eq_ignore_ascii_case(&keccak_signature_hex(PARENT_UPDATED_SIGNATURE)) {
        let parent = normalize_topic_address(
            raw_log
                .topics
                .get(1)
                .context("ParentUpdated missing parent topic")?,
        )?;
        let sender = normalize_topic_address(
            raw_log
                .topics
                .get(2)
                .context("ParentUpdated missing sender topic")?,
        )?;
        let label = decode_dynamic_string(&raw_log.data, 0)?;
        return Ok(Some(RegistryObservation::ParentUpdated {
            parent,
            label,
            sender,
            reference,
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
    let word = word_at(data, word_index)?;
    decode_usize(word)
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

fn decode_u64_topic(value: &str) -> Result<i64> {
    let bytes = decode_hex_32(value)?;
    if bytes[..24].iter().any(|byte| *byte != 0) {
        bail!("indexed u64 topic exceeds supported width");
    }
    let mut tail = [0u8; 8];
    tail.copy_from_slice(&bytes[24..32]);
    i64::try_from(u64::from_be_bytes(tail)).context("indexed u64 topic does not fit in i64")
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

fn decode_hex_32(value: &str) -> Result<[u8; 32]> {
    let normalized = normalize_hex_32(value)?;
    let mut output = [0u8; 32];
    for (index, chunk) in normalized.as_bytes()[2..].chunks(2).enumerate() {
        let hex = std::str::from_utf8(chunk).context("hex chunk must be UTF-8")?;
        output[index] =
            u8::from_str_radix(hex, 16).with_context(|| format!("invalid hex byte {hex}"))?;
    }
    Ok(output)
}

fn normalize_topic_address(value: &str) -> Result<String> {
    let normalized = normalize_hex_32(value)?;
    Ok(format!("0x{}", &normalized[26..]))
}
