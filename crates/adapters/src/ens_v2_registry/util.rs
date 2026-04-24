use std::time::Duration;

use anyhow::{Context, Result, bail};
use bigname_storage::CanonicalityState;
use serde_json::Value;
use sha3::{Digest, Keccak256};
use sqlx::types::{Uuid, time::OffsetDateTime};

use super::{constants::ZERO_ADDRESS, types::ObservationRef};

pub(super) fn event_position_timestamp(reference: &ObservationRef) -> OffsetDateTime {
    let offset_micros = reference
        .transaction_index
        .saturating_mul(1_000)
        .saturating_add(reference.log_index.max(0));
    reference.block_timestamp + Duration::from_micros(offset_micros.max(0) as u64)
}

pub(super) fn normalize_address(value: &str) -> String {
    value.to_ascii_lowercase()
}

pub(super) fn null_if_zero_address(value: &str) -> Value {
    if normalize_address(value) == ZERO_ADDRESS {
        Value::Null
    } else {
        Value::String(normalize_address(value))
    }
}

pub(super) fn parse_canonicality_state(value: &str) -> Result<CanonicalityState> {
    match value {
        "observed" => Ok(CanonicalityState::Observed),
        "canonical" => Ok(CanonicalityState::Canonical),
        "safe" => Ok(CanonicalityState::Safe),
        "finalized" => Ok(CanonicalityState::Finalized),
        "orphaned" => Ok(CanonicalityState::Orphaned),
        _ => bail!("unknown canonicality_state value {value}"),
    }
}

pub(super) fn dns_encode(labels: &[Vec<u8>]) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    for label in labels {
        let length = u8::try_from(label.len()).context("label exceeds DNS label length")?;
        if length == 0 {
            bail!("empty label is not encodable");
        }
        output.push(length);
        output.extend_from_slice(label);
    }
    output.push(0);
    Ok(output)
}

pub(super) fn display_name(name: &str) -> String {
    let mut labels = name.split('.');
    let Some(first) = labels.next() else {
        return name.to_owned();
    };
    let mut first_chars = first.chars();
    let display_first = match first_chars.next() {
        Some(first_char) => format!(
            "{}{}",
            first_char.to_uppercase(),
            first_chars.as_str().to_ascii_lowercase()
        ),
        None => first.to_owned(),
    };
    std::iter::once(display_first)
        .chain(labels.map(|label| label.to_ascii_lowercase()))
        .collect::<Vec<_>>()
        .join(".")
}

pub(super) fn namehash_bytes(labels: &[Vec<u8>]) -> [u8; 32] {
    let mut node = [0u8; 32];
    for label in labels.iter().rev() {
        let label_hash = keccak256_bytes(label);
        let mut combined = [0u8; 64];
        combined[..32].copy_from_slice(&node);
        combined[32..].copy_from_slice(&label_hash);
        node = keccak256_bytes(&combined);
    }
    node
}

pub(super) fn keccak_signature_hex(signature: &str) -> String {
    format!("0x{}", hex_string(keccak256_bytes(signature.as_bytes())))
}

pub(super) fn keccak256_bytes(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Keccak256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut output = [0u8; 32];
    output.copy_from_slice(&digest);
    output
}

pub(super) fn deterministic_uuid(seed: &str) -> Uuid {
    let mut digest = Keccak256::new();
    digest.update(seed.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.finalize()[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

pub(super) fn hex_string(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
