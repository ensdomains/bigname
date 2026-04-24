use anyhow::{Result, bail};
use sha3::{Digest, Keccak256};

use super::{
    REVERSE_CLAIMED_SIGNATURE, SOURCE_FAMILY_BASENAMES_BASE_PRIMARY,
    SOURCE_FAMILY_ENS_V1_REVERSE_L1,
};

pub(super) fn supports_reverse_claim_source_family(source_family: &str) -> bool {
    matches!(
        source_family,
        SOURCE_FAMILY_ENS_V1_REVERSE_L1 | SOURCE_FAMILY_BASENAMES_BASE_PRIMARY
    )
}

pub(super) fn normalize_address(value: &str) -> Result<String> {
    let normalized = value.to_ascii_lowercase();
    if !normalized.starts_with("0x") || normalized.len() != 42 {
        bail!("expected 20-byte address, got {value}");
    }
    Ok(normalized)
}

pub(super) fn reverse_label_for_address(address: &str) -> Result<String> {
    Ok(normalize_address(address)?
        .trim_start_matches("0x")
        .to_owned())
}

pub(super) fn reverse_node_for_address(address: &str) -> Result<String> {
    let reverse_label = reverse_label_for_address(address)?;
    Ok(namehash_hex(&[
        reverse_label.into_bytes(),
        b"addr".to_vec(),
        b"reverse".to_vec(),
    ]))
}

pub(super) fn normalize_hex_32(value: &str) -> Result<String> {
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

pub(super) fn normalize_topic_address(value: &str) -> Result<String> {
    let normalized = normalize_hex_32(value)?;
    Ok(format!("0x{}", &normalized[26..]))
}

pub(super) fn reverse_claimed_topic0() -> String {
    keccak256_hex(REVERSE_CLAIMED_SIGNATURE.as_bytes())
}

pub(super) fn namehash_hex(labels: &[Vec<u8>]) -> String {
    let mut node = [0u8; 32];
    for label in labels.iter().rev() {
        let label_hash = {
            let mut digest = Keccak256::new();
            digest.update(label);
            let output = digest.finalize();
            let mut bytes = [0u8; 32];
            bytes.copy_from_slice(&output);
            bytes
        };
        let mut digest = Keccak256::new();
        digest.update(node);
        digest.update(label_hash);
        let output = digest.finalize();
        node.copy_from_slice(&output);
    }

    hex_string(&node)
}

pub(super) fn keccak256_hex(bytes: &[u8]) -> String {
    let mut digest = Keccak256::new();
    digest.update(bytes);
    hex_string(&digest.finalize())
}

pub(super) fn hex_string(bytes: &[u8]) -> String {
    let mut output = String::from("0x");
    for byte in bytes {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}
