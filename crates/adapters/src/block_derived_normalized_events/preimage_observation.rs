use anyhow::{Context, Result, bail};

use super::decoding::{hex_string, keccak256_hex, namehash_hex};
use super::types::PreimageObservation;

pub(super) fn observe_dns_encoded_name(bytes: &[u8]) -> Result<PreimageObservation> {
    if bytes.is_empty() {
        bail!("dns-encoded name payload must not be empty");
    }

    let mut labels = Vec::<Vec<u8>>::new();
    let mut cursor = 0usize;
    loop {
        if cursor >= bytes.len() {
            bail!("dns-encoded name payload is missing the root terminator");
        }
        let label_length = usize::from(bytes[cursor]);
        cursor += 1;
        if label_length == 0 {
            if cursor != bytes.len() {
                bail!("dns-encoded name payload has trailing bytes after the root terminator");
            }
            break;
        }
        if cursor + label_length > bytes.len() {
            bail!("dns-encoded name label exceeds the available payload");
        }
        labels.push(bytes[cursor..cursor + label_length].to_vec());
        cursor += label_length;
    }

    let decoded_labels = labels
        .iter()
        .map(|label| String::from_utf8(label.clone()))
        .collect::<std::result::Result<Vec<_>, _>>()
        .ok();
    let labelhashes = labels
        .iter()
        .map(|label| keccak256_hex(label))
        .collect::<Vec<_>>();
    let namehash = namehash_hex(&labels);

    Ok(PreimageObservation {
        dns_encoded_name: hex_string(bytes),
        decoded_name: decoded_labels.map(|labels| labels.join(".")),
        labelhashes,
        namehash,
    })
}

pub(super) fn observe_registrar_eth_name(label: &str) -> Result<PreimageObservation> {
    if label.is_empty() {
        bail!("registrar label must not be empty");
    }

    let label_length =
        u8::try_from(label.len()).context("registrar label exceeds supported DNS label length")?;
    let mut dns_name = Vec::with_capacity(label.len() + 6);
    dns_name.push(label_length);
    dns_name.extend_from_slice(label.as_bytes());
    dns_name.push(3);
    dns_name.extend_from_slice(b"eth");
    dns_name.push(0);

    observe_dns_encoded_name(&dns_name)
}

pub(super) fn observe_single_label(label: &str) -> Result<PreimageObservation> {
    if label.is_empty() {
        bail!("label must not be empty");
    }

    let label_length = u8::try_from(label.len()).context("label exceeds supported DNS length")?;
    let mut dns_name = Vec::with_capacity(label.len() + 2);
    dns_name.push(label_length);
    dns_name.extend_from_slice(label.as_bytes());
    dns_name.push(0);

    observe_dns_encoded_name(&dns_name)
}
