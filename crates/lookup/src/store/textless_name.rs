//! The DNS wire name of a name whose surface stores no raw bytes.
//!
//! Such a surface carries only its label-hash path. A resolver call needs the bytes of every
//! label, so the wire name is built only when `label_preimages` holds, for each label of the
//! path, bytes that hash to it and that passed normalization. Otherwise there is nothing honest
//! to send and the caller refuses the lookup before any provider call.
use alloy_primitives::{B256, keccak256};
use sqlx::{Postgres, Transaction};

use crate::{Result, error::database};

/// The wire name of the label-hash path `labelhashes` (leaf first), or `None` when a label has no
/// verified preimage or the labels do not hash to `namehash`.
pub(super) async fn dns_name_from_preimages(
    transaction: &mut Transaction<'_, Postgres>,
    labelhashes: &[String],
    namehash: &str,
) -> Result<Option<Vec<u8>>> {
    let labels: Vec<Option<Vec<u8>>> = sqlx::query_scalar(
        "SELECT preimage.raw_label
         FROM unnest($1::text[]) WITH ORDINALITY AS path(labelhash, position)
         LEFT JOIN label_preimages preimage
           ON preimage.labelhash = lower(path.labelhash)
          AND preimage.decoded_label IS NOT NULL AND preimage.normalized_under_version
         ORDER BY path.position",
    )
    .bind(labelhashes)
    .fetch_all(&mut **transaction)
    .await
    .map_err(database(
        "load the label bytes of a name without stored bytes",
    ))?;
    Ok(labels
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .and_then(|labels| dns_name_from_labels(&labels, namehash)))
}

/// The wire name of `labels` (leaf first) when they hash to `namehash` and each fits the
/// one-byte length a DNS label has.
fn dns_name_from_labels(labels: &[Vec<u8>], namehash: &str) -> Option<Vec<u8>> {
    if labels.is_empty() {
        return None;
    }
    let node = labels.iter().rev().fold(B256::ZERO, |node, label| {
        keccak256([node.as_slice(), keccak256(label).as_slice()].concat())
    });
    if namehash.parse::<B256>().ok()? != node {
        return None;
    }
    let mut dns_name = Vec::new();
    for label in labels {
        let length = u8::try_from(label.len())
            .ok()
            .filter(|length| *length > 0)?;
        dns_name.push(length);
        dns_name.extend_from_slice(label);
    }
    dns_name.push(0);
    Some(dns_name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::abi::{dns_encode_name, namehash};

    fn labels(name: &str) -> Vec<Vec<u8>> {
        name.split('.')
            .map(|label| label.as_bytes().to_vec())
            .collect()
    }

    fn node(name: &str) -> String {
        format!(
            "{:#x}",
            B256::from(namehash(name).expect("a normalized name"))
        )
    }

    #[test]
    fn complete_labels_encode_as_the_stored_wire_name_would() {
        for name in ["eth", "alice.eth", "sub.alice.eth"] {
            assert_eq!(
                dns_name_from_labels(&labels(name), &node(name)),
                Some(dns_encode_name(name).expect("a normalized name")),
                "{name}"
            );
        }
    }

    #[test]
    fn labels_that_do_not_hash_to_the_node_give_no_wire_name() {
        assert_eq!(
            dns_name_from_labels(&labels("alice.eth"), &node("bob.eth")),
            None
        );
        assert_eq!(
            dns_name_from_labels(&labels("alice.eth"), &node("eth.alice")),
            None
        );
        assert_eq!(
            dns_name_from_labels(&labels("alice.eth"), "not a node"),
            None
        );
        assert_eq!(
            dns_name_from_labels(&[], &format!("{:#x}", B256::ZERO)),
            None
        );
    }

    #[test]
    fn a_label_longer_than_a_dns_label_gives_no_wire_name() {
        let long = vec![b'a'; 256];
        let path = vec![long.clone(), b"eth".to_vec()];
        let node = path.iter().rev().fold(B256::ZERO, |node, label| {
            keccak256([node.as_slice(), keccak256(label).as_slice()].concat())
        });
        assert_eq!(dns_name_from_labels(&path, &format!("{node:#x}")), None);
        let fits = vec![vec![b'a'; 255], b"eth".to_vec()];
        let node = fits.iter().rev().fold(B256::ZERO, |node, label| {
            keccak256([node.as_slice(), keccak256(label).as_slice()].concat())
        });
        assert!(dns_name_from_labels(&fits, &format!("{node:#x}")).is_some());
    }
}
