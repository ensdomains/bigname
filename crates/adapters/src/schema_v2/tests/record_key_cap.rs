//! Whoever may set a name's records chooses each text, data or DNS record key, at any length
//! (upstream: .refs/ens_v1/contracts/resolvers/profiles/TextResolver.sol:L15-L22 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/resolvers/profiles/DataResolver.sol:L19-L27 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/resolvers/profiles/DNSResolver.sol:L51 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L188-L195 @ ens_v2@a971bd6)
//! (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L221-L228 @ ens_v2@a971bd6).
//!
//! The interpreted record key (`text:{key}`, `text_opaque:0x{hex}`, `data:{key}`,
//! `dns:{resource}:0x{hex(name)}`) is part of the primary key and the hydration-order index of
//! the record tables, and a PostgreSQL btree index row holds at most 2,704 bytes. Interpret skips
//! a log whose formatted record key is over 2,400 bytes, as it skips a log whose indexed key is
//! not the hash of its key. Every key of 2,400 bytes or fewer is interpreted as before.
use serde_json::Value;

use super::*;

sol! {
    event DNSRecordChanged(bytes32 indexed node, bytes name, uint16 resource, bytes record);
    event DNSRecordDeleted(bytes32 indexed node, bytes name, uint16 resource);
    event TextUpdated(uint256 indexed recordId, string indexed keyHash, string key, string value);
    event DataUpdated(uint256 indexed recordId, string indexed keyHash, string key, bytes value);
}

const TEXT_CHANGED: &str =
    "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value)";
const TEXT_CHANGED_WITHOUT_VALUE: &str =
    "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key)";

fn node() -> B256 {
    B256::repeat_byte(0x41)
}

fn interpret_logs(
    manifest: ManifestInput,
    admissions: Vec<AddressAdmissionInput>,
    logs: Vec<alloy_primitives::LogData>,
) -> anyhow::Result<BatchOutput> {
    interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![manifest],
        discovery_rules: Vec::new(),
        admissions,
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: logs
            .into_iter()
            .enumerate()
            .map(|(index, log)| raw_at(log, 1, index as i64, CONTRACT))
            .collect(),
    })
}

/// A v1 `TextChanged(node, key, key, value)` log with raw key and value bytes.
fn v1_text(key: &[u8], value: &[u8]) -> alloy_primitives::LogData {
    with_topic0(
        raw_resolver_strings::RawTextChanged {
            node: node(),
            indexedKey: keccak256(key),
            key: key.to_vec().into(),
            value: value.to_vec().into(),
        }
        .encode_log_data(),
        resolver_strings::TextChanged::SIGNATURE_HASH,
    )
}

fn interpret_v1_text(logs: Vec<alloy_primitives::LogData>) -> anyhow::Result<BatchOutput> {
    interpret_logs(
        manifest(
            4151,
            "ens_v1_resolver_l1",
            "TextChanged",
            TEXT_CHANGED,
            &[],
            &["RecordChanged"],
        ),
        Vec::new(),
        logs,
    )
}

/// The `(record_key, value)` of every `RecordChanged` in the batch, in log order.
fn record_changes(output: &BatchOutput) -> Vec<(String, Value)> {
    output
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "RecordChanged")
        .map(|event| {
            (
                event.after_state["record_key"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
                event.after_state["value"].clone(),
            )
        })
        .collect()
}

fn assert_skipped(output: &BatchOutput, case: &str) {
    let changes = record_changes(output);
    assert!(
        changes.is_empty(),
        "{case}: a record key over the cap must be skipped, got keys of {:?} bytes",
        changes.iter().map(|(key, _)| key.len()).collect::<Vec<_>>()
    );
}

/// Incompressible hex text of `length` bytes, from keccak256 of each index.
fn hash_hex(length: usize) -> String {
    let mut text = String::new();
    let mut index = 0_u64;
    while text.len() < length {
        text.push_str(&hex::encode(&keccak256(index.to_be_bytes())[..16]));
        index += 1;
    }
    text.truncate(length);
    text
}

#[test]
fn a_text_key_at_the_cap_is_written_and_one_byte_over_is_skipped() -> anyhow::Result<()> {
    let at_cap = interpret_v1_text(vec![v1_text("k".repeat(2_395).as_bytes(), b"value")])?;
    let changes = record_changes(&at_cap);
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].0.len(), 2_400);
    assert!(changes[0].0.starts_with("text:k"));

    let over = interpret_v1_text(vec![v1_text("k".repeat(2_396).as_bytes(), b"value")])?;
    assert_skipped(&over, "text key of 2,401 bytes");
    Ok(())
}

#[test]
fn a_text_key_far_over_the_cap_is_skipped() -> anyhow::Result<()> {
    // 84 hex strings of 32 characters: 2,688 bytes, 2,693 with the `text:` prefix.
    let hashes = hash_hex(84 * 32);
    assert_eq!(hashes.len(), 2_688);
    for key in [hashes, hash_hex(9_000)] {
        let output = interpret_v1_text(vec![v1_text(key.as_bytes(), b"value")])?;
        assert_skipped(&output, &format!("text key of {} bytes", key.len()));
    }
    Ok(())
}

#[test]
fn the_largest_key_on_mainnet_still_updates_under_its_own_key() -> anyhow::Result<()> {
    let key = "k".repeat(2_121);
    let output = interpret_v1_text(vec![
        v1_text(key.as_bytes(), b"first"),
        v1_text(key.as_bytes(), b"second"),
    ])?;
    let record_key = format!("text:{key}");
    assert_eq!(record_key.len(), 2_126);
    assert_eq!(
        record_changes(&output),
        vec![
            (record_key.clone(), json!("first")),
            (record_key, json!("second")),
        ]
    );
    Ok(())
}

#[test]
fn an_opaque_text_key_is_measured_after_hex_formatting() -> anyhow::Result<()> {
    let at_cap = interpret_v1_text(vec![v1_text(&[0xff; 1_193], b"value")])?;
    let changes = record_changes(&at_cap);
    assert_eq!(changes.len(), 1);
    assert!(
        changes[0].0.starts_with("text_opaque:0x"),
        "{}",
        changes[0].0
    );
    assert_eq!(changes[0].0.len(), 2_400);

    let over = interpret_v1_text(vec![v1_text(&[0xff; 1_194], b"value")])?;
    assert_skipped(&over, "opaque text key of 1,194 bytes, 2,402 formatted");
    Ok(())
}

#[test]
fn a_dns_record_key_over_the_cap_is_skipped() -> anyhow::Result<()> {
    let dns_manifest = || {
        manifest_with_events(
            4152,
            "ens",
            "ens_v1_resolver_l1",
            &[
                (
                    "DNSRecordChanged",
                    "event DNSRecordChanged(bytes32 indexed node, bytes name, uint16 resource, bytes record)",
                    &[],
                    &["RecordChanged"],
                ),
                (
                    "DNSRecordDeleted",
                    "event DNSRecordDeleted(bytes32 indexed node, bytes name, uint16 resource)",
                    &[],
                    &["RecordChanged"],
                ),
            ],
        )
    };
    let changed = |name: &[u8]| {
        DNSRecordChanged {
            node: node(),
            name: name.to_vec().into(),
            resource: 1,
            record: vec![0x01].into(),
        }
        .encode_log_data()
    };
    let deleted = |name: &[u8]| {
        DNSRecordDeleted {
            node: node(),
            name: name.to_vec().into(),
            resource: 1,
        }
        .encode_log_data()
    };
    let long_name = vec![b'a'; 1_197];
    for (event, short, long) in [
        (
            "DNSRecordChanged",
            changed(b"abc".as_slice()),
            changed(long_name.as_slice()),
        ),
        (
            "DNSRecordDeleted",
            deleted(b"abc".as_slice()),
            deleted(long_name.as_slice()),
        ),
    ] {
        let written = interpret_logs(dns_manifest(), Vec::new(), vec![short])?;
        let changes = record_changes(&written);
        assert_eq!(changes.len(), 1, "{event}");
        assert_eq!(changes[0].0, "dns:1:0x616263", "{event}");

        let over = interpret_logs(dns_manifest(), Vec::new(), vec![long])?;
        assert_skipped(&over, &format!("{event} key of 2,402 bytes"));
    }
    Ok(())
}

#[test]
fn a_data_key_over_the_cap_is_skipped() -> anyhow::Result<()> {
    let key = "k".repeat(2_396);
    let output = interpret_logs(
        manifest(
            4153,
            "ens_v1_resolver_l1",
            "DataChanged",
            "event DataChanged(bytes32 indexed node, string indexed indexedKey, string key, bytes indexed indexedData)",
            &[],
            &["RecordChanged"],
        ),
        Vec::new(),
        vec![
            resolver_strings::DataChanged {
                node: node(),
                indexedKey: keccak256(key.as_bytes()),
                key,
                indexedData: keccak256(b"data"),
            }
            .encode_log_data(),
        ],
    )?;
    assert_skipped(&output, "data key of 2,401 bytes");
    Ok(())
}

#[test]
fn a_legacy_text_key_over_the_cap_is_skipped() -> anyhow::Result<()> {
    let key = "k".repeat(2_396);
    let indexed = legacy_text_without_value::TextChanged {
        node: node(),
        indexedKey: keccak256(key.as_bytes()),
        key: key.clone(),
    }
    .encode_log_data();
    let unindexed = legacy_unindexed_text_without_value::TextChanged {
        node: node(),
        indexedKey: key.clone(),
        key,
    }
    .encode_log_data();
    for (shape, log) in [("three-topic", indexed), ("two-topic", unindexed)] {
        let output = interpret_logs(
            manifest(
                4154,
                "ens_v1_resolver_l1",
                "TextChanged",
                TEXT_CHANGED_WITHOUT_VALUE,
                &[],
                &["RecordChanged"],
            ),
            Vec::new(),
            vec![log],
        )?;
        assert_skipped(&output, &format!("{shape} TextChanged without value"));
    }
    Ok(())
}

#[test]
fn an_ensv2_resolver_text_key_over_the_cap_is_skipped() -> anyhow::Result<()> {
    let output = interpret_logs(
        manifest(
            4155,
            "ens_v2_resolver_l1",
            "TextChanged",
            TEXT_CHANGED,
            &[],
            &["RecordChanged"],
        ),
        vec![admission(4155, "resolver")],
        vec![v1_text("k".repeat(2_396).as_bytes(), b"value")],
    )?;
    assert_skipped(&output, "ENSv2 resolver TextChanged");
    Ok(())
}

#[test]
fn a_record_id_text_or_data_key_over_the_cap_is_skipped() -> anyhow::Result<()> {
    let record_id_manifest = || {
        manifest_with_events(
            4156,
            "ens",
            "ens_v2_resolver_l1",
            &[
                (
                    "Linked",
                    "event Linked(uint256 indexed recordId, bytes32 indexed node, bytes name)",
                    &["resolver"],
                    &["ResolverRecordLinked", "PreimageObserved"],
                ),
                (
                    "TextUpdated",
                    "event TextUpdated(uint256 indexed recordId, string indexed keyHash, string key, string value)",
                    &["resolver"],
                    &["RecordChanged"],
                ),
                (
                    "DataUpdated",
                    "event DataUpdated(uint256 indexed recordId, string indexed keyHash, string key, bytes value)",
                    &["resolver"],
                    &["RecordChanged"],
                ),
            ],
        )
    };
    let key = "k".repeat(2_396);
    let text = TextUpdated {
        recordId: U256::from(7),
        keyHash: keccak256(key.as_bytes()),
        key: key.clone(),
        value: "value".to_owned(),
    }
    .encode_log_data();
    let data = DataUpdated {
        recordId: U256::from(7),
        keyHash: keccak256(key.as_bytes()),
        key,
        value: vec![0x01].into(),
    }
    .encode_log_data();
    for (event, log) in [("TextUpdated", text), ("DataUpdated", data)] {
        let output = interpret_logs(
            record_id_manifest(),
            vec![admission(4156, "resolver")],
            vec![log],
        )?;
        assert_skipped(&output, event);
    }
    Ok(())
}

#[test]
fn a_key_over_the_cap_does_not_stop_the_batch() -> anyhow::Result<()> {
    let output = interpret_v1_text(vec![
        v1_text(b"url", b"https://example.test"),
        v1_text("k".repeat(2_396).as_bytes(), b"value"),
        v1_text(b"avatar", b"ipfs://avatar"),
    ])?;
    assert!(output.decode_skips.is_empty());
    let changes = record_changes(&output);
    assert!(
        changes.len() == 2,
        "the over-cap log must be skipped, got keys of {:?} bytes",
        changes.iter().map(|(key, _)| key.len()).collect::<Vec<_>>()
    );
    assert_eq!(
        changes,
        vec![
            ("text:url".to_owned(), json!("https://example.test")),
            ("text:avatar".to_owned(), json!("ipfs://avatar")),
        ]
    );
    Ok(())
}

#[test]
fn a_multibyte_text_key_is_measured_in_bytes() -> anyhow::Result<()> {
    // `€` is three bytes in UTF-8. Both keys format to 804 characters.
    let at_cap = format!("{}k", "€".repeat(798));
    let written = interpret_v1_text(vec![v1_text(at_cap.as_bytes(), b"value")])?;
    let changes = record_changes(&written);
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].0, format!("text:{at_cap}"));
    assert_eq!(changes[0].0.len(), 2_400);
    assert_eq!(changes[0].0.chars().count(), 804);

    let over = "€".repeat(799);
    assert_eq!(format!("text:{over}").chars().count(), 804);
    let output = interpret_v1_text(vec![v1_text(over.as_bytes(), b"value")])?;
    assert_skipped(&output, "text key of 804 characters and 2,402 bytes");
    Ok(())
}

/// `count` distinct ten-letter labels: `aaaaaaaaaa`, `aaaaaaaaab`, and so on.
fn distinct_labels(count: usize) -> Vec<Vec<u8>> {
    (0..count)
        .map(|index| {
            (0..10_u32)
                .rev()
                .map(|place| b"abcdefghijklmnopqrstuvwxyz"[index / 26_usize.pow(place) % 26])
                .collect()
        })
        .collect()
}

/// An ENSv2 resolver's `NamedResource` names a resource by a DNS-encoded name of any length,
/// from any address. The name builds no record key, so the cap never reads it, and no column a
/// btree index reads carries the whole name. One log of 300 distinct labels, more than the 127 a
/// 255-byte DNS name can hold, in a DNS-encoded name of 3,301 bytes, longer than one btree index
/// row holds, is observed whole.
#[test]
fn a_long_dns_name_from_a_named_resource_is_observed_and_stored_whole() -> anyhow::Result<()> {
    let labels = distinct_labels(300);
    let mut name = Vec::new();
    for label in &labels {
        name.push(u8::try_from(label.len())?);
        name.extend_from_slice(label);
    }
    name.push(0);
    assert_eq!(name.len(), 3_301);
    let output = interpret_logs(
        manifest(
            4157,
            "ens_v2_resolver_l1",
            "NamedResource",
            "event NamedResource(uint256 indexed resource, bytes name)",
            &[],
            &["PreimageObserved"],
        ),
        Vec::new(),
        vec![
            v2_resolver::NamedResource {
                resource: U256::from(1),
                name: name.into(),
            }
            .encode_log_data(),
        ],
    )?;

    assert!(output.decode_skips.is_empty());
    assert!(record_changes(&output).is_empty());
    assert_eq!(
        output
            .normalized_events
            .iter()
            .map(|event| event.event_kind.as_str())
            .collect::<Vec<_>>(),
        vec!["PreimageObserved"]
    );
    assert_eq!(output.name_surfaces.len(), 1);
    let surface = &output.name_surfaces[0];
    assert_eq!(
        surface.namehash,
        crate::schema_v2::common::namehash_raw(labels.iter().map(Vec::as_slice))
    );
    assert_eq!(surface.visibility_state, "active");
    assert_eq!(surface.labelhashes.len(), 300);
    let raw = surface.raw.as_ref().expect("the whole name is retained");
    assert_eq!(raw.raw_labels.len(), 300);
    assert_eq!(raw.raw_name.len(), 3_299);
    assert_eq!(raw.dns_encoded_name.len(), 3_301);
    assert_eq!(output.label_preimages.len(), 300);
    Ok(())
}
