//! Names a log carries as text are the caller's, with no length check. A resolver's `setName`
//! emits the name it is given unchanged
//! (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L13-L19 @ ens_v1@91c966f).
//! An account names its own reverse record through the reverse registrar's `setName`, which
//! passes the name to that resolver
//! (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L105-L132 @ ens_v1@91c966f),
//! or through the default reverse registrar's `setName`, which emits `NameForAddrChanged` with
//! the name unchanged
//! (upstream: .refs/ens_v1/contracts/reverseRegistrar/DefaultReverseRegistrar.sol:L26-L28 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/reverseRegistrar/StandaloneReverseRegistrar.sol:L28-L31 @ ens_v1@91c966f).
//! The collector also reads a resolver `NameChanged` from an address no manifest declares, as
//! the rows below for any address show. So nothing bounds the labels of a name one log carries.
//!
//! The invariant: collecting a batch's dependencies costs time linear in the bytes of its logs.
//! A dot-separated name of more than `MAX_NAME_SEGMENTS` labels, more than any DNS-encodable
//! name has, is observed as no name at all. The record that carries it keeps its value, the
//! collector requests none of its nodes, and the lookahead and full-state loaders still agree
//! in one attempt. An ENSv2 resolver's DNS-encoded name over the cap requests only its own
//! node, the one interpreting it reads. A name the declared NameWrapper emits is not capped.
//!
//! | input | emitter | route | expected | test |
//! |---|---|---|---|---|
//! | `a.eth` | declared resolver | selected | record node, the name and each parent | `two_label_name_from_a_declared_resolver_requests_the_name_and_its_parents` |
//! | one label | any address | selected | record node, the name and the root | `single_label_name_from_any_resolver_requests_the_name_and_the_root` |
//! | 127 and 128 empty labels | any address | selected | every parent; a shadow name | `dotted_name_at_or_below_the_label_cap_is_a_shadow_name_with_every_parent_requested` |
//! | 129 empty labels | any address | selected | record node only | `name_changed_over_the_segment_cap_requests_its_node_only` |
//! | 129 empty labels | any address | selected | record kept, no name observed | `name_changed_over_the_segment_cap_keeps_its_record_and_observes_no_name` |
//! | 1,001 empty labels | any address | selected | at most `MAX_NAME_SEGMENTS + 2` requests, under 5 s | `dot_dense_name_changed_from_an_undeclared_resolver_collects_in_linear_time` |
//! | 100,001 empty labels | any address | selected | record node only, under 5 s; no name observed | `hundred_kilobyte_dotted_name_collects_one_request_in_linear_time` |
//! | 129 empty labels | declared resolver | selected | record kept, no name observed | `declared_resolver_name_over_the_segment_cap_keeps_its_record_and_observes_no_name` |
//! | `Linked`, 128 and 200 one-byte DNS labels | any address | speculative | every parent at the cap; the name's node only over it | `linked_name_over_the_label_cap_from_an_undeclared_resolver_requests_its_node_only` |
//! | `NamedResource`, 129 one-byte DNS labels | any address | selected | the name's node only; one attempt | `named_resource_over_the_label_cap_from_any_address_matches_full_state_in_one_attempt` |
//! | truncated DNS label | declared wrapper | selected | collection fails, as before | `wrapped_name_with_a_truncated_label_fails_collection_as_before` |
//! | 128 labels, one not UTF-8 | any address | selected | every parent; a shadow name | `non_utf8_label_at_the_label_cap_stays_a_shadow_name` |
//! | 129 empty labels, loaded again and redone | any address | selected | same requests, no retry | `name_over_the_label_cap_loads_in_one_attempt_on_every_pass` (Interpret) |
//! | two names of 129 empty labels | any address | selected | both record nodes only | `two_names_over_the_label_cap_in_one_batch_request_their_record_nodes_only` |
//! | `NameWrapped`, 129 labels | declared wrapper | selected | every parent; one attempt | `wrapped_name_over_the_label_cap_requests_every_parent_and_matches_full_state` |
//! | `NameWrapped`, 1,000 labels | declared wrapper | selected | every parent, under 5 s | `wrapped_name_of_a_thousand_labels_collects_in_linear_time` |
//! | ENSv2 resolver `NameChanged`, 129 labels | declared resolver | selected | record kept, no name observed | `ensv2_resolver_name_over_the_label_cap_keeps_its_record_and_observes_no_name` |
//! | `NameForAddrChanged`, 129 labels | declared reverse registrar | selected | record kept, no name observed | `reverse_name_over_the_label_cap_keeps_its_record_and_observes_no_name` |
//! | one label, empty labels, 128 labels | any address | selected | the fold's nodes equal every suffix hashed alone | `collected_nodes_equal_every_suffix_hashed_from_scratch` |
//! | the root, `eth`, `foo.eth` | none | none | the EIP-137 namehashes | `namehash_matches_the_eip_137_vectors` |
use std::time::{Duration, Instant};

use super::*;
use crate::schema_v2::common::{MAX_NAME_SEGMENTS, child_namehash, namehash_raw};

sol! {
    event Linked(uint256 indexed recordId, bytes32 indexed node, bytes name);
}

const UNDECLARED: &str = "0x0000000000000000000000000000000000000099";
const V1_RESOLVER: i64 = 91;
const V2_RESOLVER: i64 = 92;
const REVERSE: i64 = 93;
const WRAPPER: i64 = 94;

fn v1_resolver() -> ManifestInput {
    manifest(
        V1_RESOLVER,
        "ens_v1_resolver_l1",
        "NameChanged",
        "event NameChanged(bytes32 indexed node, string name)",
        &[],
        &["RecordChanged"],
    )
}

fn wrapper() -> ManifestInput {
    manifest(
        WRAPPER,
        "ens_v1_wrapper_l1",
        "NameWrapped",
        "event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry)",
        &["name_wrapper"],
        &[
            "TokenControlTransferred",
            "ExpiryChanged",
            "PermissionScopeChanged",
            "SurfaceBound",
            "AuthorityEpochChanged",
        ],
    )
}

/// A resolver `NameChanged` for `node` whose name is the raw bytes `raw_name`.
fn name_changed(node: B256, raw_name: &[u8], log_index: i64, emitter: &str) -> RawLogInput {
    raw_at(
        with_topic0(
            raw_resolver_strings::RawNameChanged {
                node,
                name: raw_name.to_vec().into(),
            }
            .encode_log_data(),
            resolver_name::NameChanged::SIGNATURE_HASH,
        ),
        1,
        log_index,
        emitter,
    )
}

/// An ENSv1 resolver `NameChanged` whose name is `dots` dots, emitted by an address no
/// manifest declares. The resolver family's generic events are matched across every emitter,
/// so anyone can put this log in front of the collector.
fn undeclared_dot_name_changed(node: B256, dots: usize) -> BatchInput {
    input(
        vec![v1_resolver()],
        vec![],
        vec![name_changed(
            node,
            ".".repeat(dots).as_bytes(),
            0,
            UNDECLARED,
        )],
    )
}

fn dns(labels: &[&[u8]]) -> anyhow::Result<Vec<u8>> {
    let mut encoded = Vec::new();
    for label in labels {
        encoded.push(u8::try_from(label.len())?);
        encoded.extend_from_slice(label);
    }
    encoded.push(0);
    Ok(encoded)
}

/// A declared NameWrapper's `NameWrapped` for the name `labels` spell.
fn wrapped(labels: &[&[u8]]) -> anyhow::Result<RawLogInput> {
    Ok(raw_at(
        NameWrapped {
            node: namehash_raw(labels.iter().copied()).parse()?,
            name: dns(labels)?.into(),
            owner: CONTRACT.parse()?,
            fuses: 1,
            expiry: 1_000_000,
        }
        .encode_log_data(),
        1,
        0,
        CONTRACT,
    ))
}

fn collect(input: &BatchInput) -> anyhow::Result<V1BatchDependencies> {
    collect_v1_batch_dependencies(input, &input.manifests)
}

fn requests(nodes: impl IntoIterator<Item = String>) -> BTreeSet<V1NodeRequest> {
    nodes
        .into_iter()
        .map(|node| V1NodeRequest {
            namespace: "ens".to_owned(),
            node,
        })
        .collect()
}

fn topic(node: B256) -> String {
    format!("{node:#x}")
}

fn dotted(raw_name: &[u8]) -> Vec<&[u8]> {
    raw_name.split(|byte| *byte == b'.').collect()
}

/// The node of the name `labels` spell and of each of its parents, each hashed from scratch.
fn every_suffix(labels: &[&[u8]]) -> Vec<String> {
    (0..=labels.len())
        .map(|start| namehash_raw(labels[start..].iter().copied()))
        .collect()
}

fn assert_no_name_observation(output: &BatchOutput) {
    assert!(
        output.name_surfaces.is_empty(),
        "{} name surfaces",
        output.name_surfaces.len()
    );
    assert!(
        output.label_preimages.is_empty(),
        "{} label preimages",
        output.label_preimages.len()
    );
    assert!(
        output
            .normalized_events
            .iter()
            .all(|event| event.event_kind != "PreimageObserved"),
        "a name over the cap must observe no preimage"
    );
}

fn record_field<'a>(output: &'a BatchOutput, field: &str) -> &'a Value {
    &output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .expect("the name record")
        .after_state[field]
}

#[test]
fn two_label_name_from_a_declared_resolver_requests_the_name_and_its_parents() -> anyhow::Result<()>
{
    let node = B256::repeat_byte(0x01);
    let batch = input(
        vec![v1_resolver()],
        vec![admission(V1_RESOLVER, "resolver")],
        vec![name_changed(node, b"a.eth", 0, CONTRACT)],
    );
    let mut expected = every_suffix(&dotted(b"a.eth"));
    expected.push(topic(node));
    assert_eq!(collect(&batch)?.nodes, requests(expected));
    let output = interpret_test_batch(batch)?;
    for label in [b"a".as_slice(), b"eth".as_slice()] {
        assert!(
            output
                .label_preimages
                .iter()
                .any(|preimage| preimage.raw_label == label)
        );
    }
    Ok(())
}

#[test]
fn single_label_name_from_any_resolver_requests_the_name_and_the_root() -> anyhow::Result<()> {
    let node = B256::repeat_byte(0x02);
    let batch = input(
        vec![v1_resolver()],
        vec![],
        vec![name_changed(node, b"x", 0, UNDECLARED)],
    );
    let mut expected = every_suffix(&[b"x".as_slice()]);
    expected.push(topic(node));
    assert_eq!(expected.len(), 3);
    assert_eq!(collect(&batch)?.nodes, requests(expected));
    interpret_test_batch(batch)?;
    Ok(())
}

#[test]
fn dotted_name_at_or_below_the_label_cap_is_a_shadow_name_with_every_parent_requested()
-> anyhow::Result<()> {
    for segments in [MAX_NAME_SEGMENTS - 1, MAX_NAME_SEGMENTS] {
        let node = B256::repeat_byte(0x03);
        let raw_name = ".".repeat(segments - 1);
        let labels = dotted(raw_name.as_bytes());
        assert_eq!(labels.len(), segments);
        let batch = undeclared_dot_name_changed(node, segments - 1);
        let mut expected = every_suffix(&labels);
        expected.push(topic(node));
        assert_eq!(
            collect(&batch)?.nodes,
            requests(expected),
            "{segments} segments"
        );
        let output = interpret_test_batch(batch)?;
        assert_empty_segment_shadow(&output, &namehash_raw(labels.into_iter()), &raw_name);
    }
    Ok(())
}

#[test]
fn dot_dense_name_changed_from_an_undeclared_resolver_collects_in_linear_time() -> anyhow::Result<()>
{
    // 1,000 dots split into 1,001 empty labels. Hashing every suffix of the name from scratch
    // costs the square of the label count; one right-to-left fold costs twice the count.
    let input = undeclared_dot_name_changed(B256::repeat_byte(0x91), 1_000);
    let started = Instant::now();
    let dependencies = collect_v1_batch_dependencies(&input, &input.manifests)?;
    let elapsed = started.elapsed();
    eprintln!(
        "collected {} node requests in {elapsed:?}",
        dependencies.nodes.len()
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "collecting one dot-dense NameChanged took {elapsed:?}"
    );
    assert!(
        dependencies.nodes.len() <= MAX_NAME_SEGMENTS + 2,
        "one name requested {} nodes",
        dependencies.nodes.len()
    );
    Ok(())
}

#[test]
fn name_changed_over_the_segment_cap_requests_its_node_only() -> anyhow::Result<()> {
    // One dot more than the cap gives a name over the cap. It is observed as no name, so the
    // collector asks for the log's own node and for none of the name's.
    let node = B256::repeat_byte(0x92);
    let input = undeclared_dot_name_changed(node, MAX_NAME_SEGMENTS);
    let dependencies = collect_v1_batch_dependencies(&input, &input.manifests)?;
    let expected = requests([topic(node)]);
    assert_eq!(
        dependencies.nodes.len(),
        expected.len(),
        "requested {} nodes",
        dependencies.nodes.len()
    );
    assert_eq!(dependencies.nodes, expected);
    assert!(dependencies.unsupported.is_empty(), "{dependencies:?}");
    Ok(())
}

#[test]
fn name_changed_over_the_segment_cap_keeps_its_record_and_observes_no_name() -> anyhow::Result<()> {
    let raw_name = ".".repeat(MAX_NAME_SEGMENTS);
    let output = interpret_test_batch(undeclared_dot_name_changed(
        B256::repeat_byte(0x04),
        MAX_NAME_SEGMENTS,
    ))?;
    assert_eq!(last_scoped_attempts(), 1);
    assert_no_name_observation(&output);
    assert_eq!(record_field(&output, "raw_name"), &json!(raw_name));
    Ok(())
}

#[test]
fn hundred_kilobyte_dotted_name_collects_one_request_in_linear_time() -> anyhow::Result<()> {
    let node = B256::repeat_byte(0x05);
    let batch = undeclared_dot_name_changed(node, 100_000);
    let started = Instant::now();
    let dependencies = collect(&batch)?;
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(5),
        "collecting a 100 KB name took {elapsed:?}"
    );
    assert_eq!(dependencies.nodes, requests([topic(node)]));
    let output = interpret_test_batch(batch)?;
    assert_eq!(last_scoped_attempts(), 1);
    assert_no_name_observation(&output);
    Ok(())
}

#[test]
fn declared_resolver_name_over_the_segment_cap_keeps_its_record_and_observes_no_name()
-> anyhow::Result<()> {
    let node = B256::repeat_byte(0x06);
    let raw_name = ".".repeat(MAX_NAME_SEGMENTS);
    let batch = input(
        vec![v1_resolver()],
        vec![admission(V1_RESOLVER, "resolver")],
        vec![name_changed(node, raw_name.as_bytes(), 0, CONTRACT)],
    );
    assert_eq!(collect(&batch)?.nodes, requests([topic(node)]));
    let output = interpret_test_batch(batch)?;
    assert_eq!(last_scoped_attempts(), 1);
    assert_no_name_observation(&output);
    assert_eq!(record_field(&output, "raw_name"), &json!(raw_name));
    Ok(())
}

#[test]
fn linked_name_over_the_label_cap_from_an_undeclared_resolver_requests_its_node_only()
-> anyhow::Result<()> {
    // `Linked` is matched only from a declared resolver. From any other address the collector
    // still decodes it against the active ENSv2 resolver ABI, in case the batch admits the
    // address before the log.
    let resolver = manifest(
        V2_RESOLVER,
        "ens_v2_resolver_l1",
        "Linked",
        "event Linked(uint256 indexed recordId, bytes32 indexed node, bytes name)",
        &["resolver"],
        &["ResolverRecordLinked", "PreimageObserved"],
    );
    for count in [MAX_NAME_SEGMENTS, 200] {
        let labels = vec![b"a".as_slice(); count];
        let whole = namehash_raw(labels.iter().copied());
        let batch = input(
            vec![resolver.clone()],
            vec![],
            vec![raw_at(
                Linked {
                    recordId: U256::from(1),
                    node: whole.parse()?,
                    name: dns(&labels)?.into(),
                }
                .encode_log_data(),
                1,
                0,
                UNDECLARED,
            )],
        );
        let expected = if count > MAX_NAME_SEGMENTS {
            vec![whole]
        } else {
            every_suffix(&labels)
        };
        assert_eq!(collect(&batch)?.nodes, requests(expected), "{count} labels");
    }
    Ok(())
}

#[test]
fn named_resource_over_the_label_cap_from_any_address_matches_full_state_in_one_attempt()
-> anyhow::Result<()> {
    let labels = [b"a".as_slice(); MAX_NAME_SEGMENTS + 1];
    let whole = namehash_raw(labels.iter().copied());
    let batch = input(
        vec![manifest(
            V2_RESOLVER,
            "ens_v2_resolver_l1",
            "NamedResource",
            "event NamedResource(uint256 indexed resource, bytes name)",
            &[],
            &["PreimageObserved"],
        )],
        vec![],
        vec![raw_at(
            super::super::v2_resolver::NamedResource {
                resource: U256::from(1),
                name: dns(&labels)?.into(),
            }
            .encode_log_data(),
            1,
            0,
            UNDECLARED,
        )],
    );
    assert_eq!(collect(&batch)?.nodes, requests([whole.clone()]));
    let output = interpret_test_batch(batch)?;
    assert_eq!(last_scoped_attempts(), 1);
    assert!(
        output
            .name_surfaces
            .iter()
            .any(|surface| surface.namehash == whole)
    );
    Ok(())
}

#[test]
fn wrapped_name_with_a_truncated_label_fails_collection_as_before() -> anyhow::Result<()> {
    let batch = input(
        vec![wrapper()],
        vec![admission(WRAPPER, "name_wrapper")],
        vec![raw_at(
            NameWrapped {
                node: B256::repeat_byte(0x08),
                name: b"\x05ab\0".to_vec().into(),
                owner: CONTRACT.parse()?,
                fuses: 1,
                expiry: 1_000_000,
            }
            .encode_log_data(),
            1,
            0,
            CONTRACT,
        )],
    );
    let error = collect(&batch).expect_err("a declared wrapper's malformed name stops collection");
    assert!(
        format!("{error:#}").contains("DNS label is truncated"),
        "{error:#}"
    );
    Ok(())
}

#[test]
fn non_utf8_label_at_the_label_cap_stays_a_shadow_name() -> anyhow::Result<()> {
    let node = B256::repeat_byte(0x09);
    let mut raw_name = vec![0xff];
    for _ in 1..MAX_NAME_SEGMENTS {
        raw_name.extend_from_slice(b".a");
    }
    let batch = input(
        vec![v1_resolver()],
        vec![],
        vec![name_changed(node, &raw_name, 0, UNDECLARED)],
    );
    let labels = dotted(&raw_name);
    assert_eq!(labels.len(), MAX_NAME_SEGMENTS);
    let mut expected = every_suffix(&labels);
    expected.push(topic(node));
    assert_eq!(collect(&batch)?.nodes, requests(expected));
    let output = interpret_test_batch(batch)?;
    assert_shadow_output(
        &output,
        &namehash_raw(labels.iter().copied()),
        &[0xff],
        None,
    );
    Ok(())
}

#[test]
fn two_names_over_the_label_cap_in_one_batch_request_their_record_nodes_only() -> anyhow::Result<()>
{
    let (first, second) = (B256::repeat_byte(0x11), B256::repeat_byte(0x12));
    let raw_name = ".".repeat(MAX_NAME_SEGMENTS);
    let batch = input(
        vec![v1_resolver()],
        vec![],
        vec![
            name_changed(first, raw_name.as_bytes(), 0, UNDECLARED),
            name_changed(second, raw_name.as_bytes(), 1, UNDECLARED),
        ],
    );
    assert_eq!(
        collect(&batch)?.nodes,
        requests([topic(first), topic(second)])
    );
    let output = interpret_test_batch(batch)?;
    assert_eq!(last_scoped_attempts(), 1);
    assert_no_name_observation(&output);
    assert_eq!(
        output
            .normalized_events
            .iter()
            .filter(|event| event.event_kind == "RecordChanged")
            .count(),
        2
    );
    Ok(())
}

#[test]
fn wrapped_name_over_the_label_cap_requests_every_parent_and_matches_full_state()
-> anyhow::Result<()> {
    let mut labels = vec![b"a".as_slice(); MAX_NAME_SEGMENTS];
    labels.push(b"eth".as_slice());
    let batch = input(
        vec![wrapper()],
        vec![admission(WRAPPER, "name_wrapper")],
        vec![wrapped(&labels)?],
    );
    assert_eq!(collect(&batch)?.nodes, requests(every_suffix(&labels)));
    let output = interpret_test_batch(batch)?;
    assert_eq!(last_scoped_attempts(), 1);
    let whole = namehash_raw(labels.iter().copied());
    assert!(
        output
            .name_surfaces
            .iter()
            .any(|surface| surface.namehash == whole)
    );
    Ok(())
}

#[test]
fn wrapped_name_of_a_thousand_labels_collects_in_linear_time() -> anyhow::Result<()> {
    // Hashing every suffix of 1,000 labels from scratch costs about a million hashes; the fold
    // costs two thousand.
    let labels = [b"a".as_slice(); 1_000];
    let batch = input(
        vec![wrapper()],
        vec![admission(WRAPPER, "name_wrapper")],
        vec![wrapped(&labels)?],
    );
    let started = Instant::now();
    let dependencies = collect(&batch)?;
    let elapsed = started.elapsed();
    eprintln!(
        "collected {} node requests in {elapsed:?}",
        dependencies.nodes.len()
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "collecting one 1,000-label NameWrapped took {elapsed:?}"
    );
    assert_eq!(dependencies.nodes.len(), 1_001);
    assert!(
        dependencies
            .nodes
            .is_superset(&requests([namehash_raw(labels.iter().copied())]))
    );
    Ok(())
}

#[test]
fn ensv2_resolver_name_over_the_label_cap_keeps_its_record_and_observes_no_name()
-> anyhow::Result<()> {
    let node = B256::repeat_byte(0x13);
    let raw_name = ".".repeat(MAX_NAME_SEGMENTS);
    let batch = input(
        vec![manifest(
            V2_RESOLVER,
            "ens_v2_resolver_l1",
            "NameChanged",
            "event NameChanged(bytes32 indexed node, string name)",
            &[],
            &["RecordChanged"],
        )],
        vec![admission(V2_RESOLVER, "resolver")],
        vec![name_changed(node, raw_name.as_bytes(), 0, CONTRACT)],
    );
    assert_eq!(collect(&batch)?.nodes, requests([topic(node)]));
    let output = interpret_test_batch(batch)?;
    assert_eq!(last_scoped_attempts(), 1);
    assert_no_name_observation(&output);
    assert_eq!(record_field(&output, "raw_name"), &json!(raw_name));
    Ok(())
}

#[test]
fn reverse_name_over_the_label_cap_keeps_its_record_and_observes_no_name() -> anyhow::Result<()> {
    let raw_name = ".".repeat(MAX_NAME_SEGMENTS);
    let encoded = with_topic0(
        raw_resolver_strings::RawNameForAddrChanged {
            addr: CONTRACT.parse()?,
            name: raw_name.as_bytes().to_vec().into(),
        }
        .encode_log_data(),
        resolver_strings::NameForAddrChanged::SIGNATURE_HASH,
    );
    let batch = input(
        vec![manifest(
            REVERSE,
            "ens_v1_reverse_l1",
            "NameForAddrChanged",
            "event NameForAddrChanged(address indexed addr, string name)",
            &["default_reverse_registrar"],
            &["ReverseChanged", "RecordChanged"],
        )],
        vec![admission(REVERSE, "default_reverse_registrar")],
        vec![raw_at(encoded, 1, 0, CONTRACT)],
    );
    let dependencies = collect(&batch)?;
    assert!(dependencies.nodes.is_empty(), "{:?}", dependencies.nodes);
    let output = interpret_test_batch(batch)?;
    assert_eq!(last_scoped_attempts(), 1);
    assert_no_name_observation(&output);
    assert_eq!(record_field(&output, "raw_name"), &json!(raw_name));
    Ok(())
}

#[test]
fn collected_nodes_equal_every_suffix_hashed_from_scratch() -> anyhow::Result<()> {
    let labels_at_cap = ["a"; MAX_NAME_SEGMENTS].join(".");
    let empty_at_cap = ".".repeat(MAX_NAME_SEGMENTS - 1);
    let mixed_at_cap = (0..MAX_NAME_SEGMENTS)
        .map(|index| if index % 3 == 0 { "" } else { "x" })
        .collect::<Vec<_>>()
        .join(".");
    for raw_name in [
        b"alice".as_slice(),
        b"..".as_slice(),
        labels_at_cap.as_bytes(),
        empty_at_cap.as_bytes(),
        mixed_at_cap.as_bytes(),
    ] {
        let node = B256::repeat_byte(0x14);
        let batch = input(
            vec![v1_resolver()],
            vec![],
            vec![name_changed(node, raw_name, 0, UNDECLARED)],
        );
        let mut expected = every_suffix(&dotted(raw_name));
        expected.push(topic(node));
        assert_eq!(
            collect(&batch)?.nodes,
            requests(expected),
            "{}",
            String::from_utf8_lossy(raw_name)
        );
    }
    Ok(())
}

/// `every_suffix` hashes through `namehash_raw`, so the fold's equality test above cannot see a
/// change to `namehash_raw` itself. These are the EIP-137 values.
#[test]
fn namehash_matches_the_eip_137_vectors() -> anyhow::Result<()> {
    const ROOT: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";
    const ETH: &str = "0x93cdeb708b7545dc668eb9280176169d1c33cfd8ed6f04690a0bcc88a93fc4ae";
    const FOO_ETH: &str = "0xde9b09fd7c5f901e23a3f19fecc54828e9c848539801e86591bd9801b019f84f";
    assert_eq!(namehash_raw(std::iter::empty()), ROOT);
    assert_eq!(namehash_raw([b"eth".as_slice()].into_iter()), ETH);
    assert_eq!(
        namehash_raw([b"foo".as_slice(), b"eth".as_slice()].into_iter()),
        FOO_ETH
    );
    assert_eq!(
        format!("{:#x}", child_namehash(ETH.parse()?, b"foo")),
        FOO_ETH
    );
    Ok(())
}
