//! An emitter no manifest declares can emit a well-formed ENSv2 registry log that contradicts
//! itself or the emitter's own earlier logs. Interpret skips that log with one operator
//! diagnostic row and interprets the rest of the batch as if the log were absent. A
//! manifest-declared emitter still stops the batch on the same log. Every other adapter error
//! stops the batch for both.

use super::*;

const MANIFEST: i64 = 4130;
const CHILD: &str = "0x0000000000000000000000000000000000000061";
const LABEL_REGISTERED: &str = "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)";

fn registry_manifest() -> ManifestInput {
    manifest_with_events(
        MANIFEST,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "RegistryCreated",
                "event RegistryCreated()",
                &["registry"],
                &["RegistryCreated"],
            ),
            (
                "LabelRegistered",
                LABEL_REGISTERED,
                &["registry"],
                &["RegistrationGranted"],
            ),
            (
                "LabelReserved",
                "event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint64 expiry, address indexed sender)",
                &["registry"],
                &["RegistrationReserved"],
            ),
            (
                "LabelUnregistered",
                "event LabelUnregistered(uint256 indexed tokenId, address indexed sender)",
                &["registry"],
                &["RegistrationReleased"],
            ),
            (
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["registry"],
                &["TokenResourceLinked"],
            ),
            (
                "TransferBatch",
                "event TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values)",
                &["registry"],
                &["TokenControlTransferred"],
            ),
            (
                "EACRolesChanged",
                "event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap)",
                &["registry"],
                &["PermissionChanged", "RootPermissionChanged"],
            ),
            (
                "TokenRegenerated",
                "event TokenRegenerated(uint256 indexed oldTokenId, uint256 indexed newTokenId)",
                &["registry"],
                &["TokenRegenerated"],
            ),
            (
                "SubregistryUpdated",
                "event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender)",
                &["registry"],
                &["SubregistryChanged"],
            ),
            (
                "ResolverUpdated",
                "event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender)",
                &["registry"],
                &["ResolverChanged"],
            ),
            (
                "ParentUpdated",
                "event ParentUpdated(address indexed parent, string label, address indexed sender)",
                &["registry"],
                &["ParentChanged"],
            ),
        ],
    )
}

/// A registry admitted by its own `RegistryCreated` announcement, which no manifest declares.
fn undeclared() -> AddressAdmissionInput {
    let mut announced = admission(MANIFEST, "registry");
    announced.discovery_edge_kind = Some("registry_announcement".to_owned());
    announced.discovery_from_contract_instance_id = Some(announced.contract_instance_id);
    announced.discovery_observation_key = Some("registry-announcement:self".to_owned());
    announced
}

fn declared() -> AddressAdmissionInput {
    admission(MANIFEST, "registry")
}

/// The blocks come from every log, so the same input without some of its logs keeps its blocks.
fn batch(admissions: Vec<AddressAdmissionInput>, raw_logs: Vec<RawLogInput>) -> BatchInput {
    let mut blocks = std::collections::BTreeMap::new();
    for raw in &raw_logs {
        blocks
            .entry((raw.block_number, raw.block_hash.clone()))
            .or_insert_with(|| RawBlockInput {
                chain_id: raw.chain_id.clone(),
                block_hash: raw.block_hash.clone(),
                block_number: raw.block_number,
                block_timestamp: raw.block_timestamp,
                canonicality_state: raw.canonicality_state.clone(),
            });
    }
    BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![registry_manifest()],
        discovery_rules: [
            ("registry_announcement", "reachable_from_root"),
            ("subregistry", "linked_subregistry_event"),
            ("resolver", "protocol_event"),
        ]
        .into_iter()
        .map(|(edge_kind, admission)| DiscoveryRuleInput {
            manifest_id: MANIFEST,
            edge_kind: edge_kind.to_owned(),
            from_role: Some("registry".to_owned()),
            admission: admission.to_owned(),
        })
        .collect(),
        admissions,
        prior_events: Vec::new(),
        blocks: blocks.into_values().collect(),
        raw_logs,
    }
}

/// Interprets `input`, and the same input without the logs at `skipped`. The first output must
/// be the second plus one inconsistent-log row for each skipped log, in log order.
fn assert_skipped(input: BatchInput, skipped: &[usize], site: &str) -> anyhow::Result<BatchOutput> {
    let mut without = input.clone();
    without.raw_logs = input
        .raw_logs
        .iter()
        .enumerate()
        .filter(|(index, _)| !skipped.contains(index))
        .map(|(_, raw)| raw.clone())
        .collect();
    let output = interpret_test_batch(input.clone())?;
    let mut interpreted = output.clone();
    let skips = std::mem::take(&mut interpreted.decode_skips);
    assert_eq!(
        interpreted,
        interpret_test_batch(without)?,
        "the skipped logs must leave the output of the rest of the batch unchanged"
    );
    assert_eq!(skips.len(), skipped.len(), "{skips:#?}");
    for (skip, index) in skips.iter().zip(skipped) {
        let raw = &input.raw_logs[*index];
        assert_eq!(
            (
                skip.block_number,
                skip.log_index,
                skip.transaction_hash.as_str()
            ),
            (
                raw.block_number,
                raw.log_index,
                raw.transaction_hash.as_str()
            )
        );
        assert_eq!(skip.emitting_address, raw.emitting_address);
        assert_eq!(skip.source_family, "ens_v2_registry_l1");
        assert_eq!(skip.inconsistency_site, Some(site));
        assert!(
            skip.decode_context
                .starts_with(&format!("inconsistent {site}: ")),
            "{}",
            skip.decode_context
        );
    }
    Ok(output)
}

fn assert_stops(input: BatchInput, message: &str) {
    let error = interpret_test_batch(input).expect_err("the batch stops");
    let error = format!("{error:#}");
    assert!(error.contains(message), "{error}");
}

fn sender() -> Address {
    Address::repeat_byte(0x51)
}

fn registered(token: U256, label: &str, hashed: &str) -> alloy_primitives::LogData {
    v2_registry::LabelRegistered {
        tokenId: token,
        labelHash: keccak256(hashed.as_bytes()),
        label: label.to_owned(),
        owner: sender(),
        expiry: 1_000,
        sender: sender(),
    }
    .encode_log_data()
}

fn reserved(token: U256, label: &str, hashed: &str) -> alloy_primitives::LogData {
    v2_registry::LabelReserved {
        tokenId: token,
        labelHash: keccak256(hashed.as_bytes()),
        label: label.to_owned(),
        expiry: 1_000,
        sender: sender(),
    }
    .encode_log_data()
}

fn linked(token: U256, resource: u64) -> alloy_primitives::LogData {
    v2_registry::TokenResource {
        tokenId: token,
        resource: U256::from(resource),
    }
    .encode_log_data()
}

fn roles_changed(resource: u64) -> alloy_primitives::LogData {
    EACRolesChanged {
        resource: U256::from(resource),
        account: Address::repeat_byte(0x52),
        oldRoleBitmap: U256::ZERO,
        newRoleBitmap: U256::from(1),
    }
    .encode_log_data()
}

fn logs(encoded: Vec<alloy_primitives::LogData>) -> Vec<RawLogInput> {
    (0_i64..)
        .zip(encoded)
        .map(|(log_index, encoded)| raw_at(encoded, 1, log_index, CONTRACT))
        .collect()
}

fn label_hash_mismatch() -> Vec<RawLogInput> {
    logs(vec![registered(
        versioned_token("alice", 1),
        "alice",
        "bob",
    )])
}

fn uneven_transfer_batch() -> Vec<RawLogInput> {
    logs(vec![
        v2_registry::TransferBatch {
            operator: sender(),
            from: sender(),
            to: Address::repeat_byte(0x52),
            ids: vec![U256::from(7), U256::from(8)],
            values: vec![U256::from(1)],
        }
        .encode_log_data(),
    ])
}

/// The token is registered and linked to resource 99, then linked to resource 100.
fn conflicting_token_resource() -> Vec<RawLogInput> {
    let token = versioned_token("alice", 1);
    logs(vec![
        registered(token, "alice", "alice"),
        linked(token, 99),
        linked(token, 100),
    ])
}

/// Tokens 7 and 8 are both linked to resource 99, so neither role change on it names one token.
fn ambiguous_token_resource() -> Vec<RawLogInput> {
    logs(vec![
        linked(U256::from(7), 99),
        linked(U256::from(8), 99),
        roles_changed(99),
        roles_changed(99),
    ])
}

fn regenerated_without_predecessor() -> Vec<RawLogInput> {
    logs(vec![
        v2_registry::TokenRegenerated {
            oldTokenId: U256::from(7),
            newTokenId: U256::from(8),
        }
        .encode_log_data(),
    ])
}

fn regenerated(old_token: U256, new_token: U256) -> alloy_primitives::LogData {
    v2_registry::TokenRegenerated {
        oldTokenId: old_token,
        newTokenId: new_token,
    }
    .encode_log_data()
}

/// One block of `CHILD` logs: `before`, then the `RegistryCreated` that admits `CHILD`, then
/// `after`. No manifest declares `CHILD`.
fn announced_between(
    before: Vec<alloy_primitives::LogData>,
    after: Vec<alloy_primitives::LogData>,
) -> BatchInput {
    let encoded = before
        .into_iter()
        .chain([RegistryCreated {}.encode_log_data()])
        .chain(after);
    let raw_logs = (0_i64..)
        .zip(encoded)
        .map(|(log_index, encoded)| raw_at(encoded, 1, log_index, CHILD))
        .collect();
    batch(Vec::new(), raw_logs)
}

#[test]
fn an_undeclared_label_that_does_not_hash_to_its_label_hash_is_skipped() -> anyhow::Result<()> {
    let output = assert_skipped(
        batch(vec![undeclared()], label_hash_mismatch()),
        &[0],
        "label_hash_mismatch",
    )?;
    assert!(output.normalized_events.is_empty());
    Ok(())
}

#[test]
fn a_declared_label_that_does_not_hash_to_its_label_hash_stops() {
    assert_stops(
        batch(vec![declared()], label_hash_mismatch()),
        "LabelRegistered label does not hash to labelHash",
    );
}

#[test]
fn an_undeclared_uneven_transfer_batch_is_skipped() -> anyhow::Result<()> {
    let output = assert_skipped(
        batch(vec![undeclared()], uneven_transfer_batch()),
        &[0],
        "transfer_batch_length_mismatch",
    )?;
    assert!(output.normalized_events.is_empty());
    Ok(())
}

#[test]
fn a_declared_uneven_transfer_batch_stops() {
    assert_stops(
        batch(vec![declared()], uneven_transfer_batch()),
        "TransferBatch ids and values differ in length",
    );
}

#[test]
fn an_undeclared_token_resource_that_relinks_a_token_is_skipped() -> anyhow::Result<()> {
    let output = assert_skipped(
        batch(vec![undeclared()], conflicting_token_resource()),
        &[2],
        "token_resource_conflict",
    )?;
    let links = output
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "TokenResourceLinked")
        .collect::<Vec<_>>();
    assert_eq!(links.len(), 1, "the first link is kept");
    assert_eq!(links[0].log_index, Some(1));
    Ok(())
}

#[test]
fn a_declared_token_resource_that_relinks_a_token_stops() {
    assert_stops(
        batch(vec![declared()], conflicting_token_resource()),
        "does not confirm its retained resource",
    );
}

#[test]
fn an_undeclared_role_change_on_a_resource_two_tokens_link_is_skipped() -> anyhow::Result<()> {
    let output = assert_skipped(
        batch(vec![undeclared()], ambiguous_token_resource()),
        &[2, 3],
        "token_resource_ambiguous",
    )?;
    assert_eq!(
        output
            .normalized_events
            .iter()
            .filter(|event| event.event_kind == "TokenResourceLinked")
            .count(),
        2,
        "both links are kept"
    );
    Ok(())
}

#[test]
fn a_declared_role_change_on_a_resource_two_tokens_link_stops() {
    assert_stops(
        batch(vec![declared()], ambiguous_token_resource()),
        "maps to more than one retained token",
    );
}

#[test]
fn an_undeclared_regeneration_of_an_unknown_token_is_skipped() -> anyhow::Result<()> {
    let output = assert_skipped(
        batch(vec![undeclared()], regenerated_without_predecessor()),
        &[0],
        "token_regenerated_without_predecessor",
    )?;
    assert!(output.normalized_events.is_empty());
    Ok(())
}

#[test]
fn a_declared_regeneration_of_an_unknown_token_stops() {
    assert_stops(
        batch(vec![declared()], regenerated_without_predecessor()),
        "has no retained TokenResource predecessor",
    );
}

/// One transaction: a valid registration, a reservation whose label does not hash to its label
/// hash, and a valid link for the registered token. The registration and the link apply in full.
#[test]
fn an_inconsistent_log_between_valid_logs_of_one_transaction_skips_only_itself()
-> anyhow::Result<()> {
    let alice = versioned_token("alice", 1);
    let raw_logs = logs(vec![
        registered(alice, "alice", "alice"),
        reserved(versioned_token("bob", 1), "bob", "carol"),
        linked(alice, 99),
    ]);
    assert!(
        raw_logs
            .iter()
            .all(|raw| raw.transaction_hash == raw_logs[0].transaction_hash)
    );
    let output = assert_skipped(
        batch(vec![undeclared()], raw_logs),
        &[1],
        "label_hash_mismatch",
    )?;
    let at = |kind: &str| {
        output
            .normalized_events
            .iter()
            .filter(|event| event.event_kind == kind)
            .map(|event| event.log_index)
            .collect::<Vec<_>>()
    };
    assert_eq!(at("RegistrationGranted"), [Some(0)]);
    assert_eq!(at("TokenResourceLinked"), [Some(2)]);
    assert!(at("RegistrationReserved").is_empty());
    Ok(())
}

/// A registry announced in the block of its inconsistent log is skipped there, whether the log
/// comes after the announcement or before it and is read in its place in the block's next pass.
/// That skip is not also recorded as a log that preceded its emitter's admission.
#[test]
fn an_inconsistent_log_in_its_registrys_announcement_block_is_skipped_once() -> anyhow::Result<()> {
    let announced = |log_index| raw_at(RegistryCreated {}.encode_log_data(), 1, log_index, CHILD);
    let inconsistent = |log_index| {
        raw_at(
            registered(U256::from(7), "alice", "bob"),
            1,
            log_index,
            CHILD,
        )
    };
    for (case, raw_logs, skipped) in [
        (
            "before the announcement",
            vec![inconsistent(0), announced(1)],
            0,
        ),
        (
            "after the announcement",
            vec![announced(0), inconsistent(1)],
            1,
        ),
    ] {
        let output = assert_skipped(
            batch(Vec::new(), raw_logs),
            &[skipped],
            "label_hash_mismatch",
        )
        .with_context(|| case.to_owned())?;
        assert!(
            output
                .discovery_edges
                .iter()
                .any(|edge| edge.edge_kind == "registry_announcement"),
            "{case}: the announcement still admits the registry"
        );
    }
    Ok(())
}

/// A registry's logs before its announcement are read in log order once the block is interpreted
/// again with the registry admitted, so a check on a later log of that block sees them. The
/// skipped log is the one that contradicts them, not an earlier one.
#[test]
fn a_check_after_the_announcement_reads_the_registrys_earlier_logs_in_that_block()
-> anyhow::Result<()> {
    let (seven, eight) = (U256::from(7), U256::from(8));
    for (site, before, after, skipped) in [
        (
            "token_resource_conflict",
            vec![linked(seven, 99)],
            vec![linked(seven, 100)],
            2,
        ),
        (
            "token_resource_ambiguous",
            vec![linked(seven, 99)],
            vec![linked(eight, 99), roles_changed(99)],
            3,
        ),
        (
            "token_resource_ambiguous",
            vec![linked(seven, 99), linked(eight, 99)],
            vec![roles_changed(99)],
            3,
        ),
    ] {
        assert_skipped(announced_between(before, after), &[skipped], site)
            .with_context(|| format!("{site} at log {skipped}"))?;
    }
    Ok(())
}

/// Each log that gives the emitter state for a token comes before the registry's announcement in
/// the same block, and that token's regeneration after it. The regeneration applies.
#[test]
fn a_regeneration_after_the_announcement_reads_each_predecessor_before_it() -> anyhow::Result<()> {
    let (old_token, new_token) = (versioned_token("alice", 1), versioned_token("alice", 2));
    let first_token = versioned_token("alice", 0);
    for (predecessor, before) in [
        (
            "LabelRegistered",
            vec![registered(old_token, "alice", "alice")],
        ),
        ("LabelReserved", vec![reserved(old_token, "alice", "alice")]),
        ("TokenResource", vec![linked(old_token, 99)]),
        (
            "SubregistryUpdated",
            vec![
                v2_registry::SubregistryUpdated {
                    tokenId: old_token,
                    subregistry: Address::repeat_byte(0x53),
                    sender: sender(),
                }
                .encode_log_data(),
            ],
        ),
        (
            "ResolverUpdated",
            vec![
                v2_registry::ResolverUpdated {
                    tokenId: old_token,
                    resolver: Address::repeat_byte(0x54),
                    sender: sender(),
                }
                .encode_log_data(),
            ],
        ),
        (
            "TokenRegenerated",
            vec![
                registered(first_token, "alice", "alice"),
                regenerated(first_token, old_token),
            ],
        ),
    ] {
        let regeneration = i64::try_from(before.len())? + 1;
        let output = interpret_test_batch(announced_between(
            before,
            vec![regenerated(old_token, new_token)],
        ))
        .with_context(|| predecessor.to_owned())?;
        assert!(
            output.decode_skips.is_empty(),
            "{predecessor}: {:#?}",
            output.decode_skips
        );
        assert!(
            output.normalized_events.iter().any(|event| {
                event.log_index == Some(regeneration) && event.event_kind == "TokenRegenerated"
            }),
            "{predecessor}: the regeneration after the announcement applies"
        );
    }
    Ok(())
}

/// Another token's registration before the announcement is no predecessor. A regeneration after
/// the announcement is still skipped. A first link, and a role change on a resource one token
/// links, still apply.
#[test]
fn a_check_after_the_announcement_without_a_predecessor_in_that_block_reads_none()
-> anyhow::Result<()> {
    let other = || vec![registered(versioned_token("bob", 1), "bob", "bob")];
    assert_skipped(
        announced_between(other(), vec![regenerated(U256::from(7), U256::from(8))]),
        &[2],
        "token_regenerated_without_predecessor",
    )?;
    for after in [
        vec![linked(U256::from(7), 100)],
        vec![linked(U256::from(8), 99), roles_changed(99)],
    ] {
        let output = interpret_test_batch(announced_between(other(), after))?;
        assert!(output.decode_skips.is_empty(), "{:#?}", output.decode_skips);
    }
    Ok(())
}

/// The registry announces itself before its registration, link and regeneration, in the order
/// upstream emits them, here without the mint and role logs between
/// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L113 @ ens_v2@a971bd64)
/// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L467-L471 @ ens_v2@a971bd64)
/// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L540 @ ens_v2@a971bd64).
/// The block is interpreted again once the announcement admits the registry, and every output
/// vector is the one a single pass writes.
#[test]
fn an_announcement_first_block_keeps_its_output() -> anyhow::Result<()> {
    let old_token = versioned_token("alice", 1);
    let output = interpret_test_batch(announced_between(
        Vec::new(),
        vec![
            registered(old_token, "alice", "alice"),
            linked(old_token, 99),
            regenerated(old_token, versioned_token("alice", 2)),
        ],
    ))?;
    let events = output
        .normalized_events
        .iter()
        .map(|event| (event.log_index, event.event_kind.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(
        events,
        [
            (Some(0), "RegistryCreated"),
            (Some(1), "RegistrationGranted"),
            (Some(1), "PreimageObserved"),
            (Some(2), "TokenResourceLinked"),
            (Some(3), "TokenRegenerated"),
        ]
    );
    let edges = output
        .discovery_edges
        .iter()
        .map(|edge| (edge.edge_kind.as_str(), edge.discovery_source.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(edges, [("registry_announcement", "RegistryCreated")]);
    assert!(output.decode_skips.is_empty(), "{:#?}", output.decode_skips);
    Ok(())
}

/// Discovery is forward-only across blocks. The log of an emitter announced only in the next block
/// stays uninterpreted and is recorded once as preceding its admission. A pointer log before its
/// own registry's announcement in the same block runs in its place.
#[test]
fn a_log_before_its_emitters_announcement_block_is_recorded_as_pre_admission() -> anyhow::Result<()>
{
    const LATER: &str = "0x0000000000000000000000000000000000000062";
    let pointer = v2_registry::SubregistryUpdated {
        tokenId: U256::from(7),
        subregistry: Address::repeat_byte(0x53),
        sender: sender(),
    }
    .encode_log_data();
    let output = interpret_test_batch(batch(
        Vec::new(),
        vec![
            raw_at(pointer, 1, 0, CHILD),
            raw_at(registered(U256::from(8), "bob", "bob"), 1, 1, LATER),
            raw_at(RegistryCreated {}.encode_log_data(), 1, 2, CHILD),
            raw_at(RegistryCreated {}.encode_log_data(), 2, 0, LATER),
        ],
    ))?;
    let events = output
        .normalized_events
        .iter()
        .map(|event| {
            (
                event.block_number,
                event.log_index,
                event.event_kind.as_str(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        events,
        [
            (Some(1), Some(0), "SubregistryChanged"),
            (Some(1), Some(2), "RegistryCreated"),
            (Some(2), Some(0), "RegistryCreated"),
        ]
    );
    assert_eq!(output.decode_skips.len(), 1, "{:#?}", output.decode_skips);
    let skip = &output.decode_skips[0];
    assert_eq!(
        (
            skip.block_number,
            skip.log_index,
            skip.emitting_address.as_str()
        ),
        (1, 1, LATER)
    );
    assert_eq!(skip.inconsistency_site, None);
    assert!(
        skip.decode_context
            .starts_with("log precedes its emitter's discovery admission at block 2"),
        "{}",
        skip.decode_context
    );
    Ok(())
}

const FIRST: &str = "0x0000000000000000000000000000000000000071";
const SECOND: &str = "0x0000000000000000000000000000000000000072";
const THIRD: &str = "0x0000000000000000000000000000000000000073";

fn parent_updated(parent: &str, label: &str) -> alloy_primitives::LogData {
    v2_registry::ParentUpdated {
        parent: parent.parse().expect("parent address"),
        label: label.to_owned(),
        sender: sender(),
    }
    .encode_log_data()
}

fn subregistry(token: U256, registry: &str) -> alloy_primitives::LogData {
    v2_registry::SubregistryUpdated {
        tokenId: token,
        subregistry: registry.parse().expect("subregistry address"),
        sender: sender(),
    }
    .encode_log_data()
}

/// Block 1: the declared `.eth` registry registers `alice` and points it at `registries[0]`.
/// Block 2, one log per transaction: each undeclared registry claims its parent, registers its
/// label and points it at the next registry. Then the registries announce themselves in
/// `announced` order.
fn connected_registries(registries: &[&str], labels: &[&str], announced: &[usize]) -> BatchInput {
    let alice = versioned_token("alice", 1);
    let mut raw_logs = vec![
        raw_at_transaction(registered(alice, "alice", "alice"), 1, 0, 0, CONTRACT),
        raw_at_transaction(subregistry(alice, registries[0]), 1, 0, 1, CONTRACT),
    ];
    let mut encoded = Vec::new();
    for (index, (registry, label)) in registries.iter().zip(labels).enumerate() {
        let (parent, parent_label) = match index {
            0 => (CONTRACT, "alice"),
            _ => (registries[index - 1], labels[index - 1]),
        };
        let token = versioned_token(label, 1);
        encoded.push((*registry, parent_updated(parent, parent_label)));
        encoded.push((*registry, registered(token, label, label)));
        if let Some(next) = registries.get(index + 1) {
            encoded.push((*registry, subregistry(token, next)));
        }
    }
    for &index in announced {
        encoded.push((registries[index], RegistryCreated {}.encode_log_data()));
    }
    raw_logs.extend(
        (0_i64..)
            .zip(encoded)
            .map(|(log_index, (registry, encoded))| {
                raw_at_transaction(encoded, 2, log_index, log_index, registry)
            }),
    );
    batch(vec![declared()], raw_logs)
}

/// The first event that names `raw_name`: its log, transaction and kind.
fn first_naming<'a>(
    output: &'a BatchOutput,
    raw_name: &str,
) -> (Option<i64>, Option<&'a str>, &'a str) {
    let logical_name_id = output
        .name_surfaces
        .iter()
        .find(|surface| surface.raw_name() == Some(raw_name))
        .map(|surface| surface.logical_name_id.as_str())
        .unwrap_or_else(|| panic!("no surface for {raw_name}"));
    output
        .normalized_events
        .iter()
        .find(|event| event.logical_name_id.as_deref() == Some(logical_name_id))
        .map(|event| {
            (
                event.log_index,
                event.transaction_hash.as_deref(),
                event.event_kind.as_str(),
            )
        })
        .unwrap_or_else(|| panic!("no event names {raw_name}"))
}

/// `sub.alice.eth` points at the second registry before that registry claims it as its parent,
/// and the second registry announces itself before the first. In log order the claim completes
/// the mount, so `bob.sub.alice.eth` is granted at its own registration log.
#[test]
fn connected_registries_announced_in_reverse_order_grant_at_the_registration_log()
-> anyhow::Result<()> {
    let output = interpret_test_batch(connected_registries(
        &[FIRST, SECOND],
        &["sub", "bob"],
        &[1, 0],
    ))?;
    assert_eq!(
        first_naming(&output, "bob.sub.alice.eth"),
        (Some(4), Some("transaction-2-4"), "RegistrationGranted")
    );
    assert!(output.decode_skips.is_empty(), "{:#?}", output.decode_skips);
    Ok(())
}

#[test]
fn connected_registries_announced_in_forward_order_grant_at_the_registration_log()
-> anyhow::Result<()> {
    let output = interpret_test_batch(connected_registries(
        &[FIRST, SECOND],
        &["sub", "bob"],
        &[0, 1],
    ))?;
    assert_eq!(
        first_naming(&output, "bob.sub.alice.eth"),
        (Some(4), Some("transaction-2-4"), "RegistrationGranted")
    );
    assert!(output.decode_skips.is_empty(), "{:#?}", output.decode_skips);
    Ok(())
}

/// Each registry's names are granted at their own registration logs although the chain
/// announces from the bottom up.
#[test]
fn a_three_registry_chain_announced_in_reverse_grants_at_each_registration_log()
-> anyhow::Result<()> {
    let output = interpret_test_batch(connected_registries(
        &[FIRST, SECOND, THIRD],
        &["sub", "deep", "bob"],
        &[2, 1, 0],
    ))?;
    for (raw_name, log_index) in [
        ("sub.alice.eth", 1),
        ("deep.sub.alice.eth", 4),
        ("bob.deep.sub.alice.eth", 7),
    ] {
        assert_eq!(
            first_naming(&output, raw_name),
            (
                Some(log_index),
                Some(format!("transaction-2-{log_index}").as_str()),
                "RegistrationGranted"
            ),
            "{raw_name}"
        );
    }
    Ok(())
}

/// Two registries interleave their logs and announce after them. The first pass admits both,
/// and the second admits nothing new, so it is the last.
#[test]
fn interleaved_announcements_take_exactly_two_passes() -> anyhow::Result<()> {
    crate::schema_v2::session::reset_block_passes();
    let output = interpret_test_batch(batch(
        Vec::new(),
        vec![
            raw_at(registered(versioned_token("x", 1), "x", "x"), 1, 0, SECOND),
            raw_at(registered(versioned_token("y", 1), "y", "y"), 1, 1, FIRST),
            raw_at(RegistryCreated {}.encode_log_data(), 1, 2, FIRST),
            raw_at(registered(versioned_token("z", 1), "z", "z"), 1, 3, SECOND),
            raw_at(RegistryCreated {}.encode_log_data(), 1, 4, SECOND),
        ],
    ))?;
    assert_eq!(
        crate::schema_v2::session::block_passes(),
        std::collections::BTreeMap::from([(1, 2)])
    );
    let granted = output
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "RegistrationGranted")
        .map(|event| event.log_index)
        .collect::<Vec<_>>();
    assert_eq!(granted, [Some(0), Some(1), Some(3)]);
    assert!(output.decode_skips.is_empty(), "{:#?}", output.decode_skips);
    Ok(())
}

/// Every admission a block makes is found in its first pass, so the second pass re-admits only
/// entries the catalog holds and no block takes a third.
#[test]
fn no_block_takes_a_third_pass() -> anyhow::Result<()> {
    crate::schema_v2::session::reset_block_passes();
    interpret_test_batch(connected_registries(
        &[FIRST, SECOND, THIRD],
        &["sub", "deep", "bob"],
        &[2, 1, 0],
    ))?;
    let passes = crate::schema_v2::session::block_passes();
    assert!(passes.values().all(|passes| *passes <= 2), "{passes:?}");
    Ok(())
}

/// A registry that never announces itself stays unselected in a block another registry
/// announces in. No row is written, since no admission follows in the batch.
#[test]
fn logs_of_an_emitter_never_announced_stay_unselected_without_a_row() -> anyhow::Result<()> {
    let output = interpret_test_batch(batch(
        Vec::new(),
        vec![
            raw_at(registered(versioned_token("x", 1), "x", "x"), 1, 0, SECOND),
            raw_at(RegistryCreated {}.encode_log_data(), 1, 1, FIRST),
            raw_at(registered(versioned_token("y", 1), "y", "y"), 1, 2, SECOND),
        ],
    ))?;
    assert!(
        output
            .normalized_events
            .iter()
            .all(|event| { event.raw_fact_ref["emitting_address"].as_str() != Some(SECOND) }),
        "{:#?}",
        output.normalized_events
    );
    assert!(output.decode_skips.is_empty(), "{:#?}", output.decode_skips);
    Ok(())
}

/// A manifest-declared registry's inconsistent log in a block that announces another registry
/// stops the batch in the block's first pass.
#[test]
fn a_declared_emitters_inconsistent_log_in_an_announcing_block_stops_in_the_first_pass() {
    crate::schema_v2::session::reset_block_passes();
    let mut raw_logs = vec![raw_at(RegistryCreated {}.encode_log_data(), 1, 0, CHILD)];
    raw_logs.extend(
        label_hash_mismatch()
            .into_iter()
            .map(|raw| raw_at_index(raw, 1)),
    );
    assert_stops(
        batch(vec![declared()], raw_logs),
        "LabelRegistered label does not hash to labelHash",
    );
    assert_eq!(
        crate::schema_v2::session::block_passes(),
        std::collections::BTreeMap::from([(1, 1)])
    );
}

fn raw_at_index(mut raw: RawLogInput, log_index: i64) -> RawLogInput {
    raw.log_index = log_index;
    raw
}

/// The registry's log in the block before its announcement block stays uninterpreted with one
/// row. Its log before the announcement in the announcing block runs in its place.
#[test]
fn an_emitter_announced_in_the_second_block_runs_its_first_block_logs_as_pre_admission()
-> anyhow::Result<()> {
    let output = interpret_test_batch(batch(
        Vec::new(),
        vec![
            raw_at(registered(versioned_token("x", 1), "x", "x"), 1, 0, CHILD),
            raw_at(registered(versioned_token("y", 1), "y", "y"), 2, 0, CHILD),
            raw_at(RegistryCreated {}.encode_log_data(), 2, 1, CHILD),
        ],
    ))?;
    let granted = output
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "RegistrationGranted")
        .map(|event| (event.block_number, event.log_index))
        .collect::<Vec<_>>();
    assert_eq!(granted, [(Some(2), Some(0))]);
    let skips = output
        .decode_skips
        .iter()
        .map(|skip| (skip.block_number, skip.log_index, skip.inconsistency_site))
        .collect::<Vec<_>>();
    assert_eq!(skips, [(1, 0, None)]);
    Ok(())
}

/// A registry that points one of its tokens' resolver at itself in its announcement block logs
/// one warning for the log, although the block is interpreted twice.
#[test]
fn a_resolver_pointer_to_its_own_registry_in_an_announcing_block_warns_once() -> anyhow::Result<()>
{
    let warnings = SelfTargetWarnings::default();
    let input = batch(
        Vec::new(),
        vec![
            raw_at(RegistryCreated {}.encode_log_data(), 1, 0, CHILD),
            raw_at(
                v2_registry::ResolverUpdated {
                    tokenId: U256::from(7),
                    resolver: CHILD.parse()?,
                    sender: sender(),
                }
                .encode_log_data(),
                1,
                1,
                CHILD,
            ),
        ],
    );
    tracing::subscriber::with_default(warnings.clone(), || {
        crate::schema_v2::prepare_schema_v2_batch_incremental(
            input,
            None,
            crate::schema_v2::StateCacheCapacity::Unlimited,
        )
    })?;
    assert_eq!(warnings.count(), 1);
    Ok(())
}

/// Counts the self-target discovery warnings logged on the test's thread.
#[derive(Clone, Default)]
struct SelfTargetWarnings(std::sync::Arc<std::sync::atomic::AtomicUsize>);

impl SelfTargetWarnings {
    fn count(&self) -> usize {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
}

struct Message(bool);

impl tracing::field::Visit for Message {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}").starts_with("discovery pointer targets its own emitter");
        }
    }
}

impl tracing::Subscriber for SelfTargetWarnings {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        *metadata.level() == tracing::Level::WARN
    }

    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        let mut message = Message(false);
        event.record(&mut message);
        if message.0 {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    fn enter(&self, _: &tracing::span::Id) {}

    fn exit(&self, _: &tracing::span::Id) {}
}

/// The token's link comes from an earlier batch, so the check reads restored state, on both
/// loaders. The conflicting link is skipped, and the regeneration of the same token still applies.
#[test]
fn an_inconsistent_log_against_restored_state_is_skipped() -> anyhow::Result<()> {
    let old_token = versioned_token("alice", 1);
    let first = interpret_test_batch(batch(
        vec![undeclared()],
        logs(vec![
            registered(old_token, "alice", "alice"),
            linked(old_token, 99),
        ]),
    ))?;
    let mut input = batch(
        vec![undeclared()],
        vec![
            raw_at(linked(old_token, 100), 2, 0, CONTRACT),
            raw_at(
                v2_registry::TokenRegenerated {
                    oldTokenId: old_token,
                    newTokenId: versioned_token("alice", 2),
                }
                .encode_log_data(),
                2,
                1,
                CONTRACT,
            ),
        ],
    );
    input.prior_events = first.normalized_events.iter().map(prior_event).collect();
    let output = assert_skipped(input, &[0], "token_resource_conflict")?;
    let kinds = output
        .normalized_events
        .iter()
        .map(|event| (event.log_index, event.event_kind.as_str()))
        .collect::<Vec<_>>();
    assert!(kinds.contains(&(Some(1), "TokenRegenerated")), "{kinds:?}");
    assert!(
        kinds.iter().all(|(log_index, _)| *log_index != Some(0)),
        "{kinds:?}"
    );
    Ok(())
}

/// Batch 2 of the undeclared registry, restored from batch 1's events, holds one role change on
/// resource 99.
fn role_change_after(first: Vec<alloy_primitives::LogData>) -> anyhow::Result<BatchInput> {
    let first = interpret_test_batch(batch(vec![undeclared()], logs(first)))?;
    let mut input = batch(
        vec![undeclared()],
        vec![raw_at(roles_changed(99), 2, 0, CONTRACT)],
    );
    input.prior_events = first.normalized_events.iter().map(prior_event).collect();
    Ok(input)
}

/// Both tokens linked to resource 99 come back from an earlier batch, on both loaders, so the
/// role change still names two tokens.
#[test]
fn a_role_change_on_a_resource_two_restored_tokens_link_is_skipped() -> anyhow::Result<()> {
    let input = role_change_after(vec![linked(U256::from(7), 99), linked(U256::from(8), 99)])?;
    assert_skipped(input, &[0], "token_resource_ambiguous")?;
    Ok(())
}

/// Token 8 is released in the earlier batch, so only token 7 is restored for resource 99 and the
/// role change applies to it.
#[test]
fn a_role_change_after_one_of_two_linked_tokens_is_released_applies() -> anyhow::Result<()> {
    let input = role_change_after(vec![
        linked(U256::from(7), 99),
        linked(U256::from(8), 99),
        v2_registry::LabelUnregistered {
            tokenId: U256::from(8),
            sender: sender(),
        }
        .encode_log_data(),
    ])?;
    let output = interpret_test_batch(input)?;
    assert!(output.decode_skips.is_empty(), "{:#?}", output.decode_skips);
    let kinds = output
        .normalized_events
        .iter()
        .map(|event| (event.log_index, event.event_kind.as_str()))
        .collect::<Vec<_>>();
    assert!(kinds.contains(&(Some(0), "PermissionChanged")), "{kinds:?}");
    Ok(())
}

#[test]
fn an_undeclared_malformed_log_keeps_its_decode_context() -> anyhow::Result<()> {
    let mut raw_logs = label_hash_mismatch();
    raw_logs[0].data.truncate(32);
    let output = interpret_test_batch(batch(vec![undeclared()], raw_logs))?;
    assert_eq!(output.decode_skips.len(), 1);
    assert_eq!(
        output.decode_skips[0].decode_context,
        "LabelRegistered log is malformed"
    );
    assert_eq!(output.decode_skips[0].inconsistency_site, None);
    Ok(())
}

/// A failure after the log's state is committed is not a property of the log: the announcement
/// rule is missing from the manifest.
#[test]
fn an_undeclared_announcement_without_its_rule_still_stops() {
    let mut input = batch(
        vec![undeclared()],
        logs(vec![RegistryCreated {}.encode_log_data()]),
    );
    input.discovery_rules.clear();
    assert_stops(
        input,
        "RegistryCreated is not admitted by a registry_announcement manifest rule",
    );
}

/// A manifest that does not declare the normalized event an adapter writes is a manifest defect,
/// not an inconsistent log.
#[test]
fn a_missing_normalized_event_declaration_stops_for_every_emitter() {
    for admitted in [undeclared(), declared()] {
        let mut input = batch(
            vec![admitted],
            logs(vec![registered(U256::from(7), "alice", "alice")]),
        );
        input.manifests = vec![manifest_with_events(
            MANIFEST,
            "ens",
            "ens_v2_registry_l1",
            &[(
                "LabelRegistered",
                LABEL_REGISTERED,
                &["registry"],
                &["RegistrationReserved"],
            )],
        )];
        assert_stops(
            input,
            "does not declare required normalized event RegistrationGranted",
        );
    }
}
