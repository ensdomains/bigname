use std::panic::{AssertUnwindSafe, catch_unwind};

use alloy_primitives::keccak256;
use serde_json::json;
use uuid::Uuid;

use super::*;

const REGISTRY: &str = "0x0000000000000000000000000000000000000043";
const REGISTRAR: &str = "0x0000000000000000000000000000000000000057";
const STRANGER: &str = "0x00000000000000000000000000000000000000aa";
const NEW_OWNER: &str =
    "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)";
const NEW_OWNER_SIGNATURE: &str = "NewOwner(bytes32,bytes32,address)";
const TRANSFER: &str = "event Transfer(bytes32 indexed node, address owner)";

fn topic(signature: &str) -> String {
    format!("{:#x}", keccak256(signature.as_bytes()))
}

fn manifest(manifest_id: i64, source_family: &str, events: &[(&str, &[&str])]) -> Manifest {
    manifest_with(manifest_id, source_family, events, json!({}))
}

fn manifest_with(
    manifest_id: i64,
    source_family: &str,
    events: &[(&str, &[&str])],
    extra: serde_json::Value,
) -> Manifest {
    let mut payload = extra;
    payload["abi"] = json!({
        "events": events
            .iter()
            .map(|(fragment, roles)| json!({"fragment": fragment, "emitter_roles": roles}))
            .collect::<Vec<_>>(),
    });
    Manifest {
        manifest_id,
        namespace: "ens".to_owned(),
        source_family: source_family.to_owned(),
        chain_id: "1".to_owned(),
        deployment_label: "fixture".to_owned(),
        payload_json: payload.to_string(),
    }
}

fn admission(address: &str, manifest_id: i64, role: &str) -> Admission {
    Admission {
        address: address.to_owned(),
        source_manifest_id: Some(manifest_id),
        role: Some(role.to_owned()),
        ..Admission::default()
    }
}

fn log(block_number: i64, log_index: i64, emitter: &str, topics: Vec<String>) -> Log {
    Log {
        block_number,
        block_hash: format!("0xblock{block_number}"),
        transaction_hash: format!("0xtx{block_number}"),
        transaction_index: 0,
        log_index,
        emitting_address: emitter.to_owned(),
        topics,
    }
}

fn registry_batch(logs: Vec<Log>) -> Batch {
    Batch {
        manifests: vec![
            manifest(
                1,
                "ens_v1_registrar_l1",
                &[(
                    "event NameRenewed(uint256 indexed id, uint256 expires)",
                    &["registrar"],
                )],
            ),
            manifest(
                2,
                "ens_v1_registry_l1",
                &[(NEW_OWNER, &["registry"]), (TRANSFER, &["registry"])],
            ),
        ],
        rules: Vec::new(),
        admissions: vec![
            admission(REGISTRAR, 1, "registrar"),
            admission(REGISTRY, 2, "registry"),
        ],
        logs,
    }
}

fn panic_message(result: std::thread::Result<()>) -> String {
    let payload = result.expect_err("the check must fail");
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| {
            payload
                .downcast_ref::<&str>()
                .map(|text| (*text).to_owned())
        })
        .unwrap_or_default()
}

fn check_message(batch: &Batch, outcome: &Outcome) -> String {
    panic_message(catch_unwind(AssertUnwindSafe(|| {
        assert_selectable(batch, outcome);
    })))
}

#[test]
fn matched_log_is_not_reported() {
    let batch = registry_batch(vec![log(1, 0, REGISTRY, vec![topic(NEW_OWNER_SIGNATURE)])]);
    assert!(unmatched(&batch, &Outcome::default()).is_empty());
    assert_selectable(&batch, &Outcome::default());
}

#[test]
fn undeclared_topic_reports_the_admitting_manifest_and_its_events() {
    // A sol! struct named RegistryNewOwner encodes this topic0, which no manifest declares.
    let renamed = topic("RegistryNewOwner(bytes32,bytes32,address)");
    let batch = registry_batch(vec![log(1, 0, REGISTRY, vec![renamed.clone()])]);
    let found = unmatched(&batch, &Outcome::default());
    assert_eq!(found.len(), 1);
    let Why::NoTopic { admitted, declared } = &found[0].why else {
        panic!("expected a no-topic report, got {:?}", found[0].why);
    };
    assert_eq!(admitted.len(), 1);
    assert_eq!(admitted[0].manifest_id, 2);
    assert_eq!(admitted[0].role.as_deref(), Some("registry"));
    assert_eq!(
        declared
            .iter()
            .map(|event| event.signature.as_str())
            .collect::<Vec<_>>(),
        [NEW_OWNER_SIGNATURE, "Transfer(bytes32,address)"]
    );
    let message = check_message(&batch, &Outcome::default());
    for expected in [
        "undeclared_topic_reports_the_admitting_manifest_and_its_events",
        REGISTRY,
        renamed.as_str(),
        "no topic",
        "role registry by manifest 2 (ens_v1_registry_l1)",
        NEW_OWNER_SIGNATURE,
        "allow_unmatched(",
    ] {
        assert!(
            message.contains(expected),
            "{expected} missing from:\n{message}"
        );
    }
}

#[test]
fn declared_topic_under_another_role_is_a_role_mismatch() {
    // NewOwner is declared for the registry role only; the registrar emits it.
    let batch = registry_batch(vec![log(1, 0, REGISTRAR, vec![topic(NEW_OWNER_SIGNATURE)])]);
    let found = unmatched(&batch, &Outcome::default());
    assert_eq!(found.len(), 1);
    let Why::RoleMismatch { admitted, declared } = &found[0].why else {
        panic!("expected a role mismatch, got {:?}", found[0].why);
    };
    assert_eq!(admitted[0].role.as_deref(), Some("registrar"));
    assert_eq!(declared[0].emitter_roles, ["registry"]);
    let message = check_message(&batch, &Outcome::default());
    assert!(message.contains("role mismatch"), "{message}");
    assert!(message.contains("roles [registry]"), "{message}");
    assert!(message.contains("role registrar"), "{message}");
}

#[test]
fn log_from_an_unadmitted_emitter_names_the_manifests_that_declare_its_topic() {
    let batch = registry_batch(vec![log(1, 0, STRANGER, vec![topic(NEW_OWNER_SIGNATURE)])]);
    let found = unmatched(&batch, &Outcome::default());
    assert_eq!(found.len(), 1);
    let Why::NoAdmission { declared_by } = &found[0].why else {
        panic!("expected a no-admission report, got {:?}", found[0].why);
    };
    assert_eq!(declared_by.len(), 1);
    assert_eq!(declared_by[0].manifest_id, 2);
    let message = check_message(&batch, &Outcome::default());
    assert!(
        message.contains("no admission covers the emitter"),
        "{message}"
    );
    assert!(
        message.contains("manifest 2 (ens_v1_registry_l1)"),
        "{message}"
    );
}

#[test]
fn a_log_with_no_topics_is_reported() {
    let batch = registry_batch(vec![log(1, 0, REGISTRY, Vec::new())]);
    let found = unmatched(&batch, &Outcome::default());
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].why, Why::NoTopic0);
}

#[test]
fn an_allowance_accepts_and_is_marked_used() {
    let undeclared = "Undeclared(address)";
    for key in [
        undeclared.to_owned(),
        topic(undeclared),
        REGISTRY.to_ascii_uppercase().replace("0X", "0x"),
    ] {
        let batch = registry_batch(vec![log(1, 0, REGISTRY, vec![topic(undeclared)])]);
        let guard = allow_unmatched(&[(key.as_str(), "the test drops this log on purpose")]);
        assert_selectable(&batch, &Outcome::default());
        // Dropping the guard checks that the entry was used; it was, so this does not panic.
        drop(guard);
    }
}

#[test]
fn an_allowance_only_accepts_the_logs_it_names() {
    let batch = registry_batch(vec![
        log(1, 0, REGISTRY, vec![topic("Undeclared(address)")]),
        log(1, 1, REGISTRY, vec![topic("Other(address)")]),
    ]);
    let _guard = allow_unmatched(&[("Undeclared(address)", "dropped on purpose")]);
    let message = check_message(&batch, &Outcome::default());
    assert!(message.contains("1 fixture log(s)"), "{message}");
    assert!(message.contains(&topic("Other(address)")), "{message}");
}

#[test]
fn an_unused_allowance_fails_when_its_scope_ends() {
    let batch = registry_batch(vec![log(1, 0, REGISTRY, vec![topic(NEW_OWNER_SIGNATURE)])]);
    let message = panic_message(catch_unwind(AssertUnwindSafe(|| {
        let _guard = allow_unmatched(&[("Undeclared(address)", "a stale reason")]);
        assert_selectable(&batch, &Outcome::default());
    })));
    assert!(message.contains("matched no unmatched log"), "{message}");
    assert!(
        message.contains("Undeclared(address) (a stale reason)"),
        "{message}"
    );
    assert!(
        message.contains("an_unused_allowance_fails_when_its_scope_ends"),
        "{message}"
    );
}

#[test]
fn an_allowance_needs_a_reason_and_a_recognisable_key() {
    for (key, reason, expected) in [
        ("Undeclared(address)", " ", "needs a reason"),
        ("0x1234", "reason", "neither a topic0"),
        ("Undeclared", "reason", "neither a topic0"),
    ] {
        let message = panic_message(catch_unwind(|| {
            let _guard = allow_unmatched(&[(key, reason)]);
        }));
        assert!(message.contains(expected), "{key}: {message}");
    }
}

#[test]
fn captured_receipt_topics_need_an_allowance() {
    // A captured receipt carries every log of its transactions, including events of the
    // admitted contracts that no manifest declares.
    let ownership = "OwnershipTransferred(address,address)";
    let receipt = || {
        registry_batch(vec![
            log(1, 0, REGISTRY, vec![topic(ownership)]),
            log(1, 1, REGISTRY, vec![topic(NEW_OWNER_SIGNATURE)]),
            log(1, 2, REGISTRY, vec![topic(ownership)]),
        ])
    };
    let found = unmatched(&receipt(), &Outcome::default());
    assert_eq!(
        found
            .iter()
            .map(|item| item.log.log_index)
            .collect::<Vec<_>>(),
        [0, 2]
    );
    let _guard = allow_unmatched(&[(
        ownership,
        "captured on-chain receipt; production drops this topic identically",
    )]);
    assert_selectable(&receipt(), &Outcome::default());
}

#[test]
fn a_recorded_pre_admission_skip_is_accepted() {
    let early = log(1, 0, STRANGER, vec![topic(NEW_OWNER_SIGNATURE)]);
    let outcome = Outcome {
        decode_skips: [(
            early.block_hash.clone(),
            early.transaction_hash.clone(),
            early.log_index,
        )]
        .into_iter()
        .collect(),
        ..Outcome::default()
    };
    assert!(unmatched(&registry_batch(vec![early]), &outcome).is_empty());
}

#[test]
fn an_unrecorded_pre_admission_drop_is_reported() {
    let mut batch = registry_batch(vec![log(5, 0, STRANGER, vec![topic(NEW_OWNER_SIGNATURE)])]);
    batch.admissions.push(Admission {
        address: STRANGER.to_owned(),
        source_manifest_id: Some(2),
        discovery_edge_kind: Some("resolver".to_owned()),
        active_from_block: Some(10),
        ..Admission::default()
    });
    let found = unmatched(&batch, &Outcome::default());
    assert_eq!(found.len(), 1);
    assert_eq!(
        found[0].why,
        Why::PreAdmissionUnrecorded { admitted_at: 10 }
    );
}

const SUBREGISTRY_UPDATED: &str =
    "event SubregistryUpdated(uint256 indexed tokenId, address subregistry)";
const SUBREGISTRY_UPDATED_SIGNATURE: &str = "SubregistryUpdated(uint256,address)";
const ANNOUNCED: &str = "0x00000000000000000000000000000000000000bb";

fn announcement_batch(logs: Vec<Log>) -> Batch {
    Batch {
        manifests: vec![manifest(
            1,
            "ens_v2_registry_l1",
            &[(SUBREGISTRY_UPDATED, &["registry"])],
        )],
        rules: Vec::new(),
        admissions: Vec::new(),
        logs,
    }
}

fn announced_instance() -> Uuid {
    Uuid::from_u128(0xbb)
}

fn announcement_outcome(at: Position) -> Outcome {
    let key = format!("registry-announcement:{ANNOUNCED}");
    Outcome {
        edges: vec![Edge {
            edge_kind: "registry_announcement".to_owned(),
            from: announced_instance(),
            to: announced_instance(),
            source_manifest_id: 1,
            observation_key: key,
            position: at,
        }],
        contract_addresses: vec![(announced_instance(), ANNOUNCED.to_owned(), at.0)],
        ..Outcome::default()
    }
}

#[test]
fn a_log_from_an_emitter_discovered_in_the_batch_is_matched() {
    let subregistry = || vec![topic(SUBREGISTRY_UPDATED_SIGNATURE)];
    // The announcement is log 1 of block 5. Log 0 of that block precedes it but is still read:
    // the adapter re-reads the block once the announcement admits the emitter.
    let batch = announcement_batch(vec![
        log(5, 0, ANNOUNCED, subregistry()),
        log(5, 2, ANNOUNCED, subregistry()),
        log(6, 0, ANNOUNCED, subregistry()),
    ]);
    assert!(unmatched(&batch, &announcement_outcome((5, 0, 1))).is_empty());
    // Without the in-batch admission every log is unadmitted.
    assert_eq!(unmatched(&batch, &Outcome::default()).len(), 3);
    // A log in an earlier block than the admission is not read.
    let earlier = announcement_batch(vec![log(4, 0, ANNOUNCED, subregistry())]);
    assert_eq!(
        unmatched(&earlier, &announcement_outcome((5, 0, 1))).len(),
        1
    );
}

#[test]
fn a_log_after_its_admission_is_retired_is_reported() {
    let subregistry = || vec![topic(SUBREGISTRY_UPDATED_SIGNATURE)];
    let batch = announcement_batch(vec![
        log(6, 0, ANNOUNCED, subregistry()),
        log(7, 3, ANNOUNCED, subregistry()),
        log(7, 5, ANNOUNCED, subregistry()),
        log(8, 0, ANNOUNCED, subregistry()),
    ]);
    let mut outcome = announcement_outcome((5, 0, 1));
    outcome.closures.push(Closure {
        edge_kind: "registry_announcement".to_owned(),
        from: announced_instance(),
        observation_key: format!("registry-announcement:{ANNOUNCED}"),
        position: (7, 0, 3),
    });
    // The closure at log 3 of block 7 is read with its admission; later logs are not.
    assert_eq!(
        unmatched(&batch, &outcome)
            .iter()
            .map(|item| item.log.position())
            .collect::<Vec<_>>(),
        [(7, 0, 5), (8, 0, 0)]
    );
    // An input admission with the same observation is retired by the closure too.
    let mut declared = batch.clone();
    declared.admissions.push(Admission {
        address: ANNOUNCED.to_owned(),
        source_manifest_id: Some(1),
        discovery_edge_kind: Some("registry_announcement".to_owned()),
        discovery_from: Some(announced_instance()),
        discovery_observation_key: Some(format!("registry-announcement:{ANNOUNCED}")),
        ..Admission::default()
    });
    let closure_only = Outcome {
        closures: outcome.closures.clone(),
        ..Outcome::default()
    };
    assert_eq!(unmatched(&declared, &closure_only).len(), 2);
}

const TEXT_CHANGED: &str = "event TextChanged(bytes32 indexed node, string indexed indexedKey, \
                            string key, string value)";
const TEXT_CHANGED_SIGNATURE: &str = "TextChanged(bytes32,string,string,string)";
const APPROVAL_FOR_ALL: &str =
    "event ApprovalForAll(address indexed owner, address indexed operator, bool approved)";
const APPROVAL_FOR_ALL_SIGNATURE: &str = "ApprovalForAll(address,address,bool)";
const POINTED: &str = "0x00000000000000000000000000000000000000cc";

/// A registry manifest (`registry_family`) and a resolver manifest (`resolver_family`) declaring
/// `event` (with `signature`) for any role, and two logs of it from `POINTED`, either side of the
/// registry pointing a name at it at block 5.
fn pointer_batch(
    registry_family: &str,
    resolver_family: &str,
    event: &str,
    signature: &str,
) -> (Batch, Outcome) {
    let batch = Batch {
        manifests: vec![
            manifest(1, registry_family, &[(SUBREGISTRY_UPDATED, &["registry"])]),
            manifest(2, resolver_family, &[(event, &[])]),
        ],
        rules: Vec::new(),
        admissions: Vec::new(),
        logs: vec![
            log(4, 0, POINTED, vec![topic(signature)]),
            log(6, 0, POINTED, vec![topic(signature)]),
        ],
    };
    let pointed = Uuid::from_u128(0xcc);
    let outcome = Outcome {
        edges: vec![Edge {
            edge_kind: "resolver".to_owned(),
            from: Uuid::from_u128(0x43),
            to: pointed,
            source_manifest_id: 1,
            observation_key: "resolver:fixture".to_owned(),
            position: (5, 0, 0),
        }],
        contract_addresses: vec![(pointed, POINTED.to_owned(), 5)],
        ..Outcome::default()
    };
    (batch, outcome)
}

#[test]
fn a_resolver_edge_infers_the_resolver_family_unless_a_registry_pointer_made_it() {
    // A Basenames registry's resolver edge admits the resolver, and its event comes from the
    // inferred resolver family. The log before the edge's block is not read. (Basenames resolver
    // records match from any emitter, so the row uses an address-scoped approval, which does not.)
    let (batch, outcome) = pointer_batch(
        "basenames_base_registry",
        "basenames_base_resolver",
        APPROVAL_FOR_ALL,
        APPROVAL_FOR_ALL_SIGNATURE,
    );
    let reported = unmatched(&batch, &outcome);
    assert_eq!(
        reported
            .iter()
            .map(|item| item.log.position())
            .collect::<Vec<_>>(),
        [(4, 0, 0)]
    );
    assert_eq!(
        reported[0].why,
        Why::PreAdmissionUnrecorded { admitted_at: 5 }
    );
    // An ENSv2 registry's resolver pointer binds the name and admits nothing (`discovery.rs`), so
    // both logs are unadmitted, and neither is blamed on a missing pre-admission skip.
    let (batch, outcome) = pointer_batch(
        "ens_v2_registry_l1",
        "ens_v2_resolver_l1",
        TEXT_CHANGED,
        TEXT_CHANGED_SIGNATURE,
    );
    let reported = unmatched(&batch, &outcome);
    assert_eq!(reported.len(), 2);
    assert!(
        reported
            .iter()
            .all(|item| matches!(item.why, Why::NoAdmission { .. })),
        "{reported:?}"
    );
}

#[test]
fn match_all_and_announced_implementation_candidates_count_as_matched() {
    let implementation = "0x00000000000000000000000000000000000000cc";
    let upgraded = "Upgraded(address)";
    let batch = |block| Batch {
        manifests: vec![
            manifest(
                1,
                "ens_v1_resolver_l1",
                &[(
                    "event AddrChanged(bytes32 indexed node, address a)",
                    &["resolver"],
                )],
            ),
            manifest_with(
                2,
                "ens_v2_resolver_l1",
                &[(
                    "event Upgraded(address indexed implementation)",
                    &["resolver"],
                )],
                json!({"resolver_implementations": [{"address": implementation, "start_block": 10}]}),
            ),
        ],
        rules: Vec::new(),
        admissions: Vec::new(),
        logs: vec![
            log(
                block,
                0,
                STRANGER,
                vec![topic("AddrChanged(bytes32,address)")],
            ),
            log(
                block,
                1,
                STRANGER,
                vec![
                    topic(upgraded),
                    format!("0x{:0>64}", implementation.trim_start_matches("0x")),
                ],
            ),
        ],
    };
    assert!(unmatched(&batch(10), &Outcome::default()).is_empty());
    // An announcement before the implementation's declared start selects nothing.
    let early = unmatched(&batch(9), &Outcome::default());
    assert_eq!(
        early
            .iter()
            .map(|item| item.log.log_index)
            .collect::<Vec<_>>(),
        [1]
    );
}

#[test]
fn a_public_resolver_refusal_is_reported() {
    let resolver = "0x00000000000000000000000000000000000000dd";
    let batch = Batch {
        manifests: vec![manifest(
            1,
            "ens_v2_resolver_l1",
            &[(
                "event AliasChanged(bytes indexed fromName, bytes toName)",
                &["public_resolver_v2"],
            )],
        )],
        rules: Vec::new(),
        admissions: vec![admission(resolver, 1, "public_resolver_v2")],
        logs: vec![log(
            1,
            0,
            resolver,
            vec![topic("AliasChanged(bytes,bytes)")],
        )],
    };
    let found = unmatched(&batch, &Outcome::default());
    assert_eq!(found.len(), 1);
    assert!(
        matches!(found[0].why, Why::Refused { .. }),
        "{:?}",
        found[0].why
    );
    assert_eq!(static_selection(&batch), [false]);
}

#[test]
fn each_chunk_is_judged_on_its_own_admissions() {
    let subregistry = || vec![topic(SUBREGISTRY_UPDATED_SIGNATURE)];
    // Chunk 1 announces the emitter and reads its log.
    let first = announcement_batch(vec![log(5, 2, ANNOUNCED, subregistry())]);
    assert!(unmatched(&first, &announcement_outcome((5, 0, 1))).is_empty());
    // Chunk 2 does not look back at chunk 1's output: its log needs the admission in its input,
    // which is what the loader reads back from the stored edge.
    let mut second = announcement_batch(vec![log(9, 0, ANNOUNCED, subregistry())]);
    assert_eq!(unmatched(&second, &Outcome::default()).len(), 1);
    second.admissions.push(Admission {
        address: ANNOUNCED.to_owned(),
        source_manifest_id: Some(1),
        discovery_edge_kind: Some("registry_announcement".to_owned()),
        discovery_from: Some(announced_instance()),
        discovery_observation_key: Some(format!("registry-announcement:{ANNOUNCED}")),
        active_from_block: Some(5),
        ..Admission::default()
    });
    assert!(unmatched(&second, &Outcome::default()).is_empty());
}

#[test]
fn the_drift_guard_passes_when_oracle_and_catalog_agree() {
    let batch = registry_batch(vec![
        log(1, 0, REGISTRY, vec![topic(NEW_OWNER_SIGNATURE)]),
        log(1, 1, REGISTRY, vec![topic("Undeclared(address)")]),
    ]);
    assert_matches_catalog(&batch, &[Some(true), Some(false)]);
    // A selector error is not compared: the adapter fails such a batch on its own.
    assert_matches_catalog(&batch, &[None, Some(false)]);
}

#[test]
fn a_drift_between_oracle_and_catalog_fails() {
    let batch = registry_batch(vec![
        log(1, 0, REGISTRY, vec![topic(NEW_OWNER_SIGNATURE)]),
        log(1, 1, REGISTRY, vec![topic("Undeclared(address)")]),
    ]);
    let message = panic_message(catch_unwind(|| {
        assert_matches_catalog(&batch, &[Some(true), Some(true)]);
    }));
    assert!(
        message.contains("disagrees with Catalog::select"),
        "{message}"
    );
    assert!(
        message.contains("log_index 1") && message.contains("Catalog::select selects"),
        "{message}"
    );
    assert!(message.contains("fixture-log oracle drops"), "{message}");
}

mod red_demo;
