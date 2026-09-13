//! NameWrapper holder, operator, and per-token delegate permission rows.
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L214-L238 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L815-L840 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L105-L117 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L269-L278 @ ens_v1@91c966f)

use super::*;

const MANIFEST_ID: i64 = 7301;
const HOLDER: &str = "0x00000000000000000000000000000000000000a1";
const NEXT_HOLDER: &str = "0x00000000000000000000000000000000000000a2";
const OPERATOR: &str = "0x00000000000000000000000000000000000000e1";
const DELEGATE: &str = "0x00000000000000000000000000000000000000d1";
const NEXT_DELEGATE: &str = "0x00000000000000000000000000000000000000d2";
const LAST_DELEGATE: &str = "0x00000000000000000000000000000000000000d3";
const CANNOT_UNWRAP: u32 = 1;
const CANNOT_APPROVE: u32 = 64;
const PARENT_CANNOT_CONTROL: u32 = 1 << 16;
const HOLDER_POWERS: &[&str] = &[
    "resource_control",
    "set_resolver",
    "set_ttl",
    "create_subnames",
    "transfer",
    "unwrap",
    "burn_fuses",
    "approve",
    "extend_subname_expiry",
    "extend_expiry",
];

fn wrapper_manifest() -> ManifestInput {
    manifest_with_events(
        MANIFEST_ID,
        "ens",
        "ens_v1_wrapper_l1",
        &[
            (
                "NameWrapped",
                "event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry)",
                &["name_wrapper"],
                &[
                    "TokenControlTransferred",
                    "ExpiryChanged",
                    "PermissionScopeChanged",
                    "PermissionChanged",
                ],
            ),
            (
                "TransferSingle",
                "event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)",
                &["name_wrapper"],
                &["TokenControlTransferred", "PermissionChanged"],
            ),
            (
                "TransferBatch",
                "event TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values)",
                &["name_wrapper"],
                &["TokenControlTransferred", "PermissionChanged"],
            ),
            (
                "NameUnwrapped",
                "event NameUnwrapped(bytes32 indexed node, address owner)",
                &["name_wrapper"],
                &["SurfaceUnbound", "PermissionChanged"],
            ),
            (
                "FusesSet",
                "event FusesSet(bytes32 indexed node, uint32 fuses)",
                &["name_wrapper"],
                &["PermissionScopeChanged"],
            ),
            (
                "ApprovalForAll",
                "event ApprovalForAll(address indexed owner, address indexed operator, bool approved)",
                &["name_wrapper"],
                &[],
            ),
            (
                "Approval",
                "event Approval(address indexed owner, address indexed approved, uint256 indexed tokenId)",
                &["name_wrapper"],
                &[],
            ),
        ],
    )
}

fn node() -> B256 {
    node_of(&["alice", "eth"])
}

fn node_of(labels: &[&str]) -> B256 {
    crate::schema_v2::common::namehash_raw(labels.iter().map(|label| label.as_bytes()))
        .parse()
        .expect("namehash must parse")
}

fn dns_name(labels: &[&str]) -> Vec<u8> {
    let mut encoded = Vec::new();
    for label in labels {
        encoded.push(u8::try_from(label.len()).expect("short label"));
        encoded.extend_from_slice(label.as_bytes());
    }
    encoded.push(0);
    encoded
}

fn address(value: &str) -> Address {
    value.parse().expect("address literal must parse")
}

fn wrapped(block: i64, fuses: u32) -> RawLogInput {
    wrapped_labels(block, &["alice", "eth"], fuses)
}

fn wrapped_labels(block: i64, labels: &[&str], fuses: u32) -> RawLogInput {
    raw_at(
        NameWrapped {
            node: node_of(labels),
            name: dns_name(labels).into(),
            owner: address(HOLDER),
            fuses,
            expiry: 42,
        }
        .encode_log_data(),
        block,
        0,
        CONTRACT,
    )
}

fn transfer_batch(block: i64, from: &str, to: Address, nodes: &[B256]) -> RawLogInput {
    raw_at(
        v2_registry::TransferBatch {
            operator: address(from),
            from: address(from),
            to,
            ids: nodes
                .iter()
                .map(|node| U256::from_be_bytes(node.0))
                .collect(),
            values: nodes.iter().map(|_| U256::from(1)).collect(),
        }
        .encode_log_data(),
        block,
        0,
        CONTRACT,
    )
}

fn approval_for_all(block: i64, owner: &str, operator: &str, approved: bool) -> RawLogInput {
    raw_at(
        approvals::ApprovalForAll {
            owner: address(owner),
            operator: address(operator),
            approved,
        }
        .encode_log_data(),
        block,
        0,
        CONTRACT,
    )
}

fn approval(block: i64, owner: &str, approved: Address) -> RawLogInput {
    raw_at(
        approvals::Approval {
            owner: address(owner),
            approved,
            tokenId: U256::from_be_bytes(node().0),
        }
        .encode_log_data(),
        block,
        0,
        CONTRACT,
    )
}

fn transfer(block: i64, from: &str, to: Address) -> RawLogInput {
    raw_at(
        v2_registry::TransferSingle {
            operator: address(from),
            from: address(from),
            to,
            id: U256::from_be_bytes(node().0),
            value: U256::from(1),
        }
        .encode_log_data(),
        block,
        0,
        CONTRACT,
    )
}

fn fuses_set(block: i64, fuses: u32) -> RawLogInput {
    raw_at(
        FusesSet {
            node: node(),
            fuses,
        }
        .encode_log_data(),
        block,
        0,
        CONTRACT,
    )
}

fn unwrapped(block: i64, owner: &str) -> RawLogInput {
    raw_at(
        NameUnwrapped {
            node: node(),
            owner: address(owner),
        }
        .encode_log_data(),
        block,
        0,
        CONTRACT,
    )
}

fn input(raw_logs: Vec<RawLogInput>) -> BatchInput {
    let blocks = raw_logs
        .iter()
        .map(|raw| RawBlockInput {
            chain_id: raw.chain_id.clone(),
            block_hash: raw.block_hash.clone(),
            block_number: raw.block_number,
            block_timestamp: raw.block_timestamp,
            canonicality_state: raw.canonicality_state.clone(),
        })
        .collect();
    BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![wrapper_manifest()],
        discovery_rules: Vec::new(),
        admissions: vec![admission(MANIFEST_ID, "name_wrapper")],
        prior_events: Vec::new(),
        blocks,
        raw_logs,
    }
}

/// `(block, subject, relation, granted, powers)` for every `PermissionChanged` row of `alice.eth`.
fn permission_rows(output: &BatchOutput) -> Vec<(i64, String, String, bool, serde_json::Value)> {
    permission_rows_for(output, node())
}

fn permission_rows_for(
    output: &BatchOutput,
    node: B256,
) -> Vec<(i64, String, String, bool, serde_json::Value)> {
    output
        .normalized_events
        .iter()
        .filter(|event| {
            let expected = json!(format!("{node:#x}"));
            event.event_kind == "PermissionChanged"
                && (event.after_state["grant_source"]["node"] == expected
                    || event.after_state["revocation_source"]["node"] == expected)
        })
        .map(|event| {
            let after = &event.after_state;
            let granted = !after["grant_source"].is_null();
            let source = if granted {
                &after["grant_source"]
            } else {
                &after["revocation_source"]
            };
            assert_eq!(source["kind"], "ens_v1_authority");
            assert_eq!(source["authority_kind"], "wrapper");
            assert_eq!(source["authority_contract"], CONTRACT);
            assert_eq!(source["node"], format!("{node:#x}"));
            assert_eq!(after["scope"], json!({"kind": "resource"}));
            (
                event.block_number.expect("wrapper rows carry a block"),
                after["subject"].as_str().expect("subject").to_owned(),
                source["relation_kind"]
                    .as_str()
                    .expect("relation")
                    .to_owned(),
                granted,
                after["effective_powers"].clone(),
            )
        })
        .collect()
}

fn row(
    block: i64,
    subject: &str,
    relation: &str,
    granted: bool,
    powers: &[&str],
) -> (i64, String, String, bool, serde_json::Value) {
    (
        block,
        subject.to_owned(),
        relation.to_owned(),
        granted,
        if granted { json!(powers) } else { json!([]) },
    )
}

const DELEGATE_POWERS: &[&str] = &["extend_subname_expiry"];

#[test]
fn wrapper_holder_operator_and_delegate_rows_follow_the_token_lifecycle() -> anyhow::Result<()> {
    let logs = vec![
        wrapped(1, 0),
        approval_for_all(2, HOLDER, OPERATOR, true),
        approval(3, HOLDER, address(DELEGATE)),
        approval(4, HOLDER, address(NEXT_DELEGATE)),
        transfer(5, HOLDER, address(NEXT_HOLDER)),
        approval(6, NEXT_HOLDER, address(LAST_DELEGATE)),
        unwrapped(7, NEXT_HOLDER),
    ];
    let output = interpret_test_batch(input(logs.clone()))?;

    assert_eq!(
        permission_rows(&output),
        vec![
            row(1, HOLDER, "holder", true, HOLDER_POWERS),
            row(3, DELEGATE, "token_approval", true, DELEGATE_POWERS),
            row(4, DELEGATE, "token_approval", false, DELEGATE_POWERS),
            row(4, NEXT_DELEGATE, "token_approval", true, DELEGATE_POWERS),
            row(5, NEXT_DELEGATE, "token_approval", false, DELEGATE_POWERS),
            row(5, HOLDER, "holder", false, HOLDER_POWERS),
            row(5, NEXT_HOLDER, "holder", true, HOLDER_POWERS),
            row(6, LAST_DELEGATE, "token_approval", true, DELEGATE_POWERS),
            row(7, NEXT_HOLDER, "holder", false, HOLDER_POWERS),
            row(7, LAST_DELEGATE, "token_approval", false, DELEGATE_POWERS),
        ]
    );
    let delegate = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "PermissionChanged" && event.block_number == Some(3))
        .expect("delegate grant");
    assert_eq!(
        delegate.after_state["transfer_behavior"],
        "cleared_on_transfer_unless_cannot_approve"
    );
    assert_eq!(
        delegate.after_state["grant_source"]["source_event_kind"],
        "Approval"
    );
    let holder = &output.normalized_events[0];
    assert_eq!(holder.event_kind, "TokenControlTransferred");

    // The operator approval is account state: no resource row, wrapper authority, holder powers
    // are fanned out by Project.
    let operator = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "AccountPermissionChanged")
        .expect("operator approval");
    assert_eq!(operator.derivation_kind, "standard_approval");
    assert!(operator.logical_name_id.is_none() && operator.resource_id.is_none());
    assert_eq!(
        operator.after_state["effective_powers"],
        json!(["wrapper_control"])
    );
    assert_eq!(operator.after_state["scope"]["authority_kind"], "wrapper");
    assert_eq!(
        operator.after_state["scope"]["authority_contract"],
        CONTRACT
    );
    assert_eq!(operator.after_state["scope"]["owner"], HOLDER);
    assert_eq!(operator.after_state["subject"], OPERATOR);
    assert_eq!(
        operator.after_state["transfer_behavior"],
        json!({"mode": "owner_scoped", "on_holder_change": "ceases_to_apply"})
    );

    // Restoring interpreter state from the earlier rows recovers the live delegate, so the
    // transfer in the tail still revokes it.
    let head = input(logs[..4].to_vec());
    let (first, _) = interpret_test_batch_incremental(head.clone(), None)?;
    let mut tail = input(logs[4..].to_vec());
    for prior in [
        first.normalized_events.iter().map(prior_event).collect(),
        seam::fold_prior_events(vec![], &first.normalized_events, &head.blocks)?,
    ] {
        tail.prior_events = prior;
        let restored = interpret_test_batch(tail.clone())?;
        assert_eq!(
            permission_rows(&restored),
            permission_rows(&output)
                .into_iter()
                .filter(|(block, ..)| *block >= 5)
                .collect::<Vec<_>>()
        );
    }
    Ok(())
}

#[test]
fn cannot_approve_keeps_the_delegate_across_transfer_and_a_burn_revokes_everything()
-> anyhow::Result<()> {
    let output = interpret_test_batch(input(vec![
        wrapped(1, 0),
        fuses_set(2, PARENT_CANNOT_CONTROL | CANNOT_UNWRAP | CANNOT_APPROVE),
        approval(3, HOLDER, address(DELEGATE)),
        transfer(4, HOLDER, address(NEXT_HOLDER)),
        approval_for_all(5, HOLDER, OPERATOR, false),
        transfer(6, NEXT_HOLDER, Address::ZERO),
    ]))?;

    assert_eq!(
        permission_rows(&output),
        vec![
            row(1, HOLDER, "holder", true, HOLDER_POWERS),
            row(3, DELEGATE, "token_approval", true, DELEGATE_POWERS),
            row(4, HOLDER, "holder", false, HOLDER_POWERS),
            row(4, NEXT_HOLDER, "holder", true, HOLDER_POWERS),
            row(6, NEXT_HOLDER, "holder", false, HOLDER_POWERS),
            row(6, DELEGATE, "token_approval", false, DELEGATE_POWERS),
        ]
    );
    let revoked_operator = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "AccountPermissionChanged")
        .expect("operator revocation");
    assert_eq!(revoked_operator.after_state["approved"], false);
    assert_eq!(revoked_operator.after_state["effective_powers"], json!([]));
    assert_eq!(
        revoked_operator.after_state["revocation_source"]["source_event"],
        "ApprovalForAll"
    );
    Ok(())
}

#[test]
fn repeated_and_foreign_wrapper_approvals_emit_nothing() -> anyhow::Result<()> {
    let output = interpret_test_batch(input(vec![
        wrapped(1, 0),
        approval(2, HOLDER, address(DELEGATE)),
        approval(3, HOLDER, address(DELEGATE)),
        approval(4, HOLDER, Address::ZERO),
        approval(5, HOLDER, Address::ZERO),
    ]))?;
    assert_eq!(
        permission_rows(&output),
        vec![
            row(1, HOLDER, "holder", true, HOLDER_POWERS),
            row(2, DELEGATE, "token_approval", true, DELEGATE_POWERS),
            row(4, DELEGATE, "token_approval", false, DELEGATE_POWERS),
        ]
    );

    // An approval for a token the wrapper never wrapped has no name to bind to.
    let foreign = interpret_test_batch(input(vec![approval(1, HOLDER, address(DELEGATE))]))?;
    assert!(foreign.normalized_events.is_empty());
    Ok(())
}

/// Replays `logs[split..]` on state restored from `logs[..split]` through both prior-event paths
/// and asserts the permission rows match the single-pass rows from `split_block` on.
fn assert_restore_matches(
    logs: &[RawLogInput],
    split: usize,
    split_block: i64,
) -> anyhow::Result<()> {
    let output = interpret_test_batch(input(logs.to_vec()))?;
    let head = input(logs[..split].to_vec());
    let (first, _) = interpret_test_batch_incremental(head.clone(), None)?;
    let mut tail = input(logs[split..].to_vec());
    for prior in [
        first.normalized_events.iter().map(prior_event).collect(),
        seam::fold_prior_events(vec![], &first.normalized_events, &head.blocks)?,
    ] {
        tail.prior_events = prior;
        let restored = interpret_test_batch(tail.clone())?;
        assert_eq!(
            permission_rows(&restored),
            permission_rows(&output)
                .into_iter()
                .filter(|(block, ..)| *block >= split_block)
                .collect::<Vec<_>>()
        );
    }
    Ok(())
}

// `_beforeTransfer` deletes the approval whoever the recipient is; the delegate who becomes the
// holder loses the token-approval row and gains the holder row, and the revocation is emitted
// first so Project's newest-row fold over (resource, subject, scope) keeps the holder grant.
// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L837-L840 @ ens_v1@91c966f)
#[test]
fn a_delegate_who_becomes_the_holder_still_loses_the_token_approval() -> anyhow::Result<()> {
    let logs = vec![
        wrapped(1, 0),
        approval(2, HOLDER, address(DELEGATE)),
        transfer(3, HOLDER, address(DELEGATE)),
    ];
    let output = interpret_test_batch(input(logs.clone()))?;
    assert_eq!(
        permission_rows(&output),
        vec![
            row(1, HOLDER, "holder", true, HOLDER_POWERS),
            row(2, DELEGATE, "token_approval", true, DELEGATE_POWERS),
            row(3, DELEGATE, "token_approval", false, DELEGATE_POWERS),
            row(3, HOLDER, "holder", false, HOLDER_POWERS),
            row(3, DELEGATE, "holder", true, HOLDER_POWERS),
        ]
    );
    let position = |relation: &str| {
        output
            .normalized_events
            .iter()
            .position(|event| {
                event.block_number == Some(3)
                    && event.event_kind == "PermissionChanged"
                    && event.after_state["subject"] == DELEGATE
                    && event
                        .after_state
                        .get("grant_source")
                        .filter(|source| !source.is_null())
                        .or_else(|| event.after_state.get("revocation_source"))
                        .is_some_and(|source| source["relation_kind"] == relation)
            })
            .expect("row for the delegate")
    };
    assert!(position("token_approval") < position("holder"));
    assert_restore_matches(&logs, 2, 3)
}

// The ERC-1155 burn already revoked the holder; the `NameUnwrapped` that follows it must not
// revoke the same holder again, in the first pass or after a restore.
// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1031 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L269-L278 @ ens_v1@91c966f)
#[test]
fn a_burn_followed_by_name_unwrapped_revokes_the_holder_once() -> anyhow::Result<()> {
    let logs = vec![
        wrapped(1, 0),
        approval(2, HOLDER, address(DELEGATE)),
        transfer(3, HOLDER, Address::ZERO),
        unwrapped(4, HOLDER),
    ];
    let output = interpret_test_batch(input(logs.clone()))?;
    assert_eq!(
        permission_rows(&output),
        vec![
            row(1, HOLDER, "holder", true, HOLDER_POWERS),
            row(2, DELEGATE, "token_approval", true, DELEGATE_POWERS),
            row(3, HOLDER, "holder", false, HOLDER_POWERS),
            row(3, DELEGATE, "token_approval", false, DELEGATE_POWERS),
        ]
    );
    let unbound = output
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "AuthorityEpochChanged"
                && event.after_state["source_event"] == "NameUnwrapped"
        })
        .expect("NameUnwrapped closes the wrapper authority epoch");
    assert!(unbound.resource_id.is_some());
    assert_restore_matches(&logs, 3, 4)?;

    // A plain transfer to a new holder followed by an unwrap still revokes that holder once.
    let logs = vec![
        wrapped(1, 0),
        transfer(2, HOLDER, address(NEXT_HOLDER)),
        unwrapped(3, NEXT_HOLDER),
    ];
    let output = interpret_test_batch(input(logs.clone()))?;
    assert_eq!(
        permission_rows(&output),
        vec![
            row(1, HOLDER, "holder", true, HOLDER_POWERS),
            row(2, HOLDER, "holder", false, HOLDER_POWERS),
            row(2, NEXT_HOLDER, "holder", true, HOLDER_POWERS),
            row(3, NEXT_HOLDER, "holder", false, HOLDER_POWERS),
        ]
    );
    assert_restore_matches(&logs, 2, 3)
}

// `TransferBatch` moves every listed token; each wrapped name changes holder independently.
// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L155-L197 @ ens_v1@91c966f)
#[test]
fn transfer_batch_moves_the_holder_of_every_wrapped_name_it_lists() -> anyhow::Result<()> {
    let bob = node_of(&["bob", "eth"]);
    let output = interpret_test_batch(input(vec![
        wrapped(1, 0),
        wrapped_labels(2, &["bob", "eth"], 0),
        transfer_batch(3, HOLDER, address(NEXT_HOLDER), &[node(), bob]),
    ]))?;
    for (block, node) in [(1, node()), (2, bob)] {
        assert_eq!(
            permission_rows_for(&output, node),
            vec![
                row(block, HOLDER, "holder", true, HOLDER_POWERS),
                row(3, HOLDER, "holder", false, HOLDER_POWERS),
                row(3, NEXT_HOLDER, "holder", true, HOLDER_POWERS),
            ]
        );
    }
    let transfers = output
        .normalized_events
        .iter()
        .filter(|event| {
            event.event_kind == "TokenControlTransferred"
                && event.after_state["source_event"] == "TransferBatch"
        })
        .count();
    assert_eq!(transfers, 2);
    Ok(())
}

// Under CANNOT_APPROVE the approval survives transfers; a delegate who received the token and
// later passes it on is still `getApproved`, so its token-approval grant is re-emitted after its
// holder revocation and stays the newest row for that subject.
// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L108-L121 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L837-L840 @ ens_v1@91c966f)
#[test]
fn a_retained_delegate_who_passes_the_token_on_keeps_its_approval_row() -> anyhow::Result<()> {
    let logs = vec![
        wrapped(1, 0),
        fuses_set(2, PARENT_CANNOT_CONTROL | CANNOT_UNWRAP | CANNOT_APPROVE),
        approval(3, HOLDER, address(DELEGATE)),
        transfer(4, HOLDER, address(DELEGATE)),
        transfer(5, DELEGATE, address(NEXT_HOLDER)),
    ];
    let output = interpret_test_batch(input(logs.clone()))?;
    assert_eq!(
        permission_rows(&output),
        vec![
            row(1, HOLDER, "holder", true, HOLDER_POWERS),
            row(3, DELEGATE, "token_approval", true, DELEGATE_POWERS),
            row(4, HOLDER, "holder", false, HOLDER_POWERS),
            row(4, DELEGATE, "holder", true, HOLDER_POWERS),
            row(5, DELEGATE, "holder", false, HOLDER_POWERS),
            row(5, NEXT_HOLDER, "holder", true, HOLDER_POWERS),
            row(5, DELEGATE, "token_approval", true, DELEGATE_POWERS),
        ]
    );
    assert_restore_matches(&logs, 4, 5)
}
