//! ENSv2 registry operator approvals under the checked-in Sepolia manifests: every registry
//! keeps its own owner-to-operator approvals, and the approval is an account fact that reads
//! and moves no registry state.
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/erc1155/ERC1155Singleton.sol:L73-L75 @ ens_v2_sepolia_20261001@07e55a05)
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/erc1155/ERC1155Singleton.sol:L351-L357 @ ens_v2_sepolia_20261001@07e55a05)
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L622-L636 @ ens_v2_sepolia_20261001@07e55a05)

use super::*;

const REGISTRY_MANIFEST: i64 = 7401;
const ROOT_MANIFEST: i64 = 7402;
const ETH_REGISTRY: &str = "0xd4ebcbbdf463c9c45784603db0ddd499bc44a8b4";
const ROOT_REGISTRY: &str = "0xb458d6a3a77919449d03e7a6903c26827c1ec43f";
const USER_REGISTRY: &str = "0x00000000000000000000000000000000000000e7";
/// A resolver a registry points at, admitted under the registry's manifest by a resolver edge.
const POINTED_RESOLVER: &str = "0x00000000000000000000000000000000000000f1";
const OWNER: &str = "0x00000000000000000000000000000000000000a1";
const OPERATOR: &str = "0x00000000000000000000000000000000000000e1";

fn checked_in(manifest_id: i64, family: &str) -> ManifestInput {
    let repository = bigname_manifests::load_repository(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/sepolia"),
    )
    .unwrap();
    let manifest = &repository
        .manifests()
        .iter()
        .find(|loaded| loaded.manifest.source_family == family)
        .unwrap()
        .manifest;
    ManifestInput {
        manifest_id,
        manifest_version: i64::try_from(manifest.manifest_version).unwrap(),
        namespace: manifest.namespace.clone(),
        source_family: family.to_owned(),
        chain_id: CHAIN.to_owned(),
        deployment_label: manifest.deployment_epoch.clone(),
        normalizer_version: manifest.normalizer_version.clone(),
        payload_json: serde_json::to_string(manifest).unwrap(),
    }
}

fn instance(address: &str) -> Uuid {
    Uuid::from_u128(u128::from_str_radix(&address[34..], 16).unwrap())
}

fn admitted(
    address: &str,
    manifest_id: i64,
    role: Option<&str>,
    edge: Option<&str>,
) -> AddressAdmissionInput {
    AddressAdmissionInput {
        address: address.to_owned(),
        contract_instance_id: instance(address),
        source_manifest_id: Some(manifest_id),
        role: role.map(str::to_owned),
        discovery_edge_kind: edge.map(str::to_owned),
        discovery_from_contract_instance_id: edge.map(|edge| {
            instance(if edge == "resolver" {
                ETH_REGISTRY
            } else {
                address
            })
        }),
        discovery_observation_key: edge.map(|edge| format!("{edge}:fixture")),
        active_from_block: Some(0),
        active_to_block: None,
    }
}

fn input(raw_logs: Vec<RawLogInput>, last_block: i64) -> BatchInput {
    let rule = |manifest_id, edge_kind: &str, role: &str| DiscoveryRuleInput {
        manifest_id,
        edge_kind: edge_kind.to_owned(),
        from_role: Some(role.to_owned()),
        admission: "reachable_from_root".to_owned(),
    };
    BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![
            checked_in(REGISTRY_MANIFEST, "ens_v2_registry_l1"),
            checked_in(ROOT_MANIFEST, "ens_v2_root_l1"),
        ],
        discovery_rules: vec![
            rule(REGISTRY_MANIFEST, "subregistry", "registry"),
            rule(REGISTRY_MANIFEST, "resolver", "registry"),
            rule(REGISTRY_MANIFEST, "registry_announcement", "registry"),
            rule(ROOT_MANIFEST, "subregistry", "root_registry"),
            rule(ROOT_MANIFEST, "resolver", "root_registry"),
        ],
        admissions: vec![
            admitted(ETH_REGISTRY, REGISTRY_MANIFEST, Some("registry"), None),
            admitted(ROOT_REGISTRY, ROOT_MANIFEST, Some("root_registry"), None),
            admitted(
                USER_REGISTRY,
                REGISTRY_MANIFEST,
                Some("registry"),
                Some("registry_announcement"),
            ),
            admitted(POINTED_RESOLVER, REGISTRY_MANIFEST, None, Some("resolver")),
        ],
        prior_events: Vec::new(),
        // Every block is present whether or not it has a log, so a batch with an approval and
        // the same batch without it settle expiries at the same block boundaries.
        blocks: (1..=last_block)
            .map(|number| RawBlockInput {
                chain_id: CHAIN.to_owned(),
                block_hash: format!("block-{number}"),
                block_number: number,
                block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(number),
                canonicality_state: "canonical".to_owned(),
            })
            .collect(),
        raw_logs,
    }
}

fn approval(block: i64, log: i64, registry: &str, approved: bool) -> RawLogInput {
    raw_at(
        approvals::ApprovalForAll {
            owner: OWNER.parse().unwrap(),
            operator: OPERATOR.parse().unwrap(),
            approved,
        }
        .encode_log_data(),
        block,
        log,
        registry,
    )
}

/// `_register` with an owner: LabelRegistered, the mint, TokenResource and the owner's roles.
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L448-L514 @ ens_v2_sepolia_20261001@07e55a05)
fn registration(block: i64, first_log: i64, label: &str, expiry: u64) -> Vec<RawLogInput> {
    let token = versioned_token(label, 0);
    let owner: Address = OWNER.parse().unwrap();
    [
        v2_registry::LabelRegistered {
            tokenId: token,
            labelHash: keccak256(label.as_bytes()),
            label: label.to_owned(),
            owner,
            expiry,
            sender: owner,
        }
        .encode_log_data(),
        v2_registry::TransferSingle {
            operator: owner,
            from: Address::ZERO,
            to: owner,
            id: token,
            value: U256::from(1),
        }
        .encode_log_data(),
        v2_registry::TokenResource {
            tokenId: token,
            resource: token,
        }
        .encode_log_data(),
        v2_resolver::EACRolesChanged {
            resource: token,
            account: owner,
            oldRoleBitmap: U256::ZERO,
            newRoleBitmap: U256::from(0x1111),
        }
        .encode_log_data(),
    ]
    .into_iter()
    .zip(first_log..)
    .map(|(encoded, log)| raw_at(encoded, block, log, ETH_REGISTRY))
    .collect()
}

fn account_changes(output: &BatchOutput) -> Vec<&NormalizedEvent> {
    output
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "AccountPermissionChanged")
        .collect()
}

#[test]
fn every_admitted_registry_states_its_own_operator_approvals() -> anyhow::Result<()> {
    let output = interpret_test_batch(input(
        vec![
            approval(1, 0, ETH_REGISTRY, true),
            approval(1, 1, ROOT_REGISTRY, true),
            approval(1, 2, USER_REGISTRY, true),
            approval(2, 0, USER_REGISTRY, false),
        ],
        2,
    ))?;
    assert!(output.decode_skips.is_empty(), "{:?}", output.decode_skips);
    assert_eq!(
        output.normalized_events.len(),
        4,
        "an approval writes its account fact and nothing else"
    );
    let stated = account_changes(&output)
        .into_iter()
        .map(|event| {
            assert_eq!(event.derivation_kind, "standard_approval");
            assert_eq!(event.logical_name_id, None);
            assert_eq!(event.resource_id, None);
            let after = &event.after_state;
            assert_eq!(after["subject"], OPERATOR);
            assert_eq!(after["relation_kind"], "operator");
            assert_eq!(after["scope"]["authority_kind"], "ens_v2_registry");
            assert_eq!(after["scope"]["owner"], OWNER);
            assert_eq!(
                after["effective_powers"],
                json!([]),
                "the operator's powers are the token owner's and are not stored"
            );
            assert_eq!(
                after["transfer_behavior"],
                json!({"mode": "owner_scoped", "on_holder_change": "ceases_to_apply"})
            );
            let source = json!({"kind": "raw_log", "source_event": "ApprovalForAll"});
            if after["approved"] == true {
                assert_eq!(after["grant_source"], source);
                assert!(after["revocation_source"].is_null());
            } else {
                assert_eq!(after["revocation_source"], source);
            }
            (
                event.source_family.as_str(),
                after["scope"]["authority_contract"].as_str().unwrap(),
                after["scope"]["authority_contract_instance_id"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
                after["approved"].as_bool().unwrap(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        stated,
        vec![
            (
                "ens_v2_registry_l1",
                ETH_REGISTRY,
                instance(ETH_REGISTRY).to_string(),
                true
            ),
            (
                "ens_v2_root_l1",
                ROOT_REGISTRY,
                instance(ROOT_REGISTRY).to_string(),
                true
            ),
            (
                "ens_v2_registry_l1",
                USER_REGISTRY,
                instance(USER_REGISTRY).to_string(),
                true
            ),
            (
                "ens_v2_registry_l1",
                USER_REGISTRY,
                instance(USER_REGISTRY).to_string(),
                false
            ),
        ]
    );
    let revoked = &account_changes(&output)[3];
    assert_eq!(
        revoked.before_state["approved"], true,
        "the revocation follows the approval of the same owner, operator and registry"
    );
    Ok(())
}

/// A resolver a registry names is admitted under the registry's manifest, but it is not a
/// registry: its ApprovalForAll is a resolver approval, which this adapter does not read.
#[test]
fn a_resolver_admitted_by_a_registry_pointer_states_no_registry_approval() -> anyhow::Result<()> {
    let output = interpret_test_batch(input(vec![approval(1, 0, POINTED_RESOLVER, true)], 1))?;
    assert_eq!(output.normalized_events, Vec::new());
    Ok(())
}

#[test]
fn a_malformed_registry_approval_is_fatal() {
    let mut malformed = approval(1, 0, ETH_REGISTRY, true);
    malformed.topics.truncate(2);
    let error = interpret_test_batch(input(vec![malformed], 1)).unwrap_err();
    assert!(
        format!("{error:#}").contains("ApprovalForAll log is malformed"),
        "{error:#}"
    );
}

#[test]
fn a_registry_approval_declaration_must_keep_its_event_list_empty() {
    let declared = |normalized_events: &[&str]| {
        let mut batch = input(Vec::new(), 1);
        batch.manifests = vec![manifest(
            REGISTRY_MANIFEST,
            "ens_v2_registry_l1",
            "ApprovalForAll",
            "event ApprovalForAll(address indexed account, address indexed operator, bool approved)",
            &["registry"],
            normalized_events,
        )];
        batch.admissions.truncate(1);
        batch.discovery_rules.clear();
        interpret_test_batch(batch)
    };
    declared(&[]).unwrap();
    let error = declared(&["AccountPermissionChanged"]).unwrap_err();
    assert!(format!("{error:#}").contains("ApprovalForAll"), "{error:#}");
}

/// The approval sits between a registration's expiry and the next registry log. Whether it is
/// there or not, every other event keeps its identity, position and states, and so does every
/// other row the batch writes.
#[test]
fn an_approval_moves_no_other_event() -> anyhow::Result<()> {
    let logs = |with_approvals: bool| {
        let mut logs = registration(1, 0, "alice", 5);
        if with_approvals {
            logs.push(approval(1, 4, ETH_REGISTRY, true));
            logs.push(approval(7, 0, ETH_REGISTRY, false));
            logs.push(approval(9, 0, ETH_REGISTRY, true));
        }
        logs.extend(registration(9, 1, "bob", 100));
        logs
    };
    let without = interpret_test_batch(input(logs(false), 10))?;
    let mut with = interpret_test_batch(input(logs(true), 10))?;
    assert_eq!(account_changes(&with).len(), 3);
    assert!(
        without.normalized_events.iter().any(|event| event
            .block_number
            .is_some_and(|block| (5..9).contains(&block))),
        "the fixture has a transition at the expiry, before the next registry log"
    );
    with.normalized_events
        .retain(|event| event.event_kind != "AccountPermissionChanged");
    assert_eq!(with, without);
    Ok(())
}

/// A session restored from retained events, the approval among them, continues exactly as the
/// session that interpreted the logs.
#[test]
fn a_restored_session_continues_past_an_approval_as_the_live_one_does() -> anyhow::Result<()> {
    let mut first_logs = registration(1, 0, "alice", 5);
    first_logs.push(approval(7, 0, ETH_REGISTRY, true));
    let first_input = input(first_logs, 7);
    let first_blocks = first_input.blocks.clone();
    let (first, session) = interpret_test_batch_incremental(first_input, None)?;
    let prior = seam::fold_prior_events(Vec::new(), &first.normalized_events, &first_blocks)?;

    let mut second_logs = registration(9, 0, "bob", 100);
    second_logs.push(approval(9, 4, ETH_REGISTRY, false));
    let mut second_input = input(second_logs, 9);
    second_input.blocks.retain(|block| block.block_number > 7);
    let mut fresh_input = second_input.clone();
    fresh_input.prior_events = prior;
    let fresh = interpret_test_batch(fresh_input)?;
    let (second, _) = interpret_test_batch_incremental(second_input, Some(session))?;
    assert_eq!(second, fresh);
    let revoked = account_changes(&second);
    assert_eq!(revoked.len(), 1);
    assert_eq!(revoked[0].before_state["approved"], true);
    assert_eq!(revoked[0].after_state["approved"], false);
    Ok(())
}
