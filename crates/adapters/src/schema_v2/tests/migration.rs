use std::{collections::BTreeSet, str::FromStr};

use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use serde_json::{Value, json};
use uuid::Uuid;

use super::*;

mod child;

const MIGRATION_MANIFEST_ID: i64 = 100;
const REGISTRY_MANIFEST_ID: i64 = 101;
const V1_REGISTRY_MANIFEST_ID: i64 = 102;
const V1_WRAPPER_MANIFEST_ID: i64 = 103;
const V1_REGISTRAR_MANIFEST_ID: i64 = 104;

sol! {
    event ProxyDeployed(address indexed sender, address indexed proxyAddress, uint256 salt, address implementation);
    event BridgeNameRenewed(uint256 indexed tokenId, string label, uint64 duration, uint64 newExpiry, address paymentToken, bytes32 indexed referrer, uint256 amount);
    event ControllerAdded(address indexed controller);
    event ControllerRemoved(address indexed controller);
    event BaseNameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
    event BaseNameRenewed(uint256 indexed id, uint256 expires);
}

#[test]
fn catalog_fixture_records_reproducible_provenance_and_corrections() -> anyhow::Result<()> {
    let fixture = fixture()?;
    assert_eq!(
        fixture["provenance"]["commit"],
        "d110108f2f098d1b43804c64c80d0b4588286326"
    );
    assert_eq!(fixture["provenance"]["validated_scenario_count"], 63);
    assert_eq!(
        fixture["provenance"]["derived_sources"]
            .as_array()
            .map(Vec::len),
        Some(14)
    );
    assert!(
        fixture["corrections"]["registry_resolver_clear"]
            .as_str()
            .is_some_and(|value| value.contains("only when"))
    );
    assert!(
        fixture["corrections"]["registry_ttl_clear"]
            .as_str()
            .is_some_and(|value| value.contains("only when"))
    );
    Ok(())
}

#[test]
fn cross_family_registrar_transfer_emits_one_unwrapped_candidate_boundary() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["U-01"];
    let unrelated_cleanup = &fixture["scenarios"]["G-02"];
    let addresses = &fixture["addresses"];
    let base_token = decimal_u256(&scenario["base_token_id"])?;
    let v2_token = decimal_u256(&scenario["v2_token_id"])?;
    let label = scenario["label"].as_str().unwrap();
    let block = scenario["migration_block"].as_i64().unwrap();
    let graveyard = address(addresses, "graveyard")?;
    let unlocked = address(addresses, "unlocked_controller")?;
    let owner = Address::from([0x11; 20]);
    let mut output = interpret_test_batch(batch(
        vec![
            raw_at_transaction(
                with_topic0(
                    BaseNameRegistered {
                        id: decimal_u256(&unrelated_cleanup["token_id"])?,
                        owner: graveyard,
                        expires: decimal_u256(&unrelated_cleanup["cleanup_expiry"])?,
                    }
                    .encode_log_data(),
                    keccak256(b"NameRegistered(uint256,address,uint256)"),
                ),
                block,
                0,
                3,
                addresses["base_registrar"].as_str().unwrap(),
            ),
            raw_at_transaction(
                super::v1_registrar::Transfer {
                    from: unlocked,
                    to: graveyard,
                    tokenId: base_token,
                }
                .encode_log_data(),
                block,
                0,
                scenario["graveyard_transfer_log_index"].as_i64().unwrap(),
                addresses["base_registrar"].as_str().unwrap(),
            ),
            raw_at_transaction(
                super::v2_registry::LabelRegistered {
                    tokenId: v2_token,
                    labelHash: keccak256(label.as_bytes()),
                    label: label.to_owned(),
                    owner,
                    expiry: scenario["stored_expiry"].as_u64().unwrap(),
                    sender: unlocked,
                }
                .encode_log_data(),
                block,
                0,
                scenario["v2_registration_log_index"].as_i64().unwrap(),
                addresses["eth_registry"].as_str().unwrap(),
            ),
            raw_at_transaction(
                super::v2_registry::TokenResource {
                    tokenId: v2_token,
                    resource: v2_token,
                }
                .encode_log_data(),
                block,
                0,
                scenario["v2_registration_log_index"].as_i64().unwrap() + 2,
                addresses["eth_registry"].as_str().unwrap(),
            ),
            raw_at_transaction(
                super::v2_registry::TokenRegenerated {
                    oldTokenId: v2_token,
                    newTokenId: v2_token + U256::from(1),
                }
                .encode_log_data(),
                block,
                0,
                scenario["v2_registration_log_index"].as_i64().unwrap() + 3,
                addresses["eth_registry"].as_str().unwrap(),
            ),
            raw_at_transaction(
                super::v2_registry::TransferSingle {
                    operator: Address::from([0x77; 20]),
                    from: owner,
                    to: Address::from([0x78; 20]),
                    id: v2_token + U256::from(1),
                    value: U256::from(1),
                }
                .encode_log_data(),
                block,
                0,
                scenario["v2_registration_log_index"].as_i64().unwrap() + 4,
                addresses["eth_registry"].as_str().unwrap(),
            ),
            raw_at_transaction(
                super::v2_registry::ExpiryUpdated {
                    tokenId: v2_token + U256::from(1),
                    newExpiry: scenario["stored_expiry"].as_u64().unwrap() + 1,
                    sender: Address::from([0x77; 20]),
                }
                .encode_log_data(),
                block,
                0,
                scenario["v2_registration_log_index"].as_i64().unwrap() + 5,
                addresses["eth_registry"].as_str().unwrap(),
            ),
        ],
        &fixture,
        true,
    ))?;

    let boundaries = output
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "MigrationApplied")
        .collect::<Vec<_>>();
    assert_eq!(boundaries.len(), 1);
    let boundary = boundaries[0];
    assert_eq!(boundary.consumer_visibility, "candidate");
    assert_eq!(boundary.migration_correlation_ids.len(), 1);
    assert_eq!(boundary.after_state["migration_path"], "unwrapped");
    assert_eq!(
        boundary.after_state["stored_expiry"],
        scenario["stored_expiry"]
    );
    assert_eq!(
        boundary.after_state["v2_registration_boundary"]["log_index"],
        scenario["v2_registration_log_index"]
    );
    for required in [
        "logical_name_id",
        "namehash",
        "predecessor_binding",
        "successor_binding",
        "successor_registry_contract_instance_id",
        "evidence",
        "consumer_visibility",
    ] {
        assert!(
            boundary.after_state.get(required).is_some(),
            "boundary payload lacks {required}"
        );
    }
    assert_eq!(output.migration_candidate_identity_effects.len(), 1);
    assert!(output.normalized_events.iter().any(|event| {
        event.event_kind == "RegistrationReleased"
            && event.logical_name_id.as_deref()
                != Some(boundary.logical_name_id.as_deref().unwrap())
    }));
    let successor_binding = output
        .surface_bindings
        .iter()
        .find(|binding| binding.logical_name_id == boundary.logical_name_id.as_deref().unwrap())
        .expect("ordinary ENSv2 successor binding");
    assert_eq!(
        boundary.after_state["successor_binding"]["binding_id"],
        successor_binding.surface_binding_id.to_string()
    );
    assert_eq!(
        boundary.after_state["predecessor_binding"]["selection"],
        "active_immediately_before_predecessor_cleanup"
    );
    let cleanup = &boundary.after_state["predecessor_binding"]["predecessor_cleanup"];
    assert_eq!(cleanup["source_event"], "Transfer");
    assert_eq!(
        cleanup["log_index"],
        scenario["graveyard_transfer_log_index"]
    );
    assert_eq!(
        boundary.after_state["predecessor_binding"]["resource"]["token_id"],
        scenario["labelhash"]
    );
    assert_eq!(
        boundary.after_state["predecessor_binding"]["resource"]["anchor_kind"],
        "registrar_backed_registration"
    );
    assert_eq!(
        boundary.after_state["predecessor_binding"]["resource"]["contract_instance_id"],
        Uuid::from_u128(6).to_string(),
        "moving attribution between manifest families must preserve address-keyed contract identity"
    );
    assert!(
        boundary.after_state["predecessor_binding"]
            .get("binding_id")
            .is_none(),
        "slice 1 must not invent a predecessor row identity"
    );
    assert!(output.normalized_events.iter().all(|event| {
        event.source_family != "ens_v2_migration_l1" || event.consumer_visibility == "candidate"
    }));
    let unrelated_expiry = output
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "ExpiryChanged"
                && event.after_state["expiry"]
                    == json!(scenario["stored_expiry"].as_u64().unwrap() + 1)
        })
        .expect("co-located same-name expiry update remains independently admitted");
    assert!(
        output
            .migration_event_associations
            .iter()
            .all(|association| association.event_identity != unrelated_expiry.event_identity),
        "an unrelated same-name action must not become migration evidence"
    );
    let regenerated = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "TokenRegenerated")
        .expect("registration token regeneration remains independently admitted");
    assert!(
        output
            .migration_event_associations
            .iter()
            .any(|association| association.event_identity == regenerated.event_identity),
        "the path-owned token regeneration must be migration evidence"
    );
    let unrelated_transfer = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "TokenControlTransferred")
        .expect("co-located positive transfer remains independently admitted");
    assert!(
        output
            .migration_event_associations
            .iter()
            .all(|association| association.event_identity != unrelated_transfer.event_identity),
        "an unrelated positive transfer must not become migration evidence"
    );
    assert_activated_transition(&mut output, "unwrapped")?;
    Ok(())
}

#[test]
fn co_admitted_registrar_transfer_keeps_ordinary_event_and_gains_association() -> anyhow::Result<()>
{
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["U-01"];
    let addresses = &fixture["addresses"];
    let label = scenario["label"].as_str().unwrap();
    let base_token = decimal_u256(&scenario["base_token_id"])?;
    let v2_token = decimal_u256(&scenario["v2_token_id"])?;
    let migration_block = scenario["migration_block"].as_i64().unwrap();
    let wrapper = address(addresses, "name_wrapper")?;
    let unlocked = address(addresses, "unlocked_controller")?;
    let graveyard = address(addresses, "graveyard")?;
    let mut dns_name = Vec::with_capacity(label.len() + 6);
    dns_name.push(u8::try_from(label.len())?);
    dns_name.extend_from_slice(label.as_bytes());
    dns_name.extend_from_slice(b"\x03eth\0");

    let setup = interpret_test_batch(batch(
        vec![
            raw_at_transaction(
                super::NameWrapped {
                    node: scenario["namehash"].as_str().unwrap().parse()?,
                    name: dns_name.into(),
                    owner: unlocked,
                    fuses: 0,
                    expiry: scenario["stored_expiry"].as_u64().unwrap() + (90 * 24 * 60 * 60),
                }
                .encode_log_data(),
                migration_block - 2,
                0,
                0,
                addresses["name_wrapper"].as_str().unwrap(),
            ),
            raw_at_transaction(
                super::NameUnwrapped {
                    node: scenario["namehash"].as_str().unwrap().parse()?,
                    owner: unlocked,
                }
                .encode_log_data(),
                migration_block - 1,
                0,
                0,
                addresses["name_wrapper"].as_str().unwrap(),
            ),
            raw_at_transaction(
                super::v1_registrar::Transfer {
                    from: wrapper,
                    to: unlocked,
                    tokenId: base_token,
                }
                .encode_log_data(),
                migration_block - 1,
                0,
                1,
                addresses["base_registrar"].as_str().unwrap(),
            ),
        ],
        &fixture,
        false,
    ))?;
    assert!(setup.normalized_events.iter().any(|event| {
        event.source_family == "ens_v1_registrar_l1"
            && event.event_kind == "TokenControlTransferred"
    }));

    let migration_log = scenario["graveyard_transfer_log_index"].as_i64().unwrap();
    let mut input = batch(
        vec![
            raw_at_transaction(
                super::v1_registrar::Transfer {
                    from: unlocked,
                    to: graveyard,
                    tokenId: base_token,
                }
                .encode_log_data(),
                migration_block,
                0,
                migration_log,
                addresses["base_registrar"].as_str().unwrap(),
            ),
            raw_at_transaction(
                super::v2_registry::LabelRegistered {
                    tokenId: v2_token,
                    labelHash: keccak256(label.as_bytes()),
                    label: label.to_owned(),
                    owner: Address::from([0x11; 20]),
                    expiry: scenario["stored_expiry"].as_u64().unwrap(),
                    sender: unlocked,
                }
                .encode_log_data(),
                migration_block,
                0,
                scenario["v2_registration_log_index"].as_i64().unwrap(),
                addresses["eth_registry"].as_str().unwrap(),
            ),
            raw_at_transaction(
                super::v2_registry::TokenResource {
                    tokenId: v2_token,
                    resource: v2_token,
                }
                .encode_log_data(),
                migration_block,
                0,
                scenario["v2_registration_log_index"].as_i64().unwrap() + 1,
                addresses["eth_registry"].as_str().unwrap(),
            ),
        ],
        &fixture,
        true,
    );
    let without_predecessor = interpret_test_batch(input.clone())?;
    let fallback_cleanup_identity = without_predecessor
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "MigrationApplied")
        .and_then(|boundary| {
            boundary.after_state["predecessor_binding"]["predecessor_cleanup"]["event_identity"]
                .as_str()
        })
        .expect("candidate without ordinary predecessor evidence still records cleanup identity")
        .to_owned();
    input.prior_events = setup
        .normalized_events
        .iter()
        .map(super::prior_event)
        .collect();
    let output = interpret_test_batch(input)?;

    let transfer = output
        .normalized_events
        .iter()
        .find(|event| {
            event.source_family == "ens_v1_registrar_l1"
                && event.event_kind == "TokenControlTransferred"
                && event.log_index == Some(migration_log)
        })
        .expect("the independently admitted registrar Transfer remains an ordinary event");
    assert_eq!(transfer.consumer_visibility, "activated");
    assert!(transfer.migration_correlation_ids.is_empty());
    let boundary_cleanup_identity = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "MigrationApplied")
        .and_then(|boundary| {
            boundary.after_state["predecessor_binding"]["predecessor_cleanup"]["event_identity"]
                .as_str()
        })
        .expect("co-admitted predecessor boundary records its cleanup identity");
    assert_eq!(boundary_cleanup_identity, transfer.event_identity);
    assert_eq!(
        fallback_cleanup_identity, transfer.event_identity,
        "candidate cleanup identity must not depend on whether ordinary predecessor evidence materialized"
    );
    assert!(
        output
            .migration_event_associations
            .iter()
            .any(|association| association.event_identity == transfer.event_identity),
        "the ordinary registrar Transfer must separately retain its migration relationship"
    );
    Ok(())
}

#[test]
fn sepolia_base_registrar_admission_restores_unwrapped_fallback_authority() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let addresses = &fixture["addresses"];
    let registrar = addresses["base_registrar"].as_str().unwrap();
    let wrapper = addresses["name_wrapper"].as_str().unwrap();
    let registry = "0x0000000000000000000000000000000000000099";
    let label = b"restored";
    let labelhash = keccak256(label);
    let node = super::common::namehash(&["restored".to_owned(), "eth".to_owned()]);
    let block = fixture["scenarios"]["U-01"]["migration_block"]
        .as_i64()
        .unwrap();
    let owner = Address::from([0x51; 20]);
    let successor = Address::from([0x52; 20]);
    let old_registrar_expiry = u64::try_from(block)? + 10_000;
    let renewed_registrar_expiry = old_registrar_expiry + 20_000;
    let wrapper_expiry = old_registrar_expiry + (90 * 24 * 60 * 60);
    let logs = vec![
        raw_at_transaction(
            super::v1_registry::Transfer {
                node: node.parse()?,
                owner: wrapper.parse()?,
            }
            .encode_log_data(),
            block,
            0,
            0,
            registry,
        ),
        raw_at_transaction(
            super::NameWrapped {
                node: node.parse()?,
                name: b"\x08restored\x03eth\0".to_vec().into(),
                owner,
                fuses: (1 << 16) | (1 << 17),
                expiry: wrapper_expiry,
            }
            .encode_log_data(),
            block,
            0,
            1,
            wrapper,
        ),
        raw_at_transaction(
            ControllerAdded {
                controller: wrapper.parse()?,
            }
            .encode_log_data(),
            block + 1,
            0,
            0,
            registrar,
        ),
        base_renewed_at(
            addresses,
            U256::from_be_bytes(*labelhash),
            renewed_registrar_expiry,
            block + 1,
            1,
        ),
        raw_at_transaction(
            ControllerRemoved {
                controller: wrapper.parse()?,
            }
            .encode_log_data(),
            block + 1,
            0,
            2,
            registrar,
        ),
        raw_at_transaction(
            super::v1_registry::Transfer {
                node: node.parse()?,
                owner,
            }
            .encode_log_data(),
            block + 2,
            0,
            0,
            registry,
        ),
        raw_at_transaction(
            super::NameUnwrapped {
                node: node.parse()?,
                owner,
            }
            .encode_log_data(),
            block + 2,
            0,
            1,
            wrapper,
        ),
        registrar_transfer_at(wrapper.parse()?, owner, labelhash, block + 2, 2, registrar),
        registrar_transfer_at(owner, successor, labelhash, block + 3, 0, registrar),
    ];

    let mut before_input = batch(logs.clone(), &fixture, false);
    admit_v1_registry(&mut before_input, registry);
    before_input
        .manifests
        .retain(|manifest| manifest.source_family != "ens_v1_registrar_l1");
    before_input
        .admissions
        .retain(|admission| admission.source_manifest_id != Some(V1_REGISTRAR_MANIFEST_ID));
    let before = interpret_test_batch(before_input)?;
    assert!(!before.normalized_events.iter().any(|event| {
        event.event_kind == "SurfaceBound" && event.after_state["source_event"] == "Transfer"
    }));

    let mut standalone_input = batch(logs.clone(), &fixture, false);
    admit_v1_registry(&mut standalone_input, registry);
    standalone_input
        .manifests
        .retain(|manifest| manifest.source_family != "ens_v2_migration_l1");
    standalone_input
        .admissions
        .retain(|admission| admission.source_manifest_id != Some(MIGRATION_MANIFEST_ID));
    let standalone = interpret_test_batch(standalone_input)?;
    assert!(
        standalone
            .normalized_events
            .iter()
            .all(|event| { event.source_family != "ens_v2_migration_l1" })
    );
    let standalone_transfer = standalone
        .normalized_events
        .iter()
        .find(|event| {
            event.source_family == "ens_v1_registrar_l1"
                && event.event_kind == "TokenControlTransferred"
                && event.after_state["source_event"] == "Transfer"
                && event.block_number == Some(block + 2)
        })
        .expect("ordinary registrar unwrap transfer without ENSv1→ENSv2 migration correlation");
    assert_eq!(
        standalone_transfer.after_state["expiry"],
        old_registrar_expiry
    );
    assert_eq!(
        standalone_transfer.after_state["fallback_from_wrapper"],
        true
    );

    let mut after_input = batch(logs, &fixture, false);
    admit_v1_registry(&mut after_input, registry);
    let retained_blocks = after_input
        .raw_logs
        .iter()
        .map(|raw| RawBlockInput {
            chain_id: raw.chain_id.clone(),
            block_hash: raw.block_hash.clone(),
            block_number: raw.block_number,
            block_timestamp: raw.block_timestamp,
            canonicality_state: raw.canonicality_state.clone(),
        })
        .collect::<Vec<_>>();
    let after = interpret_test_batch(after_input)?;
    let fallback = after
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "SurfaceBound" && event.after_state["source_event"] == "Transfer"
        })
        .expect("admitted BaseRegistrar transfer restores the unwrap fallback");
    assert_eq!(fallback.source_family, "ens_v1_registrar_l1");
    let transfer = after
        .normalized_events
        .iter()
        .find(|event| {
            event.source_family == "ens_v1_registrar_l1"
                && event.event_kind == "TokenControlTransferred"
        })
        .expect("admitted BaseRegistrar transfer state");
    let prior = super::super::seam::fold_prior_events(
        Vec::new(),
        &after.normalized_events,
        &retained_blocks,
    )?;
    let expired_block = i64::try_from(renewed_registrar_expiry)? + (90 * 24 * 60 * 60) + 1;
    let resume = |at, to, prior| {
        let mut input = batch(
            vec![registrar_transfer_at(
                successor, to, labelhash, at, 0, registrar,
            )],
            &fixture,
            false,
        );
        admit_v1_registry(&mut input, registry);
        input.prior_events = prior;
        interpret_test_batch(input)
    };
    assert!(has_v1_registrar_transfer(&resume(
        block + 4,
        Address::from([0x53; 20]),
        prior.clone(),
    )?));
    assert!(!has_v1_registrar_transfer(&resume(
        expired_block,
        Address::from([0x54; 20]),
        prior,
    )?));
    assert_eq!(transfer.after_state["expiry"], renewed_registrar_expiry);
    assert!(after.resources.iter().any(|resource| {
        Some(resource.resource_id) == fallback.resource_id && resource.token_lineage_id.is_some()
    }));
    Ok(())
}

fn registrar_transfer_at(
    from: Address,
    to: Address,
    token: B256,
    block: i64,
    log: i64,
    registrar: &str,
) -> RawLogInput {
    raw_at_transaction(
        super::v1_registrar::Transfer {
            from,
            to,
            tokenId: U256::from_be_bytes(*token),
        }
        .encode_log_data(),
        block,
        0,
        log,
        registrar,
    )
}

fn has_v1_registrar_transfer(output: &BatchOutput) -> bool {
    output.normalized_events.iter().any(|event| {
        event.source_family == "ens_v1_registrar_l1"
            && event.event_kind == "TokenControlTransferred"
    })
}

#[test]
fn unlocked_wrapped_catalog_shape_is_distinguished_per_name() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["W-01"];
    let addresses = &fixture["addresses"];
    let base_token = decimal_u256(&scenario["base_token_id"])?;
    let v2_token = decimal_u256(&scenario["v2_token_id"])?;
    let label = scenario["label"].as_str().unwrap();
    let block = scenario["migration_block"].as_i64().unwrap();
    let owner = Address::from([0x11; 20]);
    let mut output = interpret_test_batch(batch(
        vec![
            raw_at_transaction(
                super::v1_registrar::Transfer {
                    from: address(addresses, "name_wrapper")?,
                    to: address(addresses, "graveyard")?,
                    tokenId: base_token,
                }
                .encode_log_data(),
                block,
                0,
                scenario["graveyard_transfer_log_index"].as_i64().unwrap(),
                addresses["base_registrar"].as_str().unwrap(),
            ),
            raw_at_transaction(
                super::v2_registry::LabelRegistered {
                    tokenId: v2_token,
                    labelHash: keccak256(label.as_bytes()),
                    label: label.to_owned(),
                    owner,
                    expiry: scenario["stored_expiry"].as_u64().unwrap(),
                    sender: address(addresses, "unlocked_controller")?,
                }
                .encode_log_data(),
                block,
                0,
                scenario["v2_registration_log_index"].as_i64().unwrap(),
                addresses["eth_registry"].as_str().unwrap(),
            ),
            raw_at_transaction(
                super::v2_registry::TokenResource {
                    tokenId: v2_token,
                    resource: v2_token,
                }
                .encode_log_data(),
                block,
                0,
                scenario["v2_registration_log_index"].as_i64().unwrap() + 2,
                addresses["eth_registry"].as_str().unwrap(),
            ),
        ],
        &fixture,
        true,
    ))?;
    let boundary = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "MigrationApplied")
        .expect("unlocked-wrapped authority boundary");
    assert_eq!(boundary.after_state["migration_path"], "unlocked_wrapped");
    assert_eq!(
        boundary.after_state["predecessor_binding"]["resource"]["anchor_kind"],
        "wrapper_backed_control"
    );
    assert_eq!(
        boundary.after_state["predecessor_binding"]["resource"]["contract_address"],
        addresses["name_wrapper"]
    );
    assert!(
        boundary.after_state["predecessor_binding"]["resource"]
            .get("contract_instance_id")
            .is_none()
    );
    assert_eq!(
        boundary.after_state["stored_expiry"],
        scenario["stored_expiry"]
    );
    assert_eq!(boundary.migration_correlation_ids.len(), 1);
    assert_activated_transition(&mut output, "unlocked_wrapped")?;
    Ok(())
}

#[test]
fn two_names_in_one_transaction_keep_separate_authority_boundaries() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let addresses = &fixture["addresses"];
    let unwrapped = &fixture["scenarios"]["U-01"];
    let wrapped = &fixture["scenarios"]["W-01"];
    let block = unwrapped["migration_block"].as_i64().unwrap();
    let mut logs = unlocked_name_logs(unwrapped, addresses, block, 1, false)?;
    logs.extend(unlocked_name_logs(wrapped, addresses, block, 5, true)?);
    let canonical_logs = logs.clone();
    let mut output = interpret_test_batch(batch(logs, &fixture, true))?;
    let boundaries = output
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "MigrationApplied")
        .map(|event| {
            (
                event.logical_name_id.clone().unwrap(),
                event.migration_correlation_ids[0].clone(),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(boundaries.len(), 2);
    assert_eq!(output.migration_candidate_identity_effects.len(), 2);
    for effect in &output.migration_candidate_identity_effects {
        let logical_name = effect.proposed_effect["logical_name_id"].as_str().unwrap();
        assert_eq!(
            effect.migration_correlation_ids,
            [boundaries[logical_name].clone()]
        );
    }
    for association in &output.migration_event_associations {
        let Some(event) = output
            .normalized_events
            .iter()
            .find(|event| event.event_identity == association.event_identity)
        else {
            continue;
        };
        if let Some(logical_name) = event.logical_name_id.as_deref()
            && let Some(expected) = boundaries.get(logical_name)
        {
            assert_eq!(&association.migration_correlation_id, expected);
        }
    }
    super::super::migration::inject_activated_transition_for_test(&mut output)?;
    assert_eq!(output.migration_authority_transitions.len(), 2);
    assert!(
        output
            .migration_authority_transitions
            .iter()
            .all(|transition| {
                output.surface_bindings.iter().any(|binding| {
                    binding.surface_binding_id == transition.successor_surface_binding_id
                        && binding.logical_name_id == transition.logical_name_id
                })
            })
    );
    let canonical_ids = output
        .migration_authority_transitions
        .iter()
        .map(|transition| transition.boundary_event_identity.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let mut orphaned_logs = canonical_logs.clone();
    for raw in &mut orphaned_logs {
        raw.canonicality_state = "orphaned".to_owned();
    }
    let mut orphaned = interpret_test_batch(batch(orphaned_logs, &fixture, true))?;
    assert!(
        orphaned
            .migration_candidate_identity_effects
            .iter()
            .all(|effect| { effect.canonicality_state == "orphaned" })
    );
    super::super::migration::inject_activated_transition_for_test(&mut orphaned)?;
    assert_eq!(
        canonical_ids,
        orphaned
            .migration_authority_transitions
            .iter()
            .map(|transition| transition.boundary_event_identity.clone())
            .collect()
    );
    let mut restored = interpret_test_batch(batch(canonical_logs, &fixture, true))?;
    super::super::migration::inject_activated_transition_for_test(&mut restored)?;
    assert_eq!(
        output.migration_authority_transitions,
        restored.migration_authority_transitions
    );
    Ok(())
}

#[test]
fn resolver_and_ttl_clears_are_optional_boundary_evidence() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["U-01"];
    let addresses = &fixture["addresses"];
    let block = scenario["migration_block"].as_i64().unwrap();
    let migration_logs = unlocked_name_logs(scenario, addresses, block, 4, false)?;
    let absent = interpret_test_batch(batch(migration_logs.clone(), &fixture, true))?;
    assert_eq!(
        absent
            .normalized_events
            .iter()
            .filter(|event| event.event_kind == "MigrationApplied")
            .count(),
        1
    );

    let registry = "0x0000000000000000000000000000000000000099";
    let node = scenario["namehash"].as_str().unwrap().parse::<B256>()?;
    let mut logs = migration_logs;
    logs.extend([
        raw_at_transaction(
            super::v1_registry::NewResolver {
                node,
                resolver: Address::ZERO,
            }
            .encode_log_data(),
            block,
            0,
            2,
            registry,
        ),
        raw_at_transaction(
            super::v1_registry::NewTTL { node, ttl: 0 }.encode_log_data(),
            block,
            0,
            3,
            registry,
        ),
    ]);
    logs.sort_by_key(|raw| raw.log_index);
    let mut input = batch(logs, &fixture, true);
    input.manifests.push(manifest_with_events(
        V1_REGISTRY_MANIFEST_ID,
        "ens",
        "ens_v1_registry_l1",
        &[
            (
                "NewResolver",
                "event NewResolver(bytes32 indexed node, address resolver)",
                &["registry"],
                &["ResolverChanged"],
            ),
            (
                "NewTTL",
                "event NewTTL(bytes32 indexed node, uint64 ttl)",
                &["registry"],
                &[],
            ),
        ],
    ));
    input.admissions.push(admission_at(
        V1_REGISTRY_MANIFEST_ID,
        "registry",
        registry,
        102,
    ));
    let present = interpret_test_batch(input)?;
    assert_eq!(
        present
            .normalized_events
            .iter()
            .filter(|event| event.event_kind == "MigrationApplied")
            .count(),
        1
    );
    let resolver = present
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "ResolverChanged")
        .expect("ordinary resolver clear remains visible");
    assert!(
        present
            .migration_event_associations
            .iter()
            .all(|association| { association.event_identity != resolver.event_identity })
    );
    Ok(())
}

#[test]
fn cross_family_registrar_renewal_preserves_resource_anchored_multiplicity() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["R-01"];
    let addresses = &fixture["addresses"];
    let label = scenario["label"].as_str().unwrap();
    let base_token = decimal_u256(&scenario["base_token_id"])?;
    let v2_token = decimal_u256(&scenario["v2_token_id"])?;
    let renewal_block = scenario["renewal_block"].as_i64().unwrap();
    let sender = Address::from([0x22; 20]);
    let payment = Address::from([0x33; 20]);
    let logs = vec![
        raw_at_transaction(
            super::v2_registry::LabelReserved {
                tokenId: v2_token,
                labelHash: keccak256(label.as_bytes()),
                label: label.to_owned(),
                expiry: 1_822_787_383,
                sender,
            }
            .encode_log_data(),
            226,
            0,
            1,
            addresses["eth_registry"].as_str().unwrap(),
        ),
        raw_at_transaction(
            super::v2_registry::ExpiryUpdated {
                tokenId: v2_token,
                newExpiry: scenario["decoded_v2_expiry"].as_u64().unwrap(),
                sender,
            }
            .encode_log_data(),
            renewal_block,
            0,
            1,
            addresses["eth_registry"].as_str().unwrap(),
        ),
        raw_at_transaction(
            with_topic0(
                BaseNameRenewed {
                    id: base_token,
                    expires: U256::from(scenario["v1_expiry"].as_u64().unwrap()),
                }
                .encode_log_data(),
                keccak256(b"NameRenewed(uint256,uint256)"),
            ),
            renewal_block,
            0,
            2,
            addresses["base_registrar"].as_str().unwrap(),
        ),
        raw_at_transaction(
            with_topic0(
                BridgeNameRenewed {
                    tokenId: v2_token,
                    label: label.to_owned(),
                    duration: scenario["duration"].as_u64().unwrap(),
                    newExpiry: scenario["decoded_v2_expiry"].as_u64().unwrap(),
                    paymentToken: payment,
                    referrer: B256::ZERO,
                    amount: U256::from(640_000_005_u64),
                }
                .encode_log_data(),
                alloy_primitives::keccak256(
                    b"NameRenewed(uint256,string,uint64,uint64,address,bytes32,uint256)",
                ),
            ),
            renewal_block,
            0,
            3,
            addresses["renewal_bridge"].as_str().unwrap(),
        ),
    ];
    let output = interpret_test_batch(batch(logs.clone(), &fixture, true))?;
    let renewals = output
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "RegistrationRenewed")
        .collect::<Vec<_>>();
    let expiries = output
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "ExpiryChanged")
        .collect::<Vec<_>>();
    assert_eq!(renewals.len(), 2, "events: {:#?}", output.normalized_events);
    assert_eq!(expiries.len(), 2, "events: {:#?}", output.normalized_events);
    assert_eq!(
        renewals
            .iter()
            .filter(|event| event.source_family == "ens_v2_migration_l1")
            .count(),
        2
    );
    assert!(
        !renewals
            .iter()
            .any(|event| event.source_family == "ens_v2_registry_l1")
    );
    assert!(
        !output
            .normalized_events
            .iter()
            .any(|event| event.event_kind == "MigrationApplied")
    );
    assert!(
        !output
            .normalized_events
            .iter()
            .any(|event| event.event_kind == "RegistrationGranted")
    );
    assert_eq!(output.token_lineages.len(), 1);
    assert!(output.surface_bindings.is_empty());
    assert!(
        expiries
            .iter()
            .filter(|event| event.source_family == "ens_v2_registry_l1")
            .all(|event| event.resource_id.is_some())
    );
    let bridge = renewals
        .iter()
        .find(|event| {
            event.source_family == "ens_v2_migration_l1"
                && event.after_state.get("duration").is_some()
        })
        .expect("bridge renewal");
    let base = renewals
        .iter()
        .find(|event| {
            event.source_family == "ens_v2_migration_l1"
                && event.after_state.get("duration").is_none()
        })
        .expect("BaseRegistrar renewal");
    let registry = expiries
        .iter()
        .find(|event| event.source_family == "ens_v2_registry_l1")
        .expect("v2 registry expiry arm");
    assert_eq!(bridge.resource_id, registry.resource_id);
    assert!(base.resource_id.is_none());
    assert_eq!(
        bridge.raw_fact_ref["state_scope"],
        format!(
            "migration-renewal:bridge:{}",
            bridge.logical_name_id.as_deref().unwrap()
        )
    );
    assert_eq!(
        base.raw_fact_ref["state_scope"],
        format!(
            "migration-renewal:base-registrar:{}",
            base.logical_name_id.as_deref().unwrap()
        )
    );
    assert_ne!(
        bridge.raw_fact_ref["interpreter_state_key"],
        base.raw_fact_ref["interpreter_state_key"]
    );
    assert!(
        bridge.raw_fact_ref["interpreter_state_key"]
            .as_str()
            .unwrap()
            .contains(&bridge.resource_id.unwrap().to_string()),
        "the bridge state key must agree with its post-correlation resource anchor"
    );
    assert_eq!(bridge.before_state, json!({}));
    assert_eq!(base.before_state, json!({}));
    assert_eq!(
        base.after_state["resource_anchor"]["anchor_kind"],
        "registrar_backed_registration"
    );
    assert_eq!(
        base.after_state["resource_anchor"]["consumer_visibility"],
        "candidate"
    );

    let versioned_token = v2_token + U256::from(1);
    let mut versioned_logs = logs.clone();
    versioned_logs[0] = raw_at_transaction(
        super::v2_registry::LabelReserved {
            tokenId: versioned_token,
            labelHash: keccak256(label.as_bytes()),
            label: label.to_owned(),
            expiry: 1_822_787_383,
            sender,
        }
        .encode_log_data(),
        226,
        0,
        1,
        addresses["eth_registry"].as_str().unwrap(),
    );
    versioned_logs[1] = raw_at_transaction(
        super::v2_registry::ExpiryUpdated {
            tokenId: versioned_token,
            newExpiry: scenario["decoded_v2_expiry"].as_u64().unwrap(),
            sender,
        }
        .encode_log_data(),
        renewal_block,
        0,
        1,
        addresses["eth_registry"].as_str().unwrap(),
    );
    versioned_logs[3] = raw_at_transaction(
        with_topic0(
            BridgeNameRenewed {
                tokenId: versioned_token,
                label: label.to_owned(),
                duration: scenario["duration"].as_u64().unwrap(),
                newExpiry: scenario["decoded_v2_expiry"].as_u64().unwrap(),
                paymentToken: payment,
                referrer: B256::ZERO,
                amount: U256::from(640_000_005_u64),
            }
            .encode_log_data(),
            alloy_primitives::keccak256(
                b"NameRenewed(uint256,string,uint64,uint64,address,bytes32,uint256)",
            ),
        ),
        renewal_block,
        0,
        3,
        addresses["renewal_bridge"].as_str().unwrap(),
    );
    let versioned = interpret_test_batch(batch(versioned_logs, &fixture, true))?;
    let versioned_renewals = versioned
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "RegistrationRenewed")
        .collect::<Vec<_>>();
    assert_eq!(
        versioned_renewals
            .iter()
            .filter(|event| event.source_family == "ens_v2_migration_l1")
            .count(),
        2,
        "a versioned reserved entry still retains both synchronized-renewal arms"
    );
    assert!(
        versioned_renewals
            .iter()
            .all(|event| { event.after_state["lifecycle_classification"] != "historical_renewal" })
    );
    assert!(
        versioned_renewals
            .iter()
            .all(|event| event.resource_id.is_none()),
        "a non-derived reservation must not invent resource anchors for either renewal arm"
    );
    let versioned_registry = versioned
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "ExpiryChanged"
                && event.source_family == "ens_v2_registry_l1"
                && event.after_state["expiry"] == scenario["decoded_v2_expiry"]
        })
        .expect("resource-less registry expiry arm");
    assert!(versioned_registry.resource_id.is_none());
    let correlation_ids = versioned_renewals
        .iter()
        .flat_map(|event| event.migration_correlation_ids.iter())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        correlation_ids.len(),
        1,
        "resource-less renewal arms must share one correlation identity"
    );
    let correlation_id = *correlation_ids.iter().next().unwrap();
    assert!(versioned_renewals.iter().all(|event| {
        event.migration_correlation_ids.as_slice() == [correlation_id.to_owned()]
    }));
    assert!(
        versioned
            .migration_event_associations
            .iter()
            .any(|association| {
                association.event_identity == versioned_registry.event_identity
                    && association.migration_correlation_id == correlation_id.as_str()
                    && association.correlation_kind == "synchronized_renewal"
            })
    );

    let mut nonstandard_delta = logs.clone();
    nonstandard_delta[2] = raw_at_transaction(
        with_topic0(
            BaseNameRenewed {
                id: base_token,
                expires: U256::from(scenario["v1_expiry"].as_u64().unwrap() - 123),
            }
            .encode_log_data(),
            keccak256(b"NameRenewed(uint256,uint256)"),
        ),
        renewal_block,
        0,
        2,
        addresses["base_registrar"].as_str().unwrap(),
    );
    let nonstandard = interpret_test_batch(batch(nonstandard_delta, &fixture, true))?;
    assert_eq!(
        nonstandard
            .normalized_events
            .iter()
            .filter(|event| event.event_kind == "RegistrationRenewed")
            .count(),
        2,
        "decoded per-resource expiries must not be rejected by a reconstructed fixed offset"
    );

    let second_v2_expiry = scenario["decoded_v2_expiry"].as_u64().unwrap() + 3_600;
    let second_v1_expiry = scenario["v1_expiry"].as_u64().unwrap() + 3_600;
    let mut repeated = logs;
    repeated.extend([
        raw_at_transaction(
            super::v2_registry::ExpiryUpdated {
                tokenId: v2_token,
                newExpiry: second_v2_expiry,
                sender,
            }
            .encode_log_data(),
            renewal_block,
            0,
            4,
            addresses["eth_registry"].as_str().unwrap(),
        ),
        raw_at_transaction(
            with_topic0(
                BaseNameRenewed {
                    id: base_token,
                    expires: U256::from(second_v1_expiry),
                }
                .encode_log_data(),
                keccak256(b"NameRenewed(uint256,uint256)"),
            ),
            renewal_block,
            0,
            5,
            addresses["base_registrar"].as_str().unwrap(),
        ),
        raw_at_transaction(
            with_topic0(
                BridgeNameRenewed {
                    tokenId: v2_token,
                    label: label.to_owned(),
                    duration: scenario["duration"].as_u64().unwrap(),
                    newExpiry: second_v2_expiry,
                    paymentToken: payment,
                    referrer: B256::ZERO,
                    amount: U256::from(640_000_005_u64),
                }
                .encode_log_data(),
                alloy_primitives::keccak256(
                    b"NameRenewed(uint256,string,uint64,uint64,address,bytes32,uint256)",
                ),
            ),
            renewal_block,
            0,
            6,
            addresses["renewal_bridge"].as_str().unwrap(),
        ),
    ]);
    let repeated = interpret_test_batch(batch(repeated, &fixture, true))?;
    let migration_renewals = repeated
        .normalized_events
        .iter()
        .filter(|event| {
            event.source_family == "ens_v2_migration_l1"
                && event.event_kind == "RegistrationRenewed"
        })
        .collect::<Vec<_>>();
    assert_eq!(migration_renewals.len(), 4);
    for (base_index, bridge_index) in [(2, 3), (5, 6)] {
        let base = migration_renewals
            .iter()
            .find(|event| event.log_index == Some(base_index))
            .expect("each BaseRegistrar renewal remains correlated");
        let bridge = migration_renewals
            .iter()
            .find(|event| event.log_index == Some(bridge_index))
            .expect("each bridge renewal remains correlated");
        assert_eq!(
            base.migration_correlation_ids, bridge.migration_correlation_ids,
            "each repeated renewal must use its own BaseRegistrar/bridge envelope"
        );
    }
    Ok(())
}

#[test]
fn reserved_facts_survive_claim_and_incremental_rebuild_boundaries() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["R-01"];
    let addresses = &fixture["addresses"];
    let label = scenario["label"].as_str().unwrap();
    let token = decimal_u256(&scenario["v2_token_id"])?;
    let base_token = decimal_u256(&scenario["base_token_id"])?;
    let owner = Address::from([0x41; 20]);
    let sender = Address::from([0x42; 20]);
    let resolver = Address::from([0x43; 20]);
    let expiry = scenario["decoded_v2_expiry"].as_u64().unwrap();
    let stages = vec![
        vec![raw_at_transaction(
            super::v2_registry::LabelReserved {
                tokenId: token,
                labelHash: keccak256(label.as_bytes()),
                label: label.to_owned(),
                expiry: expiry - 100,
                sender,
            }
            .encode_log_data(),
            226,
            0,
            0,
            addresses["eth_registry"].as_str().unwrap(),
        )],
        vec![raw_at_transaction(
            super::v2_registry::ResolverUpdated {
                tokenId: token,
                resolver,
                sender,
            }
            .encode_log_data(),
            226,
            0,
            1,
            addresses["eth_registry"].as_str().unwrap(),
        )],
        vec![raw_at_transaction(
            super::v2_registry::ExpiryUpdated {
                tokenId: token,
                newExpiry: expiry,
                sender,
            }
            .encode_log_data(),
            227,
            0,
            0,
            addresses["eth_registry"].as_str().unwrap(),
        )],
        vec![
            raw_at_transaction(
                super::v1_registrar::Transfer {
                    from: address(addresses, "unlocked_controller")?,
                    to: address(addresses, "graveyard")?,
                    tokenId: base_token,
                }
                .encode_log_data(),
                228,
                0,
                0,
                addresses["base_registrar"].as_str().unwrap(),
            ),
            raw_at_transaction(
                super::v2_registry::LabelRegistered {
                    tokenId: token,
                    labelHash: keccak256(label.as_bytes()),
                    label: label.to_owned(),
                    owner,
                    expiry,
                    sender: address(addresses, "unlocked_controller")?,
                }
                .encode_log_data(),
                228,
                0,
                1,
                addresses["eth_registry"].as_str().unwrap(),
            ),
            raw_at_transaction(
                super::v2_registry::TokenResource {
                    tokenId: token,
                    resource: token,
                }
                .encode_log_data(),
                228,
                0,
                2,
                addresses["eth_registry"].as_str().unwrap(),
            ),
        ],
        vec![raw_at_transaction(
            super::v2_registry::ExpiryUpdated {
                tokenId: token,
                newExpiry: expiry + 1,
                sender,
            }
            .encode_log_data(),
            229,
            0,
            0,
            addresses["eth_registry"].as_str().unwrap(),
        )],
        vec![raw_at_transaction(
            with_topic0(
                BaseNameRenewed {
                    id: base_token,
                    expires: U256::from(scenario["v1_expiry"].as_u64().unwrap() + 1),
                }
                .encode_log_data(),
                keccak256(b"NameRenewed(uint256,uint256)"),
            ),
            230,
            0,
            0,
            addresses["base_registrar"].as_str().unwrap(),
        )],
    ];

    let (preclaim, preclaim_session) = interpret_test_batch_incremental(
        batch(
            stages[..3].iter().flatten().cloned().collect(),
            &fixture,
            true,
        ),
        None,
    )?;
    let reserved = preclaim
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationReserved")
        .expect("reservation evidence");
    let resource_id = reserved
        .resource_id
        .expect("reserved registry-entry resource");
    for event in preclaim.normalized_events.iter().filter(|event| {
        matches!(
            event.event_kind.as_str(),
            "ResolverChanged" | "ExpiryChanged"
        )
    }) {
        assert_eq!(event.resource_id, Some(resource_id));
    }
    assert!(!preclaim.normalized_events.iter().any(|event| {
        matches!(
            event.event_kind.as_str(),
            "RegistrationGranted" | "RegistrationRenewed" | "AuthorityTransferred"
        )
    }));
    assert_eq!(preclaim.token_lineages.len(), 1);
    assert!(preclaim.surface_bindings.is_empty());

    let block = |number| RawBlockInput {
        chain_id: CHAIN.to_owned(),
        block_hash: format!("block-{number}"),
        block_number: number,
        block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(number),
        canonicality_state: "canonical".to_owned(),
    };
    let prior_events = seam::fold_prior_events(
        Vec::new(),
        &preclaim.normalized_events,
        &[block(226), block(227)],
    )?;
    let mut restored_claim_input = batch(stages[3].clone(), &fixture, true);
    restored_claim_input.prior_events = prior_events;
    let restored_claim = interpret_test_batch(restored_claim_input)?;
    let (session_claim, _) = interpret_test_batch_incremental(
        batch(stages[3].clone(), &fixture, true),
        Some(preclaim_session),
    )?;
    assert_eq!(
        restored_claim.normalized_events,
        session_claim.normalized_events
    );
    assert_eq!(restored_claim.resources, session_claim.resources);
    assert_eq!(restored_claim.token_lineages, session_claim.token_lineages);
    assert_eq!(
        restored_claim.migration_event_associations,
        session_claim.migration_event_associations
    );

    let all_logs = stages.iter().flatten().cloned().collect::<Vec<_>>();
    let (full, full_session) =
        interpret_test_batch_incremental(batch(all_logs, &fixture, true), None)?;
    let resource_lineages = full
        .resources
        .iter()
        .filter(|resource| resource.resource_id == resource_id)
        .map(|resource| resource.token_lineage_id)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        resource_lineages,
        std::collections::BTreeSet::from([Some(preclaim.token_lineages[0].token_lineage_id)]),
        "a persisted reserved resource cannot acquire different lineage metadata at claim"
    );
    let claim = full
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "RegistrationGranted"
                && event.after_state["source_event"] == "LabelRegistered"
        })
        .expect("reserved entry claim");
    assert_eq!(claim.resource_id, Some(resource_id));
    assert_eq!(claim.after_state["expiry"], expiry);
    assert_eq!(
        full.normalized_events
            .iter()
            .filter(|event| event.event_kind == "MigrationApplied")
            .count(),
        1
    );
    let residue = full
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "RegistrationRenewed"
                && event.after_state["lifecycle_classification"] == "historical_renewal"
        })
        .expect("post-migration ENSv1 renewal residue");
    assert!(residue.resource_id.is_none());

    let mut mismatched_claim = stages[3].clone();
    mismatched_claim[2] = raw_at_transaction(
        super::v2_registry::TokenResource {
            tokenId: token,
            resource: token + U256::from(1),
        }
        .encode_log_data(),
        228,
        0,
        2,
        addresses["eth_registry"].as_str().unwrap(),
    );
    let mismatched_logs = stages[..3]
        .iter()
        .flatten()
        .cloned()
        .chain(mismatched_claim)
        .collect();
    let mismatch = interpret_test_batch(batch(mismatched_logs, &fixture, true))
        .expect_err("TokenResource must confirm the resource retained from reservation");
    assert!(format!("{mismatch:#}").contains("does not confirm"));

    let mut incremental_events = Vec::new();
    let mut incremental_associations = Vec::new();
    let mut session = None;
    for stage in stages {
        let (output, next) =
            interpret_test_batch_incremental(batch(stage, &fixture, true), session)?;
        incremental_events.extend(output.normalized_events);
        incremental_associations.extend(output.migration_event_associations);
        session = Some(next);
    }
    assert_eq!(incremental_events, full.normalized_events);
    assert_eq!(incremental_associations, full.migration_event_associations);
    let probe = raw_at_transaction(
        super::v2_registry::ExpiryUpdated {
            tokenId: token,
            newExpiry: expiry + 2,
            sender,
        }
        .encode_log_data(),
        231,
        0,
        0,
        addresses["eth_registry"].as_str().unwrap(),
    );
    let (full_probe, _) = interpret_test_batch_incremental(
        batch(vec![probe.clone()], &fixture, true),
        Some(full_session),
    )?;
    let (incremental_probe, _) =
        interpret_test_batch_incremental(batch(vec![probe], &fixture, true), session)?;
    assert_eq!(incremental_probe, full_probe);
    assert!(full_probe.normalized_events.iter().all(|event| {
        !matches!(
            event.event_kind.as_str(),
            "ExpiryChanged" | "RegistrationRenewed"
        ) || event.resource_id == Some(resource_id)
    }));
    Ok(())
}

#[test]
fn premigration_reservation_flood_is_ownerless_and_linear() -> anyhow::Result<()> {
    const RESERVATIONS: usize = 256;
    let fixture = fixture()?;
    let addresses = &fixture["addresses"];
    let sender = Address::from([0x51; 20]);
    let mut logs = Vec::with_capacity(RESERVATIONS);
    for index in 0..RESERVATIONS {
        let label = format!("premigration-{index}");
        let mut token = keccak256(label.as_bytes()).0;
        token[28..].fill(0);
        logs.push(raw_at_transaction(
            super::v2_registry::LabelReserved {
                tokenId: U256::from_be_bytes(token),
                labelHash: keccak256(label.as_bytes()),
                label,
                expiry: 2_000_000_000,
                sender,
            }
            .encode_log_data(),
            226,
            0,
            i64::try_from(index).unwrap(),
            addresses["eth_registry"].as_str().unwrap(),
        ));
    }
    crate::schema_v2::state::reset_v2_refresh_visits();
    let (output, _) = interpret_test_batch_incremental(batch(logs, &fixture, true), None)?;
    assert_eq!(
        output
            .normalized_events
            .iter()
            .filter(|event| event.event_kind == "RegistrationReserved")
            .count(),
        RESERVATIONS
    );
    assert!(!output.normalized_events.iter().any(|event| {
        matches!(
            event.event_kind.as_str(),
            "RegistrationGranted" | "RegistrationRenewed" | "AuthorityTransferred" | "SurfaceBound"
        )
    }));
    assert_eq!(output.token_lineages.len(), RESERVATIONS);
    assert_eq!(
        output.label_preimages.len(),
        RESERVATIONS + 1,
        "the shared .eth preimage must be submitted once per adapter batch"
    );
    assert!(output.surface_bindings.is_empty());
    assert!(
        crate::schema_v2::state::v2_refresh_visits() <= RESERVATIONS * 3,
        "reservation refresh must remain linear"
    );

    let label = "versioned-reservation";
    let mut versioned_token = keccak256(label.as_bytes()).0;
    versioned_token[28..].fill(0);
    versioned_token[31] = 1;
    let versioned = interpret_test_batch(batch(
        vec![raw_at_transaction(
            super::v2_registry::LabelReserved {
                tokenId: U256::from_be_bytes(versioned_token),
                labelHash: keccak256(label.as_bytes()),
                label: label.to_owned(),
                expiry: 2_000_000_000,
                sender,
            }
            .encode_log_data(),
            226,
            1,
            0,
            addresses["eth_registry"].as_str().unwrap(),
        )],
        &fixture,
        true,
    ))?;
    let reservation = versioned
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationReserved")
        .expect("versioned reservation evidence");
    assert!(reservation.resource_id.is_none());
    assert!(versioned.resources.is_empty());
    assert!(versioned.token_lineages.is_empty());
    Ok(())
}

#[test]
fn cross_family_registrar_controller_uses_a_separate_name_wrapper_envelope() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["R-05"];
    let addresses = &fixture["addresses"];
    let controller = scenario["controller"]
        .as_str()
        .unwrap()
        .parse::<Address>()?;
    let token_id = decimal_u256(&scenario["base_token_id"])?;
    let expiry = decimal_u256(&scenario["synchronized_expiry"])?;
    let block = scenario["configuration_block"].as_i64().unwrap();
    let logs = vec![
        raw_at_transaction(
            ControllerAdded { controller }.encode_log_data(),
            block,
            0,
            scenario["add_log_index"].as_i64().unwrap(),
            addresses["base_registrar"].as_str().unwrap(),
        ),
        raw_at_transaction(
            with_topic0(
                BaseNameRenewed {
                    id: token_id,
                    expires: expiry,
                }
                .encode_log_data(),
                keccak256(b"NameRenewed(uint256,uint256)"),
            ),
            block,
            0,
            1,
            addresses["base_registrar"].as_str().unwrap(),
        ),
        raw_at_transaction(
            ControllerRemoved { controller }.encode_log_data(),
            block,
            0,
            scenario["remove_log_index"].as_i64().unwrap(),
            addresses["base_registrar"].as_str().unwrap(),
        ),
    ];
    let output = interpret_test_batch(batch(logs.clone(), &fixture, false))?;
    let events = output
        .normalized_events
        .iter()
        .filter(|event| {
            matches!(
                event.event_kind.as_str(),
                "PermissionChanged" | "RegistrationRenewed" | "ExpiryChanged"
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(events.len(), 4);
    assert!(events.iter().all(|event| {
        event.migration_correlation_ids.len() == 1
            && event.migration_correlation_ids == events[0].migration_correlation_ids
    }));
    assert!(
        events
            .iter()
            .filter(|event| event.event_kind == "RegistrationRenewed")
            .all(|event| {
                event.resource_id.is_none()
                    && event.after_state["resource_anchor"]["anchor_kind"]
                        == "registrar_backed_registration"
            })
    );

    let unrelated = Address::from([0x99; 20]);
    let mut unrelated_logs = logs.clone();
    unrelated_logs[0] = raw_at_transaction(
        ControllerAdded {
            controller: unrelated,
        }
        .encode_log_data(),
        block,
        0,
        0,
        addresses["base_registrar"].as_str().unwrap(),
    );
    unrelated_logs[2] = raw_at_transaction(
        ControllerRemoved {
            controller: unrelated,
        }
        .encode_log_data(),
        block,
        0,
        3,
        addresses["base_registrar"].as_str().unwrap(),
    );
    let unrelated_output = interpret_test_batch(batch(unrelated_logs, &fixture, false))?;
    assert!(unrelated_output.normalized_events.iter().all(|event| {
        !matches!(
            event.event_kind.as_str(),
            "RegistrationRenewed" | "ExpiryChanged"
        ) || event.after_state["lifecycle_classification"] == "historical_renewal"
    }));
    assert!(unrelated_output.resources.is_empty());
    assert!(unrelated_output.token_lineages.is_empty());
    assert!(
        unrelated_output
            .normalized_events
            .iter()
            .all(|event| { event.after_state.get("wrapper_expiry").is_none() })
    );

    let mut mismatched_logs = logs;
    mismatched_logs[2] = raw_at_transaction(
        ControllerRemoved {
            controller: unrelated,
        }
        .encode_log_data(),
        block,
        0,
        3,
        addresses["base_registrar"].as_str().unwrap(),
    );
    let mismatched_output = interpret_test_batch(batch(mismatched_logs, &fixture, false))?;
    assert!(mismatched_output.normalized_events.iter().all(|event| {
        !matches!(
            event.event_kind.as_str(),
            "RegistrationRenewed" | "ExpiryChanged"
        ) || event.after_state["lifecycle_classification"] == "historical_renewal"
    }));
    assert!(mismatched_output.resources.is_empty());
    assert!(mismatched_output.token_lineages.is_empty());
    assert!(
        mismatched_output
            .normalized_events
            .iter()
            .all(|event| { event.after_state.get("wrapper_expiry").is_none() })
    );
    Ok(())
}

#[test]
fn numeric_base_registrar_events_are_silent_without_the_migration_family() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let addresses = &fixture["addresses"];
    let registrar = addresses["base_registrar"].as_str().unwrap();
    let controller = address(addresses, "name_wrapper")?;
    let block = fixture["scenarios"]["R-05"]["configuration_block"]
        .as_i64()
        .unwrap();
    let token = decimal_u256(&fixture["scenarios"]["R-05"]["base_token_id"])?;
    let mut input = batch(
        vec![
            raw_at_transaction(
                ControllerAdded { controller }.encode_log_data(),
                block,
                0,
                0,
                registrar,
            ),
            raw_at_transaction(
                ControllerRemoved { controller }.encode_log_data(),
                block,
                0,
                1,
                registrar,
            ),
            raw_at_transaction(
                with_topic0(
                    BaseNameRegistered {
                        id: token,
                        owner: Address::from([0x51; 20]),
                        expires: U256::from(2_000_000_000_u64),
                    }
                    .encode_log_data(),
                    keccak256(b"NameRegistered(uint256,address,uint256)"),
                ),
                block,
                0,
                2,
                registrar,
            ),
            base_renewed_at(addresses, token, 2_000_000_001, block, 3),
        ],
        &fixture,
        false,
    );
    input
        .manifests
        .retain(|manifest| manifest.source_family != "ens_v2_migration_l1");
    input
        .admissions
        .retain(|admission| admission.source_manifest_id != Some(MIGRATION_MANIFEST_ID));

    let output = interpret_test_batch(input)?;
    assert!(output.normalized_events.is_empty());
    assert!(output.resources.is_empty());
    assert!(output.token_lineages.is_empty());
    assert!(output.migration_event_associations.is_empty());
    Ok(())
}

#[test]
fn unmatched_controller_addition_cannot_change_a_later_transaction_renewal() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let addresses = &fixture["addresses"];
    let registrar = addresses["base_registrar"].as_str().unwrap();
    let wrapper = addresses["name_wrapper"].as_str().unwrap();
    let registry = "0x0000000000000000000000000000000000000099";
    let label = b"transaction-scoped";
    let labelhash = keccak256(label);
    let node = super::common::namehash(&["transaction-scoped".to_owned(), "eth".to_owned()]);
    let block = fixture["scenarios"]["R-05"]["configuration_block"]
        .as_i64()
        .unwrap();
    let registrar_expiry = u64::try_from(block)? + 20_000;
    let wrapper_expiry = registrar_expiry + (90 * 24 * 60 * 60);
    let setup_and_unmatched_add = vec![
        raw_at_transaction(
            super::v1_registry::Transfer {
                node: node.parse()?,
                owner: wrapper.parse()?,
            }
            .encode_log_data(),
            block,
            0,
            0,
            registry,
        ),
        raw_at_transaction(
            super::NameWrapped {
                node: node.parse()?,
                name: b"\x12transaction-scoped\x03eth\0".to_vec().into(),
                owner: Address::from([0x51; 20]),
                fuses: (1 << 16) | (1 << 17),
                expiry: wrapper_expiry,
            }
            .encode_log_data(),
            block,
            0,
            1,
            wrapper,
        ),
        raw_at_transaction(
            ControllerAdded {
                controller: wrapper.parse()?,
            }
            .encode_log_data(),
            block + 1,
            0,
            0,
            registrar,
        ),
    ];
    let renewal = base_renewed_at(
        addresses,
        U256::from_be_bytes(*labelhash),
        registrar_expiry,
        block + 2,
        0,
    );

    let mut full_input = batch(
        setup_and_unmatched_add
            .iter()
            .cloned()
            .chain(std::iter::once(renewal.clone()))
            .collect(),
        &fixture,
        false,
    );
    admit_v1_registry(&mut full_input, registry);
    let (full, _) = interpret_test_batch_incremental(full_input, None)?;

    let mut first_input = batch(setup_and_unmatched_add, &fixture, false);
    admit_v1_registry(&mut first_input, registry);
    let (_, session) = interpret_test_batch_incremental(first_input, None)?;
    let mut second_input = batch(vec![renewal], &fixture, false);
    admit_v1_registry(&mut second_input, registry);
    let (split, _) = interpret_test_batch_incremental(second_input, Some(session))?;

    let renewal_state = |output: &BatchOutput| {
        output
            .normalized_events
            .iter()
            .find(|event| {
                event.source_family == "ens_v2_migration_l1"
                    && event.event_kind == "RegistrationRenewed"
            })
            .map(|event| event.after_state.clone())
            .expect("migration-family BaseRegistrar renewal")
    };
    assert_eq!(renewal_state(&full), renewal_state(&split));
    assert!(renewal_state(&full).get("wrapper_expiry").is_none());
    Ok(())
}

#[test]
fn candidate_registrar_renewal_cannot_change_later_ordinary_wrapper_output() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let addresses = &fixture["addresses"];
    let registrar = addresses["base_registrar"].as_str().unwrap();
    let wrapper = addresses["name_wrapper"].as_str().unwrap();
    let registry = "0x0000000000000000000000000000000000000099";
    let label = b"candidate-state";
    let labelhash = keccak256(label);
    let node = super::common::namehash(&["candidate-state".to_owned(), "eth".to_owned()]);
    let block = fixture["scenarios"]["R-05"]["configuration_block"]
        .as_i64()
        .unwrap();
    let initial_wrapper_expiry = 7_776_100;
    let registrar_expiry = 200;
    let later_wrapper_expiry = 7_776_300;
    let setup = vec![
        raw_at_transaction(
            super::v1_registry::Transfer {
                node: node.parse()?,
                owner: wrapper.parse()?,
            }
            .encode_log_data(),
            block,
            0,
            0,
            registry,
        ),
        raw_at_transaction(
            super::NameWrapped {
                node: node.parse()?,
                name: b"\x0fcandidate-state\x03eth\0".to_vec().into(),
                owner: Address::from([0x51; 20]),
                fuses: (1 << 16) | (1 << 17),
                expiry: initial_wrapper_expiry,
            }
            .encode_log_data(),
            block,
            0,
            1,
            wrapper,
        ),
    ];
    let correlation = vec![
        raw_at_transaction(
            ControllerAdded {
                controller: wrapper.parse()?,
            }
            .encode_log_data(),
            block + 1,
            0,
            0,
            registrar,
        ),
        base_renewed_at(
            addresses,
            U256::from_be_bytes(*labelhash),
            registrar_expiry,
            block + 1,
            1,
        ),
        raw_at_transaction(
            ControllerRemoved {
                controller: wrapper.parse()?,
            }
            .encode_log_data(),
            block + 1,
            0,
            2,
            registrar,
        ),
    ];
    let later = raw_at_transaction(
        super::ExpiryExtended {
            node: node.parse()?,
            expiry: later_wrapper_expiry,
        }
        .encode_log_data(),
        block + 2,
        0,
        0,
        wrapper,
    );
    let run = |logs| -> anyhow::Result<BatchOutput> {
        let mut input = batch(logs, &fixture, false);
        admit_v1_registry(&mut input, registry);
        interpret_test_batch(input)
    };
    let wrapper_expiry_event = |output: &BatchOutput| {
        output
            .normalized_events
            .iter()
            .find(|event| {
                event.source_family == "ens_v1_wrapper_l1"
                    && event.after_state["source_event"] == "ExpiryExtended"
            })
            .cloned()
            .expect("ordinary NameWrapper ExpiryExtended output")
    };

    let control = run(setup
        .iter()
        .cloned()
        .chain(std::iter::once(later.clone()))
        .collect())?;
    let correlated = run(setup
        .iter()
        .cloned()
        .chain(correlation.iter().cloned())
        .chain(std::iter::once(later.clone()))
        .collect())?;
    assert!(correlated.normalized_events.iter().any(|event| {
        event.source_family == "ens_v2_migration_l1"
            && event.after_state["source_event"] == "NameRenewed"
            && event.after_state["wrapper_expiry"] == 7_776_200
    }));
    assert_eq!(
        wrapper_expiry_event(&correlated),
        wrapper_expiry_event(&control),
        "candidate renewal state must not alter a later ordinary event during full replay"
    );

    let seeded_incomplete = run(setup
        .iter()
        .cloned()
        .chain(correlation.iter().take(2).cloned())
        .collect())?;
    let unseeded_incomplete = run(correlation.iter().take(2).cloned().collect())?;
    let historical_renewal = |output: &BatchOutput| {
        output
            .normalized_events
            .iter()
            .find(|event| {
                event.source_family == "ens_v2_migration_l1"
                    && event.event_kind == "RegistrationRenewed"
            })
            .cloned()
            .expect("incomplete controller envelope remains historical renewal evidence")
    };
    assert_eq!(
        historical_renewal(&seeded_incomplete).migration_correlation_ids,
        historical_renewal(&unseeded_incomplete).migration_correlation_ids,
        "unproven wrapper state must not change historical-renewal evidence identity"
    );
    assert!(
        historical_renewal(&seeded_incomplete)
            .after_state
            .get("wrapper_expiry")
            .is_none()
    );

    let staged = run(setup.into_iter().chain(correlation).collect::<Vec<_>>())?;
    let mut cold_input = batch(vec![later], &fixture, false);
    admit_v1_registry(&mut cold_input, registry);
    cold_input.prior_events = staged
        .normalized_events
        .iter()
        .map(super::prior_event)
        .collect();
    let cold = interpret_test_batch(cold_input)?;
    assert_eq!(
        wrapper_expiry_event(&cold),
        wrapper_expiry_event(&control),
        "candidate renewal state must not alter a later ordinary event after cold restore"
    );
    Ok(())
}

#[test]
fn unwrap_fallback_cannot_materialize_a_stale_wrapper_derived_registrar_expiry()
-> anyhow::Result<()> {
    let fixture = fixture()?;
    let addresses = &fixture["addresses"];
    let registrar = addresses["base_registrar"].as_str().unwrap();
    let wrapper = addresses["name_wrapper"].as_str().unwrap();
    let registry = "0x0000000000000000000000000000000000000099";
    let owner = Address::from([0x51; 20]);
    let label = b"candidate-fallback";
    let labelhash = keccak256(label);
    let node = super::common::namehash(&["candidate-fallback".to_owned(), "eth".to_owned()]);
    let block = fixture["scenarios"]["R-05"]["configuration_block"]
        .as_i64()
        .unwrap();
    let grace = 90 * 24 * 60 * 60;
    let initial_registrar_expiry = u64::try_from(block)? + 10;
    let initial_wrapper_expiry = initial_registrar_expiry + grace;
    let renewed_registrar_expiry = initial_registrar_expiry + 1_000;
    let unwrap_block = i64::try_from(initial_wrapper_expiry + 1)?;
    let logs = vec![
        raw_at_transaction(
            super::v1_registry::Transfer {
                node: node.parse()?,
                owner: wrapper.parse()?,
            }
            .encode_log_data(),
            block,
            0,
            0,
            registry,
        ),
        raw_at_transaction(
            super::NameWrapped {
                node: node.parse()?,
                name: b"\x12candidate-fallback\x03eth\0".to_vec().into(),
                owner,
                fuses: (1 << 16) | (1 << 17),
                expiry: initial_wrapper_expiry,
            }
            .encode_log_data(),
            block,
            0,
            1,
            wrapper,
        ),
        raw_at_transaction(
            ControllerAdded {
                controller: wrapper.parse()?,
            }
            .encode_log_data(),
            block + 1,
            0,
            0,
            registrar,
        ),
        base_renewed_at(
            addresses,
            U256::from_be_bytes(*labelhash),
            renewed_registrar_expiry,
            block + 1,
            1,
        ),
        raw_at_transaction(
            ControllerRemoved {
                controller: wrapper.parse()?,
            }
            .encode_log_data(),
            block + 1,
            0,
            2,
            registrar,
        ),
        raw_at_transaction(
            super::NameUnwrapped {
                node: node.parse()?,
                owner,
            }
            .encode_log_data(),
            unwrap_block,
            0,
            0,
            wrapper,
        ),
        raw_at_transaction(
            super::v1_registrar::Transfer {
                from: wrapper.parse()?,
                to: owner,
                tokenId: U256::from_be_bytes(*labelhash),
            }
            .encode_log_data(),
            unwrap_block,
            0,
            1,
            registrar,
        ),
    ];
    let mut input = batch(logs.clone(), &fixture, false);
    admit_v1_registry(&mut input, registry);
    let output = interpret_test_batch(input)?;
    let fallback = output
        .normalized_events
        .iter()
        .find(|event| {
            event.source_family == "ens_v1_registrar_l1"
                && event.event_kind == "TokenControlTransferred"
                && event.block_number == Some(unwrap_block)
        })
        .expect("ordered unwrap transfer materializes registrar authority");
    assert_eq!(fallback.after_state["fallback_from_wrapper"], true);
    assert_eq!(
        fallback.after_state["expiry"], renewed_registrar_expiry,
        "fallback must use the proven syncWrapper expiry without mutating ordinary wrapper state"
    );
    assert!(output.normalized_events.iter().any(|event| {
        event.source_family == "ens_v1_registrar_l1"
            && event.event_kind == "SurfaceBound"
            && event.block_number == Some(unwrap_block)
    }));

    let mut setup_input = batch(logs[..5].to_vec(), &fixture, false);
    admit_v1_registry(&mut setup_input, registry);
    let (setup_output, session) = interpret_test_batch_incremental(setup_input, None)?;
    let mut incremental_input = batch(logs[5..].to_vec(), &fixture, false);
    admit_v1_registry(&mut incremental_input, registry);
    let (incremental, _) = interpret_test_batch_incremental(incremental_input, Some(session))?;
    let mut cold_input = batch(logs[5..].to_vec(), &fixture, false);
    admit_v1_registry(&mut cold_input, registry);
    cold_input.prior_events = setup_output
        .normalized_events
        .iter()
        .map(super::prior_event)
        .collect();
    let cold = interpret_test_batch(cold_input)?;
    let full_unwrap_events = output
        .normalized_events
        .iter()
        .filter(|event| event.block_number == Some(unwrap_block))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        incremental.normalized_events, full_unwrap_events,
        "incremental replay must retain the proven syncWrapper expiry separately from wrapper state"
    );
    assert_eq!(
        cold.normalized_events, full_unwrap_events,
        "cold restore must retain the proven syncWrapper expiry separately from wrapper state"
    );
    Ok(())
}

#[test]
fn compacted_restore_retains_sync_wrapper_expiry_after_later_numeric_renewal() -> anyhow::Result<()>
{
    let fixture = fixture()?;
    let mut probe = wrapper_fallback_probe(&fixture, "shadowed-expiry")?;
    probe.setup_logs.extend([
        raw_at_transaction(
            ControllerAdded {
                controller: probe.wrapper.parse()?,
            }
            .encode_log_data(),
            probe.block + 1,
            0,
            0,
            &probe.registrar,
        ),
        base_renewed_at(
            &fixture["addresses"],
            U256::from_be_bytes(*probe.labelhash),
            1_248,
            probe.block + 1,
            1,
        ),
        raw_at_transaction(
            ControllerRemoved {
                controller: probe.wrapper.parse()?,
            }
            .encode_log_data(),
            probe.block + 1,
            0,
            2,
            &probe.registrar,
        ),
        base_renewed_at(
            &fixture["addresses"],
            U256::from_be_bytes(*probe.labelhash),
            2_248,
            probe.block + 2,
            0,
        ),
    ]);
    let later_numeric_renewal = probe
        .setup_logs
        .last()
        .cloned()
        .expect("later numeric renewal");

    let replay = replay_fallback_with_rank_one_restore(&fixture, probe)?;
    let control = interpret_test_batch(batch(vec![later_numeric_renewal], &fixture, false))?;
    let logical_name_id = replay
        .full_tail
        .iter()
        .find(|event| event.event_kind == "TokenControlTransferred")
        .and_then(|event| event.logical_name_id.as_deref())
        .expect("fallback transfer logical name");
    let full_renewals = replay
        .setup_output
        .normalized_events
        .iter()
        .filter(|event| {
            event.source_family == "ens_v2_migration_l1"
                && event.event_kind == "RegistrationRenewed"
                && event.logical_name_id.as_deref() == Some(logical_name_id)
        })
        .collect::<Vec<_>>();
    assert_eq!(full_renewals.len(), 2);
    let control_renewal = control
        .normalized_events
        .iter()
        .find(|event| {
            event.source_family == "ens_v2_migration_l1"
                && event.event_kind == "RegistrationRenewed"
        })
        .expect("control historical renewal");
    assert_eq!(
        full_renewals[1].migration_correlation_ids, control_renewal.migration_correlation_ids,
        "restore-only expiry must not change operation-local migration evidence identity"
    );
    assert_eq!(
        full_renewals[0].raw_fact_ref[seam::INTERPRETER_STATE_KEY],
        full_renewals[1].raw_fact_ref[seam::INTERPRETER_STATE_KEY],
        "the later numeric renewal must shadow the synchronized renewal under production compaction"
    );
    assert_eq!(
        replay
            .compacted_prior
            .iter()
            .filter(|event| {
                event.source_family == "ens_v2_migration_l1"
                    && event.event_kind == "RegistrationRenewed"
                    && event.logical_name_id.as_deref() == Some(logical_name_id)
            })
            .count(),
        1,
        "rank-1 restore must retain only the latest renewal row for this state key"
    );
    assert_fallback_replay_matches(&replay, 1_248);
    Ok(())
}

#[test]
fn completed_correlation_groups_retain_monotone_max_expiry_across_replay_modes()
-> anyhow::Result<()> {
    let fixture = fixture()?;
    let mut probe = wrapper_fallback_probe(&fixture, "monotone-expiry")?;
    probe.setup_logs.extend([
        raw_at_transaction(
            ControllerAdded {
                controller: probe.wrapper.parse()?,
            }
            .encode_log_data(),
            probe.block + 1,
            0,
            0,
            &probe.registrar,
        ),
        base_renewed_at(
            &fixture["addresses"],
            U256::from_be_bytes(*probe.labelhash),
            1_248,
            probe.block + 1,
            1,
        ),
        raw_at_transaction(
            ControllerRemoved {
                controller: probe.wrapper.parse()?,
            }
            .encode_log_data(),
            probe.block + 1,
            0,
            2,
            &probe.registrar,
        ),
        raw_at_transaction(
            ControllerAdded {
                controller: probe.wrapper.parse()?,
            }
            .encode_log_data(),
            probe.block + 2,
            0,
            0,
            &probe.registrar,
        ),
        base_renewed_at(
            &fixture["addresses"],
            U256::from_be_bytes(*probe.labelhash),
            848,
            probe.block + 2,
            1,
        ),
        raw_at_transaction(
            ControllerRemoved {
                controller: probe.wrapper.parse()?,
            }
            .encode_log_data(),
            probe.block + 2,
            0,
            2,
            &probe.registrar,
        ),
    ]);

    let replay = replay_fallback_with_rank_one_restore(&fixture, probe)?;
    assert_fallback_replay_matches(&replay, 1_248);
    Ok(())
}

#[test]
fn registrar_controller_envelope_cannot_cross_transaction_boundaries() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let mut probe = wrapper_fallback_probe(&fixture, "cross-transaction")?;
    probe.setup_logs.extend([
        raw_at_transaction(
            ControllerAdded {
                controller: probe.wrapper.parse()?,
            }
            .encode_log_data(),
            probe.block + 1,
            0,
            0,
            &probe.registrar,
        ),
        base_renewed_at(
            &fixture["addresses"],
            U256::from_be_bytes(*probe.labelhash),
            1_248,
            probe.block + 2,
            0,
        ),
        raw_at_transaction(
            ControllerRemoved {
                controller: probe.wrapper.parse()?,
            }
            .encode_log_data(),
            probe.block + 3,
            0,
            0,
            &probe.registrar,
        ),
    ]);

    let replay = replay_fallback_with_rank_one_restore(&fixture, probe)?;
    assert_fallback_replay_matches(&replay, 248);
    Ok(())
}

#[test]
fn registrar_controller_envelope_requires_matching_controller_removal() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let mut probe = wrapper_fallback_probe(&fixture, "matching-removal")?;
    probe.setup_logs.extend([
        raw_at_transaction(
            ControllerAdded {
                controller: probe.wrapper.parse()?,
            }
            .encode_log_data(),
            probe.block + 1,
            0,
            0,
            &probe.registrar,
        ),
        base_renewed_at(
            &fixture["addresses"],
            U256::from_be_bytes(*probe.labelhash),
            1_248,
            probe.block + 1,
            1,
        ),
        raw_at_transaction(
            ControllerRemoved {
                controller: Address::from([0x99; 20]),
            }
            .encode_log_data(),
            probe.block + 1,
            0,
            2,
            &probe.registrar,
        ),
    ]);

    let replay = replay_fallback_with_rank_one_restore(&fixture, probe)?;
    assert_fallback_replay_matches(&replay, 248);
    Ok(())
}

struct WrapperFallbackProbe {
    registrar: String,
    wrapper: String,
    registry: String,
    labelhash: B256,
    block: i64,
    unwrap_block: i64,
    setup_logs: Vec<RawLogInput>,
    tail_logs: Vec<RawLogInput>,
}

fn wrapper_fallback_probe(fixture: &Value, label: &str) -> anyhow::Result<WrapperFallbackProbe> {
    let addresses = &fixture["addresses"];
    let registrar = addresses["base_registrar"].as_str().unwrap().to_owned();
    let wrapper = addresses["name_wrapper"].as_str().unwrap().to_owned();
    let registry = "0x0000000000000000000000000000000000000099".to_owned();
    let owner = Address::from([0x51; 20]);
    let labelhash = keccak256(label.as_bytes());
    let node = super::common::namehash(&[label.to_owned(), "eth".to_owned()]);
    let block = fixture["scenarios"]["R-05"]["configuration_block"]
        .as_i64()
        .unwrap();
    let initial_registrar_expiry = 248_u64;
    let initial_wrapper_expiry = initial_registrar_expiry + (90 * 24 * 60 * 60);
    let unwrap_block = i64::try_from(initial_wrapper_expiry + 1)?;
    let mut dns_name = Vec::with_capacity(label.len() + 6);
    dns_name.push(u8::try_from(label.len())?);
    dns_name.extend_from_slice(label.as_bytes());
    dns_name.extend_from_slice(b"\x03eth\0");
    let setup_logs = vec![
        raw_at_transaction(
            super::v1_registry::Transfer {
                node: node.parse()?,
                owner: wrapper.parse()?,
            }
            .encode_log_data(),
            block,
            0,
            0,
            &registry,
        ),
        raw_at_transaction(
            super::NameWrapped {
                node: node.parse()?,
                name: dns_name.into(),
                owner,
                fuses: (1 << 16) | (1 << 17),
                expiry: initial_wrapper_expiry,
            }
            .encode_log_data(),
            block,
            0,
            1,
            &wrapper,
        ),
    ];
    let tail_logs = vec![
        raw_at_transaction(
            super::NameUnwrapped {
                node: node.parse()?,
                owner,
            }
            .encode_log_data(),
            unwrap_block,
            0,
            0,
            &wrapper,
        ),
        raw_at_transaction(
            super::v1_registrar::Transfer {
                from: wrapper.parse()?,
                to: owner,
                tokenId: U256::from_be_bytes(*labelhash),
            }
            .encode_log_data(),
            unwrap_block,
            0,
            1,
            &registrar,
        ),
    ];
    Ok(WrapperFallbackProbe {
        registrar,
        wrapper,
        registry,
        labelhash,
        block,
        unwrap_block,
        setup_logs,
        tail_logs,
    })
}

struct CompactedFallbackReplay {
    setup_output: BatchOutput,
    compacted_prior: Vec<PriorEventInput>,
    full_tail: Vec<NormalizedEvent>,
    incremental_output: BatchOutput,
    cold_output: BatchOutput,
}

fn replay_fallback_with_rank_one_restore(
    fixture: &Value,
    probe: WrapperFallbackProbe,
) -> anyhow::Result<CompactedFallbackReplay> {
    let mut full_input = batch(
        probe
            .setup_logs
            .iter()
            .chain(&probe.tail_logs)
            .cloned()
            .collect(),
        fixture,
        false,
    );
    admit_v1_registry(&mut full_input, &probe.registry);
    let full = interpret_test_batch(full_input)?;

    let mut setup_input = batch(probe.setup_logs.clone(), fixture, false);
    admit_v1_registry(&mut setup_input, &probe.registry);
    let (setup_output, session) = interpret_test_batch_incremental(setup_input, None)?;
    let mut blocks = probe
        .setup_logs
        .iter()
        .map(|raw| RawBlockInput {
            chain_id: raw.chain_id.clone(),
            block_hash: raw.block_hash.clone(),
            block_number: raw.block_number,
            block_timestamp: raw.block_timestamp,
            canonicality_state: raw.canonicality_state.clone(),
        })
        .collect::<Vec<_>>();
    blocks.dedup_by(|left, right| {
        left.block_number == right.block_number && left.block_hash == right.block_hash
    });
    let compacted_prior =
        seam::fold_prior_events(Vec::new(), &setup_output.normalized_events, &blocks)?;

    let tail_logs = probe.tail_logs;
    let mut cold_input = batch(tail_logs.clone(), fixture, false);
    admit_v1_registry(&mut cold_input, &probe.registry);
    cold_input.prior_events = compacted_prior.clone();
    let cold_output = interpret_test_batch(cold_input)?;
    let mut incremental_input = batch(tail_logs, fixture, false);
    admit_v1_registry(&mut incremental_input, &probe.registry);
    let (incremental_output, _) =
        interpret_test_batch_incremental(incremental_input, Some(session))?;
    let full_tail = full
        .normalized_events
        .into_iter()
        .filter(|event| event.block_number == Some(probe.unwrap_block))
        .collect();
    Ok(CompactedFallbackReplay {
        setup_output,
        compacted_prior,
        full_tail,
        incremental_output,
        cold_output,
    })
}

fn assert_fallback_replay_matches(replay: &CompactedFallbackReplay, expected_expiry: u64) {
    fn fallback(events: &[NormalizedEvent]) -> &NormalizedEvent {
        events
            .iter()
            .find(|event| {
                event.source_family == "ens_v1_registrar_l1"
                    && event.event_kind == "TokenControlTransferred"
            })
            .expect("ordered unwrap transfer materializes registrar authority")
    }
    let full = fallback(&replay.full_tail);
    let cold = fallback(&replay.cold_output.normalized_events);
    assert_eq!(
        cold.after_state, full.after_state,
        "rank-1-compacted cold restore must match full replay"
    );
    assert_eq!(full.after_state["expiry"], expected_expiry);
    assert_eq!(
        replay.cold_output.normalized_events, replay.full_tail,
        "rank-1-compacted cold restore must reproduce the full unwrap output"
    );
    assert_eq!(
        replay.incremental_output.normalized_events, replay.full_tail,
        "incremental replay must reproduce the full unwrap output"
    );
}

fn admit_v1_registry(input: &mut BatchInput, address: &str) {
    input.manifests.push(manifest_with_events(
        V1_REGISTRY_MANIFEST_ID,
        "ens",
        "ens_v1_registry_l1",
        &[(
            "Transfer",
            "event Transfer(bytes32 indexed node, address owner)",
            &["registry"],
            &["AuthorityTransferred", "PermissionChanged"],
        )],
    ));
    input.admissions.push(admission_at(
        V1_REGISTRY_MANIFEST_ID,
        "registry",
        address,
        102,
    ));
}

#[test]
fn cross_family_registrar_correlation_keeps_the_graveyard_launch_floor() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["U-01"];
    let addresses = &fixture["addresses"];
    let block = scenario["migration_block"].as_i64().unwrap();
    let mut input = batch(
        unlocked_name_logs(scenario, addresses, block, 1, false)?,
        &fixture,
        true,
    );
    input
        .admissions
        .iter_mut()
        .find(|admission| admission.role.as_deref() == Some("graveyard"))
        .expect("migration Graveyard admission")
        .active_from_block = Some(block + 1);

    let output = interpret_test_batch(input)?;
    assert!(
        output
            .normalized_events
            .iter()
            .all(|event| event.event_kind != "MigrationApplied"),
        "pre-launch BaseRegistrar observations must not derive a migration boundary"
    );
    assert!(output.normalized_events.iter().all(|event| {
        event.source_family != "ens_v2_migration_l1"
            || event.raw_fact_ref["emitting_address"] != addresses["base_registrar"]
    }));
    Ok(())
}

#[test]
fn controller_add_remove_share_state_and_restore_through_compaction() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["R-05"];
    let addresses = &fixture["addresses"];
    let controller = scenario["controller"]
        .as_str()
        .unwrap()
        .parse::<Address>()?;
    let block = scenario["configuration_block"].as_i64().unwrap();
    let input = batch(
        vec![
            raw_at_transaction(
                ControllerAdded { controller }.encode_log_data(),
                block,
                0,
                scenario["add_log_index"].as_i64().unwrap(),
                addresses["base_registrar"].as_str().unwrap(),
            ),
            raw_at_transaction(
                ControllerRemoved { controller }.encode_log_data(),
                block,
                0,
                scenario["remove_log_index"].as_i64().unwrap(),
                addresses["base_registrar"].as_str().unwrap(),
            ),
        ],
        &fixture,
        false,
    );
    let output = interpret_test_batch(input)?;
    let events = output
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "PermissionChanged")
        .collect::<Vec<_>>();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].before_state, json!({}));
    assert_eq!(events[0].after_state["approved"], true);
    assert_eq!(events[1].before_state["approved"], true);
    assert_eq!(events[1].after_state["approved"], false);
    assert_eq!(
        events[0].raw_fact_ref["interpreter_state_key"],
        events[1].raw_fact_ref["interpreter_state_key"]
    );
    Ok(())
}

#[test]
fn cross_family_registrar_cleanup_and_historical_renewal_reject_lookalikes() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let cleanup = &fixture["scenarios"]["G-02"];
    let renewal = &fixture["scenarios"]["R-01"];
    let addresses = &fixture["addresses"];
    let output = interpret_test_batch(batch(
        vec![
            raw_at_transaction(
                with_topic0(
                    BaseNameRegistered {
                        id: decimal_u256(&cleanup["token_id"])?,
                        owner: address(addresses, "graveyard")?,
                        expires: decimal_u256(&cleanup["cleanup_expiry"])?,
                    }
                    .encode_log_data(),
                    keccak256(b"NameRegistered(uint256,address,uint256)"),
                ),
                cleanup["cleanup_block"].as_i64().unwrap(),
                0,
                cleanup["name_registered_log_index"].as_i64().unwrap(),
                addresses["base_registrar"].as_str().unwrap(),
            ),
            raw_at_transaction(
                with_topic0(
                    BaseNameRenewed {
                        id: decimal_u256(&renewal["base_token_id"])?,
                        expires: U256::from(renewal["v1_expiry"].as_u64().unwrap()),
                    }
                    .encode_log_data(),
                    keccak256(b"NameRenewed(uint256,uint256)"),
                ),
                cleanup["cleanup_block"].as_i64().unwrap(),
                1,
                0,
                addresses["base_registrar"].as_str().unwrap(),
            ),
        ],
        &fixture,
        false,
    ))?;
    assert!(
        !output
            .normalized_events
            .iter()
            .any(|event| event.event_kind == "MigrationApplied")
    );
    assert!(
        output.normalized_events.iter().any(|event| {
            event.event_kind == "RegistrationReleased"
                && event.consumer_visibility == "candidate"
                && event.after_state["owner"] == addresses["graveyard"]
                && event.after_state["lifecycle_classification"] == "graveyard_cleanup"
                && event.after_state["authority_effect"] == "none"
        }),
        "events: {:#?}",
        output.normalized_events
    );
    let historical_renewal = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationRenewed")
        .expect("unmatched launch-bounded renewal remains historical evidence");
    assert_eq!(
        historical_renewal.after_state["lifecycle_classification"],
        "historical_renewal"
    );
    assert_eq!(historical_renewal.after_state["authority_effect"], "none");
    assert!(historical_renewal.resource_id.is_none());
    assert!(!output.normalized_events.iter().any(|event| {
        matches!(
            event.event_kind.as_str(),
            "RegistrationGranted" | "RegistrarNameRegistered" | "AuthorityTransferred"
        )
    }));
    assert!(output.resources.is_empty());
    assert!(output.token_lineages.is_empty());
    assert!(output.surface_bindings.is_empty());

    let ordinary_expiry = interpret_test_batch(batch(
        vec![raw_at_transaction(
            with_topic0(
                BaseNameRegistered {
                    id: decimal_u256(&cleanup["token_id"])? + U256::from(1),
                    owner: address(addresses, "graveyard")?,
                    expires: U256::from(2_000_000_000_u64),
                }
                .encode_log_data(),
                keccak256(b"NameRegistered(uint256,address,uint256)"),
            ),
            cleanup["cleanup_block"].as_i64().unwrap(),
            0,
            0,
            addresses["base_registrar"].as_str().unwrap(),
        )],
        &fixture,
        false,
    ))?;
    assert!(
        ordinary_expiry.normalized_events.is_empty(),
        "Graveyard ownership without the terminal expiry class is not cleanup evidence"
    );

    let terminal_lookalike = interpret_test_batch(batch(
        vec![raw_at_transaction(
            with_topic0(
                BaseNameRegistered {
                    id: decimal_u256(&cleanup["token_id"])? + U256::from(3),
                    owner: address(addresses, "graveyard")?,
                    expires: decimal_u256(&cleanup["cleanup_expiry"])? + U256::from(1),
                }
                .encode_log_data(),
                keccak256(b"NameRegistered(uint256,address,uint256)"),
            ),
            cleanup["cleanup_block"].as_i64().unwrap(),
            0,
            0,
            addresses["base_registrar"].as_str().unwrap(),
        )],
        &fixture,
        false,
    ))?;
    assert!(
        terminal_lookalike.normalized_events.is_empty(),
        "a controller registration to the Graveyard with a different high expiry is not cleanup"
    );

    let fork_deployer = Address::from([0x99; 20]);
    let fork_rehearsal = interpret_test_batch(batch(
        vec![
            raw_at_transaction(
                ControllerAdded {
                    controller: fork_deployer,
                }
                .encode_log_data(),
                cleanup["cleanup_block"].as_i64().unwrap(),
                0,
                0,
                addresses["base_registrar"].as_str().unwrap(),
            ),
            raw_at_transaction(
                with_topic0(
                    BaseNameRegistered {
                        id: decimal_u256(&cleanup["token_id"])? + U256::from(2),
                        owner: Address::from([0x98; 20]),
                        expires: U256::from(2_000_000_000_u64),
                    }
                    .encode_log_data(),
                    keccak256(b"NameRegistered(uint256,address,uint256)"),
                ),
                cleanup["cleanup_block"].as_i64().unwrap(),
                1,
                0,
                addresses["base_registrar"].as_str().unwrap(),
            ),
        ],
        &fixture,
        false,
    ))?;
    assert!(fork_rehearsal.normalized_events.iter().all(|event| {
        !matches!(
            event.event_kind.as_str(),
            "RegistrationGranted" | "RegistrarNameRegistered" | "RegistrationRenewed"
        )
    }));
    Ok(())
}

#[test]
fn locked_migration_does_not_require_a_registrar_admission() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["L-01"];
    let addresses = &fixture["addresses"];
    let block = scenario["migration_block"].as_i64().unwrap();
    let token = decimal_u256(&scenario["v2_token_id"])?;
    let mut input = batch(
        vec![
            registry_created_raw(scenario, addresses),
            factory_raw(scenario, addresses)?,
            raw_at_transaction(
                super::v2_registry::LabelRegistered {
                    tokenId: token,
                    labelHash: scenario["labelhash"].as_str().unwrap().parse()?,
                    label: scenario["label"].as_str().unwrap().to_owned(),
                    owner: Address::from([0x11; 20]),
                    expiry: scenario["stored_expiry"].as_u64().unwrap(),
                    sender: address(addresses, "locked_controller")?,
                }
                .encode_log_data(),
                block,
                0,
                scenario["v2_registration_log_index"].as_i64().unwrap(),
                addresses["eth_registry"].as_str().unwrap(),
            ),
            raw_at_transaction(
                super::v2_registry::TokenResource {
                    tokenId: token,
                    resource: token,
                }
                .encode_log_data(),
                block,
                0,
                scenario["v2_registration_log_index"].as_i64().unwrap() + 2,
                addresses["eth_registry"].as_str().unwrap(),
            ),
        ],
        &fixture,
        false,
    );
    input
        .manifests
        .retain(|manifest| manifest.source_family != "ens_v1_registrar_l1");
    input
        .admissions
        .retain(|admission| admission.source_manifest_id != Some(V1_REGISTRAR_MANIFEST_ID));

    let output = interpret_test_batch(input)?;
    let boundary = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "MigrationApplied")
        .expect("locked migration boundary without registrar admission");
    assert_eq!(boundary.after_state["migration_path"], "locked_wrapped");
    assert_eq!(
        boundary.after_state["predecessor_binding"]["resource"]["anchor_kind"],
        "wrapper_backed_control"
    );
    Ok(())
}

#[test]
fn migration_registry_association_preserves_the_ordinary_announcement_edge() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["L-01"];
    let addresses = &fixture["addresses"];
    let block = scenario["migration_block"].as_i64().unwrap();
    let locked = address(addresses, "locked_controller")?;
    let v2_token = decimal_u256(&scenario["v2_token_id"])?;
    let registry_created = registry_created_raw(scenario, addresses);
    let factory = factory_raw(scenario, addresses)?;
    let registration = raw_at_transaction(
        super::v2_registry::LabelRegistered {
            tokenId: v2_token,
            labelHash: scenario["labelhash"].as_str().unwrap().parse()?,
            label: scenario["label"].as_str().unwrap().to_owned(),
            owner: Address::from([0x11; 20]),
            expiry: scenario["stored_expiry"].as_u64().unwrap(),
            sender: locked,
        }
        .encode_log_data(),
        block,
        0,
        scenario["v2_registration_log_index"].as_i64().unwrap(),
        addresses["eth_registry"].as_str().unwrap(),
    );
    let token_resource = raw_at_transaction(
        super::v2_registry::TokenResource {
            tokenId: v2_token,
            resource: v2_token,
        }
        .encode_log_data(),
        block,
        0,
        scenario["v2_registration_log_index"].as_i64().unwrap() + 2,
        addresses["eth_registry"].as_str().unwrap(),
    );
    let mut output = interpret_test_batch(batch(
        vec![
            registry_created.clone(),
            factory.clone(),
            registration.clone(),
            token_resource.clone(),
        ],
        &fixture,
        false,
    ))?;
    assert_eq!(output.discovery_edges.len(), 1);
    assert_eq!(output.discovery_edges[0].edge_kind, "registry_announcement");
    let registry_event = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistryCreated")
        .expect("ordinary registry announcement");
    assert_eq!(registry_event.source_family, "ens_v2_registry_l1");
    assert_eq!(registry_event.consumer_visibility, "activated");
    assert!(registry_event.migration_correlation_ids.is_empty());
    assert_eq!(output.migration_discovery_associations.len(), 1);
    let association = &output.migration_discovery_associations[0];
    assert_eq!(association.consumer_visibility, "candidate");
    assert!(association.logical_edge_identity.starts_with("0x"));
    assert_eq!(association.logical_edge_identity.len(), 66);
    assert!(output.migration_event_associations.iter().any(|candidate| {
        candidate.event_identity == registry_event.event_identity
            && candidate.migration_correlation_id == association.migration_correlation_id
    }));
    let boundary = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "MigrationApplied")
        .expect("locked-wrapped authority boundary");
    assert_eq!(boundary.after_state["migration_path"], "locked_wrapped");
    assert_eq!(
        boundary.after_state["predecessor_binding"]["resource"]["anchor_kind"],
        "wrapper_backed_control"
    );
    assert_eq!(
        boundary.after_state["predecessor_binding"]["resource"]["contract_address"],
        addresses["name_wrapper"]
    );
    assert_eq!(
        boundary.after_state["stored_expiry"],
        scenario["stored_expiry"]
    );
    assert!(output.normalized_events.iter().any(|event| {
        event.event_kind == "ContractDiscovered" && event.consumer_visibility == "candidate"
    }));

    let later = raw_at_transaction(
        super::v2_registry::ParentUpdated {
            parent: address(addresses, "eth_registry")?,
            label: scenario["label"].as_str().unwrap().to_owned(),
            sender: locked,
        }
        .encode_log_data(),
        block + 1,
        0,
        0,
        addresses["locked_registry"].as_str().unwrap(),
    );
    let mut restored = batch(vec![later.clone()], &fixture, false);
    restored.admissions.push(AddressAdmissionInput {
        address: addresses["locked_registry"].as_str().unwrap().to_owned(),
        contract_instance_id: output.discovery_edges[0].to_contract_instance_id,
        source_manifest_id: Some(REGISTRY_MANIFEST_ID),
        role: None,
        discovery_edge_kind: Some("registry_announcement".to_owned()),
        discovery_from_contract_instance_id: Some(
            output.discovery_edges[0].from_contract_instance_id,
        ),
        discovery_observation_key: Some(output.discovery_edges[0].observation_key.clone()),
        active_from_block: Some(block),
        active_to_block: None,
    });
    restored.admissions.push(AddressAdmissionInput {
        address: addresses["locked_registry"].as_str().unwrap().to_owned(),
        contract_instance_id: output.discovery_edges[0].to_contract_instance_id,
        source_manifest_id: Some(REGISTRY_MANIFEST_ID),
        role: None,
        discovery_edge_kind: Some("migration_registry_creation".to_owned()),
        discovery_from_contract_instance_id: Some(
            output.discovery_edges[0].from_contract_instance_id,
        ),
        discovery_observation_key: Some(
            json!({
                "id":association.migration_correlation_id,
                "evidence":association.evidence_refs,
            })
            .to_string(),
        ),
        active_from_block: Some(block),
        active_to_block: None,
    });
    let live_follow = interpret_test_batch(restored)?;
    let live_parent = live_follow
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "ParentChanged")
        .expect("later proxy fact is interpreted after restart");
    assert_eq!(live_parent.consumer_visibility, "activated");
    let live_candidate = live_follow
        .migration_event_associations
        .iter()
        .find(|candidate| candidate.event_identity == live_parent.event_identity)
        .expect("later ordinary fact carries candidate augmentation");
    assert_eq!(
        live_candidate.migration_correlation_id,
        association.migration_correlation_id
    );

    let full = interpret_test_batch(batch(
        vec![
            registry_created.clone(),
            factory,
            registration.clone(),
            token_resource.clone(),
            later.clone(),
        ],
        &fixture,
        false,
    ))?;
    let full_parent = full
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "ParentChanged")
        .expect("full replay later proxy fact");
    assert_eq!(live_parent, full_parent);
    let full_candidate = full
        .migration_event_associations
        .iter()
        .find(|candidate| candidate.event_identity == full_parent.event_identity)
        .expect("full replay candidate augmentation");
    assert_eq!(live_candidate, full_candidate);
    assert_eq!(
        output
            .normalized_events
            .iter()
            .chain(&live_follow.normalized_events)
            .cloned()
            .collect::<Vec<_>>(),
        full.normalized_events,
        "incremental normalized events differ from a clean replay"
    );
    assert_eq!(
        output
            .surface_bindings
            .iter()
            .chain(&live_follow.surface_bindings)
            .cloned()
            .collect::<Vec<_>>(),
        full.surface_bindings,
        "incremental binding ranges or canonicality differ from a clean replay"
    );
    assert_eq!(
        output
            .binding_closures
            .iter()
            .chain(&live_follow.binding_closures)
            .cloned()
            .collect::<Vec<_>>(),
        full.binding_closures,
        "incremental binding closes differ from a clean replay"
    );
    assert_eq!(
        output
            .migration_candidate_identity_effects
            .iter()
            .chain(&live_follow.migration_candidate_identity_effects)
            .cloned()
            .collect::<Vec<_>>(),
        full.migration_candidate_identity_effects,
        "incremental candidate identity effects differ from a clean replay"
    );
    let mut incremental_associations = std::collections::BTreeMap::new();
    for association in output
        .migration_event_associations
        .iter()
        .chain(&live_follow.migration_event_associations)
    {
        let key = (
            association.event_identity.clone(),
            association.migration_correlation_id.clone(),
        );
        if let Some(existing) = incremental_associations.insert(key, association.clone()) {
            assert_eq!(
                existing, *association,
                "incremental replay produced conflicting migration association evidence"
            );
        }
    }
    let full_associations = full
        .migration_event_associations
        .iter()
        .map(|association| {
            (
                (
                    association.event_identity.clone(),
                    association.migration_correlation_id.clone(),
                ),
                association.clone(),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(
        incremental_associations, full_associations,
        "incremental migration associations differ from a clean replay"
    );

    let control = interpret_test_batch(registry_only_batch(
        vec![registry_created, registration, token_resource, later],
        &fixture,
    ))?;
    assert_eq!(activated_events(&full), control.normalized_events);
    assert_eq!(full.name_surfaces, control.name_surfaces);
    assert_eq!(full.resources, control.resources);
    assert_eq!(full.surface_bindings, control.surface_bindings);
    assert_eq!(full.binding_closures, control.binding_closures);
    assert_eq!(full.discovery_edges, control.discovery_edges);
    assert_activated_transition(&mut output, "locked_wrapped")?;
    Ok(())
}

fn activated_events(output: &BatchOutput) -> Vec<NormalizedEvent> {
    output
        .normalized_events
        .iter()
        .filter(|event| event.consumer_visibility == "activated")
        .cloned()
        .collect()
}

fn assert_activated_transition(output: &mut BatchOutput, path: &str) -> anyhow::Result<()> {
    assert!(
        output.migration_authority_transitions.is_empty(),
        "candidate interpretation must not schedule a binding write"
    );
    super::super::migration::inject_activated_transition_for_test(output)?;
    let transition = output
        .migration_authority_transitions
        .iter()
        .find(|transition| {
            output.normalized_events.iter().any(|event| {
                event.event_identity == transition.boundary_event_identity
                    && event.after_state["migration_path"] == path
            })
        })
        .expect("test-only activated authority transition");
    assert!(output.normalized_events.iter().any(|event| {
        event.event_identity == transition.boundary_event_identity
            && event.consumer_visibility == "activated"
    }));
    assert_eq!(transition.expected_predecessor_arm, "ens_v1");
    assert_eq!(transition.successor_arm, "ens_v2");
    assert_eq!(
        transition.predecessor_selector["selection"],
        if path == "unwrapped" {
            "active_immediately_before_predecessor_cleanup"
        } else {
            "active_immediately_before_boundary"
        }
    );
    let successor = output
        .surface_bindings
        .iter()
        .find(|binding| binding.surface_binding_id == transition.successor_surface_binding_id)
        .expect("transition names its concrete successor binding");
    assert_eq!(successor.block_number, transition.block_number);
    assert_eq!(
        successor.provenance[seam::TRANSACTION_INDEX_KEY],
        transition.transaction_index
    );
    assert!(
        successor.provenance[seam::LOG_INDEX_KEY]
            .as_i64()
            .is_some_and(|log| log >= transition.log_index)
    );
    Ok(())
}

fn registry_created_raw(scenario: &Value, addresses: &Value) -> RawLogInput {
    raw_at_transaction(
        super::RegistryCreated {}.encode_log_data(),
        scenario["migration_block"].as_i64().unwrap(),
        0,
        scenario["registry_created_log_index"].as_i64().unwrap(),
        addresses["locked_registry"].as_str().unwrap(),
    )
}

fn factory_raw(scenario: &Value, addresses: &Value) -> anyhow::Result<RawLogInput> {
    Ok(raw_at_transaction(
        ProxyDeployed {
            sender: address(addresses, "locked_controller")?,
            proxyAddress: address(addresses, "locked_registry")?,
            salt: decimal_u256(&scenario["factory_salt"])?,
            implementation: Address::from([0x44; 20]),
        }
        .encode_log_data(),
        scenario["migration_block"].as_i64().unwrap(),
        0,
        scenario["factory_log_index"].as_i64().unwrap(),
        addresses["factory"].as_str().unwrap(),
    ))
}

fn fixture() -> anyhow::Result<Value> {
    Ok(serde_json::from_str(include_str!(
        "../../../tests/fixtures/interpreters/migration-catalog.json"
    ))?)
}

fn decimal_u256(value: &Value) -> anyhow::Result<U256> {
    Ok(U256::from_str(
        value.as_str().expect("fixture integer is a decimal string"),
    )?)
}

fn address(addresses: &Value, key: &str) -> anyhow::Result<Address> {
    Ok(addresses[key].as_str().expect("fixture address").parse()?)
}

fn unlocked_name_logs(
    scenario: &Value,
    addresses: &Value,
    block: i64,
    first_log: i64,
    wrapped: bool,
) -> anyhow::Result<Vec<RawLogInput>> {
    let controller = address(addresses, "unlocked_controller")?;
    let from = if wrapped {
        address(addresses, "name_wrapper")?
    } else {
        controller
    };
    let token = decimal_u256(&scenario["v2_token_id"])?;
    Ok(vec![
        raw_at_transaction(
            super::v1_registrar::Transfer {
                from,
                to: address(addresses, "graveyard")?,
                tokenId: decimal_u256(&scenario["base_token_id"])?,
            }
            .encode_log_data(),
            block,
            0,
            first_log,
            addresses["base_registrar"].as_str().unwrap(),
        ),
        raw_at_transaction(
            super::v2_registry::LabelRegistered {
                tokenId: token,
                labelHash: keccak256(scenario["label"].as_str().unwrap().as_bytes()),
                label: scenario["label"].as_str().unwrap().to_owned(),
                owner: Address::from([0x11; 20]),
                expiry: scenario["stored_expiry"].as_u64().unwrap(),
                sender: controller,
            }
            .encode_log_data(),
            block,
            0,
            first_log + 1,
            addresses["eth_registry"].as_str().unwrap(),
        ),
        raw_at_transaction(
            super::v2_registry::TokenResource {
                tokenId: token,
                resource: token,
            }
            .encode_log_data(),
            block,
            0,
            first_log + 2,
            addresses["eth_registry"].as_str().unwrap(),
        ),
    ])
}

fn batch(raw_logs: Vec<RawLogInput>, fixture: &Value, include_registry_setup: bool) -> BatchInput {
    let addresses = &fixture["addresses"];
    let mut manifests = vec![
        migration_manifest(),
        wrapper_manifest(),
        registrar_manifest(),
    ];
    let mut admissions = migration_admissions(addresses);
    admissions.push(admission_at(
        V1_REGISTRAR_MANIFEST_ID,
        "registrar",
        addresses["base_registrar"].as_str().unwrap(),
        6,
    ));
    admissions.push(admission_at(
        V1_WRAPPER_MANIFEST_ID,
        "name_wrapper",
        addresses["name_wrapper"].as_str().unwrap(),
        7,
    ));
    let mut discovery_rules = Vec::new();
    if include_registry_setup
        || raw_logs.iter().any(|raw| {
            raw.emitting_address
                .eq_ignore_ascii_case(addresses["locked_registry"].as_str().unwrap())
        })
    {
        manifests.push(registry_manifest());
        admissions.push(admission_at(
            REGISTRY_MANIFEST_ID,
            "registry",
            addresses["eth_registry"].as_str().unwrap(),
            101,
        ));
        discovery_rules.push(DiscoveryRuleInput {
            manifest_id: REGISTRY_MANIFEST_ID,
            edge_kind: "registry_announcement".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "reachable_from_root".to_owned(),
        });
        discovery_rules.push(DiscoveryRuleInput {
            manifest_id: REGISTRY_MANIFEST_ID,
            edge_kind: "resolver".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "protocol_event".to_owned(),
        });
        discovery_rules.push(DiscoveryRuleInput {
            manifest_id: REGISTRY_MANIFEST_ID,
            edge_kind: "subregistry".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "reachable_from_root".to_owned(),
        });
    }
    BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests,
        discovery_rules,
        admissions,
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs,
    }
}

fn registry_only_batch(raw_logs: Vec<RawLogInput>, fixture: &Value) -> BatchInput {
    let addresses = &fixture["addresses"];
    BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![registry_manifest()],
        discovery_rules: vec![
            DiscoveryRuleInput {
                manifest_id: REGISTRY_MANIFEST_ID,
                edge_kind: "registry_announcement".to_owned(),
                from_role: Some("registry".to_owned()),
                admission: "reachable_from_root".to_owned(),
            },
            DiscoveryRuleInput {
                manifest_id: REGISTRY_MANIFEST_ID,
                edge_kind: "resolver".to_owned(),
                from_role: Some("registry".to_owned()),
                admission: "protocol_event".to_owned(),
            },
        ],
        admissions: vec![admission_at(
            REGISTRY_MANIFEST_ID,
            "registry",
            addresses["eth_registry"].as_str().unwrap(),
            101,
        )],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs,
    }
}

fn migration_manifest() -> ManifestInput {
    let mut manifest = manifest_with_events(
        MIGRATION_MANIFEST_ID,
        "ens",
        "ens_v2_migration_l1",
        &[
            (
                "ProxyDeployed",
                "event ProxyDeployed(address indexed sender, address indexed proxyAddress, uint256 salt, address implementation)",
                &["verifiable_factory"],
                &["ContractDiscovered"],
            ),
            (
                "NameRenewed",
                "event NameRenewed(uint256 indexed tokenId, string label, uint64 duration, uint64 newExpiry, address paymentToken, bytes32 indexed referrer, uint256 amount)",
                &["ens_v1_renewal_bridge"],
                &["RegistrationRenewed", "PreimageObserved"],
            ),
        ],
    );
    let mut payload =
        serde_json::from_str::<Value>(&manifest.payload_json).expect("fixture manifest payload");
    payload["correlation_addresses"] = json!({
        "ens_v1_name_wrapper": fixture()
            .expect("migration fixture")
            ["addresses"]["name_wrapper"]
            .as_str()
            .expect("fixture NameWrapper address"),
        "ens_v1_base_registrar": fixture()
            .expect("migration fixture")
            ["addresses"]["base_registrar"]
            .as_str()
            .expect("fixture BaseRegistrar address"),
    });
    manifest.payload_json = payload.to_string();
    manifest
}

fn registrar_manifest() -> ManifestInput {
    manifest_with_events(
        V1_REGISTRAR_MANIFEST_ID,
        "ens",
        "ens_v1_registrar_l1",
        &[
            (
                "ControllerAdded",
                "event ControllerAdded(address indexed controller)",
                &["registrar"],
                &["PermissionChanged"],
            ),
            (
                "ControllerRemoved",
                "event ControllerRemoved(address indexed controller)",
                &["registrar"],
                &["PermissionChanged"],
            ),
            (
                "NameRegistered",
                "event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires)",
                &["registrar"],
                &["RegistrationReleased"],
            ),
            (
                "NameRenewed",
                "event NameRenewed(uint256 indexed id, uint256 expires)",
                &["registrar"],
                &["RegistrationRenewed", "ExpiryChanged"],
            ),
            (
                "Transfer",
                "event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)",
                &["registrar"],
                &[
                    "TokenControlTransferred",
                    "PermissionChanged",
                    "SurfaceUnbound",
                    "SurfaceBound",
                    "AuthorityEpochChanged",
                    "ResolverChanged",
                ],
            ),
        ],
    )
}

fn registry_manifest() -> ManifestInput {
    manifest_with_events(
        REGISTRY_MANIFEST_ID,
        "ens",
        "ens_v2_registry_l1",
        &[
            (
                "RegistryCreated",
                "event RegistryCreated()",
                &[],
                &["RegistryCreated"],
            ),
            (
                "LabelRegistered",
                "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
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
                "TokenResource",
                "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)",
                &["registry"],
                &["TokenResourceLinked"],
            ),
            (
                "TokenRegenerated",
                "event TokenRegenerated(uint256 indexed oldTokenId, uint256 indexed newTokenId)",
                &["registry"],
                &["TokenRegenerated"],
            ),
            (
                "TransferSingle",
                "event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)",
                &["registry"],
                &["TokenControlTransferred"],
            ),
            (
                "ExpiryUpdated",
                "event ExpiryUpdated(uint256 indexed tokenId, uint64 indexed newExpiry, address indexed sender)",
                &["registry"],
                &["ExpiryChanged", "RegistrationRenewed"],
            ),
            (
                "ResolverUpdated",
                "event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender)",
                &["registry"],
                &["ResolverChanged"],
            ),
            (
                "SubregistryUpdated",
                "event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender)",
                &["registry"],
                &["SubregistryChanged"],
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

/// The ENSv1 NameWrapper the ENSv1→ENSv2 migration manifest names as a correlation address. A
/// child's predecessor cleanup is emitted here, so a batch carrying that child's ENSv1→ENSv2
/// migration carries it.
fn wrapper_manifest() -> ManifestInput {
    manifest_with_events(
        V1_WRAPPER_MANIFEST_ID,
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
                    "SurfaceBound",
                    "AuthorityEpochChanged",
                    "PreimageObserved",
                ],
            ),
            (
                "TransferSingle",
                "event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)",
                &["name_wrapper"],
                &["TokenControlTransferred", "PermissionScopeChanged"],
            ),
            (
                "NameUnwrapped",
                "event NameUnwrapped(bytes32 indexed node, address owner)",
                &["name_wrapper"],
                &["SurfaceUnbound", "AuthorityEpochChanged"],
            ),
            (
                "ExpiryExtended",
                "event ExpiryExtended(bytes32 indexed node, uint64 expiry)",
                &["name_wrapper"],
                &["ExpiryChanged"],
            ),
        ],
    )
}

fn migration_admissions(addresses: &Value) -> Vec<AddressAdmissionInput> {
    [
        (
            "unlocked_migration_controller",
            "unlocked_controller",
            1_u128,
        ),
        ("locked_migration_controller", "locked_controller", 2),
        ("graveyard", "graveyard", 3),
        ("ens_v1_renewal_bridge", "renewal_bridge", 4),
        ("verifiable_factory", "factory", 5),
    ]
    .into_iter()
    .map(|(role, key, instance)| {
        admission_at(
            MIGRATION_MANIFEST_ID,
            role,
            addresses[key].as_str().unwrap(),
            instance,
        )
    })
    .collect()
}

fn admission_at(
    manifest_id: i64,
    role: &str,
    emitting_address: &str,
    instance: u128,
) -> AddressAdmissionInput {
    AddressAdmissionInput {
        address: emitting_address.to_owned(),
        contract_instance_id: Uuid::from_u128(instance),
        source_manifest_id: Some(manifest_id),
        role: Some(role.to_owned()),
        discovery_edge_kind: None,
        discovery_from_contract_instance_id: None,
        discovery_observation_key: None,
        active_from_block: Some(0),
        active_to_block: None,
    }
}

#[test]
fn graveyard_class_expiry_with_foreign_owner_is_not_cleanup_evidence() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let cleanup = &fixture["scenarios"]["G-02"];
    let addresses = &fixture["addresses"];
    let output = interpret_test_batch(batch(
        vec![raw_at_transaction(
            with_topic0(
                BaseNameRegistered {
                    id: decimal_u256(&cleanup["token_id"])? + U256::from(4),
                    owner: Address::from([0x66; 20]),
                    expires: decimal_u256(&cleanup["cleanup_expiry"])?,
                }
                .encode_log_data(),
                keccak256(b"NameRegistered(uint256,address,uint256)"),
            ),
            cleanup["cleanup_block"].as_i64().unwrap(),
            0,
            0,
            addresses["base_registrar"].as_str().unwrap(),
        )],
        &fixture,
        false,
    ))?;
    assert!(
        output.normalized_events.is_empty(),
        "the cleanup expiry class without the Graveyard owner is not cleanup evidence: {:#?}",
        output.normalized_events
    );
    Ok(())
}

#[test]
fn aligned_v1_expiry_keeps_base_renewal_out_of_v2_evidence() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["R-01"];
    let addresses = &fixture["addresses"];
    let label = scenario["label"].as_str().unwrap();
    let v2_token = decimal_u256(&scenario["v2_token_id"])?;
    let renewal_block = scenario["renewal_block"].as_i64().unwrap();
    let aligned = scenario["decoded_v2_expiry"].as_u64().unwrap();
    let output = interpret_test_batch(batch(
        vec![
            reserved_at(addresses, v2_token, label, 1_822_787_383, 226, 1),
            expiry_updated_at(addresses, v2_token, aligned, renewal_block, 1),
            base_renewed_at(
                addresses,
                decimal_u256(&scenario["base_token_id"])?,
                aligned,
                renewal_block,
                2,
            ),
            bridge_renewed_at(
                addresses,
                v2_token,
                label,
                scenario["duration"].as_u64().unwrap(),
                aligned,
                renewal_block,
                3,
            ),
        ],
        &fixture,
        true,
    ))?;
    assert_synchronized_pair(&output);
    let base_expiry = output
        .normalized_events
        .iter()
        .find(|event| {
            event.source_family == "ens_v2_migration_l1" && event.event_kind == "ExpiryChanged"
        })
        .expect("BaseRegistrar expiry arm");
    assert!(
        output
            .migration_event_associations
            .iter()
            .all(|association| association.event_identity != base_expiry.event_identity),
        "aligned migration-family arms must not self-match into the v2 evidence set"
    );
    Ok(())
}

#[test]
fn bulk_renewals_with_a_shared_expiry_correlate_per_name_envelopes() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["R-01"];
    let addresses = &fixture["addresses"];
    let label = scenario["label"].as_str().unwrap();
    let second_label = "r01b";
    let v2_token = decimal_u256(&scenario["v2_token_id"])?;
    let second_v2_token = versioned_token(second_label, 0);
    let second_base_token = U256::from_be_bytes(*keccak256(second_label.as_bytes()));
    let renewal_block = scenario["renewal_block"].as_i64().unwrap();
    let shared_expiry = scenario["decoded_v2_expiry"].as_u64().unwrap();
    let duration = scenario["duration"].as_u64().unwrap();
    let v1_expiry = scenario["v1_expiry"].as_u64().unwrap();
    let output = interpret_test_batch(batch(
        vec![
            reserved_at(addresses, v2_token, label, 1_822_787_383, 226, 1),
            reserved_at(
                addresses,
                second_v2_token,
                second_label,
                1_822_787_383,
                226,
                2,
            ),
            expiry_updated_at(addresses, v2_token, shared_expiry, renewal_block, 1),
            base_renewed_at(
                addresses,
                decimal_u256(&scenario["base_token_id"])?,
                v1_expiry,
                renewal_block,
                2,
            ),
            bridge_renewed_at(
                addresses,
                v2_token,
                label,
                duration,
                shared_expiry,
                renewal_block,
                3,
            ),
            expiry_updated_at(addresses, second_v2_token, shared_expiry, renewal_block, 4),
            base_renewed_at(
                addresses,
                second_base_token,
                v1_expiry + 7,
                renewal_block,
                5,
            ),
            bridge_renewed_at(
                addresses,
                second_v2_token,
                second_label,
                duration,
                shared_expiry,
                renewal_block,
                6,
            ),
        ],
        &fixture,
        true,
    ))?;
    let renewals = migration_renewals(&output);
    assert_eq!(renewals.len(), 4, "events: {:#?}", output.normalized_events);
    assert!(
        renewals
            .iter()
            .all(|event| event.after_state["lifecycle_classification"] != "historical_renewal"),
        "events: {:#?}",
        output.normalized_events
    );
    let correlation = |log_index: i64| {
        let event = renewals
            .iter()
            .find(|event| event.log_index == Some(log_index))
            .unwrap_or_else(|| panic!("correlated renewal arm at log {log_index}"));
        assert_eq!(event.migration_correlation_ids.len(), 1);
        event.migration_correlation_ids[0].clone()
    };
    assert_eq!(correlation(2), correlation(3));
    assert_eq!(correlation(5), correlation(6));
    assert_ne!(
        correlation(3),
        correlation(6),
        "each bulk-renewed name keeps its own envelope"
    );
    Ok(())
}

#[test]
fn later_idempotent_expiry_update_does_not_collapse_the_renewal_envelope() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["R-01"];
    let addresses = &fixture["addresses"];
    let label = scenario["label"].as_str().unwrap();
    let v2_token = decimal_u256(&scenario["v2_token_id"])?;
    let renewal_block = scenario["renewal_block"].as_i64().unwrap();
    let expiry = scenario["decoded_v2_expiry"].as_u64().unwrap();
    let output = interpret_test_batch(batch(
        vec![
            reserved_at(addresses, v2_token, label, 1_822_787_383, 226, 1),
            expiry_updated_at(addresses, v2_token, expiry, renewal_block, 1),
            base_renewed_at(
                addresses,
                decimal_u256(&scenario["base_token_id"])?,
                scenario["v1_expiry"].as_u64().unwrap(),
                renewal_block,
                2,
            ),
            bridge_renewed_at(
                addresses,
                v2_token,
                label,
                scenario["duration"].as_u64().unwrap(),
                expiry,
                renewal_block,
                3,
            ),
            expiry_updated_at(addresses, v2_token, expiry, renewal_block, 4),
        ],
        &fixture,
        true,
    ))?;
    assert_synchronized_pair(&output);
    let echo = output
        .normalized_events
        .iter()
        .find(|event| {
            event.source_family == "ens_v2_registry_l1"
                && event.event_kind == "ExpiryChanged"
                && event.log_index == Some(4)
        })
        .expect("idempotent registry echo after the bridge log");
    assert!(
        output
            .migration_event_associations
            .iter()
            .all(|association| association.event_identity != echo.event_identity),
        "a post-bridge echo must stay outside the envelope"
    );
    Ok(())
}

#[test]
fn same_transaction_noise_leaves_the_renewal_correlation_id_unchanged() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["R-01"];
    let addresses = &fixture["addresses"];
    let label = scenario["label"].as_str().unwrap();
    let v2_token = decimal_u256(&scenario["v2_token_id"])?;
    let base_token = decimal_u256(&scenario["base_token_id"])?;
    let renewal_block = scenario["renewal_block"].as_i64().unwrap();
    let expiry = scenario["decoded_v2_expiry"].as_u64().unwrap();
    let duration = scenario["duration"].as_u64().unwrap();
    let v1_expiry = scenario["v1_expiry"].as_u64().unwrap();
    let real_legs = |logs: &mut Vec<RawLogInput>| {
        logs.extend([
            expiry_updated_at(addresses, v2_token, expiry, renewal_block, 2),
            base_renewed_at(addresses, base_token, v1_expiry, renewal_block, 3),
            bridge_renewed_at(
                addresses,
                v2_token,
                label,
                duration,
                expiry,
                renewal_block,
                4,
            ),
        ]);
    };
    let mut plain_logs = vec![reserved_at(
        addresses,
        v2_token,
        label,
        1_822_787_383,
        226,
        1,
    )];
    real_legs(&mut plain_logs);
    let mut noisy_logs = vec![
        reserved_at(addresses, v2_token, label, 1_822_787_383, 226, 1),
        expiry_updated_at(addresses, v2_token, 111_222_333, renewal_block, 0),
        reserved_at(addresses, v2_token, label, expiry, renewal_block, 1),
    ];
    real_legs(&mut noisy_logs);
    let plain = interpret_test_batch(batch(plain_logs, &fixture, true))?;
    let noisy = interpret_test_batch(batch(noisy_logs, &fixture, true))?;
    let plain_id = assert_synchronized_pair(&plain);
    let noisy_id = assert_synchronized_pair(&noisy);
    assert_eq!(
        noisy_id, plain_id,
        "same-transaction noise must not enter the correlation evidence"
    );
    Ok(())
}

fn reserved_at(
    addresses: &Value,
    token: U256,
    label: &str,
    expiry: u64,
    block: i64,
    log_index: i64,
) -> RawLogInput {
    raw_at_transaction(
        super::v2_registry::LabelReserved {
            tokenId: token,
            labelHash: keccak256(label.as_bytes()),
            label: label.to_owned(),
            expiry,
            sender: Address::from([0x22; 20]),
        }
        .encode_log_data(),
        block,
        0,
        log_index,
        addresses["eth_registry"].as_str().unwrap(),
    )
}

fn expiry_updated_at(
    addresses: &Value,
    token: U256,
    new_expiry: u64,
    block: i64,
    log_index: i64,
) -> RawLogInput {
    raw_at_transaction(
        super::v2_registry::ExpiryUpdated {
            tokenId: token,
            newExpiry: new_expiry,
            sender: Address::from([0x22; 20]),
        }
        .encode_log_data(),
        block,
        0,
        log_index,
        addresses["eth_registry"].as_str().unwrap(),
    )
}

fn base_renewed_at(
    addresses: &Value,
    base_token: U256,
    expires: u64,
    block: i64,
    log_index: i64,
) -> RawLogInput {
    raw_at_transaction(
        with_topic0(
            BaseNameRenewed {
                id: base_token,
                expires: U256::from(expires),
            }
            .encode_log_data(),
            keccak256(b"NameRenewed(uint256,uint256)"),
        ),
        block,
        0,
        log_index,
        addresses["base_registrar"].as_str().unwrap(),
    )
}

fn bridge_renewed_at(
    addresses: &Value,
    token: U256,
    label: &str,
    duration: u64,
    new_expiry: u64,
    block: i64,
    log_index: i64,
) -> RawLogInput {
    raw_at_transaction(
        with_topic0(
            BridgeNameRenewed {
                tokenId: token,
                label: label.to_owned(),
                duration,
                newExpiry: new_expiry,
                paymentToken: Address::from([0x33; 20]),
                referrer: B256::ZERO,
                amount: U256::from(640_000_005_u64),
            }
            .encode_log_data(),
            keccak256(b"NameRenewed(uint256,string,uint64,uint64,address,bytes32,uint256)"),
        ),
        block,
        0,
        log_index,
        addresses["renewal_bridge"].as_str().unwrap(),
    )
}

fn migration_renewals(output: &BatchOutput) -> Vec<&NormalizedEvent> {
    output
        .normalized_events
        .iter()
        .filter(|event| {
            event.source_family == "ens_v2_migration_l1"
                && event.event_kind == "RegistrationRenewed"
        })
        .collect()
}

/// Asserts one intact base+bridge synchronized-renewal envelope and returns its correlation id.
fn assert_synchronized_pair(output: &BatchOutput) -> String {
    let renewals = migration_renewals(output);
    assert_eq!(renewals.len(), 2, "events: {:#?}", output.normalized_events);
    assert!(
        renewals
            .iter()
            .all(|event| event.after_state["lifecycle_classification"] != "historical_renewal"),
        "events: {:#?}",
        output.normalized_events
    );
    let bridge = renewals
        .iter()
        .find(|event| event.after_state.get("duration").is_some())
        .expect("bridge renewal arm");
    let base = renewals
        .iter()
        .find(|event| event.after_state.get("duration").is_none())
        .expect("BaseRegistrar renewal arm");
    assert_eq!(bridge.migration_correlation_ids.len(), 1);
    assert_eq!(
        base.migration_correlation_ids,
        bridge.migration_correlation_ids
    );
    bridge.migration_correlation_ids[0].clone()
}
