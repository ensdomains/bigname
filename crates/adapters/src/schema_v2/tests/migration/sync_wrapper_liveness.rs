//! `ETHRenewerV1.syncWrapper` adds the NameWrapper as a BaseRegistrar controller, renews each
//! label through the wrapped controller with duration 0, and removes the controller again.
//! `NameWrapper.renew` writes the wrapper expiry only when the renewed lease is later than the
//! block timestamp and the NameWrapper holds the registrar token, so an envelope over a lease
//! still expired, or over a name that has left the NameWrapper, derives no wrapper expiry.
//! (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/ETHRenewerV1.sol:L106-L112 @ ens_v2_sepolia_20260629@ccaeb58)
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L318-L337 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L71-L76 @ ens_v1@91c966f)

use super::*;

const WRAPPED_CONTROLLER: &str = "0x0000000000000000000000000000000000000a44";
const REGISTRY: &str = "0x0000000000000000000000000000000000000099";

fn sync_envelope(
    addresses: &Value,
    token: U256,
    expires: u64,
    block: i64,
) -> anyhow::Result<Vec<RawLogInput>> {
    let registrar = addresses["base_registrar"].as_str().unwrap();
    let wrapper: Address = addresses["name_wrapper"].as_str().unwrap().parse()?;
    Ok(vec![
        raw_at_transaction(
            ControllerAdded {
                controller: wrapper,
            }
            .encode_log_data(),
            block,
            0,
            0,
            registrar,
        ),
        base_renewed_at(addresses, token, expires, block, 1),
        raw_at_transaction(
            ControllerRemoved {
                controller: wrapper,
            }
            .encode_log_data(),
            block,
            0,
            2,
            registrar,
        ),
    ])
}

fn migration_renewals_with_wrapper_expiry(output: &BatchOutput) -> Vec<&NormalizedEvent> {
    output
        .normalized_events
        .iter()
        .filter(|event| {
            event.source_family == "ens_v2_migration_l1"
                && event.after_state["source_event"] == "NameRenewed"
                && event.after_state.get("wrapper_expiry").is_some()
        })
        .collect()
}

/// The envelope over a wrapped name whose lease is still expired at the block, as on a name in
/// grace: the NameWrapper writes nothing, so the envelope's renewal evidence carries no wrapper
/// expiry for the unwrap fallback to read.
#[test]
fn a_sync_envelope_over_an_expired_lease_records_no_wrapper_expiry() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let addresses = &fixture["addresses"];
    let wrapper = addresses["name_wrapper"].as_str().unwrap();
    let label = b"expired-sync";
    let node = super::super::common::namehash(&["expired-sync".to_owned(), "eth".to_owned()]);
    let block = fixture["scenarios"]["R-05"]["configuration_block"]
        .as_i64()
        .unwrap();
    // The lease ended before the envelope's block, so the duration-0 renewal leaves it expired.
    let registrar_expiry = u64::try_from(block)? - 38;
    let mut logs = vec![
        raw_at_transaction(
            super::super::v1_registry::Transfer {
                node: node.parse()?,
                owner: wrapper.parse()?,
            }
            .encode_log_data(),
            block,
            0,
            0,
            REGISTRY,
        ),
        raw_at_transaction(
            super::super::NameWrapped {
                node: node.parse()?,
                name: b"\x0cexpired-sync\x03eth\0".to_vec().into(),
                owner: Address::from([0x51; 20]),
                fuses: (1 << 16) | (1 << 17),
                expiry: 7_776_100,
            }
            .encode_log_data(),
            block,
            0,
            1,
            wrapper,
        ),
    ];
    logs.extend(sync_envelope(
        addresses,
        U256::from_be_bytes(*keccak256(label)),
        registrar_expiry,
        block + 1,
    )?);
    let mut input = batch(logs, &fixture, false);
    admit_v1_registry(&mut input, REGISTRY);
    let output = interpret_test_batch(input)?;
    assert!(
        output.normalized_events.iter().any(|event| {
            event.source_family == "ens_v2_migration_l1"
                && event.after_state["source_event"] == "NameRenewed"
        }),
        "the envelope's renewal stays migration evidence"
    );
    assert_eq!(
        migration_renewals_with_wrapper_expiry(&output),
        Vec::<&NormalizedEvent>::new()
    );
    Ok(())
}

/// A name that left the NameWrapper through the unlocked-wrapped ENSv1→ENSv2 migration, its
/// BaseRegistrar token held by the Graveyard. A later ENSv1 renewal, through the wrapped
/// controller directly or inside a `syncWrapper` envelope, finds a registrar token the
/// NameWrapper does not hold, so `NameWrapper.renew` writes nothing and bigname derives no
/// wrapper expiry, live lease or not.
#[test]
fn a_migrated_wrapped_name_renewed_on_ensv1_derives_no_wrapper_expiry() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["W-01"];
    let addresses = &fixture["addresses"];
    let base_token = decimal_u256(&scenario["base_token_id"])?;
    let v2_token = decimal_u256(&scenario["v2_token_id"])?;
    let label = scenario["label"].as_str().unwrap();
    let block = scenario["migration_block"].as_i64().unwrap();
    let stored_expiry = scenario["stored_expiry"].as_u64().unwrap();
    let owner = Address::from([0x11; 20]);
    let mut dns_name = Vec::with_capacity(label.len() + 6);
    dns_name.push(u8::try_from(label.len())?);
    dns_name.extend_from_slice(label.as_bytes());
    dns_name.extend_from_slice(b"\x03eth\0");
    let mut logs = vec![
        raw_at_transaction(
            super::super::NameWrapped {
                node: scenario["namehash"].as_str().unwrap().parse()?,
                name: dns_name.into(),
                owner,
                fuses: 0,
                expiry: stored_expiry + (90 * 24 * 60 * 60),
            }
            .encode_log_data(),
            block - 1,
            0,
            0,
            addresses["name_wrapper"].as_str().unwrap(),
        ),
        raw_at_transaction(
            super::super::NameUnwrapped {
                node: scenario["namehash"].as_str().unwrap().parse()?,
                owner: address(addresses, "graveyard")?,
            }
            .encode_log_data(),
            block,
            0,
            scenario["name_unwrapped_log_index"].as_i64().unwrap(),
            addresses["name_wrapper"].as_str().unwrap(),
        ),
        raw_at_transaction(
            super::super::v1_registrar::Transfer {
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
            super::super::v2_registry::LabelRegistered {
                tokenId: v2_token,
                labelHash: keccak256(label.as_bytes()),
                label: label.to_owned(),
                owner,
                expiry: stored_expiry,
                sender: address(addresses, "unlocked_controller")?,
            }
            .encode_log_data(),
            block,
            0,
            scenario["v2_registration_log_index"].as_i64().unwrap(),
            addresses["eth_registry"].as_str().unwrap(),
        ),
        raw_at_transaction(
            super::super::v2_registry::TokenResource {
                tokenId: v2_token,
                resource: v2_token,
            }
            .encode_log_data(),
            block,
            0,
            scenario["v2_registration_log_index"].as_i64().unwrap() + 2,
            addresses["eth_registry"].as_str().unwrap(),
        ),
    ];
    let renewed = stored_expiry + 365 * 24 * 60 * 60;
    let controller_renewed = |log_index| {
        raw_at_transaction(
            super::super::wrapped_controller::NameRenewed {
                name: label.to_owned(),
                label: keccak256(label.as_bytes()),
                cost: U256::ZERO,
                expires: U256::from(renewed),
            }
            .encode_log_data(),
            block + 1,
            0,
            log_index,
            WRAPPED_CONTROLLER,
        )
    };
    let mut envelope = sync_envelope(addresses, base_token, renewed, block + 1)?;
    envelope.insert(2, controller_renewed(2));
    envelope[3].log_index = 3;
    logs.extend(envelope);
    logs.push(base_renewed_at(addresses, base_token, renewed, block + 2, 0));
    let mut direct = controller_renewed(1);
    direct.block_number = block + 2;
    direct.block_hash = format!("block-{}", block + 2);
    direct.transaction_hash = format!("transaction-{}-0", block + 2);
    direct.block_timestamp += time::Duration::seconds(1);
    logs.push(direct);

    let mut input = batch(logs, &fixture, true);
    let registrar = input
        .manifests
        .iter_mut()
        .find(|manifest| manifest.manifest_id == V1_REGISTRAR_MANIFEST_ID)
        .expect("registrar manifest");
    let mut payload: Value = serde_json::from_str(&registrar.payload_json)?;
    payload["abi"]["events"]
        .as_array_mut()
        .expect("ABI events")
        .push(json!({
            "name": "NameRenewed",
            "fragment": "event NameRenewed(string name, bytes32 indexed label, uint256 cost, uint256 expires)",
            "emitter_roles": ["wrapped_registrar_controller"],
            "normalized_events": ["ExpiryChanged", "PreimageObserved"],
        }));
    registrar.payload_json = serde_json::to_string(&payload)?;
    input.admissions.push(admission_at(
        V1_REGISTRAR_MANIFEST_ID,
        "wrapped_registrar_controller",
        WRAPPED_CONTROLLER,
        9,
    ));
    let output = interpret_test_batch(input)?;

    assert!(
        output
            .normalized_events
            .iter()
            .any(|event| event.event_kind == "MigrationApplied"),
        "the unlocked-wrapped migration still applies"
    );
    let wrapper_expiries = output
        .normalized_events
        .iter()
        .filter(|event| {
            event.event_kind == "ExpiryChanged"
                && event.after_state["authority_kind"] == "wrapper"
                && event.block_number > Some(block)
        })
        .collect::<Vec<_>>();
    assert_eq!(wrapper_expiries, Vec::<&NormalizedEvent>::new());
    assert_eq!(
        migration_renewals_with_wrapper_expiry(&output),
        Vec::<&NormalizedEvent>::new()
    );
    Ok(())
}
