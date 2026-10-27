//! Rows 8 to 10: a migration controller is the address whose declared range holds the
//! transaction's block. A role with a retired range beside the live one correlates the live
//! controller and not the retired one, whatever order the rows load in.
use super::*;

const RETIRED: &str = "0x00000000000000000000000000000000000000c0";

/// The locked `.eth` migration L-01 (`ProxyDeployed` and `LabelRegistered` sent by the fixture's
/// locked controller), with the locked controller role declared as `ranges`, in that order.
fn locked_migration(ranges: &[(&str, i64, Option<i64>)]) -> anyhow::Result<BatchOutput> {
    locked_migration_expiring(ranges, None)
}

/// [`locked_migration`] whose `LabelRegistered` carries `expiry` instead of the fixture's.
fn locked_migration_expiring(
    ranges: &[(&str, i64, Option<i64>)],
    expiry: Option<u64>,
) -> anyhow::Result<BatchOutput> {
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
                super::super::v2_registry::LabelRegistered {
                    tokenId: token,
                    labelHash: scenario["labelhash"].as_str().unwrap().parse()?,
                    label: scenario["label"].as_str().unwrap().to_owned(),
                    owner: Address::from([0x11; 20]),
                    expiry: expiry.unwrap_or(scenario["stored_expiry"].as_u64().unwrap()),
                    sender: address(addresses, "locked_controller")?,
                }
                .encode_log_data(),
                block,
                0,
                scenario["v2_registration_log_index"].as_i64().unwrap(),
                addresses["eth_registry"].as_str().unwrap(),
            ),
            raw_at_transaction(
                super::super::v2_registry::TokenResource {
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
        .admissions
        .retain(|admission| admission.role.as_deref() != Some("locked_migration_controller"));
    for (index, (address, from, to)) in ranges.iter().enumerate() {
        let address = match *address {
            "fixture" => addresses["locked_controller"].as_str().unwrap(),
            other => other,
        };
        let mut row = admission_at(
            MIGRATION_MANIFEST_ID,
            "locked_migration_controller",
            address,
            900 + index as u128,
        );
        row.active_from_block = Some(*from);
        row.active_to_block = *to;
        input.admissions.push(row);
    }
    interpret_test_batch(input)
}

fn locked_boundary(output: &BatchOutput) -> bool {
    output.normalized_events.iter().any(|event| {
        event.event_kind == "MigrationApplied"
            && event.after_state["migration_path"] == "locked_wrapped"
    })
}

fn block() -> anyhow::Result<i64> {
    Ok(fixture()?["scenarios"]["L-01"]["migration_block"]
        .as_i64()
        .unwrap())
}

#[test]
fn the_live_controller_correlates_beside_a_retired_one_in_either_order() -> anyhow::Result<()> {
    let block = block()?;
    let retired = (RETIRED, 0, Some(block - 1));
    let live = ("fixture", block, None);
    for (case, ranges) in [
        ("retired first", [retired, live]),
        ("live first", [live, retired]),
    ] {
        let output = locked_migration(&ranges)?;
        assert!(locked_boundary(&output), "{case}");
        assert_eq!(
            output.migration_candidate_identity_effects.len(),
            1,
            "{case}"
        );
    }
    Ok(())
}

#[test]
fn a_controller_outside_its_range_correlates_nothing() -> anyhow::Result<()> {
    let block = block()?;
    // (case, locked controller rows): the logs' sender is the fixture's controller.
    for (case, ranges) in [
        (
            "retired at block - 1, live elsewhere",
            vec![("fixture", 0, Some(block - 1)), (RETIRED, block, None)],
        ),
        (
            "live elsewhere, retired at block - 1",
            vec![(RETIRED, block, None), ("fixture", 0, Some(block - 1))],
        ),
        (
            "declared from block + 1",
            vec![("fixture", block + 1, None)],
        ),
    ] {
        let output = locked_migration(&ranges)?;
        assert!(!locked_boundary(&output), "{case}");
        assert!(
            output.migration_candidate_identity_effects.is_empty(),
            "{case}"
        );
        assert!(
            output
                .normalized_events
                .iter()
                .all(|event| event.consumer_visibility != "candidate"),
            "{case}"
        );
    }
    // The boundaries of the ranges: from = block and to = block both hold it.
    for ranges in [
        vec![("fixture", block, None)],
        vec![("fixture", 0, Some(block))],
    ] {
        assert!(locked_boundary(&locked_migration(&ranges)?));
    }
    Ok(())
}

/// Row 19, a pin of today's behaviour: a locked name whose `CAN_EXTEND_EXPIRY` fuse was set
/// migrates with expiry `type(uint64).max`, and the boundary keeps that exact word. Serving it
/// as a finite expiry, not as no expiry, is `storage::expiry`'s rule for ENSv2 registries.
/// (upstream: .refs/ens_v2_mainnet_20261002/contracts/src/migration/LockedWrapperReceiver.sol:L127-L134 @ ens_v2_mainnet_20261002@4f47cbcc)
#[test]
fn a_locked_migration_keeps_the_maximum_expiry_word() -> anyhow::Result<()> {
    let output = locked_migration_expiring(&[("fixture", 0, None)], Some(u64::MAX))?;
    let boundary = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "MigrationApplied")
        .expect("locked migration boundary");
    assert_eq!(
        boundary.after_state["stored_expiry"]
            .to_string()
            .trim_matches('"'),
        u64::MAX.to_string()
    );
    Ok(())
}
