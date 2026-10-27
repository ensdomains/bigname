use serde_json::{Value, json};

use super::{Changed, SenderProbe, migration_change_qualifies};
use crate::redo_first_effect::{ManifestDiff, Side};

const CONTROLLER: &str = "0x00000000000000000000000000000000000c0001";
const MOVED: &str = "0x00000000000000000000000000000000000c0002";
const FACTORY: &str = "0x00000000000000000000000000000000000f0001";
const IMPLEMENTATION: &str = "0x00000000000000000000000000000000000e0001";
const PROXY_DEPLOYED: &str = "0x0a2c575ff341b41da136c9ccae74ec230a927a024d18f0dccf46d123f28f5f54";
/// The family's `ProxyDeployed` fragment, as the manifest declares it.
const FRAGMENT: &str = "event ProxyDeployed(address indexed sender, address indexed proxyAddress, \
                        uint256 salt, address implementation)";

fn contract(role: &str, address: &str, start: i64) -> Value {
    json!({"role": role, "address": address, "proxy_kind": "none", "start_block": start})
}

/// A migration payload over `contracts`, with the address watch every declared contract compiles
/// for the family's one event.
fn payload(contracts: &[Value]) -> Value {
    with_fragment(contracts, FRAGMENT)
}

fn with_fragment(contracts: &[Value], fragment: &str) -> Value {
    let watch: Vec<Value> = contracts
        .iter()
        .map(|contract| {
            json!({"emitter": {"kind": "address", "family": "ens_v2_migration_l1",
                "address": contract["address"].as_str().unwrap().to_ascii_lowercase()},
                "topic0": PROXY_DEPLOYED, "start": contract["start_block"]})
        })
        .collect();
    json!({
        "manifest_version": 1, "namespace": "ens", "source_family": "ens_v2_migration_l1",
        "chain": "ethereum-mainnet", "deployment_epoch": "ens_v2_mainnet_20261002",
        "rollout_status": "active", "normalizer_version": "ensip15@ens-normalize-0.1.1",
        "correlation_addresses": {"ens_v1_name_wrapper": "0x01"}, "capability_flags": {},
        "roots": [], "discovery_rules": [], "contracts": contracts,
        "abi": {"events": [{"name": "ProxyDeployed", "fragment": fragment,
            "emitter_roles": ["verifiable_factory"]}]},
        "_bigname_compiled_watch": watch,
    })
}

fn active(payload: &Value) -> Side<'_> {
    Side {
        rollout_status: Some("active"),
        payload,
    }
}

fn verdict(old: &Value, new: &Value) -> Result<Vec<Changed>, String> {
    migration_change_qualifies(Some(&active(old)), Some(&active(new)))
}

fn refused(old: &Value, new: &Value, reason: &str) {
    let error = verdict(old, new).expect_err("the change keeps the full range");
    assert!(error.contains(reason), "{reason:?} not in {error:?}");
}

fn changed(role: &str, address: &str, start: i64) -> Changed {
    Changed {
        role: role.to_owned(),
        address: address.to_owned(),
        start,
        sender: None,
    }
}

/// A changed controller, probed at the factory's `ProxyDeployed` topic `position`.
fn controller(address: &str, start: i64, position: i32) -> Changed {
    Changed {
        sender: Some(SenderProbe {
            factory: FACTORY.to_owned(),
            topic0: PROXY_DEPLOYED.to_owned(),
            position,
        }),
        ..changed("locked_migration_controller", address, start)
    }
}

fn factory() -> Value {
    contract("verifiable_factory", FACTORY, 5)
}

// Row 1, the predicate's half. A role the baseline does not declare: one added declaration, and a
// diff of its start and address.
#[test]
fn an_added_role_qualifies_with_its_start() {
    let old = payload(&[factory()]);
    let new = payload(&[
        factory(),
        contract("user_registry_implementation", CONTROLLER, 40),
    ]);
    assert_eq!(
        verdict(&old, &new),
        Ok(vec![changed(
            "user_registry_implementation",
            CONTROLLER,
            40
        )])
    );
    let mut diff = ManifestDiff::default();
    diff.add(Some(&active(&old)), Some(&active(&new)));
    assert_eq!(diff.starts.into_iter().collect::<Vec<_>>(), [40]);
    assert_eq!(diff.addresses.into_iter().collect::<Vec<_>>(), [CONTROLLER]);
}

// Row 2, the predicate's half. A singleton role moved: both declarations, so the diff starts at
// the old start, below the new one.
#[test]
fn a_moved_singleton_qualifies_from_the_lower_start() {
    let old = payload(&[
        factory(),
        contract("locked_migration_controller", CONTROLLER, 20),
    ]);
    let new = payload(&[
        factory(),
        contract("locked_migration_controller", MOVED, 30),
    ]);
    assert_eq!(
        verdict(&old, &new),
        Ok(vec![
            controller(CONTROLLER, 20, 2),
            controller(MOVED, 30, 2)
        ])
    );
    let mut diff = ManifestDiff::default();
    diff.add(Some(&active(&old)), Some(&active(&new)));
    assert_eq!(diff.starts.into_iter().collect::<Vec<_>>(), [20, 30]);
    assert_eq!(diff.addresses.len(), 2, "{:?}", diff.addresses);
}

// An address added beside the unchanged entries of a repeatable role. No migration role is
// repeatable today (the loader refuses a second entry), so this pins the shape for one.
#[test]
fn an_address_added_beside_unchanged_entries_qualifies() {
    let old = payload(&[factory(), contract("helper", CONTROLLER, 20)]);
    let new = payload(&[
        factory(),
        contract("helper", CONTROLLER, 20),
        contract("helper", MOVED, 30),
    ]);
    assert_eq!(verdict(&old, &new), Ok(vec![changed("helper", MOVED, 30)]));
}

// Row 3, the predicate's half.
#[test]
fn a_removed_declaration_is_refused() {
    let old = payload(&[
        factory(),
        contract("locked_migration_controller", CONTROLLER, 20),
    ]);
    refused(
        &old,
        &payload(&[factory()]),
        "it removes the locked_migration_controller declaration",
    );
}

// Row 5, the predicate's half. `plan` refuses it first with the every-block reason.
#[test]
fn a_changed_correlation_address_is_refused() {
    let old = payload(&[factory()]);
    let mut new = old.clone();
    new["correlation_addresses"]["ens_v1_name_wrapper"] = json!("0x02");
    refused(&old, &new, "it changes `correlation_addresses`");
}

// Row 11.
#[test]
fn a_start_edit_of_an_unchanged_address_is_refused() {
    let old = payload(&[
        factory(),
        contract("locked_migration_controller", CONTROLLER, 20),
    ]);
    let new = payload(&[
        factory(),
        contract("locked_migration_controller", CONTROLLER, 30),
    ]);
    refused(
        &old,
        &new,
        &format!("it edits the locked_migration_controller declaration of {CONTROLLER} in place"),
    );
}

// Row 12. A changed declaration that is a proxy, on either side.
#[test]
fn a_changed_proxy_declaration_is_refused() {
    let old = payload(&[factory()]);
    let mut proxy = contract("user_registry_implementation", CONTROLLER, 40);
    proxy["proxy_kind"] = json!("uups");
    proxy["implementation"] = json!(MOVED);
    refused(
        &old,
        &payload(&[factory(), proxy.clone()]),
        "is a proxy, whose implementation the bounded redo does not follow",
    );
    let moved = payload(&[
        factory(),
        contract("user_registry_implementation", MOVED, 50),
    ]);
    refused(
        &payload(&[factory(), proxy]),
        &moved,
        "is a proxy, whose implementation the bounded redo does not follow",
    );
}

// Row 13. Every top-level field the declarations do not hold is compared whole, and a manifest
// that is not active on both sides, new or retired keeps the full range.
#[test]
fn a_changed_identity_or_status_is_refused() {
    let old = payload(&[factory()]);
    for (field, value) in [
        ("deployment_epoch", json!("ens_v2_mainnet_20261018")),
        ("rollout_status", json!("deprecated")),
        ("chain", json!("ethereum-sepolia")),
        ("namespace", json!("basenames")),
        ("discovery_rules", json!([{"edge_kind": "resolver"}])),
    ] {
        let mut new = old.clone();
        new[field] = value;
        new["contracts"] = json!([
            factory(),
            contract("user_registry_implementation", CONTROLLER, 40)
        ]);
        refused(&old, &new, &format!("it changes `{field}`"));
    }
    let retired = Side {
        rollout_status: Some("deprecated"),
        payload: &old,
    };
    let error = migration_change_qualifies(Some(&active(&old)), Some(&retired))
        .expect_err("a retirement keeps the full range");
    assert!(error.contains("not active on both sides"), "{error}");
    let error = migration_change_qualifies(None, Some(&active(&old)))
        .expect_err("a new manifest keeps the full range");
    assert!(error.contains("the manifest is new or retired"), "{error}");
}

// Row 14. Moving the factory moves the emitter of the watched `ProxyDeployed`.
#[test]
fn a_moved_event_emitter_is_refused() {
    let old = payload(&[factory()]);
    let new = payload(&[contract("verifiable_factory", MOVED, 5)]);
    refused(
        &old,
        &new,
        "it changes the verifiable_factory declaration, whose address emits the family's events",
    );
}

// The compiled watch plan may differ only in the watches of the added and moved addresses.
#[test]
fn a_compiled_watch_change_beyond_the_changed_addresses_is_refused() {
    let old = payload(&[factory()]);
    let mut new = payload(&[
        factory(),
        contract("user_registry_implementation", CONTROLLER, 40),
    ]);
    assert!(verdict(&old, &new).is_ok());
    new["_bigname_compiled_watch"][0]["start"] = json!(0);
    refused(
        &old,
        &new,
        "its compiled watch plan changes beyond the added and moved addresses",
    );
}

// Two roles changed in one sync: an addition and a move, each reported.
#[test]
fn an_addition_and_a_move_in_one_change_qualify_together() {
    let old = payload(&[
        factory(),
        contract("locked_migration_controller", CONTROLLER, 20),
    ]);
    let new = payload(&[
        factory(),
        contract("locked_migration_controller", MOVED, 30),
        contract("user_registry_implementation", IMPLEMENTATION, 10),
    ]);
    let declared = verdict(&old, &new).expect("both changes qualify");
    assert_eq!(declared.len(), 3, "{declared:?}");
    assert_eq!(
        declared.iter().map(|entry| entry.start).min(),
        Some(10),
        "{declared:?}"
    );
}

// The factory names the controller in the topic after the topic0 and every indexed input before
// `sender`, read from the fragment's indexed flags.
#[test]
fn the_sender_topic_follows_the_fragments_indexed_inputs() {
    let old = payload(&[factory()]);
    let contracts = [
        factory(),
        contract("locked_migration_controller", CONTROLLER, 20),
    ];
    let reordered = "event ProxyDeployed(address indexed proxyAddress, uint256 salt, \
                     address indexed sender, address implementation)";
    let mut new = with_fragment(&contracts, reordered);
    let mut old_reordered = old.clone();
    old_reordered["abi"] = new["abi"].clone();
    let mut probed = controller(CONTROLLER, 20, 3);
    let event: super::ManifestAbiEvent =
        serde_json::from_value(json!({"name": "ProxyDeployed", "fragment": reordered})).unwrap();
    probed.sender.as_mut().unwrap().topic0 = event.topic0().unwrap().unwrap();
    assert_eq!(verdict(&old_reordered, &new), Ok(vec![probed]));
    assert_eq!(
        verdict(&old, &payload(&contracts)),
        Ok(vec![controller(CONTROLLER, 20, 2)])
    );
    new["abi"]["events"][0]["fragment"] = json!(
        "event ProxyDeployed(address sender, address indexed proxyAddress, uint256 salt, \
         address implementation)"
    );
    let mut old_unindexed = old;
    old_unindexed["abi"] = new["abi"].clone();
    refused(
        &old_unindexed,
        &new,
        "its ProxyDeployed fragment does not index `sender`",
    );
}

// Without a declared factory no factory log is interpreted, so the controller has no probe.
#[test]
fn a_controller_without_a_declared_factory_has_no_probe() {
    let new = payload(&[contract("locked_migration_controller", CONTROLLER, 20)]);
    assert_eq!(
        verdict(&payload(&[]), &new),
        Ok(vec![changed("locked_migration_controller", CONTROLLER, 20)])
    );
}
