use serde_json::json;

use super::{ManifestDiff, Side, require_addresses};

fn side<'a>(status: &'a str, payload: &'a serde_json::Value) -> Side<'a> {
    Side {
        rollout_status: Some(status),
        payload,
    }
}

#[test]
fn a_new_contract_contributes_its_start_address_and_watch() {
    let before = json!({"contracts": [], "_bigname_compiled_watch": []});
    let after = json!({
        "contracts": [{"role": "resolver", "address": "0xAB00000000000000000000000000000000000001",
            "proxy_kind": "none", "start_block": 40}],
        "_bigname_compiled_watch": [{"emitter": {"kind": "address", "family": "f",
            "address": "0xab00000000000000000000000000000000000001"}, "topic0": "0xT", "start": 40}],
    });
    let mut diff = ManifestDiff::default();
    diff.add(
        Some(&side("active", &before)),
        Some(&side("active", &after)),
    );
    assert_eq!(diff.starts.into_iter().collect::<Vec<_>>(), [40]);
    assert_eq!(
        diff.addresses.into_iter().collect::<Vec<_>>(),
        ["0xab00000000000000000000000000000000000001"]
    );
    assert_eq!(
        diff.watches.into_iter().collect::<Vec<_>>(),
        [(
            Some("0xab00000000000000000000000000000000000001".to_owned()),
            "0xt".to_owned(),
            40,
            None
        )]
    );
}

#[test]
fn an_unchanged_declaration_and_a_flag_edit_contribute_nothing() {
    let contract = json!({"role": "registry", "address": "0x01", "start_block": 7});
    let before = json!({"contracts": [contract], "capability_flags": {"a": "supported"}});
    let after = json!({"contracts": [contract], "capability_flags": {"a": "unsupported"}});
    let mut diff = ManifestDiff::default();
    diff.add(
        Some(&side("active", &before)),
        Some(&side("active", &after)),
    );
    assert_eq!(diff, ManifestDiff::default());
}

#[test]
fn a_rule_starts_at_its_role_and_a_status_change_moves_every_declaration() {
    let payload = json!({
        "roots": [{"name": "registry", "address": "0x02", "start_block": 9}],
        "discovery_rules": [{"edge_kind": "resolver", "from_role": "registry",
            "admission": "reachable_from_root"}],
    });
    let mut diff = ManifestDiff::default();
    diff.add(
        Some(&side("shadow", &payload)),
        Some(&side("active", &payload)),
    );
    assert_eq!(diff.starts.into_iter().collect::<Vec<_>>(), [9]);
    assert_eq!(diff.addresses.into_iter().collect::<Vec<_>>(), ["0x02"]);
}

#[test]
fn a_removed_watch_and_a_later_start_are_read_from_their_old_start() {
    let any = json!({"kind": "all"});
    let before = json!({"_bigname_compiled_watch": [
        {"emitter": any, "topic0": "0xA", "start": 0},
        {"emitter": any, "topic0": "0xB", "start": 10},
        {"emitter": any, "topic0": "0xC", "start": 5},
    ]});
    let after = json!({"_bigname_compiled_watch": [
        {"emitter": any, "topic0": "0xB", "start": 30},
        {"emitter": any, "topic0": "0xC", "start": 5},
    ]});
    let mut diff = ManifestDiff::default();
    diff.add(
        Some(&side("active", &before)),
        Some(&side("active", &after)),
    );
    assert_eq!(
        diff.watches.into_iter().collect::<Vec<_>>(),
        [
            (None, "0xa".to_owned(), 0, None),
            (None, "0xb".to_owned(), 10, Some(29)),
        ]
    );
    let mut retired = ManifestDiff::default();
    retired.add(
        Some(&side("active", &before)),
        Some(&side("deprecated", &before)),
    );
    assert_eq!(retired.watches.len(), 3, "{retired:?}");
}

#[test]
fn a_changed_rule_names_the_addresses_its_from_role_declares() {
    let contracts = json!([
        {"role": "registry", "address": "0xAB00000000000000000000000000000000000001",
            "start_block": 50},
        {"role": "other", "address": "0xab00000000000000000000000000000000000002",
            "start_block": 60},
    ]);
    let rule = json!({"edge_kind": "resolver", "from_role": "registry",
        "admission": "reachable_from_root"});
    let before = json!({"contracts": contracts, "discovery_rules": [rule]});
    let after = json!({"contracts": contracts, "discovery_rules": []});
    let mut diff = ManifestDiff::default();
    diff.add(
        Some(&side("active", &before)),
        Some(&side("active", &after)),
    );
    assert_eq!(diff.starts.into_iter().collect::<Vec<_>>(), [50]);
    assert_eq!(
        diff.rule_sources.into_iter().collect::<Vec<_>>(),
        [(
            "0xab00000000000000000000000000000000000001".to_owned(),
            "resolver".to_owned()
        )]
    );
    let any_role = json!({"contracts": contracts, "discovery_rules": [{"edge_kind": "resolver",
        "admission": "reachable_from_root"}]});
    let mut diff = ManifestDiff::default();
    diff.add(
        Some(&side("active", &any_role)),
        Some(&side("active", &after)),
    );
    assert_eq!(diff.rule_sources.len(), 2, "{diff:?}");
}

#[test]
fn a_diff_naming_something_other_than_an_address_is_refused() {
    let mut diff = ManifestDiff::default();
    diff.addresses
        .insert("0xab00000000000000000000000000000000000001".to_owned());
    assert!(require_addresses(&diff, "chain").is_ok());
    diff.addresses.insert("0x02".to_owned());
    let error = require_addresses(&diff, "chain").expect_err("0x02 is not an address");
    assert!(
        error.to_string().contains(
            "a changed manifest declaration names \"0x02\", which is not a 20-byte hex address"
        ),
        "{error}"
    );
}
