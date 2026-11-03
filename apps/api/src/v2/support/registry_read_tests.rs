//! The decision table of the ENSv1 registry read: one row per input class the composed row can
//! carry, with what the row's own fields decide.
use serde_json::{Value, json};
use sqlx::types::{Uuid, time::OffsetDateTime};

use super::*;

const SERVED: &str = "0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41";
const REGISTRY: &str = "0x231b0ee14048e9dccd1d247744d114a4eb5e8e63";

/// An active ENSv1 registration that serves `served` and whose registry pointer is `registry`.
fn row(served: Value, registry: Value) -> NameCurrentRow {
    NameCurrentRow {
        logical_name_id: "ens:brantly.eth".to_owned(),
        namespace: "ens".to_owned(),
        canonical_display_name: "brantly.eth".to_owned(),
        normalized_name: "brantly.eth".to_owned(),
        namehash: "0x00".to_owned(),
        surface_binding_id: Some(Uuid::from_u128(1)),
        resource_id: Some(Uuid::from_u128(2)),
        serving_resource_id: None,
        token_lineage_id: None,
        binding_kind: None,
        declared_summary: json!({
            "registration": {"status": "active", "authority_kind": "registrar"},
            "resolver": {"chain_id": "ethereum-mainnet", "address": served},
            "ens_v1_resolver": registry,
        }),
        provenance: json!({
            "authority_selection": {"authority_arm": "ens_v1"},
            "resolver_pointer_source_family": "ens_v1_registry_l1",
        }),
        coverage: json!({}),
        chain_positions: json!({}),
        canonicality_summary: json!({}),
        manifest_version: 1,
        last_recomputed_at: OffsetDateTime::from_unix_timestamp(1_717_171_719)
            .expect("test timestamp"),
    }
}

fn pointer(address: &str) -> Value {
    json!({"chain_id": "ethereum-mainnet", "address": address})
}

fn stale() -> NameCurrentRow {
    row(json!(SERVED), pointer(REGISTRY))
}

#[test]
fn the_registry_read_decides_each_input_class() {
    let resolver = || Decision::Resolver(REGISTRY.to_owned());
    let cases: Vec<(&str, NameCurrentRow, Decision)> = vec![
        (
            "registry names another resolver (brantly.eth)",
            stale(),
            resolver(),
        ),
        (
            "pointers agree (ens.eth)",
            row(json!(SERVED), pointer(SERVED)),
            Decision::Composed,
        ),
        (
            "pointers agree in another case",
            row(
                json!(SERVED.to_ascii_uppercase().replacen("0X", "0x", 1)),
                pointer(SERVED),
            ),
            Decision::Composed,
        ),
        (
            "registry pointer null, resolver served",
            row(json!(SERVED), Value::Null),
            Decision::NullPointer,
        ),
        (
            "both null",
            row(Value::Null, Value::Null),
            Decision::Composed,
        ),
        (
            "served pointer zero, registry null",
            row(json!(ZERO_ADDRESS), Value::Null),
            Decision::Composed,
        ),
        (
            "served pointer cleared, registry names one",
            row(Value::Null, pointer(REGISTRY)),
            resolver(),
        ),
        (
            "no stored registry pointer",
            {
                let mut row = stale();
                row.declared_summary
                    .as_object_mut()
                    .expect("summary object")
                    .remove("ens_v1_resolver");
                row
            },
            Decision::Composed,
        ),
        (
            "ens_v0 arm",
            {
                let mut row = stale();
                row.provenance["authority_selection"]["authority_arm"] = json!("ens_v0");
                row
            },
            resolver(),
        ),
        (
            "migrated, ens_v2 arm",
            {
                let mut row = stale();
                row.provenance["authority_selection"]["authority_arm"] = json!("ens_v2");
                row
            },
            Decision::Composed,
        ),
        (
            "ENSv2 pointer family",
            {
                let mut row = stale();
                row.provenance["resolver_pointer_source_family"] = json!("ens_v2_registry_l1");
                row
            },
            Decision::Composed,
        ),
        (
            "Basenames pointer family",
            {
                let mut row = stale();
                row.provenance["resolver_pointer_source_family"] = json!("basenames_base_registry");
                row
            },
            Decision::Composed,
        ),
        (
            "withheld, no live ENSv2 entry",
            {
                let mut row = stale();
                row.declared_summary["unresolvable_reason"] = json!("no_live_ens_v2_entry");
                row
            },
            Decision::Composed,
        ),
        (
            "Different, target not projected",
            {
                let mut row = stale();
                row.declared_summary["resolution_unsupported_reason"] =
                    json!("ens_v2_path_target_not_projected");
                row
            },
            Decision::Composed,
        ),
        (
            "coverage keeps the resolver out",
            {
                let mut row = stale();
                row.coverage = json!({"unsupported_reason": "current_authority_not_projected"});
                row
            },
            Decision::Composed,
        ),
        (
            "no current registration",
            {
                let mut row = stale();
                row.declared_summary["registration"] = json!({"status": "expired"});
                row
            },
            Decision::Composed,
        ),
    ];
    for (class, row, expected) in cases {
        assert_eq!(decide(&row), expected, "{class}");
    }
}

/// A row that composes its node's ENSv1 registry clear: the latest pointer write serves no
/// resolver and the stored registry pointer is null.
fn cleared() -> NameCurrentRow {
    let mut row = row(Value::Null, Value::Null);
    row.declared_summary["resolver"] =
        json!({"chain_id": null, "address": null, "latest_event_kind": "ResolverChanged"});
    row
}

#[test]
fn the_registry_clear_predicate_decides_each_input_class() {
    let with = |change: fn(&mut NameCurrentRow)| {
        let mut row = cleared();
        change(&mut row);
        row
    };
    let cases: Vec<(&str, NameCurrentRow, bool)> = vec![
        ("ENSv1 registry clear", cleared(), true),
        (
            "ens_v0 arm",
            with(|row| row.provenance["authority_selection"]["authority_arm"] = json!("ens_v0")),
            true,
        ),
        (
            "registrar pointer family",
            with(|row| {
                row.provenance["resolver_pointer_source_family"] = json!("ens_v1_registrar_l1")
            }),
            true,
        ),
        (
            "wrapper pointer family",
            with(|row| {
                row.provenance["resolver_pointer_source_family"] = json!("ens_v1_wrapper_l1")
            }),
            true,
        ),
        (
            "no pointer write observed",
            with(|row| {
                row.declared_summary["resolver"]["latest_event_kind"] = Value::Null;
                row.provenance
                    .as_object_mut()
                    .expect("provenance object")
                    .remove("resolver_pointer_source_family");
            }),
            false,
        ),
        (
            "no latest_event_kind",
            with(|row| row.declared_summary["resolver"]["latest_event_kind"] = Value::Null),
            false,
        ),
        (
            "cleared ENSv1 pointer under an ENSv2 arm",
            with(|row| {
                row.provenance["authority_selection"]["authority_arm"] = json!("ens_v2");
                row.provenance["resolver_pointer_source_family"] = json!("ens_v2_registry_l1");
            }),
            false,
        ),
        (
            "ENSv2 arm with an ENSv1 family",
            with(|row| row.provenance["authority_selection"]["authority_arm"] = json!("ens_v2")),
            false,
        ),
        (
            "ENSv2 pointer family",
            with(|row| {
                row.provenance["resolver_pointer_source_family"] = json!("ens_v2_registry_l1")
            }),
            false,
        ),
        (
            "Basenames pointer family",
            with(|row| {
                row.provenance["resolver_pointer_source_family"] = json!("basenames_base_registry")
            }),
            false,
        ),
        (
            "withheld, path has no resolver",
            with(|row| {
                row.declared_summary["unresolvable_reason"] = json!("ens_v2_path_no_resolver")
            }),
            false,
        ),
        (
            "withheld, no live ENSv2 entry",
            with(|row| row.declared_summary["unresolvable_reason"] = json!("no_live_ens_v2_entry")),
            false,
        ),
        (
            "path target not projected",
            with(|row| {
                row.declared_summary["resolution_unsupported_reason"] =
                    json!("ens_v2_path_target_not_projected")
            }),
            false,
        ),
        (
            "withheld live pointer: the registry names a resolver",
            with(|row| row.declared_summary["ens_v1_resolver"] = pointer(REGISTRY)),
            false,
        ),
        (
            "no stored registry pointer",
            with(|row| {
                row.declared_summary
                    .as_object_mut()
                    .expect("summary object")
                    .remove("ens_v1_resolver");
            }),
            false,
        ),
        (
            "served address",
            with(|row| {
                row.declared_summary["resolver"]["chain_id"] = json!("ethereum-mainnet");
                row.declared_summary["resolver"]["address"] = json!(SERVED);
            }),
            false,
        ),
        (
            "unsupported resolver",
            with(|row| row.declared_summary["resolver"]["status"] = json!("unsupported")),
            false,
        ),
        (
            "a DNS name",
            with(|row| row.normalized_name = "nick.xyz".to_owned()),
            false,
        ),
        (
            "a subname",
            with(|row| row.normalized_name = "sub.brantly.eth".to_owned()),
            true,
        ),
        (
            "ownerless registry-only subname whose path has no resolver (54.99club.eth shape)",
            with(|row| {
                row.normalized_name = "54.99club.eth".to_owned();
                row.declared_summary["registration"] =
                    json!({"status": "active", "authority_kind": "registry_only"});
                row.declared_summary["unresolvable_reason"] = json!("ens_v2_path_no_resolver");
            }),
            false,
        ),
        (
            "coverage keeps the resolver out",
            with(|row| {
                row.coverage = json!({"unsupported_reason": "current_authority_not_projected"})
            }),
            false,
        ),
        (
            "released registration",
            with(|row| row.declared_summary["registration"] = json!({"status": "released"})),
            false,
        ),
        (
            "no resolver block",
            with(|row| {
                row.declared_summary
                    .as_object_mut()
                    .expect("summary object")
                    .remove("resolver");
            }),
            false,
        ),
    ];
    for (class, row, expected) in cases {
        assert_eq!(composed_ens_v1_registry_clear(&row), expected, "{class}");
    }
}
