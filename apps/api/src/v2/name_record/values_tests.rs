//! The event-linked registry serving gate on constructed rows: an ownerless ENSv1 name served
//! through its retained registry resolver pointer, with and without a released registration.
use serde_json::json;
use sqlx::types::{Uuid, time::OffsetDateTime};

use super::*;

/// An ownerless `.eth` row as composed: no authority resource or binding, and a serving resource
/// its retained registry resolver pointer selects.
fn ownerless_row(registration: Value) -> NameCurrentRow {
    NameCurrentRow {
        logical_name_id: "ens:0x01".into(),
        namespace: "ens".into(),
        canonical_display_name: "ownerless.eth".into(),
        normalized_name: "ownerless.eth".into(),
        namehash: "0x01".into(),
        surface_binding_id: None,
        resource_id: None,
        serving_resource_id: Some(Uuid::from_u128(0x2200)),
        token_lineage_id: None,
        binding_kind: None,
        declared_summary: json!({"registration": registration}),
        provenance: json!({
            "authority_selection": {"authority_arm": "ens_v1"},
            "read_reachability": {"basis": "retained_registry_resolver_pointer"}
        }),
        coverage: json!({}),
        chain_positions: json!({}),
        canonicality_summary: json!({}),
        manifest_version: 1,
        last_recomputed_at: OffsetDateTime::UNIX_EPOCH,
    }
}

#[test]
fn a_retained_pointer_serves_an_unreleased_ownerless_registration() {
    let row = ownerless_row(json!({"authority_kind": null, "status": "unregistered"}));
    assert!(row_has_current_registration(&row));
    assert!(row_serves_resolver(&row));
}

/// The stored summary already carries the release, with no fill involved.
#[test]
fn a_retained_pointer_serves_no_stored_released_registration() {
    for registration in [
        json!({"authority_kind": null, "released_at": "1700000160"}),
        json!({"authority_kind": null, "status": "released"}),
    ] {
        let row = ownerless_row(registration);
        assert!(!row_has_current_registration(&row), "{row:#?}");
        assert!(!row_serves_resolver(&row), "{row:#?}");
    }
}

/// An ENSv2 TLD served through its root-registry pointer keeps serving with a released summary.
#[test]
fn a_root_registry_pointer_serves_a_released_registration() {
    let mut row = ownerless_row(json!({"authority_kind": null, "released_at": "1700000160"}));
    row.canonical_display_name = "eth".into();
    row.normalized_name = "eth".into();
    row.provenance = json!({
        "authority_selection": {"authority_arm": "ens_v2"},
        "read_reachability": {"basis": "root_registry_resolver_pointer"}
    });
    assert!(row_has_current_registration(&row));
    assert!(row_serves_resolver(&row));
}

/// A Basenames registry name on the same retained basis serves no released registration.
#[test]
fn a_retained_pointer_serves_no_released_basenames_registration() {
    let mut row = ownerless_row(json!({"authority_kind": null, "released_at": "1700000160"}));
    row.namespace = bigname_storage::BASENAMES_NAMESPACE.into();
    row.canonical_display_name = "ownerless.base.eth".into();
    row.normalized_name = "ownerless.base.eth".into();
    assert!(!row_has_current_registration(&row), "{row:#?}");
    assert!(!row_serves_resolver(&row), "{row:#?}");
}

/// The identity row lookup serves, built from `row`.
fn identity_row(row: &NameCurrentRow) -> bigname_storage::IdentityNameCurrentRow {
    bigname_storage::IdentityNameCurrentRow {
        logical_name_id: row.logical_name_id.clone(),
        namespace: row.namespace.clone(),
        canonical_display_name: row.canonical_display_name.clone(),
        normalized_name: row.normalized_name.clone(),
        namehash: row.namehash.clone(),
        labelhash: None,
        labelhash_count: None,
        surface_binding_id: row.surface_binding_id,
        resource_id: row.resource_id,
        serving_resource_id: row.serving_resource_id,
        binding_kind: row.binding_kind,
        record_inventory_boundary_key: None,
        declared_summary: row.declared_summary.clone(),
        provenance: row.provenance.clone(),
        coverage: row.coverage.clone(),
        chain_positions: row.chain_positions.clone(),
        last_recomputed_at: row.last_recomputed_at,
    }
}

/// Lookup gates a stored released registration on the retained pointer as name detail does, so
/// it serves no resolver and, with no record inventory, no primary address or records
/// (`lookup/build.rs`). Its unreleased guard and the root-registry pointer still serve.
#[test]
fn lookup_serves_no_stored_released_registration_through_a_retained_pointer() {
    let released = ownerless_row(json!({"authority_kind": null, "released_at": "1700000160"}));
    let identity = identity_row(&released);
    assert!(
        !identity_row_has_current_registration(&identity),
        "{identity:#?}"
    );
    assert!(!identity_row_serves_resolver(&identity), "{identity:#?}");

    let unreleased = identity_row(&ownerless_row(json!({"authority_kind": null})));
    assert!(identity_row_has_current_registration(&unreleased));
    assert!(identity_row_serves_resolver(&unreleased));

    let mut root = released;
    root.provenance["read_reachability"]["basis"] = json!("root_registry_resolver_pointer");
    let root = identity_row(&root);
    assert!(identity_row_has_current_registration(&root));
    assert!(identity_row_serves_resolver(&root));
}
