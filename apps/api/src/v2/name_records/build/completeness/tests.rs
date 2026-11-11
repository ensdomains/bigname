use bigname_storage::{NameCurrentRow, RecordInventoryCurrentRow};
use serde_json::{Value, json};
use sqlx::types::Uuid;

use super::*;
use crate::v2::vocab::Completeness;

fn row(status: &str, reason: Option<&str>, declared_summary: Value) -> NameCurrentRow {
    let mut coverage = json!({"status": status});
    if let Some(reason) = reason {
        coverage["unsupported_reason"] = json!(reason);
    }
    NameCurrentRow {
        logical_name_id: "ens:alice.eth".to_owned(),
        namespace: "ens".to_owned(),
        canonical_display_name: "alice.eth".to_owned(),
        normalized_name: "alice.eth".to_owned(),
        namehash: "0xalice".to_owned(),
        surface_binding_id: Some(Uuid::from_u128(0x607)),
        resource_id: Some(Uuid::from_u128(0x606)),
        serving_resource_id: None,
        token_lineage_id: None,
        binding_kind: Some(bigname_storage::SurfaceBindingKind::DeclaredRegistryPath),
        declared_summary,
        provenance: json!({"authority_selection": {"authority_arm": "ens_v2"}}),
        coverage,
        chain_positions: json!({}),
        canonicality_summary: json!({}),
        manifest_version: 1,
        last_recomputed_at: sqlx::types::time::OffsetDateTime::UNIX_EPOCH,
    }
}

fn active() -> Value {
    json!({"registration": {"status": "active", "authority_kind": "registrar"}})
}

fn inventory(coverage: Value) -> RecordInventoryCurrentRow {
    RecordInventoryCurrentRow {
        resource_id: Uuid::from_u128(0x606),
        record_version_boundary: json!({}),
        enumeration_basis: json!({}),
        selectors: json!([]),
        explicit_gaps: json!([]),
        unsupported_families: json!([]),
        last_change: None,
        entries: json!([]),
        provenance: json!({}),
        coverage,
        chain_positions: json!({}),
        canonicality_summary: json!({}),
        manifest_version: 1,
        last_recomputed_at: sqlx::types::time::OffsetDateTime::UNIX_EPOCH,
    }
}

fn applied(row: &NameCurrentRow, record_inventory: Option<&RecordInventoryCurrentRow>) -> Meta {
    let mut meta = Meta {
        source: Some(Source::Indexed),
        ..Meta::default()
    };
    apply_name_completeness(&mut meta, row, record_inventory).expect("a mapped reason");
    meta
}

fn assert_unsupported(meta: &Meta, reason: &str) {
    assert_eq!(meta.completeness, Some(Completeness::Unsupported));
    assert_eq!(meta.unsupported_fields, Some(vec!["records".to_owned()]));
    assert_eq!(meta.unsupported_reason.as_deref(), Some(reason));
}

#[test]
fn an_authoritative_inventory_is_full_on_either_coverage_status() {
    for status in ["full", "projected"] {
        let meta = applied(
            &row("projected", None, active()),
            Some(&inventory(json!({"status": status}))),
        );
        assert_eq!(meta.completeness, Some(Completeness::Full), "{status}");
        assert_eq!(meta.unsupported_fields, None, "{status}");
        assert_eq!(meta.unsupported_reason, None, "{status}");
    }
}

#[test]
fn a_coverage_that_fails_the_authoritative_test_is_not_authoritative() {
    for coverage in [
        json!({"status": "unsupported", "unsupported_reason": "resolver_not_declared"}),
        json!({"status": "unsupported"}),
        json!({"status": "projected", "unsupported_reason": null}),
    ] {
        let meta = applied(
            &row("projected", None, active()),
            Some(&inventory(coverage)),
        );
        assert_unsupported(&meta, "inventory_not_authoritative");
    }
}

#[test]
fn no_inventory_is_not_available() {
    assert_unsupported(
        &applied(&row("projected", None, active()), None),
        "inventory_not_available",
    );
}

/// The name-level short circuit reports the reason each key carries, even with an inventory.
#[test]
fn an_authority_unsupported_name_reports_the_mapped_name_level_reason() {
    let unsupported = row(
        "unsupported",
        Some("conflicting_current_ens_authority"),
        active(),
    );
    let meta = applied(
        &unsupported,
        Some(&inventory(json!({"status": "projected"}))),
    );
    assert_unsupported(&meta, "conflicting_current_ens_authority");
}

#[test]
fn an_unprojected_authority_reports_no_inventory() {
    let unsupported = row(
        "unsupported",
        Some("current_authority_not_projected"),
        active(),
    );
    assert_unsupported(&applied(&unsupported, None), "inventory_not_available");
}

/// The one exception to the short circuit, `build::authority_unsupported_reason`: a TLD served
/// through its root-registry pointer classifies through that pointer's inventory.
#[test]
fn a_root_registry_pointer_follows_its_inventory() {
    let mut pointer = row(
        "unsupported",
        Some("current_authority_not_projected"),
        json!({}),
    );
    pointer.surface_binding_id = None;
    pointer.binding_kind = None;
    pointer.serving_resource_id = pointer.resource_id.take();
    pointer.provenance = json!({"read_reachability": {"basis": "root_registry_resolver_pointer"}});
    let serving = inventory(json!({"status": "projected"}));
    let meta = applied(&pointer, Some(&serving));
    assert_eq!(meta.completeness, Some(Completeness::Full));
    let refused = inventory(json!({"status": "unsupported"}));
    assert_unsupported(
        &applied(&pointer, Some(&refused)),
        "inventory_not_authoritative",
    );
}

#[test]
fn a_withheld_resolution_reports_the_reason_each_key_carries() {
    let mut summary = active();
    summary["resolution_unsupported_reason"] = json!("ens_v2_path_target_not_projected");
    let withheld = row("projected", None, summary);
    let meta = applied(&withheld, Some(&inventory(json!({"status": "projected"}))));
    assert_unsupported(&meta, "ens_v2_path_target_not_projected");
}

/// These names answer their keys `not_found`, and no inventory stands behind that answer, even
/// if the row still reaches one.
#[test]
fn a_name_serving_no_records_is_not_available() {
    let unresolvable = {
        let mut summary = active();
        summary["unresolvable_reason"] = json!("no_live_ens_v2_entry");
        row("projected", None, summary)
    };
    let released = row(
        "projected",
        None,
        json!({"registration": {"status": "released", "authority_kind": "ens_v2_registry"}}),
    );
    assert!(released.released_ens_v2_registration());
    for row in [unresolvable, released] {
        let meta = applied(&row, Some(&inventory(json!({"status": "projected"}))));
        assert_unsupported(&meta, "inventory_not_available");
    }
}

#[test]
fn a_verified_answer_leaves_the_fields_absent() {
    let mut meta = Meta {
        source: Some(Source::Verified),
        ..Meta::default()
    };
    apply_name_completeness(&mut meta, &row("projected", None, active()), None)
        .expect("a verified answer sets nothing");
    assert_eq!(meta.completeness, None);
    assert_eq!(meta.unsupported_fields, None);
    assert_eq!(meta.unsupported_reason, None);
}
