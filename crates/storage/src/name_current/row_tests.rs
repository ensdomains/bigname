use serde_json::{Value, json};
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

use super::NameCurrentRow;
use crate::SurfaceBindingKind;

fn row(authority_arm: &str, registration: Value) -> NameCurrentRow {
    NameCurrentRow {
        logical_name_id: "ens:0x01".into(),
        namespace: "ens".into(),
        canonical_display_name: "alice.eth".into(),
        normalized_name: "alice.eth".into(),
        namehash: "0x01".into(),
        surface_binding_id: Some(Uuid::from_u128(1)),
        resource_id: Some(Uuid::from_u128(2)),
        serving_resource_id: None,
        token_lineage_id: None,
        binding_kind: Some(SurfaceBindingKind::DeclaredRegistryPath),
        declared_summary: json!({"registration": registration}),
        provenance: json!({"authority_selection": {"authority_arm": authority_arm}}),
        coverage: json!({}),
        chain_positions: json!({}),
        canonicality_summary: json!({}),
        manifest_version: 1,
        last_recomputed_at: OffsetDateTime::UNIX_EPOCH,
    }
}

#[test]
fn a_released_ens_v2_registration_serves_no_record_resource() {
    for registration in [
        json!({"status": "released", "released_at": 1_776_384_003}),
        json!({"status": "released"}),
        // `released_at` decides whatever the lifecycle reads.
        json!({"status": "active", "lifecycle_status": "active", "released_at": 1_776_384_003}),
    ] {
        let row = row("ens_v2", registration);
        assert!(row.released_ens_v2_registration(), "{row:#?}");
        assert_eq!(row.record_serving_resource_id(), None);
        assert_eq!(row.audit_record_resource_id(), row.resource_id);
    }
}

#[test]
fn other_rows_keep_their_record_resource() {
    let released = json!({"status": "released", "released_at": 1_776_384_003});
    let mut ens_v0 = row("ens_v1", released.clone());
    ens_v0.provenance["authority_selection"]["registry_generation"] = json!("old");
    let mut ownerless = row("ens_v2", released.clone());
    ownerless.provenance["authority_selection"]["ownerless_registry"] = json!(true);
    let mut event_linked = row("ens_v2", released.clone());
    event_linked.surface_binding_id = None;
    event_linked.resource_id = None;
    event_linked.binding_kind = None;
    event_linked.serving_resource_id = Some(Uuid::from_u128(3));
    event_linked.provenance["read_reachability"] =
        json!({"basis": "root_registry_resolver_pointer"});
    for row in [
        // A released ENSv1 lease.
        row("ens_v1", released.clone()),
        ens_v0,
        ownerless,
        row("ens_v2", json!({"status": "active", "released_at": null})),
        event_linked,
    ] {
        assert!(!row.released_ens_v2_registration(), "{row:#?}");
        assert!(row.record_serving_resource_id().is_some(), "{row:#?}");
        assert_eq!(
            row.record_serving_resource_id(),
            row.audit_record_resource_id()
        );
    }
}
