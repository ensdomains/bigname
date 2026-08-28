use bigname_storage::{NameCurrentRow, RecordInventoryCurrentRow};
use serde_json::json;
use sqlx::types::time::OffsetDateTime;

use super::*;
use crate::v2::{ErrorCode, support::parse_resolution_record_key};

#[test]
fn auto_rejects_derived_answer_from_nonauthoritative_inventory() {
    let timestamp =
        OffsetDateTime::from_unix_timestamp(1_717_171_719).expect("test timestamp must be valid");
    let row = NameCurrentRow {
        logical_name_id: "ens:alice.eth".to_owned(),
        namespace: "ens".to_owned(),
        canonical_display_name: "alice.eth".to_owned(),
        normalized_name: "alice.eth".to_owned(),
        namehash: "0xalice".to_owned(),
        surface_binding_id: None,
        resource_id: None,
        token_lineage_id: None,
        binding_kind: None,
        declared_summary: json!({}),
        provenance: json!({}),
        coverage: json!({"status":"projected"}),
        chain_positions: json!({}),
        canonicality_summary: json!({}),
        manifest_version: 1,
        last_recomputed_at: timestamp,
    };
    let inventory = RecordInventoryCurrentRow {
        resource_id: "00000000-0000-0000-0000-000000000606"
            .parse()
            .expect("test resource id"),
        record_version_boundary: json!({}),
        enumeration_basis: json!({}),
        selectors: json!([]),
        explicit_gaps: json!([]),
        unsupported_families: json!([]),
        last_change: None,
        entries: json!([{
            "record_key":"addr:2147483648",
            "record_family":"addr",
            "selector_key":"2147483648",
            "status":"success",
            "value":"0x0000000000000000000000000000000000000def"
        }]),
        provenance: json!({"read_rules":[{
            "kind":"ensip19_default_address",
            "source_record_key":"addr:2147483648"
        }]}),
        coverage: json!({
            "status":"unsupported",
            "unsupported_reason":"coverage_incomplete"
        }),
        chain_positions: json!({}),
        canonicality_summary: json!({}),
        manifest_version: 1,
        last_recomputed_at: timestamp,
    };
    let record = parse_resolution_record_key("addr:2147483649").expect("test selector must parse");

    let indexed = indexed_record_answer(Some(&inventory), &record)
        .expect("explicit indexed evaluation must complete");
    assert_eq!(
        indexed.status,
        Status::Unsupported,
        "explicit indexed must not serve a derived value from incomplete coverage"
    );
    assert!(
        indexed_satisfying_record_answer(&row, Some(&inventory), &record)
            .expect("auto satisfaction must evaluate")
            .is_none(),
        "auto must verify when projected inventory authority is incomplete"
    );
}

#[test]
fn auto_accepts_exact_success_from_nonauthoritative_inventory() {
    let timestamp =
        OffsetDateTime::from_unix_timestamp(1_717_171_719).expect("test timestamp must be valid");
    let row = NameCurrentRow {
        logical_name_id: "ens:alice.eth".to_owned(),
        namespace: "ens".to_owned(),
        canonical_display_name: "alice.eth".to_owned(),
        normalized_name: "alice.eth".to_owned(),
        namehash: "0xalice".to_owned(),
        surface_binding_id: None,
        resource_id: None,
        token_lineage_id: None,
        binding_kind: None,
        declared_summary: json!({}),
        provenance: json!({}),
        coverage: json!({"status":"projected"}),
        chain_positions: json!({}),
        canonicality_summary: json!({}),
        manifest_version: 1,
        last_recomputed_at: timestamp,
    };
    let inventory = RecordInventoryCurrentRow {
        resource_id: "00000000-0000-0000-0000-000000000606"
            .parse()
            .expect("test resource id"),
        record_version_boundary: json!({}),
        enumeration_basis: json!({}),
        selectors: json!([]),
        explicit_gaps: json!([]),
        unsupported_families: json!([]),
        last_change: None,
        entries: json!([{
            "record_key":"addr:60",
            "record_family":"addr",
            "selector_key":"60",
            "status":"success",
            "value":"0x0000000000000000000000000000000000000def"
        }]),
        provenance: json!({}),
        coverage: json!({
            "status":"unsupported",
            "unsupported_reason":"coverage_incomplete"
        }),
        chain_positions: json!({}),
        canonicality_summary: json!({}),
        manifest_version: 1,
        last_recomputed_at: timestamp,
    };
    let record = parse_resolution_record_key("addr:60").expect("test selector must parse");

    let answer = indexed_satisfying_record_answer(&row, Some(&inventory), &record)
        .expect("auto satisfaction must evaluate")
        .expect("an exact indexed success does not depend on inventory exhaustiveness");
    assert_eq!(answer.status, Status::Ok);
    assert!(answer.meta.is_none());
}

#[test]
fn product_record_reason_maps_storage_projection_reasons() {
    assert_eq!(
        product_record_reason("value_not_retained_in_normalized_events")
            .expect("known reason must map"),
        "value_not_retained"
    );
    assert_eq!(
        product_record_reason("record_family_not_supported_in_phase6_projection")
            .expect("known reason must map"),
        "record_family_not_supported"
    );
    assert_eq!(
        product_record_reason("resolver_family_pending").expect("product reason must pass"),
        "resolver_family_pending"
    );
}

#[test]
fn product_record_reason_rejects_unmapped_pipeline_vocabulary() {
    for reason in ["raw_log_missing_record_cache", "record_sidecar_missing"] {
        let error =
            product_record_reason(reason).expect_err("pipeline vocabulary must fail loudly");

        assert_eq!(error.code(), ErrorCode::InternalError);
    }
}
