use bigname_storage::{NameCurrentRow, RecordInventoryCurrentRow, SurfaceBindingKind};
use serde_json::{Value, json};
use sqlx::types::time::OffsetDateTime;

use super::*;
use crate::v2::{ErrorCode, support::parse_resolution_record_key};

#[test]
fn auto_rejects_derived_answer_from_nonauthoritative_inventory() {
    let timestamp =
        OffsetDateTime::from_unix_timestamp(1_717_171_719).expect("test timestamp must be valid");
    let row = current_name_row(timestamp);
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
        indexed_satisfying_record_answer(&row, Some(&inventory), &record, false)
            .expect("auto satisfaction must evaluate")
            .is_none(),
        "auto must verify when projected inventory authority is incomplete"
    );
}

#[test]
fn unsupported_inventory_refuses_exact_success_and_leaves_auto_unsatisfied() {
    let timestamp =
        OffsetDateTime::from_unix_timestamp(1_717_171_719).expect("test timestamp must be valid");
    let row = current_name_row(timestamp);
    let inventory = exact_success_inventory();
    let record = parse_resolution_record_key("addr:60").expect("test selector must parse");

    let indexed = indexed_record_answer(Some(&inventory), &record)
        .expect("explicit indexed evaluation must complete");
    assert_eq!(indexed.status, Status::Unsupported);
    assert_eq!(indexed.value, None);
    assert_eq!(
        indexed.unsupported_reason.as_deref(),
        Some("resolver_implementation_unknown"),
        "the row's own reason is the public reason"
    );
    assert!(
        indexed_satisfying_record_answer(&row, Some(&inventory), &record, false)
            .expect("auto satisfaction must evaluate")
            .is_none(),
        "a retained value on an unsupported row does not satisfy auto"
    );
}

#[test]
fn unsupported_inventory_reason_maps_through_the_projected_row_vocabulary() {
    let record = parse_resolution_record_key("addr:60").expect("test selector must parse");
    for (row_reason, public_reason) in [
        // Pipeline wording cannot cross the serving boundary, and must not fail the request.
        (
            json!("coverage_incomplete"),
            "unsupported_reason_unrecognized",
        ),
        // A row naming no reason reports the generic non-authoritative reason.
        (Value::Null, "indexed_record_inventory_not_authoritative"),
    ] {
        let mut inventory = exact_success_inventory();
        inventory.coverage = json!({"status":"unsupported","unsupported_reason": row_reason});

        let indexed = indexed_record_answer(Some(&inventory), &record)
            .expect("an unsupported inventory reason must not fail the request");
        assert_eq!(indexed.status, Status::Unsupported, "{public_reason}");
        assert_eq!(indexed.unsupported_reason.as_deref(), Some(public_reason));
    }
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

#[test]
fn auto_null_resolver_falls_through_only_for_the_ens_mainnet_discovery_shape() {
    let record = parse_resolution_record_key("addr:60").expect("test selector must parse");
    let row = null_resolver_discovery_row();
    assert!(
        indexed_satisfying_record_answer(&row, Some(&exact_success_inventory()), &record, true)
            .expect("candidate must evaluate")
            .is_none()
    );
    let (source, records) = build_auto_name_records(
        &row,
        Some(&exact_success_inventory()),
        std::slice::from_ref(&record),
        Some(VerifiedRecordLookup::NotSupported),
        false,
        true,
    )
    .expect("unavailable execution must stay explicit");
    assert_eq!(source, Source::Verified);
    assert_eq!(
        records.records.expect("requested records")["addr:60"].status,
        Status::Unsupported
    );

    let mut rejected = Vec::new();
    let mut basenames = row.clone();
    basenames.namespace = "basenames".to_owned();
    rejected.push(basenames);
    let mut non_ethereum = row.clone();
    non_ethereum.chain_positions["ethereum"]["chain_id"] = json!("base-mainnet");
    rejected.push(non_ethereum);
    let mut alias = row.clone();
    alias.declared_summary["topology"]["alias"] = json!({
        "final_target": {"logical_name_id":"ens:target"},
        "hops": [{"logical_name_id":"ens:target"}]
    });
    rejected.push(alias);
    let mut wildcard = row.clone();
    wildcard.declared_summary["topology"]["wildcard"] = json!({
        "source": {"logical_name_id":"ens:ancestor"},
        "matched_labels": ["alice"]
    });
    rejected.push(wildcard);
    let mut transport = row.clone();
    transport.declared_summary["topology"]["transport"] = json!({
        "source_chain_id": "ethereum-mainnet",
        "target_chain_id": "ethereum-mainnet",
        "contract_address": "0x1000000000000000000000000000000000000001",
        "latest_event_kind": "ResolverChanged"
    });
    rejected.push(transport);
    let mut subregistry = row.clone();
    subregistry.declared_summary["topology"]["subregistry_path"] = json!([{}]);
    rejected.push(subregistry);
    let mut malformed = row.clone();
    malformed.declared_summary["topology"]["resolver_path"] = json!([]);
    rejected.push(malformed);

    for rejected_row in rejected {
        let answer = indexed_satisfying_record_answer(&rejected_row, None, &record, true)
            .expect("rejected shape must retain the terminal miss")
            .expect("rejected shape must not fall through");
        assert_eq!(answer.status, Status::NotFound);
    }
}

#[test]
fn sepolia_null_resolver_admission_matches_executed_discovery_and_rejects_route_changes() {
    use super::super::{ZERO_ADDRESS, ensure_executed_route_matches_admission};

    let record = parse_resolution_record_key("addr:60").expect("test selector must parse");
    let mut row = null_resolver_discovery_row();
    row.chain_positions = json!({"ethereum-sepolia":{"chain_id":"ethereum-sepolia"}});
    row.provenance = json!({"authority_selection":{"authority_arm":"ens_v2"}});
    row.declared_summary["topology"] = json!({
        "registry_path": [], "subregistry_path": [],
        "resolver_path": [{"logical_name_id":row.logical_name_id,
            "chain_id":"ethereum-sepolia", "address":null}],
        "wildcard":{"source":null,"matched_labels":[]},
        "alias":{"final_target":null,"hops":[]}, "version_boundaries":{},
        "transport":{"source_chain_id":null,"target_chain_id":null,
            "contract_address":null,"latest_event_kind":null}
    });
    for candidate in [row.clone(), {
        let mut absent = row.clone();
        absent
            .declared_summary
            .as_object_mut()
            .unwrap()
            .remove("topology");
        absent
    }] {
        let admitted = ens_universal_resolver_discovery_candidate(&candidate);
        assert!(admitted, "Sepolia direct-null shape must agree with Lookup");
        assert!(ensure_executed_route_matches_admission(ZERO_ADDRESS, admitted).is_ok());
        assert!(
            ensure_executed_route_matches_admission(
                "0x1000000000000000000000000000000000000001",
                admitted
            )
            .is_err(),
            "a discovery-to-direct route change must remain stale"
        );
        assert!(
            indexed_satisfying_record_answer(&candidate, None, &record, true)
                .expect("candidate must evaluate")
                .is_none()
        );
        let (_, unavailable) = build_auto_name_records(
            &candidate,
            None,
            std::slice::from_ref(&record),
            Some(VerifiedRecordLookup::NotSupported),
            false,
            true,
        )
        .expect("unadmitted execution must remain explicit");
        assert_eq!(
            unavailable.records.unwrap()["addr:60"].status,
            Status::Unsupported
        );
    }

    let mut rejected = Vec::new();
    let mut wrong_hop = row.clone();
    wrong_hop.declared_summary["topology"]["resolver_path"][0]["chain_id"] =
        json!("ethereum-mainnet");
    rejected.push(wrong_hop);
    let mut wrong_slot = row.clone();
    wrong_slot.chain_positions = json!({"ethereum":{"chain_id":"ethereum-sepolia"}});
    rejected.push(wrong_slot);
    let mut alias = row.clone();
    alias.declared_summary["topology"]["alias"] = json!({
        "final_target":{"logical_name_id":"ens:other"},"hops":[{"logical_name_id":"ens:other"}]
    });
    rejected.push(alias);
    let mut malformed = row.clone();
    malformed.declared_summary["topology"]["resolver_path"] = json!([]);
    rejected.push(malformed);
    let mut unsupported = row;
    unsupported.declared_summary["resolver"]["status"] = json!("unsupported");
    rejected.push(unsupported);
    for candidate in rejected {
        assert!(!ens_universal_resolver_discovery_candidate(&candidate));
        assert!(ensure_executed_route_matches_admission(ZERO_ADDRESS, false).is_err());
    }
}

#[test]
fn null_resolver_discovery_keeps_avatar_stale_without_a_record_boundary() {
    let record = parse_resolution_record_key("avatar").expect("test selector must parse");
    let (_, records) = build_auto_name_records(
        &null_resolver_discovery_row(),
        None,
        std::slice::from_ref(&record),
        Some(VerifiedRecordLookup::Stale(
            "selected_block_changed".to_owned(),
        )),
        false,
        true,
    )
    .expect("stale discovery answer must build");

    assert_eq!(
        records.records.expect("requested record map")["avatar"].status,
        Status::Stale
    );
}

fn null_resolver_discovery_row() -> NameCurrentRow {
    let timestamp =
        OffsetDateTime::from_unix_timestamp(1_717_171_719).expect("test timestamp must be valid");
    let logical_name_id = "ens:0x787192fc5378cc32aa956ddfdedbf26b24e8d78e40109add0eea2c1a012c3dec";
    let mut row = current_name_row(timestamp);
    row.logical_name_id = logical_name_id.to_owned();
    row.namehash = logical_name_id.trim_start_matches("ens:").to_owned();
    row.declared_summary["resolver"] = json!({"chain_id": null, "address": null});
    row.chain_positions = json!({"ethereum":{"chain_id":"ethereum-mainnet"}});
    row
}

fn current_name_row(timestamp: OffsetDateTime) -> NameCurrentRow {
    NameCurrentRow {
        logical_name_id: "ens:alice.eth".to_owned(),
        namespace: "ens".to_owned(),
        canonical_display_name: "alice.eth".to_owned(),
        normalized_name: "alice.eth".to_owned(),
        namehash: "0xalice".to_owned(),
        surface_binding_id: Some(
            "00000000-0000-0000-0000-000000000607"
                .parse()
                .expect("test surface binding id"),
        ),
        resource_id: Some(
            "00000000-0000-0000-0000-000000000606"
                .parse()
                .expect("test resource id"),
        ),
        serving_resource_id: None,
        token_lineage_id: None,
        binding_kind: Some(SurfaceBindingKind::DeclaredRegistryPath),
        declared_summary: json!({
            "registration": {
                "status": "active",
                "authority_kind": "registrar"
            }
        }),
        provenance: json!({}),
        coverage: json!({"status":"projected"}),
        chain_positions: json!({}),
        canonicality_summary: json!({}),
        manifest_version: 1,
        last_recomputed_at: timestamp,
    }
}

fn exact_success_inventory() -> RecordInventoryCurrentRow {
    RecordInventoryCurrentRow {
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
            "unsupported_reason":"resolver_implementation_unknown"
        }),
        chain_positions: json!({}),
        canonicality_summary: json!({}),
        manifest_version: 1,
        last_recomputed_at: OffsetDateTime::from_unix_timestamp(1_717_171_719)
            .expect("test timestamp must be valid"),
    }
}
