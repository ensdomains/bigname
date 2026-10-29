//! The composed shapes `legacy_lease_shape` matches and what the fill writes into each, on
//! constructed registrations and lease events.
use bigname_storage::public_name_fields::{
    RegistrationStatus, ens_v1, has_registration_identity, registration_fields,
};

use super::*;

const NAME: &str = "migrated.eth";
const LEASE: &str = "00000000-0000-0000-0000-00000000a001";
const OLDER_LEASE: &str = "00000000-0000-0000-0000-00000000a002";
const HANDOFF_BLOCK: i64 = 120;
const HANDOFF_SECONDS: i64 = 1_700_000_120;
const GRANT_BLOCK: i64 = 130;
const GRANT_SECONDS: i64 = 1_700_000_130;
const RELEASE_BLOCK: i64 = 160;
const RELEASE_SECONDS: i64 = 1_700_000_160;
/// The time of the snapshot's block, which the fill dates a filled expiry's status at.
const SNAPSHOT_SECONDS: i64 = 1_700_000_170;

fn event(kind: &str, block_number: i64, resource_id: &str, expiry: Option<i64>) -> LeaseEvent {
    LeaseEvent {
        kind: kind.to_owned(),
        block_number,
        resource_id: resource_id.to_owned(),
        expiry,
    }
}

/// A registry-only registration as composed with no lease: the composed clock is set, the
/// lease fields are null.
fn registry_only_summary() -> Value {
    json!({
        "registration": {
            "authority_key": "registry-only:ethereum-mainnet:0x01",
            "authority_kind": "registry_only",
            "ens_v1_expiry": null,
            "expiry": "1900000000",
            "grace_ends_at": "1907776000",
            "identity_resource_id": null,
            "latest_event_kind": null,
            "lifecycle_status": "active",
            "registered_at": null,
            "registrant": null,
            "released_at": null,
            "resource_id": null,
            "status": "active"
        },
        "control": {
            "owner": "0x00000000000000000000000000000000000000a5",
            "registry_owner": "0x00000000000000000000000000000000000000a5"
        }
    })
}

fn ens_v1_provenance() -> Value {
    json!({"authority_selection": {
        "authority_arm": "ens_v1",
        "registry_handoff_block_number": HANDOFF_BLOCK
    }})
}

/// A lease's events newest first, as the normalizer stores them: a grant beside the first
/// renewal, and an ExpiryChanged beside each renewal.
fn renewed_lease() -> Vec<LeaseEvent> {
    vec![
        event("ExpiryChanged", 150, LEASE, Some(1_850_000_000)),
        event("RegistrationRenewed", 150, LEASE, Some(1_850_000_000)),
        event("ExpiryChanged", 140, LEASE, Some(1_820_000_000)),
        event("RegistrationRenewed", 140, LEASE, Some(1_820_000_000)),
        event("ExpiryChanged", 130, LEASE, Some(1_800_000_000)),
        event("RegistrationRenewed", 130, LEASE, Some(1_800_000_000)),
        event("RegistrationGranted", 130, LEASE, Some(1_800_000_000)),
    ]
}

fn times() -> BTreeMap<i64, i64> {
    BTreeMap::from([
        (HANDOFF_BLOCK, HANDOFF_SECONDS),
        (GRANT_BLOCK, GRANT_SECONDS),
        (RELEASE_BLOCK, RELEASE_SECONDS),
    ])
}

fn heal(summary: &mut Value, events: &[LeaseEvent]) -> bool {
    let registration = summary["registration"].as_object_mut().unwrap();
    heal_registration(registration, events, Some(HANDOFF_BLOCK), &times())
}

fn shape(name: &str, provenance: &Value, summary: &Value) -> Option<LegacyLeaseShape> {
    legacy_lease_shape("ens", name, provenance, summary)
}

#[test]
fn a_registry_only_registration_without_a_lease_is_the_unleased_shape() {
    assert_eq!(
        shape(NAME, &ens_v1_provenance(), &registry_only_summary()),
        Some(LegacyLeaseShape::Unleased)
    );
}

#[test]
fn a_name_outside_an_ens_v1_eth_second_level_registration_is_not_a_shape() {
    let summary = registry_only_summary();
    let provenance = ens_v1_provenance();
    assert_eq!(
        legacy_lease_shape("basenames", NAME, &provenance, &summary),
        None
    );
    for name in ["sub.migrated.eth", "migrated.xyz", "eth", ".eth"] {
        assert_eq!(shape(name, &provenance, &summary), None, "{name}");
    }
    let ens_v2 = json!({"authority_selection": {"authority_arm": "ens_v2"}});
    assert_eq!(shape(NAME, &ens_v2, &summary), None);
    assert_eq!(shape(NAME, &json!({}), &summary), None);
    assert_eq!(shape(NAME, &provenance, &json!({})), None);
    assert_eq!(
        shape(NAME, &provenance, &json!({"registration": null})),
        None
    );
}

#[test]
fn a_registration_that_carries_a_lease_or_another_authority_is_not_a_shape() {
    let provenance = ens_v1_provenance();
    let with = |field: &str, value: Value| {
        let mut summary = registry_only_summary();
        summary["registration"][field] = value;
        summary
    };
    for summary in [
        with("resource_id", json!(LEASE)),
        with("ens_v1_expiry", json!("1850000000")),
        with("authority_kind", json!("wrapper")),
        with("authority_kind", json!("registrar")),
    ] {
        assert_eq!(shape(NAME, &provenance, &summary), None, "{summary}");
    }
    // A released lease's tombstone keeps the lease as its resource and has no authority kind.
    let mut tombstone = with("authority_kind", Value::Null);
    tombstone["registration"]["resource_id"] = json!(LEASE);
    tombstone["registration"]["status"] = json!("released");
    assert_eq!(shape(NAME, &provenance, &tombstone), None);
}

#[test]
fn a_renewed_lease_serves_its_identity_expiry_and_handoff_registration_time() {
    let mut summary = registry_only_summary();
    assert!(heal(&mut summary, &renewed_lease()));
    let registration = &summary["registration"];
    assert_eq!(registration["resource_id"], json!(LEASE));
    assert_eq!(registration["identity_resource_id"], json!(LEASE));
    assert_eq!(registration["ens_v1_expiry"], json!("1850000000"));
    assert_eq!(
        registration["registered_at"],
        json!(HANDOFF_SECONDS.to_string())
    );
    assert_eq!(registration["latest_event_kind"], json!("ExpiryChanged"));
    // A composed expiry, grace end and status stand.
    assert_eq!(registration["expiry"], json!("1900000000"));
    assert_eq!(registration["grace_ends_at"], json!("1907776000"));
    assert_eq!(registration["status"], json!("active"));
    assert_eq!(registration["lifecycle_status"], json!("active"));
    assert_eq!(registration["released_at"], Value::Null);

    let fields = registration_fields("ens", &summary, true);
    assert_eq!(fields.status, RegistrationStatus::Active);
    assert_eq!(fields.registered_at.as_deref(), Some("1700000120"));
    assert!(has_registration_identity("ens", &summary, true));
    let ens_v1 = ens_v1(Some("ens_v1"), &summary).unwrap().unwrap();
    assert_eq!(ens_v1.expires_at, Some(Some("1850000000".to_owned())));
}

#[test]
fn without_a_handoff_block_the_lease_starts_at_its_first_event() {
    let mut summary = registry_only_summary();
    let registration = summary["registration"].as_object_mut().unwrap();
    assert!(heal_registration(
        registration,
        &renewed_lease(),
        None,
        &times()
    ));
    assert_eq!(
        registration["registered_at"],
        json!(GRANT_SECONDS.to_string())
    );
}

#[test]
fn a_lapsed_lease_is_released_with_its_final_expiry() {
    let mut summary = registry_only_summary();
    summary["registration"]["expiry"] = Value::Null;
    summary["registration"]["grace_ends_at"] = Value::Null;
    let events = vec![
        event(
            "RegistrationReleased",
            RELEASE_BLOCK,
            LEASE,
            Some(1_710_000_000),
        ),
        event("ExpiryChanged", 140, LEASE, Some(1_710_000_000)),
        event("RegistrationRenewed", 140, LEASE, Some(1_710_000_000)),
        event("RegistrationGranted", 130, LEASE, Some(1_705_000_000)),
    ];
    assert!(heal(&mut summary, &events));
    let registration = &summary["registration"];
    assert_eq!(registration["status"], json!("released"));
    assert_eq!(registration["lifecycle_status"], json!("released"));
    assert_eq!(
        registration["released_at"],
        json!(RELEASE_SECONDS.to_string())
    );
    assert_eq!(registration["ens_v1_expiry"], json!("1710000000"));
    assert_eq!(registration["expiry"], json!("1710000000"));
    assert_eq!(registration["grace_ends_at"], json!("1717776000"));
    assert_eq!(
        registration["registered_at"],
        json!(HANDOFF_SECONDS.to_string())
    );
    assert_eq!(registration["resource_id"], json!(LEASE));
    assert!(registration.get("lapsed_registration").is_none());

    let fields = registration_fields("ens", &summary, true);
    assert_eq!(fields.status, RegistrationStatus::Released);
    assert_eq!(fields.registered_at.as_deref(), Some("1700000120"));
}

#[test]
fn a_release_without_an_expiry_writes_no_lease_expiry() {
    let mut summary = registry_only_summary();
    let events = vec![
        event("RegistrationReleased", RELEASE_BLOCK, LEASE, None),
        event("RegistrationGranted", 130, LEASE, None),
    ];
    assert!(heal(&mut summary, &events));
    let registration = &summary["registration"];
    assert_eq!(registration["status"], json!("released"));
    assert_eq!(registration["ens_v1_expiry"], Value::Null);
    assert_eq!(registration["expiry"], json!("1900000000"));
    assert_eq!(registration["resource_id"], json!(LEASE));
}

#[test]
fn a_lease_expiry_at_the_signed_maximum_serves_the_exact_grace_end() {
    let mut summary = registry_only_summary();
    summary["registration"]["expiry"] = Value::Null;
    summary["registration"]["grace_ends_at"] = Value::Null;
    let events = vec![event("RegistrationGranted", 130, LEASE, Some(i64::MAX))];
    assert!(heal(&mut summary, &events));
    let registration = &summary["registration"];
    assert_eq!(registration["ens_v1_expiry"], json!(i64::MAX.to_string()));
    assert_eq!(registration["expiry"], json!(i64::MAX.to_string()));
    assert_eq!(registration["grace_ends_at"], json!("9223372036862551807"));
}

#[test]
fn a_lease_in_grace_keeps_the_composed_status() {
    let mut summary = registry_only_summary();
    summary["registration"]["expiry"] = json!("1690000000");
    summary["registration"]["grace_ends_at"] = json!("1697776000");
    summary["registration"]["status"] = json!("expired");
    summary["registration"]["lifecycle_status"] = json!("expired");
    let events = vec![event(
        "RegistrationGranted",
        130,
        LEASE,
        Some(1_690_000_000),
    )];
    assert!(heal(&mut summary, &events));
    let registration = &summary["registration"];
    assert_eq!(registration["status"], json!("expired"));
    assert_eq!(registration["lifecycle_status"], json!("expired"));
    assert_eq!(registration["released_at"], Value::Null);
    assert_eq!(registration["ens_v1_expiry"], json!("1690000000"));
    assert_eq!(registration["resource_id"], json!(LEASE));
    assert_eq!(
        registration_fields("ens", &summary, true).status,
        RegistrationStatus::Expired
    );
}

#[test]
fn an_unknown_block_time_leaves_registered_at_null_and_still_serves_the_lease() {
    let mut summary = registry_only_summary();
    let registration = summary["registration"].as_object_mut().unwrap();
    assert!(heal_registration(
        registration,
        &renewed_lease(),
        Some(HANDOFF_BLOCK),
        &BTreeMap::new()
    ));
    assert_eq!(registration["registered_at"], Value::Null);
    assert_eq!(registration["ens_v1_expiry"], json!("1850000000"));
    assert_eq!(registration["resource_id"], json!(LEASE));
}

#[test]
fn an_older_lease_does_not_date_or_release_the_latest_one() {
    let mut summary = registry_only_summary();
    let events = vec![
        event("RegistrationRenewed", 150, LEASE, Some(1_850_000_000)),
        event(
            "RegistrationReleased",
            140,
            OLDER_LEASE,
            Some(1_600_000_000),
        ),
        event("RegistrationGranted", 125, OLDER_LEASE, Some(1_600_000_000)),
    ];
    let times = BTreeMap::from([(125, 1_700_000_125), (150, 1_700_000_150)]);
    let registration = summary["registration"].as_object_mut().unwrap();
    assert!(heal_registration(registration, &events, None, &times));
    assert_eq!(registration["resource_id"], json!(LEASE));
    assert_eq!(registration["ens_v1_expiry"], json!("1850000000"));
    assert_eq!(registration["status"], json!("active"));
    assert_eq!(registration["released_at"], Value::Null);
    assert_eq!(registration["registered_at"], json!("1700000150"));
}

#[test]
fn an_ownerless_registration_without_an_authority_kind_is_the_unleased_shape() {
    let mut summary = registry_only_summary();
    for field in ["authority_kind", "authority_key", "expiry", "grace_ends_at"] {
        summary["registration"][field] = Value::Null;
    }
    summary["registration"]["status"] = json!("unregistered");
    summary["registration"]["lifecycle_status"] = json!("unregistered");
    let mut provenance = ens_v1_provenance();
    provenance["authority_selection"]["ownerless_registry"] = json!(true);
    assert_eq!(
        shape(NAME, &provenance, &summary),
        Some(LegacyLeaseShape::Unleased)
    );
    assert!(heal(&mut summary, &renewed_lease()));
    let registration = &summary["registration"];
    assert_eq!(registration["identity_resource_id"], json!(LEASE));
    assert_eq!(registration["ens_v1_expiry"], json!("1850000000"));
    assert_eq!(registration["expiry"], json!("1850000000"));
    assert_eq!(registration["grace_ends_at"], json!("1857776000"));
    // The heal writes a status only on a release, and an ownerless row has no public
    // authority, so it serves no `ens_v1` object.
    assert_eq!(registration["status"], json!("unregistered"));
    assert_eq!(
        registration_fields("ens", &summary, false).status,
        RegistrationStatus::Unregistered
    );
    assert!(ens_v1(None, &summary).unwrap().is_none());
}

#[test]
fn the_unleased_shape_does_not_read_the_ownerless_flag() {
    let mut summary = registry_only_summary();
    summary["registration"]["authority_kind"] = Value::Null;
    let provenance = ens_v1_provenance();
    assert!(
        provenance
            .pointer("/authority_selection/ownerless_registry")
            .is_none()
    );
    assert_eq!(
        shape(NAME, &provenance, &summary),
        Some(LegacyLeaseShape::Unleased)
    );
}

#[test]
fn an_undated_registrar_registration_is_dated_at_its_grant_block() {
    let mut summary = json!({
        "registration": {
            "authority_kind": "registrar",
            "resource_id": LEASE,
            "ens_v1_expiry": "1850000000",
            "expiry": "1850000000",
            "registered_at": null,
            "identity_resource_id": null,
            "registrant": "0x00000000000000000000000000000000000000a5",
            "status": "active",
            "lifecycle_status": "active"
        }
    });
    assert_eq!(
        shape(NAME, &ens_v1_provenance(), &summary),
        Some(LegacyLeaseShape::UndatedLease(LEASE.to_owned()))
    );
    let registration = summary["registration"].as_object_mut().unwrap();
    assert!(date_registration(
        registration,
        LEASE,
        Some(GRANT_BLOCK),
        &times()
    ));
    assert_eq!(
        registration["registered_at"],
        json!(GRANT_SECONDS.to_string())
    );
    assert_eq!(registration["identity_resource_id"], json!(LEASE));
    assert_eq!(registration["ens_v1_expiry"], json!("1850000000"));
    assert_eq!(registration["status"], json!("active"));
    let fields = registration_fields("ens", &summary, true);
    assert_eq!(fields.registered_at.as_deref(), Some("1700000130"));
    assert!(has_registration_identity("ens", &summary, true));
}

#[test]
fn an_undated_registration_without_a_grant_or_its_block_time_is_left_as_composed() {
    let mut summary = json!({"registration": {
        "authority_kind": "registrar",
        "resource_id": LEASE,
        "registered_at": null,
        "identity_resource_id": null
    }});
    let before = summary.clone();
    let registration = summary["registration"].as_object_mut().unwrap();
    assert!(!date_registration(registration, LEASE, None, &times()));
    assert!(!date_registration(
        registration,
        LEASE,
        Some(GRANT_BLOCK),
        &BTreeMap::new()
    ));
    assert_eq!(summary, before);
}

#[test]
fn a_dated_registrar_registration_is_not_a_shape() {
    let dated = json!({"registration": {
        "authority_kind": "registrar",
        "resource_id": LEASE,
        "registered_at": "1700000130"
    }});
    assert_eq!(shape(NAME, &ens_v1_provenance(), &dated), None);
    let no_lease = json!({"registration": {
        "authority_kind": "registrar",
        "resource_id": null,
        "registered_at": null
    }});
    assert_eq!(shape(NAME, &ens_v1_provenance(), &no_lease), None);
}

#[test]
fn no_registrar_events_leave_the_registration_as_composed() {
    let mut summary = registry_only_summary();
    let before = summary.clone();
    assert!(!heal(&mut summary, &[]));
    assert_eq!(summary, before);
}

/// A stored term that outlives the released lease, as an ENSv2 reservation extended past the
/// lease's grace does, keeps the composed status and its own dates. The lease is still the
/// registration's identity.
#[test]
fn a_released_lease_under_a_stored_term_that_outlives_it_keeps_the_composed_status() {
    let mut summary = registry_only_summary();
    let events = vec![
        event(
            "RegistrationReleased",
            RELEASE_BLOCK,
            LEASE,
            Some(1_690_000_000),
        ),
        event("RegistrationGranted", 130, LEASE, Some(1_690_000_000)),
    ];
    assert!(heal(&mut summary, &events));
    let registration = &summary["registration"];
    assert_eq!(registration["status"], json!("active"));
    assert_eq!(registration["lifecycle_status"], json!("active"));
    assert_eq!(registration["released_at"], Value::Null);
    assert_eq!(registration["expiry"], json!("1900000000"));
    assert_eq!(registration["grace_ends_at"], json!("1907776000"));
    assert_eq!(registration["ens_v1_expiry"], json!("1690000000"));
    assert_eq!(registration["resource_id"], json!(LEASE));
    assert_eq!(
        registration_fields("ens", &summary, true).status,
        RegistrationStatus::Active
    );
}

/// A stored term that ended before the snapshot does not hold the released lease open.
#[test]
fn a_released_lease_under_a_stored_term_that_ended_is_released() {
    let mut summary = registry_only_summary();
    summary["registration"]["expiry"] = json!("1600000000");
    summary["registration"]["grace_ends_at"] = json!("1607776000");
    let events = vec![
        event(
            "RegistrationReleased",
            RELEASE_BLOCK,
            LEASE,
            Some(1_690_000_000),
        ),
        event("RegistrationGranted", 130, LEASE, Some(1_690_000_000)),
    ];
    assert!(heal(&mut summary, &events));
    let registration = &summary["registration"];
    assert_eq!(registration["status"], json!("released"));
    assert_eq!(registration["lifecycle_status"], json!("released"));
    assert_eq!(
        registration["released_at"],
        json!(RELEASE_SECONDS.to_string())
    );
    assert_eq!(registration["expiry"], json!("1600000000"));
}

/// When the fill supplies the expiry, the status is the lease's at the snapshot's block time: in
/// grace once the expiry has passed.
#[test]
fn a_filled_expiry_in_grace_at_the_snapshot_serves_expired() {
    let mut summary = registry_only_summary();
    summary["registration"]["expiry"] = Value::Null;
    summary["registration"]["grace_ends_at"] = Value::Null;
    let expiry = SNAPSHOT_SECONDS - 1;
    let events = vec![event("RegistrationGranted", 130, LEASE, Some(expiry))];
    assert!(heal(&mut summary, &events));
    let registration = &summary["registration"];
    assert_eq!(registration["expiry"], json!(expiry.to_string()));
    assert_eq!(registration["lifecycle_status"], json!("expired"));
    assert_eq!(registration["released_at"], Value::Null);
    assert_eq!(
        registration_fields("ens", &summary, true).status,
        RegistrationStatus::Expired
    );
}

/// A registration whose stored lifecycle is `unregistered` keeps it when the fill supplies a live
/// lease's expiry, whatever its control status reads.
#[test]
fn a_stored_unregistered_lifecycle_keeps_its_status_when_the_fill_supplies_the_expiry() {
    let mut summary = registry_only_summary();
    for field in ["authority_kind", "status", "expiry", "grace_ends_at"] {
        summary["registration"][field] = Value::Null;
    }
    summary["registration"]["lifecycle_status"] = json!("unregistered");
    let events = vec![event(
        "RegistrationGranted",
        130,
        LEASE,
        Some(1_900_000_000),
    )];
    assert!(heal(&mut summary, &events));
    let registration = &summary["registration"];
    assert_eq!(registration["expiry"], json!("1900000000"));
    assert_eq!(registration["lifecycle_status"], json!("unregistered"));
    assert_eq!(
        registration_fields("ens", &summary, true).status,
        RegistrationStatus::Unregistered
    );
}

/// An ENSv2 allocation keeps its own schedule when the fill supplies a lease expiry.
#[test]
fn an_ens_v2_allocation_keeps_its_status_when_the_fill_supplies_the_expiry() {
    let mut summary = registry_only_summary();
    summary["registration"]["expiry"] = Value::Null;
    summary["registration"]["grace_ends_at"] = Value::Null;
    summary["registration"]["canonical_allocation"] = json!(true);
    let events = vec![event(
        "RegistrationGranted",
        130,
        LEASE,
        Some(SNAPSHOT_SECONDS - 1),
    )];
    assert!(heal(&mut summary, &events));
    assert_eq!(summary["registration"]["lifecycle_status"], json!("active"));
}
