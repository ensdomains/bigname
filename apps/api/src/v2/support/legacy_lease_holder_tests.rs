//! What the fill serves as `owner`, `manager`, `status` and `authority` once it has healed a
//! registration, on constructed rows: a registry-only row as stored with no lease, and an
//! ownerless row as stored, whose registry owner is zero.
use bigname_storage::{
    name_current_public_authority,
    public_name_fields::{RegistrationStatus, registration_fields},
};
use serde_json::{Value, json};

use super::{LeaseEvent, is_current_lease, is_eth_second_level, serve_lease_holder};

const REGISTRY_OWNER: &str = "0x00000000000000000000000000000000000000a5";
const HOLDER: &str = "0x00000000000000000000000000000000000000b7";
const ZERO: &str = "0x0000000000000000000000000000000000000000";
const LEASE_EXPIRY: i64 = 1_800_000_000;
const RELEASED_AT: &str = "1700000160";
const GRACE: i64 = 90 * 24 * 60 * 60;

/// A registry-only registration the fill healed with a live lease.
fn healed_registry_only() -> Value {
    json!({
        "registration": {
            "authority_kind": "registry_only",
            "resource_id": "00000000-0000-0000-0000-00000000a001",
            "ens_v1_expiry": LEASE_EXPIRY.to_string(),
            "expiry": LEASE_EXPIRY.to_string(),
            "lifecycle_status": "active",
            "registrant": null,
            "released_at": null,
            "status": "active"
        },
        "control": {"owner": REGISTRY_OWNER, "registry_owner": REGISTRY_OWNER}
    })
}

/// An ownerless registration the fill healed with a lease: stored `unregistered`, with no
/// control but its status.
fn healed_ownerless() -> Value {
    json!({
        "registration": {
            "authority_kind": null,
            "resource_id": "00000000-0000-0000-0000-00000000a001",
            "ens_v1_expiry": LEASE_EXPIRY.to_string(),
            "expiry": LEASE_EXPIRY.to_string(),
            "lifecycle_status": "unregistered",
            "registrant": null,
            "released_at": null,
            "status": "unregistered"
        },
        "control": {"status": "unregistered"}
    })
}

fn provenance(ownerless: bool) -> Value {
    json!({"authority_selection": {
        "authority_arm": "ens_v1", "registry_generation": "current",
        "ownerless_registry": ownerless
    }})
}

/// The served `owner`, `manager` and `status`.
fn served(summary: &Value) -> (Option<String>, Option<String>, RegistrationStatus) {
    let fields = registration_fields("ens", summary, true);
    (fields.owner, fields.manager, fields.status)
}

fn released_by_its_lease(summary: &mut Value) {
    summary["registration"]["latest_event_kind"] = json!("RegistrationReleased");
    summary["registration"]["status"] = json!("released");
    summary["registration"]["lifecycle_status"] = json!("released");
    summary["registration"]["released_at"] = json!(RELEASED_AT);
}

#[test]
fn a_live_lease_is_owned_by_its_holder_and_managed_by_the_registry_owner() {
    let mut summary = healed_registry_only();
    let mut provenance = provenance(false);
    serve_lease_holder(&mut summary, &mut provenance, Some(HOLDER), Some(0));
    assert_eq!(
        served(&summary),
        (
            Some(HOLDER.to_owned()),
            Some(REGISTRY_OWNER.to_owned()),
            RegistrationStatus::Active
        )
    );
    assert_eq!(summary["registration"]["registrant"], json!(HOLDER));
    assert_eq!(summary["control"]["registrant"], json!(HOLDER));
    assert!(summary["registration"].get("lapsed_registration").is_none());
    assert_eq!(provenance, self::provenance(false));
}

#[test]
fn a_lease_with_no_holder_keeps_the_composed_owner() {
    let mut summary = healed_registry_only();
    let composed = summary.clone();
    serve_lease_holder(&mut summary, &mut provenance(false), None, Some(0));
    assert_eq!(summary, composed);
}

#[test]
fn a_lease_the_fill_serves_released_has_no_owner_or_manager_and_a_lapsed_holder() {
    let mut summary = healed_registry_only();
    released_by_its_lease(&mut summary);
    serve_lease_holder(&mut summary, &mut provenance(false), Some(HOLDER), Some(0));
    assert_eq!(served(&summary), (None, None, RegistrationStatus::Released));
    assert_eq!(
        summary["registration"]["lapsed_registration"],
        json!({"owner": HOLDER, "released_at": RELEASED_AT, "release_kind": "expired"})
    );
    assert_eq!(summary["registration"]["registrant"], json!(HOLDER));
}

#[test]
fn a_release_with_no_holder_has_a_lapsed_registration_with_no_owner() {
    let mut summary = healed_registry_only();
    released_by_its_lease(&mut summary);
    serve_lease_holder(&mut summary, &mut provenance(false), None, Some(0));
    assert_eq!(served(&summary), (None, None, RegistrationStatus::Released));
    assert_eq!(
        summary["registration"]["lapsed_registration"],
        json!({"owner": null, "released_at": RELEASED_AT, "release_kind": "expired"})
    );
}

/// Released by the lease's schedule, or by a stored term that lapsed, the registration's own
/// status, which a retained registry pointer's serving is keyed on, is released too.
#[test]
fn a_row_released_by_its_lifecycle_alone_has_no_owner_or_manager() {
    let mut summary = healed_registry_only();
    summary["registration"]["lifecycle_status"] = json!("released");
    serve_lease_holder(&mut summary, &mut provenance(false), Some(HOLDER), Some(0));
    assert_eq!(served(&summary), (None, None, RegistrationStatus::Released));
    assert!(summary["registration"].get("lapsed_registration").is_none());
    assert_eq!(summary["registration"]["registrant"], json!(HOLDER));
    assert_eq!(summary["registration"]["status"], json!("released"));
    assert!(bigname_storage::public_name_fields::registration_released(
        &summary
    ));
}

#[test]
fn a_zero_registry_owner_manages_nothing() {
    let mut summary = healed_registry_only();
    summary["control"]["registry_owner"] = json!(ZERO);
    serve_lease_holder(&mut summary, &mut provenance(false), Some(HOLDER), Some(0));
    assert_eq!(
        served(&summary),
        (Some(HOLDER.to_owned()), None, RegistrationStatus::Active)
    );
}

/// An ownerless row takes the lease's status at the snapshot's block time, its holder as owner,
/// no manager, and the `authority` of its arm.
#[test]
fn an_ownerless_row_serves_its_lease_holder_status_and_authority() {
    for (clock, status) in [
        (LEASE_EXPIRY - 1, RegistrationStatus::Active),
        (LEASE_EXPIRY + GRACE, RegistrationStatus::Expired),
    ] {
        let mut summary = healed_ownerless();
        let mut provenance = provenance(true);
        assert_eq!(name_current_public_authority(&provenance), None);
        serve_lease_holder(&mut summary, &mut provenance, Some(HOLDER), Some(clock));
        assert_eq!(
            served(&summary),
            (Some(HOLDER.to_owned()), None, status),
            "{clock}"
        );
        assert_eq!(name_current_public_authority(&provenance), Some("ens_v1"));
        assert_eq!(summary["control"]["registry_owner"], Value::Null);
        assert_eq!(summary["control"]["owner"], json!(HOLDER));
    }
}

#[test]
fn an_ownerless_row_past_its_lease_grace_serves_released_with_no_owner() {
    let mut summary = healed_ownerless();
    let mut provenance = provenance(true);
    serve_lease_holder(
        &mut summary,
        &mut provenance,
        Some(HOLDER),
        Some(LEASE_EXPIRY + GRACE + 1),
    );
    assert_eq!(served(&summary), (None, None, RegistrationStatus::Released));
    assert_eq!(name_current_public_authority(&provenance), Some("ens_v1"));
}

#[test]
fn an_ownerless_row_whose_lease_the_fill_released_keeps_the_release() {
    let mut summary = healed_ownerless();
    released_by_its_lease(&mut summary);
    let mut provenance = provenance(true);
    // At a clock before the lease's expiry, the release the fill serves still stands.
    serve_lease_holder(&mut summary, &mut provenance, Some(HOLDER), Some(0));
    assert_eq!(served(&summary), (None, None, RegistrationStatus::Released));
    assert_eq!(
        summary["registration"]["lapsed_registration"]["owner"],
        json!(HOLDER)
    );
    assert_eq!(name_current_public_authority(&provenance), Some("ens_v1"));
}

#[test]
fn an_ownerless_row_without_a_known_clock_stays_as_composed() {
    let mut summary = healed_ownerless();
    let composed = summary.clone();
    let mut provenance = provenance(true);
    serve_lease_holder(&mut summary, &mut provenance, Some(HOLDER), None);
    assert_eq!(summary, composed);
    assert_eq!(provenance, self::provenance(true));
}

/// A lease released under a stored term that outlives the release is served by its stored
/// term, so the released lease's holder is not its owner (scope row 8b, not in this fill).
#[test]
fn a_release_under_a_live_stored_term_keeps_the_composed_control() {
    let mut summary = healed_registry_only();
    summary["registration"]["latest_event_kind"] = json!("RegistrationReleased");
    summary["registration"]["released_at"] = json!(RELEASED_AT);
    let composed = summary.clone();
    serve_lease_holder(&mut summary, &mut provenance(false), Some(HOLDER), Some(0));
    assert_eq!(summary, composed);
    assert_eq!(
        served(&summary),
        (
            Some(REGISTRY_OWNER.to_owned()),
            Some(REGISTRY_OWNER.to_owned()),
            RegistrationStatus::Active
        )
    );
}

#[test]
fn a_eth_second_level_name_is_recognized_by_its_text_or_its_label_hashes() {
    let eth = "[4f5b812789fc606be1b3b16908db13fc7a9adf7ca72641f84d75b47069d3d7f0]";
    let label = format!("[{}]", "ab".repeat(32));
    for name in [
        "migrated.eth".to_owned(),
        format!("{label}.eth"),
        format!("{label}.{eth}"),
        format!("migrated.{eth}"),
    ] {
        assert!(is_eth_second_level(&name), "{name}");
    }
    for name in [
        "eth".to_owned(),
        ".eth".to_owned(),
        eth.to_owned(),
        "sub.migrated.eth".to_owned(),
        format!("sub.{label}.{eth}"),
        "migrated.xyz".to_owned(),
        format!("{label}.[{}]", "cd".repeat(32)),
    ] {
        assert!(!is_eth_second_level(&name), "{name}");
    }
}

fn lease_event(block_number: i64, resource_id: &str, implies_lease: bool) -> LeaseEvent {
    LeaseEvent {
        kind: "RegistrationRenewed".to_owned(),
        block_number,
        resource_id: resource_id.to_owned(),
        expiry: Some(LEASE_EXPIRY),
        implies_lease,
    }
}

/// The fill serves a holder only for the lease the composer's implied lease reads: the latest
/// non-derived registrar row at or after the first current-registry record.
#[test]
fn only_the_implied_lease_is_current() {
    const HANDOFF: Option<i64> = Some(120);
    let cases: [(&str, Vec<LeaseEvent>, Option<i64>, bool); 7] = [
        (
            "since the handoff",
            vec![lease_event(122, "a", true)],
            HANDOFF,
            true,
        ),
        (
            "at the handoff",
            vec![lease_event(120, "a", true)],
            HANDOFF,
            true,
        ),
        (
            "before the handoff",
            vec![lease_event(119, "a", true)],
            HANDOFF,
            false,
        ),
        ("no handoff", vec![lease_event(119, "a", true)], None, true),
        (
            "derived latest row on the same lease",
            vec![lease_event(125, "a", false), lease_event(122, "a", true)],
            HANDOFF,
            true,
        ),
        (
            "derived latest row on another lease",
            vec![lease_event(125, "b", false), lease_event(122, "a", true)],
            HANDOFF,
            false,
        ),
        (
            "only derived rows",
            vec![lease_event(125, "a", false)],
            HANDOFF,
            false,
        ),
    ];
    for (case, events, handoff, current) in cases {
        assert_eq!(is_current_lease(&events, handoff), current, "{case}");
    }
    assert!(!is_current_lease(&[], None));
}
