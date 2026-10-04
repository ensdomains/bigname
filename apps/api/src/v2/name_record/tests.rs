use serde_json::json;

use super::*;
use crate::v2::vocab::WrapperState;

#[test]
fn registration_status_classifier_covers_authority_kind_domain() {
    let active = json!({
        "status": "active",
        "authority_kind": "registrar",
        "released_at": null,
        "expiry": "2000-01-01T00:00:00Z"
    });
    assert_eq!(
        classify_registration_status("ens", Some(&active), Some("0xabc"), true),
        RegistrationStatus::Active
    );
    assert_eq!(
        classify_registration_status("basenames", Some(&active), Some("0xabc"), true),
        RegistrationStatus::Active
    );

    let registered = json!({
        "status": "active",
        "authority_kind": "registry_only",
        "released_at": null
    });
    assert_eq!(
        classify_registration_status("ens", Some(&registered), Some("0xabc"), true),
        RegistrationStatus::Registered
    );

    let ens_v2_registered = json!({
        "status": "active",
        "authority_kind": "ens_v2_registry",
        "released_at": null
    });
    assert_eq!(
        classify_registration_status("ens", Some(&ens_v2_registered), Some("0xabc"), true),
        RegistrationStatus::Registered
    );

    let wrapped = json!({
        "status": "active",
        "authority_kind": "wrapper",
        "released_at": null
    });
    assert_eq!(
        classify_registration_status("ens", Some(&wrapped), Some("0xabc"), true),
        RegistrationStatus::Wrapped
    );
    assert_eq!(
        classify_registration_status("basenames", Some(&wrapped), Some("0xabc"), true),
        RegistrationStatus::Unregistered
    );

    let released = json!({
        "status": "released",
        "authority_kind": "registrar",
        "released_at": "2026-06-14T00:00:00Z"
    });
    assert_eq!(
        classify_registration_status("ens", Some(&released), Some("0xabc"), true),
        RegistrationStatus::Released
    );

    let unregistered = json!({
        "status": "active",
        "authority_kind": "unknown_authority",
        "released_at": null
    });
    assert_eq!(
        classify_registration_status("ens", Some(&active), Some("0xabc"), false),
        RegistrationStatus::Unregistered
    );
    assert_eq!(
        classify_registration_status("ens", Some(&unregistered), Some("0xabc"), true),
        RegistrationStatus::Unregistered
    );
}

#[test]
fn resolver_omits_unknown_chain_id_instead_of_guessing_mainnet() {
    let missing_chain = json!({
        "resolver": {
            "address": "0x0000000000000000000000000000000000000abc"
        }
    });
    assert_eq!(resolver(&missing_chain), None);

    let unknown_chain = json!({
        "resolver": {
            "chain_id": "unknown-mainnet",
            "address": "0x0000000000000000000000000000000000000abc"
        }
    });
    assert_eq!(resolver(&unknown_chain), None);
}

#[test]
fn wrapper_metadata_is_atomic_and_validates_named_fuses() {
    let summary = json!({
        "wrapper_state": "locked",
        "wrapper_fuses": {
            "fuses": 196_609,
            "cannot_unwrap": true,
            "cannot_burn_fuses": false,
            "cannot_transfer": false,
            "cannot_set_resolver": false,
            "cannot_set_ttl": false,
            "cannot_create_subdomain": false,
            "cannot_approve": false,
            "parent_cannot_control": true,
            "is_dot_eth": true,
            "can_extend_expiry": false
        }
    });
    let (state, fuses) = wrapper_metadata(&summary)
        .expect("wrapper metadata must parse")
        .expect("valid wrapper summary");
    assert_eq!(state, WrapperState::Locked);
    assert_eq!(fuses.fuses, 196_609);
    assert!(fuses.cannot_unwrap);
    assert!(fuses.parent_cannot_control);
    assert!(fuses.is_dot_eth);

    assert!(wrapper_metadata(&json!({"wrapper_state": "locked"})).is_err());
    assert!(
        wrapper_metadata(&json!({
            "wrapper_state": "unknown",
            "wrapper_fuses": summary["wrapper_fuses"]
        }))
        .is_err()
    );
    assert!(
        wrapper_metadata(&json!({
            "wrapper_state": "wrapped",
            "wrapper_fuses": summary["wrapper_fuses"]
        }))
        .is_err()
    );
    let mut inconsistent = summary;
    inconsistent["wrapper_fuses"]["cannot_unwrap"] = json!(false);
    assert!(wrapper_metadata(&inconsistent).is_err());
}

#[test]
fn wrapper_metadata_rejects_wrapped_state_with_cannot_transfer() {
    let summary = wrapper_summary("wrapped", 4);

    assert!(wrapper_metadata(&summary).is_err());
}

#[test]
fn wrapper_metadata_rejects_emancipated_state_with_low_fuse_without_cannot_unwrap() {
    let summary = wrapper_summary("emancipated", (1 << 16) | 2);

    assert!(wrapper_metadata(&summary).is_err());
}

#[test]
fn wrapper_metadata_rejects_reserved_low_fuse_without_locked_pair() {
    let summary = wrapper_summary("wrapped", 0x8000);

    assert!(wrapper_metadata(&summary).is_err());
}

#[test]
fn wrapper_metadata_accepts_parent_controlled_high_fuse_without_locked_pair() {
    let summary = wrapper_summary("wrapped", 1 << 18);

    let (state, fuses) = wrapper_metadata(&summary)
        .expect("parent-controlled fuse metadata must parse")
        .expect("wrapped summary must remain present");
    assert_eq!(state, WrapperState::Wrapped);
    assert_eq!(fuses.fuses, 1 << 18);
    assert!(fuses.can_extend_expiry);
}

#[test]
fn wrapper_metadata_rejects_is_dot_eth_without_parent_cannot_control() {
    let summary = wrapper_summary("wrapped", 1 << 17);

    assert!(wrapper_metadata(&summary).is_err());
}

#[test]
fn wrapper_metadata_accepts_emancipated_is_dot_eth_with_parent_cannot_control() {
    let summary = wrapper_summary("emancipated", (1 << 16) | (1 << 17));

    let (state, fuses) = wrapper_metadata(&summary)
        .expect("emancipated .eth metadata must parse")
        .expect("emancipated .eth summary must remain present");
    assert_eq!(state, WrapperState::Emancipated);
    assert!(fuses.parent_cannot_control);
    assert!(fuses.is_dot_eth);
}

#[test]
fn wrapper_metadata_accepts_locked_is_dot_eth_with_parent_cannot_control() {
    let summary = wrapper_summary("locked", 1 | (1 << 16) | (1 << 17));

    let (state, fuses) = wrapper_metadata(&summary)
        .expect("locked .eth metadata must parse")
        .expect("locked .eth summary must remain present");
    assert_eq!(state, WrapperState::Locked);
    assert!(fuses.cannot_unwrap);
    assert!(fuses.parent_cannot_control);
    assert!(fuses.is_dot_eth);
}

#[test]
fn ens_v1_object_follows_authority_and_carries_the_lease_expiry() {
    let mut summary = wrapper_summary("emancipated", (1 << 16) | (1 << 17));
    summary["registration"] = json!({"expiry": "1803965433", "ens_v1_expiry": "1798608633"});
    summary["wrapper_expiry_seconds"] = json!(1_806_384_633_u64);
    let object = |authority| {
        ens_v1(authority, &summary)
            .expect("valid wrapper summary")
            .map(|object| serde_json::to_value(object).expect("ens_v1 serializes"))
    };
    for authority in [Authority::EnsV1, Authority::EnsV0] {
        let object = object(Some(authority)).expect("ENSv1 authority serves ens_v1");
        assert_eq!(object["expires_at"], json!("1798608633"));
        assert_eq!(object["wrapper_state"], json!("emancipated"));
        assert_eq!(
            object["wrapper_fuses"]["parent_cannot_control"],
            json!(true)
        );
        assert_eq!(object["wrapper_expires_at"], json!("1806384633"));
        assert!(object.get("wrapper_expires_at_reason").is_none());
    }
    assert_eq!(object(Some(Authority::EnsV2)), None);
    assert_eq!(object(None), None);

    // No lease: the key is present with null; no wrapper state: the wrapper keys are omitted.
    let object = serde_json::to_value(
        ens_v1(
            Some(Authority::EnsV1),
            &json!({"registration": {"expiry": "1"}}),
        )
        .expect("summary without wrapper metadata")
        .expect("ENSv1 authority serves ens_v1"),
    )
    .expect("ens_v1 serializes");
    assert_eq!(object, json!({"expires_at": null}));

    // Inconsistent stored wrapper metadata fails whatever the authority.
    assert!(ens_v1(Some(Authority::EnsV2), &json!({"wrapper_state": "locked"})).is_err());
}

#[test]
fn ens_v1_wrapper_expiry_is_exact_or_null_with_its_reason() {
    let served = |word: serde_json::Value| {
        let mut summary = wrapper_summary("wrapped", 0);
        summary["wrapper_expiry_seconds"] = word;
        let object = ens_v1(Some(Authority::EnsV1), &summary)
            .expect("valid wrapper summary")
            .expect("ENSv1 authority serves ens_v1");
        let object = serde_json::to_value(object).expect("ens_v1 serializes");
        (
            object["wrapper_expires_at"].clone(),
            object.get("wrapper_expires_at_reason").cloned(),
        )
    };
    assert_eq!(
        served(json!(u64::MAX - 1)),
        (json!("18446744073709551614"), None)
    );
    assert_eq!(
        served(json!(u64::MAX)),
        (json!(null), Some(json!("no_expiry")))
    );
    assert_eq!(served(json!(0)), (json!(null), Some(json!("not_set"))));
}

#[test]
fn ens_v1_wrapper_expiry_follows_the_wrapper_state_or_its_lapse() {
    let object = |summary: &serde_json::Value| {
        ens_v1(Some(Authority::EnsV1), summary).map(|object| {
            object.map(|object| serde_json::to_value(object).expect("ens_v1 serializes"))
        })
    };
    // A wrapper state without its expiry, or an expiry beside neither a state nor a lapse, is
    // inconsistent whatever the authority.
    assert!(object(&wrapper_summary("locked", 1 | (1 << 16) | (1 << 17))).is_err());
    for summary in [
        json!({"wrapper_expiry_seconds": 1_000}),
        json!({"wrapper_expiry_seconds": 1_000, "wrapper_masked": false}),
    ] {
        assert!(object(&summary).is_err(), "{summary}");
        assert!(
            ens_v1(Some(Authority::EnsV2), &summary).is_err(),
            "{summary}"
        );
    }
    let mut unreadable = wrapper_summary("wrapped", 0);
    unreadable["wrapper_expiry_seconds"] = json!("soon");
    assert!(object(&unreadable).is_err());

    // A lapsed emancipated or locked wrapper serves its past expiry without a state.
    let lapsed = object(&json!({"registration": {"ens_v1_expiry": "1798608633"},
        "wrapper_masked": true, "wrapper_expiry_seconds": 1_000}))
    .expect("lapsed wrapper summary");
    assert_eq!(
        lapsed,
        Some(json!({"expires_at": "1798608633", "wrapper_expires_at": "1000"}))
    );
    // A masked wrapper whose state is unknown has no expiry to serve.
    let unknown = object(&json!({"wrapper_masked": true})).expect("masked wrapper summary");
    assert_eq!(unknown, Some(json!({"expires_at": null})));
}

fn wrapper_summary(state: &str, fuses: u32) -> serde_json::Value {
    json!({
        "wrapper_state": state,
        "wrapper_fuses": {
            "fuses": fuses,
            "cannot_unwrap": fuses & 1 != 0,
            "cannot_burn_fuses": fuses & 2 != 0,
            "cannot_transfer": fuses & 4 != 0,
            "cannot_set_resolver": fuses & 8 != 0,
            "cannot_set_ttl": fuses & 16 != 0,
            "cannot_create_subdomain": fuses & 32 != 0,
            "cannot_approve": fuses & 64 != 0,
            "parent_cannot_control": fuses & (1 << 16) != 0,
            "is_dot_eth": fuses & (1 << 17) != 0,
            "can_extend_expiry": fuses & (1 << 18) != 0
        }
    })
}

#[test]
fn lapsed_registration_held_through_is_a_closed_set() {
    let held_through = |authority_kind: serde_json::Value| {
        let summary = json!({"registration": {"lapsed_registration": {
            "owner": "0x00000000000000000000000000000000000000aa",
            "authority_kind": authority_kind,
            "released_at": 1_700_000_000,
        }}});
        serde_json::to_value(lapsed_registration(&summary).expect("lapsed block"))
            .expect("lapsed block serializes")
    };
    assert_eq!(
        held_through(json!("registrar"))["held_through"],
        "registrar"
    );
    assert_eq!(held_through(json!("wrapper"))["held_through"], "wrapper");
    // Project's other authority kinds never hold a lease, so they are not served as one.
    for other in [
        json!("registry_only"),
        json!("ens_v2_registry"),
        json!(null),
    ] {
        let lapsed = held_through(other.clone());
        assert!(lapsed.get("held_through").is_none(), "{other}: {lapsed}");
        assert_eq!(
            lapsed["owner"],
            "0x00000000000000000000000000000000000000aa"
        );
    }
}
