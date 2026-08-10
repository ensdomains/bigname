use serde_json::json;

use super::*;

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
