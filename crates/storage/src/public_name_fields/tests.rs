use super::*;
use serde_json::json;

#[test]
fn stored_search_fields_preserve_present_null_and_absent_expiry() {
    for (summary, present) in [
        (json!({}), false),
        (
            json!({"registration":{"expires_at_reason":"not_applicable"}}),
            true,
        ),
    ] {
        let fields = SearchFields {
            registration: registration_fields("ens", &summary, false),
            ens_v1: ens_v1(Some("ens_v1"), &summary).unwrap(),
        };
        let stored = serde_json::to_value(&fields).unwrap();
        let decoded: SearchFields = serde_json::from_value(stored.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), stored);
        assert_eq!(stored.get("expires_at").is_some(), present);
        assert_eq!(stored.get("grace_ends_at").is_some(), present);
        if present {
            assert!(stored["expires_at"].is_null());
            assert!(stored["grace_ends_at"].is_null());
        }
        assert_eq!(stored["ens_v1"], json!({"expires_at": null}));
    }
}

#[test]
fn creation_is_declared_or_minimum_current_position_not_a_stored_clock() {
    let first = json!({"base":{"timestamp":"1700000000"},"ethereum":{"timestamp":"1699999998"}});
    let next = json!({"base":{"timestamp":"1700000010"},"ethereum":{"timestamp":"1700000008"}});
    assert_eq!(resolve_created_at(None, &first), Some("1699999998".into()));
    assert_eq!(resolve_created_at(None, &next), Some("1700000008".into()));
    assert_eq!(resolve_created_at(Some("17"), &next), Some("17".into()));
}

#[test]
fn expiry_stays_exact_above_calendar_range_through_storage_json() {
    let summary = json!({"registration":{"expiry":"18446744073709551615","grace_ends_at":"18446744073709551615"}});
    let fields = SearchFields {
        registration: registration_fields("ens", &summary, true),
        ens_v1: None,
    };
    let json = serde_json::to_value(fields).unwrap();
    let decoded: SearchFields = serde_json::from_value(json.clone()).unwrap();
    assert_eq!(json, serde_json::to_value(decoded).unwrap());
    assert_eq!(json["expires_at"], "18446744073709551615");
}

#[test]
fn invalid_wrapper_is_rejected_even_when_authority_would_omit_the_object() {
    for authority in [None, Some("ens_v2"), Some("ens_v1")] {
        assert!(ens_v1(authority, &json!({"wrapper_state":"wrapped"})).is_err());
    }
}
