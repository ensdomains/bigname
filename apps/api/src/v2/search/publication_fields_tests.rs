use super::*;
use bigname_storage::{families::search_dictionary::SearchRow, public_name_fields::SearchFields};
use serde_json::{Value, json};

#[test]
fn stored_presence_reaches_the_actual_search_http_builder() {
    for (payload, present) in [
        (
            json!({"status":"unregistered","ens_v1":{"expires_at":null}}),
            false,
        ),
        (
            json!({"status":"active","expires_at":null,"expires_at_reason":"no_expiry","grace_ends_at":null,"ens_v1":{"expires_at":null}}),
            true,
        ),
    ] {
        let encoded = serde_json::to_string(&payload).unwrap();
        let fields: SearchFields = serde_json::from_str(&encoded).unwrap();
        let row = SearchRow {
            name: "name.eth".into(),
            display_name: "name.eth".into(),
            namespace: "ens".into(),
            namehash: "0x1234".into(),
            owner: None,
            authority: Some("ens_v1".into()),
            fields,
            created_at: Some("1700000000".into()),
        };
        let body = serde_json::to_value(build_compact_search_name(&row).unwrap()).unwrap();
        assert_eq!(body.get("expires_at").is_some(), present);
        assert_eq!(body.get("grace_ends_at").is_some(), present);
        if present {
            assert_eq!(body["expires_at"], Value::Null);
            assert_eq!(body["grace_ends_at"], Value::Null);
        }
        assert_eq!(body["ens_v1"], json!({"expires_at":null}));
        assert_eq!(body["created_at"], "1700000000");
        assert!(body.get("created_at_declared").is_none());
        assert!(body.get("lapsed_registration").is_none());
        assert!(body.get("expires_window_index").is_none());
    }
}
