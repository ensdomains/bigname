use bigname_storage::CanonicalityState;
use serde_json::json;

use super::*;

fn row_detail(row: &StorageHistoryEvent, event_type: HistoryEventType) -> EventDetail {
    build_event_detail(row, event_type, &HistoryRowContext::default())
}

fn row(event_kind: &str, before: Value, after: Value) -> StorageHistoryEvent {
    StorageHistoryEvent {
        normalized_event_id: 1,
        event_identity: "event:1".to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: Some("ens:alice.eth".to_owned()),
        resource_id: None,
        registration_id: None,
        event_kind: event_kind.to_owned(),
        source_family: "ens_v1_registry_l1".to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some("ethereum-mainnet".to_owned()),
        block_number: Some(100),
        block_hash: Some("0xblock".to_owned()),
        block_timestamp: None,
        transaction_hash: Some("0xtx".to_owned()),
        transaction_index: Some(0),
        log_index: Some(0),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "emitting_address": "0x00000000000000000000000000000000000000AA",
        }),
        derivation_kind: "direct".to_owned(),
        canonicality_state: CanonicalityState::Canonical,
        before_state: before,
        after_state: after,
        migration_correlation_ids: Vec::new(),
        consumer_visibility: "activated".to_owned(),
        migration_associations: json!([]),
        provenance: json!({}),
        coverage: json!({}),
    }
}

#[test]
fn history_payment_values_preserve_precision_zero_and_source_meaning() {
    let maximum = alloy_primitives::U256::MAX.to_string();
    let mut event = row(
        "RegistrationGranted",
        json!({}),
        json!({
            "source_event":"NameRegistered","cost":maximum,"referrer":format!("0x{}","0".repeat(64)),
        }),
    );
    event.source_family = "ens_v1_registrar_l1".into();
    let data = row_detail(&event, HistoryEventType::Registration).data;
    assert_eq!(data["cost"], maximum);
    assert_eq!(data["referrer"], format!("0x{}", "0".repeat(64)));
    assert!(!data.contains_key("base_cost"));
    event.after_state = json!({"source_event":"NameRegistered","base_cost":"000123","premium":"0"});
    let data = row_detail(&event, HistoryEventType::Registration).data;
    assert_eq!(data["base_cost"], "123");
    assert_eq!(data["premium"], "0");
    assert!(!data.contains_key("cost"));
    event.after_state["source_event"] = json!("NameRenewed");
    assert!(
        !row_detail(&event, HistoryEventType::Registration)
            .data
            .contains_key("base_cost")
    );
    event.source_family = "ens_v2_registrar_l1".into();
    event.event_kind = "RegistrationRenewed".into();
    event.after_state = json!({"source_event":"NameRenewed","amount":"0","base":"55",
        "payment_token":"0x0000000000000000000000000000000000000000"});
    let data = row_detail(&event, HistoryEventType::Renewal).data;
    assert_eq!(data["cost"], "0");
    assert_eq!(
        data["payment_token"]["address"],
        "0x0000000000000000000000000000000000000000"
    );
    event.after_state["amount"] = json!("invalid");
    assert!(
        !row_detail(&event, HistoryEventType::Renewal)
            .data
            .contains_key("cost")
    );
    event.after_state.as_object_mut().unwrap().remove("amount");
    assert_eq!(
        row_detail(&event, HistoryEventType::Renewal).data["cost"],
        "55"
    );
    event.source_family = "basenames_base_registrar".into();
    assert!(
        !row_detail(&event, HistoryEventType::Renewal)
            .data
            .contains_key("cost")
    );
}

#[test]
fn history_canonical_id_and_operator_use_only_valid_event_evidence() {
    let mut event = row(
        "PermissionChanged",
        json!({}),
        json!({"upstream_resource":"0x100000002"}),
    );
    event.source_family = "ens_v2_registry_l1".into();
    assert_eq!(
        row_detail(&event, HistoryEventType::Permission).data["canonical_id"],
        "4294967296"
    );
    event.after_state["token_id"] = json!("0x100000009");
    event.after_state["labelhash"] = json!("0x1ffffffff");
    assert_eq!(
        row_detail(&event, HistoryEventType::Permission).data["canonical_id"],
        "4294967296"
    );
    event.after_state["token_id"] = json!("0x200000009");
    assert!(
        !row_detail(&event, HistoryEventType::Permission)
            .data
            .contains_key("canonical_id")
    );
    event.after_state["token_id"] = json!("invalid");
    assert!(
        !row_detail(&event, HistoryEventType::Permission)
            .data
            .contains_key("canonical_id")
    );
    event.after_state = json!({"upstream_resource":"0x1","root_resource":false});
    assert_eq!(
        row_detail(&event, HistoryEventType::Permission).data["canonical_id"],
        "0"
    );
    event.after_state["root_resource"] = json!(true);
    assert!(
        !row_detail(&event, HistoryEventType::Permission)
            .data
            .contains_key("canonical_id")
    );
    event.after_state = json!({"operator":"0x00000000000000000000000000000000000000AA"});
    assert_eq!(
        row_detail(&event, HistoryEventType::Transfer).data["operator"],
        "0x00000000000000000000000000000000000000aa"
    );
    event.source_family = "ens_v1_registrar_l1".into();
    assert!(
        !row_detail(&event, HistoryEventType::Transfer)
            .data
            .contains_key("operator")
    );
}

#[test]
fn expiry_timestamps_preserve_finite_words_and_contextual_nulls() {
    for expiry in [0_u64, 253_402_300_800, 9_007_199_254_740_993, u64::MAX] {
        let event = row("RegistrationRenewed", json!({}), json!({"expiry": expiry}));
        let data = row_detail(&event, HistoryEventType::Renewal).data;
        assert_eq!(data["expires_at"], expiry.to_string());
        assert!(!data.contains_key("expires_at_reason"));
    }
    let mut event = row("ExpiryChanged", json!({}), json!({"expiry": u64::MAX}));
    event.source_family = "ens_v1_wrapper_l1".into();
    let data = row_detail(&event, HistoryEventType::Expiry).data;
    assert_eq!(data["expires_at"], Value::Null);
    assert_eq!(data["expires_at_reason"], "no_expiry");
    event.after_state = json!({"expiry": 0, "source_event": "LabelUnregistered"});
    event.source_family = "ens_v2_registry_l1".into();
    let data = row_detail(&event, HistoryEventType::Expiry).data;
    assert_eq!(data["expires_at"], Value::Null);
    assert_eq!(data["expires_at_reason"], "released");
}

#[test]
fn include_accepts_only_data_and_raw_in_any_order() {
    assert_eq!(
        history_include(&[]).expect("empty include is valid"),
        HistoryInclude::default()
    );
    assert_eq!(
        history_include(&["data".to_owned()]).expect("data is valid"),
        HistoryInclude::DATA
    );
    assert_eq!(
        history_include(&["raw".to_owned()]).expect("raw is valid"),
        HistoryInclude::RAW
    );
    let both = HistoryInclude {
        data: true,
        raw: true,
    };
    assert_eq!(
        history_include(&["data".to_owned(), "raw".to_owned()]).expect("both are valid"),
        both
    );
    assert_eq!(
        history_include(&["raw".to_owned(), "data".to_owned()]).expect("both are valid"),
        both
    );
    assert!(history_include(&["bogus".to_owned()]).is_err());
    assert!(history_include(&["kind".to_owned()]).is_err());
    assert!(history_include(&["data".to_owned(), "events".to_owned()]).is_err());
}

#[test]
fn raw_kind_is_exposed_only_behind_include_raw() {
    let row = row("RegistrationRenewed", json!({}), json!({}));
    assert_eq!(raw_event_kind(&row, HistoryInclude::default()), None);
    assert_eq!(raw_event_kind(&row, HistoryInclude::DATA), None);
    assert_eq!(
        raw_event_kind(&row, HistoryInclude::RAW),
        Some("RegistrationRenewed".to_owned())
    );
}

#[test]
fn detail_exposes_lower_cased_emitter_without_the_raw_kind() {
    let detail = row_detail(
        &row(
            "RegistrationRenewed",
            json!({}),
            json!({ "expiry": 1_950_000_000_i64 }),
        ),
        HistoryEventType::Renewal,
    );
    assert_eq!(
        detail.contract_address,
        Some("0x00000000000000000000000000000000000000aa".to_owned())
    );
    assert!(
        serde_json::to_value(&detail)
            .expect("detail must serialize")
            .get("kind")
            .is_none()
    );
    assert_eq!(
        Value::Object(detail.data),
        json!({ "action":"registration_renewed", "expires_at": "1950000000" })
    );

    let mut state_derived = row("ExpiryChanged", json!({}), json!({ "expiry": null }));
    state_derived.raw_fact_ref = json!({ "kind": "interpreter_state" });
    let detail = row_detail(&state_derived, HistoryEventType::Expiry);
    assert_eq!(detail.contract_address, None);
    assert_eq!(
        Value::Object(detail.data),
        json!({"action":"expiry_changed"})
    );
}

#[test]
fn registration_and_pointer_types_use_dictionary_shapes() {
    let detail = row_detail(
        &row(
            "RegistrationGranted",
            json!({}),
            json!({
                "owner": "0x00000000000000000000000000000000000000BB",
                "expiry": "1900000000",
                "resolver": "0x0000000000000000000000000000000000000abc",
                "subregistry": ZERO_ADDRESS,
            }),
        ),
        HistoryEventType::Registration,
    );
    let mut data = detail.data;
    let action_id = data
        .remove("action_id")
        .expect("a logged grant has an action");
    assert!(action_id.as_str().is_some_and(|id| id.len() == 64));
    assert_eq!(data.remove("action_role"), Some(json!("registered")));
    assert_eq!(
        Value::Object(data),
        json!({
            "action":"registration_granted",
            "owner": "0x00000000000000000000000000000000000000bb",
            "expires_at": "1900000000",
            "resolver": {
                "chain_id": 1,
                "address": "0x0000000000000000000000000000000000000abc",
            },
        })
    );

    let cleared = row_detail(
        &row(
            "ResolverChanged",
            json!({ "resolver": "0x0000000000000000000000000000000000000abc" }),
            json!({ "resolver": ZERO_ADDRESS }),
        ),
        HistoryEventType::Resolver,
    );
    assert_eq!(
        Value::Object(cleared.data),
        json!({"action":"resolver_changed"})
    );

    let mut unknown_chain = row(
        "SubregistryChanged",
        json!({}),
        json!({ "subregistry": "0x0000000000000000000000000000000000000abc" }),
    );
    unknown_chain.chain_id = Some("unknown-chain".to_owned());
    assert_eq!(
        Value::Object(row_detail(&unknown_chain, HistoryEventType::Subregistry).data),
        json!({"action":"subregistry_changed"})
    );
}

#[test]
fn registration_action_groups_by_transaction_contract_and_token() {
    let grant = |identity: &str, emitter: &str, token: &str, tx: Option<&str>| {
        let mut row = row(
            "RegistrationGranted",
            json!({}),
            json!({ "token_id": token, "registrant": "0x00000000000000000000000000000000000000aa" }),
        );
        row.event_identity = identity.to_owned();
        row.raw_fact_ref["emitting_address"] = json!(emitter);
        row.transaction_hash = tx.map(str::to_owned);
        row
    };
    let action = |row: &StorageHistoryEvent| registration_action(row);
    let registry = "0x00000000000000000000000000000000000000Aa";
    let parent = "0x00000000000000000000000000000000000000bb";
    let registered = action(&grant(
        "x:0xtx:1:RegistrationGranted:0",
        registry,
        "0x01",
        Some("0xtx"),
    ))
    .expect("registered");
    // Emitter case does not split an action.
    let linked = action(&grant(
        "x:0xtx:3:RegistrationGranted:linked:0x01:0",
        &registry.to_ascii_lowercase(),
        "0x01",
        Some("0xTX"),
    ))
    .expect("linked");
    let other_token = action(&grant(
        "x:0xtx:4:RegistrationGranted:0",
        registry,
        "0x02",
        Some("0xtx"),
    ))
    .expect("second registration");
    let reachable = action(&grant(
        "x:0xtx2:2:RegistrationGranted:topology:0xaa:0x01:0",
        parent,
        "0x01",
        Some("0xtx2"),
    ))
    .expect("reachable");
    assert_eq!(registered.1, "registered");
    assert_eq!(linked, (registered.0.clone(), "linked"));
    assert_ne!(other_token.0, registered.0);
    assert_eq!(reachable.1, "reachable");
    assert_ne!(reachable.0, registered.0);
    assert_eq!(action(&grant("x:state", registry, "0x01", None)), None);

    // One parent `SubregistryUpdated` makes two descendant registries reachable; both grants carry
    // the parent's log and the same token, and each is its own action.
    let descendant = |registry: &str, name: &str| {
        let mut row = grant(
            &format!("x:0xtx3:7:RegistrationGranted:topology:{registry}:0x01:0"),
            parent,
            "0x01",
            Some("0xtx3"),
        );
        row.log_index = Some(7);
        row.logical_name_id = Some(name.to_owned());
        action(&row).expect("reachable grant")
    };
    let first = descendant(
        "0x00000000000000000000000000000000000000c1",
        "ens:a.sub.eth",
    );
    let second = descendant(
        "0x00000000000000000000000000000000000000c2",
        "ens:a.other.eth",
    );
    let relinked = descendant(
        "0x00000000000000000000000000000000000000c1",
        "ens:a.more.eth",
    );
    assert_ne!(first.0, second.0);
    assert_ne!(first.0, relinked.0);
    assert_eq!(
        first,
        descendant(
            "0x00000000000000000000000000000000000000C1",
            "ens:a.sub.eth"
        )
    );
}

#[test]
fn transfer_authority_record_primary_name_and_permission_payloads() {
    let transfer = row_detail(
        &row(
            "TokenControlTransferred",
            json!({ "from": "0x00000000000000000000000000000000000000AA" }),
            json!({ "to": "0x00000000000000000000000000000000000000bb", "fuses": 65537 }),
        ),
        HistoryEventType::Transfer,
    );
    assert_eq!(
        Value::Object(transfer.data),
        json!({
            "action":"token_transferred",
            "from": "0x00000000000000000000000000000000000000aa",
            "to": "0x00000000000000000000000000000000000000bb",
            "fuses": 65537,
        })
    );

    let authority = row_detail(
        &row(
            "AuthorityEpochChanged",
            json!({ "registry_owner": "0x00000000000000000000000000000000000000aa" }),
            json!({ "registry_owner": "0x00000000000000000000000000000000000000cc" }),
        ),
        HistoryEventType::Authority,
    );
    assert_eq!(
        Value::Object(authority.data),
        json!({
            "action":"authority_changed",
            "owner": "0x00000000000000000000000000000000000000cc",
            "from": "0x00000000000000000000000000000000000000aa",
        })
    );

    let record = row_detail(
        &row(
            "RecordChanged",
            json!({}),
            json!({
                "record_key": "text:avatar",
                "record_family": "text",
                "value": "ipfs://avatar",
                "value_retained": true,
            }),
        ),
        HistoryEventType::Record,
    );
    assert_eq!(
        Value::Object(record.data),
        json!({
            "action":"record_changed",
            "key": "text:avatar",
            "value": "ipfs://avatar",
            "resolver": {
                "chain_id": 1,
                "address": "0x00000000000000000000000000000000000000aa",
            },
        })
    );
    let resolver = json!({
        "chain_id": 1,
        "address": "0x00000000000000000000000000000000000000cc",
    });
    let unretained = row_detail(
        &row(
            "RecordChanged",
            json!({}),
            json!({
                "record_key": "addr:2147483658",
                "coin_type": "2147483658",
                "storage_model": "resolver_record_id",
                "resolver": "0x00000000000000000000000000000000000000CC",
                "resolver_record_id": "9",
            }),
        ),
        HistoryEventType::Record,
    );
    assert_eq!(
        Value::Object(unretained.data),
        json!({
            "action":"record_changed",
            "key": "addr:2147483658",
            "coin_type": 2_147_483_658_u64,
            "resolver": resolver,
            "record_id": "9",
        })
    );
    let version = row_detail(
        &row(
            "RecordVersionChanged",
            json!({}),
            json!({
                "version": 2,
                "resolver": "0x00000000000000000000000000000000000000cc",
                "node": "0x00000000000000000000000000000000000000000000000000000000000000AB",
            }),
        ),
        HistoryEventType::Record,
    );
    assert_eq!(
        Value::Object(version.data),
        json!({
            "action":"record_version_changed",
            "resolver": resolver,
            "node": "0x00000000000000000000000000000000000000000000000000000000000000ab",
        })
    );

    let primary = row_detail(
        &row(
            "ReverseChanged",
            json!({}),
            json!({
                "address": "0x00000000000000000000000000000000000000AA",
                "coin_type": "60",
                "reverse_name": "aa.addr.reverse",
            }),
        ),
        HistoryEventType::PrimaryName,
    );
    assert_eq!(
        Value::Object(primary.data),
        json!({ "action":"primary_name_recorded", "address": "0x00000000000000000000000000000000000000aa", "coin_type": 60 })
    );

    let permission = row_detail(
        &row(
            "EACRolesChanged",
            json!({}),
            json!({
                "subject": "0x00000000000000000000000000000000000000DD",
                "effective_powers": ["resource_control", "set_resolver"],
                "scope": { "kind": "resolver" },
                "role_bitmap": "0x1",
            }),
        ),
        HistoryEventType::Permission,
    );
    assert_eq!(
        Value::Object(permission.data),
        json!({
            "action":"permission_changed",
            "address": "0x00000000000000000000000000000000000000dd",
            "powers": ["registration_control", "set_resolver"],
        })
    );
    let fuses = row_detail(
        &row(
            "PermissionScopeChanged",
            json!({ "fuses": 0 }),
            json!({ "fuses": 196609, "wrapper_state": "locked" }),
        ),
        HistoryEventType::Permission,
    );
    assert_eq!(
        Value::Object(fuses.data),
        json!({ "action":"permission_changed", "fuses": 196609 })
    );
}

/// An ENSv2 role change states the account's old roles on the log, so the row separates what was
/// granted and revoked from the resulting set; an ENSv1 grant's before state is the adapter's
/// template and yields neither list.
#[test]
fn permission_rows_derive_changes_only_from_a_logged_previous_set() {
    let subject = "0x00000000000000000000000000000000000000DD";
    let scope = json!({
        "kind": "resolver",
        "chain_id": "ethereum-mainnet",
        "resolver_address": "0x00000000000000000000000000000000000000EE",
    });
    let changed = row_detail(
        &row(
            "PermissionChanged",
            json!({"subject": subject, "role_bitmap": "0x10", "effective_powers": ["set_text"]}),
            json!({"subject": subject, "scope": scope, "role_bitmap": "0x11",
                "effective_powers": ["set_addr", "set_text"]}),
        ),
        HistoryEventType::Permission,
    );
    assert_eq!(
        Value::Object(changed.data),
        json!({
            "action":"permission_changed",
            "address": "0x00000000000000000000000000000000000000dd",
            "grant_scope": {"kind": "resolver", "detail": {"resolver": {
                "chain_id": 1, "address": "0x00000000000000000000000000000000000000ee"}}},
            "powers": ["set_addr", "set_text"],
            "added_powers": ["set_addr"],
            "removed_powers": [],
        })
    );

    let template = row_detail(
        &row(
            "PermissionChanged",
            json!({"subject": subject, "effective_powers": []}),
            json!({"subject": subject, "scope": {"kind": "resource"},
                "effective_powers": ["resource_control"]}),
        ),
        HistoryEventType::Permission,
    );
    assert_eq!(
        Value::Object(template.data),
        json!({
            "action":"permission_changed",
            "address": "0x00000000000000000000000000000000000000dd",
            "grant_scope": {"kind": "registration", "detail": {}},
            "powers": ["registration_control"],
        })
    );
}

/// A registry root role change is a `permission` row in the root scope. Its log states the
/// account's old roles, so the row lists what the change granted and revoked.
#[test]
fn root_role_changes_render_as_permission_rows_with_logged_changes() {
    assert_eq!(
        super::super::history_event_type("RootPermissionChanged"),
        Some(HistoryEventType::Permission)
    );
    let subject = "0x00000000000000000000000000000000000000DD";
    let detail = row_detail(
        &row(
            "RootPermissionChanged",
            json!({"subject": subject, "role_bitmap": "0x10001",
                "effective_powers": ["registrar", "renew"]}),
            json!({"subject": subject, "role_bitmap": "0x10010", "old_role_bitmap": "0x10001",
                "effective_powers": ["register_reserved", "renew"], "root_resource": true,
                "scope": {"kind": "registry_root", "chain_id": "ethereum-mainnet",
                    "registry_address": "0x00000000000000000000000000000000000000aa"}}),
        ),
        HistoryEventType::Permission,
    );
    let mut data = Value::Object(detail.data);
    assert_eq!(data["grant_scope"]["kind"], "root", "{data}");
    data.as_object_mut().map(|data| data.remove("grant_scope"));
    assert_eq!(
        data,
        json!({
            "action":"permission_changed",
            "address": "0x00000000000000000000000000000000000000dd",
            "powers": ["register_reserved", "renew"],
            "added_powers": ["register_reserved"],
            "removed_powers": ["registrar"],
        })
    );
}

#[test]
fn wrapping_action_requires_positive_completion_evidence_at_the_mint() {
    let after = json!({"source_event":"TransferSingle","wrapper_mint":true,
        "matched_wrapper_completion":{"source_event":"NameWrapped"},
        "node":"0xnode","fuses":17,"expiry":1900000000});
    let mut event = row("AuthorityEpochChanged", json!({}), after.clone());
    event.source_family = "ens_v1_wrapper_l1".into();
    let data = row_detail(&event, HistoryEventType::Authority).data;
    assert_eq!(data["action"], "name_wrapped");
    assert_eq!(data["node"], "0xnode");
    assert_eq!(data["fuses"], 17);
    assert_eq!(data["expires_at"], "1900000000");
    for (family, field, value) in [
        ("ens_v1_registry_l1", "wrapper_mint", json!(true)),
        ("ens_v1_wrapper_l1", "wrapper_mint", Value::Null),
        ("ens_v1_wrapper_l1", "wrapper_mint", json!(false)),
        ("ens_v1_wrapper_l1", "wrapper_mint", json!("true")),
        ("ens_v1_wrapper_l1", "matched_wrapper_completion", json!({})),
        (
            "ens_v1_wrapper_l1",
            "matched_wrapper_completion",
            json!({"source_event":"Other"}),
        ),
        ("ens_v1_wrapper_l1", "source_event", json!("TransferBatch")),
    ] {
        event.source_family = family.into();
        event.after_state = after.clone();
        event.after_state[field] = value;
        let data = row_detail(&event, HistoryEventType::Authority).data;
        assert_eq!(data["action"], "authority_changed", "{event:?}");
        assert!(!data.contains_key("node"), "{data:?}");
        assert!(!data.contains_key("fuses"), "{data:?}");
    }
    event.after_state =
        json!({"source_event":"NameWrapped","node":"0xnode","fuses":17,"expiry":1900000000});
    assert_eq!(
        row_detail(&event, HistoryEventType::Authority).data["action"],
        "name_wrapped"
    );
}
