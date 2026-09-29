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
fn expiry_timestamps_follow_the_shared_range_rule() {
    for (expiry, expected) in [
        (json!(-1), None),
        (json!("-1"), None),
        (json!(0), Some("1970-01-01T00:00:00Z")),
        (json!(253_402_300_799_u64), Some("9999-12-31T23:59:59Z")),
        (json!(253_402_300_800_u64), None),
        (json!(1_735_689_600.5), Some("2025-01-01T00:00:00Z")),
        (json!("1735689600.5"), Some("2025-01-01T00:00:00Z")),
    ] {
        assert_eq!(
            timestamp_field(&json!({ "expiry": expiry }), "expiry"),
            expected.map(Value::from),
            "{expiry}"
        );
    }
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
        json!({ "expires_at": "2031-10-17T10:40:00Z" })
    );

    let mut state_derived = row("ExpiryChanged", json!({}), json!({ "expiry": null }));
    state_derived.raw_fact_ref = json!({ "kind": "interpreter_state" });
    let detail = row_detail(&state_derived, HistoryEventType::Expiry);
    assert_eq!(detail.contract_address, None);
    assert!(detail.data.is_empty());
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
            "owner": "0x00000000000000000000000000000000000000bb",
            "expires_at": "2030-03-17T17:46:40Z",
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
    assert!(cleared.data.is_empty());

    let mut unknown_chain = row(
        "SubregistryChanged",
        json!({}),
        json!({ "subregistry": "0x0000000000000000000000000000000000000abc" }),
    );
    unknown_chain.chain_id = Some("unknown-chain".to_owned());
    assert!(
        row_detail(&unknown_chain, HistoryEventType::Subregistry)
            .data
            .is_empty()
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
        json!({ "address": "0x00000000000000000000000000000000000000aa", "coin_type": 60 })
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
    assert_eq!(Value::Object(fuses.data), json!({ "fuses": 196609 }));
}
