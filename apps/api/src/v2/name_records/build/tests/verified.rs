use bigname_lookup::{
    LedgerAction, LookupPosition, LookupRecordResult, LookupRecordStatus, LookupResponse,
};

use super::*;

#[test]
fn verified_statuses_preserve_reason_defaults_and_mapping() {
    use LookupRecordStatus::{ExecutionFailed, NotFound, Success, Unsupported};

    for (status, reason, expected) in [
        (Success, None, json!({"status":"ok","value":"answer"})),
        (NotFound, None, json!({"status":"not_found"})),
        (
            Unsupported,
            None,
            json!({"status":"unsupported","unsupported_reason":"verified_records_not_supported"}),
        ),
        (
            ExecutionFailed,
            None,
            json!({"status":"failed","failure_reason":"verified_record_read_failed"}),
        ),
        (
            Success,
            Some("raw_log_missing_record_cache"),
            json!({"status":"ok","value":"answer"}),
        ),
        (
            NotFound,
            Some("record_absent"),
            json!({"status":"not_found","failure_reason":"record_absent"}),
        ),
        (
            Unsupported,
            Some("resolver_family_pending"),
            json!({"status":"unsupported","unsupported_reason":"resolver_family_pending"}),
        ),
        (
            ExecutionFailed,
            Some("call_reverted"),
            json!({"status":"failed","failure_reason":"call_reverted"}),
        ),
        (
            NotFound,
            Some("value_not_retained_in_normalized_events"),
            json!({"status":"not_found","failure_reason":"value_not_retained"}),
        ),
        (
            Unsupported,
            Some("record_family_not_supported_in_phase6_projection"),
            json!({"status":"unsupported","unsupported_reason":"record_family_not_supported"}),
        ),
        (
            ExecutionFailed,
            Some("value_not_retained_in_normalized_events"),
            json!({"status":"failed","failure_reason":"value_not_retained"}),
        ),
        (
            NotFound,
            Some(""),
            json!({"status":"not_found","failure_reason":""}),
        ),
        (
            Unsupported,
            Some(""),
            json!({"status":"unsupported","unsupported_reason":""}),
        ),
        (
            ExecutionFailed,
            Some(""),
            json!({"status":"failed","failure_reason":""}),
        ),
    ] {
        let mut result = lookup_result("text:url", status);
        result.value = Some(json!({"value":"answer"}));
        result.failure_reason = reason.map(str::to_owned);
        result.unsupported_reason = reason.map(str::to_owned);
        let answers = verified_answers(&["text:url"], vec![result]).expect("answer must map");
        assert_eq!(
            json!(answers["text:url"]),
            expected,
            "{status:?}, {reason:?}"
        );
    }
}

#[test]
fn verified_success_preserves_scalar_and_nested_value_formatting() {
    for (value, expected) in [
        (None, None),
        (Some(Value::Null), None),
        (Some(json!("direct")), Some("direct")),
        (Some(json!("")), Some("")),
        (Some(json!(42)), Some("42")),
        (Some(json!(false)), Some("false")),
        (
            Some(json!({"value":"nested","extra":"ignored"})),
            Some("nested"),
        ),
        (Some(json!({"value":17})), Some("17")),
        (Some(json!({"value":true})), Some("true")),
        (Some(json!({"value":null})), None),
        (Some(json!({"value":{"value":"too deep"}})), None),
        (Some(json!({"other":"field"})), None),
        (Some(json!(["array"])), None),
    ] {
        let mut result = lookup_result("text:url", LookupRecordStatus::Success);
        result.value = value.clone();
        let answers = verified_answers(&["text:url"], vec![result]).expect("value must map");
        let expected = match expected {
            Some(value) => json!({"status":"ok","value":value}),
            None => json!({"status":"ok"}),
        };
        assert_eq!(json!(answers["text:url"]), expected, "{value:?}");
    }
}

#[test]
fn verified_selection_keeps_last_duplicate_and_missing_result_reason() {
    let mut discarded = lookup_result("text:url", LookupRecordStatus::ExecutionFailed);
    discarded.failure_reason = Some("raw_log_missing_record_cache".to_owned());
    let mut selected = lookup_result("text:url", LookupRecordStatus::Success);
    selected.value = Some(json!("last result"));
    let mut unrequested = lookup_result("text:unrequested", LookupRecordStatus::Unsupported);
    unrequested.unsupported_reason = Some("raw_log_missing_record_cache".to_owned());
    let results = vec![discarded, selected, unrequested];

    let answers = verified_answers(&["text:url", "text:missing", "text:url"], results.clone())
        .expect("unrequested and replaced results must not be mapped");
    assert_eq!(
        json!(answers),
        json!({
            "text:missing": {
                "status":"unsupported",
                "unsupported_reason":"verified resolution entrypoint is not yet supported"
            },
            "text:url":{"status":"ok","value":"last result"}
        })
    );
    assert!(
        verified_answers(&[], results)
            .expect("empty selection must map")
            .is_empty()
    );
}

#[test]
fn verified_failure_reasons_still_reject_pipeline_vocabulary() {
    for status in [
        LookupRecordStatus::NotFound,
        LookupRecordStatus::Unsupported,
        LookupRecordStatus::ExecutionFailed,
    ] {
        let mut result = lookup_result("text:url", status);
        result.failure_reason = Some("raw_log_missing_record_cache".to_owned());
        result.unsupported_reason = result.failure_reason.clone();
        let error = verified_answers(&["text:url"], vec![result])
            .expect_err("selected pipeline reason must fail the request");
        assert_eq!(error.code(), ErrorCode::InternalError, "{status:?}");
    }
}

fn lookup_result(key: &str, status: LookupRecordStatus) -> LookupRecordResult {
    let record = parse_resolution_record_key(key).expect("test selector must parse");
    LookupRecordResult {
        record_key: record.record_key,
        record_family: record.record_family,
        selector_key: record.selector_key,
        status,
        value: None,
        failure_reason: None,
        unsupported_reason: None,
        ccip_read: false,
        ledger_action: LedgerAction::None,
    }
}

fn verified_answers(
    keys: &[&str],
    results: Vec<LookupRecordResult>,
) -> V2Result<BTreeMap<String, RecordAnswer>> {
    let records = keys
        .iter()
        .map(|key| parse_resolution_record_key(key).unwrap())
        .collect::<Vec<_>>();
    let position = LookupPosition {
        chain_id: "ethereum-mainnet".to_owned(),
        block_number: 1,
        block_hash: "0xblock".to_owned(),
        timestamp: "1717171719".to_owned(),
    };
    let response = LookupResponse {
        logical_name_id: "ens:alice.eth".to_owned(),
        name: "alice.eth".to_owned(),
        resolver_chain_id: position.chain_id.clone(),
        resolver_address: "0xresolver".to_owned(),
        entrypoint_chain_id: position.chain_id.clone(),
        entrypoint_address: "0xresolver".to_owned(),
        authoritative_position: position.clone(),
        execution_position: position,
        observed_positions: json!({}),
        records: results,
    };
    verified_record_answers(
        &current_name_row(OffsetDateTime::from_unix_timestamp(1_717_171_719).unwrap()),
        &records,
        Some(VerifiedRecordLookup::Found {
            response: Box::new(response),
        }),
        false,
    )
}
