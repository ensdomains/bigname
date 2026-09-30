use bigname_lookup::{LedgerAction, LookupRecordResult, LookupRecordStatus};

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
        let answer = verified_answer_from_result(&result).expect("answer must map");
        assert_eq!(json!(answer), expected, "{status:?}, {reason:?}");
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
        let answer = verified_answer_from_result(&result).expect("value must map");
        let expected = match expected {
            Some(value) => json!({"status":"ok","value":value}),
            None => json!({"status":"ok"}),
        };
        assert_eq!(json!(answer), expected, "{value:?}");
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

    let records = ["text:url", "text:missing", "text:url"]
        .map(|key| parse_resolution_record_key(key).expect("test selector must parse"));
    let answers = verified_answers_from_results(&results, &records)
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
        verified_answers_from_results(&results, &[])
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
        let error = verified_answer_from_result(&result)
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
