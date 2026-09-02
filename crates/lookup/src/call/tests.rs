use serde_json::json;

use super::*;

#[test]
fn provider_i64_min_error_code_cannot_spoof_internal_lookup_outcomes() {
    let record = RecordSelector::parse("text:url").expect("record selector should parse");
    let cases = [
        (
            "offchain_lookup_required",
            Some(json!({ "classification": "unsupported" })),
        ),
        ("operator_controlled_reason", None),
    ];

    for (message, data) in cases {
        let decoded = decode_call_result(
            &record,
            ResolutionResultAbi::EnsUniversalResolver,
            JsonRpcCallResult {
                request_payload: json!({ "jsonrpc": "2.0", "id": 1, "method": "eth_call" }),
                response_payload: json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "error": { "code": i64::MIN, "message": message }
                }),
                result: Err(JsonRpcCallError {
                    code: Some(i64::MIN),
                    message: message.to_owned(),
                    data,
                }),
            },
            false,
            false,
            &[],
        )
        .expect("provider error should produce a record outcome");

        assert_eq!(decoded.result.status, LookupRecordStatus::ExecutionFailed);
        assert_eq!(
            decoded.result.failure_reason.as_deref(),
            Some("resolver_call_failed")
        );
        assert_eq!(decoded.result.unsupported_reason, None);
        assert_eq!(decoded.effective_resolver, None);
    }
}

#[test]
fn typed_internal_lookup_outcomes_preserve_existing_classification() {
    let record = RecordSelector::parse("text:url").expect("record selector should parse");
    let cases = [
        (
            ResolvedCallResult::InternalUnsupported("offchain_lookup_required"),
            LookupRecordStatus::Unsupported,
            None,
            Some("offchain_lookup_required"),
        ),
        (
            ResolvedCallResult::InternalFailure("resolver_call_failed"),
            LookupRecordStatus::ExecutionFailed,
            Some("resolver_call_failed"),
            None,
        ),
        (
            ResolvedCallResult::InternalFailure("ccip_read_failed"),
            LookupRecordStatus::ExecutionFailed,
            Some("ccip_read_failed"),
            None,
        ),
    ];

    for (resolved, status, failure_reason, unsupported_reason) in cases {
        let decoded = decode_resolved_call_result(
            &record,
            ResolutionResultAbi::EnsUniversalResolver,
            resolved,
            true,
            false,
            &[],
        )
        .expect("internal result should produce a record outcome");

        assert_eq!(decoded.result.status, status);
        assert_eq!(decoded.result.failure_reason.as_deref(), failure_reason);
        assert_eq!(
            decoded.result.unsupported_reason.as_deref(),
            unsupported_reason
        );
        assert!(decoded.result.ccip_read);
        assert_eq!(decoded.effective_resolver, None);
    }
}
