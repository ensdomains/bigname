//! Kept out of the parent module so a test edit does not rotate the interpreter content hash:
//! the parent is a covered semantic source and is hashed whole.

use super::*;

fn raw(json: &str) -> Box<serde_json::value::RawValue> {
    serde_json::value::RawValue::from_string(json.to_owned()).expect("raw value must parse")
}

#[test]
fn a_success_payload_is_the_answer_and_a_failure_is_not() {
    let (_, result) = classify_response(
        "eth_call",
        ResponsePacket::Single(alloy_json_rpc::Response {
            id: alloy_json_rpc::Id::Number(1),
            payload: ResponsePayload::Success(raw("\"0x2a\"")),
        }),
    )
    .expect("a single response must classify");
    assert_eq!(result, Ok(Value::String("0x2a".to_owned())));

    let (_, result) = classify_response(
        "eth_call",
        ResponsePacket::Single(alloy_json_rpc::Response {
            id: alloy_json_rpc::Id::Number(1),
            payload: ResponsePayload::Failure(alloy_json_rpc::ErrorPayload {
                code: -32000,
                message: "execution reverted".into(),
                data: None,
            }),
        }),
    )
    .expect("a failure response must classify");
    assert_eq!(
        result,
        Err(JsonRpcCallError {
            code: Some(-32000),
            message: "execution reverted".to_owned(),
            data: None,
        })
    );
}

#[test]
fn a_batch_reply_is_not_an_answer_to_a_single_request() {
    // Taking an element out of a batch here would widen what counts as an answer, which is why
    // this decision lives in the hashed module rather than in transport.
    let error = classify_response("eth_call", ResponsePacket::Batch(Vec::new()))
        .expect_err("a batch reply must not classify");
    assert!(error.to_string().contains("batch response"), "{error}");
}

fn failed_call(code: i64, message: &str) -> anyhow::Error {
    anyhow::Error::new(JsonRpcCallError {
        code: Some(code),
        message: message.to_owned(),
        data: None,
    })
    .context("ENS reverse-name Multicall3 eth_call failed")
}

#[test]
fn only_an_error_naming_the_block_or_its_state_reports_the_block_unavailable() {
    for (code, message) in [
        (-32000, "header not found"),
        (
            -32000,
            "missing trie node 5f1c (path ) state 0x5f1c is not available",
        ),
        (-32000, "hash 0x01 is not currently canonical"),
        (-32001, "requested resource not found"),
        (-32602, "Unknown block"),
    ] {
        let error = failed_call(code, message);
        assert!(rpc_error_reports_block_unavailable(&error), "{message}");
        assert_eq!(
            format!("{error:#}"),
            format!("ENS reverse-name Multicall3 eth_call failed: {message}")
        );
    }
    for error in [
        failed_call(-32000, "execution reverted"),
        failed_call(-32000, "out of gas"),
        failed_call(-32005, "response size exceeded"),
        anyhow::anyhow!("header not found").context("failed to send JSON-RPC request"),
    ] {
        assert!(!rpc_error_reports_block_unavailable(&error), "{error:#}");
    }
}
