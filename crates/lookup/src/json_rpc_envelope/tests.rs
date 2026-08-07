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
