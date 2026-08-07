//! JSON-RPC envelope interpretation: which provider response counts as an answer, and what value
//! is taken from it.
//!
//! Split out of `rpc` because project hydration calls through here before rows are persisted —
//! widening what counts as a success turns a batch that aborts today into persisted
//! `primary_names_current` and `record_inventory_current` values — which puts this module inside
//! the interpreter content hash. The rest of `rpc` is HTTP client construction, timeouts, and
//! endpoint configuration, which can only abort a request, plus a head-block read no hydration
//! path calls. Route any new provider-result interpretation hydration reaches through here.

use alloy_json_rpc::{ResponsePacket, ResponsePayload};
use anyhow::{Context, Result, bail};
use serde_json::Value;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct JsonRpcCallResult {
    pub request_payload: Value,
    pub response_payload: Value,
    pub result: std::result::Result<Value, JsonRpcCallError>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct JsonRpcCallError {
    pub code: Option<i64>,
    pub message: String,
    pub data: Option<Value>,
}

type ClassifiedResponse = (Value, std::result::Result<Value, JsonRpcCallError>);

pub(crate) fn classify_response(
    request_context: &str,
    response: ResponsePacket,
) -> Result<ClassifiedResponse> {
    let ResponsePacket::Single(response) = response else {
        bail!("provider returned a batch response for single JSON-RPC request {request_context}");
    };
    let response_payload =
        serde_json::to_value(&response).context("failed to encode JSON-RPC response")?;
    let result = match response.payload {
        ResponsePayload::Success(result) => {
            Ok(raw_value_to_json(result.as_ref()).context("failed to decode JSON-RPC result")?)
        }
        ResponsePayload::Failure(error) => Err(JsonRpcCallError {
            code: Some(error.code),
            message: error.message.into_owned(),
            data: error
                .data
                .as_deref()
                .map(raw_value_to_json)
                .transpose()
                .context("failed to decode JSON-RPC error data")?,
        }),
    };
    Ok((response_payload, result))
}

fn raw_value_to_json(value: &serde_json::value::RawValue) -> Result<Value> {
    serde_json::from_str(value.get()).context("failed to decode raw JSON value")
}

#[cfg(test)]
#[path = "json_rpc_envelope/tests.rs"]
mod tests;
