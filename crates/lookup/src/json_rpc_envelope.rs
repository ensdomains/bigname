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

impl std::fmt::Display for JsonRpcCallError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for JsonRpcCallError {}

/// Message fragments this code treats as "the endpoint cannot serve the requested block or its
/// state". The list is a heuristic chosen here, not a statement about any client or provider.
const BLOCK_UNAVAILABLE_MESSAGES: &[&str] = &[
    "header not found",
    "header for hash not found",
    "block not found",
    "unknown block",
    "missing trie node",
    "state not available",
    "state is not available",
    "historical state",
    "pruned",
    "not currently canonical",
];

/// The error code this code treats the same way, whatever the message.
const RESOURCE_NOT_FOUND: i64 = -32001;

/// Whether a failed call carries a JSON-RPC error this code treats as "block unavailable", as
/// opposed to failing on what the call holds. This is a heuristic over the error code and
/// message. A miss falls back to the caller's ordinary failure handling; a false match only
/// stops hydration's pass for that one block. A transport failure or timeout never matches.
pub fn rpc_error_reports_block_unavailable(error: &anyhow::Error) -> bool {
    error
        .chain()
        .filter_map(|cause| cause.downcast_ref::<JsonRpcCallError>())
        .any(|error| {
            let message = error.message.to_ascii_lowercase();
            error.code == Some(RESOURCE_NOT_FOUND)
                || BLOCK_UNAVAILABLE_MESSAGES
                    .iter()
                    .any(|known| message.contains(known))
        })
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
