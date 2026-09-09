use std::{collections::BTreeMap, time::Duration};

use crate::measurement::{self as memory, Measured};
use anyhow::{Context, Result, bail};
use reqwest::Url;
use serde_json::{Value, json};
use tracing::warn;

use super::{JsonRpcProvider, provider_error_text};

const MAX_ATTEMPTS: usize = 5;

#[derive(Clone, Debug)]
pub(super) struct BatchCall {
    pub method: &'static str,
    pub params: Vec<Value>,
}

impl JsonRpcProvider {
    pub(super) async fn request(&self, method: &str, params: Vec<Value>) -> Result<Option<Value>> {
        for attempt in 0..MAX_ATTEMPTS {
            match self.request_once(method, params.clone()).await {
                Ok(value) => return Ok(value),
                Err(error) if retryable(&error) && attempt + 1 < MAX_ATTEMPTS => {
                    warn!(
                        component = "ingest_provider",
                        method,
                        attempt = attempt + 1,
                        error = %provider_error_text(&error),
                        "retrying transient JSON-RPC request"
                    );
                    backoff(attempt).await;
                }
                Err(error) => return Err(error),
            }
        }
        bail!("JSON-RPC retry loop exited unexpectedly")
    }

    pub(super) async fn batch(&self, calls: Vec<BatchCall>) -> Result<Vec<Option<Value>>> {
        if calls.is_empty() {
            return Ok(Vec::new());
        }
        for attempt in 0..MAX_ATTEMPTS {
            match self.batch_once(&calls).await {
                Ok(values) => return Ok(values),
                Err(error) if retryable(&error) && attempt + 1 < MAX_ATTEMPTS => {
                    warn!(
                        component = "ingest_provider",
                        request_context = "batch",
                        attempt = attempt + 1,
                        error = %provider_error_text(&error),
                        "retrying transient JSON-RPC batch"
                    );
                    backoff(attempt).await;
                }
                Err(error) if !retryable(&error) => {
                    let mut values = Vec::with_capacity(calls.len());
                    for call in calls {
                        values.push(self.request(call.method, call.params).await.with_context(
                            || format!("batch fallback failed for {}", call.method),
                        )?);
                    }
                    return Ok(values);
                }
                Err(error) => return Err(error),
            }
        }
        bail!("JSON-RPC batch retry loop exited unexpectedly")
    }

    async fn request_once(&self, method: &str, params: Vec<Value>) -> Result<Option<Value>> {
        let body = self
            .send(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params,
            }))
            .await?;
        response_result(&body, method)
    }

    async fn batch_once(&self, calls: &[BatchCall]) -> Result<Vec<Option<Value>>> {
        let request = Value::Array(
            calls
                .iter()
                .enumerate()
                .map(|(index, call)| {
                    json!({
                        "jsonrpc": "2.0",
                        "id": index + 1,
                        "method": call.method,
                        "params": call.params,
                    })
                })
                .collect(),
        );
        let body = self.send(request).await?;
        let responses = body
            .as_array()
            .context("expected JSON-RPC batch response array")?;
        let mut by_id = BTreeMap::new();
        for response in responses {
            let id = response
                .get("id")
                .and_then(Value::as_u64)
                .context("JSON-RPC batch response has no integer id")?;
            if by_id
                .insert(id, response_result(response, "batch")?)
                .is_some()
            {
                bail!("provider returned duplicate JSON-RPC batch id {id}");
            }
        }
        (1..=calls.len() as u64)
            .map(|id| {
                by_id
                    .remove(&id)
                    .with_context(|| format!("provider omitted JSON-RPC batch id {id}"))
            })
            .collect()
    }

    async fn send(&self, request: Value) -> Result<Value> {
        self.request_attempts
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut observation = SendObservation::new(&request);
        let (client, client_id) = self.client.snapshot();
        let response = match client
            .post(self.endpoint.clone())
            .json(&request)
            .send()
            .await
        {
            Ok(response) => response,
            Err(mut error) => {
                redact_url(&mut error);
                self.client.record_error(client_id, &error)?;
                return Err(error).context("failed to send JSON-RPC request");
            }
        };
        observation.outcome = "read_failed_or_cancelled";
        let status = response.status();
        let body = response
            .text()
            .await
            .context("failed to read JSON-RPC response")?;
        observation.bytes = Some(body.len() as u64);
        observation.outcome = "http_error";
        memory::transport(body.len());
        memory::observe("rpc_response_text", || body.footprint());
        if !status.is_success() {
            bail!(
                "provider request failed with HTTP {status}: {}",
                truncate(&body)
            );
        }
        observation.outcome = "decode_error";
        serde_json::from_str::<Value>(&body)
            .inspect(|parsed| {
                if observation.context.is_some() {
                    observation.outcome = if parsed.get("error").is_some()
                        || parsed.as_array().is_some_and(|items| {
                            items.iter().any(|item| item.get("error").is_some())
                        }) {
                        "rpc_error"
                    } else {
                        "success"
                    };
                }
                memory::observe("rpc_text_and_json", || {
                    body.footprint().combine(parsed.footprint())
                })
            })
            .context("failed to decode JSON-RPC response")
    }
}

// No request or response payload is retained by this cancellation-safe diagnostic guard.
struct SendObservation {
    context: Option<(memory::Context, u64)>,
    method: &'static str,
    range: (Option<u64>, Option<u64>),
    outcome: &'static str,
    bytes: Option<u64>,
}
impl SendObservation {
    fn new(request: &Value) -> Self {
        let context = memory::transport_start();
        let method = match request.get("method").and_then(Value::as_str) {
            Some("eth_getLogs") => "eth_getLogs",
            Some("eth_getBlockByNumber") => "eth_getBlockByNumber",
            Some("eth_getBlockByHash") => "eth_getBlockByHash",
            Some("eth_getBlockReceipts") => "eth_getBlockReceipts",
            Some("eth_getTransactionReceipt") => "eth_getTransactionReceipt",
            _ if request.is_array() => "batch",
            _ => "other",
        };
        let number = |key| {
            request
                .pointer(key)
                .and_then(Value::as_str)
                .and_then(|s| s.strip_prefix("0x"))
                .and_then(|s| u64::from_str_radix(s, 16).ok())
        };
        Self {
            context,
            method,
            range: (number("/params/0/fromBlock"), number("/params/0/toBlock")),
            outcome: "send_failed_or_cancelled",
            bytes: None,
        }
    }
}
impl Drop for SendObservation {
    fn drop(&mut self) {
        if let Some((context, ordinal)) = &self.context {
            memory::transport_outcome(
                context,
                *ordinal,
                self.method,
                self.range,
                self.outcome,
                self.bytes,
            );
        }
    }
}

fn response_result(response: &Value, method: &str) -> Result<Option<Value>> {
    if let Some(error) = response.get("error") {
        let code = error
            .get("code")
            .and_then(Value::as_i64)
            .unwrap_or_default();
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("unknown JSON-RPC error");
        bail!("provider returned JSON-RPC error for {method}: {code}: {message}");
    }
    let result = response.get("result").cloned().unwrap_or(Value::Null);
    memory::observe("rpc_json_and_result_clone", || {
        response.footprint().combine(result.footprint())
    });
    Ok((!result.is_null()).then_some(result))
}

pub(super) fn retryable(error: &anyhow::Error) -> bool {
    if error.chain().any(|cause| {
        cause
            .downcast_ref::<reqwest::Error>()
            .is_some_and(|error| error.is_connect() || error.is_timeout())
    }) {
        return true;
    }
    let message = format!("{error:#}").to_ascii_lowercase();
    [
        "http 429", "http 500", "http 502", "http 503", "http 504", "http 520", "http 521",
        "http 522", "http 524",
    ]
    .iter()
    .any(|needle| message.contains(needle))
        || [
            "too many requests",
            "rate limit",
            "retry later",
            "temporarily unavailable",
            "service unavailable",
            "bad gateway",
            "gateway timeout",
            "timed out",
            "timeout",
            "connection reset",
            "connection closed",
            "provider block hashes changed during range log lookup",
            "provider returned log outside resolved block",
            "reth db block hashes changed during log lookup",
            "-32005",
        ]
        .iter()
        .any(|needle| message.contains(needle))
}

fn redact_url(error: &mut reqwest::Error) {
    let Some(url) = error.url_mut() else {
        return;
    };
    let _ = url.set_username("");
    let _ = url.set_password(None);
    let _ = url.set_port(None);
    url.set_path("");
    url.set_query(None);
    url.set_fragment(None);
}

fn truncate(body: &str) -> &str {
    let end = body
        .char_indices()
        .nth(512)
        .map(|(index, _)| index)
        .unwrap_or(body.len());
    &body[..end]
}

async fn backoff(attempt: usize) {
    let delay = 250_u64.saturating_mul(1_u64 << attempt.min(4));
    tokio::time::sleep(Duration::from_millis(delay)).await;
}

pub(super) fn validate_endpoint(endpoint: &str) -> Result<Url> {
    let endpoint =
        Url::parse(endpoint).with_context(|| format!("failed to parse RPC endpoint {endpoint}"))?;
    if !matches!(endpoint.scheme(), "http" | "https") {
        bail!("RPC endpoint must use http:// or https://");
    }
    Ok(endpoint)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mid_fetch_reorg_races_are_retryable() {
        for message in [
            "provider block hashes changed during range log lookup",
            "provider returned log outside resolved block 10 0xabc",
            "Reth DB block hashes changed during log lookup",
        ] {
            assert!(retryable(&anyhow::anyhow!(message)), "{message}");
        }
    }

    #[test]
    fn hosted_rpc_edge_gateway_statuses_are_retryable() {
        for status in [520, 521, 522, 524] {
            assert!(
                retryable(&anyhow::anyhow!(
                    "provider request failed with HTTP {status}"
                )),
                "HTTP {status}"
            );
        }
    }
}
