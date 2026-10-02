use std::{collections::BTreeMap, time::Duration};

use anyhow::{Context, Result, bail};
use reqwest::Url;
use serde_json::{Value, json};
use tracing::warn;

use super::{JsonRpcProvider, RpcCountKind, redacted_text};

const MAX_ATTEMPTS: usize = 5;

#[derive(Clone, Debug)]
pub(super) struct BatchCall {
    pub method: &'static str,
    pub params: Vec<Value>,
}

/// Data attempts are sent only on a client the RPC chain check verified; the check's own probes
/// are not.
#[derive(Clone, Copy, Eq, PartialEq)]
enum Dispatch {
    Data,
    Probe,
}

impl JsonRpcProvider {
    pub(super) async fn request(&self, method: &str, params: Vec<Value>) -> Result<Option<Value>> {
        self.request_with(method, params, Dispatch::Data).await
    }

    pub(super) async fn probe(&self, method: &str, params: Vec<Value>) -> Result<Option<Value>> {
        self.request_with(method, params, Dispatch::Probe).await
    }

    async fn request_with(
        &self,
        method: &str,
        params: Vec<Value>,
        dispatch: Dispatch,
    ) -> Result<Option<Value>> {
        for attempt in 0..MAX_ATTEMPTS {
            match self.request_once(method, params.clone(), dispatch).await {
                Ok(value) => return Ok(value),
                Err(error) if retryable(&error) && attempt + 1 < MAX_ATTEMPTS => {
                    warn!(
                        component = "ingest_provider",
                        method,
                        attempt = attempt + 1,
                        error = %redacted_text(&error, self.endpoint.as_str()),
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
        self.ensure_chain().await?;
        if self.config.rpc_batch_size() == 1 {
            let mut values = Vec::with_capacity(calls.len());
            for call in calls {
                values.push(self.request(call.method, call.params).await?);
            }
            return Ok(values);
        }
        for attempt in 0..MAX_ATTEMPTS {
            match self.batch_once(&calls).await {
                Ok(values) => return Ok(values),
                Err(error) if retryable(&error) && attempt + 1 < MAX_ATTEMPTS => {
                    warn!(
                        component = "ingest_provider",
                        request_context = "batch",
                        attempt = attempt + 1,
                        error = %redacted_text(&error, self.endpoint.as_str()),
                        "retrying transient JSON-RPC batch"
                    );
                    backoff(attempt).await;
                }
                Err(error) if super::chain_check::chain_mismatch_in(&error).is_some() => {
                    return Err(error);
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

    async fn request_once(
        &self,
        method: &str,
        params: Vec<Value>,
        dispatch: Dispatch,
    ) -> Result<Option<Value>> {
        self.counters.add(RpcCountKind::Calls, [method]);
        let body = self
            .send(
                json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": method,
                    "params": params,
                }),
                dispatch,
            )
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
        self.counters
            .add(RpcCountKind::Calls, calls.iter().map(|call| call.method));
        let body = self.send(request, Dispatch::Data).await?;
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

    async fn send(&self, request: Value, dispatch: Dispatch) -> Result<Value> {
        // Bound actual HTTP work, including retries and standalone fallback calls.
        // Release the permit after the body is read, before any retry backoff.
        let (permit, client, client_id) = loop {
            let check = match dispatch {
                Dispatch::Data => self.ensure_chain().await?,
                Dispatch::Probe => None,
            };
            let permit = self.in_flight.acquire().await?;
            let (client, client_id) = self.client.snapshot();
            if check.is_none_or(|check| check.covers(client_id)) {
                break (permit, client, client_id);
            }
        };
        self.request_attempts
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let body = self.post(&client, client_id, &request, permit).await;
        self.counters.add(
            RpcCountKind::Requests,
            [if body.is_ok() { "ok" } else { "failed" }],
        );
        body
    }

    async fn post(
        &self,
        client: &reqwest::Client,
        client_id: u64,
        request: &Value,
        permit: tokio::sync::SemaphorePermit<'_>,
    ) -> Result<Value> {
        let response = match client
            .post(self.endpoint.clone())
            .json(request)
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
        let status = response.status();
        let body = response
            .text()
            .await
            .context("failed to read JSON-RPC response")?;
        drop(permit);
        if !status.is_success() {
            return Err(HttpStatusError {
                status,
                body: truncate(&body).to_owned(),
            }
            .into());
        }
        serde_json::from_str(&body).context("failed to decode JSON-RPC response")
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
        return Err(JsonRpcError {
            method: method.to_owned(),
            code,
            message: message.to_owned(),
        }
        .into());
    }
    let result = response.get("result").cloned().unwrap_or(Value::Null);
    Ok((!result.is_null()).then_some(result))
}

pub(super) fn retryable(error: &anyhow::Error) -> bool {
    if super::chain_check::chain_mismatch_in(error).is_some() {
        return false;
    }
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
            // dRPC answers HTTP 400 when no node it routes to serves the request right now.
            "route your request to suitable provider",
            "provider block hashes changed during range log lookup",
            "provider block disappeared during range log lookup",
            "provider returned log outside resolved block",
            "provider omitted receipt for selected transaction",
            "provider omitted transaction for selected log",
            "provider returned a pending transaction for selected log",
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

/// A non-success HTTP answer. Its body can echo the request URI or a key taken from it, so
/// redacted renderings leave the body out.
#[derive(Debug)]
pub(super) struct HttpStatusError {
    status: reqwest::StatusCode,
    body: String,
}

impl HttpStatusError {
    pub(super) fn without_body(&self) -> String {
        format!("provider request failed with HTTP {}", self.status)
    }
}

impl std::fmt::Display for HttpStatusError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.without_body(), self.body)
    }
}

impl std::error::Error for HttpStatusError {}

/// A JSON-RPC error's message is provider text that can echo the endpoint's key; redacted
/// renderings keep only its code and the code's standard meaning.
#[derive(Debug)]
pub(super) struct JsonRpcError {
    method: String,
    code: i64,
    message: String,
}

impl JsonRpcError {
    /// Names the standard meaning of the code (JSON-RPC 2.0 and EIP-1474) in place of the
    /// provider's own message.
    pub(super) fn without_message(&self) -> String {
        let meaning = match self.code {
            -32700 => "parse error",
            -32600 => "invalid request",
            -32601 => "method not found",
            -32602 => "invalid params",
            -32603 => "internal error",
            -32000 => "invalid input or server error",
            -32001 => "resource not found",
            -32002 => "resource unavailable",
            -32003 => "transaction rejected",
            -32004 => "method not supported",
            -32005 => "limit exceeded",
            -32006 => "JSON-RPC version not supported",
            _ => "provider-specific error",
        };
        format!(
            "provider returned JSON-RPC error for {}: {} ({meaning}; provider message omitted)",
            self.method, self.code
        )
    }
}

impl std::fmt::Display for JsonRpcError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "provider returned JSON-RPC error for {}: {}: {}",
            self.method, self.code, self.message
        )
    }
}

impl std::error::Error for JsonRpcError {}

fn truncate(body: &str) -> &str {
    let end = body
        .char_indices()
        .nth(512)
        .map(|(index, _)| index)
        .unwrap_or(body.len());
    &body[..end]
}

pub(super) async fn backoff(attempt: usize) {
    let delay = 250_u64.saturating_mul(1_u64 << attempt.min(4));
    tokio::time::sleep(Duration::from_millis(delay)).await;
}

pub(super) fn validate_endpoint(endpoint: &str) -> Result<Url> {
    let endpoint = Url::parse(endpoint).context("failed to parse RPC endpoint URL")?;
    if !matches!(endpoint.scheme(), "http" | "https") {
        bail!("RPC endpoint must use http:// or https://");
    }
    Ok(endpoint)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_json_rpc_error_renders_like_its_provider_text_until_redacted() {
        let error = |code| JsonRpcError {
            method: "eth_getLogs".to_owned(),
            code,
            message: "query returned more than 10000 results".to_owned(),
        };
        assert_eq!(
            error(-32005).to_string(),
            "provider returned JSON-RPC error for eth_getLogs: -32005: query returned more than \
             10000 results"
        );
        assert!(retryable(&error(-32005).into()));
        assert_eq!(
            error(-39999).without_message(),
            "provider returned JSON-RPC error for eth_getLogs: -39999 (provider-specific error; \
             provider message omitted)"
        );
    }

    #[test]
    fn mid_fetch_reorg_races_are_retryable() {
        for message in [
            "provider block hashes changed during range log lookup",
            "provider block disappeared during range log lookup: 10",
            "provider returned log outside resolved block 10 0xabc",
            "Reth DB block hashes changed during log lookup",
        ] {
            assert!(retryable(&anyhow::anyhow!(message)), "{message}");
        }
    }

    #[test]
    fn missing_initial_blocks_and_malformed_headers_are_not_retryable() {
        for message in ["provider omitted block 10", "block number is missing"] {
            assert!(!retryable(&anyhow::anyhow!(message)), "{message}");
        }
    }

    #[test]
    fn a_hosted_routing_refusal_is_retryable() {
        assert!(retryable(&anyhow::anyhow!(
            "provider request failed with HTTP 400 Bad Request: {{\"message\":\"Can't route your \
             request to suitable provider, if you specified certain providers revise the list\"}}"
        )));
        assert!(!retryable(&anyhow::anyhow!(
            "provider request failed with HTTP 400 Bad Request: invalid JSON"
        )));
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
