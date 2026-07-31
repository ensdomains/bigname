use std::{collections::BTreeMap, time::Duration};

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
        let status = response.status();
        let body = response
            .text()
            .await
            .context("failed to read JSON-RPC response")?;
        if !status.is_success() {
            bail!(
                "provider request failed with HTTP {status}: {}",
                truncate(&body)
            );
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
        bail!("provider returned JSON-RPC error for {method}: {code}: {message}");
    }
    let result = response.get("result").cloned().unwrap_or(Value::Null);
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
    ["http 429", "http 500", "http 502", "http 503", "http 504"]
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
