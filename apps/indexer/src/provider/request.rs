use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::Request;
use serde_json::{Value, json};

use super::{
    JsonRpcProvider,
    payload_cache::{JsonRpcPayloadFingerprint, JsonRpcResultPayload},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct JsonRpcBatchCall {
    pub(super) method: &'static str,
    pub(super) params: Vec<Value>,
}

impl JsonRpcProvider {
    pub(super) async fn fetch_json_rpc_result(
        &self,
        method: &str,
        params: Vec<Value>,
    ) -> Result<Option<Value>> {
        Ok(self
            .fetch_json_rpc_result_with_payload(method, params)
            .await?
            .result)
    }

    pub(super) async fn fetch_json_rpc_result_with_payload(
        &self,
        method: &str,
        params: Vec<Value>,
    ) -> Result<JsonRpcResultPayload> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });
        let body = self.send_json_rpc_payload(method, payload).await?;

        let fingerprint = JsonRpcPayloadFingerprint::for_body(&body)?;
        let response = serde_json::from_slice::<JsonRpcResponse>(&body)
            .context("failed to decode JSON-RPC response")?;
        if let Some(error) = response.error {
            bail!(
                "provider returned JSON-RPC error {}: {}",
                error.code,
                error.message
            );
        }

        Ok(JsonRpcResultPayload {
            result: response.result,
            fingerprint,
        })
    }

    pub(super) async fn fetch_json_rpc_batch_results(
        &self,
        calls: Vec<JsonRpcBatchCall>,
    ) -> Result<Vec<Option<Value>>> {
        if calls.is_empty() {
            return Ok(Vec::new());
        }

        match self.try_fetch_json_rpc_batch_results(&calls).await {
            Ok(results) => Ok(results),
            Err(batch_error) => {
                let mut results = Vec::with_capacity(calls.len());
                for call in calls {
                    let method = call.method;
                    let result = self
                        .fetch_json_rpc_result(method, call.params)
                        .await
                        .with_context(|| {
                            format!(
                                "provider JSON-RPC batch failed ({batch_error}); individual retry for {} also failed",
                                method
                            )
                        })?;
                    results.push(result);
                }
                Ok(results)
            }
        }
    }

    async fn try_fetch_json_rpc_batch_results(
        &self,
        calls: &[JsonRpcBatchCall],
    ) -> Result<Vec<Option<Value>>> {
        let payload = Value::Array(
            calls
                .iter()
                .enumerate()
                .map(|(index, call)| {
                    json!({
                        "jsonrpc": "2.0",
                        "id": index + 1,
                        "method": call.method,
                        "params": call.params.clone(),
                    })
                })
                .collect(),
        );
        let body = self.send_json_rpc_payload("batch", payload).await?;
        let response_value = serde_json::from_slice::<Value>(&body)
            .context("failed to decode JSON-RPC batch response")?;
        let response_values = response_value
            .as_array()
            .context("expected JSON-RPC batch response array")?;
        let expected_methods = calls
            .iter()
            .enumerate()
            .map(|(index, call)| ((index + 1) as i64, call.method))
            .collect::<BTreeMap<_, _>>();
        let mut results_by_id = BTreeMap::<i64, Option<Value>>::new();

        for response_value in response_values {
            let response = serde_json::from_value::<JsonRpcResponse>(response_value.clone())
                .context("failed to decode JSON-RPC batch response item")?;
            let id = response.response_id()?;
            let method = expected_methods
                .get(&id)
                .with_context(|| format!("provider returned unexpected JSON-RPC batch id {id}"))?;
            if let Some(error) = response.error {
                bail!(
                    "provider returned JSON-RPC error for batched {method} id {id}: {}: {}",
                    error.code,
                    error.message
                );
            }
            if results_by_id.insert(id, response.result).is_some() {
                bail!("provider returned duplicate JSON-RPC batch response id {id}");
            }
        }

        let mut results = Vec::with_capacity(calls.len());
        for id in 1..=calls.len() as i64 {
            results.push(
                results_by_id
                    .remove(&id)
                    .with_context(|| format!("provider omitted JSON-RPC batch response id {id}"))?,
            );
        }
        if !results_by_id.is_empty() {
            bail!("provider returned extra JSON-RPC batch responses");
        }

        Ok(results)
    }

    async fn send_json_rpc_payload(&self, request_context: &str, payload: Value) -> Result<Bytes> {
        let request = Request::post(self.endpoint.clone())
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(payload.to_string())))
            .context("failed to build JSON-RPC request")?;
        let response =
            self.client.request(request).await.with_context(|| {
                format!("failed to send JSON-RPC request for {request_context}")
            })?;
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .context("failed to read JSON-RPC response body")?
            .to_bytes();

        if !status.is_success() {
            let response_body = String::from_utf8_lossy(&body);
            bail!(
                "provider request for {request_context} failed with HTTP {status}: {response_body}"
            );
        }

        Ok(body)
    }
}
#[derive(Debug)]
struct JsonRpcResponse {
    id: Option<Value>,
    result: Option<Value>,
    error: Option<JsonRpcError>,
}

impl JsonRpcResponse {
    fn response_id(&self) -> Result<i64> {
        self.id
            .as_ref()
            .and_then(Value::as_i64)
            .context("missing or non-integer JSON-RPC response id")
    }
}

impl<'de> serde::Deserialize<'de> for JsonRpcResponse {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        struct RawJsonRpcResponse {
            id: Option<Value>,
            result: Option<Value>,
            error: Option<JsonRpcError>,
        }

        let raw = RawJsonRpcResponse::deserialize(deserializer)?;
        Ok(Self {
            id: raw.id,
            result: raw.result,
            error: raw.error,
        })
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
struct JsonRpcError {
    code: i64,
    message: String,
}
