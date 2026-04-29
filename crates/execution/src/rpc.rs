use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request, Uri};
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::TokioExecutor,
};
use serde_json::{Value, json};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChainRpcUrls {
    urls: BTreeMap<String, String>,
}

impl ChainRpcUrls {
    pub fn from_entries(entries: &[String]) -> Result<Self> {
        let mut urls = BTreeMap::new();
        for entry in entries {
            for item in entry
                .split(',')
                .map(str::trim)
                .filter(|item| !item.is_empty())
            {
                let (chain, url) = item.split_once('=').with_context(|| {
                    format!("invalid chain RPC entry {item}; expected <chain>=<url>")
                })?;
                let chain = chain.trim();
                let url = url.trim();
                if chain.is_empty() || url.is_empty() {
                    bail!("invalid chain RPC entry {item}; expected non-empty <chain>=<url>");
                }
                if urls.insert(chain.to_owned(), url.to_owned()).is_some() {
                    bail!("duplicate chain RPC entry for {chain}");
                }
            }
        }

        Ok(Self { urls })
    }

    pub fn from_comma_delimited(value: &str) -> Result<Self> {
        Self::from_entries(&[value.to_owned()])
    }

    pub fn is_empty(&self) -> bool {
        self.urls.is_empty()
    }

    pub fn url_for(&self, chain_id: &str) -> Option<&str> {
        self.urls.get(chain_id).map(String::as_str)
    }
}

#[derive(Clone)]
pub(crate) struct JsonRpcHttpClient {
    endpoint: Uri,
    client: Client<HttpConnector, Full<Bytes>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct JsonRpcCallResult {
    pub(crate) request_payload: Value,
    pub(crate) response_payload: Value,
    pub(crate) result: std::result::Result<Value, JsonRpcCallError>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct JsonRpcCallError {
    pub(crate) code: Option<i64>,
    pub(crate) message: String,
}

impl JsonRpcHttpClient {
    pub(crate) fn new(endpoint: &str) -> Result<Self> {
        let endpoint = endpoint
            .parse::<Uri>()
            .with_context(|| format!("failed to parse RPC endpoint {endpoint}"))?;
        if endpoint.scheme_str() != Some("http") {
            bail!(
                "unsupported RPC endpoint scheme for {endpoint}; bootstrap on-demand execution currently supports only http:// URLs"
            );
        }

        let connector = HttpConnector::new();
        let client = Client::builder(TokioExecutor::new()).build(connector);
        Ok(Self { endpoint, client })
    }

    pub(crate) async fn call(&self, method: &str, params: Vec<Value>) -> Result<JsonRpcCallResult> {
        let request_payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });
        let response_payload = self
            .send_json_rpc_payload(method, request_payload.clone())
            .await?;
        let response = decode_json_rpc_response(&response_payload)?;
        let result = if let Some(error) = response.error {
            Err(JsonRpcCallError {
                code: Some(error.code),
                message: error.message,
            })
        } else {
            response.result.ok_or_else(|| JsonRpcCallError {
                code: None,
                message: "JSON-RPC response omitted result".to_owned(),
            })
        };

        Ok(JsonRpcCallResult {
            request_payload,
            response_payload,
            result,
        })
    }

    async fn send_json_rpc_payload(&self, request_context: &str, payload: Value) -> Result<Value> {
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

        serde_json::from_slice::<Value>(&body).context("failed to decode JSON-RPC response")
    }
}

fn decode_json_rpc_response(value: &Value) -> Result<JsonRpcResponse> {
    serde_json::from_value(value.clone()).context("failed to decode JSON-RPC response")
}

#[derive(Debug)]
struct JsonRpcResponse {
    result: Option<Value>,
    error: Option<JsonRpcError>,
}

impl<'de> serde::Deserialize<'de> for JsonRpcResponse {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        struct RawJsonRpcResponse {
            result: Option<Value>,
            error: Option<JsonRpcError>,
        }

        let raw = RawJsonRpcResponse::deserialize(deserializer)?;
        Ok(Self {
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
