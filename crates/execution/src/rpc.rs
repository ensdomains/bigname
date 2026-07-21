use std::{collections::BTreeMap, sync::LazyLock, time::Duration};

use alloy_json_rpc::{
    Id, Request as JsonRpcRequest, RequestPacket, ResponsePacket, ResponsePayload,
};
use alloy_transport_http::Http;
use anyhow::{Context, Result, anyhow, bail};
use reqwest::Url;
use serde_json::Value;
use tower::Service;

const RPC_CONNECT_TIMEOUT_ENV: &str = "BIGNAME_API_RPC_CONNECT_TIMEOUT_MS";
const RPC_TOTAL_TIMEOUT_ENV: &str = "BIGNAME_API_RPC_TIMEOUT_MS";
const DEFAULT_RPC_CONNECT_TIMEOUT_MS: u64 = 2_000;
const DEFAULT_RPC_TOTAL_TIMEOUT_MS: u64 = 8_000;

static JSON_RPC_HTTP_CLIENT: LazyLock<std::result::Result<reqwest::Client, String>> =
    LazyLock::new(|| {
        RpcHttpTimeouts::from_env()
            .and_then(build_json_rpc_http_client)
            .map_err(|error| error.to_string())
    });

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RpcHttpTimeouts {
    connect: Duration,
    total: Duration,
}

impl RpcHttpTimeouts {
    fn from_env() -> Result<Self> {
        Self::from_millis(
            timeout_millis_from_env(RPC_CONNECT_TIMEOUT_ENV, DEFAULT_RPC_CONNECT_TIMEOUT_MS)?,
            timeout_millis_from_env(RPC_TOTAL_TIMEOUT_ENV, DEFAULT_RPC_TOTAL_TIMEOUT_MS)?,
        )
    }

    fn from_millis(connect: u64, total: u64) -> Result<Self> {
        if connect == 0 {
            bail!("{RPC_CONNECT_TIMEOUT_ENV} must be greater than zero");
        }
        if total == 0 {
            bail!("{RPC_TOTAL_TIMEOUT_ENV} must be greater than zero");
        }
        if connect > total {
            bail!("{RPC_CONNECT_TIMEOUT_ENV} must not exceed {RPC_TOTAL_TIMEOUT_ENV}");
        }
        Ok(Self {
            connect: Duration::from_millis(connect),
            total: Duration::from_millis(total),
        })
    }
}

fn timeout_millis_from_env(name: &str, default: u64) -> Result<u64> {
    match std::env::var(name) {
        Ok(value) => value
            .parse::<u64>()
            .with_context(|| format!("{name} must be an integer number of milliseconds")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(anyhow!(error)).with_context(|| format!("failed to read {name}")),
    }
}

fn build_json_rpc_http_client(timeouts: RpcHttpTimeouts) -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .connect_timeout(timeouts.connect)
        .timeout(timeouts.total)
        .build()
        .context("failed to configure execution JSON-RPC HTTP client")
}

pub fn validate_rpc_http_client_config() -> Result<()> {
    JSON_RPC_HTTP_CLIENT
        .as_ref()
        .map(|_| ())
        .map_err(|error| anyhow!(error.clone()))
}

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
    transport: Http<reqwest::Client>,
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
    pub(crate) data: Option<Value>,
}

impl JsonRpcHttpClient {
    pub(crate) fn new(endpoint: &str) -> Result<Self> {
        let client = JSON_RPC_HTTP_CLIENT
            .as_ref()
            .map_err(|error| anyhow!(error.clone()))?
            .clone();
        Self::with_client(endpoint, client)
    }

    fn with_client(endpoint: &str, client: reqwest::Client) -> Result<Self> {
        let endpoint = endpoint
            .parse::<Url>()
            .with_context(|| format!("failed to parse RPC endpoint {endpoint}"))?;
        if !matches!(endpoint.scheme(), "http" | "https") {
            bail!(
                "unsupported RPC endpoint scheme for {endpoint}; on-demand execution supports http:// and https:// URLs"
            );
        }

        Ok(Self {
            transport: Http::with_client(client, endpoint),
        })
    }

    #[cfg(test)]
    pub(crate) fn new_with_timeouts(
        endpoint: &str,
        connect_timeout: Duration,
        total_timeout: Duration,
    ) -> Result<Self> {
        let connect_ms = u64::try_from(connect_timeout.as_millis())
            .context("test RPC connect timeout does not fit u64 milliseconds")?;
        let total_ms = u64::try_from(total_timeout.as_millis())
            .context("test RPC total timeout does not fit u64 milliseconds")?;
        let client =
            build_json_rpc_http_client(RpcHttpTimeouts::from_millis(connect_ms, total_ms)?)?;
        Self::with_client(endpoint, client)
    }

    pub(crate) async fn call(&self, method: &str, params: Vec<Value>) -> Result<JsonRpcCallResult> {
        let request = JsonRpcRequest::new(method.to_owned(), Id::Number(1), params)
            .serialize()
            .context("failed to encode JSON-RPC request")?;
        let request_payload = serde_json::from_str(request.serialized().get())
            .context("failed to decode serialized JSON-RPC request")?;
        let (response_payload, result) = self.send_json_rpc_request(method, request).await?;

        Ok(JsonRpcCallResult {
            request_payload,
            response_payload,
            result,
        })
    }

    async fn send_json_rpc_request(
        &self,
        request_context: &str,
        request: alloy_json_rpc::SerializedRequest,
    ) -> Result<(Value, std::result::Result<Value, JsonRpcCallError>)> {
        let mut transport = self.transport.clone();
        let response = transport
            .call(RequestPacket::Single(request))
            .await
            .with_context(|| format!("failed to send JSON-RPC request for {request_context}"))?;
        let ResponsePacket::Single(response) = response else {
            bail!(
                "provider returned a batch response for single JSON-RPC request {request_context}"
            );
        };
        let response_payload =
            serde_json::to_value(&response).context("failed to encode JSON-RPC response")?;
        let result =
            match response.payload {
                ResponsePayload::Success(result) => Ok(raw_value_to_json(result.as_ref())
                    .context("failed to decode JSON-RPC result")?),
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
}

fn raw_value_to_json(value: &serde_json::value::RawValue) -> Result<Value> {
    serde_json::from_str(value.get()).context("failed to decode raw JSON value")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_rpc_client_accepts_https_endpoints() -> Result<()> {
        JsonRpcHttpClient::new("https://rpc.example.test")?;
        Ok(())
    }

    #[test]
    fn rpc_timeout_configuration_rejects_zero_and_inverted_values() {
        assert!(RpcHttpTimeouts::from_millis(0, 8_000).is_err());
        assert!(RpcHttpTimeouts::from_millis(2_000, 0).is_err());
        assert!(RpcHttpTimeouts::from_millis(8_001, 8_000).is_err());
        assert_eq!(
            RpcHttpTimeouts::from_millis(2_000, 8_000).expect("timeouts must parse"),
            RpcHttpTimeouts {
                connect: Duration::from_secs(2),
                total: Duration::from_secs(8),
            }
        );
    }
}
