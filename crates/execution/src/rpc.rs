use std::{collections::BTreeMap, sync::LazyLock};

use alloy_json_rpc::{
    Id, Request as JsonRpcRequest, RequestPacket, ResponsePacket, ResponsePayload,
};
use alloy_transport_http::Http;
use anyhow::{Context, Result, bail};
use reqwest::Url;
use serde_json::Value;
use tower::Service;

static JSON_RPC_HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(reqwest::Client::new);

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

    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.urls
            .iter()
            .map(|(chain_id, url)| (chain_id.as_str(), url.as_str()))
    }
}

pub async fn fetch_network_head_block_number(endpoint: &str) -> Result<i64> {
    let response = JsonRpcHttpClient::new(endpoint)?
        .call("eth_blockNumber", Vec::new())
        .await?;
    let result = response.result.map_err(|error| {
        anyhow::anyhow!(
            "eth_blockNumber failed with provider code {:?}: {}",
            error.code,
            error.message
        )
    })?;
    parse_quantity_i64(&result).context("eth_blockNumber returned an invalid block quantity")
}

fn parse_quantity_i64(value: &Value) -> Result<i64> {
    let quantity = value
        .as_str()
        .context("JSON-RPC quantity must be a string")?;
    let digits = quantity
        .strip_prefix("0x")
        .context("JSON-RPC quantity must start with 0x")?;
    if digits.is_empty() {
        bail!("JSON-RPC quantity must contain hexadecimal digits");
    }
    let value = u64::from_str_radix(digits, 16).context("JSON-RPC quantity is not hexadecimal")?;
    i64::try_from(value).context("JSON-RPC quantity exceeds the supported signed block range")
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
        let endpoint = endpoint
            .parse::<Url>()
            .with_context(|| format!("failed to parse RPC endpoint {endpoint}"))?;
        if !matches!(endpoint.scheme(), "http" | "https") {
            bail!(
                "unsupported RPC endpoint scheme for {endpoint}; on-demand execution supports http:// and https:// URLs"
            );
        }

        Ok(Self {
            transport: Http::with_client(JSON_RPC_HTTP_CLIENT.clone(), endpoint),
        })
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
    fn network_head_quantity_parser_is_strict_and_bounded() -> Result<()> {
        assert_eq!(parse_quantity_i64(&Value::String("0x2a".to_owned()))?, 42);
        assert!(parse_quantity_i64(&Value::String("2a".to_owned())).is_err());
        assert!(parse_quantity_i64(&Value::String("0x".to_owned())).is_err());
        assert!(parse_quantity_i64(&Value::String("0x8000000000000000".to_owned())).is_err());
        Ok(())
    }
}
