use std::{sync::LazyLock, time::Duration};

use alloy_primitives::Bytes;
use alloy_sol_types::{SolCall, SolError, SolValue, sol};
use anyhow::{Context, Result, bail};
use reqwest::StatusCode;
use serde_json::{Value, json};

use super::OffchainLookup;
use crate::abi::{hex_string, hex_to_bytes};

const LOCAL_BATCH_GATEWAY_URL: &str = "x-batch-gateway:true";
const MAX_GATEWAY_URLS: usize = 4;
#[cfg(not(test))]
const GATEWAY_CONNECT_TIMEOUT: Duration = Duration::from_millis(1000);
#[cfg(test)]
const GATEWAY_CONNECT_TIMEOUT: Duration = Duration::from_millis(100);
const GATEWAY_TIMEOUT: Duration = Duration::from_millis(1500);

static HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .connect_timeout(GATEWAY_CONNECT_TIMEOUT)
        .timeout(GATEWAY_TIMEOUT)
        .build()
        .expect("CCIP gateway HTTP client configuration must be valid")
});

mod contracts {
    use super::*;

    sol! {
        #[derive(Debug, PartialEq, Eq)]
        struct Request {
            address sender;
            string[] urls;
            bytes data;
        }

        function query(Request[] requests) external view returns (
            bool[] failures,
            bytes[] responses
        );
    }
}

#[derive(Debug)]
pub(crate) struct GatewayError {
    message: String,
    transport: Option<bool>,
}

impl GatewayError {
    pub const fn is_transport_failure(&self) -> bool {
        self.transport.is_some()
    }

    pub const fn is_timeout(&self) -> bool {
        matches!(self.transport, Some(true))
    }
}

impl std::fmt::Display for GatewayError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for GatewayError {}

pub(crate) async fn fetch(lookup: &OffchainLookup) -> std::result::Result<Vec<u8>, GatewayError> {
    fetch_inner(lookup).await.map_err(|error| GatewayError {
        transport: gateway_transport_classification(&error),
        message: format!("failed to complete CCIP-Read gateway request: {error:#}"),
    })
}

async fn fetch_inner(lookup: &OffchainLookup) -> Result<Vec<u8>> {
    if lookup
        .urls
        .iter()
        .any(|url| url.eq_ignore_ascii_case(LOCAL_BATCH_GATEWAY_URL))
    {
        let requests = decode_batch_query(&lookup.call_data)?;
        let results =
            futures_util::future::join_all(requests.into_iter().map(|request| async move {
                fetch_standard(&request.sender, &request.urls, &request.data).await
            }))
            .await;
        let mut failures = Vec::with_capacity(results.len());
        let mut responses = Vec::with_capacity(results.len());
        let mut transport_error = None;
        for result in results {
            match result {
                Ok(response) => {
                    failures.push(false);
                    responses.push(response);
                }
                Err(error) => match retain_transport_error(&mut transport_error, error) {
                    Ok(()) => {}
                    Err(error) => {
                        failures.push(true);
                        responses.push(
                            alloy_sol_types::Revert::from(format!(
                                "CCIP gateway request failed: {error}"
                            ))
                            .abi_encode(),
                        );
                    }
                },
            }
        }
        if let Some((_, error)) = transport_error {
            return Err(error);
        }
        let responses = responses
            .iter()
            .map(|response| Bytes::copy_from_slice(response))
            .collect::<Vec<_>>();
        return Ok((failures, responses).abi_encode_params());
    }
    fetch_standard(&lookup.sender, &lookup.urls, &lookup.call_data).await
}

struct BatchRequest {
    sender: String,
    urls: Vec<String>,
    data: Vec<u8>,
}

fn decode_batch_query(call_data: &[u8]) -> Result<Vec<BatchRequest>> {
    let decoded = contracts::queryCall::abi_decode(call_data)
        .context("batch gateway query calldata malformed")?;
    Ok(decoded
        .requests
        .into_iter()
        .map(|request| BatchRequest {
            sender: hex_string(request.sender.as_slice()),
            urls: request.urls,
            data: request.data.to_vec(),
        })
        .collect())
}

async fn fetch_standard(sender: &str, urls: &[String], call_data: &[u8]) -> Result<Vec<u8>> {
    let data = hex_string(call_data);
    let mut last_error = None;
    let mut transport_error = None;
    for template in urls
        .iter()
        .filter(|url| !url.eq_ignore_ascii_case(LOCAL_BATCH_GATEWAY_URL))
        .take(MAX_GATEWAY_URLS)
    {
        match fetch_one(template, sender, &data).await {
            Ok(response) => return Ok(response),
            Err(error)
                if error
                    .downcast_ref::<GatewayStatusError>()
                    .is_some_and(GatewayStatusError::is_client_error) =>
            {
                return match transport_error {
                    Some((_, transport_error)) => Err(transport_error),
                    None => Err(error),
                };
            }
            Err(error) => match retain_transport_error(&mut transport_error, error) {
                Ok(()) => {}
                Err(error) => last_error = Some(error),
            },
        }
    }

    if let Some((_, error)) = transport_error {
        Err(error)
    } else if let Some(error) = last_error {
        Err(error)
    } else {
        bail!("CCIP-Read supplied no usable HTTP gateway URL")
    }
}

async fn fetch_one(template: &str, sender: &str, data: &str) -> Result<Vec<u8>> {
    let url = template.replace("{sender}", sender);
    let response = if url.contains("{data}") {
        HTTP_CLIENT.get(url.replace("{data}", data)).send().await
    } else {
        HTTP_CLIENT
            .post(&url)
            .json(&json!({ "sender": sender, "data": data }))
            .send()
            .await
    }
    .with_context(|| format!("failed to send CCIP gateway request to {url}"))?;
    let status = response.status();
    let body = response
        .bytes()
        .await
        .with_context(|| format!("failed to read CCIP gateway response from {url}"))?;
    if !status.is_success() {
        return Err(GatewayStatusError { status }.into());
    }
    decode_body(&body).with_context(|| format!("failed to decode CCIP gateway response from {url}"))
}

fn decode_body(body: &[u8]) -> Result<Vec<u8>> {
    if let Ok(value) = serde_json::from_slice::<Value>(body) {
        let payload = match value {
            Value::Object(object) => object
                .get("data")
                .and_then(Value::as_str)
                .map(str::to_owned),
            Value::String(value) => Some(value),
            _ => None,
        };
        if let Some(payload) = payload {
            return hex_to_bytes(&payload);
        }
    }
    let text = std::str::from_utf8(body).context("gateway response is not UTF-8")?;
    hex_to_bytes(text.trim())
}

fn gateway_transport_classification(error: &anyhow::Error) -> Option<bool> {
    error.chain().find_map(|cause| {
        let error = cause.downcast_ref::<reqwest::Error>()?;
        if error.is_timeout() {
            return Some(!error.is_connect());
        }
        (error.is_connect()
            || error.is_body()
            || (error.is_request()
                && !error.is_builder()
                && !error.is_redirect()
                && !error.is_status()
                && !error.is_decode()))
        .then_some(false)
    })
}

fn retain_transport_error(
    selected: &mut Option<(bool, anyhow::Error)>,
    error: anyhow::Error,
) -> std::result::Result<(), anyhow::Error> {
    let Some(configured_timeout) = gateway_transport_classification(&error) else {
        return Err(error);
    };
    let replace = match selected.as_ref() {
        None => true,
        Some((current_timeout, _)) => *current_timeout && !configured_timeout,
    };
    if replace {
        *selected = Some((configured_timeout, error));
    }
    Ok(())
}

#[derive(Debug)]
struct GatewayStatusError {
    status: StatusCode,
}

impl GatewayStatusError {
    fn is_client_error(&self) -> bool {
        self.status.is_client_error()
    }
}

impl std::fmt::Display for GatewayStatusError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "CCIP gateway returned HTTP {}", self.status)
    }
}

impl std::error::Error for GatewayStatusError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn portable_gateway_response_shapes_match_legacy_execution() -> Result<()> {
        for body in [
            br#"{"data":"0xabcd"}"#.as_slice(),
            br#""0xabcd""#.as_slice(),
            b"0xabcd\n".as_slice(),
        ] {
            assert_eq!(decode_body(body)?, vec![0xab, 0xcd]);
        }
        Ok(())
    }

    #[test]
    fn batch_gateway_query_round_trips() -> Result<()> {
        let requests = vec![contracts::Request {
            sender: "0x1111111111111111111111111111111111111111".parse()?,
            urls: vec!["https://gateway.example/{data}".to_owned()],
            data: Bytes::from(vec![0xab, 0xcd]),
        }];
        let calldata = contracts::queryCall { requests }.abi_encode();
        let decoded = decode_batch_query(&calldata)?;
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].data, vec![0xab, 0xcd]);
        Ok(())
    }
}
