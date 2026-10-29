use std::{sync::LazyLock, time::Duration};

use alloy_primitives::Bytes;
use alloy_sol_types::{SolCall, SolError, SolValue, sol};
use anyhow::{Context, Result, bail};
use futures_util::{StreamExt, stream};
use reqwest::StatusCode;
use serde_json::{Value, json};

use super::OffchainLookup;
use crate::abi::{hex_string, hex_to_bytes};

const LOCAL_BATCH_GATEWAY_URL: &str = "x-batch-gateway:true";
const MAX_GATEWAY_URLS: usize = 4;
/// A batch the Universal Resolver builds carries one lookup per call it was
/// asked to make: one for a plain resolver call, one per entry of a
/// `multicall()` (upstream: .refs/ens_v1/contracts/universalResolver/ResolverCaller.sol:L98-L122 @ ens_v1@91c966f;
/// upstream: .refs/ens_v1/contracts/ccipRead/CCIPBatcher.sol:L42-L53 @ ens_v1@91c966f),
/// and bigname asks for one record per call. The batch calldata is still
/// contract-chosen revert data, so its length is capped before any request is
/// launched, the fan-out runs a few requests at a time, and the decoded inner
/// responses share the single-answer byte cap below.
const MAX_BATCH_GATEWAY_REQUESTS: usize = 8;
const MAX_BATCH_GATEWAY_CONCURRENCY: usize = 4;
#[cfg(not(test))]
const GATEWAY_CONNECT_TIMEOUT: Duration = Duration::from_millis(1000);
#[cfg(test)]
const GATEWAY_CONNECT_TIMEOUT: Duration = Duration::from_millis(100);
const GATEWAY_TIMEOUT: Duration = Duration::from_millis(1500);
/// A CCIP-Read answer is one ABI-encoded resolver result. Cap the read so a
/// gateway URL decoded from revert data cannot stream unbounded bytes into the
/// serving path within the request timeout.
const MAX_GATEWAY_RESPONSE_BYTES: usize = 1 << 20;

static HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .connect_timeout(GATEWAY_CONNECT_TIMEOUT)
        .timeout(GATEWAY_TIMEOUT)
        // A gateway answers in one hop. Following redirects would let a URL that
        // passed any origin check bounce the request to an unrelated host, so a
        // 3xx is surfaced as an ordinary unsuccessful gateway status instead.
        .redirect(reqwest::redirect::Policy::none())
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
        return fetch_batch(decode_batch_query(&lookup.call_data)?).await;
    }
    fetch_standard(&lookup.sender, &lookup.urls, &lookup.call_data).await
}

async fn fetch_batch(requests: Vec<BatchRequest>) -> Result<Vec<u8>> {
    if requests.len() > MAX_BATCH_GATEWAY_REQUESTS {
        bail!(
            "CCIP batch gateway query carries {} requests; at most {MAX_BATCH_GATEWAY_REQUESTS} are followed",
            requests.len()
        );
    }
    let mut results = stream::iter(requests)
        .map(|request| async move {
            fetch_standard(&request.sender, &request.urls, &request.data).await
        })
        .buffered(MAX_BATCH_GATEWAY_CONCURRENCY);
    let mut failures = Vec::new();
    let mut responses = Vec::new();
    let mut transport_error = None;
    let mut total_bytes = 0_usize;
    while let Some(result) = results.next().await {
        let response = match result {
            Ok(response) => {
                failures.push(false);
                response
            }
            Err(error) => match retain_transport_error(&mut transport_error, error) {
                Ok(()) => continue,
                Err(error) => {
                    failures.push(true);
                    alloy_sol_types::Revert::from(format!("CCIP gateway request failed: {error}"))
                        .abi_encode()
                }
            },
        };
        total_bytes += response.len();
        if total_bytes > MAX_GATEWAY_RESPONSE_BYTES {
            bail!(
                "CCIP batch gateway responses exceeded {MAX_GATEWAY_RESPONSE_BYTES} bytes in total"
            );
        }
        responses.push(response);
    }
    if let Some((_, error)) = transport_error {
        return Err(error);
    }
    let responses = responses
        .iter()
        .map(|response| Bytes::copy_from_slice(response))
        .collect::<Vec<_>>();
    Ok((failures, responses).abi_encode_params())
}

#[cfg(test)]
pub(crate) fn encode_batch_query_for_test(
    requests: Vec<(alloy_primitives::Address, Vec<String>, Vec<u8>)>,
) -> Vec<u8> {
    let requests = requests
        .into_iter()
        .map(|(sender, urls, data)| contracts::Request {
            sender,
            urls,
            data: Bytes::from(data),
        })
        .collect();
    contracts::queryCall { requests }.abi_encode()
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
    let use_get = url.contains("{data}");
    let url = if use_get {
        url.replace("{data}", data)
    } else {
        url
    };
    ensure_fetchable_scheme(&url)?;
    let response = if use_get {
        HTTP_CLIENT.get(&url).send().await
    } else {
        HTTP_CLIENT
            .post(&url)
            .json(&json!({ "sender": sender, "data": data }))
            .send()
            .await
    }
    .with_context(|| format!("failed to send CCIP gateway request to {url}"))?;
    let status = response.status();
    let body = read_capped_body(response)
        .await
        .with_context(|| format!("failed to read CCIP gateway response from {url}"))?;
    if !status.is_success() {
        return Err(GatewayStatusError { status }.into());
    }
    decode_body(&body).with_context(|| format!("failed to decode CCIP gateway response from {url}"))
}

/// The URL list is a field of the `OffchainLookup` error the reverting contract
/// raises (upstream: .refs/ens_v1/contracts/ccipRead/EIP3668.sol:L6-L12 @ ens_v1@91c966f),
/// filled from that contract's own state — the Basenames L1 resolver copies its
/// `url` storage into it (upstream: .refs/basenames/src/L1/L1Resolver.sol:L171-L173 @ basenames@1809bbc)
/// — so `lookup.urls` is contract-chosen input decoded from revert data.
/// Restrict it to the two schemes a gateway is defined over before the client
/// is handed the string.
fn ensure_fetchable_scheme(url: &str) -> Result<()> {
    let parsed = reqwest::Url::parse(url)
        .with_context(|| format!("CCIP gateway URL is not a valid absolute URL: {url}"))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        bail!(
            "CCIP gateway URL scheme `{}` is not supported: {url}",
            parsed.scheme()
        );
    }
    Ok(())
}

async fn read_capped_body(mut response: reqwest::Response) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if body.len() + chunk.len() > MAX_GATEWAY_RESPONSE_BYTES {
            bail!("CCIP gateway response exceeded {MAX_GATEWAY_RESPONSE_BYTES} bytes");
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
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
    fn only_http_schemes_reach_the_gateway_client() {
        for url in ["http://gateway.invalid/q", "https://gateway.invalid/q"] {
            assert!(ensure_fetchable_scheme(url).is_ok(), "rejected {url}");
        }
        // `lookup.urls` is decoded from the reverting contract's `OffchainLookup`
        // data (upstream: .refs/ens_v1/contracts/ccipRead/EIP3668.sol:L6-L12 @ ens_v1@91c966f),
        // so a non-HTTP scheme and a relative reference must both fail before
        // the client sees them.
        for url in [
            "file:///etc/passwd",
            "ftp://gateway.invalid/q",
            "gopher://gateway.invalid/",
            "data:text/plain,payload",
            "/etc/passwd",
            "",
        ] {
            assert!(ensure_fetchable_scheme(url).is_err(), "accepted {url}");
        }
    }

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
