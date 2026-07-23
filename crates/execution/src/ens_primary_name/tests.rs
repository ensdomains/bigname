use super::*;
use alloy_primitives::{Bytes, FixedBytes, hex};
use alloy_sol_types::{SolError, SolValue, sol};
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};

const TEST_BLOCK_HASH: &str = "0x1234000000000000000000000000000000000000000000000000000000000000";

struct PrimaryNameTimeoutDnsResolver {
    recovery_address: SocketAddr,
    attempts: Arc<AtomicU64>,
}

impl reqwest::dns::Resolve for PrimaryNameTimeoutDnsResolver {
    fn resolve(&self, _name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            return Box::pin(std::future::pending());
        }
        let address = self.recovery_address;
        Box::pin(async move { Ok(Box::new(std::iter::once(address)) as reqwest::dns::Addrs) })
    }
}

struct PrimaryNameCallbackTimeoutDnsResolver {
    rpc_address: SocketAddr,
    attempts: Arc<AtomicU64>,
}

impl reqwest::dns::Resolve for PrimaryNameCallbackTimeoutDnsResolver {
    fn resolve(&self, _name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        if self.attempts.fetch_add(1, Ordering::SeqCst) > 0 {
            return Box::pin(std::future::pending());
        }
        let address = self.rpc_address;
        Box::pin(async move { Ok(Box::new(std::iter::once(address)) as reqwest::dns::Addrs) })
    }
}

fn test_block_selector() -> Value {
    json!({
        "blockHash": TEST_BLOCK_HASH,
        "requireCanonical": true,
    })
}

enum MockRpcResponse {
    Result(Value),
    Error {
        code: i64,
        message: String,
        data: Value,
    },
}

async fn spawn_mock_rpc(responses: Vec<Value>) -> Result<(String, JoinHandle<Result<Vec<Value>>>)> {
    spawn_mock_rpc_responses(
        responses
            .into_iter()
            .map(MockRpcResponse::Result)
            .collect::<Vec<_>>(),
    )
    .await
}

async fn spawn_mock_rpc_responses(
    responses: Vec<MockRpcResponse>,
) -> Result<(String, JoinHandle<Result<Vec<Value>>>)> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind mock RPC listener")?;
    let url = format!("http://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let mut requests = Vec::new();
        for response_result in responses {
            let (mut socket, _) = listener
                .accept()
                .await
                .context("failed to accept mock RPC request")?;
            let request_payload = read_http_json_body(&mut socket).await?;
            requests.push(request_payload);
            write_json_rpc_response(&mut socket, response_result).await?;
        }
        Ok(requests)
    });

    Ok((url, handle))
}

mod ccip_test_abi {
    use super::*;

    sol! {
        error OffchainLookup(
            address sender,
            string[] urls,
            bytes callData,
            bytes4 callbackFunction,
            bytes extraData
        );

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

async fn read_http_json_body(socket: &mut tokio::net::TcpStream) -> Result<Value> {
    let mut buffer = Vec::new();
    let mut scratch = [0_u8; 1024];
    let (body_start, content_length) = loop {
        let bytes_read = socket
            .read(&mut scratch)
            .await
            .context("failed to read mock RPC request")?;
        if bytes_read == 0 {
            bail!("mock RPC request closed before headers finished");
        }
        buffer.extend_from_slice(&scratch[..bytes_read]);
        if let Some(body_start) = find_header_end(&buffer) {
            let headers = std::str::from_utf8(&buffer[..body_start])
                .context("mock RPC request headers were not utf8")?;
            let content_length = parse_content_length(headers)?;
            break (body_start, content_length);
        }
    };

    while buffer.len() < body_start + content_length {
        let bytes_read = socket
            .read(&mut scratch)
            .await
            .context("failed to read mock RPC request body")?;
        if bytes_read == 0 {
            bail!("mock RPC request closed before body finished");
        }
        buffer.extend_from_slice(&scratch[..bytes_read]);
    }

    serde_json::from_slice(&buffer[body_start..body_start + content_length])
        .context("failed to parse mock RPC request body")
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| position + 4)
}

fn parse_content_length(headers: &str) -> Result<usize> {
    headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>())
        })
        .transpose()
        .context("mock RPC request content-length was invalid")?
        .with_context(|| "mock RPC request did not include content-length")
}

async fn write_json_rpc_response(
    socket: &mut tokio::net::TcpStream,
    response: MockRpcResponse,
) -> Result<()> {
    let body = match response {
        MockRpcResponse::Result(result) => json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": result,
        }),
        MockRpcResponse::Error {
            code,
            message,
            data,
        } => json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": {
                "code": code,
                "message": message,
                "data": data,
            },
        }),
    }
    .to_string();
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    socket
        .write_all(response.as_bytes())
        .await
        .context("failed to write mock RPC response")
}

async fn join_requests(handle: JoinHandle<Result<Vec<Value>>>) -> Result<Vec<Value>> {
    handle
        .await
        .context("mock RPC task panicked or was cancelled")?
}

fn encode_universal_resolver_return(result: Vec<u8>, resolver_address: Address) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&abi_word_usize(64));
    let mut resolver_word = [0_u8; 32];
    resolver_word[12..].copy_from_slice(resolver_address.as_slice());
    encoded.extend_from_slice(&resolver_word);
    encoded.extend_from_slice(&abi_word_usize(result.len()));
    encoded.extend_from_slice(&result);
    let padding = (32 - (result.len() % 32)) % 32;
    encoded.extend(std::iter::repeat_n(0_u8, padding));
    encoded
}

fn abi_word_usize(value: usize) -> [u8; 32] {
    let mut word = [0_u8; 32];
    word[24..].copy_from_slice(&(value as u64).to_be_bytes());
    word
}

fn encoded_local_batch_offchain_lookup_error() -> String {
    let sender = ENS_UNIVERSAL_RESOLVER_ADDRESS
        .parse::<Address>()
        .expect("Universal Resolver address must parse");
    hex_string(
        &ccip_test_abi::OffchainLookup {
            sender,
            urls: vec!["x-batch-gateway:true".to_owned()],
            callData: Bytes::from(ccip_test_abi::queryCall { requests: vec![] }.abi_encode()),
            callbackFunction: FixedBytes::from(&[0x12, 0x34, 0x56, 0x78]),
            extraData: Bytes::from(vec![0xef]),
        }
        .abi_encode(),
    )
}

fn encoded_standard_offchain_lookup_error(url: String) -> String {
    let sender = ENS_UNIVERSAL_RESOLVER_ADDRESS
        .parse::<Address>()
        .expect("Universal Resolver address must parse");
    hex_string(
        &ccip_test_abi::OffchainLookup {
            sender,
            urls: vec![url],
            callData: Bytes::copy_from_slice(&[0xab, 0xcd]),
            callbackFunction: FixedBytes::from([0x12, 0x34, 0x56, 0x78]),
            extraData: Bytes::copy_from_slice(&[0xef]),
        }
        .abi_encode(),
    )
}

async fn spawn_hanging_ccip_gateway() -> Result<(String, JoinHandle<Result<()>>)> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind hanging CCIP gateway")?;
    let url = format!("http://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let (_socket, _) = listener
            .accept()
            .await
            .context("failed to accept hanging CCIP gateway request")?;
        std::future::pending::<()>().await;
        Ok(())
    });
    Ok((url, handle))
}

async fn spawn_callback_hanging_rpc(
    offchain_lookup: String,
) -> Result<(String, JoinHandle<Result<()>>)> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind callback-timeout RPC listener")?;
    let url = format!("http://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let (mut initial_socket, _) = listener
            .accept()
            .await
            .context("failed to accept initial callback-timeout RPC request")?;
        read_http_json_body(&mut initial_socket).await?;
        write_json_rpc_response(
            &mut initial_socket,
            MockRpcResponse::Error {
                code: 3,
                message: "execution reverted".to_owned(),
                data: json!(offchain_lookup),
            },
        )
        .await?;

        let (mut callback_socket, _) = listener
            .accept()
            .await
            .context("failed to accept hanging callback RPC request")?;
        read_http_json_body(&mut callback_socket).await?;
        std::future::pending::<()>().await;
        Ok(())
    });
    Ok((url, handle))
}

#[test]
fn builds_expected_reverse_node() {
    let node = reverse_node("0x8e8db5ccef88cca9d624701db544989c996e3216")
        .expect("reverse node must build");

    assert_eq!(
        hex::encode(node),
        "658ecd2fe8aadf31c3ee6126e11967ff852cfd7592ef26c28e0b65c30e4e8628"
    );
}

#[test]
fn rejects_non_normalized_reverse_node_address() {
    assert!(reverse_node("8e8db5ccef88cca9d624701db544989c996e3216").is_err());
    assert!(reverse_node("0xabc").is_err());
}

#[tokio::test]
async fn lookup_ens_reverse_primary_name_executes_configured_rpc_calls() -> Result<()> {
    let resolver_address = "0xa2c122be93b0074270ebee7f6b7292c7deb45047"
        .parse::<Address>()
        .context("resolver address must parse")?;
    let (rpc_url, handle) = spawn_mock_rpc(vec![
        Value::String(hex_string(&resolver_address.abi_encode())),
        Value::String(hex_string(&"taytems.eth".to_owned().abi_encode())),
    ])
    .await?;
    let chain_rpc_urls =
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM_MAINNET_CHAIN_ID}={rpc_url}")])?;

    let result = lookup_ens_reverse_primary_name(OnDemandEnsPrimaryNameRequest {
        normalized_address: "0x8e8db5ccef88cca9d624701db544989c996e3216",
        chain_rpc_urls: &chain_rpc_urls,
        block_hash: TEST_BLOCK_HASH,
    })
    .await
    .expect("mock RPC lookup must succeed")
    .expect("mock RPC lookup must return a claim");

    assert_eq!(
        result,
        OnDemandEnsPrimaryName {
            name: "taytems.eth".to_owned(),
            resolver_address: "0xa2c122be93b0074270ebee7f6b7292c7deb45047".to_owned(),
        }
    );

    let reverse_node = reverse_node("0x8e8db5ccef88cca9d624701db544989c996e3216")?;
    let expected_resolver_call = hex_string(
        &abi::resolverCall {
            node: B256::from(reverse_node),
        }
        .abi_encode(),
    );
    let expected_name_call = hex_string(
        &abi::nameCall {
            node: B256::from(reverse_node),
        }
        .abi_encode(),
    );

    let requests = join_requests(handle).await?;
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0]["method"], "eth_call");
    assert_eq!(requests[0]["params"][0]["to"], ENS_REGISTRY_ADDRESS);
    assert_eq!(requests[0]["params"][0]["data"], expected_resolver_call);
    assert_eq!(requests[0]["params"][1], test_block_selector());
    assert_eq!(requests[1]["method"], "eth_call");
    assert_eq!(
        requests[1]["params"][0]["to"],
        "0xa2c122be93b0074270ebee7f6b7292c7deb45047"
    );
    assert_eq!(requests[1]["params"][0]["data"], expected_name_call);
    assert_eq!(requests[1]["params"][1], test_block_selector());

    Ok(())
}

#[tokio::test]
async fn lookup_ens_reverse_primary_name_returns_none_for_zero_resolver() -> Result<()> {
    let (rpc_url, handle) =
        spawn_mock_rpc(vec![Value::String(hex_string(&Address::ZERO.abi_encode()))]).await?;
    let chain_rpc_urls =
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM_MAINNET_CHAIN_ID}={rpc_url}")])?;

    let result = lookup_ens_reverse_primary_name(OnDemandEnsPrimaryNameRequest {
        normalized_address: "0x8e8db5ccef88cca9d624701db544989c996e3216",
        chain_rpc_urls: &chain_rpc_urls,
        block_hash: TEST_BLOCK_HASH,
    })
    .await
    .expect("mock RPC lookup must not error");

    assert_eq!(result, None);
    assert_eq!(join_requests(handle).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn lookup_ens_reverse_primary_name_rejects_malformed_rpc_return() -> Result<()> {
    let (rpc_url, handle) = spawn_mock_rpc(vec![Value::String("0x1234".to_owned())]).await?;
    let chain_rpc_urls =
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM_MAINNET_CHAIN_ID}={rpc_url}")])?;

    let error = lookup_ens_reverse_primary_name(OnDemandEnsPrimaryNameRequest {
        normalized_address: "0x8e8db5ccef88cca9d624701db544989c996e3216",
        chain_rpc_urls: &chain_rpc_urls,
        block_hash: TEST_BLOCK_HASH,
    })
    .await
    .expect_err("malformed RPC return must fail");

    assert_eq!(error.kind(), OnDemandEnsPrimaryNameErrorKind::Execution);
    assert_eq!(join_requests(handle).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn verify_ens_primary_name_forward_address_executes_universal_resolver_call() -> Result<()> {
    let resolver_address = "0xa2c122be93b0074270ebee7f6b7292c7deb45047"
        .parse::<Address>()
        .context("resolver address must parse")?;
    let requested_address = "0x8e8db5ccef88cca9d624701db544989c996e3216"
        .parse::<Address>()
        .context("requested address must parse")?;
    let universal_return =
        encode_universal_resolver_return(requested_address.abi_encode(), resolver_address);
    let (rpc_url, handle) =
        spawn_mock_rpc(vec![Value::String(hex_string(&universal_return))]).await?;
    let chain_rpc_urls =
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM_MAINNET_CHAIN_ID}={rpc_url}")])?;

    let result =
        verify_ens_primary_name_forward_address(OnDemandEnsPrimaryNameVerificationRequest {
            normalized_address: "0x8e8db5ccef88cca9d624701db544989c996e3216",
            normalized_name: "taytems.eth",
            chain_rpc_urls: &chain_rpc_urls,
            block_hash: TEST_BLOCK_HASH,
        })
        .await
        .expect("mock RPC verification must succeed");

    assert_eq!(
        result.resolved_address,
        Some("0x8e8db5ccef88cca9d624701db544989c996e3216".to_owned())
    );
    assert_eq!(result.evidence.contracts_called.len(), 1);

    let node = namehash("taytems.eth")?;
    let selector = SupportedVerifiedResolutionRecordKey::Addr {
        coin_type: "60".to_owned(),
    };
    let resolver_call = resolver_record_call(&selector, "addr:60", node)?;
    let universal_call =
        universal_resolver_call(&dns_encode_name("taytems.eth")?, resolver_call.calldata());
    let requests = join_requests(handle).await?;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0]["method"], "eth_call");
    assert_eq!(
        requests[0]["params"][0]["to"],
        ENS_UNIVERSAL_RESOLVER_ADDRESS
    );
    assert_eq!(
        requests[0]["params"][0]["data"],
        universal_call.calldata_hex()
    );
    assert_eq!(requests[0]["params"][1], test_block_selector());
    Ok(())
}

#[tokio::test]
async fn lookup_ens_forward_address_at_block_uses_hash_pinned_eth_call() -> Result<()> {
    let resolver_address = "0xa2c122be93b0074270ebee7f6b7292c7deb45047"
        .parse::<Address>()
        .context("resolver address must parse")?;
    let requested_address = "0x8e8db5ccef88cca9d624701db544989c996e3216"
        .parse::<Address>()
        .context("requested address must parse")?;
    let universal_return =
        encode_universal_resolver_return(requested_address.abi_encode(), resolver_address);
    let (rpc_url, handle) =
        spawn_mock_rpc(vec![Value::String(hex_string(&universal_return))]).await?;
    let chain_rpc_urls =
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM_MAINNET_CHAIN_ID}={rpc_url}")])?;

    let result = lookup_ens_forward_address_at_block(EnsForwardAddressLookupRequest {
        normalized_name: "taytems.eth",
        chain_rpc_urls: &chain_rpc_urls,
        block_number: 123,
        block_hash: TEST_BLOCK_HASH,
        follow_ccip_read: false,
    })
    .await
    .expect("mock RPC lookup must succeed");

    assert_eq!(
        result,
        Some("0x8e8db5ccef88cca9d624701db544989c996e3216".to_owned())
    );

    let requests = join_requests(handle).await?;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0]["method"], "eth_call");
    assert_eq!(
        requests[0]["params"][1],
        json!({
            "blockHash": TEST_BLOCK_HASH,
            "requireCanonical": true,
        })
    );
    Ok(())
}

#[tokio::test]
async fn lookup_ens_forward_address_at_block_can_decline_ccip_read() -> Result<()> {
    let (rpc_url, handle) = spawn_mock_rpc_responses(vec![MockRpcResponse::Error {
        code: 3,
        message: "execution reverted".to_owned(),
        data: json!(encoded_local_batch_offchain_lookup_error()),
    }])
    .await?;
    let chain_rpc_urls =
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM_MAINNET_CHAIN_ID}={rpc_url}")])?;

    let error = lookup_ens_forward_address_at_block(EnsForwardAddressLookupRequest {
        normalized_name: "taytems.eth",
        chain_rpc_urls: &chain_rpc_urls,
        block_number: 123,
        block_hash: TEST_BLOCK_HASH,
        follow_ccip_read: false,
    })
    .await
    .expect_err("projection lookup must fail closed on OffchainLookup");

    assert_eq!(error.kind(), OnDemandEnsPrimaryNameErrorKind::Execution);
    assert!(error.is_offchain_lookup_required());
    assert!(!error.is_plain_execution_revert());
    assert!(error.message().contains("OffchainLookup required"));
    let requests = join_requests(handle).await?;
    assert_eq!(requests.len(), 1);

    let resolver_address = "0xa2c122be93b0074270ebee7f6b7292c7deb45047"
        .parse::<Address>()
        .context("resolver address must parse")?;
    let requested_address = "0x8e8db5ccef88cca9d624701db544989c996e3216"
        .parse::<Address>()
        .context("requested address must parse")?;
    let universal_return =
        encode_universal_resolver_return(requested_address.abi_encode(), resolver_address);
    let (rpc_url, handle) = spawn_mock_rpc_responses(vec![
        MockRpcResponse::Error {
            code: 3,
            message: "execution reverted".to_owned(),
            data: json!(encoded_local_batch_offchain_lookup_error()),
        },
        MockRpcResponse::Result(Value::String(hex_string(&universal_return))),
    ])
    .await?;
    let chain_rpc_urls =
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM_MAINNET_CHAIN_ID}={rpc_url}")])?;

    let result = lookup_ens_forward_address_at_block(EnsForwardAddressLookupRequest {
        normalized_name: "taytems.eth",
        chain_rpc_urls: &chain_rpc_urls,
        block_number: 123,
        block_hash: TEST_BLOCK_HASH,
        follow_ccip_read: true,
    })
    .await
    .expect("explicit CCIP-following lookup must use the callback response");

    assert_eq!(
        result,
        Some("0x8e8db5ccef88cca9d624701db544989c996e3216".to_owned())
    );
    let requests = join_requests(handle).await?;
    assert_eq!(requests.len(), 2);
    Ok(())
}

#[tokio::test]
async fn verified_primary_name_exposes_ccip_trace_evidence() -> Result<()> {
    let resolver_address = "0xa2c122be93b0074270ebee7f6b7292c7deb45047"
        .parse::<Address>()
        .context("resolver address must parse")?;
    let requested_address = "0x8e8db5ccef88cca9d624701db544989c996e3216"
        .parse::<Address>()
        .context("requested address must parse")?;
    let universal_return =
        encode_universal_resolver_return(requested_address.abi_encode(), resolver_address);
    let (rpc_url, handle) = spawn_mock_rpc_responses(vec![
        MockRpcResponse::Error {
            code: 3,
            message: "execution reverted".to_owned(),
            data: json!(encoded_local_batch_offchain_lookup_error()),
        },
        MockRpcResponse::Result(Value::String(hex_string(&universal_return))),
    ])
    .await?;
    let chain_rpc_urls =
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM_MAINNET_CHAIN_ID}={rpc_url}")])?;

    let result =
        verify_ens_primary_name_forward_address(OnDemandEnsPrimaryNameVerificationRequest {
            normalized_address: "0x8e8db5ccef88cca9d624701db544989c996e3216",
            normalized_name: "taytems.eth",
            chain_rpc_urls: &chain_rpc_urls,
            block_hash: TEST_BLOCK_HASH,
        })
        .await
        .expect("CCIP-following primary-name verification must succeed");

    assert_eq!(result.evidence.ccip_step_payloads.len(), 1);
    assert_eq!(result.evidence.contracts_called.len(), 1);
    assert_eq!(join_requests(handle).await?.len(), 2);
    Ok(())
}

#[tokio::test]
async fn primary_name_rpc_connect_timeout_is_transient_transport() -> Result<()> {
    let connect_timeout = Duration::from_millis(25);
    let response_timeout = Duration::from_secs(1);
    let attempts = Arc::new(AtomicU64::new(0));
    let client = reqwest::Client::builder()
        .connect_timeout(connect_timeout)
        .timeout(response_timeout)
        .no_proxy()
        .dns_resolver(PrimaryNameTimeoutDnsResolver {
            recovery_address: "127.0.0.1:9".parse()?,
            attempts: Arc::clone(&attempts),
        })
        .build()
        .context("failed to build primary-name connect-timeout RPC client")?;
    let chain_rpc_urls = ChainRpcUrls::from_entries(&[format!(
        "{ETHEREUM_MAINNET_CHAIN_ID}=http://primary-rpc.connect-timeout.test"
    )])?
    .with_test_http_client(client, connect_timeout, response_timeout)?;

    let error = lookup_ens_reverse_primary_name(OnDemandEnsPrimaryNameRequest {
        normalized_address: "0x8e8db5ccef88cca9d624701db544989c996e3216",
        chain_rpc_urls: &chain_rpc_urls,
        block_hash: TEST_BLOCK_HASH,
    })
    .await
    .expect_err("primary-name provider connect timeout must remain transient");

    assert!(error.is_transport_failure());
    assert!(!error.is_configured_timeout());
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn primary_name_ccip_callback_rpc_connect_timeout_is_transient() -> Result<()> {
    let (rpc_url, rpc) = spawn_mock_rpc_responses(vec![MockRpcResponse::Error {
        code: 3,
        message: "execution reverted".to_owned(),
        data: json!(encoded_local_batch_offchain_lookup_error()),
    }])
    .await?;
    let rpc_address = rpc_url
        .strip_prefix("http://")
        .context("mock RPC URL must be HTTP")?
        .parse::<SocketAddr>()?;
    let attempts = Arc::new(AtomicU64::new(0));
    let connect_timeout = Duration::from_millis(25);
    let response_timeout = Duration::from_secs(1);
    let client = reqwest::Client::builder()
        .connect_timeout(connect_timeout)
        .timeout(response_timeout)
        .no_proxy()
        .dns_resolver(PrimaryNameCallbackTimeoutDnsResolver {
            rpc_address,
            attempts: Arc::clone(&attempts),
        })
        .build()?;
    let endpoint = "http://primary-callback.connect-timeout.test";
    let chain_rpc_urls =
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM_MAINNET_CHAIN_ID}={endpoint}")])?
            .with_test_http_client(client, connect_timeout, response_timeout)?;

    let error = lookup_ens_forward_address_at_block(EnsForwardAddressLookupRequest {
        normalized_name: "taytems.eth",
        chain_rpc_urls: &chain_rpc_urls,
        block_number: 123,
        block_hash: TEST_BLOCK_HASH,
        follow_ccip_read: true,
    })
    .await
    .expect_err("a CCIP callback provider connect timeout must remain transient");

    assert!(error.is_transport_failure());
    assert!(!error.is_configured_timeout());
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(join_requests(rpc).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn primary_name_ccip_callback_rpc_response_timeout_is_configured_transport() -> Result<()> {
    let (rpc_url, rpc) =
        spawn_callback_hanging_rpc(encoded_local_batch_offchain_lookup_error()).await?;
    let chain_rpc_urls =
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM_MAINNET_CHAIN_ID}={rpc_url}")])?
            .with_http_timeouts(Duration::from_millis(10), Duration::from_millis(25))?;

    let error = lookup_ens_forward_address_at_block(EnsForwardAddressLookupRequest {
        normalized_name: "taytems.eth",
        chain_rpc_urls: &chain_rpc_urls,
        block_number: 123,
        block_hash: TEST_BLOCK_HASH,
        follow_ccip_read: true,
    })
    .await
    .expect_err("a CCIP callback provider response timeout must remain durable");
    rpc.abort();

    assert!(error.is_transport_failure());
    assert!(error.is_configured_timeout());
    assert_eq!(error.evidence().ccip_step_payloads.len(), 2);
    assert_eq!(
        error.evidence().ccip_step_payloads[1]["configured_timeout"],
        json!(true)
    );
    Ok(())
}

#[tokio::test]
async fn primary_name_gateway_connect_timeout_is_transient_transport_with_evidence() -> Result<()> {
    let (rpc_url, rpc) = spawn_mock_rpc_responses(vec![MockRpcResponse::Error {
        code: 3,
        message: "execution reverted".to_owned(),
        data: json!(encoded_standard_offchain_lookup_error(
            "http://primary-gateway.connect-timeout.test:9/{data}".to_owned()
        )),
    }])
    .await?;
    let chain_rpc_urls =
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM_MAINNET_CHAIN_ID}={rpc_url}")])?;

    let error = lookup_ens_forward_address_at_block(EnsForwardAddressLookupRequest {
        normalized_name: "taytems.eth",
        chain_rpc_urls: &chain_rpc_urls,
        block_number: 123,
        block_hash: TEST_BLOCK_HASH,
        follow_ccip_read: true,
    })
    .await
    .expect_err("primary-name gateway connect timeout must remain transient");

    assert!(error.is_transport_failure());
    assert!(!error.is_configured_timeout());
    assert_eq!(error.evidence().contracts_called.len(), 1);
    assert_eq!(error.evidence().ccip_step_payloads.len(), 1);
    assert_eq!(
        error.evidence().ccip_step_payloads[0]["configured_timeout"],
        json!(false)
    );
    assert_eq!(join_requests(rpc).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn primary_name_gateway_response_timeout_is_configured_transport_with_evidence() -> Result<()>
{
    let (gateway_url, gateway) = spawn_hanging_ccip_gateway().await?;
    let (rpc_url, rpc) = spawn_mock_rpc_responses(vec![MockRpcResponse::Error {
        code: 3,
        message: "execution reverted".to_owned(),
        data: json!(encoded_standard_offchain_lookup_error(format!(
            "{gateway_url}/{{data}}"
        ))),
    }])
    .await?;
    let chain_rpc_urls =
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM_MAINNET_CHAIN_ID}={rpc_url}")])?;

    let error = lookup_ens_forward_address_at_block(EnsForwardAddressLookupRequest {
        normalized_name: "taytems.eth",
        chain_rpc_urls: &chain_rpc_urls,
        block_number: 123,
        block_hash: TEST_BLOCK_HASH,
        follow_ccip_read: true,
    })
    .await
    .expect_err("gateway response timeout must remain an in-band transport failure");
    gateway.abort();

    assert!(error.is_transport_failure());
    assert!(error.is_configured_timeout());
    assert_eq!(error.evidence().contracts_called.len(), 1);
    assert_eq!(error.evidence().ccip_step_payloads.len(), 1);
    assert_eq!(
        error.evidence().ccip_step_payloads[0]["configured_timeout"],
        json!(true)
    );
    assert_eq!(join_requests(rpc).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn primary_name_configuration_failure_records_no_contract_call() {
    let error = lookup_ens_reverse_primary_name(OnDemandEnsPrimaryNameRequest {
        normalized_address: "0x8e8db5ccef88cca9d624701db544989c996e3216",
        chain_rpc_urls: &ChainRpcUrls::default(),
        block_hash: TEST_BLOCK_HASH,
    })
    .await
    .expect_err("missing provider configuration must fail");

    assert!(error.evidence().contracts_called.is_empty());
}

#[tokio::test]
async fn ccip_following_lookup_keeps_ok_none_plain_revert_behavior() -> Result<()> {
    let (rpc_url, handle) = spawn_mock_rpc_responses(vec![MockRpcResponse::Error {
        code: 3,
        message: "execution reverted".to_owned(),
        data: Value::Null,
    }])
    .await?;
    let chain_rpc_urls =
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM_MAINNET_CHAIN_ID}={rpc_url}")])?;

    let error = lookup_ens_forward_address_at_block(EnsForwardAddressLookupRequest {
        normalized_name: "missing-forward.eth",
        chain_rpc_urls: &chain_rpc_urls,
        block_number: 123,
        block_hash: TEST_BLOCK_HASH,
        follow_ccip_read: true,
    })
    .await
    .expect_err("plain Universal Resolver revert must fail closed");

    assert_eq!(error.kind(), OnDemandEnsPrimaryNameErrorKind::Execution);
    assert!(error.is_plain_execution_revert());
    assert!(!error.is_offchain_lookup_required());
    assert_eq!(
        error.message(),
        "ENS primary-name RPC call failed: execution reverted"
    );
    assert_eq!(join_requests(handle).await?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn verify_ens_primary_name_forward_address_returns_none_for_zero_addr() -> Result<()> {
    let resolver_address = "0xa2c122be93b0074270ebee7f6b7292c7deb45047"
        .parse::<Address>()
        .context("resolver address must parse")?;
    let universal_return =
        encode_universal_resolver_return(Address::ZERO.abi_encode(), resolver_address);
    let (rpc_url, handle) =
        spawn_mock_rpc(vec![Value::String(hex_string(&universal_return))]).await?;
    let chain_rpc_urls =
        ChainRpcUrls::from_entries(&[format!("{ETHEREUM_MAINNET_CHAIN_ID}={rpc_url}")])?;

    let result =
        verify_ens_primary_name_forward_address(OnDemandEnsPrimaryNameVerificationRequest {
            normalized_address: "0x8e8db5ccef88cca9d624701db544989c996e3216",
            normalized_name: "taytems.eth",
            chain_rpc_urls: &chain_rpc_urls,
            block_hash: TEST_BLOCK_HASH,
        })
        .await
        .expect("mock RPC verification must succeed");

    assert_eq!(result.resolved_address, None);
    assert_eq!(result.evidence.contracts_called.len(), 1);
    assert_eq!(join_requests(handle).await?.len(), 1);
    Ok(())
}
