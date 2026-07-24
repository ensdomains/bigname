use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::Context;
use bigname_storage::ENS_NAMESPACE;
use sqlx::types::time::OffsetDateTime;

use super::*;

struct CallbackTimeoutDnsResolver {
    rpc_address: SocketAddr,
    attempts: Arc<AtomicU64>,
}

impl reqwest::dns::Resolve for CallbackTimeoutDnsResolver {
    fn resolve(&self, _name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        if self.attempts.fetch_add(1, Ordering::SeqCst) > 0 {
            return Box::pin(std::future::pending());
        }
        let address = self.rpc_address;
        Box::pin(async move { Ok(Box::new(std::iter::once(address)) as reqwest::dns::Addrs) })
    }
}

#[tokio::test]
async fn rpc_transport_timeout_is_an_in_band_selector_failure() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let (_socket, _) = listener.accept().await?;
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok::<_, anyhow::Error>(())
    });
    let rpc = JsonRpcHttpClient::new_with_timeouts(
        &endpoint,
        Duration::from_millis(10),
        Duration::from_millis(25),
    )?;
    let row = test_name_current_row();
    let record = EnsResolutionRecord::new("addr:60", "addr", Some("60".to_owned()));
    let block = ExecutionBlock {
        chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
        block_number: 21_000_000,
        block_hash: "0xabc123".to_owned(),
    };

    let selector_call = execute_record_call(
        &row,
        &record,
        b"\x05alice\x03eth\x00",
        [0_u8; 32],
        &block,
        &rpc,
        false,
    )
    .await?;
    server.abort();

    let verified_query = selector_call.verified_query(Uuid::from_u128(1));
    assert_eq!(
        verified_query.get("status").and_then(Value::as_str),
        Some("execution_failed")
    );
    assert_eq!(
        verified_query.get("failure_reason").and_then(Value::as_str),
        Some("resolver_call_failed")
    );
    assert!(selector_call.raw_call_snapshot.is_none());
    assert!(selector_call.request_hash.is_none());
    assert!(selector_call.response_hash.is_none());
    Ok(())
}

#[tokio::test]
async fn followable_offchain_lookup_with_gateway_success_still_returns_selector_value() -> Result<()>
{
    use alloy_primitives::{Address, Bytes};
    use alloy_sol_types::SolValue;

    let (gateway_url, gateway) = spawn_test_gateway().await?;
    let universal_resolver = ENS_UNIVERSAL_RESOLVER_ADDRESS.parse::<Address>()?;
    let selector_return = ("ipfs://avatar".to_owned(),).abi_encode_params();
    let callback_return =
        (Bytes::from(selector_return), Address::repeat_byte(0x22)).abi_encode_params();
    let (rpc_url, rpc) = spawn_ccip_test_rpc(
        encoded_offchain_lookup_error_with(universal_resolver, format!("{gateway_url}/{{data}}")),
        crate::ens_resolution_abi::hex_string(&callback_return),
    )
    .await?;
    let client = JsonRpcHttpClient::new(&rpc_url)?;
    let row = test_name_current_row();
    let record = EnsResolutionRecord::new("avatar", "text", Some("avatar".to_owned()));

    let selector_call = execute_record_call(
        &row,
        &record,
        b"\x05alice\x03eth\x00",
        [0_u8; 32],
        &ExecutionBlock {
            chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
            block_number: 21_000_000,
            block_hash: "0xabc123".to_owned(),
        },
        &client,
        true,
    )
    .await?;

    let verified_query = selector_call.verified_query(Uuid::from_u128(1));
    assert_eq!(
        verified_query["value"]["value"],
        json!("ipfs://avatar"),
        "unexpected verified query: {verified_query}"
    );
    let summary = selector_call
        .ccip_summary
        .context("successful gateway follow must retain CCIP evidence")?;
    assert_eq!(summary.gateway_digests.len(), 1);
    assert_eq!(summary.step_payloads.len(), 1);
    assert!(summary.failure_payload.is_none());
    gateway.await??;
    rpc.await??;
    Ok(())
}

#[tokio::test]
async fn ccip_callback_rpc_connect_timeout_aborts_the_selector_call() -> Result<()> {
    let (gateway_url, gateway) = spawn_test_gateway().await?;
    let universal_resolver = ENS_UNIVERSAL_RESOLVER_ADDRESS.parse()?;
    let (rpc_url, rpc) = spawn_ccip_test_rpc_actions(vec![CcipTestRpcAction::Json(
        ccip_initial_error_response(encoded_offchain_lookup_error_with(
            universal_resolver,
            format!("{gateway_url}/{{data}}"),
        )),
    )])
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
        .dns_resolver(CallbackTimeoutDnsResolver {
            rpc_address,
            attempts: Arc::clone(&attempts),
        })
        .build()?;
    let endpoint = "http://resolution-callback.connect-timeout.test";
    let rpc_urls = crate::rpc::ChainRpcUrls::from_entries(&[format!(
        "{ETHEREUM_MAINNET_CHAIN_ID}={endpoint}"
    )])?
    .with_test_http_client(client, connect_timeout, response_timeout)?;
    let client = JsonRpcHttpClient::new_for_rpc_urls(endpoint, &rpc_urls)?;
    let row = test_name_current_row();
    let record = EnsResolutionRecord::new("avatar", "text", Some("avatar".to_owned()));

    let result = execute_record_call(
        &row,
        &record,
        b"\x05alice\x03eth\x00",
        [0_u8; 32],
        &ExecutionBlock {
            chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
            block_number: 21_000_000,
            block_hash: "0xabc123".to_owned(),
        },
        &client,
        true,
    )
    .await;
    let Err(_error) = result else {
        panic!("a CCIP callback provider connect timeout must abort before persistence");
    };

    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    gateway.await??;
    rpc.await??;
    Ok(())
}

#[tokio::test]
async fn ccip_callback_rpc_response_timeout_is_an_in_band_selector_failure() -> Result<()> {
    let (gateway_url, gateway) = spawn_test_gateway().await?;
    let universal_resolver = ENS_UNIVERSAL_RESOLVER_ADDRESS.parse()?;
    let (rpc_url, rpc) = spawn_ccip_test_rpc_actions(vec![
        CcipTestRpcAction::Json(ccip_initial_error_response(
            encoded_offchain_lookup_error_with(
                universal_resolver,
                format!("{gateway_url}/{{data}}"),
            ),
        )),
        CcipTestRpcAction::Hang,
    ])
    .await?;
    let client = JsonRpcHttpClient::new_with_timeouts(
        &rpc_url,
        Duration::from_millis(10),
        Duration::from_millis(25),
    )?;
    let row = test_name_current_row();
    let record = EnsResolutionRecord::new("avatar", "text", Some("avatar".to_owned()));

    let selector_call = execute_record_call(
        &row,
        &record,
        b"\x05alice\x03eth\x00",
        [0_u8; 32],
        &ExecutionBlock {
            chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
            block_number: 21_000_000,
            block_hash: "0xabc123".to_owned(),
        },
        &client,
        true,
    )
    .await?;
    rpc.abort();
    gateway.await??;

    let verified_query = selector_call.verified_query(Uuid::from_u128(1));
    assert_eq!(verified_query["status"], json!("execution_failed"));
    assert_eq!(
        verified_query["failure_reason"],
        json!("resolver_call_failed")
    );
    let summary = selector_call
        .ccip_summary
        .context("durable callback timeout must retain CCIP evidence")?;
    assert_eq!(summary.step_payloads.len(), 1);
    assert_eq!(
        summary
            .failure_payload
            .as_ref()
            .map(|payload| &payload["configured_timeout"]),
        Some(&json!(true))
    );
    Ok(())
}

#[test]
fn malformed_universal_resolver_success_is_selector_failure_for_latest_calls() -> Result<()> {
    let row = test_name_current_row();
    let record = EnsResolutionRecord::new("avatar", "text", Some("avatar".to_owned()));
    let selector = parse_supported_verified_resolution_record_key(&record.record_key)?;
    let block = ExecutionBlock {
        chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
        block_number: 21_000_000,
        block_hash: "0xabc123".to_owned(),
    };
    let selector_call = selector_call_from_rpc_result(SelectorCallRpcContext {
        row: &row,
        record: &record,
        selector: &selector,
        result: JsonRpcCallResult {
            request_payload: json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_call",
                "params": [
                    {
                        "to": ENS_UNIVERSAL_RESOLVER_ADDRESS,
                        "data": "0x9061b923"
                    },
                    "latest"
                ]
            }),
            response_payload: json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": "0x"
            }),
            result: Ok(json!("0x")),
        },
        block: &block,
        contract_call: json!({
            "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
            "contract_address": ENS_UNIVERSAL_RESOLVER_ADDRESS,
            "selector": "0x9061b923",
            "record_key": "avatar",
            "resolver_selector": "0x59d1d43c"
        }),
        resolver_selector: "0x59d1d43c".to_owned(),
        universal_calldata: "0x9061b923".to_owned(),
        block_selector: json!("latest"),
        persist_raw_call_snapshot: false,
        ccip_summary: None,
        latency_ms: 7,
    })?;

    assert_eq!(selector_call.block_selector, json!("latest"));
    assert!(selector_call.request_hash.is_some());
    assert!(selector_call.response_hash.is_some());
    assert!(selector_call.raw_call_snapshot.is_none());

    let verified_query = selector_call.verified_query(Uuid::from_u128(1));
    assert_eq!(
        verified_query.get("status").and_then(Value::as_str),
        Some("execution_failed")
    );
    assert_eq!(
        verified_query.get("failure_reason").and_then(Value::as_str),
        Some("resolver_return_data_malformed")
    );
    assert_eq!(
        verified_query.get("record_key").and_then(Value::as_str),
        Some("avatar")
    );
    Ok(())
}

#[test]
fn offchain_lookup_revert_is_selector_local_unsupported() -> Result<()> {
    let row = test_name_current_row();
    let record = EnsResolutionRecord::new("avatar", "text", Some("avatar".to_owned()));
    let selector = parse_supported_verified_resolution_record_key(&record.record_key)?;
    let block = ExecutionBlock {
        chain_id: ETHEREUM_MAINNET_CHAIN_ID.to_owned(),
        block_number: 21_000_000,
        block_hash: "0xabc123".to_owned(),
    };
    let selector_call = selector_call_from_rpc_result(SelectorCallRpcContext {
        row: &row,
        record: &record,
        selector: &selector,
        result: JsonRpcCallResult {
            request_payload: json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_call",
                "params": [
                    {
                        "to": ENS_UNIVERSAL_RESOLVER_ADDRESS,
                        "data": "0x9061b923"
                    },
                    {
                        "blockHash": "0xabc123",
                        "requireCanonical": true
                    }
                ]
            }),
            response_payload: json!({
                "jsonrpc": "2.0",
                "id": 1,
                "error": {
                    "code": 3,
                    "message": "execution reverted",
                    "data": encoded_offchain_lookup_error(),
                }
            }),
            result: Err(JsonRpcCallError {
                code: Some(3),
                message: "execution reverted".to_owned(),
                data: Some(json!(encoded_offchain_lookup_error())),
            }),
        },
        block: &block,
        contract_call: json!({
            "chain_id": ETHEREUM_MAINNET_CHAIN_ID,
            "contract_address": ENS_UNIVERSAL_RESOLVER_ADDRESS,
            "selector": "0x9061b923",
            "record_key": "avatar",
            "resolver_selector": "0x59d1d43c"
        }),
        resolver_selector: "0x59d1d43c".to_owned(),
        universal_calldata: "0x9061b923".to_owned(),
        block_selector: json!({
            "blockHash": "0xabc123",
            "requireCanonical": true
        }),
        persist_raw_call_snapshot: true,
        ccip_summary: None,
        latency_ms: 7,
    })?;

    let verified_query = selector_call.verified_query(Uuid::from_u128(1));
    assert_eq!(
        verified_query.get("status").and_then(Value::as_str),
        Some("unsupported")
    );
    assert_eq!(
        verified_query
            .get("unsupported_reason")
            .and_then(Value::as_str),
        Some("offchain_lookup_required")
    );
    assert_eq!(
        verified_query.get("record_key").and_then(Value::as_str),
        Some("avatar")
    );
    Ok(())
}

#[test]
fn selected_block_unavailable_rpc_error_is_not_cacheable_selector_failure() {
    let error = JsonRpcCallError {
        code: Some(-32001),
        message: "header not found".to_owned(),
        data: None,
    };

    let Err(error) = decode_rpc_error(&error) else {
        panic!("selected-block provider unavailability must fail before persistence");
    };
    assert!(
        error
            .to_string()
            .contains("verified resolution RPC provider could not serve selected block"),
        "unexpected error: {error:?}"
    );
}

#[test]
fn plain_execution_revert_uses_typed_failure_reason() -> Result<()> {
    let status = decode_rpc_error(&JsonRpcCallError {
        code: Some(3),
        message: "execution reverted".to_owned(),
        data: None,
    })?;
    let selector_call = SelectorCall {
        status,
        request_hash: None,
        response_hash: None,
        raw_call_snapshot: None,
        contract_call: json!({ "record_key": "avatar" }),
        universal_calldata: "0x9061b923".to_owned(),
        resolver_selector: "0x59d1d43c".to_owned(),
        block_selector: json!("latest"),
        ccip_summary: None,
        latency_ms: 7,
    };
    let verified_query = selector_call.verified_query(Uuid::from_u128(1));

    assert_eq!(
        verified_query.get("failure_reason").and_then(Value::as_str),
        Some("resolver_call_reverted")
    );
    Ok(())
}

fn test_name_current_row() -> NameCurrentRow {
    NameCurrentRow {
        logical_name_id: "ens:alice.eth".to_owned(),
        namespace: ENS_NAMESPACE.to_owned(),
        canonical_display_name: "alice.eth".to_owned(),
        normalized_name: "alice.eth".to_owned(),
        namehash: "namehash:alice.eth".to_owned(),
        surface_binding_id: None,
        resource_id: None,
        token_lineage_id: None,
        binding_kind: None,
        declared_summary: json!({}),
        provenance: json!({}),
        coverage: json!({}),
        chain_positions: json!({}),
        canonicality_summary: json!({}),
        manifest_version: 1,
        last_recomputed_at: OffsetDateTime::from_unix_timestamp(1)
            .expect("test timestamp must be valid"),
    }
}

fn encoded_offchain_lookup_error() -> String {
    encoded_offchain_lookup_error_with(
        alloy_primitives::Address::repeat_byte(0x11),
        "https://gateway.example/{data}".to_owned(),
    )
}

fn encoded_offchain_lookup_error_with(sender: alloy_primitives::Address, url: String) -> String {
    use alloy_primitives::{Bytes, FixedBytes};
    use alloy_sol_types::{SolError, sol};

    sol! {
        #[derive(Debug, PartialEq, Eq)]
        error OffchainLookup(
            address sender,
            string[] urls,
            bytes callData,
            bytes4 callbackFunction,
            bytes extraData
        );
    }

    crate::ens_resolution_abi::hex_string(
        &OffchainLookup {
            sender,
            urls: vec![url],
            callData: Bytes::copy_from_slice(&[0xab, 0xcd]),
            callbackFunction: FixedBytes::from([0x12, 0x34, 0x56, 0x78]),
            extraData: Bytes::copy_from_slice(&[0xef]),
        }
        .abi_encode(),
    )
}

async fn spawn_test_gateway() -> Result<(String, tokio::task::JoinHandle<Result<()>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        read_test_http_request(&mut socket).await?;
        write_test_http_response(&mut socket, br#"{"data":"0xabcd"}"#).await
    });
    Ok((url, handle))
}

async fn spawn_ccip_test_rpc(
    offchain_lookup: String,
    callback_result: String,
) -> Result<(String, tokio::task::JoinHandle<Result<()>>)> {
    spawn_ccip_test_rpc_actions(vec![
        CcipTestRpcAction::Json(ccip_initial_error_response(offchain_lookup)),
        CcipTestRpcAction::Json(json!({ "jsonrpc": "2.0", "id": 1, "result": callback_result })),
    ])
    .await
}

enum CcipTestRpcAction {
    Json(Value),
    Hang,
}

fn ccip_initial_error_response(offchain_lookup: String) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "error": {
            "code": 3,
            "message": "execution reverted",
            "data": offchain_lookup,
        }
    })
}

async fn spawn_ccip_test_rpc_actions(
    actions: Vec<CcipTestRpcAction>,
) -> Result<(String, tokio::task::JoinHandle<Result<()>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        for action in actions {
            let (mut socket, _) = listener.accept().await?;
            read_test_http_request(&mut socket).await?;
            match action {
                CcipTestRpcAction::Json(body) => {
                    write_test_http_response(&mut socket, body.to_string().as_bytes()).await?;
                }
                CcipTestRpcAction::Hang => std::future::pending::<()>().await,
            }
        }
        Ok(())
    });
    Ok((url, handle))
}

async fn read_test_http_request(socket: &mut tokio::net::TcpStream) -> Result<()> {
    use tokio::io::AsyncReadExt;

    let mut buffer = Vec::new();
    let mut scratch = [0_u8; 1024];
    let (body_start, content_length) = loop {
        let bytes_read = socket.read(&mut scratch).await?;
        if bytes_read == 0 {
            bail!("test HTTP request closed before headers finished");
        }
        buffer.extend_from_slice(&scratch[..bytes_read]);
        if let Some(position) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            let body_start = position + 4;
            let headers = std::str::from_utf8(&buffer[..body_start])?;
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>())
                })
                .transpose()?
                .unwrap_or(0);
            break (body_start, content_length);
        }
    };
    while buffer.len() < body_start + content_length {
        let bytes_read = socket.read(&mut scratch).await?;
        if bytes_read == 0 {
            bail!("test HTTP request closed before body finished");
        }
        buffer.extend_from_slice(&scratch[..bytes_read]);
    }
    Ok(())
}

async fn write_test_http_response(socket: &mut tokio::net::TcpStream, body: &[u8]) -> Result<()> {
    use tokio::io::AsyncWriteExt;

    socket
        .write_all(
            format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nconnection: close\r\ncontent-length: {}\r\n\r\n",
                body.len()
            )
            .as_bytes(),
        )
        .await?;
    socket.write_all(body).await?;
    Ok(())
}
