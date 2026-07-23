const BASE_PRIMARY_COIN_TYPE: &str = "2147492101";

fn primary_name_selected_block_selector() -> Value {
    json!({
        "blockHash": "0xbinding",
        "requireCanonical": true,
    })
}

fn primary_name_fallback_chain_positions() -> Value {
    json!({
        "ethereum": {
            "chain_id": "ethereum-mainnet",
            "block_number": 21_000_003,
            "block_hash": "0xbinding",
            "timestamp": "2026-04-17T00:00:03Z",
        }
    })
}

fn primary_name_verified_state_without_provenance(payload: &PrimaryNameResponse) -> Value {
    let mut state = payload
        .verified_state
        .clone()
        .expect("verified primary-name state must be present");
    state["verified_primary_name"]
        .as_object_mut()
        .expect("verified primary-name section must be an object")
        .remove("provenance");
    state
}

fn assert_persisted_primary_name_fallback_metadata(payload: &PrimaryNameResponse) {
    assert_eq!(payload.chain_positions, primary_name_fallback_chain_positions());
    assert_eq!(payload.consistency, "head");
    let provenance = payload
        .provenance
        .as_object()
        .expect("persisted fallback route provenance must be present");
    assert!(
        provenance
            .get("execution_trace_id")
            .and_then(Value::as_str)
            .is_some()
    );
    assert_eq!(
        provenance.get("manifest_versions"),
        Some(&json!([{
            "source_family": "ens_execution",
            "manifest_version": 1,
        }]))
    );
    assert_eq!(
        payload
            .verified_state
            .as_ref()
            .and_then(|state| state.get("verified_primary_name"))
            .and_then(|verified| verified.get("provenance")),
        Some(&payload.provenance)
    );
}

fn primary_name_supported_coverage(namespace: &str) -> Value {
    let source_classes_considered = match namespace {
        "ens" => json!(["ens_v1_reverse_l1", "ens_execution"]),
        "basenames" => json!(["basenames_base_primary", "basenames_execution"]),
        other => panic!("unsupported test namespace {other}"),
    };

    json!({
        "status": "partial",
        "exhaustiveness": "non_enumerable",
        "source_classes_considered": source_classes_considered,
        "enumeration_basis": "primary_name_lookup",
        "unsupported_reason": null,
    })
}

fn primary_name_unsupported_coverage() -> Value {
    json!({
        "status": "unsupported",
        "exhaustiveness": "not_applicable",
        "source_classes_considered": [],
        "enumeration_basis": "primary_name_lookup",
        "unsupported_reason": "primary-name exact-tuple persisted readback is not supported for the requested tuple",
    })
}

fn primary_name_universal_resolver_addr60_response(address: &str) -> Value {
    json!(format!(
        "0x{}{}{}{}",
        primary_name_left_pad_hex("40", 64),
        primary_name_padded_address_hex(
            "0xa2c122be93b0074270ebee7f6b7292c7deb45047"
        ),
        primary_name_left_pad_hex("20", 64),
        primary_name_padded_address_hex(address),
    ))
}

fn primary_name_reverse_name_response(name: &str) -> Value {
    let name_hex = name
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let padded_name_hex_len = name_hex.len().next_multiple_of(64);
    json!(format!(
        "0x{}{}{}",
        primary_name_left_pad_hex("20", 64),
        primary_name_left_pad_hex(&format!("{:x}", name.len()), 64),
        format!("{name_hex:0<padded_name_hex_len$}"),
    ))
}

fn encoded_primary_name_offchain_lookup(url: String) -> Result<String> {
    use alloy_primitives::{Address, Bytes, FixedBytes};
    use alloy_sol_types::{SolError, sol};

    sol! {
        error OffchainLookup(
            address sender,
            string[] urls,
            bytes callData,
            bytes4 callbackFunction,
            bytes extraData
        );
    }

    Ok(format!(
        "0x{}",
        hex::encode(
            OffchainLookup {
                sender: "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe".parse::<Address>()?,
                urls: vec![url],
                callData: Bytes::copy_from_slice(&[0xab, 0xcd]),
                callbackFunction: FixedBytes::from([0x12, 0x34, 0x56, 0x78]),
                extraData: Bytes::copy_from_slice(&[0xef]),
            }
            .abi_encode()
        )
    ))
}

fn primary_name_padded_address_hex(address: &str) -> String {
    let stripped = address
        .strip_prefix("0x")
        .expect("test address must be 0x-prefixed");
    assert_eq!(stripped.len(), 40, "test address must be 20 bytes");
    primary_name_left_pad_hex(stripped, 64)
}

fn primary_name_left_pad_hex(value: &str, width: usize) -> String {
    assert!(value.len() <= width, "test hex value must fit padded width");
    format!("{value:0>width$}")
}

async fn spawn_primary_name_mock_rpc(
    responses: Vec<Value>,
) -> Result<(String, tokio::task::JoinHandle<Result<Vec<Value>>>)> {
    spawn_primary_name_mock_rpc_responses(
        responses
            .into_iter()
            .map(PrimaryNameMockRpcResponse::Result)
            .collect(),
    )
    .await
}

enum PrimaryNameMockRpcResponse {
    Result(Value),
    Error {
        code: i64,
        message: String,
        data: Value,
    },
}

async fn spawn_primary_name_mock_rpc_responses(
    responses: Vec<PrimaryNameMockRpcResponse>,
) -> Result<(String, tokio::task::JoinHandle<Result<Vec<Value>>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind mock primary-name RPC listener")?;
    let url = format!("http://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let mut requests = Vec::new();
        for response in responses {
            let (mut socket, _) = listener
                .accept()
                .await
                .context("failed to accept mock primary-name RPC request")?;
            let request_payload = read_primary_name_mock_rpc_request(&mut socket).await?;
            requests.push(request_payload);
            write_primary_name_mock_rpc_response_kind(&mut socket, response).await?;
        }
        Ok(requests)
    });

    Ok((url, handle))
}

async fn spawn_primary_name_transport_recovery_rpc()
-> Result<(String, tokio::task::JoinHandle<Result<Vec<Value>>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind recovering mock primary-name RPC listener")?;
    let url = format!("http://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let mut requests = Vec::new();

        let (mut dropped_socket, _) = listener
            .accept()
            .await
            .context("failed to accept dropped mock primary-name RPC request")?;
        requests.push(read_primary_name_mock_rpc_request(&mut dropped_socket).await?);
        drop(dropped_socket);

        for response_result in [
            json!("0x000000000000000000000000a2c122be93b0074270ebee7f6b7292c7deb45047"),
            primary_name_reverse_name_response("taytems.eth"),
            primary_name_universal_resolver_addr60_response(
                "0x8e8db5ccef88cca9d624701db544989c996e3216",
            ),
        ] {
            let (mut socket, _) = listener
                .accept()
                .await
                .context("failed to accept recovered mock primary-name RPC request")?;
            requests.push(read_primary_name_mock_rpc_request(&mut socket).await?);
            write_primary_name_mock_rpc_response(&mut socket, response_result).await?;
        }

        Ok(requests)
    });

    Ok((url, handle))
}

async fn spawn_hanging_primary_name_rpc()
-> Result<(String, tokio::task::JoinHandle<Result<()>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind hanging mock primary-name RPC listener")?;
    let url = format!("http://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let (mut socket, _) = listener
            .accept()
            .await
            .context("failed to accept hanging mock primary-name RPC request")?;
        read_primary_name_mock_rpc_request(&mut socket).await?;
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        Ok(())
    });
    Ok((url, handle))
}

async fn spawn_hanging_primary_name_gateway()
-> Result<(String, tokio::task::JoinHandle<Result<()>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind hanging mock primary-name gateway")?;
    let url = format!("http://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let (_socket, _) = listener
            .accept()
            .await
            .context("failed to accept hanging mock primary-name gateway request")?;
        std::future::pending::<()>().await;
        Ok(())
    });
    Ok((url, handle))
}

async fn spawn_successful_primary_name_gateway()
-> Result<(String, tokio::task::JoinHandle<Result<()>>)> {
    use tokio::io::AsyncWriteExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind successful mock primary-name gateway")?;
    let url = format!("http://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let (mut socket, _) = listener
            .accept()
            .await
            .context("failed to accept successful mock primary-name gateway request")?;
        let body = br#"{"data":"0xabcd"}"#;
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
    });
    Ok((url, handle))
}

async fn spawn_primary_name_callback_drop_then_recovery_rpc(
    offchain_lookup: String,
    recovered_address: &str,
) -> Result<(String, tokio::task::JoinHandle<Result<Vec<Value>>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind callback-recovery primary-name RPC listener")?;
    let url = format!("http://{}", listener.local_addr()?);
    let recovered_address = recovered_address.to_owned();
    let handle = tokio::spawn(async move {
        let resolver =
            json!("0x000000000000000000000000a2c122be93b0074270ebee7f6b7292c7deb45047");
        let reverse_name = primary_name_reverse_name_response("taytems.eth");
        let mut requests = Vec::new();
        for response in [
            PrimaryNameMockRpcResponse::Result(resolver.clone()),
            PrimaryNameMockRpcResponse::Result(reverse_name.clone()),
            PrimaryNameMockRpcResponse::Error {
                code: 3,
                message: "execution reverted".to_owned(),
                data: json!(offchain_lookup),
            },
        ] {
            let (mut socket, _) = listener
                .accept()
                .await
                .context("failed to accept pre-callback primary-name RPC request")?;
            requests.push(read_primary_name_mock_rpc_request(&mut socket).await?);
            write_primary_name_mock_rpc_response_kind(&mut socket, response).await?;
        }

        let (mut dropped_callback, _) = listener
            .accept()
            .await
            .context("failed to accept dropped callback RPC request")?;
        requests.push(read_primary_name_mock_rpc_request(&mut dropped_callback).await?);
        drop(dropped_callback);

        for response in [
            resolver,
            reverse_name,
            primary_name_universal_resolver_addr60_response(&recovered_address),
        ] {
            let (mut socket, _) = listener
                .accept()
                .await
                .context("failed to accept recovered primary-name RPC request")?;
            requests.push(read_primary_name_mock_rpc_request(&mut socket).await?);
            write_primary_name_mock_rpc_response(&mut socket, response).await?;
        }
        Ok(requests)
    });
    Ok((url, handle))
}

async fn spawn_hanging_primary_name_callback_rpc(
    offchain_lookup: String,
) -> Result<(String, tokio::task::JoinHandle<Result<()>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind hanging callback primary-name RPC listener")?;
    let url = format!("http://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        for response in [
            PrimaryNameMockRpcResponse::Result(
                json!("0x000000000000000000000000a2c122be93b0074270ebee7f6b7292c7deb45047"),
            ),
            PrimaryNameMockRpcResponse::Result(primary_name_reverse_name_response("taytems.eth")),
            PrimaryNameMockRpcResponse::Error {
                code: 3,
                message: "execution reverted".to_owned(),
                data: json!(offchain_lookup),
            },
        ] {
            let (mut socket, _) = listener
                .accept()
                .await
                .context("failed to accept pre-timeout primary-name RPC request")?;
            read_primary_name_mock_rpc_request(&mut socket).await?;
            write_primary_name_mock_rpc_response_kind(&mut socket, response).await?;
        }

        let (mut callback_socket, _) = listener
            .accept()
            .await
            .context("failed to accept hanging callback RPC request")?;
        read_primary_name_mock_rpc_request(&mut callback_socket).await?;
        std::future::pending::<()>().await;
        Ok(())
    });
    Ok((url, handle))
}

async fn spawn_primary_name_mock_rpc_with_last_response_gate(
    responses: Vec<Value>,
) -> Result<(
    String,
    tokio::sync::oneshot::Receiver<()>,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<Vec<Value>>>,
)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind gated mock primary-name RPC listener")?;
    let url = format!("http://{}", listener.local_addr()?);
    let (last_request_reached_tx, last_request_reached_rx) = tokio::sync::oneshot::channel();
    let (release_last_response_tx, release_last_response_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move {
        let response_count = responses.len();
        let mut requests = Vec::new();
        let mut last_request_reached_tx = Some(last_request_reached_tx);
        let mut release_last_response_rx = Some(release_last_response_rx);
        for (index, response_result) in responses.into_iter().enumerate() {
            let (mut socket, _) = listener
                .accept()
                .await
                .context("failed to accept gated mock primary-name RPC request")?;
            let request_payload = read_primary_name_mock_rpc_request(&mut socket).await?;
            requests.push(request_payload);
            if index + 1 == response_count {
                let Some(reached_tx) = last_request_reached_tx.take() else {
                    anyhow::bail!("gated mock primary-name RPC reached its last request twice");
                };
                if reached_tx.send(()).is_err() {
                    anyhow::bail!("gated mock primary-name RPC last-request receiver dropped");
                }
                release_last_response_rx
                    .take()
                    .context("gated mock primary-name RPC release receiver missing")?
                    .await
                    .context("gated mock primary-name RPC release sender dropped")?;
            }
            write_primary_name_mock_rpc_response(&mut socket, response_result).await?;
        }
        Ok(requests)
    });

    Ok((
        url,
        last_request_reached_rx,
        release_last_response_tx,
        handle,
    ))
}

async fn read_primary_name_mock_rpc_request(socket: &mut tokio::net::TcpStream) -> Result<Value> {
    use tokio::io::AsyncReadExt;

    let mut buffer = Vec::new();
    let mut scratch = [0_u8; 1024];
    let (body_start, content_length) = loop {
        let bytes_read = socket
            .read(&mut scratch)
            .await
            .context("failed to read mock primary-name RPC request")?;
        if bytes_read == 0 {
            anyhow::bail!("mock primary-name RPC request closed before headers finished");
        }
        buffer.extend_from_slice(&scratch[..bytes_read]);
        if let Some(body_start) = primary_name_mock_header_end(&buffer) {
            let headers = std::str::from_utf8(&buffer[..body_start])
                .context("mock primary-name RPC request headers were not utf8")?;
            let content_length = primary_name_mock_content_length(headers)?;
            break (body_start, content_length);
        }
    };

    while buffer.len() < body_start + content_length {
        let bytes_read = socket
            .read(&mut scratch)
            .await
            .context("failed to read mock primary-name RPC request body")?;
        if bytes_read == 0 {
            anyhow::bail!("mock primary-name RPC request closed before body finished");
        }
        buffer.extend_from_slice(&scratch[..bytes_read]);
    }

    serde_json::from_slice(&buffer[body_start..body_start + content_length])
        .context("failed to parse mock primary-name RPC request body")
}

fn primary_name_mock_header_end(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| position + 4)
}

fn primary_name_mock_content_length(headers: &str) -> Result<usize> {
    headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>())
        })
        .transpose()
        .context("mock primary-name RPC request content-length was invalid")?
        .with_context(|| "mock primary-name RPC request did not include content-length")
}

async fn write_primary_name_mock_rpc_response(
    socket: &mut tokio::net::TcpStream,
    result: Value,
) -> Result<()> {
    write_primary_name_mock_rpc_response_kind(
        socket,
        PrimaryNameMockRpcResponse::Result(result),
    )
    .await
}

async fn write_primary_name_mock_rpc_response_kind(
    socket: &mut tokio::net::TcpStream,
    response: PrimaryNameMockRpcResponse,
) -> Result<()> {
    use tokio::io::AsyncWriteExt;

    let body = match response {
        PrimaryNameMockRpcResponse::Result(result) => json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": result,
        }),
        PrimaryNameMockRpcResponse::Error {
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
        .context("failed to write mock primary-name RPC response")
}

async fn join_primary_name_mock_rpc_requests(
    handle: tokio::task::JoinHandle<Result<Vec<Value>>>,
) -> Result<Vec<Value>> {
    handle
        .await
        .context("mock primary-name RPC task panicked or was cancelled")?
}

#[test]
fn primary_name_response_uses_on_demand_claim_and_verification_for_default_tuple_miss() -> Result<()>
{
    let lookup_state = PrimaryNameLookupState {
        tuple_state: PrimaryNameTupleState::TupleMissing,
        normalized_claim_name: None,
        claim_name_is_normalized: false,
        on_demand_claim: OnDemandPrimaryNameClaimState::Found(OnDemandPrimaryNameClaim {
            raw_name: "taytems.eth".to_owned(),
            normalized_name: "taytems.eth".to_owned(),
            resolver_address: "0xa2c122be93b0074270ebee7f6b7292c7deb45047".to_owned(),
        }),
        on_demand_verified: OnDemandPrimaryNameVerificationState::Verified(json!({
            "status": "success",
            "name": {
                "logical_name_id": "ens:taytems.eth",
                "namespace": "ens",
                "normalized_name": "taytems.eth",
                "canonical_display_name": "taytems.eth",
                "namehash": bigname_execution::ens_namehash_hex("taytems.eth")?,
            },
        })),
        persisted_verified: None,
    };

    let payload = build_primary_name_response(
        "0x8e8db5ccef88cca9d624701db544989c996e3216".to_owned(),
        "ens".to_owned(),
        "60".to_owned(),
        ResolutionMode::Both,
        &lookup_state,
        None,
    );

    assert_eq!(
        payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "success",
                "name": "taytems.eth",
                "provenance": {
                    "source_family": "ens_reverse_rpc",
                    "resolver_address": "0xa2c122be93b0074270ebee7f6b7292c7deb45047",
                },
            }
        }))
    );
    assert_eq!(
        primary_name_verified_state_without_provenance(&payload),
        json!({
            "verified_primary_name": {
                "status": "success",
                "name": {
                    "logical_name_id": "ens:taytems.eth",
                    "namespace": "ens",
                    "normalized_name": "taytems.eth",
                    "canonical_display_name": "taytems.eth",
                    "namehash": bigname_execution::ens_namehash_hex("taytems.eth")?,
                },
            }
        })
    );
    assert_eq!(
        payload.coverage,
        json!({
            "status": "partial",
            "exhaustiveness": "non_enumerable",
            "source_classes_considered": ["ens_reverse_rpc", "ens_execution"],
            "enumeration_basis": "primary_name_lookup",
            "unsupported_reason": null,
        })
    );
    Ok(())
}

#[test]
fn primary_name_response_reports_supported_tuple_class_without_persisted_verified_outcome()
-> Result<()> {
    let address = "0x0000000000000000000000000000000000000abc";
    let lookup_state = PrimaryNameLookupState {
        tuple_state: PrimaryNameTupleState::TuplePresent(PrimaryNameCurrentRow {
            address: address.to_owned(),
            namespace: "ens".to_owned(),
            coin_type: "60".to_owned(),
            claim_status: PrimaryNameClaimStatus::Success,
            raw_claim_name: None,
            claim_provenance: json!({
                "source_family": "ens_v1_reverse_l1",
            }),
        }),
        normalized_claim_name: Some("alice.eth".to_owned()),
        claim_name_is_normalized: true,
        on_demand_claim: OnDemandPrimaryNameClaimState::NotAttempted,
        on_demand_verified: OnDemandPrimaryNameVerificationState::NotAttempted,
        persisted_verified: None,
    };

    let payload = build_primary_name_response(
        address.to_owned(),
        "ens".to_owned(),
        "60".to_owned(),
        ResolutionMode::Both,
        &lookup_state,
        None,
    );

    assert_eq!(
        primary_name_verified_state_without_provenance(&payload),
        json!({
            "verified_primary_name": {
                "status": "not_found",
            }
        })
    );
    assert_eq!(payload.coverage, primary_name_supported_coverage("ens"));
    Ok(())
}

#[tokio::test]
async fn get_primary_names_uses_configured_on_demand_rpc_for_default_tuple_miss() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.seed_default_ens_snapshot_selector_position().await?;
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![
        json!("0x000000000000000000000000a2c122be93b0074270ebee7f6b7292c7deb45047"),
        json!(
            "0x0000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000b74617974656d732e657468000000000000000000000000000000000000000000"
        ),
    ])
    .await?;
    let chain_rpc_urls =
        bigname_execution::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;

    let response = app_router(database.app_state_with_chain_rpc_urls(chain_rpc_urls))
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x8e8db5ccef88cca9d624701db544989c996e3216")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("primary-name on-demand tuple miss request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "success",
                "name": "taytems.eth",
                "provenance": {
                    "source_family": "ens_reverse_rpc",
                    "resolver_address": "0xa2c122be93b0074270ebee7f6b7292c7deb45047",
                },
            }
        }))
    );
    assert_eq!(payload.verified_state, None);
    assert_eq!(
        payload.coverage,
        json!({
            "status": "partial",
            "exhaustiveness": "non_enumerable",
            "source_classes_considered": ["ens_reverse_rpc"],
            "enumeration_basis": "primary_name_lookup",
            "unsupported_reason": null,
        })
    );
    assert_eq!(
        payload.provenance,
        json!({ "source_family": "ens_reverse_rpc" })
    );
    assert_eq!(payload.chain_positions, primary_name_fallback_chain_positions());
    assert_eq!(payload.consistency, "head");

    let rpc_requests = join_primary_name_mock_rpc_requests(rpc_handle).await?;
    assert_eq!(rpc_requests.len(), 2);
    assert_eq!(rpc_requests[0]["method"], "eth_call");
    assert_eq!(
        rpc_requests[0]["params"][0]["to"],
        bigname_execution::ENS_REGISTRY_ADDRESS
    );
    assert_eq!(
        rpc_requests[0]["params"][1],
        primary_name_selected_block_selector()
    );
    assert_eq!(rpc_requests[1]["method"], "eth_call");
    assert_eq!(
        rpc_requests[1]["params"][0]["to"],
        "0xa2c122be93b0074270ebee7f6b7292c7deb45047"
    );
    assert_eq!(
        rpc_requests[1]["params"][1],
        primary_name_selected_block_selector()
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_canonical_coin_type_reaches_on_demand_fallback() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.seed_default_ens_snapshot_selector_position().await?;
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![
        json!("0x000000000000000000000000a2c122be93b0074270ebee7f6b7292c7deb45047"),
        primary_name_reverse_name_response("taytems.eth"),
    ])
    .await?;
    let chain_rpc_urls =
        bigname_execution::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;

    let response = app_router(database.app_state_with_chain_rpc_urls(chain_rpc_urls))
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x8e8db5ccef88cca9d624701db544989c996e3216?namespace=ens&coin_type=060")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("canonical coin_type on-demand primary-name request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        payload.data,
        json!({
            "address": "0x8e8db5ccef88cca9d624701db544989c996e3216",
            "namespace": "ens",
            "coin_type": "60",
        })
    );
    assert_eq!(
        payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "success",
                "name": "taytems.eth",
                "provenance": {
                    "source_family": "ens_reverse_rpc",
                    "resolver_address": "0xa2c122be93b0074270ebee7f6b7292c7deb45047",
                },
            }
        }))
    );

    assert_eq!(join_primary_name_mock_rpc_requests(rpc_handle).await?.len(), 2);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_canonicalizes_coin_type_before_lookup_and_response() -> Result<()> {
    let database = TestDatabase::new(false).await?;
    database.create_primary_names_current_table().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    database
        .insert_primary_name_current_claim_row_with_provenance(
            address,
            "ens",
            "60",
            PrimaryNameClaimStatus::Success,
            None,
            json!({
                "source_family": "ens_v1_reverse_l1",
            }),
        )
        .await?;
    database
        .insert_primary_name_current_normalized_claim_name(
            address,
            "ens",
            "60",
            Some("alice.eth"),
            true,
        )
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=ens&coin_type=060&mode=declared"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("canonical coin_type primary-name request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        payload.data,
        json!({
            "address": address,
            "namespace": "ens",
            "coin_type": "60",
        })
    );
    assert_eq!(
        payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "success",
                "name": "alice.eth",
                "provenance": {
                    "source_family": "ens_v1_reverse_l1",
                },
            }
        }))
    );
    assert_eq!(payload.coverage, primary_name_supported_coverage("ens"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_reports_on_demand_unnormalizable_claim_as_invalid_name() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.seed_default_ens_snapshot_selector_position().await?;
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![
        json!("0x000000000000000000000000a2c122be93b0074270ebee7f6b7292c7deb45047"),
        primary_name_reverse_name_response("alice..eth"),
    ])
    .await?;
    let chain_rpc_urls =
        bigname_execution::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;

    let response = app_router(database.app_state_with_chain_rpc_urls(chain_rpc_urls))
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x8e8db5ccef88cca9d624701db544989c996e3216")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("primary-name on-demand invalid-name request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "invalid_name",
                "raw_claim_name": "alice..eth",
                "provenance": {
                    "source_family": "ens_reverse_rpc",
                    "resolver_address": "0xa2c122be93b0074270ebee7f6b7292c7deb45047",
                },
            }
        }))
    );
    assert_eq!(payload.verified_state, None);
    assert_eq!(
        payload.coverage,
        json!({
            "status": "partial",
            "exhaustiveness": "non_enumerable",
            "source_classes_considered": ["ens_reverse_rpc"],
            "enumeration_basis": "primary_name_lookup",
            "unsupported_reason": null,
        })
    );

    let rpc_requests = join_primary_name_mock_rpc_requests(rpc_handle).await?;
    assert_eq!(rpc_requests.len(), 2);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_verifies_default_tuple_miss_with_on_demand_rpc() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_default_ens_primary_name_fallback_context()
        .await?;
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![
        json!("0x000000000000000000000000a2c122be93b0074270ebee7f6b7292c7deb45047"),
        json!(
            "0x0000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000b74617974656d732e657468000000000000000000000000000000000000000000"
        ),
        primary_name_universal_resolver_addr60_response(
            "0x8e8db5ccef88cca9d624701db544989c996e3216",
        ),
    ])
    .await?;
    let chain_rpc_urls =
        bigname_execution::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;

    let response = app_router(database.app_state_with_chain_rpc_urls(chain_rpc_urls))
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x8e8db5ccef88cca9d624701db544989c996e3216?mode=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("primary-name on-demand verified tuple miss request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "success",
                "name": "taytems.eth",
                "provenance": {
                    "source_family": "ens_reverse_rpc",
                    "resolver_address": "0xa2c122be93b0074270ebee7f6b7292c7deb45047",
                },
            }
        }))
    );
    assert_eq!(
        primary_name_verified_state_without_provenance(&payload),
        json!({
            "verified_primary_name": {
                "status": "success",
                "name": {
                    "logical_name_id": "ens:taytems.eth",
                    "namespace": "ens",
                    "normalized_name": "taytems.eth",
                    "canonical_display_name": "taytems.eth",
                    "namehash": bigname_execution::ens_namehash_hex("taytems.eth")?,
                },
            }
        })
    );
    assert_persisted_primary_name_fallback_metadata(&payload);
    assert_eq!(
        payload.coverage,
        json!({
            "status": "partial",
            "exhaustiveness": "non_enumerable",
            "source_classes_considered": ["ens_reverse_rpc", "ens_execution"],
            "enumeration_basis": "primary_name_lookup",
            "unsupported_reason": null,
        })
    );

    let rpc_requests = join_primary_name_mock_rpc_requests(rpc_handle).await?;
    assert_eq!(rpc_requests.len(), 3);
    for request in &rpc_requests {
        assert_eq!(
            request["params"][1],
            primary_name_selected_block_selector()
        );
    }
    assert_eq!(
        rpc_requests[2]["params"][0]["to"],
        bigname_execution::ENS_UNIVERSAL_RESOLVER_ADDRESS
    );

    let database_url = std::env::var("BIGNAME_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .unwrap_or_else(|_| default_database_url().to_owned());
    let projection_pool = PgPool::connect_with(
        PgConnectOptions::from_str(&database_url)?
            .database(&database.database_name)
            .disable_statement_logging(),
    )
    .await?;
    let mut unrelated_projection_write = projection_pool.begin().await?;
    bigname_storage::lock_primary_name_tuple_in_transaction(
        &mut unrelated_projection_write,
        "0x0000000000000000000000000000000000000def",
        "ens",
        "60",
    )
    .await?;
    let unrelated_lock_state = database.app_state();
    let unrelated_lock_response = tokio::time::timeout(
        std::time::Duration::from_millis(250),
        app_router(unrelated_lock_state).oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x8e8db5ccef88cca9d624701db544989c996e3216?mode=both")
                .body(Body::empty())
                .expect("request must build"),
        ),
    )
    .await
    .context("unrelated primary-name tuple lock must not block route-local readback")?
    .context("unrelated-lock primary-name readback request failed")?;
    unrelated_projection_write.commit().await?;
    assert_eq!(unrelated_lock_response.status(), StatusCode::OK);

    let mut projection_write = projection_pool.begin().await?;
    bigname_storage::lock_primary_name_tuple_in_transaction(
        &mut projection_write,
        "0x8e8db5ccef88cca9d624701db544989c996e3216",
        "ens",
        "60",
    )
    .await?;
    let readback_state = database.app_state();
    let mut readback_task = tokio::spawn(async move {
        app_router(readback_state)
            .oneshot(
                Request::builder()
                    .uri("/v1/primary-names/0x8e8db5ccef88cca9d624701db544989c996e3216?mode=both")
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .context("persisted primary-name fallback readback request failed")
    });
    let completed_while_projection_write_locked = tokio::time::timeout(
        std::time::Duration::from_millis(250),
        &mut readback_task,
    )
    .await;
    projection_write.commit().await?;
    let readback_was_serialized = completed_while_projection_write_locked.is_err();
    let second_response = match completed_while_projection_write_locked {
        Ok(response) => response.context("persisted readback task panicked")??,
        Err(_) => readback_task
            .await
            .context("persisted readback task panicked")??,
    };
    assert!(
        readback_was_serialized,
        "route-local trace readback must serialize with projection writes"
    );
    assert_eq!(second_response.status(), StatusCode::OK);
    let second_payload: PrimaryNameResponse = read_json(second_response).await?;
    assert_eq!(second_payload, payload);

    let mut projection_insert = projection_pool.begin().await?;
    bigname_storage::lock_primary_name_tuple_in_transaction(
        &mut projection_insert,
        "0x8e8db5ccef88cca9d624701db544989c996e3216",
        "ens",
        "60",
    )
    .await?;
    let raced_readback_state = database.app_state();
    let mut raced_readback_task = tokio::spawn(async move {
        app_router(raced_readback_state)
            .oneshot(
                Request::builder()
                    .uri("/v1/primary-names/0x8e8db5ccef88cca9d624701db544989c996e3216?mode=both")
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .context("projection-raced primary-name fallback readback request failed")
    });
    let raced_readback_before_insert = tokio::time::timeout(
        std::time::Duration::from_millis(250),
        &mut raced_readback_task,
    )
    .await;
    assert!(
        raced_readback_before_insert.is_err(),
        "route-local trace readback must wait for the projection write fence"
    );
    sqlx::query(
        r#"
        INSERT INTO primary_names_current (
            address,
            namespace,
            coin_type,
            claim_status,
            raw_claim_name,
            normalized_claim_name,
            claim_name_is_normalized,
            claim_provenance
        )
        VALUES ($1, 'ens', '60', 'not_found', NULL, NULL, FALSE, '{}'::jsonb)
        "#,
    )
    .bind("0x8e8db5ccef88cca9d624701db544989c996e3216")
    .execute(&mut *projection_insert)
    .await?;
    projection_insert.commit().await?;
    let raced_response = raced_readback_task
        .await
        .context("projection-raced persisted readback task panicked")??;
    assert_eq!(raced_response.status(), StatusCode::OK);
    let raced_payload: PrimaryNameResponse = read_json(raced_response).await?;
    assert_eq!(
        raced_payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "not_found",
                "provenance": {},
            }
        }))
    );
    assert_eq!(
        raced_payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "not_found",
            }
        }))
    );
    assert!(raced_payload.provenance.is_null());
    assert_eq!(raced_payload.chain_positions, json!({}));
    projection_pool.close().await;

    let persisted_outcome_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM execution_cache_outcomes
        WHERE request_type = 'verified_primary_name'
          AND namespace = 'ens'
          AND request_key = 'ens:0x8e8db5ccef88cca9d624701db544989c996e3216:60'
        "#,
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(
        persisted_outcome_count, 0,
        "the projection write must invalidate the route-local outcome after winning the fence"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_serves_projection_that_wins_route_local_persistence_fence()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_default_ens_primary_name_fallback_context()
        .await?;
    let (rpc_url, last_rpc_request_reached, release_last_rpc_response, rpc_handle) =
        spawn_primary_name_mock_rpc_with_last_response_gate(vec![
        json!("0x000000000000000000000000a2c122be93b0074270ebee7f6b7292c7deb45047"),
        primary_name_reverse_name_response("taytems.eth"),
        primary_name_universal_resolver_addr60_response(
            "0x8e8db5ccef88cca9d624701db544989c996e3216",
        ),
    ])
    .await?;
    let chain_rpc_urls =
        bigname_execution::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;

    let database_url = std::env::var("BIGNAME_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .unwrap_or_else(|_| default_database_url().to_owned());
    let projection_pool = PgPool::connect_with(
        PgConnectOptions::from_str(&database_url)?
            .database(&database.database_name)
            .disable_statement_logging(),
    )
    .await?;
    let request_state = database.app_state_with_chain_rpc_urls(chain_rpc_urls);
    let mut request_task = tokio::spawn(async move {
        app_router(request_state)
            .oneshot(
                Request::builder()
                    .uri("/v1/primary-names/0x8e8db5ccef88cca9d624701db544989c996e3216?mode=both")
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .context("projection-raced route-local primary-name request failed")
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        last_rpc_request_reached,
    )
    .await
    .context("route-local primary-name RPC did not reach its final call")?
    .context("route-local primary-name RPC last-call signal dropped")?;

    let mut projection_insert = projection_pool.begin().await?;
    bigname_storage::lock_primary_name_tuple_in_transaction(
        &mut projection_insert,
        "0x8e8db5ccef88cca9d624701db544989c996e3216",
        "ens",
        "60",
    )
    .await?;
    assert!(
        release_last_rpc_response.send(()).is_ok(),
        "route-local primary-name request dropped before its final RPC response"
    );
    let rpc_requests = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        join_primary_name_mock_rpc_requests(rpc_handle),
    )
    .await
    .context("route-local primary-name RPC calls did not complete")??;
    assert_eq!(rpc_requests.len(), 3);

    let raced_before_insert = tokio::time::timeout(
        std::time::Duration::from_millis(250),
        &mut request_task,
    )
    .await;
    assert!(
        raced_before_insert.is_err(),
        "route-local persistence must wait for the projection-write fence"
    );
    sqlx::query(
        r#"
        INSERT INTO primary_names_current (
            address,
            namespace,
            coin_type,
            claim_status,
            raw_claim_name,
            normalized_claim_name,
            claim_name_is_normalized,
            claim_provenance
        )
        VALUES ($1, 'ens', '60', 'not_found', NULL, NULL, FALSE, '{}'::jsonb)
        "#,
    )
    .bind("0x8e8db5ccef88cca9d624701db544989c996e3216")
    .execute(&mut *projection_insert)
    .await?;
    projection_insert.commit().await?;

    let raced_response = request_task
        .await
        .context("projection-raced route-local primary-name task panicked")??;
    assert_eq!(raced_response.status(), StatusCode::OK);
    let raced_payload: PrimaryNameResponse = read_json(raced_response).await?;
    assert_eq!(
        raced_payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "not_found",
                "provenance": {},
            }
        }))
    );
    assert_eq!(
        raced_payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "not_found",
            }
        }))
    );
    assert!(raced_payload.provenance.is_null());
    assert_eq!(raced_payload.chain_positions, json!({}));

    let persisted_outcome_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM execution_cache_outcomes
        WHERE request_type = 'verified_primary_name'
          AND namespace = 'ens'
          AND request_key = 'ens:0x8e8db5ccef88cca9d624701db544989c996e3216:60'
        "#,
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(persisted_outcome_count, 0);

    projection_pool.close().await;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_rejects_case_variant_on_demand_claim_before_forward_lookup()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_default_ens_primary_name_fallback_context()
        .await?;
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![
        json!("0x000000000000000000000000a2c122be93b0074270ebee7f6b7292c7deb45047"),
        primary_name_reverse_name_response("Taytems.eth"),
    ])
    .await?;
    let chain_rpc_urls =
        bigname_execution::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;

    let response = app_router(database.app_state_with_chain_rpc_urls(chain_rpc_urls))
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x8e8db5ccef88cca9d624701db544989c996e3216?mode=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("case-variant on-demand primary-name request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "success",
                "name": "taytems.eth",
                "provenance": {
                    "source_family": "ens_reverse_rpc",
                    "resolver_address": "0xa2c122be93b0074270ebee7f6b7292c7deb45047",
                },
            }
        }))
    );
    assert_eq!(
        primary_name_verified_state_without_provenance(&payload),
        json!({
            "verified_primary_name": {
                "status": "invalid_name",
                "failure_reason": bigname_execution::VERIFIED_PRIMARY_NAME_CLAIM_NOT_NORMALIZED_REASON,
            }
        })
    );
    assert_eq!(
        payload.coverage,
        json!({
            "status": "partial",
            "exhaustiveness": "non_enumerable",
            "source_classes_considered": ["ens_reverse_rpc"],
            "enumeration_basis": "primary_name_lookup",
            "unsupported_reason": null,
        })
    );
    assert_persisted_primary_name_fallback_metadata(&payload);
    assert_eq!(join_primary_name_mock_rpc_requests(rpc_handle).await?.len(), 2);
    let execution_trace_id = payload.provenance["execution_trace_id"]
        .as_str()
        .context("persisted fallback provenance must include execution_trace_id")?
        .parse::<Uuid>()?;
    let trace = bigname_storage::load_execution_trace(&database.pool, execution_trace_id)
        .await?
        .context("normalization-gated fallback trace must be durable")?;
    assert_eq!(
        trace
            .steps
            .iter()
            .map(|step| step.step_kind.as_str())
            .collect::<Vec<_>>(),
        vec!["call_ens_reverse_lookup", "normalize_claimed_name"]
    );
    assert!(
        trace
            .steps
            .iter()
            .all(|step| step.step_kind != "call_universal_resolver")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_does_not_trim_on_demand_claim_before_verification() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_default_ens_primary_name_fallback_context()
        .await?;
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![
        json!("0x000000000000000000000000a2c122be93b0074270ebee7f6b7292c7deb45047"),
        primary_name_reverse_name_response(" taytems.eth "),
    ])
    .await?;
    let chain_rpc_urls =
        bigname_execution::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;

    let response = app_router(database.app_state_with_chain_rpc_urls(chain_rpc_urls))
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x8e8db5ccef88cca9d624701db544989c996e3216?mode=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("whitespace-padded on-demand primary-name request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "invalid_name",
                "raw_claim_name": " taytems.eth ",
                "provenance": {
                    "source_family": "ens_reverse_rpc",
                    "resolver_address": "0xa2c122be93b0074270ebee7f6b7292c7deb45047",
                },
            }
        }))
    );
    assert_eq!(
        primary_name_verified_state_without_provenance(&payload),
        json!({
            "verified_primary_name": {
                "status": "invalid_name",
                "failure_reason": "claim_name_not_normalizable",
            }
        })
    );
    assert_persisted_primary_name_fallback_metadata(&payload);
    assert_eq!(join_primary_name_mock_rpc_requests(rpc_handle).await?.len(), 2);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_reports_on_demand_forward_addr_miss() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_default_ens_primary_name_fallback_context()
        .await?;
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![
        json!("0x000000000000000000000000a2c122be93b0074270ebee7f6b7292c7deb45047"),
        json!(
            "0x0000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000b74617974656d732e657468000000000000000000000000000000000000000000"
        ),
        primary_name_universal_resolver_addr60_response(
            "0x0000000000000000000000000000000000000000",
        ),
    ])
    .await?;
    let chain_rpc_urls =
        bigname_execution::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;

    let response = app_router(database.app_state_with_chain_rpc_urls(chain_rpc_urls))
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x8e8db5ccef88cca9d624701db544989c996e3216?mode=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("primary-name on-demand forward miss request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        primary_name_verified_state_without_provenance(&payload),
        json!({
            "verified_primary_name": {
                "status": "not_found",
            }
        })
    );
    assert_eq!(
        payload.coverage,
        json!({
            "status": "partial",
            "exhaustiveness": "non_enumerable",
            "source_classes_considered": ["ens_reverse_rpc", "ens_execution"],
            "enumeration_basis": "primary_name_lookup",
            "unsupported_reason": null,
        })
    );
    assert_persisted_primary_name_fallback_metadata(&payload);

    assert_eq!(join_primary_name_mock_rpc_requests(rpc_handle).await?.len(), 3);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_reports_partial_coverage_for_on_demand_rpc_miss() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.seed_default_ens_snapshot_selector_position().await?;
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![json!(
        "0x0000000000000000000000000000000000000000000000000000000000000000"
    )])
    .await?;
    let chain_rpc_urls =
        bigname_execution::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;

    let response = app_router(database.app_state_with_chain_rpc_urls(chain_rpc_urls))
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x8e8db5ccef88cca9d624701db544989c996e3216")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("primary-name on-demand miss request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "not_found",
            }
        }))
    );
    assert_eq!(payload.verified_state, None);
    assert_eq!(
        payload.coverage,
        json!({
            "status": "partial",
            "exhaustiveness": "non_enumerable",
            "source_classes_considered": ["ens_reverse_rpc"],
            "enumeration_basis": "primary_name_lookup",
            "unsupported_reason": null,
        })
    );

    let rpc_requests = join_primary_name_mock_rpc_requests(rpc_handle).await?;
    assert_eq!(rpc_requests.len(), 1);
    assert_eq!(rpc_requests[0]["method"], "eth_call");
    assert_eq!(
        rpc_requests[0]["params"][0]["to"],
        bigname_execution::ENS_REGISTRY_ADDRESS
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_surfaces_reverse_provider_failure_as_execution_failed() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.seed_default_ens_snapshot_selector_position().await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x8e8db5ccef88cca9d624701db544989c996e3216")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("unconfigured primary-name fallback request failed")?;

    assert_eq!(response.status(), StatusCode::OK);
    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "execution_failed",
                "failure_reason": "resolver_call_failed",
            }
        }))
    );
    assert_eq!(
        payload.coverage,
        json!({
            "status": "partial",
            "exhaustiveness": "non_enumerable",
            "source_classes_considered": ["ens_reverse_rpc"],
            "enumeration_basis": "primary_name_lookup",
            "unsupported_reason": null,
        })
    );
    assert_eq!(
        payload.provenance,
        json!({ "source_family": "ens_reverse_rpc" })
    );
    assert_eq!(payload.chain_positions, primary_name_fallback_chain_positions());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_freezes_bootstrap_mode_envelopes() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let default_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000ABC?namespace=ens&coin_type=60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("default primary-name request failed")?;
    let declared_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=declared")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("declared primary-name request failed")?;
    let verified_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("verified primary-name request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("both primary-name request failed")?;

    assert_eq!(default_response.status(), StatusCode::OK);
    assert_eq!(declared_response.status(), StatusCode::OK);
    assert_eq!(verified_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let default_payload: PrimaryNameResponse = read_json(default_response).await?;
    let declared_payload: PrimaryNameResponse = read_json(declared_response).await?;
    let verified_payload: PrimaryNameResponse = read_json(verified_response).await?;
    let both_payload: PrimaryNameResponse = read_json(both_response).await?;

    assert_eq!(
        default_payload.data,
        json!({
            "address": "0x0000000000000000000000000000000000000abc",
            "namespace": "ens",
            "coin_type": "60",
        })
    );
    assert_eq!(default_payload.data, declared_payload.data);
    assert_eq!(default_payload.data, verified_payload.data);
    assert_eq!(default_payload.data, both_payload.data);

    assert_eq!(
        default_payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "unsupported",
                "unsupported_reason": "declared primary-name claim surface is not yet supported",
            }
        }))
    );
    assert_eq!(
        declared_payload.declared_state,
        default_payload.declared_state
    );
    assert_eq!(default_payload.verified_state, None);
    assert_eq!(declared_payload.verified_state, None);
    assert_eq!(verified_payload.declared_state, None);
    assert_eq!(
        verified_payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "unsupported",
                "unsupported_reason": "verified primary-name entrypoint is not yet supported",
            }
        }))
    );
    assert_eq!(
        both_payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "unsupported",
                "unsupported_reason": "declared primary-name claim surface is not yet supported",
            }
        }))
    );
    assert_eq!(both_payload.verified_state, verified_payload.verified_state);
    assert_eq!(default_payload.coverage, primary_name_unsupported_coverage());
    assert!(default_payload.provenance.is_null());
    assert_eq!(default_payload.chain_positions, json!({}));
    assert_eq!(default_payload.consistency, "head");
    assert!(default_payload.last_updated.ends_with('Z'));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_does_not_conflate_tuple_miss_provider_failure_with_not_found()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_default_ens_primary_name_fallback_context()
        .await?;
    database
        .insert_primary_name_current_row("0x0000000000000000000000000000000000000abc", "ens", "61")
        .await?;

    let other_verified_primary_name = json!({
        "status": "success",
        "name": {
            "logical_name_id": "ens:other.eth",
            "namespace": "ens",
            "normalized_name": "other.eth",
            "canonical_display_name": "other.eth",
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000456",
            "resource_id": "00000000-0000-0000-0000-000000000999",
            "binding_kind": "declared_registry_path"
        }
    });
    let other_trace = primary_name_execution_trace(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000031),
        "ens",
        "0x0000000000000000000000000000000000000abc",
        "61",
        other_verified_primary_name.clone(),
        timestamp(1_717_172_301),
    );
    let other_outcome = primary_name_execution_outcome(
        other_trace.execution_trace_id,
        "ens",
        "0x0000000000000000000000000000000000000abc",
        "61",
        other_verified_primary_name,
        timestamp(1_717_172_301),
    );
    upsert_execution_trace(&database.pool, &other_trace).await?;
    upsert_execution_outcome(&database.pool, &other_outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("primary-name tuple miss request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "execution_failed",
                "failure_reason": "resolver_call_failed",
            }
        }))
    );
    assert_eq!(
        primary_name_verified_state_without_provenance(&payload),
        json!({
            "verified_primary_name": {
                "status": "execution_failed",
                "failure_reason": "resolver_call_failed",
            }
        })
    );
    assert_persisted_primary_name_fallback_metadata(&payload);
    let execution_trace_id = payload.provenance["execution_trace_id"]
        .as_str()
        .context("provider-failure fallback must include execution_trace_id")?
        .parse::<Uuid>()?;
    let trace = bigname_storage::load_execution_trace(&database.pool, execution_trace_id)
        .await?
        .context("provider-failure fallback trace must be durable")?;
    assert_eq!(trace.contracts_called, json!([]));
    assert_eq!(
        payload.coverage,
        json!({
            "status": "partial",
            "exhaustiveness": "non_enumerable",
            "source_classes_considered": ["ens_reverse_rpc"],
            "enumeration_basis": "primary_name_lookup",
            "unsupported_reason": null,
        })
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_does_not_persist_transient_transport_error_and_retries()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_default_ens_primary_name_fallback_context()
        .await?;
    let (rpc_url, rpc_handle) = spawn_primary_name_transport_recovery_rpc().await?;
    let chain_rpc_urls = bigname_execution::ChainRpcUrls::from_entries(&[format!(
        "ethereum-mainnet={rpc_url}"
    )])?
    .with_http_timeouts(
        std::time::Duration::from_millis(100),
        std::time::Duration::from_secs(1),
    )?;
    let uri = "/v1/primary-names/0x8e8db5ccef88cca9d624701db544989c996e3216?mode=both";

    let first_response = app_router(
        database.app_state_with_chain_rpc_urls(chain_rpc_urls.clone()),
    )
    .oneshot(
        Request::builder()
            .uri(uri)
            .body(Body::empty())
            .expect("request must build"),
    )
    .await
    .context("transient-failure primary-name request failed")?;
    assert_eq!(first_response.status(), StatusCode::CONFLICT);

    let persisted_after_failure = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM execution_cache_outcomes
        WHERE request_type = 'verified_primary_name'
          AND namespace = 'ens'
          AND request_key = 'ens:0x8e8db5ccef88cca9d624701db544989c996e3216:60'
        "#,
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(persisted_after_failure, 0);
    let persisted_trace_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM execution_traces
        WHERE request_type = 'verified_primary_name'
          AND namespace = 'ens'
          AND request_key = 'ens:0x8e8db5ccef88cca9d624701db544989c996e3216:60'
        "#,
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(
        persisted_trace_count, 0,
        "non-timeout transport failures must abort before trace persistence"
    );

    let recovered_response = app_router(database.app_state_with_chain_rpc_urls(chain_rpc_urls))
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("recovered primary-name retry failed")?;
    assert_eq!(recovered_response.status(), StatusCode::OK);
    let recovered_payload: PrimaryNameResponse = read_json(recovered_response).await?;
    assert_eq!(
        primary_name_verified_state_without_provenance(&recovered_payload)["verified_primary_name"]
            ["status"],
        json!("success")
    );
    assert_persisted_primary_name_fallback_metadata(&recovered_payload);
    assert_eq!(join_primary_name_mock_rpc_requests(rpc_handle).await?.len(), 4);

    let persisted_after_retry = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM execution_cache_outcomes
        WHERE request_type = 'verified_primary_name'
          AND namespace = 'ens'
          AND request_key = 'ens:0x8e8db5ccef88cca9d624701db544989c996e3216:60'
        "#,
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(persisted_after_retry, 1);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_does_not_persist_gateway_connect_failure_and_retries()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_default_ens_primary_name_fallback_context()
        .await?;
    let unavailable_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let unavailable_gateway = format!("http://{}", unavailable_listener.local_addr()?);
    drop(unavailable_listener);
    let offchain_lookup = encoded_primary_name_offchain_lookup(format!(
        "{unavailable_gateway}/{{data}}"
    ))?;
    let address = "0x8e8db5ccef88cca9d624701db544989c996e3216";
    let resolver = json!("0x000000000000000000000000a2c122be93b0074270ebee7f6b7292c7deb45047");
    let reverse_name = primary_name_reverse_name_response("taytems.eth");
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc_responses(vec![
        PrimaryNameMockRpcResponse::Result(resolver.clone()),
        PrimaryNameMockRpcResponse::Result(reverse_name.clone()),
        PrimaryNameMockRpcResponse::Error {
            code: 3,
            message: "execution reverted".to_owned(),
            data: json!(offchain_lookup),
        },
        PrimaryNameMockRpcResponse::Result(resolver),
        PrimaryNameMockRpcResponse::Result(reverse_name),
        PrimaryNameMockRpcResponse::Result(primary_name_universal_resolver_addr60_response(
            address,
        )),
    ])
    .await?;
    let chain_rpc_urls = bigname_execution::ChainRpcUrls::from_entries(&[format!(
        "ethereum-mainnet={rpc_url}"
    )])?;
    let uri = format!("/v1/primary-names/{address}?mode=both");

    let first_response = app_router(
        database.app_state_with_chain_rpc_urls(chain_rpc_urls.clone()),
    )
    .oneshot(
        Request::builder()
            .uri(&uri)
            .body(Body::empty())
            .expect("request must build"),
    )
    .await
    .context("gateway-failure primary-name request failed")?;
    assert_eq!(first_response.status(), StatusCode::CONFLICT);

    let persisted_after_failure: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM execution_cache_outcomes WHERE request_type = 'verified_primary_name' AND namespace = 'ens' AND request_key = 'ens:0x8e8db5ccef88cca9d624701db544989c996e3216:60'",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(persisted_after_failure, 0);
    let trace_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM execution_traces WHERE request_type = 'verified_primary_name' AND namespace = 'ens' AND request_key = 'ens:0x8e8db5ccef88cca9d624701db544989c996e3216:60'",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(trace_count, 0, "gateway failure must not persist a trace");

    let recovered_response = app_router(database.app_state_with_chain_rpc_urls(chain_rpc_urls))
        .oneshot(
            Request::builder()
                .uri(&uri)
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("recovered gateway primary-name retry failed")?;
    assert_eq!(recovered_response.status(), StatusCode::OK);
    let recovered_payload: PrimaryNameResponse = read_json(recovered_response).await?;
    assert_eq!(
        primary_name_verified_state_without_provenance(&recovered_payload)
            ["verified_primary_name"]["status"],
        json!("success")
    );
    assert_eq!(join_primary_name_mock_rpc_requests(rpc_handle).await?.len(), 6);

    let persisted_after_retry: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM execution_cache_outcomes WHERE request_type = 'verified_primary_name' AND namespace = 'ens' AND request_key = 'ens:0x8e8db5ccef88cca9d624701db544989c996e3216:60'",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(persisted_after_retry, 1);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_does_not_persist_callback_transport_failure_and_retries()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_default_ens_primary_name_fallback_context()
        .await?;
    let address = "0x8e8db5ccef88cca9d624701db544989c996e3216";
    let (gateway_url, gateway) = spawn_successful_primary_name_gateway().await?;
    let offchain_lookup =
        encoded_primary_name_offchain_lookup(format!("{gateway_url}/{{data}}"))?;
    let (rpc_url, rpc) =
        spawn_primary_name_callback_drop_then_recovery_rpc(offchain_lookup, address).await?;
    let chain_rpc_urls = bigname_execution::ChainRpcUrls::from_entries(&[format!(
        "ethereum-mainnet={rpc_url}"
    )])?;
    let uri = format!("/v1/primary-names/{address}?mode=both");

    let first_response = app_router(
        database.app_state_with_chain_rpc_urls(chain_rpc_urls.clone()),
    )
    .oneshot(
        Request::builder()
            .uri(&uri)
            .body(Body::empty())
            .expect("request must build"),
    )
    .await
    .context("callback-transport-failure primary-name request failed")?;
    assert_eq!(first_response.status(), StatusCode::CONFLICT);
    gateway.await??;

    let persisted_after_failure: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM execution_cache_outcomes WHERE request_type = 'verified_primary_name' AND namespace = 'ens' AND request_key = 'ens:0x8e8db5ccef88cca9d624701db544989c996e3216:60'",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(persisted_after_failure, 0);
    let trace_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM execution_traces WHERE request_type = 'verified_primary_name' AND namespace = 'ens' AND request_key = 'ens:0x8e8db5ccef88cca9d624701db544989c996e3216:60'",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(trace_count, 0, "callback transport failure must not persist a trace");

    let recovered_response = app_router(database.app_state_with_chain_rpc_urls(chain_rpc_urls))
        .oneshot(
            Request::builder()
                .uri(&uri)
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("recovered callback primary-name retry failed")?;
    assert_eq!(recovered_response.status(), StatusCode::OK);
    let recovered_payload: PrimaryNameResponse = read_json(recovered_response).await?;
    assert_eq!(
        primary_name_verified_state_without_provenance(&recovered_payload)
            ["verified_primary_name"]["status"],
        json!("success")
    );
    assert_eq!(join_primary_name_mock_rpc_requests(rpc).await?.len(), 7);

    let persisted_after_retry: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM execution_cache_outcomes WHERE request_type = 'verified_primary_name' AND namespace = 'ens' AND request_key = 'ens:0x8e8db5ccef88cca9d624701db544989c996e3216:60'",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(persisted_after_retry, 1);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_persists_provider_response_timeout_in_band() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_default_ens_primary_name_fallback_context()
        .await?;
    let (rpc_url, rpc_handle) = spawn_hanging_primary_name_rpc().await?;
    let chain_rpc_urls = bigname_execution::ChainRpcUrls::from_entries(&[format!(
        "ethereum-mainnet={rpc_url}"
    )])?
    .with_http_timeouts(
        std::time::Duration::from_millis(10),
        std::time::Duration::from_millis(25),
    )?;

    let response = app_router(database.app_state_with_chain_rpc_urls(chain_rpc_urls))
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x8e8db5ccef88cca9d624701db544989c996e3216?mode=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("provider-response-timeout primary-name request failed")?;
    rpc_handle.abort();
    assert_eq!(response.status(), StatusCode::OK);
    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        primary_name_verified_state_without_provenance(&payload)["verified_primary_name"],
        json!({
            "status": "execution_failed",
            "failure_reason": "resolver_call_failed",
        })
    );
    assert_persisted_primary_name_fallback_metadata(&payload);

    let persisted_outcome_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM execution_cache_outcomes
        WHERE request_type = 'verified_primary_name'
          AND namespace = 'ens'
          AND request_key = 'ens:0x8e8db5ccef88cca9d624701db544989c996e3216:60'
        "#,
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(persisted_outcome_count, 1);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_persists_callback_response_timeout_in_band() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_default_ens_primary_name_fallback_context()
        .await?;
    let (gateway_url, gateway) = spawn_successful_primary_name_gateway().await?;
    let offchain_lookup =
        encoded_primary_name_offchain_lookup(format!("{gateway_url}/{{data}}"))?;
    let (rpc_url, rpc) = spawn_hanging_primary_name_callback_rpc(offchain_lookup).await?;
    let chain_rpc_urls = bigname_execution::ChainRpcUrls::from_entries(&[format!(
        "ethereum-mainnet={rpc_url}"
    )])?
    .with_http_timeouts(
        std::time::Duration::from_millis(10),
        std::time::Duration::from_millis(25),
    )?;

    let response = app_router(database.app_state_with_chain_rpc_urls(chain_rpc_urls))
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x8e8db5ccef88cca9d624701db544989c996e3216?mode=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("callback-response-timeout primary-name request failed")?;
    rpc.abort();
    gateway.await??;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        primary_name_verified_state_without_provenance(&payload)["verified_primary_name"],
        json!({
            "status": "execution_failed",
            "failure_reason": "resolver_call_failed",
        })
    );
    assert_persisted_primary_name_fallback_metadata(&payload);
    let execution_trace_id = payload.provenance["execution_trace_id"]
        .as_str()
        .context("callback response timeout must expose a persisted execution trace")?
        .parse::<Uuid>()?;
    let trace = bigname_storage::load_execution_trace(&database.pool, execution_trace_id)
        .await?
        .context("callback response timeout trace must persist")?;
    assert!(trace.steps.iter().any(|step| {
        step.step_kind == "ccip_offchain_lookup"
            && step.step_payload["configured_timeout"] == json!(true)
            && step.step_payload["provider_callback"] == json!(true)
    }));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_persists_gateway_response_timeout_in_band() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_default_ens_primary_name_fallback_context()
        .await?;
    let (gateway_url, gateway_handle) = spawn_hanging_primary_name_gateway().await?;
    let offchain_lookup =
        encoded_primary_name_offchain_lookup(format!("{gateway_url}/{{data}}"))?;
    let resolver = json!("0x000000000000000000000000a2c122be93b0074270ebee7f6b7292c7deb45047");
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc_responses(vec![
        PrimaryNameMockRpcResponse::Result(resolver),
        PrimaryNameMockRpcResponse::Result(primary_name_reverse_name_response("taytems.eth")),
        PrimaryNameMockRpcResponse::Error {
            code: 3,
            message: "execution reverted".to_owned(),
            data: json!(offchain_lookup),
        },
    ])
    .await?;
    let chain_rpc_urls = bigname_execution::ChainRpcUrls::from_entries(&[format!(
        "ethereum-mainnet={rpc_url}"
    )])?;
    let uri = "/v1/primary-names/0x8e8db5ccef88cca9d624701db544989c996e3216?mode=both";

    let response = app_router(
        database.app_state_with_chain_rpc_urls(chain_rpc_urls.clone()),
    )
    .oneshot(
        Request::builder()
            .uri(uri)
            .body(Body::empty())
            .expect("request must build"),
    )
    .await
    .context("gateway-response-timeout primary-name request failed")?;
    gateway_handle.abort();
    assert_eq!(response.status(), StatusCode::OK);
    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        primary_name_verified_state_without_provenance(&payload)["verified_primary_name"],
        json!({
            "status": "execution_failed",
            "failure_reason": "resolver_call_failed",
        })
    );
    assert_persisted_primary_name_fallback_metadata(&payload);
    let execution_trace_id = payload.provenance["execution_trace_id"]
        .as_str()
        .context("gateway response timeout must expose a persisted execution trace")?
        .parse::<Uuid>()?;
    let trace = bigname_storage::load_execution_trace(&database.pool, execution_trace_id)
        .await?
        .context("gateway response timeout trace must persist")?;
    assert!(trace.steps.iter().any(|step| {
        step.step_kind == "ccip_offchain_lookup"
            && step.step_payload["configured_timeout"] == json!(true)
    }));

    let cached_response = app_router(database.app_state_with_chain_rpc_urls(chain_rpc_urls))
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("cached gateway-response-timeout primary-name request failed")?;
    assert_eq!(cached_response.status(), StatusCode::OK);
    let cached_payload: PrimaryNameResponse = read_json(cached_response).await?;
    assert_eq!(
        cached_payload.provenance["execution_trace_id"],
        json!(execution_trace_id.to_string())
    );
    assert_eq!(join_primary_name_mock_rpc_requests(rpc_handle).await?.len(), 3);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn primary_name_readback_treats_concurrent_route_cache_pruning_as_a_miss() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database
        .seed_default_ens_primary_name_fallback_context()
        .await?;
    let address = "0x8e8db5ccef88cca9d624701db544989c996e3216";
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(vec![
        json!("0x000000000000000000000000a2c122be93b0074270ebee7f6b7292c7deb45047"),
        primary_name_reverse_name_response("taytems.eth"),
        primary_name_universal_resolver_addr60_response(address),
    ])
    .await?;
    let chain_rpc_urls =
        bigname_execution::ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;
    let response = app_router(database.app_state_with_chain_rpc_urls(chain_rpc_urls))
        .oneshot(
            Request::builder()
                .uri(format!("/v1/primary-names/{address}?mode=both"))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(join_primary_name_mock_rpc_requests(rpc_handle).await?.len(), 3);

    let selected_snapshot = SelectedSnapshot {
        chain_positions: ChainPositions::from_value(&primary_name_fallback_chain_positions())?,
        consistency: SnapshotConsistency::Head,
    };
    let database_url = std::env::var("BIGNAME_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .unwrap_or_else(|_| default_database_url().to_owned());
    let prune_pool = PgPool::connect_with(
        PgConnectOptions::from_str(&database_url)?
            .database(&database.database_name)
            .disable_statement_logging(),
    )
    .await?;
    let (_hook_guard, hook_control) =
        support_primary_name_lookup::test_hooks::install(&database.pool).await?;
    let readback_pool = database.pool.clone();
    let readback_task = tokio::spawn(async move {
        load_persisted_primary_name_route_fallback_readback(
            &readback_pool,
            address,
            "ens",
            "60",
            &selected_snapshot,
        )
        .await
    });
    hook_control.wait_until_reached().await;

    sqlx::query(
        r#"
        UPDATE chain_checkpoints
        SET canonical_block_number = 21100003
        WHERE chain_id = 'ethereum-mainnet'
        "#,
    )
    .execute(&prune_pool)
    .await?;
    let summary = bigname_storage::prune_route_local_primary_name_execution(
        &prune_pool,
        1,
        10,
    )
    .await?;
    assert_eq!(summary.deleted_outcome_count, 1);
    assert_eq!(summary.deleted_trace_count, 1);
    hook_control.resume().await;

    let readback = readback_task
        .await
        .context("primary-name pruning readback task panicked")?
        .map_err(|error| {
            anyhow::anyhow!(
                "concurrent primary-name pruning returned {} {}: {}",
                error.status,
                error.code,
                error.message
            )
        })?;
    assert!(
        readback.is_none(),
        "a concurrently pruned route-local artifact must be treated as a cache miss"
    );

    prune_pool.close().await;
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_gates_case_variant_claim_even_with_persisted_success() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    upsert_normalized_events(
        &database.pool,
        &[
            primary_name_reverse_changed_event(
                "reverse-a-60",
                "0x0000000000000000000000000000000000000abc",
                "60",
                250,
                0,
                CanonicalityState::Canonical,
            ),
            primary_name_reverse_linked_name_event(
                "record-a-60-success",
                "0x0000000000000000000000000000000000000abc",
                "60",
                Some("Alice.eth"),
                251,
                0,
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;
    worker_primary_name::rebuild_primary_names_current(
        &database.pool,
        Some("0x0000000000000000000000000000000000000abc"),
        Some("ens"),
        Some("60"),
    )
    .await?;

    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000003f);
    let finished_at = timestamp(1_717_172_400);
    let verified_primary_name = json!({
        "status": "success",
        "name": {
            "logical_name_id": "ens:alice.eth",
            "namespace": "ens",
            "normalized_name": "alice.eth",
            "canonical_display_name": "Alice.eth",
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
            "resource_id": "00000000-0000-0000-0000-000000000456",
            "binding_kind": "declared_registry_path"
        }
    });
    upsert_execution_trace(
        &database.pool,
        &primary_name_execution_trace(
            execution_trace_id,
            "ens",
            address,
            "60",
            verified_primary_name.clone(),
            finished_at,
        ),
    )
    .await?;
    upsert_execution_outcome(
        &database.pool,
        &primary_name_execution_outcome(
            execution_trace_id,
            "ens",
            address,
            "60",
            verified_primary_name,
            finished_at,
        ),
    )
    .await?;

    let declared_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=declared")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("declared primary-name status request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed primary-name status request failed")?;

    assert_eq!(declared_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let declared_payload: PrimaryNameResponse = read_json(declared_response).await?;
    let both_payload: PrimaryNameResponse = read_json(both_response).await?;

    assert_eq!(
        declared_payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "success",
                "name": "alice.eth",
                "provenance": {
                    "source_family": "ens_v1_reverse_l1",
                    "contract_role": "reverse_registrar",
                    "contract_instance_id": "00000000-0000-0000-0000-0000000000fa",
                    "emitting_address": "0x00000000000000000000000000000000000000ad",
                },
            }
        }))
    );
    assert_eq!(declared_payload.verified_state, None);
    assert_eq!(both_payload.declared_state, declared_payload.declared_state);
    assert_eq!(
        both_payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "invalid_name",
                "failure_reason": bigname_execution::VERIFIED_PRIMARY_NAME_CLAIM_NOT_NORMALIZED_REASON,
            }
        }))
    );
    assert_eq!(declared_payload.coverage, primary_name_supported_coverage("ens"));
    assert_eq!(both_payload.coverage, primary_name_supported_coverage("ens"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_reads_basenames_declared_claim_status_for_exact_tuple() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000bcd";
    upsert_normalized_events(
        &database.pool,
        &[
            basenames_primary_name_reverse_changed_event(
                "basenames-reverse-a-60",
                address,
                BASE_PRIMARY_COIN_TYPE,
                260,
                0,
                CanonicalityState::Canonical,
            ),
            basenames_primary_name_reverse_linked_name_event(
                "basenames-record-a-60-success",
                address,
                BASE_PRIMARY_COIN_TYPE,
                Some("Alice.base.eth"),
                261,
                0,
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;
    worker_primary_name::rebuild_primary_names_current(
        &database.pool,
        Some(address),
        Some("basenames"),
        Some(BASE_PRIMARY_COIN_TYPE),
    )
    .await?;

    let declared_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=basenames&coin_type={BASE_PRIMARY_COIN_TYPE}&mode=declared"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("declared basenames primary-name status request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=basenames&coin_type={BASE_PRIMARY_COIN_TYPE}&mode=both"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed basenames primary-name status request failed")?;

    assert_eq!(declared_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let declared_payload: PrimaryNameResponse = read_json(declared_response).await?;
    let both_payload: PrimaryNameResponse = read_json(both_response).await?;

    assert_eq!(
        declared_payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "success",
                "name": "alice.base.eth",
                "provenance": {
                    "source_family": "basenames_base_primary",
                    "contract_role": "reverse_registrar",
                    "contract_instance_id": "00000000-0000-0000-0000-000000000104",
                    "emitting_address": "0x00000000000000000000000000000000000000ad",
                },
            }
        }))
    );
    assert_eq!(declared_payload.verified_state, None);
    assert_eq!(both_payload.declared_state, declared_payload.declared_state);
    assert_eq!(
        both_payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "invalid_name",
                "failure_reason": bigname_execution::VERIFIED_PRIMARY_NAME_CLAIM_NOT_NORMALIZED_REASON,
            }
        }))
    );
    assert_eq!(
        declared_payload.coverage,
        primary_name_supported_coverage("basenames")
    );
    assert_eq!(
        both_payload.coverage,
        primary_name_supported_coverage("basenames")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_gates_basenames_case_variant_claim_even_with_persisted_success()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000bca";
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000055);
    let finished_at = timestamp(1_717_172_420);
    let verified_primary_name = json!({
        "status": "success",
        "name": {
            "logical_name_id": "basenames:alice.base.eth",
            "namespace": "basenames",
            "normalized_name": "alice.base.eth",
            "canonical_display_name": "Alice.base.eth",
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000b45",
            "resource_id": "00000000-0000-0000-0000-000000000654",
            "binding_kind": "declared_registry_path"
        }
    });

    database
        .insert_primary_name_current_claim_row(
            address,
            "basenames",
            BASE_PRIMARY_COIN_TYPE,
            PrimaryNameClaimStatus::Success,
            None,
        )
        .await?;
    database
        .insert_primary_name_current_normalized_claim_name(
            address,
            "basenames",
            BASE_PRIMARY_COIN_TYPE,
            Some("alice.base.eth"),
            false,
        )
        .await?;

    upsert_execution_trace(
        &database.pool,
        &primary_name_execution_trace(
            execution_trace_id,
            "basenames",
            address,
            BASE_PRIMARY_COIN_TYPE,
            verified_primary_name.clone(),
            finished_at,
        ),
    )
    .await?;
    upsert_execution_outcome(
        &database.pool,
        &primary_name_execution_outcome(
            execution_trace_id,
            "basenames",
            address,
            BASE_PRIMARY_COIN_TYPE,
            verified_primary_name,
            finished_at,
        ),
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=basenames&coin_type={BASE_PRIMARY_COIN_TYPE}&mode=both"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed Basenames case-variant primary-name request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "success",
                "name": "alice.base.eth",
                "provenance": {},
            }
        }))
    );
    assert_eq!(
        payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "invalid_name",
                "failure_reason": bigname_execution::VERIFIED_PRIMARY_NAME_CLAIM_NOT_NORMALIZED_REASON,
            }
        }))
    );
    assert_eq!(
        payload.coverage,
        primary_name_supported_coverage("basenames")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_reads_declared_claim_provenance_for_exact_tuple() -> Result<()> {
    let database = TestDatabase::new(false).await?;
    database.create_primary_names_current_table().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    database
        .insert_primary_name_current_claim_row_with_provenance(
            address,
            "ens",
            "60",
            PrimaryNameClaimStatus::Success,
            None,
            json!({
                "source_family": "target_reverse",
                "contract_role": "reverse_registrar",
                "contract_instance_id": "00000000-0000-0000-0000-000000000123",
                "emitting_address": "0x00000000000000000000000000000000000000ad",
                "execution_trace_id": "must-be-omitted",
                "verified_primary_name_lookup": {
                    "address": address,
                    "namespace": "ens",
                    "coin_type": "60",
                },
                "verified_primary_name_invalidation": {
                    "claim_status": "success",
                    "primary_claim_source": {
                        "seed": "ignored",
                    },
                },
            }),
        )
        .await?;
    database
        .insert_primary_name_current_normalized_claim_name(
            address,
            "ens",
            "60",
            Some("alice.eth"),
            true,
        )
        .await?;
    database
        .insert_primary_name_current_claim_row_with_provenance(
            address,
            "ens",
            "61",
            PrimaryNameClaimStatus::Success,
            None,
            json!({
                "source_family": "sibling_reverse",
            }),
        )
        .await?;
    database
        .insert_primary_name_current_normalized_claim_name(
            address,
            "ens",
            "61",
            Some("beta.eth"),
            true,
        )
        .await?;

    let declared_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=declared"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("declared primary-name provenance request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=both"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed primary-name provenance request failed")?;

    assert_eq!(declared_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let declared_payload: PrimaryNameResponse = read_json(declared_response).await?;
    let both_payload: PrimaryNameResponse = read_json(both_response).await?;
    let expected_claimed_primary_name = json!({
        "status": "success",
        "name": "alice.eth",
        "provenance": {
            "source_family": "target_reverse",
            "contract_role": "reverse_registrar",
            "contract_instance_id": "00000000-0000-0000-0000-000000000123",
            "emitting_address": "0x00000000000000000000000000000000000000ad",
        },
    });

    assert_eq!(
        declared_payload.declared_state,
        Some(json!({
            "claimed_primary_name": expected_claimed_primary_name.clone(),
        }))
    );
    assert_eq!(
        declared_payload
            .declared_state
            .as_ref()
            .and_then(|declared_state| declared_state.get("claimed_primary_name"))
            .and_then(Value::as_object)
            .and_then(|claimed_primary_name| claimed_primary_name.get("name")),
        Some(&json!("alice.eth"))
    );
    assert_eq!(declared_payload.verified_state, None);
    assert_eq!(both_payload.declared_state, declared_payload.declared_state);
    assert_eq!(
        both_payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "not_found",
            }
        }))
    );
    assert_eq!(declared_payload.coverage, primary_name_supported_coverage("ens"));
    assert_eq!(both_payload.coverage, primary_name_supported_coverage("ens"));

    let claimed_primary_name = declared_payload
        .declared_state
        .as_ref()
        .and_then(|declared_state| declared_state.get("claimed_primary_name"))
        .and_then(Value::as_object)
        .expect("declared claimed_primary_name must be present");
    let provenance = claimed_primary_name
        .get("provenance")
        .and_then(Value::as_object)
        .expect("declared claimed_primary_name provenance must be present");
    assert!(!provenance.contains_key("execution_trace_id"));
    assert!(!provenance.contains_key("verified_primary_name_lookup"));
    assert!(!provenance.contains_key("verified_primary_name_invalidation"));
    assert_eq!(
        provenance.get("source_family"),
        Some(&json!("target_reverse"))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_omit_declared_claim_provenance_from_top_level_route_summary()
-> Result<()> {
    let database = TestDatabase::new(false).await?;
    database.create_primary_names_current_table().await?;
    let address = "0x0000000000000000000000000000000000000abc";

    database
        .insert_primary_name_current_claim_row_with_provenance(
            address,
            "ens",
            "60",
            PrimaryNameClaimStatus::Success,
            None,
            json!({
                "normalized_event_ids": [101, 102],
                "raw_fact_refs": [{
                    "kind": "raw_log",
                    "block_number": 101,
                }],
                "manifest_versions": [{
                    "manifest_version": 7,
                    "source_family": "ens_v1_reverse_l1",
                    "source_manifest_id": null,
                }],
                "derivation_kind": "primary_name_projection_rebuild",
                "verified_primary_name_lookup": {
                    "address": address,
                    "namespace": "ens",
                    "coin_type": "60",
                },
            }),
        )
        .await?;
    database
        .insert_primary_name_current_normalized_claim_name(
            address,
            "ens",
            "60",
            Some("alice.eth"),
            true,
        )
        .await?;

    let declared_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=declared"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("declared primary-name route provenance request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=both"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed primary-name route provenance request failed")?;

    assert_eq!(declared_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let declared_payload: PrimaryNameResponse = read_json(declared_response).await?;
    let both_payload: PrimaryNameResponse = read_json(both_response).await?;
    assert!(declared_payload.provenance.is_null());
    assert!(both_payload.provenance.is_null());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_reads_raw_claim_name_for_invalid_name_exact_tuple() -> Result<()> {
    let database = TestDatabase::new(false).await?;
    database.create_primary_names_current_table().await?;
    database
        .insert_primary_name_current_claim_row(
            "0x0000000000000000000000000000000000000abc",
            "ens",
            "60",
            PrimaryNameClaimStatus::InvalidName,
            Some("alice..eth"),
        )
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed invalid-name primary-name request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: PrimaryNameResponse = read_json(response).await?;
    let claimed_primary_name = payload
        .declared_state
        .as_ref()
        .and_then(|declared_state| declared_state.get("claimed_primary_name"))
        .and_then(Value::as_object)
        .expect("declared claimed_primary_name must be present");

    assert_eq!(
        claimed_primary_name.get("status"),
        Some(&json!("invalid_name"))
    );
    assert_eq!(
        claimed_primary_name.get("raw_claim_name"),
        Some(&json!("alice..eth"))
    );
    assert_eq!(claimed_primary_name.get("provenance"), Some(&json!({})));
    assert!(
        !claimed_primary_name.contains_key("name"),
        "declared invalid-name readback must not backfill claimed_primary_name.name"
    );
    assert_eq!(
        payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "not_found",
            }
        }))
    );
    assert_eq!(payload.coverage, primary_name_supported_coverage("ens"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_rejects_invalid_claim_name_for_exact_tuple() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    upsert_normalized_events(
        &database.pool,
        &[
            primary_name_reverse_changed_event(
                "reverse-a-60",
                address,
                "60",
                350,
                0,
                CanonicalityState::Canonical,
            ),
            primary_name_reverse_linked_name_event(
                "record-a-60-invalid-name",
                address,
                "60",
                Some("Ni\u{200d}ck.eth"),
                351,
                0,
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;
    worker_primary_name::rebuild_primary_names_current(
        &database.pool,
        Some(address),
        Some("ens"),
        Some("60"),
    )
    .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=both"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("invalid primary-name request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: PrimaryNameResponse = read_json(response).await?;
    let claimed_primary_name = payload
        .declared_state
        .as_ref()
        .and_then(|declared_state| declared_state.get("claimed_primary_name"))
        .and_then(Value::as_object)
        .expect("declared claimed_primary_name must be present");

    assert_eq!(
        claimed_primary_name.get("status"),
        Some(&json!("invalid_name"))
    );
    assert_eq!(
        claimed_primary_name.get("raw_claim_name"),
        Some(&json!("Ni\u{200d}ck.eth"))
    );
    assert!(
        !claimed_primary_name.contains_key("name"),
        "invalid raw claims must not publish claimed_primary_name.name in bootstrap mode"
    );
    let provenance = claimed_primary_name
        .get("provenance")
        .and_then(Value::as_object)
        .expect("declared invalid-name provenance must be present");
    assert_eq!(
        provenance.get("source_family"),
        Some(&json!("ens_v1_reverse_l1"))
    );
    assert_eq!(
        provenance.get("contract_role"),
        Some(&json!("reverse_registrar"))
    );
    assert_eq!(
        provenance.get("emitting_address"),
        Some(&json!("0x00000000000000000000000000000000000000ad"))
    );
    assert!(!provenance.contains_key("execution_trace_id"));
    assert!(!provenance.contains_key("verified_primary_name_lookup"));
    assert!(!provenance.contains_key("verified_primary_name_invalidation"));
    assert_eq!(
        payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "not_found",
            }
        }))
    );
    assert_eq!(payload.coverage, primary_name_supported_coverage("ens"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_reads_persisted_verified_primary_name_for_exact_tuple() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000041);
    let finished_at = timestamp(1_717_172_401);
    let verified_primary_name = json!({
        "status": "success",
        "name": {
            "logical_name_id": "ens:alice.eth",
            "namespace": "ens",
            "normalized_name": "alice.eth",
            "canonical_display_name": "Alice.eth",
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
            "resource_id": "00000000-0000-0000-0000-000000000456",
            "binding_kind": "declared_registry_path"
        }
    });

    database
        .insert_primary_name_current_row(address, "ens", "60")
        .await?;
    database
        .insert_primary_name_current_row(address, "ens", "61")
        .await?;

    let trace = primary_name_execution_trace(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name.clone(),
        finished_at,
    );
    let outcome = primary_name_execution_outcome(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name.clone(),
        finished_at,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let other_trace = primary_name_execution_trace(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000042),
        "ens",
        address,
        "61",
        json!({
            "status": "mismatch",
            "name": {
                "logical_name_id": "ens:other.eth",
                "namespace": "ens",
                "normalized_name": "other.eth",
                "canonical_display_name": "other.eth",
                "namehash": "0x0000000000000000000000000000000000000000000000000000000000000456",
                "resource_id": "00000000-0000-0000-0000-000000000999",
                "binding_kind": "declared_registry_path"
            },
            "failure_reason": "resolved_address_mismatch"
        }),
        timestamp(1_717_172_499),
    );
    let other_outcome = primary_name_execution_outcome(
        other_trace.execution_trace_id,
        "ens",
        address,
        "61",
        json!({
            "status": "mismatch",
            "name": {
                "logical_name_id": "ens:other.eth",
                "namespace": "ens",
                "normalized_name": "other.eth",
                "canonical_display_name": "other.eth",
                "namehash": "0x0000000000000000000000000000000000000000000000000000000000000456",
                "resource_id": "00000000-0000-0000-0000-000000000999",
                "binding_kind": "declared_registry_path"
            },
            "failure_reason": "resolved_address_mismatch"
        }),
        timestamp(1_717_172_499),
    );
    upsert_execution_trace(&database.pool, &other_trace).await?;
    upsert_execution_outcome(&database.pool, &other_outcome).await?;

    let verified_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("verified primary-name persisted readback request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed primary-name persisted readback request failed")?;

    assert_eq!(verified_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let verified_payload: PrimaryNameResponse = read_json(verified_response).await?;
    let both_payload: PrimaryNameResponse = read_json(both_response).await?;
    let verified_section_provenance = json!({
        "manifest_versions": primary_name_execution_manifest_versions(),
        "execution_trace_id": execution_trace_id.to_string(),
    });

    assert_eq!(verified_payload.declared_state, None);
    assert_eq!(
        verified_payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "success",
                "name": {
                    "logical_name_id": "ens:alice.eth",
                    "namespace": "ens",
                    "normalized_name": "alice.eth",
                    "canonical_display_name": "Alice.eth",
                    "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
                    "resource_id": "00000000-0000-0000-0000-000000000456",
                    "binding_kind": "declared_registry_path"
                },
                "provenance": verified_section_provenance.clone(),
            }
        }))
    );
    assert_eq!(
        both_payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "unsupported",
                "provenance": {},
            }
        }))
    );
    assert_eq!(both_payload.verified_state, verified_payload.verified_state);
    assert!(verified_payload.provenance.is_null());
    assert!(both_payload.provenance.is_null());
    let verified_primary_name = verified_payload
        .verified_state
        .as_ref()
        .and_then(|verified_state| verified_state.get("verified_primary_name"))
        .and_then(Value::as_object)
        .expect("verified_primary_name must be present");
    assert_eq!(
        verified_primary_name.get("provenance"),
        Some(&verified_section_provenance)
    );
    assert_eq!(
        verified_primary_name
            .get("provenance")
            .and_then(|provenance| provenance.get("execution_trace_id")),
        Some(&json!(execution_trace_id.to_string())),
    );
    assert_eq!(
        verified_primary_name
            .get("provenance")
            .and_then(|provenance| provenance.get("manifest_versions")),
        Some(&primary_name_execution_manifest_versions()),
    );
    assert_eq!(verified_payload.coverage, primary_name_supported_coverage("ens"));
    assert_eq!(both_payload.coverage, verified_payload.coverage);
    assert_eq!(verified_payload.last_updated, "2024-05-31T16:20:01Z");
    assert_eq!(both_payload.last_updated, "2024-05-31T16:20:01Z");

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_reads_execution_persisted_verified_primary_name_for_exact_tuple()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let expected_data = json!({
        "address": address,
        "namespace": "ens",
        "coin_type": "60",
    });
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000051);
    let finished_at = timestamp(1_717_172_417);
    let verified_primary_name = json!({
        "status": "success",
        "name": {
            "logical_name_id": "ens:alice.eth",
            "namespace": "ens",
            "normalized_name": "alice.eth",
            "canonical_display_name": "Alice.eth",
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
            "resource_id": "00000000-0000-0000-0000-000000000456",
            "binding_kind": "declared_registry_path"
        }
    });

    database
        .insert_primary_name_current_claim_row(
            address,
            "ens",
            "60",
            PrimaryNameClaimStatus::Success,
            None,
        )
        .await?;
    database
        .insert_primary_name_current_normalized_claim_name(
            address,
            "ens",
            "60",
            Some("alice.eth"),
            true,
        )
        .await?;

    let trace = primary_name_execution_trace(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name.clone(),
        finished_at,
    );
    let outcome = primary_name_execution_outcome(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name.clone(),
        finished_at,
    );
    bigname_execution::persist_ens_verified_primary_name(
        &database.pool,
        &bigname_execution::PersistEnsVerifiedPrimaryNameRequest {
            trace,
            outcome: outcome.clone(),
        },
    )
    .await?;

    let verified_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=verified"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("execution-persisted verified primary-name request failed")?;

    assert_eq!(verified_response.status(), StatusCode::OK);

    let verified_payload: PrimaryNameResponse = read_json(verified_response).await?;
    let verified_section_provenance = json!({
        "manifest_versions": primary_name_execution_manifest_versions(),
        "execution_trace_id": execution_trace_id.to_string(),
    });
    assert_eq!(verified_payload.data, expected_data);
    assert_eq!(verified_payload.coverage, primary_name_supported_coverage("ens"));
    assert_eq!(
        verified_payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "success",
                "name": {
                    "logical_name_id": "ens:alice.eth",
                    "namespace": "ens",
                    "normalized_name": "alice.eth",
                    "canonical_display_name": "Alice.eth",
                    "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
                    "resource_id": "00000000-0000-0000-0000-000000000456",
                    "binding_kind": "declared_registry_path"
                },
                "provenance": verified_section_provenance,
            }
        }))
    );
    assert_eq!(verified_payload.last_updated, "2024-05-31T16:20:17Z");

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_reads_persisted_basenames_verified_primary_name_for_exact_tuple()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000bcd";
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000004a);
    let finished_at = timestamp(1_717_172_410);
    let verified_primary_name = json!({
        "status": "success",
        "name": {
            "logical_name_id": "basenames:alice.base.eth",
            "namespace": "basenames",
            "normalized_name": "alice.base.eth",
            "canonical_display_name": "Alice.base.eth",
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000b45",
            "resource_id": "00000000-0000-0000-0000-000000000654",
            "binding_kind": "declared_registry_path"
        }
    });

    upsert_normalized_events(
        &database.pool,
        &[
            basenames_primary_name_reverse_changed_event(
                "basenames-reverse-b-60",
                address,
                BASE_PRIMARY_COIN_TYPE,
                360,
                0,
                CanonicalityState::Canonical,
            ),
            basenames_primary_name_reverse_linked_name_event(
                "basenames-record-b-60-success",
                address,
                BASE_PRIMARY_COIN_TYPE,
                Some("alice.base.eth"),
                361,
                0,
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;
    worker_primary_name::rebuild_primary_names_current(
        &database.pool,
        Some(address),
        Some("basenames"),
        Some(BASE_PRIMARY_COIN_TYPE),
    )
    .await?;

    let trace = primary_name_execution_trace(
        execution_trace_id,
        "basenames",
        address,
        BASE_PRIMARY_COIN_TYPE,
        verified_primary_name.clone(),
        finished_at,
    );
    let outcome = primary_name_execution_outcome(
        execution_trace_id,
        "basenames",
        address,
        BASE_PRIMARY_COIN_TYPE,
        verified_primary_name.clone(),
        finished_at,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let verified_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=basenames&coin_type={BASE_PRIMARY_COIN_TYPE}&mode=verified"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("verified basenames primary-name persisted readback request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=basenames&coin_type={BASE_PRIMARY_COIN_TYPE}&mode=both"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed basenames primary-name persisted readback request failed")?;

    assert_eq!(verified_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let verified_payload: PrimaryNameResponse = read_json(verified_response).await?;
    let both_payload: PrimaryNameResponse = read_json(both_response).await?;
    let verified_section_provenance = json!({
        "manifest_versions": primary_name_execution_manifest_versions_for_namespace("basenames"),
        "execution_trace_id": execution_trace_id.to_string(),
    });

    assert_eq!(verified_payload.declared_state, None);
    assert_eq!(
        verified_payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "success",
                "name": {
                    "logical_name_id": "basenames:alice.base.eth",
                    "namespace": "basenames",
                    "normalized_name": "alice.base.eth",
                    "canonical_display_name": "Alice.base.eth",
                    "namehash": "0x0000000000000000000000000000000000000000000000000000000000000b45",
                    "resource_id": "00000000-0000-0000-0000-000000000654",
                    "binding_kind": "declared_registry_path"
                },
                "provenance": verified_section_provenance.clone(),
            }
        }))
    );
    assert_eq!(
        both_payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "success",
                "name": "alice.base.eth",
                "provenance": {
                    "source_family": "basenames_base_primary",
                    "contract_role": "reverse_registrar",
                    "contract_instance_id": "00000000-0000-0000-0000-000000000168",
                    "emitting_address": "0x00000000000000000000000000000000000000ad",
                },
            }
        }))
    );
    assert_eq!(both_payload.verified_state, verified_payload.verified_state);
    assert!(verified_payload.provenance.is_null());
    assert!(both_payload.provenance.is_null());
    assert_eq!(
        verified_payload.coverage,
        primary_name_supported_coverage("basenames")
    );
    assert_eq!(both_payload.coverage, verified_payload.coverage);
    assert_eq!(verified_payload.last_updated, "2024-05-31T16:20:10Z");
    assert_eq!(both_payload.last_updated, "2024-05-31T16:20:10Z");

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_reads_persisted_basenames_verified_primary_name_not_found_without_l1_resolver_call()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000bce";
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000004b);
    let finished_at = timestamp(1_717_172_411);
    let verified_primary_name = json!({
        "status": "not_found"
    });

    database
        .insert_primary_name_current_claim_row(
            address,
            "basenames",
            BASE_PRIMARY_COIN_TYPE,
            PrimaryNameClaimStatus::NotFound,
            None,
        )
        .await?;

    let trace = primary_name_execution_trace(
        execution_trace_id,
        "basenames",
        address,
        BASE_PRIMARY_COIN_TYPE,
        verified_primary_name.clone(),
        finished_at,
    );
    let outcome = primary_name_execution_outcome(
        execution_trace_id,
        "basenames",
        address,
        BASE_PRIMARY_COIN_TYPE,
        verified_primary_name,
        finished_at,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let verified_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=basenames&coin_type={BASE_PRIMARY_COIN_TYPE}&mode=verified"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("verified basenames not_found primary-name request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=basenames&coin_type={BASE_PRIMARY_COIN_TYPE}&mode=both"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed basenames not_found primary-name request failed")?;

    assert_eq!(verified_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let verified_payload: PrimaryNameResponse = read_json(verified_response).await?;
    let both_payload: PrimaryNameResponse = read_json(both_response).await?;
    let verified_section_provenance = json!({
        "manifest_versions": primary_name_execution_manifest_versions_for_namespace("basenames"),
        "execution_trace_id": execution_trace_id.to_string(),
    });

    assert_eq!(
        verified_payload.data,
        json!({
            "address": address,
            "namespace": "basenames",
            "coin_type": BASE_PRIMARY_COIN_TYPE,
        })
    );
    assert_eq!(both_payload.data, verified_payload.data);
    assert_eq!(verified_payload.declared_state, None);
    assert_eq!(
        verified_payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "not_found",
                "provenance": verified_section_provenance.clone(),
            }
        }))
    );
    assert_eq!(
        both_payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "not_found",
                "provenance": {},
            }
        }))
    );
    assert_eq!(both_payload.verified_state, verified_payload.verified_state);
    assert!(verified_payload.provenance.is_null());
    assert!(both_payload.provenance.is_null());
    assert_eq!(
        verified_payload.coverage,
        primary_name_supported_coverage("basenames")
    );
    assert_eq!(both_payload.coverage, verified_payload.coverage);
    assert_eq!(verified_payload.last_updated, "2024-05-31T16:20:11Z");
    assert_eq!(both_payload.last_updated, "2024-05-31T16:20:11Z");

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_reads_persisted_basenames_verified_primary_name_invalid_name_without_l1_resolver_call()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000bcf";
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000004c);
    let finished_at = timestamp(1_717_172_412);
    let verified_primary_name = json!({
        "status": "invalid_name",
        "failure_reason": "claim_name_not_normalizable"
    });

    database
        .insert_primary_name_current_claim_row(
            address,
            "basenames",
            BASE_PRIMARY_COIN_TYPE,
            PrimaryNameClaimStatus::InvalidName,
            Some("alice..base.eth"),
        )
        .await?;

    let trace = primary_name_execution_trace(
        execution_trace_id,
        "basenames",
        address,
        BASE_PRIMARY_COIN_TYPE,
        verified_primary_name.clone(),
        finished_at,
    );
    let outcome = primary_name_execution_outcome(
        execution_trace_id,
        "basenames",
        address,
        BASE_PRIMARY_COIN_TYPE,
        verified_primary_name,
        finished_at,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let verified_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=basenames&coin_type={BASE_PRIMARY_COIN_TYPE}&mode=verified"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("verified basenames invalid_name primary-name request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=basenames&coin_type={BASE_PRIMARY_COIN_TYPE}&mode=both"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed basenames invalid_name primary-name request failed")?;

    assert_eq!(verified_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let verified_payload: PrimaryNameResponse = read_json(verified_response).await?;
    let both_payload: PrimaryNameResponse = read_json(both_response).await?;
    let verified_section_provenance = json!({
        "manifest_versions": primary_name_execution_manifest_versions_for_namespace("basenames"),
        "execution_trace_id": execution_trace_id.to_string(),
    });

    assert_eq!(
        verified_payload.data,
        json!({
            "address": address,
            "namespace": "basenames",
            "coin_type": BASE_PRIMARY_COIN_TYPE,
        })
    );
    assert_eq!(both_payload.data, verified_payload.data);
    assert_eq!(verified_payload.declared_state, None);
    assert_eq!(
        verified_payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "invalid_name",
                "failure_reason": "claim_name_not_normalizable",
                "provenance": verified_section_provenance.clone(),
            }
        }))
    );
    assert_eq!(
        both_payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "invalid_name",
                "raw_claim_name": "alice..base.eth",
                "provenance": {},
            }
        }))
    );
    assert_eq!(both_payload.verified_state, verified_payload.verified_state);
    let claimed_primary_name = both_payload
        .declared_state
        .as_ref()
        .and_then(|declared_state| declared_state.get("claimed_primary_name"))
        .and_then(Value::as_object)
        .expect("claimed_primary_name must be present");
    assert!(
        !claimed_primary_name.contains_key("name"),
        "invalid_name readback must not backfill claimed_primary_name.name"
    );
    assert!(verified_payload.provenance.is_null());
    assert!(both_payload.provenance.is_null());
    assert_eq!(
        verified_payload.coverage,
        primary_name_supported_coverage("basenames")
    );
    assert_eq!(both_payload.coverage, verified_payload.coverage);
    assert_eq!(verified_payload.last_updated, "2024-05-31T16:20:12Z");
    assert_eq!(both_payload.last_updated, "2024-05-31T16:20:12Z");

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_reads_persisted_verified_primary_name_mismatch_for_exact_tuple()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000043);
    let finished_at = timestamp(1_717_172_403);
    let verified_primary_name = json!({
        "status": "mismatch",
        "name": {
            "logical_name_id": "ens:alice.eth",
            "namespace": "ens",
            "normalized_name": "alice.eth",
            "canonical_display_name": "Alice.eth",
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
            "resource_id": "00000000-0000-0000-0000-000000000456",
            "binding_kind": "declared_registry_path"
        },
        "failure_reason": "resolved_target_mismatch"
    });

    database
        .insert_primary_name_current_row(address, "ens", "60")
        .await?;

    let trace = primary_name_execution_trace(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name.clone(),
        finished_at,
    );
    let outcome = primary_name_execution_outcome(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name,
        finished_at,
    );
    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let verified_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("verified primary-name persisted mismatch request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("mixed primary-name persisted mismatch request failed")?;

    assert_eq!(verified_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let verified_payload: PrimaryNameResponse = read_json(verified_response).await?;
    let both_payload: PrimaryNameResponse = read_json(both_response).await?;
    let verified_section_provenance = json!({
        "manifest_versions": primary_name_execution_manifest_versions(),
        "execution_trace_id": execution_trace_id.to_string(),
    });

    assert_eq!(verified_payload.declared_state, None);
    assert_eq!(
        verified_payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "mismatch",
                "name": {
                    "logical_name_id": "ens:alice.eth",
                    "namespace": "ens",
                    "normalized_name": "alice.eth",
                    "canonical_display_name": "Alice.eth",
                    "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
                    "resource_id": "00000000-0000-0000-0000-000000000456",
                    "binding_kind": "declared_registry_path"
                },
                "failure_reason": "resolved_target_mismatch",
                "provenance": verified_section_provenance.clone(),
            }
        }))
    );
    assert_eq!(
        both_payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "unsupported",
                "provenance": {},
            }
        }))
    );
    assert_eq!(both_payload.verified_state, verified_payload.verified_state);
    let verified_primary_name = verified_payload
        .verified_state
        .as_ref()
        .and_then(|verified_state| verified_state.get("verified_primary_name"))
        .and_then(Value::as_object)
        .expect("verified_primary_name must be present");
    assert_eq!(
        verified_primary_name.get("provenance"),
        Some(&verified_section_provenance)
    );
    assert_eq!(
        verified_primary_name
            .get("provenance")
            .and_then(|provenance| provenance.get("execution_trace_id")),
        Some(&json!(execution_trace_id.to_string())),
    );
    assert_eq!(
        verified_primary_name
            .get("provenance")
            .and_then(|provenance| provenance.get("manifest_versions")),
        Some(&primary_name_execution_manifest_versions()),
    );
    assert_eq!(verified_payload.coverage, primary_name_supported_coverage("ens"));
    assert_eq!(both_payload.coverage, verified_payload.coverage);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_rejects_malformed_persisted_verified_primary_name_section() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000044);
    let finished_at = timestamp(1_717_172_404);
    let verified_primary_name = json!({
        "status": "success",
        "name": {
            "logical_name_id": "ens:alice.eth",
            "namespace": "ens",
            "normalized_name": "alice.eth",
            "canonical_display_name": "Alice.eth",
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
            "resource_id": "00000000-0000-0000-0000-000000000456",
            "binding_kind": "declared_registry_path"
        }
    });

    database
        .insert_primary_name_current_row(address, "ens", "60")
        .await?;

    let trace = primary_name_execution_trace(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name.clone(),
        finished_at,
    );
    let mut outcome = primary_name_execution_outcome(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name,
        finished_at,
    );
    outcome
        .outcome_payload
        .as_mut()
        .and_then(Value::as_object_mut)
        .and_then(|payload| payload.get_mut("verified_primary_name"))
        .and_then(Value::as_object_mut)
        .expect("verified_primary_name section must be present")
        .insert("legacy_field".to_owned(), json!("unexpected"));

    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("malformed persisted verified primary-name request failed")?;

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "internal_error");
    assert_eq!(
        payload.error.message,
        format!("persisted verified primary-name payload mismatch for address {address}")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_treats_persisted_verified_primary_name_trace_manifest_drift_as_miss()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000045);
    let finished_at = timestamp(1_717_172_405);
    let verified_primary_name = json!({
        "status": "success",
        "name": {
            "logical_name_id": "ens:alice.eth",
            "namespace": "ens",
            "normalized_name": "alice.eth",
            "canonical_display_name": "Alice.eth",
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
            "resource_id": "00000000-0000-0000-0000-000000000456",
            "binding_kind": "declared_registry_path"
        }
    });

    database
        .insert_primary_name_current_row(address, "ens", "60")
        .await?;

    let mut trace = primary_name_execution_trace(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name.clone(),
        finished_at,
    );
    trace.manifest_context = json!({
        "manifest_versions": [{
            "manifest_version": 99,
            "source_family": "ens_v1_registry"
        }],
    });
    let outcome = primary_name_execution_outcome(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name,
        finished_at,
    );

    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("manifest-drift verified primary-name request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "not_found",
            }
        }))
    );
    assert_eq!(payload.coverage, primary_name_supported_coverage("ens"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_treats_persisted_verified_primary_name_trace_tuple_drift_as_miss()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000050);
    let finished_at = timestamp(1_717_172_416);
    let verified_primary_name = json!({
        "status": "success",
        "name": {
            "logical_name_id": "ens:alice.eth",
            "namespace": "ens",
            "normalized_name": "alice.eth",
            "canonical_display_name": "Alice.eth",
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
            "resource_id": "00000000-0000-0000-0000-000000000456",
            "binding_kind": "declared_registry_path"
        }
    });

    database
        .insert_primary_name_current_row(address, "ens", "60")
        .await?;

    let mut trace = primary_name_execution_trace(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name.clone(),
        finished_at,
    );
    trace.request_key = "ens:0x0000000000000000000000000000000000000def:60".to_owned();
    let outcome = primary_name_execution_outcome(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name,
        finished_at,
    );

    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("tuple-drift verified primary-name request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "not_found",
            }
        }))
    );
    assert_eq!(payload.coverage, primary_name_supported_coverage("ens"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_rejects_persisted_basenames_verified_primary_name_without_basenames_execution_source_family()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000bc0";
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000004d);
    let finished_at = timestamp(1_717_172_413);
    let verified_primary_name = json!({
        "status": "success",
        "name": {
            "logical_name_id": "basenames:alice.base.eth",
            "namespace": "basenames",
            "normalized_name": "alice.base.eth",
            "canonical_display_name": "Alice.base.eth",
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000b45",
            "resource_id": "00000000-0000-0000-0000-000000000654",
            "binding_kind": "declared_registry_path"
        }
    });

    database
        .insert_primary_name_current_row(address, "basenames", BASE_PRIMARY_COIN_TYPE)
        .await?;

    let mut trace = primary_name_execution_trace(
        execution_trace_id,
        "basenames",
        address,
        BASE_PRIMARY_COIN_TYPE,
        verified_primary_name.clone(),
        finished_at,
    );
    trace.manifest_context = json!({
        "manifest_versions": [{
            "manifest_version": 99,
            "source_family": "basenames_base_primary"
        }],
    });
    trace.request_metadata["cache_identity"]["manifest_versions"] = json!([{
        "manifest_version": 99,
        "source_family": "basenames_base_primary"
    }]);
    let mut outcome = primary_name_execution_outcome(
        execution_trace_id,
        "basenames",
        address,
        BASE_PRIMARY_COIN_TYPE,
        verified_primary_name,
        finished_at,
    );
    outcome.cache_key.manifest_versions = json!([{
        "manifest_version": 99,
        "source_family": "basenames_base_primary"
    }]);

    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=basenames&coin_type={BASE_PRIMARY_COIN_TYPE}&mode=verified"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("Basenames wrong-source-family verified primary-name request failed")?;

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "internal_error");
    assert_eq!(
        payload.error.message,
        format!("persisted verified primary-name provenance mismatch for address {address}")
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_treats_persisted_verified_primary_name_cache_boundary_drift_as_miss()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000004e);
    let finished_at = timestamp(1_717_172_414);
    let verified_primary_name = json!({
        "status": "success",
        "name": {
            "logical_name_id": "ens:alice.eth",
            "namespace": "ens",
            "normalized_name": "alice.eth",
            "canonical_display_name": "Alice.eth",
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
            "resource_id": "00000000-0000-0000-0000-000000000456",
            "binding_kind": "declared_registry_path"
        }
    });

    database
        .insert_primary_name_current_row(address, "ens", "60")
        .await?;

    let trace = primary_name_execution_trace(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name.clone(),
        finished_at,
    );
    let mut outcome = primary_name_execution_outcome(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name,
        finished_at,
    );
    outcome.cache_key.requested_chain_positions = json!([{
        "chain_id": "ethereum-mainnet",
        "block_number": 21_000_099,
        "block_hash": "0xstaleprimary"
    }]);
    outcome.cache_key.topology_version_boundary = record_inventory_boundary(
        "ens:stale.eth",
        Uuid::from_u128(0x0e7ec7ace0000000000000000000c001),
    );
    outcome.cache_key.record_version_boundary = record_inventory_boundary(
        "ens:stale.eth",
        Uuid::from_u128(0x0e7ec7ace0000000000000000000c002),
    );

    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("cache-boundary-drift verified primary-name request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "not_found",
            }
        }))
    );
    assert_eq!(payload.coverage, primary_name_supported_coverage("ens"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_scans_past_newer_drifted_verified_primary_name_outcome()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let verified_primary_name = json!({
        "status": "success",
        "name": {
            "logical_name_id": "ens:alice.eth",
            "namespace": "ens",
            "normalized_name": "alice.eth",
            "canonical_display_name": "Alice.eth",
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
            "resource_id": "00000000-0000-0000-0000-000000000456",
            "binding_kind": "declared_registry_path"
        }
    });

    database
        .insert_primary_name_current_row(address, "ens", "60")
        .await?;

    let older_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000052);
    let older_finished_at = timestamp(1_717_172_418);
    let older_trace = primary_name_execution_trace(
        older_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name.clone(),
        older_finished_at,
    );
    let older_outcome = primary_name_execution_outcome(
        older_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name.clone(),
        older_finished_at,
    );
    upsert_execution_trace(&database.pool, &older_trace).await?;
    upsert_execution_outcome(&database.pool, &older_outcome).await?;

    let newer_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000053);
    let newer_finished_at = timestamp(1_717_172_519);
    let newer_trace = primary_name_execution_trace(
        newer_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name.clone(),
        newer_finished_at,
    );
    let mut newer_outcome = primary_name_execution_outcome(
        newer_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name,
        newer_finished_at,
    );
    newer_outcome.cache_key.record_version_boundary = record_inventory_boundary(
        "ens:stale.eth",
        Uuid::from_u128(0x0e7ec7ace0000000000000000000c003),
    );
    upsert_execution_trace(&database.pool, &newer_trace).await?;
    upsert_execution_outcome(&database.pool, &newer_outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/primary-names/{address}?namespace=ens&coin_type=60&mode=verified"
                ))
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("masked persisted verified primary-name request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "success",
                "name": {
                    "logical_name_id": "ens:alice.eth",
                    "namespace": "ens",
                    "normalized_name": "alice.eth",
                    "canonical_display_name": "Alice.eth",
                    "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
                    "resource_id": "00000000-0000-0000-0000-000000000456",
                    "binding_kind": "declared_registry_path"
                },
                "provenance": {
                    "manifest_versions": primary_name_execution_manifest_versions(),
                    "execution_trace_id": older_trace_id.to_string(),
                },
            }
        }))
    );
    assert_eq!(payload.last_updated, "2024-05-31T16:20:18Z");

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_treats_persisted_verified_primary_name_manifest_drift_as_miss()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let address = "0x0000000000000000000000000000000000000abc";
    let execution_trace_id = Uuid::from_u128(0x0e7ec7ace0000000000000000000004f);
    let finished_at = timestamp(1_717_172_415);
    let verified_primary_name = json!({
        "status": "success",
        "name": {
            "logical_name_id": "ens:alice.eth",
            "namespace": "ens",
            "normalized_name": "alice.eth",
            "canonical_display_name": "Alice.eth",
            "namehash": "0x0000000000000000000000000000000000000000000000000000000000000123",
            "resource_id": "00000000-0000-0000-0000-000000000456",
            "binding_kind": "declared_registry_path"
        }
    });

    database
        .insert_primary_name_current_row(address, "ens", "60")
        .await?;

    let mut trace = primary_name_execution_trace(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name.clone(),
        finished_at,
    );
    trace.request_metadata["cache_identity"]["manifest_versions"] = json!([{
        "manifest_version": 4,
        "source_family": "ens_execution"
    }]);
    let outcome = primary_name_execution_outcome(
        execution_trace_id,
        "ens",
        address,
        "60",
        verified_primary_name,
        finished_at,
    );

    upsert_execution_trace(&database.pool, &trace).await?;
    upsert_execution_outcome(&database.pool, &outcome).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("manifest-drift-as-miss verified primary-name request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "not_found",
            }
        }))
    );
    assert_eq!(payload.coverage, primary_name_supported_coverage("ens"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_omits_verified_section_provenance_for_unsupported_boundaries()
-> Result<()> {
    let database = TestDatabase::new(false).await?;
    database.create_primary_names_current_table().await?;
    database
        .insert_primary_name_current_row("0x0000000000000000000000000000000000000abc", "ens", "60")
        .await?;

    let verified_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("unsupported verified primary-name request failed")?;
    let both_response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("unsupported mixed primary-name request failed")?;

    assert_eq!(verified_response.status(), StatusCode::OK);
    assert_eq!(both_response.status(), StatusCode::OK);

    let verified_payload: PrimaryNameResponse = read_json(verified_response).await?;
    let both_payload: PrimaryNameResponse = read_json(both_response).await?;
    let verified_primary_name = verified_payload
        .verified_state
        .as_ref()
        .and_then(|verified_state| verified_state.get("verified_primary_name"))
        .and_then(Value::as_object)
        .expect("verified_primary_name must be present");
    let both_verified_primary_name = both_payload
        .verified_state
        .as_ref()
        .and_then(|verified_state| verified_state.get("verified_primary_name"))
        .and_then(Value::as_object)
        .expect("verified_primary_name must be present");

    assert_eq!(verified_primary_name.get("status"), Some(&json!("not_found")));
    assert!(!verified_primary_name.contains_key("unsupported_reason"));
    assert!(!verified_primary_name.contains_key("provenance"));
    assert_eq!(both_verified_primary_name, verified_primary_name);
    assert!(verified_payload.provenance.is_null());
    assert!(both_payload.provenance.is_null());
    assert_eq!(verified_payload.coverage, primary_name_supported_coverage("ens"));
    assert_eq!(both_payload.coverage, primary_name_supported_coverage("ens"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_freezes_bootstrap_behavior_for_tuple_present() -> Result<()> {
    let database = TestDatabase::new(false).await?;
    database.create_primary_names_current_table().await?;
    database
        .insert_primary_name_current_row("0x0000000000000000000000000000000000000abc", "ens", "60")
        .await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60&mode=both")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("primary-name tuple present request failed")?;

    assert_eq!(response.status(), StatusCode::OK);

    let payload: PrimaryNameResponse = read_json(response).await?;
    assert_eq!(
        payload.declared_state,
        Some(json!({
            "claimed_primary_name": {
                "status": "unsupported",
                "provenance": {},
            }
        }))
    );
    assert_eq!(
        payload.verified_state,
        Some(json!({
            "verified_primary_name": {
                "status": "not_found",
            }
        }))
    );
    assert_eq!(payload.coverage, primary_name_supported_coverage("ens"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_defaults_namespace_and_coin_type() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let default_tuple = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("default tuple request failed")?;
    let default_namespace = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?coin_type=60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("default namespace request failed")?;

    assert_eq!(default_tuple.status(), StatusCode::OK);
    assert_eq!(default_namespace.status(), StatusCode::OK);

    let default_tuple_payload: PrimaryNameResponse = read_json(default_tuple).await?;
    let default_namespace_payload: PrimaryNameResponse = read_json(default_namespace).await?;
    assert_eq!(
        default_tuple_payload.data,
        json!({
            "address": "0x0000000000000000000000000000000000000abc",
            "namespace": "ens",
            "coin_type": "60",
        })
    );
    assert_eq!(default_namespace_payload.data, default_tuple_payload.data);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_rejects_malformed_input() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let malformed_address = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/not-an-address?namespace=ens&coin_type=60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("malformed-address request failed")?;
    let malformed_coin_type = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=60,61")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("malformed-coin-type request failed")?;
    let overflowing_coin_type = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=ens&coin_type=18446744073709551616")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("overflowing-coin-type request failed")?;

    assert_eq!(malformed_address.status(), StatusCode::BAD_REQUEST);
    assert_eq!(malformed_coin_type.status(), StatusCode::BAD_REQUEST);
    assert_eq!(overflowing_coin_type.status(), StatusCode::BAD_REQUEST);

    let malformed_address_payload: ErrorResponse = read_json(malformed_address).await?;
    let malformed_coin_type_payload: ErrorResponse = read_json(malformed_coin_type).await?;
    let overflowing_coin_type_payload: ErrorResponse = read_json(overflowing_coin_type).await?;
    assert_eq!(malformed_address_payload.error.code, "invalid_input");
    assert_eq!(
        malformed_address_payload.error.message,
        "address must be a 0x-prefixed 20-byte hex string"
    );
    assert_eq!(malformed_coin_type_payload.error.code, "invalid_input");
    assert_eq!(
        malformed_coin_type_payload.error.message,
        "coin_type must contain only decimal digits"
    );
    assert_eq!(overflowing_coin_type_payload.error.code, "invalid_input");
    assert_eq!(
        overflowing_coin_type_payload.error.message,
        "coin_type must fit in an unsigned 64-bit integer"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn get_primary_names_returns_not_found_for_unsupported_namespace() -> Result<()> {
    let database = TestDatabase::new(false).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/primary-names/0x0000000000000000000000000000000000000abc?namespace=unknown&coin_type=60")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .context("unsupported-namespace primary-name request failed")?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let payload: ErrorResponse = read_json(response).await?;
    assert_eq!(payload.error.code, "not_found");
    assert_eq!(payload.error.message, "namespace unknown is not supported");

    database.cleanup().await?;
    Ok(())
}
