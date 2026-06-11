use super::*;

#[derive(Debug, Eq, PartialEq)]
struct GatewayRequest {
    method: String,
    path: String,
    body: Vec<u8>,
}

#[test]
fn decodes_offchain_lookup_from_compat_rpc_error_shapes() -> Result<()> {
    let encoded = encoded_offchain_lookup_error();
    let shapes = [
        json!(encoded.clone()),
        json!({ "data": encoded.clone() }),
        json!({ "originalError": { "data": encoded.clone() } }),
        json!({ "error": { "data": encoded } }),
    ];

    for data in shapes {
        let lookup = offchain_lookup_from_rpc_error(&JsonRpcCallError {
            code: Some(3),
            message: "execution reverted".to_owned(),
            data: Some(data),
        })?
        .expect("OffchainLookup error data must decode");
        assert_eq!(lookup.sender, "0x1111111111111111111111111111111111111111");
        assert_eq!(lookup.urls, vec!["https://gateway.example/{data}"]);
        assert_eq!(lookup.call_data, vec![0xab, 0xcd]);
        assert_eq!(lookup.callback_function, [0x12, 0x34, 0x56, 0x78]);
        assert_eq!(lookup.extra_data, vec![0xef]);
    }

    Ok(())
}

#[test]
fn ignores_non_offchain_lookup_revert_data() -> Result<()> {
    let lookup = offchain_lookup_from_rpc_error(&JsonRpcCallError {
        code: Some(3),
        message: "execution reverted".to_owned(),
        data: Some(json!("0x08c379a0")),
    })?;

    assert_eq!(lookup, None);
    Ok(())
}

#[test]
fn decodes_gateway_response_compatibility_shapes() -> Result<()> {
    let bodies: [&[u8]; 3] = [br#"{"data":"0xabcd"}"#, br#""0xabcd""#, b"0xabcd\n"];

    for body in bodies {
        assert_eq!(decode_gateway_response_body(body)?, vec![0xab, 0xcd]);
    }

    Ok(())
}

#[test]
fn encodes_ccip_callback_calldata() {
    assert_eq!(
        hex_string(&ccip_callback_calldata(
            [0x12, 0x34, 0x56, 0x78],
            &[0xab, 0xcd],
            &[0xef],
        )),
        concat!(
            "0x12345678",
            "0000000000000000000000000000000000000000000000000000000000000040",
            "0000000000000000000000000000000000000000000000000000000000000080",
            "0000000000000000000000000000000000000000000000000000000000000002",
            "abcd000000000000000000000000000000000000000000000000000000000000",
            "0000000000000000000000000000000000000000000000000000000000000001",
            "ef00000000000000000000000000000000000000000000000000000000000000",
        )
    );
}

#[test]
fn encodes_batch_gateway_response() {
    assert_eq!(
        hex_string(&abi_encode_bool_array_and_bytes_array(
            &[false, true],
            &[vec![0xab], vec![0xcd, 0xef]],
        )),
        concat!(
            "0x",
            "0000000000000000000000000000000000000000000000000000000000000040",
            "00000000000000000000000000000000000000000000000000000000000000a0",
            "0000000000000000000000000000000000000000000000000000000000000002",
            "0000000000000000000000000000000000000000000000000000000000000000",
            "0000000000000000000000000000000000000000000000000000000000000001",
            "0000000000000000000000000000000000000000000000000000000000000002",
            "0000000000000000000000000000000000000000000000000000000000000040",
            "0000000000000000000000000000000000000000000000000000000000000080",
            "0000000000000000000000000000000000000000000000000000000000000001",
            "ab00000000000000000000000000000000000000000000000000000000000000",
            "0000000000000000000000000000000000000000000000000000000000000002",
            "cdef000000000000000000000000000000000000000000000000000000000000",
        )
    );
}

#[tokio::test]
async fn sender_only_gateway_template_uses_post_with_sender_substituted() -> Result<()> {
    let (url, handle) = spawn_gateway_response(200, br#"{"data":"0xabcd"}"#).await?;
    let sender = "0x1111111111111111111111111111111111111111";
    let response = fetch_one_gateway(
        &GATEWAY_HTTP_CLIENT,
        &format!("{url}/lookup/{{sender}}"),
        sender,
        "0x1234",
    )
    .await?;

    assert_eq!(response, vec![0xab, 0xcd]);
    let request = join_gateway_request(handle).await?;
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, format!("/lookup/{sender}"));
    assert_eq!(
        serde_json::from_slice::<Value>(&request.body)?,
        json!({
            "sender": sender,
            "data": "0x1234",
        })
    );
    Ok(())
}

#[tokio::test]
async fn gateway_client_errors_stop_without_retrying_later_urls() -> Result<()> {
    let (url, handle) = spawn_gateway_response(400, b"bad request").await?;
    let result = fetch_standard_gateway_response(
        "0x1111111111111111111111111111111111111111",
        &[
            format!("{url}/lookup/{{data}}"),
            "http://127.0.0.1:9/should-not-be-used/{data}".to_owned(),
        ],
        &[0xab, 0xcd],
    )
    .await;
    let Err(error) = result else {
        panic!("HTTP 4xx gateway responses must not fall through to later URLs");
    };

    assert!(
        error.to_string().contains("HTTP 400"),
        "expected first gateway 4xx to be returned, got {error:?}"
    );
    let request = join_gateway_request(handle).await?;
    assert_eq!(request.method, "GET");
    Ok(())
}

#[tokio::test]
async fn gateway_server_errors_retry_later_urls() -> Result<()> {
    let (first_url, first_handle) = spawn_gateway_response(500, b"try another gateway").await?;
    let (second_url, second_handle) = spawn_gateway_response(200, br#"{"data":"0xabcd"}"#).await?;
    let response = fetch_standard_gateway_response(
        "0x1111111111111111111111111111111111111111",
        &[
            format!("{first_url}/lookup/{{data}}"),
            format!("{second_url}/lookup/{{data}}"),
        ],
        &[0xab, 0xcd],
    )
    .await?;

    assert_eq!(response.body, vec![0xab, 0xcd]);
    assert_eq!(join_gateway_request(first_handle).await?.method, "GET");
    assert_eq!(join_gateway_request(second_handle).await?.method, "GET");
    Ok(())
}

#[tokio::test]
async fn malformed_gateway_bodies_retry_later_urls() -> Result<()> {
    let (first_url, first_handle) = spawn_gateway_response(200, br#"{"data":"not-hex"}"#).await?;
    let (second_url, second_handle) = spawn_gateway_response(200, br#"{"data":"0xabcd"}"#).await?;
    let response = fetch_standard_gateway_response(
        "0x1111111111111111111111111111111111111111",
        &[
            format!("{first_url}/lookup/{{data}}"),
            format!("{second_url}/lookup/{{data}}"),
        ],
        &[0xab, 0xcd],
    )
    .await?;

    assert_eq!(response.body, vec![0xab, 0xcd]);
    assert_eq!(join_gateway_request(first_handle).await?.method, "GET");
    assert_eq!(join_gateway_request(second_handle).await?.method, "GET");
    Ok(())
}

#[tokio::test]
async fn offchain_lookup_sender_mismatch_is_rejected_before_callback() -> Result<()> {
    let (rpc_url, _handle) = spawn_json_rpc_result(json!("0x")).await?;
    let rpc = JsonRpcHttpClient::new(&rpc_url)?;
    let result = follow_ccip_read(
        &rpc,
        &JsonRpcCallError {
            code: Some(3),
            message: "execution reverted".to_owned(),
            data: Some(json!(
                encoded_local_batch_offchain_lookup_error_with_sender(Address::repeat_byte(0x11))
            )),
        },
        &json!("latest"),
        "0x2222222222222222222222222222222222222222",
    )
    .await;

    let error = result.expect_err("OffchainLookup sender mismatch must be rejected");
    assert!(
        error.to_string().contains("sender"),
        "unexpected sender mismatch error: {error:?}"
    );
    Ok(())
}

fn encoded_offchain_lookup_error() -> String {
    hex_string(
        &abi::OffchainLookup {
            sender: Address::repeat_byte(0x11),
            urls: vec!["https://gateway.example/{data}".to_owned()],
            callData: Bytes::copy_from_slice(&[0xab, 0xcd]),
            callbackFunction: alloy_primitives::FixedBytes::from(&[0x12, 0x34, 0x56, 0x78]),
            extraData: Bytes::copy_from_slice(&[0xef]),
        }
        .abi_encode(),
    )
}

fn encoded_local_batch_offchain_lookup_error_with_sender(sender: Address) -> String {
    hex_string(
        &abi::OffchainLookup {
            sender,
            urls: vec![LOCAL_BATCH_GATEWAY_URL.to_owned()],
            callData: Bytes::from(abi::queryCall { requests: vec![] }.abi_encode()),
            callbackFunction: alloy_primitives::FixedBytes::from(&[0x12, 0x34, 0x56, 0x78]),
            extraData: Bytes::copy_from_slice(&[0xef]),
        }
        .abi_encode(),
    )
}

async fn spawn_gateway_response(
    status: u16,
    body: &'static [u8],
) -> Result<(String, tokio::task::JoinHandle<Result<GatewayRequest>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind mock CCIP gateway")?;
    let url = format!("http://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let (mut socket, _) = listener
            .accept()
            .await
            .context("failed to accept mock CCIP gateway request")?;
        let request = read_gateway_request(&mut socket).await?;
        write_gateway_response(&mut socket, status, body).await?;
        Ok(request)
    });
    Ok((url, handle))
}

async fn spawn_json_rpc_result(
    result: Value,
) -> Result<(String, tokio::task::JoinHandle<Result<GatewayRequest>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind mock JSON-RPC listener")?;
    let url = format!("http://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let (mut socket, _) = listener
            .accept()
            .await
            .context("failed to accept mock JSON-RPC request")?;
        let request = read_gateway_request(&mut socket).await?;
        write_json_rpc_result(&mut socket, result).await?;
        Ok(request)
    });
    Ok((url, handle))
}

async fn read_gateway_request(socket: &mut tokio::net::TcpStream) -> Result<GatewayRequest> {
    use tokio::io::AsyncReadExt;

    let mut buffer = Vec::new();
    let mut scratch = [0_u8; 1024];
    let (body_start, content_length, method, path) = loop {
        let bytes_read = socket
            .read(&mut scratch)
            .await
            .context("failed to read mock CCIP gateway request")?;
        if bytes_read == 0 {
            bail!("mock CCIP gateway request closed before headers finished");
        }
        buffer.extend_from_slice(&scratch[..bytes_read]);
        if let Some(body_start) = gateway_header_end(&buffer) {
            let headers = std::str::from_utf8(&buffer[..body_start])
                .context("mock CCIP gateway request headers were not utf8")?;
            let (method, path) = gateway_request_line(headers)?;
            let content_length = gateway_content_length(headers)?;
            break (body_start, content_length, method, path);
        }
    };

    while buffer.len() < body_start + content_length {
        let bytes_read = socket
            .read(&mut scratch)
            .await
            .context("failed to read mock CCIP gateway request body")?;
        if bytes_read == 0 {
            bail!("mock CCIP gateway request closed before body finished");
        }
        buffer.extend_from_slice(&scratch[..bytes_read]);
    }

    Ok(GatewayRequest {
        method,
        path,
        body: buffer[body_start..body_start + content_length].to_vec(),
    })
}

fn gateway_header_end(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| position + 4)
}

fn gateway_request_line(headers: &str) -> Result<(String, String)> {
    let line = headers
        .lines()
        .next()
        .context("mock CCIP gateway request missing request line")?;
    let mut parts = line.split_whitespace();
    let method = parts
        .next()
        .context("mock CCIP gateway request missing method")?
        .to_owned();
    let path = parts
        .next()
        .context("mock CCIP gateway request missing path")?
        .to_owned();
    Ok((method, path))
}

fn gateway_content_length(headers: &str) -> Result<usize> {
    Ok(headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>())
        })
        .transpose()
        .context("mock CCIP gateway request content-length was invalid")?
        .unwrap_or(0))
}

async fn write_json_rpc_result(socket: &mut tokio::net::TcpStream, result: Value) -> Result<()> {
    use tokio::io::AsyncWriteExt;

    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": result,
    })
    .to_string();
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    socket
        .write_all(response.as_bytes())
        .await
        .context("failed to write mock JSON-RPC response")
}

async fn write_gateway_response(
    socket: &mut tokio::net::TcpStream,
    status: u16,
    body: &[u8],
) -> Result<()> {
    use tokio::io::AsyncWriteExt;

    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        500 => "Internal Server Error",
        _ => "Mock Status",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\nconnection: close\r\ncontent-length: {}\r\n\r\n",
        body.len()
    );
    socket
        .write_all(response.as_bytes())
        .await
        .context("failed to write mock CCIP gateway response headers")?;
    socket
        .write_all(body)
        .await
        .context("failed to write mock CCIP gateway response body")?;
    Ok(())
}

async fn join_gateway_request(
    handle: tokio::task::JoinHandle<Result<GatewayRequest>>,
) -> Result<GatewayRequest> {
    handle
        .await
        .context("mock CCIP gateway task panicked or was cancelled")?
}
