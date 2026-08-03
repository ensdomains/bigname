use alloy_sol_types::{SolCall, SolValue, sol};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};

use super::*;

mod resolver_abi {
    use super::*;

    sol! {
        function text(bytes32 node, string key) external view returns (string);
    }
}

async fn spawn_mock_rpc(result: Value) -> Result<(String, JoinHandle<Result<Value>>)> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind mock RPC listener")?;
    let url = format!("http://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let (mut socket, _) = listener
            .accept()
            .await
            .context("failed to accept mock RPC request")?;
        let request_payload = read_http_json_body(&mut socket).await?;
        write_json_rpc_response(&mut socket, result).await?;
        Ok(request_payload)
    });

    Ok((url, handle))
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

async fn write_json_rpc_response(socket: &mut tokio::net::TcpStream, result: Value) -> Result<()> {
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
        .context("failed to write mock RPC response")
}

async fn join_request(handle: JoinHandle<Result<Value>>) -> Result<Value> {
    handle
        .await
        .context("mock RPC task panicked or was cancelled")?
}

#[test]
fn ens_namehash_hex_hashes_names() -> Result<()> {
    assert_eq!(
        ens_namehash_hex("eth")?,
        "0x93cdeb708b7545dc668eb9280176169d1c33cfd8ed6f04690a0bcc88a93fc4ae"
    );
    Ok(())
}

#[test]
fn decodes_multicall_text_results() -> Result<()> {
    let text_return = ("ipfs://avatar".to_owned(),).abi_encode_params();
    let empty_return = ("".to_owned(),).abi_encode_params();
    let malformed_return = [0xab, 0xcd];
    let encoded = (vec![
        abi::Result3 {
            success: true,
            returnData: Bytes::copy_from_slice(&text_return),
        },
        abi::Result3 {
            success: true,
            returnData: Bytes::copy_from_slice(&empty_return),
        },
        abi::Result3 {
            success: false,
            returnData: Bytes::new(),
        },
        abi::Result3 {
            success: true,
            returnData: Bytes::copy_from_slice(&malformed_return),
        },
    ],)
        .abi_encode_params();

    let results = decode_multicall_results(&encoded)?;
    assert_eq!(
        &results[..3],
        [
            EnsTextRecordMulticallResult::Success {
                value: "ipfs://avatar".to_owned()
            },
            EnsTextRecordMulticallResult::NotFound,
            EnsTextRecordMulticallResult::Failed {
                message: "resolver text call returned failure from Multicall3".to_owned()
            },
        ]
    );
    let EnsTextRecordMulticallResult::Failed { message } = &results[3] else {
        panic!("malformed resolver return data must be reported as a per-call failure");
    };
    assert!(
        message.starts_with("resolver text call return data is malformed:"),
        "{message}"
    );
    Ok(())
}

#[test]
fn encodes_text_call_targets() -> Result<()> {
    let call = multicall_call_for_text_request(&EnsTextRecordMulticallRequest {
        resolver_address: "0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41".to_owned(),
        namehash: ens_namehash_hex("taytems.eth")?,
        text_key: "avatar".to_owned(),
    })?;

    assert_eq!(
        hex_string(call.target.as_slice()),
        "0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41"
    );
    assert_eq!(&call.callData[..4], [0x59, 0xd1, 0xd4, 0x3c]);
    assert!(!call.callData.is_empty());
    Ok(())
}

#[test]
fn text_multicall_preserves_exact_keys_including_whitespace() -> Result<()> {
    for text_key in ["url ", " "] {
        let call = multicall_call_for_text_request(&EnsTextRecordMulticallRequest {
            resolver_address: "0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41".to_owned(),
            namehash: ens_namehash_hex("taytems.eth")?,
            text_key: text_key.to_owned(),
        })?;
        let decoded = resolver_abi::textCall::abi_decode(&call.callData)
            .context("text resolver calldata did not decode")?;
        assert_eq!(decoded.key, text_key);
    }
    Ok(())
}

#[test]
fn invalid_text_call_namehashes_fail_per_request() -> Result<()> {
    let requests = vec![
        EnsTextRecordMulticallRequest {
            resolver_address: "0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41".to_owned(),
            namehash: ens_namehash_hex("taytems.eth")?,
            text_key: "avatar".to_owned(),
        },
        EnsTextRecordMulticallRequest {
            resolver_address: "0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41".to_owned(),
            namehash: "not-a-namehash".to_owned(),
            text_key: "avatar".to_owned(),
        },
    ];

    let (calls, call_indices, partial_results) = multicall_calls_for_text_requests(&requests);
    assert_eq!(calls.len(), 1);
    assert_eq!(call_indices, vec![0]);
    assert!(partial_results[0].is_none());
    let Some(EnsTextRecordMulticallResult::Failed { message }) = &partial_results[1] else {
        panic!("invalid ENS namehash must become a failed per-request result");
    };
    assert!(
        message.contains("namehash not-a-namehash is invalid"),
        "{message}"
    );
    Ok(())
}

#[tokio::test]
async fn text_multicall_splices_request_build_failures() -> Result<()> {
    let encoded = (vec![abi::Result3 {
        success: true,
        returnData: Bytes::copy_from_slice(&("ipfs://avatar".to_owned(),).abi_encode_params()),
    }],)
        .abi_encode_params();
    let (rpc_url, handle) = spawn_mock_rpc(Value::String(hex_string(&encoded))).await?;
    let rpc_urls = ChainRpcUrls::from_entries(&[format!("ethereum-mainnet={rpc_url}")])?;

    let results = execute_ens_text_record_multicall(
        &rpc_urls,
        "ethereum-mainnet",
        MULTICALL3_ADDRESS,
        &EnsTextRecordMulticallBlock {
            block_number: 12_345,
            block_hash: "0xabc".to_owned(),
        },
        &[
            EnsTextRecordMulticallRequest {
                resolver_address: "0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41".to_owned(),
                namehash: ens_namehash_hex("taytems.eth")?,
                text_key: "avatar".to_owned(),
            },
            EnsTextRecordMulticallRequest {
                resolver_address: "0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41".to_owned(),
                namehash: "0x1234".to_owned(),
                text_key: "avatar".to_owned(),
            },
        ],
    )
    .await?;

    assert_eq!(results.len(), 2);
    assert_eq!(
        results[0],
        EnsTextRecordMulticallResult::Success {
            value: "ipfs://avatar".to_owned()
        }
    );
    let EnsTextRecordMulticallResult::Failed { message } = &results[1] else {
        panic!("invalid ENS namehash must stay aligned with the original request");
    };
    assert!(
        message.contains("namehash 0x1234 must contain exactly 32 bytes"),
        "{message}"
    );

    let request = join_request(handle).await?;
    assert_eq!(request["method"], "eth_call");
    assert_eq!(
        request["params"][0]["to"],
        MULTICALL3_ADDRESS.to_lowercase()
    );
    assert_eq!(
        request["params"][1],
        json!({
            "blockHash": "0xabc",
            "requireCanonical": true,
        })
    );
    Ok(())
}

#[test]
fn text_multicall_block_selector_is_hash_pinned() {
    let block = EnsTextRecordMulticallBlock {
        block_number: 12_345,
        block_hash: "0xabc".to_owned(),
    };

    assert_eq!(
        block_selector(&block),
        json!({
            "blockHash": "0xabc",
            "requireCanonical": true,
        })
    );
}
