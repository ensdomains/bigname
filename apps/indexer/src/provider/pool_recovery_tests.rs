use std::{
    future::pending,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

use super::{JsonRpcProvider, http_client::RecoveringHttpClient};

#[tokio::test]
async fn connect_timeout_does_not_rebuild_the_client_generation() -> Result<()> {
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_millis(100))
        .timeout(Duration::from_secs(1))
        .dns_resolver(PendingDnsResolver)
        .no_proxy()
        .build()
        .context("failed to build connect-timeout test client")?;
    let recovering_client =
        RecoveringHttpClient::new(Duration::from_millis(100), Duration::from_secs(1))?;
    let (_, initial_generation) = recovering_client.snapshot();

    let error = client
        .get("http://connect-timeout.test")
        .send()
        .await
        .expect_err("the pending DNS lookup must hit the connect timeout");

    assert!(
        error.is_timeout(),
        "reqwest must classify a connect timeout as a timeout: {error}"
    );
    assert!(
        error.is_connect(),
        "reqwest must classify a connect timeout as a connect error: {error}"
    );
    assert_eq!(
        recovering_client.record_transport_error(initial_generation, &error)?,
        None,
        "a timeout before a pooled connection exists must not rebuild the client"
    );
    assert_eq!(recovering_client.snapshot().1, initial_generation);

    Ok(())
}

#[derive(Debug)]
struct PendingDnsResolver;

impl reqwest::dns::Resolve for PendingDnsResolver {
    fn resolve(&self, _name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        Box::pin(pending())
    }
}

#[tokio::test]
async fn json_rpc_provider_rebuilds_pool_after_stale_keepalive_timeouts() -> Result<()> {
    let server = StaleKeepAliveServer::spawn().await?;
    let provider =
        JsonRpcProvider::new_with_request_timeout(&server.url, Duration::from_millis(100))?;

    let warm = provider
        .fetch_json_rpc_result("eth_chainId", Vec::new())
        .await?;
    assert_eq!(warm, Some(Value::String("0x1".to_owned())));
    assert_eq!(
        server.accepted_connections.load(Ordering::SeqCst),
        1,
        "warming must leave one keep-alive connection in the pool"
    );

    server.poison_existing.store(true, Ordering::SeqCst);
    let recovered = provider
        .fetch_json_rpc_result("eth_chainId", Vec::new())
        .await?;

    assert_eq!(recovered, Some(Value::String("0x1".to_owned())));
    assert_eq!(
        server.poisoned_requests.load(Ordering::SeqCst),
        1,
        "the first pooled transport timeout must rebuild the client"
    );
    assert!(
        server.accepted_connections.load(Ordering::SeqCst) >= 2,
        "recovery must open a fresh connection instead of exhausting the warmed pool"
    );
    assert_eq!(
        provider.client.snapshot().1,
        1,
        "the timeout threshold must advance the HTTP client generation"
    );

    server.task.abort();
    Ok(())
}

struct StaleKeepAliveServer {
    url: String,
    poison_existing: Arc<AtomicBool>,
    accepted_connections: Arc<AtomicUsize>,
    poisoned_requests: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

impl StaleKeepAliveServer {
    async fn spawn() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .context("failed to bind stale keep-alive test server")?;
        let address = listener
            .local_addr()
            .context("failed to read stale keep-alive test server address")?;
        let poison_existing = Arc::new(AtomicBool::new(false));
        let accepted_connections = Arc::new(AtomicUsize::new(0));
        let poisoned_requests = Arc::new(AtomicUsize::new(0));
        let task = {
            let poison_existing = Arc::clone(&poison_existing);
            let accepted_connections = Arc::clone(&accepted_connections);
            let poisoned_requests = Arc::clone(&poisoned_requests);
            tokio::spawn(async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        break;
                    };
                    accepted_connections.fetch_add(1, Ordering::SeqCst);
                    let connection_existed_before_poison = !poison_existing.load(Ordering::SeqCst);
                    let poison_existing = Arc::clone(&poison_existing);
                    let poisoned_requests = Arc::clone(&poisoned_requests);
                    tokio::spawn(async move {
                        serve_connection(
                            stream,
                            connection_existed_before_poison,
                            poison_existing,
                            poisoned_requests,
                        )
                        .await;
                    });
                }
            })
        };

        Ok(Self {
            url: format!("http://{address}"),
            poison_existing,
            accepted_connections,
            poisoned_requests,
            task,
        })
    }
}

async fn serve_connection(
    mut stream: TcpStream,
    connection_existed_before_poison: bool,
    poison_existing: Arc<AtomicBool>,
    poisoned_requests: Arc<AtomicUsize>,
) {
    let mut buffer = Vec::new();
    while let Some(request) = read_json_request(&mut stream, &mut buffer).await {
        if connection_existed_before_poison && poison_existing.load(Ordering::SeqCst) {
            poisoned_requests.fetch_add(1, Ordering::SeqCst);
            pending::<()>().await;
        }
        if write_json_response(&mut stream, &request).await.is_err() {
            return;
        }
    }
}

async fn read_json_request(stream: &mut TcpStream, buffer: &mut Vec<u8>) -> Option<Value> {
    loop {
        if let Some(header_end) = find_header_end(buffer) {
            let content_length = parse_content_length(&buffer[..header_end])?;
            let request_end = header_end.checked_add(4)?.checked_add(content_length)?;
            if buffer.len() >= request_end {
                let body = serde_json::from_slice(&buffer[header_end + 4..request_end]).ok()?;
                buffer.drain(..request_end);
                return Some(body);
            }
        }

        let mut chunk = [0_u8; 4096];
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
}

async fn write_json_response(stream: &mut TcpStream, request: &Value) -> Result<()> {
    let response_body = json!({
        "jsonrpc": "2.0",
        "id": request.get("id").cloned().unwrap_or(Value::Null),
        "result": "0x1",
    })
    .to_string();
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: keep-alive\r\n\r\n{}",
        response_body.len(),
        response_body
    );
    stream
        .write_all(response.as_bytes())
        .await
        .context("failed to write stale keep-alive test response")
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}

fn parse_content_length(headers: &[u8]) -> Option<usize> {
    let headers = std::str::from_utf8(headers).ok()?;
    headers.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("content-length")
            .then(|| value.trim().parse().ok())?
    })
}
