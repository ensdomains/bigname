use anyhow::Context;
use std::sync::Mutex;

use serde_json::{Value, json};
use tokio::io::AsyncWriteExt;

use super::*;
use crate::test_chain::read_request_body;

const SEPOLIA_GENESIS: &str = "0x25a5cc106eea7138acab33231d7160d69cb777ee0c2c553fcddf5138993e6dd9";
const MAINNET_GENESIS: &str = "0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3";
const OTHER_HASH: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";

/// An endpoint that reports `chain_id` and `genesis` (no block 0 when `None`) and answers
/// every other block read with a checkpoint-shaped block.
struct Node {
    endpoint: String,
    methods: Arc<Mutex<Vec<String>>>,
    listener: tokio::task::JoinHandle<()>,
}

impl Drop for Node {
    fn drop(&mut self) {
        self.listener.abort();
    }
}

impl Node {
    async fn start(chain_id: &str, genesis: Option<&str>) -> Result<Self> {
        Self::stalling(chain_id, genesis, None).await
    }

    /// Like [`Node::start`], but holds the first `eth_getBlockByNumber` for `block` (a hex
    /// quantity) for the given time before answering it.
    async fn stalling(
        chain_id: &str,
        genesis: Option<&str>,
        stall: Option<(&'static str, Duration)>,
    ) -> Result<Self> {
        let stall = Arc::new(Mutex::new(stall));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://user:secret@{}/v1/apikey", listener.local_addr()?);
        let methods = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&methods);
        let chain_id = chain_id.to_owned();
        let genesis = genesis.map(str::to_owned);
        let listener = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let seen = Arc::clone(&seen);
                let stall = Arc::clone(&stall);
                let chain_id = chain_id.clone();
                let genesis = genesis.clone();
                tokio::spawn(async move {
                    let request: Value =
                        serde_json::from_str(&read_request_body(&mut socket).await.unwrap())
                            .unwrap();
                    let answer = |call: &Value| {
                        let method = call["method"].as_str().unwrap().to_owned();
                        let result = match (method.as_str(), call["params"][0].as_str()) {
                            ("eth_chainId", _) => json!(chain_id),
                            ("eth_getBlockByNumber", Some("0x0")) => {
                                genesis.as_ref().map_or(Value::Null, |hash| block(0, hash))
                            }
                            ("eth_getBlockByNumber", _) => block(9, OTHER_HASH),
                            _ => Value::Null,
                        };
                        seen.lock().unwrap().push(method);
                        json!({"jsonrpc": "2.0", "id": call["id"], "result": result})
                    };
                    let calls = request
                        .as_array()
                        .cloned()
                        .unwrap_or_else(|| vec![request.clone()]);
                    let delay = {
                        let mut stall = stall.lock().unwrap();
                        let hit = stall.is_some_and(|(block, _)| {
                            calls.iter().any(|call| {
                                call["method"] == "eth_getBlockByNumber"
                                    && call["params"][0] == block
                            })
                        });
                        if hit {
                            stall.take().map(|(_, delay)| delay)
                        } else {
                            None
                        }
                    };
                    let response = match request.as_array() {
                        Some(calls) => Value::Array(calls.iter().map(answer).collect()),
                        None => answer(&request),
                    }
                    .to_string();
                    let reply = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{response}",
                        response.len()
                    );
                    if let Some(delay) = delay {
                        tokio::time::sleep(delay).await;
                    }
                    let _ = socket.write_all(reply.as_bytes()).await;
                });
            }
        });
        Ok(Self {
            endpoint,
            methods,
            listener,
        })
    }

    fn count(&self, method: &str) -> usize {
        self.methods
            .lock()
            .unwrap()
            .iter()
            .filter(|seen| *seen == method)
            .count()
    }

    fn guarded(&self, chain: &str, mode: RpcChainCheck) -> Result<JsonRpcProvider> {
        Ok(JsonRpcProvider::new(&self.endpoint)?
            .with_chain_check(ExpectedRpcChain::new(chain, "primary", mode)?))
    }
}

fn block(number: i64, hash: &str) -> Value {
    json!({
        "number": format!("0x{number:x}"),
        "hash": hash,
        "parentHash": OTHER_HASH,
        "timestamp": "0x1",
    })
}

fn mismatch_of(error: &anyhow::Error) -> RpcChainMismatch {
    chain_mismatch_in(error)
        .cloned()
        .unwrap_or_else(|| panic!("not a chain mismatch: {error:#}"))
}

#[tokio::test]
async fn a_matching_endpoint_is_checked_once_and_then_trusted() -> Result<()> {
    let node = Node::start("0xaa36a7", Some(SEPOLIA_GENESIS)).await?;
    let provider = node.guarded("ethereum-sepolia", RpcChainCheck::Full)?;

    provider.heads().await?;
    provider.resolve(&[9, 9]).await?;

    assert_eq!(node.count("eth_chainId"), 1);
    assert_eq!(
        node.methods.lock().unwrap()[..2],
        ["eth_chainId", "eth_getBlockByNumber"]
    );
    Ok(())
}

#[tokio::test]
async fn a_wrong_chain_id_is_refused_without_retry_or_url() -> Result<()> {
    let node = Node::start("0xaa36a7", Some(SEPOLIA_GENESIS)).await?;
    let counters = RpcCounters::default();
    let provider = JsonRpcProvider {
        counters: counters.source("ethereum-mainnet", "primary"),
        ..node.guarded("ethereum-mainnet", RpcChainCheck::Full)?
    };

    let error = provider.heads().await.unwrap_err();
    let mismatch = mismatch_of(&error);
    assert_eq!(
        (mismatch.expected_chain_id, mismatch.observed_chain_id),
        (1, Some(11_155_111))
    );
    assert_eq!(mismatch.observed_genesis_hash, None);
    assert!(!is_retryable(&error));
    let classified = provider_error("failed to fetch ingest target heads", error);
    assert_eq!(classified.kind(), crate::ErrorKind::Configuration);
    assert_eq!(classified.rpc_chain_mismatch(), Some(&mismatch));
    let rendered = classified.to_string();
    for needle in ["ethereum-mainnet", "primary", "11155111"] {
        assert!(rendered.contains(needle), "{rendered} lacks {needle}");
    }
    for secret in ["secret", "apikey", "127.0.0.1"] {
        assert!(!rendered.contains(secret), "{rendered} leaks {secret}");
    }
    assert_eq!(node.count("eth_chainId"), 1);
    assert_eq!(
        node.count("eth_getBlockByNumber"),
        0,
        "a wrong chain id is refused before block 0 is asked for"
    );

    provider.heads().await.unwrap_err();
    assert_eq!(node.count("eth_chainId"), 2, "a refusal is never trusted");
    let calls: Vec<_> = counters
        .snapshot()
        .into_iter()
        .filter(|(count, _)| count.kind == RpcCountKind::Calls)
        .map(|(count, total)| (count.label, total))
        .collect();
    assert_eq!(
        calls,
        [("eth_chainId".to_owned(), 2)],
        "a data call the chain guard refused is not counted"
    );
    Ok(())
}

#[tokio::test]
async fn a_wrong_genesis_is_refused_in_full_mode_only() -> Result<()> {
    let node = Node::start("0x1", Some(OTHER_HASH)).await?;

    let full = node.guarded("ethereum-mainnet", RpcChainCheck::Full)?;
    let mismatch = mismatch_of(&full.heads().await.unwrap_err());
    assert_eq!(mismatch.observed_chain_id, Some(1));
    assert_eq!(mismatch.observed_genesis_hash.as_deref(), Some(OTHER_HASH));

    let block_reads = node.count("eth_getBlockByNumber");
    node.guarded("ethereum-mainnet", RpcChainCheck::ChainIdOnly)?
        .heads()
        .await?;
    assert_eq!(
        node.count("eth_getBlockByNumber") - block_reads,
        3,
        "chain-id-only mode reads no block 0, only the three head tags"
    );
    Ok(())
}

#[tokio::test]
async fn a_missing_block_zero_or_unreadable_chain_id_is_a_mismatch() -> Result<()> {
    let no_genesis = Node::start("0x2105", None).await?;
    let mismatch = mismatch_of(
        &no_genesis
            .guarded("base-mainnet", RpcChainCheck::Full)?
            .heads()
            .await
            .unwrap_err(),
    );
    assert_eq!(mismatch.observed_chain_id, Some(8453));
    assert_eq!(mismatch.observed_genesis_hash, None);

    let unreadable = Node::start("0x", Some(SEPOLIA_GENESIS)).await?;
    let mismatch = mismatch_of(
        &unreadable
            .guarded("ethereum-sepolia", RpcChainCheck::ChainIdOnly)?
            .heads()
            .await
            .unwrap_err(),
    );
    assert_eq!(mismatch.observed_chain_id, None);
    Ok(())
}

#[tokio::test]
async fn the_check_repeats_after_the_interval_and_after_a_client_rebuild() -> Result<()> {
    let node = Node::start("0x1", Some(MAINNET_GENESIS)).await?;
    let provider = JsonRpcProvider::new(&node.endpoint)?.with_chain_check_every(
        ExpectedRpcChain::new("ethereum-mainnet", "primary", RpcChainCheck::Full)?,
        Duration::from_millis(300),
    );

    provider.resolve(&[9]).await?;
    provider.resolve(&[9]).await?;
    assert_eq!(node.count("eth_chainId"), 1);
    let check = provider.ensure_chain().await?.expect("guarded");
    assert!(check.covers(provider.client.client_id()));

    tokio::time::sleep(Duration::from_millis(350)).await;
    assert!(
        !check.covers(provider.client.client_id()),
        "an expired check does not admit a request that waited for its permit"
    );
    provider.resolve(&[9]).await?;
    assert_eq!(node.count("eth_chainId"), 2, "rechecked after the interval");

    provider.client.rebuild();
    provider.resolve(&[9]).await?;
    assert_eq!(
        node.count("eth_chainId"),
        3,
        "rechecked on the rebuilt client"
    );
    provider.resolve(&[9]).await?;
    assert_eq!(node.count("eth_chainId"), 3);
    Ok(())
}

#[tokio::test]
async fn unknown_chains_fail_closed() {
    for chain in ["test-chain", "project-fixture", "base-sepolia"] {
        let error = ExpectedRpcChain::new(chain, "primary", RpcChainCheck::Full).unwrap_err();
        assert_eq!(error.kind(), crate::ErrorKind::Configuration, "{chain}");
    }
    let error = ChainProvider::with_config(
        "test-chain",
        "primary",
        "rpc",
        "http://127.0.0.1:1",
        None,
        IngestConfig::default().with_rpc_chain_check(RpcChainCheck::ChainIdOnly),
    )
    .err()
    .expect("an unknown chain has no RPC chain check");
    assert!(format!("{error:#}").contains("no known EIP-155 chain id"));
}

#[tokio::test]
async fn an_unpinned_chain_is_held_to_its_recorded_genesis() -> Result<()> {
    let node = Node::start("0x2105", Some(SEPOLIA_GENESIS)).await?;
    let source = |recorded: &str| {
        ChainProvider::with_config(
            "base-mainnet",
            "primary",
            "rpc",
            &node.endpoint,
            Some(recorded),
            IngestConfig::default().with_rpc_chain_check(RpcChainCheck::Full),
        )
    };

    source(SEPOLIA_GENESIS)?.resolve(&[9]).await?;
    let mismatch = mismatch_of(&source(MAINNET_GENESIS)?.resolve(&[9]).await.unwrap_err());
    assert_eq!(
        (
            mismatch.expected_genesis_hash.as_deref(),
            mismatch.observed_genesis_hash.as_deref()
        ),
        (Some(MAINNET_GENESIS), Some(SEPOLIA_GENESIS))
    );

    let reference = crate::VerificationProvider::new("base-mainnet", "drpc", &node.endpoint)?
        .with_rpc_chain_check(
            "base-mainnet",
            "reference",
            RpcChainCheck::Full,
            Some(MAINNET_GENESIS),
        )?;
    let error = crate::admit_ingest_checkpoint_heads(&reference)
        .await
        .unwrap_err();
    assert_eq!(
        error
            .rpc_chain_mismatch()
            .map(|m| m.observed_genesis_hash.clone()),
        Some(Some(SEPOLIA_GENESIS.to_owned()))
    );
    Ok(())
}

#[tokio::test]
async fn a_startup_check_error_never_repeats_the_endpoint_path_or_key() -> Result<()> {
    #[derive(Clone, Default)]
    struct Logs(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for Logs {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buffer);
            Ok(buffer.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let logs = Logs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_writer({
            let logs = logs.clone();
            move || logs.clone()
        })
        .finish();
    let _logs = tracing::subscriber::set_default(subscriber);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!(
        "http://{}/v1/secret-key?token=hunter2",
        listener.local_addr()?
    );
    tokio::spawn(async move {
        let echo = "unknown key in POST /v1/secret-key?token=hunter2, token hunter2";
        let rpc_error = |code: i64, message: &str| {
            json!({"jsonrpc": "2.0", "id": 1, "error": {"code": code, "message": message}})
                .to_string()
        };
        let mut answers = vec![
            ("200 OK", rpc_error(-32000, "invalid project id hunter2")),
            (
                "200 OK",
                rpc_error(-32005, "rate limit exceeded for hunter2"),
            ),
            ("503 Service Unavailable", echo.to_owned()),
        ];
        while let Ok((mut socket, _)) = listener.accept().await {
            let _ = read_request_body(&mut socket).await;
            let (status, body) = answers
                .pop()
                .unwrap_or(("401 Unauthorized", echo.to_owned()));
            let response = format!(
                "HTTP/1.1 {status}\r\ncontent-length: {}\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
        }
    });

    let expected = ExpectedRpcChain::new("ethereum-mainnet", "primary", RpcChainCheck::Full)?;
    let error = verify_rpc_chain(&endpoint, &expected).await.unwrap_err();
    let rendered = format!("{error:#}");
    assert!(
        rendered.contains(
            "eth_chainId: -32000 (invalid input or server error; provider message omitted)"
        ),
        "{rendered}"
    );
    let logged = String::from_utf8(logs.0.lock().unwrap().clone())?;
    assert!(
        logged.contains("503") && logged.contains("-32005"),
        "{logged}"
    );
    for secret in ["secret-key", "hunter2"] {
        assert!(!rendered.contains(secret), "{rendered}");
        assert!(!logged.contains(secret), "{logged}");
    }
    Ok(())
}

#[tokio::test]
async fn verification_references_and_one_shot_checks_share_the_guard() -> Result<()> {
    let node = Node::start("0x1", Some(MAINNET_GENESIS)).await?;
    let reference = crate::VerificationProvider::new("ethereum-sepolia", "drpc", &node.endpoint)?
        .with_rpc_chain_check(
        "ethereum-sepolia",
        "reference",
        RpcChainCheck::ChainIdOnly,
        None,
    )?;
    let error = crate::admit_ingest_checkpoint_heads(&reference)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), crate::ErrorKind::Configuration);
    let mismatch = error.rpc_chain_mismatch().expect("chain mismatch");
    assert_eq!(
        (mismatch.source_key.as_str(), mismatch.observed_chain_id),
        ("reference", Some(1))
    );

    let expected = ExpectedRpcChain::new("ethereum-mainnet", "hydration", RpcChainCheck::Full)?;
    let observed = verify_rpc_chain(&node.endpoint, &expected).await?;
    assert_eq!(
        observed,
        ObservedRpcChain {
            chain_id: Some(1),
            genesis_hash: Some(MAINNET_GENESIS.to_owned()),
        }
    );
    let error = verify_rpc_chain(
        &node.endpoint,
        &ExpectedRpcChain::new("base-mainnet", "hydration", RpcChainCheck::Full)?,
    )
    .await
    .unwrap_err();
    assert_eq!(error.kind(), crate::ErrorKind::Configuration);
    Ok(())
}

#[tokio::test]
async fn a_retry_on_a_client_rebuilt_after_a_timeout_is_checked_first() -> Result<()> {
    let node = Node::stalling(
        "0x1",
        Some(MAINNET_GENESIS),
        Some(("0x9", Duration::from_secs(5))),
    )
    .await?;
    let mut provider = node.guarded("ethereum-mainnet", RpcChainCheck::Full)?;
    provider.client = super::http_client::RecoveringHttpClient::new(
        Duration::from_secs(1),
        Duration::from_secs(1),
    )?;

    provider.resolve(&[9]).await?;

    assert_eq!(
        node.methods.lock().unwrap()[..],
        [
            "eth_chainId",
            "eth_getBlockByNumber",
            "eth_getBlockByNumber",
            "eth_chainId",
            "eth_getBlockByNumber",
            "eth_getBlockByNumber",
        ],
        "the timed-out read rebuilt the client, which was checked before the retry"
    );
    Ok(())
}

#[tokio::test]
async fn a_request_that_waited_out_the_interval_for_its_permit_is_checked_again() -> Result<()> {
    let node = Node::stalling(
        "0x1",
        Some(MAINNET_GENESIS),
        Some(("0x9", Duration::from_millis(400))),
    )
    .await?;
    let provider = JsonRpcProvider::with_config(&node.endpoint, IngestConfig::new(1, 1, 1)?)?
        .with_chain_check_every(
            ExpectedRpcChain::new("ethereum-mainnet", "primary", RpcChainCheck::Full)?,
            Duration::from_millis(300),
        );

    let (stalled, waited) = tokio::join!(provider.resolve(&[9]), provider.resolve(&[9]));
    stalled?;
    waited?;

    assert_eq!(
        node.methods.lock().unwrap()[..],
        [
            "eth_chainId",
            "eth_getBlockByNumber",
            "eth_getBlockByNumber",
            "eth_chainId",
            "eth_getBlockByNumber",
            "eth_getBlockByNumber",
        ],
        "the second read waited past the interval behind the stalled one, so it was checked again"
    );
    Ok(())
}

#[tokio::test]
async fn a_client_rebuilt_while_the_check_runs_is_checked_again() -> Result<()> {
    let node = Node::stalling(
        "0x1",
        Some(MAINNET_GENESIS),
        Some(("0x0", Duration::from_millis(300))),
    )
    .await?;
    let provider = node.guarded("ethereum-mainnet", RpcChainCheck::Full)?;

    let rebuild = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        provider.client.rebuild();
    };
    let (checked, ()) = tokio::join!(provider.ensure_chain(), rebuild);
    let check = checked?.expect("guarded");

    assert_eq!(node.count("eth_chainId"), 2);
    assert!(check.covers(provider.client.client_id()));
    Ok(())
}

#[tokio::test]
async fn concurrent_requests_on_a_cold_guard_all_complete() -> Result<()> {
    let node = Node::start("0x1", Some(MAINNET_GENESIS)).await?;
    let provider = node.guarded("ethereum-mainnet", RpcChainCheck::Full)?;
    tokio::time::timeout(Duration::from_secs(10), provider.resolve(&[9; 64]))
        .await
        .context("requests waiting on the first chain check stalled")??;
    assert_eq!(node.count("eth_chainId"), 1);
    Ok(())
}
