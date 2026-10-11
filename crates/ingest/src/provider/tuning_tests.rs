use std::{collections::BTreeMap, sync::Mutex, time::Duration};

use serde_json::{Value, json};
use tokio::{io::AsyncWriteExt, sync::mpsc};

use super::*;
use crate::{
    WatchQuery,
    engine::query::{QueryContext, fetch_into},
    test_chain::{WATCHED_ADDRESS, WATCHED_TOPIC, hash_of, read_request_body},
};

#[derive(Default)]
struct Traffic {
    requests: Vec<Value>,
    active: usize,
    peak: usize,
    log_attempts: usize,
}

struct Endpoint {
    provider: JsonRpcProvider,
    traffic: Arc<Mutex<Traffic>>,
    bodies: Arc<Semaphore>,
    arrivals: mpsc::UnboundedReceiver<()>,
    listener: tokio::task::JoinHandle<()>,
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        self.listener.abort();
    }
}

async fn endpoint(
    config: IngestConfig,
    reject_batches: bool,
    block_bodies: bool,
) -> Result<Endpoint> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let provider =
        JsonRpcProvider::with_config(&format!("http://{}", listener.local_addr()?), config)?;
    let traffic = Arc::new(Mutex::new(Traffic::default()));
    let bodies = Arc::new(Semaphore::new(if block_bodies { 0 } else { 10_000 }));
    let (arrived, arrivals) = mpsc::unbounded_channel();
    let records = Arc::clone(&traffic);
    let permits = Arc::clone(&bodies);
    let listener = tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let traffic = Arc::clone(&records);
            let bodies = Arc::clone(&permits);
            let arrived = arrived.clone();
            tokio::spawn(async move {
                let request: Value =
                    serde_json::from_str(&read_request_body(&mut socket).await.unwrap()).unwrap();
                let retry_log = {
                    let mut record = traffic.lock().unwrap();
                    record.active += 1;
                    record.peak = record.peak.max(record.active);
                    record.requests.push(request.clone());
                    let is_log = request["method"] == "eth_getLogs";
                    let retry = is_log && record.log_attempts == 0;
                    record.log_attempts += usize::from(is_log);
                    retry
                };
                let (status, response) = if retry_log {
                    ("429 Too Many Requests", json!({"error":"rate limit"}))
                } else if request.is_array() && reject_batches {
                    (
                        "200 OK",
                        json!({"error":{"code":-32600,"message":"batch unsupported"}}),
                    )
                } else {
                    let response = match request.as_array() {
                        // Deliberately reverse JSON-RPC response order.
                        Some(calls) => Value::Array(calls.iter().rev().map(respond).collect()),
                        None => respond(&request),
                    };
                    ("200 OK", response)
                };
                let payload = response.to_string();
                let headers = format!(
                    "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    payload.len()
                );
                socket.write_all(headers.as_bytes()).await.unwrap();
                arrived.send(()).unwrap();
                // Headers have arrived, but the permit must remain held for the body.
                bodies.acquire().await.unwrap().forget();
                socket.write_all(payload.as_bytes()).await.unwrap();
                traffic.lock().unwrap().active -= 1;
            });
        }
    });
    Ok(Endpoint {
        provider,
        traffic,
        bodies,
        arrivals,
        listener,
    })
}

fn respond(call: &Value) -> Value {
    let result = if call["method"] == "eth_getLogs" {
        json!([])
    } else {
        let number = i64::from_str_radix(
            call["params"][0].as_str().unwrap().trim_start_matches("0x"),
            16,
        )
        .unwrap();
        json!({
            "number": format!("0x{number:x}"),
            "hash": hash_of("block", number),
            "parentHash": hash_of("block", number - 1),
            "timestamp": "0x1",
        })
    };
    json!({"jsonrpc":"2.0", "id":call["id"], "result":result})
}

#[tokio::test]
async fn configured_batch_width_preserves_order_and_one_uses_standalone_requests() -> Result<()> {
    let numbers = [9, 3, 7, 1, 5, 8, 2];
    for width in [1, 3] {
        let node = endpoint(IngestConfig::new(256, width, 2)?, false, false).await?;
        let resolved = node.provider.resolve(&numbers).await?;
        assert_eq!(
            resolved
                .iter()
                .map(|block| block.number)
                .collect::<Vec<_>>(),
            numbers
        );
        assert!(
            resolved
                .iter()
                .all(|block| block.hash == hash_of("block", block.number))
        );
        let traffic = node.traffic.lock().unwrap();
        if width == 1 {
            assert_eq!(traffic.requests.len(), numbers.len());
            assert!(traffic.requests.iter().all(Value::is_object));
        } else {
            let mut sizes = traffic
                .requests
                .iter()
                .map(|request| request.as_array().unwrap().len())
                .collect::<Vec<_>>();
            sizes.sort_unstable();
            assert_eq!(sizes, vec![1, 3, 3]);
        }
    }
    Ok(())
}

#[tokio::test]
async fn cloned_providers_share_http_limit_through_body_reads_retries_and_batch_fallback()
-> Result<()> {
    let mut node = endpoint(IngestConfig::new(256, 3, 2)?, true, true).await?;
    let first = node.provider.clone();
    let second = node.provider.clone();
    let logs = node.provider.clone();
    let pending = tokio::spawn(async move {
        let topics = [WATCHED_TOPIC.to_owned()];
        tokio::try_join!(
            first.resolve(&[1, 2, 3, 4, 5, 6, 7, 8, 9]),
            second.resolve(&[10, 11, 12, 13, 14, 15, 16, 17, 18]),
            logs.range_logs(1, 18, &[], &topics, &[])
        )
    });
    for _ in 0..2 {
        tokio::time::timeout(Duration::from_secs(5), node.arrivals.recv())
            .await?
            .unwrap();
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(100), node.arrivals.recv())
            .await
            .is_err(),
        "a third request escaped while two response bodies were pending"
    );
    node.bodies.add_permits(100);
    let (first, second, logs) = tokio::time::timeout(Duration::from_secs(10), pending).await???;
    assert_eq!(first.len() + second.len(), 18);
    assert!(logs.is_empty());
    let traffic = node.traffic.lock().unwrap();
    assert_eq!(traffic.peak, 2);
    assert_eq!(traffic.log_attempts, 2, "transient request was retried");
    assert_eq!(
        traffic
            .requests
            .iter()
            .filter(|request| request.is_array())
            .count(),
        6
    );
    assert_eq!(
        traffic
            .requests
            .iter()
            .filter(|request| request["method"] == "eth_getBlockByNumber")
            .count(),
        18,
        "rejected batches fell back to standalone calls"
    );
    Ok(())
}

#[tokio::test]
async fn configured_parallelism_bounds_actual_window_log_queries() -> Result<()> {
    let node = endpoint(IngestConfig::new(256, 3, 2)?, false, false).await?;
    let provider = Arc::new(ChainProvider::JsonRpc(node.provider.clone()));
    assert_eq!(provider.query_parallelism(), 2);
    let resolved = [ResolvedBlock {
        number: 1,
        hash: hash_of("block", 1),
    }];
    let context = QueryContext {
        provider: &provider,
        resolved: &resolved,
        coinbase: None,
        prefetch: None,
    };
    let query = WatchQuery {
        from_block: 1,
        to_block: 1,
        addresses: vec![WATCHED_ADDRESS.into()],
        topic0s: vec![WATCHED_TOPIC.into()],
        topic1s: vec![],
    };
    let mut selected = BTreeMap::new();
    fetch_into(&context, &vec![query; 5], &mut selected).await?;
    assert!(selected.is_empty());
    let traffic = node.traffic.lock().unwrap();
    assert_eq!(traffic.log_attempts, 6);
    assert!(traffic.peak <= 2);
    Ok(())
}

fn hashes(label: &str, count: usize) -> Vec<String> {
    (0..count)
        .map(|index| hash_of(label, index as i64))
        .collect()
}

fn addresses(count: usize) -> Vec<String> {
    (0..count).map(|index| format!("0x{index:040x}")).collect()
}

#[tokio::test]
async fn no_topic0_issues_no_request() -> Result<()> {
    let node = endpoint(IngestConfig::default(), false, false).await?;
    let addresses = addresses(1_001);
    assert!(
        node.provider
            .range_logs(1, 18, &addresses, &[], &[])
            .await?
            .is_empty()
    );
    assert!(
        node.provider
            .verification_logs(1, 18, &addresses, &[], &hashes("topic1", 2))
            .await?
            .is_empty()
    );
    assert!(node.traffic.lock().unwrap().requests.is_empty());
    Ok(())
}

#[test]
fn positions_over_the_limit_multiply_in_nested_order() {
    let addresses = addresses(1_001);
    let topic0s = hashes("topic0", 501);
    let topic1s = hashes("topic1", 2);
    let parts = log_filter_split::split_filter(&addresses, &topic0s, &topic1s, 500);
    let shape = parts
        .iter()
        .map(|part| {
            (
                part.addresses[0].clone(),
                part.addresses.len(),
                part.topic0s[0].clone(),
                part.topic0s.len(),
                part.topic1s.len(),
            )
        })
        .collect::<Vec<_>>();
    let mut expected = Vec::new();
    for (address, address_count) in [(0, 500), (500, 500), (1_000, 1)] {
        for (topic0, topic0_count) in [(0, 500), (500, 1)] {
            expected.push((
                addresses[address].clone(),
                address_count,
                topic0s[topic0].clone(),
                topic0_count,
                2,
            ));
        }
    }
    assert_eq!(shape, expected);
}

#[test]
fn all_emitter_split_keeps_the_wildcard() -> Result<()> {
    let topic0s = hashes("topic0", 1_501);
    let parts = log_filter_split::split_filter(&[], &topic0s, &[], 500);
    assert_eq!(parts.len(), 4);
    for part in parts {
        assert!(part.addresses.is_empty());
        let filter = types::range_log_filter(1, 2, part.addresses, part.topic0s, part.topic1s)?;
        assert!(filter.get("address").is_none(), "{filter}");
        assert_eq!(
            filter["topics"][0].as_array().unwrap().len(),
            part.topic0s.len()
        );
        assert!(filter["topics"].get(1).is_none(), "{filter}");
    }
    Ok(())
}

#[test]
fn merge_collapses_duplicates_sorts_and_refuses_conflicts() {
    let log = |block_number: i64, log_index: i64, data: u8| Log {
        block_hash: hash_of("block", block_number),
        block_number,
        transaction_hash: hash_of("transaction", block_number),
        transaction_index: 0,
        log_index,
        address: WATCHED_ADDRESS.to_owned(),
        topics: vec![WATCHED_TOPIC.to_owned()],
        data: vec![data],
    };
    let merged = log_filter_split::merge_split_logs(vec![
        vec![log(2, 0, 0), log(3, 1, 0)],
        vec![log(1, 4, 0), log(2, 0, 0), log(3, 0, 0)],
    ])
    .unwrap();
    assert_eq!(
        merged
            .iter()
            .map(|log| (log.block_number, log.log_index))
            .collect::<Vec<_>>(),
        [(1, 4), (2, 0), (3, 0), (3, 1)]
    );
    let error = log_filter_split::merge_split_logs(vec![vec![log(2, 0, 0)], vec![log(2, 0, 1)]])
        .unwrap_err();
    assert!(
        error.to_string().contains("conflicting log identity"),
        "{error}"
    );
    assert!(!is_retryable(&error));
    assert_eq!(
        provider_error("failed to fetch selected chain logs", error).kind(),
        crate::ErrorKind::DataIntegrity
    );
}
