use super::*;

fn item() -> VerificationLog {
    VerificationLog {
        block_hash: "b".into(),
        block_number: 1,
        transaction_hash: "t".into(),
        transaction_index: 0,
        log_index: 0,
        address: "a".into(),
        topics: vec!["topic".into()],
        data: vec![1, 2, 3],
    }
}

#[test]
fn logical_content_is_distinct_from_capacity() {
    let mut value = item();
    let expected = 24 + 1 + 1 + 1 + 5 + 3;
    assert_eq!(value.footprint().bytes, expected);
    value.data.reserve(1024);
    assert_eq!(value.footprint().bytes, expected);
    assert!(value.footprint().owned > expected);
    let mut values = Vec::with_capacity(8);
    values.push(value);
    assert_eq!(
        values.footprint().owned,
        8 * size_of::<VerificationLog>() as u64 + values[0].footprint().owned
    );
    assert_eq!(values.footprint().rows, 1);
}

#[tokio::test]
async fn duplicate_replacement_counts_content_once() {
    let session = Session::new("replacement").unwrap();
    session
        .scope(async {
            let mut value = item();
            value.data.reserve(4096);
            let rows = vec![value, item()];
            let mut identity = Identity::default();
            identity.query(rows.footprint(), inline(&rows).owned);
            let mut map = std::collections::BTreeMap::new();
            for value in rows {
                let key = (String::from("b"), 0);
                identity.consume(&value);
                identity.inserted(map.insert(key.clone(), value.clone()), &map, &key);
                assert_eq!(
                    identity.retained.owned,
                    map[&key].footprint().owned
                        + map.first_key_value().unwrap().0.0.capacity() as u64
                );
            }
            assert_eq!(identity.retained.rows, 1);
            assert_eq!(identity.retained.bytes, item().footprint().bytes + 9);
            assert_eq!(identity.duplicates, 1);
            assert_eq!(identity.remaining.bytes, 0);
            assert_eq!(identity.remaining.owned, 0);
            assert!(identity.peak_owned >= 4096);
            assert!(identity.clone_overlap > identity.retained.bytes);
        })
        .await;
    assert!(session.valid());
}

#[tokio::test]
async fn overflow_is_sticky_and_does_not_fail_the_application() {
    let session = Session::new("overflow").unwrap();
    let result = session
        .scope(async {
            assert_eq!(sum(u64::MAX, 1), u64::MAX);
            assert_eq!(mul(u64::MAX, 2), u64::MAX);
            observe("later", Footprint::default);
            42
        })
        .await;
    assert_eq!(result, 42);
    assert!(!session.valid());
}

#[tokio::test]
async fn attempts_and_query_totals_survive_retries() {
    let session = Session::new("attempts").unwrap();
    session
        .scope(async {
            for _ in 0..2 {
                attempt("ethereum-sepolia", 0, 5, async {
                    returned("provider", "ordinary", 0, 0, 5, || vec![item()].footprint());
                    returned("provider", "supplemental", 0, 0, 5, || {
                        vec![item()].footprint()
                    });
                })
                .await;
            }
        })
        .await;
    assert_eq!(session.run.attempts.load(Ordering::Relaxed), 2);
    assert_eq!(session.run.provider_rows.load(Ordering::Relaxed), 4);
    assert_eq!(
        session.run.provider_bytes.load(Ordering::Relaxed),
        4 * item().footprint().bytes
    );
}

#[tokio::test]
async fn blocking_context_is_explicit_and_nested_context_is_restored() {
    let first = Session::new("first").unwrap();
    let second = Session::new("second").unwrap();
    first
        .scope(async {
            query("supplemental", 3, 12, 15, async {
                let context = capture();
                let second_context = second.root();
                tokio::task::spawn_blocking(move || {
                    blocking(context, || {
                        let before = capture().unwrap();
                        assert_eq!(before.kind, "supplemental");
                        assert_eq!(before.ordinal, 3);
                        assert_eq!(before.from, 12);
                        blocking(Some(second_context), || {
                            assert_eq!(&*capture().unwrap().session.id, "second")
                        });
                        assert_eq!(&*capture().unwrap().session.id, "first");
                        transport(17);
                    })
                })
                .await
                .unwrap();
            })
            .await;
        })
        .await;
    assert_eq!(first.run.transport_bytes.load(Ordering::Relaxed), 17);
    assert_eq!(second.run.transport_bytes.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn concurrent_sessions_do_not_share_counters() {
    let first = Session::new("one").unwrap();
    let second = Session::new("two").unwrap();
    tokio::join!(
        first.scope(async {
            transport(11);
            tokio::task::yield_now().await;
            transport(13);
        }),
        second.scope(async {
            transport(19);
        })
    );
    assert_eq!(first.run.transport_bytes.load(Ordering::Relaxed), 24);
    assert_eq!(second.run.transport_bytes.load(Ordering::Relaxed), 19);
}

#[test]
fn diagnostic_identifiers_are_bounded() {
    assert!(Session::new("").is_none());
    assert!(Session::new(&"x".repeat(65)).is_none());
    assert!(Session::new("new\nline").is_none());
}

#[tokio::test]
async fn selected_facts_really_collect_transaction_associated_unselected_logs() {
    use crate::provider::ChainProvider;
    use serde_json::{Value, json};
    use std::{
        io::{Read, Write},
        net::TcpListener,
        time::Duration,
    };
    fn hash(n: i64) -> String {
        format!("0x{n:064x}")
    }
    fn response(call: &Value) -> Value {
        let id = call["id"].clone();
        let method = call["method"].as_str().unwrap();
        let hash_value = if method == "eth_getLogs" {
            &call["params"][0]["blockHash"]
        } else {
            &call["params"][0]
        };
        let number =
            i64::from_str_radix(hash_value.as_str().unwrap().trim_start_matches("0x"), 16).unwrap();
        let transaction = hash(number + 100);
        let result = match method {
            "eth_getBlockByHash" => json!({"hash":hash(number),"parentHash":hash(number-1),"number":format!("0x{number:x}"),"timestamp":"0x1","transactions":[{"hash":transaction,"blockHash":hash(number),"blockNumber":format!("0x{number:x}"),"transactionIndex":"0x0","from":"0x0000000000000000000000000000000000000001","to":null,"input":"0x","value":"0x0"}]}),
            "eth_getLogs" => json!((0..2).map(|index| json!({"blockHash":hash(number),"blockNumber":format!("0x{number:x}"),"transactionHash":transaction,"transactionIndex":"0x0","logIndex":format!("0x{index:x}"),"address":"0x0000000000000000000000000000000000000001","topics":[],"data":"0x01"})).collect::<Vec<_>>()),
            "eth_getBlockReceipts" => json!([{"transactionHash":transaction,"blockHash":hash(number),"blockNumber":format!("0x{number:x}"),"transactionIndex":"0x0","contractAddress":null,"status":"0x1"}]),
            _ => panic!("unexpected method {method}"),
        };
        json!({"jsonrpc":"2.0","id":id,"result":result})
    }
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    listener.set_nonblocking(true).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let stopped = stop.clone();
    let server = std::thread::spawn(move || {
        let end = Instant::now() + Duration::from_secs(20);
        let mut first = true;
        while !stopped.load(Ordering::Relaxed) && Instant::now() < end {
            let Ok((mut stream, _)) = listener.accept() else {
                std::thread::sleep(Duration::from_millis(5));
                continue;
            };
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut data = Vec::new();
            let (header_end, length) = loop {
                let mut byte = [0u8];
                stream.read_exact(&mut byte).unwrap();
                data.push(byte[0]);
                if data.ends_with(b"\r\n\r\n") {
                    let text = String::from_utf8_lossy(&data).to_ascii_lowercase();
                    let length: usize = text
                        .lines()
                        .find_map(|line| line.strip_prefix("content-length:"))
                        .unwrap()
                        .trim()
                        .parse()
                        .unwrap();
                    break (data.len(), length);
                }
                assert!(data.len() < 8192);
            };
            data.resize(header_end + length, 0);
            stream.read_exact(&mut data[header_end..]).unwrap();
            let request: Value = serde_json::from_slice(&data[header_end..]).unwrap();
            if first {
                first = false;
                write!(stream, "HTTP/1.1 503 Unavailable\r\nContent-Length: 5\r\nConnection: close\r\n\r\nretry").unwrap();
                continue;
            }
            let reply = if let Some(batch) = request.as_array() {
                Value::Array(batch.iter().map(response).collect())
            } else {
                response(&request)
            }
            .to_string();
            write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}", reply.len()).unwrap();
        }
    });
    let provider = ChainProvider::new("ethereum-sepolia", "rpc", &endpoint).unwrap();
    let resolved = (1..=2)
        .map(|number| ResolvedBlock {
            number,
            hash: hash(number),
        })
        .collect::<Vec<_>>();
    let selected = (1..=2)
        .map(|number| Log {
            block_hash: hash(number),
            block_number: number,
            transaction_hash: hash(number + 100),
            transaction_index: 0,
            log_index: 0,
            address: "0x0000000000000000000000000000000000000001".into(),
            topics: vec![],
            data: vec![1],
        })
        .collect();
    let session = Session::new("selected-facts").unwrap();
    let result = session
        .scope(crate::fetching::fetch_selected_facts(
            &provider, &resolved, selected,
        ))
        .await;
    stop.store(true, Ordering::Relaxed);
    server.join().unwrap();
    let facts = result.unwrap();
    assert_eq!(session.run.failed_body_bytes.load(Ordering::Relaxed), 5);
    assert!(session.run.successful_body_bytes.load(Ordering::Relaxed) > 0);
    assert_eq!(
        (
            facts.blocks.len(),
            facts.transactions.len(),
            facts.receipts.len(),
            facts.logs.len()
        ),
        (2, 2, 2, 4)
    );
    assert!(session.valid());
    assert!(session.run.sequence.load(Ordering::Relaxed) >= 4);
}

#[test]
fn disabled_observations_never_evaluate_payload_walks() {
    observe_context(None, "disabled", || {
        panic!("disabled accounting walked payload")
    });
}

#[tokio::test]
async fn transport_connect_failure_has_attempts_and_unknown_body_bytes() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let provider =
        crate::provider::ChainProvider::new("ethereum-sepolia", "rpc", &endpoint).unwrap();
    let session = Session::new("failed-transport").unwrap();
    assert!(session.scope(provider.resolve(&[1])).await.is_err());
    assert!(session.run.transport_attempts.load(Ordering::Relaxed) > 0);
    assert_eq!(session.run.failed_body_bytes.load(Ordering::Relaxed), 0);
    assert_eq!(session.run.successful_body_bytes.load(Ordering::Relaxed), 0);
    assert!(session.valid());
}
