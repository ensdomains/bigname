use std::sync::Mutex;

use super::*;
use crate::{
    WatchQuery,
    engine::prefetch::{Prefetcher, RangeLogCache},
    test_chain::{
        Tamper, TestBlock, TestChain, TestEndpoint, TestLog, TestTransaction,
        WIDEST_FILTER_POSITION, hash_of, serve, serve_with_config,
    },
};

const FIRST: i64 = 2_000;
/// The position limit the split tests configure.
const LIMIT: usize = 2;
/// `address(LIMIT)`, the first address past the first split chunk.
const LIMITED_ADDRESS: &str = "0x0000000000000000000000000000000000000102";

fn address(index: usize) -> String {
    format!("0x{:040x}", 0x100 + index)
}

fn topic0(index: usize) -> String {
    hash_of("topic0", index as i64)
}

fn topic1(index: usize) -> String {
    hash_of("topic1", index as i64)
}

/// Two blocks in which each of seven emitters logs every combination of three `topic0`s and
/// four `topic1`s. Only the first three `topic1`s are ever queried.
fn split_chain() -> TestChain {
    let blocks = (FIRST..FIRST + 2)
        .map(|number| {
            let mut log_index = 0;
            let transactions = (0..7)
                .map(|emitter| {
                    let mut logs = Vec::new();
                    for topic0_index in 0..3 {
                        for topic1_index in 0..4 {
                            logs.push(TestLog {
                                log_index,
                                address: address(emitter),
                                topics: vec![topic0(topic0_index), topic1(topic1_index)],
                                data: format!("0x{log_index:064x}"),
                            });
                            log_index += 1;
                        }
                    }
                    TestTransaction {
                        hash: hash_of(&format!("transaction{emitter}"), number),
                        index: emitter as i64,
                        logs,
                    }
                })
                .collect();
            TestBlock {
                number,
                hash: hash_of("block", number),
                parent_hash: hash_of("block", number - 1),
                transactions,
            }
        })
        .collect();
    TestChain { blocks }
}

fn split_config() -> Result<IngestConfig> {
    Ok(IngestConfig::default().with_rpc_log_filter_position_limit(LIMIT)?)
}

fn position_cap() -> Tamper {
    Tamper::FilterPositionCap {
        limit: LIMIT,
        failures: 0,
        code: -32602,
    }
}

async fn logs_for(
    endpoint: &TestEndpoint,
    addresses: &[String],
    topic0s: &[String],
    topic1s: &[String],
) -> Result<Vec<Log>> {
    endpoint
        .provider
        .range_logs(
            &split_chain().resolved(),
            FIRST,
            FIRST + 1,
            addresses,
            topic0s,
            topic1s,
        )
        .await
}

fn entries(count: usize, entry: fn(usize) -> String) -> Vec<String> {
    (0..count).map(entry).collect()
}

#[tokio::test]
async fn split_requests_never_exceed_the_limit_in_any_position() -> Result<()> {
    let capped = serve_with_config(split_chain(), position_cap(), split_config()?).await?;
    let logs = logs_for(
        &capped,
        &entries(3 * LIMIT + 1, address),
        &entries(3, topic0),
        &entries(3, topic1),
    )
    .await?;
    assert_eq!(logs.len(), 2 * 7 * 3 * 3);
    assert_eq!(capped.counts.get("eth_getLogs"), 4 * 2 * 2);
    assert_eq!(capped.counts.get(WIDEST_FILTER_POSITION), LIMIT);
    Ok(())
}

#[tokio::test]
async fn split_results_equal_one_unsplit_query() -> Result<()> {
    let topic0s = entries(3, topic0);
    let topic1s = entries(3, topic1);
    for address_count in [LIMIT, LIMIT + 1, 3 * LIMIT + 1] {
        let addresses = entries(address_count, address);
        let capped = serve_with_config(split_chain(), position_cap(), split_config()?).await?;
        let clean = serve(split_chain(), Tamper::None).await?;
        let split = logs_for(&capped, &addresses, &topic0s, &topic1s).await?;
        let unsplit = logs_for(&clean, &addresses, &topic0s, &topic1s).await?;
        assert_eq!(clean.counts.get("eth_getLogs"), 1);
        assert_eq!(unsplit.len(), 2 * address_count * 3 * 3);
        assert_eq!(split, unsplit, "{address_count} addresses");
    }
    Ok(())
}

#[tokio::test]
async fn one_failing_split_query_fails_the_whole_lookup() -> Result<()> {
    let addresses = entries(LIMIT + 1, address);
    let topic0s = entries(1, topic0);
    let failing =
        serve_with_config(split_chain(), Tamper::RangeQueryInvalid(2), split_config()?).await?;
    let error = logs_for(&failing, &addresses, &topic0s, &[])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("invalid argument"), "{error}");
    assert_eq!(failing.counts.get("eth_getLogs"), 2);

    // The prefetch stores a range only after every split query answered.
    let failing =
        serve_with_config(split_chain(), Tamper::RangeQueryInvalid(2), split_config()?).await?;
    let provider = Arc::new(failing.provider);
    let cache = tokio::sync::Mutex::new(RangeLogCache::default());
    let prefetch = Prefetcher::new(&cache, "provider".into(), FIRST + 9_999);
    let resolved = split_chain().resolved();
    let query = WatchQuery {
        from_block: FIRST,
        to_block: FIRST + 1,
        addresses,
        topic0s,
        topic1s: Vec::new(),
    };
    let prefetched = || prefetch.logs(&provider, &resolved, &query, FIRST, FIRST + 1);
    assert!(prefetched().await.is_err());
    assert_eq!(failing.counts.get("eth_getLogs"), 2);
    let logs = prefetched().await?;
    assert_eq!(logs.len(), 2 * (LIMIT + 1) * 4);
    assert_eq!(
        failing.counts.get("eth_getLogs"),
        4,
        "nothing from the failed lookup was cached"
    );
    assert_eq!(prefetched().await?, logs);
    assert_eq!(
        failing.counts.get("eth_getLogs"),
        4,
        "the full answer was cached"
    );
    Ok(())
}

#[tokio::test]
async fn split_composes_with_block_range_halving() -> Result<()> {
    let addresses = entries(LIMIT + 1, address);
    assert_eq!(addresses[LIMIT], LIMITED_ADDRESS);
    let topic0s = entries(LIMIT, topic0);
    let halving = serve_with_config(
        split_chain(),
        Tamper::RangeLimitedFor(LIMITED_ADDRESS),
        split_config()?,
    )
    .await?;
    let clean = serve(split_chain(), Tamper::None).await?;
    let split = logs_for(&halving, &addresses, &topic0s, &[]).await?;
    let unsplit = logs_for(&clean, &addresses, &topic0s, &[]).await?;
    assert_eq!(unsplit.len(), 2 * (LIMIT + 1) * LIMIT * 4);
    assert_eq!(split, unsplit);
    assert_eq!(
        halving.counts.get("eth_getLogs"),
        4,
        "two parts, one refused and halved"
    );
    assert_eq!(
        halving
            .counts
            .get(&format!("eth_getLogs({LIMITED_ADDRESS})")),
        2
    );
    for unlimited in &addresses[..LIMIT] {
        assert_eq!(halving.counts.get(&format!("eth_getLogs({unlimited})")), 1);
    }
    Ok(())
}

#[tokio::test]
async fn split_logs_counts_without_endpoint_or_entries() -> Result<()> {
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
        .with_env_filter(tracing_subscriber::EnvFilter::new("bigname_ingest=debug"))
        .with_writer({
            let logs = logs.clone();
            move || logs.clone()
        })
        .finish();
    let _logs = tracing::subscriber::set_default(subscriber);

    let node = serve(split_chain(), Tamper::None).await?;
    let provider =
        JsonRpcProvider::with_config(&format!("{}v1/secret-key", node.endpoint), split_config()?)?;
    let addresses = entries(3 * LIMIT + 1, address);
    let topic0s = entries(1, topic0);
    provider
        .range_logs(FIRST, FIRST + 1, &addresses, &topic0s, &[])
        .await?;
    let logged = String::from_utf8(logs.0.lock().unwrap().clone())?;
    assert!(
        logged.contains("address_entries=7 address_queries=4"),
        "{logged}"
    );
    assert!(logged.contains(" queries=4"), "{logged}");
    assert!(!logged.contains("secret-key"), "{logged}");
    for entry in addresses.iter().chain(&topic0s) {
        assert!(!logged.contains(&entry[2..]), "{logged}");
    }
    Ok(())
}

#[tokio::test]
async fn verification_logs_split_at_the_default_limit() -> Result<()> {
    let node = serve(split_chain(), Tamper::None).await?;
    let provider = JsonRpcProvider::new(&node.endpoint)?;
    let addresses = entries(1_001, address);
    let logs = provider
        .verification_logs(FIRST, FIRST + 1, &addresses, &entries(3, topic0), &[])
        .await?;
    assert_eq!(logs.len(), 2 * 7 * 3 * 4);
    assert_eq!(node.counts.get("eth_getLogs"), 3);
    Ok(())
}
