use std::sync::Arc;

use anyhow::Result;
use tokio::sync::Mutex;

use super::*;
use crate::{
    engine::prefetch::RangeLogCache,
    test_chain::{
        NOISE_ADDRESS, NOISE_TOPIC, Tamper, TestBlock, TestChain, TestLog, TestTransaction,
        WATCHED_ADDRESS, WATCHED_TOPIC, hash_of, serve,
    },
};

const FIRST: i64 = 1_000;

fn filter(to: i64) -> WatchFilter {
    WatchFilter::watching(WATCHED_ADDRESS, FIRST, to, &[WATCHED_TOPIC.to_owned()])
}

#[tokio::test]
async fn poisoned_prefetch_is_discarded_and_fresh_window_matches_clean_facts() -> Result<()> {
    let chain = TestChain::synthetic(FIRST, 512, 4);
    let bad = serve(chain.clone(), Tamper::RangeLogIndexDiffers(1)).await?;
    let good = serve(chain, Tamper::None).await?;
    let provider = Arc::new(bad.provider);
    let clean_provider = Arc::new(good.provider);
    let cache = Mutex::new(RangeLogCache::default());
    let prefetch = Prefetcher::new(&cache, "provider".into(), FIRST + 9_999);
    let first_filter = filter(FIRST + 255);
    let reader = WindowReader {
        provider: &provider,
        coinbase: None,
        prefetch: Some(&prefetch),
        filter: &first_filter,
    };
    // Populate the same cache used by production before loading the failed window.
    let resolved = provider
        .resolve(&(FIRST..FIRST + 256).collect::<Vec<_>>())
        .await?;
    prefetch
        .logs(
            &provider,
            &resolved,
            &first_filter.queries()[0],
            FIRST,
            FIRST + 255,
        )
        .await?;
    let actual = reader.fetch(FIRST, FIRST + 255).await?;
    let clean = WindowReader {
        provider: &clean_provider,
        coinbase: None,
        prefetch: None,
        filter: &first_filter,
    }
    .fetch(FIRST, FIRST + 255)
    .await?;
    assert_eq!(actual.facts.blocks, clean.facts.blocks);
    assert_eq!(actual.facts.transactions, clean.facts.transactions);
    assert_eq!(actual.facts.receipts, clean.facts.receipts);
    assert_eq!(actual.facts.logs, clean.facts.logs);
    assert_eq!(bad.counts.get("wide_logs"), 1);
    assert_eq!(bad.counts.get("window_logs"), 1);
    assert_eq!(bad.counts.get("eth_getTransactionReceipt"), 128);
    // The rejected wide response also contained bad logs for the following window.
    let second_filter = WatchFilter::watching(
        WATCHED_ADDRESS,
        FIRST + 256,
        FIRST + 511,
        &[WATCHED_TOPIC.to_owned()],
    );
    WindowReader {
        filter: &second_filter,
        ..reader
    }
    .fetch(FIRST + 256, FIRST + 511)
    .await?;
    assert_eq!(
        bad.counts.get("wide_logs"),
        2,
        "the poisoned range must not survive"
    );
    assert_eq!(
        bad.counts.get("window_logs"),
        1,
        "the next window should need no recovery"
    );
    Ok(())
}

#[tokio::test]
async fn second_refetch_can_recover_but_persistent_disagreement_is_terminal() -> Result<()> {
    for (tamper, succeeds) in [
        (Tamper::RangeLogIndexDiffers(2), true),
        (Tamper::RangeLogIndexDiffers(usize::MAX), false),
    ] {
        let endpoint = serve(TestChain::synthetic(FIRST, 1, 1), tamper).await?;
        let provider = Arc::new(endpoint.provider);
        let filter = filter(FIRST);
        let result = WindowReader {
            provider: &provider,
            coinbase: None,
            prefetch: None,
            filter: &filter,
        }
        .fetch(FIRST, FIRST)
        .await;
        if succeeds {
            assert_eq!(result?.facts.logs.len(), 3);
        } else {
            let error = result
                .err()
                .expect("no validated facts may escape a failed window");
            assert_eq!(error.kind(), ErrorKind::DataIntegrity);
            assert!(
                error.to_string().contains("omitted selected log"),
                "{error}"
            );
        }
        assert_eq!(endpoint.counts.get("eth_getLogs"), 3);
        assert_eq!(endpoint.counts.get("eth_getBlockByNumber"), 6);
        assert_eq!(endpoint.counts.get("eth_getTransactionReceipt"), 3);
        assert_eq!(endpoint.counts.get("eth_getTransactionByHash"), 3);
    }
    Ok(())
}

#[tokio::test]
async fn refetch_does_not_relax_other_integrity_checks_or_retry_reorg_errors() -> Result<()> {
    for (tamper, attempts, kind) in [
        (
            Tamper::ReceiptLogDiffers(FIRST),
            3,
            ErrorKind::DataIntegrity,
        ),
        (
            Tamper::RangeQueryDropsWatchedLog(FIRST),
            3,
            ErrorKind::DataIntegrity,
        ),
        (
            Tamper::BloomOmitsWatchedAddress(FIRST),
            3,
            ErrorKind::DataIntegrity,
        ),
        (
            Tamper::ReceiptBlockHashMoved(FIRST),
            1,
            ErrorKind::Transient,
        ),
    ] {
        let endpoint = serve(TestChain::synthetic(FIRST, 1, 1), tamper).await?;
        let provider = Arc::new(endpoint.provider);
        let filter = filter(FIRST);
        let error = WindowReader {
            provider: &provider,
            coinbase: None,
            prefetch: None,
            filter: &filter,
        }
        .fetch(FIRST, FIRST)
        .await
        .err()
        .expect("tampered facts must fail validation");
        assert_eq!(error.kind(), kind);
        assert_eq!(endpoint.counts.get("eth_getLogs"), attempts);
    }
    Ok(())
}

/// One block in which the watched address announces itself and then emits a scoped log,
/// while the noise address emits the same scoped topic without ever announcing.
fn announcing_chain() -> TestChain {
    let log = |log_index, address: &str, topic: &str| TestLog {
        log_index,
        address: address.to_owned(),
        topics: vec![topic.to_owned()],
        data: "0x00".to_owned(),
    };
    TestChain {
        blocks: vec![TestBlock {
            number: FIRST,
            hash: hash_of("block", FIRST),
            parent_hash: hash_of("block", FIRST - 1),
            transactions: vec![
                TestTransaction {
                    hash: hash_of("announce", FIRST),
                    index: 0,
                    logs: vec![
                        log(0, WATCHED_ADDRESS, WATCHED_TOPIC),
                        log(1, WATCHED_ADDRESS, NOISE_TOPIC),
                    ],
                },
                TestTransaction {
                    hash: hash_of("unannounced", FIRST),
                    index: 1,
                    logs: vec![log(2, NOISE_ADDRESS, NOISE_TOPIC)],
                },
            ],
        }],
    }
}

#[tokio::test]
async fn rejected_attempt_discovery_admission_does_not_survive_the_refetch() -> Result<()> {
    // `WATCHED_TOPIC` stands in for the registry announcement and `NOISE_TOPIC` for the
    // events an announced emitter is then watched for.
    let filter = WatchFilter::watching_registry_announcements(
        FIRST,
        FIRST,
        WATCHED_TOPIC,
        &[NOISE_TOPIC.to_owned()],
    );
    // The first range read attributes the announcement to the noise address, so the first
    // attempt admits the noise address and reads its scoped logs before it is rejected.
    let bad = serve(announcing_chain(), Tamper::RangeLogEmitterDiffers(1)).await?;
    let good = serve(announcing_chain(), Tamper::None).await?;
    let provider = Arc::new(bad.provider);
    let clean_provider = Arc::new(good.provider);
    let actual = WindowReader {
        provider: &provider,
        coinbase: None,
        prefetch: None,
        filter: &filter,
    }
    .fetch(FIRST, FIRST)
    .await?;
    let clean = WindowReader {
        provider: &clean_provider,
        coinbase: None,
        prefetch: None,
        filter: &filter,
    }
    .fetch(FIRST, FIRST)
    .await?;

    assert_eq!(
        bad.counts.get("eth_getLogs"),
        4,
        "two attempts, two reads each"
    );
    assert_eq!(
        bad.counts.get(&format!("eth_getLogs({NOISE_ADDRESS})")),
        1,
        "the rejected attempt admitted the noise address"
    );
    assert_eq!(
        bad.counts.get(&format!("eth_getLogs({WATCHED_ADDRESS})")),
        1,
        "the successful attempt admitted the watched address"
    );
    let admitted = actual
        .queries
        .iter()
        .flat_map(|query| query.addresses.iter().map(String::as_str))
        .collect::<Vec<_>>();
    assert_eq!(admitted, [WATCHED_ADDRESS]);
    assert_eq!(actual.queries, clean.queries);
    assert_eq!(actual.selected, clean.selected);
    assert_eq!(actual.facts.transactions, clean.facts.transactions);
    assert_eq!(actual.facts.receipts, clean.facts.receipts);
    assert_eq!(actual.facts.logs, clean.facts.logs);
    assert_eq!(actual.facts.logs.len(), 2);
    assert!(
        actual
            .facts
            .logs
            .iter()
            .all(|log| log.address == WATCHED_ADDRESS),
        "only the watched address announced itself: {:?}",
        actual.facts.logs
    );
    assert!(
        filter
            .queries()
            .iter()
            .all(|query| query.addresses.is_empty())
    );
    Ok(())
}

#[tokio::test]
async fn resolver_creation_fetches_same_transaction_and_later_records_before_returning()
-> Result<()> {
    let creation = bigname_manifests::resolver_creation_topic0();
    let mut chain = TestChain::synthetic(FIRST, 2, 1);
    let first = &mut chain.blocks[0].transactions[1];
    first.logs.insert(
        0,
        TestLog {
            log_index: 1,
            address: WATCHED_ADDRESS.to_owned(),
            topics: vec![creation.clone()],
            data: "0x".to_owned(),
        },
    );
    for (index, log) in first.logs.iter_mut().enumerate() {
        log.log_index = index as i64 + 1;
    }
    let endpoint = serve(chain, Tamper::None).await?;
    let provider = Arc::new(endpoint.provider);
    let filter = WatchFilter::watching_creation(
        FIRST,
        FIRST + 1,
        creation.clone(),
        vec![WATCHED_TOPIC.to_owned()],
    );
    let result = WindowReader {
        provider: &provider,
        coinbase: None,
        prefetch: None,
        filter: &filter,
    }
    .fetch(FIRST, FIRST + 1)
    .await?;
    let selected = result
        .selected
        .iter()
        .map(|log| (log.block_number, log.log_index, log.topics[0].clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        selected,
        vec![
            (FIRST, 1, creation),
            (FIRST, 2, WATCHED_TOPIC.to_owned()),
            (FIRST, 4, WATCHED_TOPIC.to_owned()),
            (FIRST + 1, 1, WATCHED_TOPIC.to_owned()),
            (FIRST + 1, 3, WATCHED_TOPIC.to_owned()),
        ]
    );
    assert!(
        !result
            .selected
            .iter()
            .any(|log| log.address == NOISE_ADDRESS)
    );
    assert_eq!(
        result.queries.len(),
        2,
        "creation scan plus scoped capture in the same window"
    );
    Ok(())
}

#[tokio::test]
async fn verification_expands_from_resolver_creation_the_same_way_indexing_does() -> Result<()> {
    let creation = bigname_manifests::resolver_creation_topic0();
    let mut chain = TestChain::synthetic(FIRST, 2, 1);
    let first = &mut chain.blocks[0].transactions[1];
    first.logs.insert(
        0,
        TestLog {
            log_index: 1,
            address: WATCHED_ADDRESS.to_owned(),
            topics: vec![creation.clone()],
            data: "0x".to_owned(),
        },
    );
    for (index, log) in first.logs.iter_mut().enumerate() {
        log.log_index = index as i64 + 1;
    }
    // Neither side starts with the resolver address: both must learn it from the creation log.
    let filter = WatchFilter::watching_creation(
        FIRST,
        FIRST + 1,
        creation.clone(),
        vec![WATCHED_TOPIC.to_owned()],
    );
    assert!(
        filter
            .queries()
            .iter()
            .all(|query| query.addresses.is_empty())
    );

    let indexing = serve(chain.clone(), Tamper::None).await?;
    let indexing_provider = Arc::new(indexing.provider);
    let indexed = WindowReader {
        provider: &indexing_provider,
        coinbase: None,
        prefetch: None,
        filter: &filter,
    }
    .fetch(FIRST, FIRST + 1)
    .await?
    .selected
    .into_iter()
    .map(|log| (log.block_number, log.log_index, log.address, log.topics))
    .collect::<Vec<_>>();

    let reference = serve(chain, Tamper::None).await?;
    let verified = crate::verification::VerificationProvider::from_provider(reference.provider)
        .fetch(filter.clone(), FIRST, FIRST + 1)
        .await?
        .logs
        .into_iter()
        .map(|log| (log.block_number, log.log_index, log.address, log.topics))
        .collect::<Vec<_>>();

    assert_eq!(verified, indexed);
    assert_eq!(
        verified
            .iter()
            .map(|(block, index, _, topics)| (*block, *index, topics[0].clone()))
            .collect::<Vec<_>>(),
        vec![
            (FIRST, 1, creation),
            (FIRST, 2, WATCHED_TOPIC.to_owned()),
            (FIRST, 4, WATCHED_TOPIC.to_owned()),
            (FIRST + 1, 1, WATCHED_TOPIC.to_owned()),
            (FIRST + 1, 3, WATCHED_TOPIC.to_owned()),
        ]
    );
    assert!(
        !verified
            .iter()
            .any(|(_, _, address, _)| address == NOISE_ADDRESS)
    );
    Ok(())
}
