use std::{collections::BTreeMap, sync::Arc};

use anyhow::Result as AnyResult;
use tokio::sync::Mutex;

use super::{FetchedBatch, fetch_selected_bundles, fetch_selected_facts};
use crate::{
    ErrorKind, Result,
    engine::{
        prefetch::{PREFETCH_RANGE_BLOCKS, Prefetcher, RangeLogCache},
        query::{self, QueryContext},
    },
    manifest::{WatchFilter, WatchQuery},
    provider::{ChainProvider, Log, ResolvedBlock, SharedProvider},
    test_chain::{
        BLOCK_BODY, BLOCK_RECEIPTS, EXACT_BLOCK_LOGS, Tamper, TestChain, WATCHED_ADDRESS,
        WATCHED_TOPIC, serve,
    },
};

const FIRST_BLOCK: i64 = 1_000;
const WINDOW_BLOCKS: i64 = 256;

fn watch_filter(to_block: i64) -> WatchFilter {
    WatchFilter::watching(
        WATCHED_ADDRESS,
        FIRST_BLOCK,
        to_block,
        &[WATCHED_TOPIC.to_owned()],
    )
}

/// A persisted watch query as `load_watch_filter` hands it over: already clipped to the
/// window that loaded it, which is why the prefetch cannot take its bounds at face value.
fn watch_query(from_block: i64, to_block: i64) -> WatchQuery {
    WatchQuery {
        from_block: from_block.max(FIRST_BLOCK),
        to_block,
        addresses: vec![WATCHED_ADDRESS.to_owned()],
        topic0s: vec![WATCHED_TOPIC.to_owned()],
        topic1s: Vec::new(),
    }
}

/// Everything one ingest window asks of its provider, in the order `load_window` asks it.
///
/// The engine's own window loader adds the database reads and the write; this keeps the
/// provider traffic identical so a request count here is a request count in production.
async fn run_window(
    provider: &SharedProvider,
    cache: &Mutex<RangeLogCache>,
    from: i64,
    to: i64,
    ceiling: Option<i64>,
) -> Result<FetchedBatch> {
    let numbers = (from..=to).collect::<Vec<_>>();
    let resolved = provider
        .resolve(&numbers)
        .await
        .map_err(|error| crate::provider::provider_error("resolve failed", error))?;
    let filter = watch_filter(to);
    let queries = vec![watch_query(from, to)];
    let prefetcher =
        ceiling.map(|ceiling| Prefetcher::new(cache, "test-provider".to_owned(), ceiling));
    let context = QueryContext {
        provider,
        resolved: &resolved,
        coinbase: None,
        prefetch: prefetcher.as_ref(),
    };
    let mut selected_by_identity = BTreeMap::new();
    query::fetch_into(&context, &queries, &mut selected_by_identity).await?;
    let selected = selected_by_identity.into_values().collect::<Vec<_>>();
    fetch_selected_facts(provider, &resolved, selected, &filter).await
}

/// Selected logs for a window, as the range query reports them.
async fn selected_logs(
    provider: &SharedProvider,
    resolved: &[ResolvedBlock],
    to: i64,
) -> Result<Vec<Log>> {
    let context = QueryContext {
        provider,
        resolved,
        coinbase: None,
        prefetch: None,
    };
    let mut selected = BTreeMap::new();
    query::fetch_into(&context, &[watch_query(FIRST_BLOCK, to)], &mut selected).await?;
    Ok(selected.into_values().collect())
}

fn shared(provider: ChainProvider) -> SharedProvider {
    Arc::new(provider)
}

#[tokio::test]
async fn the_per_transaction_path_stores_what_the_block_bundle_path_stored() -> AnyResult<()> {
    let chain = TestChain::synthetic(FIRST_BLOCK, WINDOW_BLOCKS, 4);
    let endpoint = serve(chain.clone(), Tamper::None).await?;
    let provider = shared(endpoint.provider);
    let resolved = chain.resolved();
    let to = FIRST_BLOCK + WINDOW_BLOCKS - 1;
    let selected = selected_logs(&provider, &resolved, to).await?;
    assert!(!selected.is_empty(), "the fixture must select logs");

    let blocks = provider.headers(&resolved).await?;
    let reference = fetch_selected_bundles(&provider, &resolved, selected.clone())
        .await?
        .into_batch(blocks.clone());
    let actual = fetch_selected_facts(&provider, &resolved, selected, &watch_filter(to)).await?;

    assert_eq!(actual.blocks, reference.blocks);
    assert_eq!(actual.transactions, reference.transactions);
    assert_eq!(actual.receipts, reference.receipts);
    assert_eq!(actual.logs, reference.logs);
    assert_eq!(actual.logs.len(), 3 * (WINDOW_BLOCKS as usize / 4));
    Ok(())
}

#[tokio::test]
async fn a_window_never_asks_for_a_block_body_receipts_or_exact_block_logs() -> AnyResult<()> {
    let chain = TestChain::synthetic(FIRST_BLOCK, WINDOW_BLOCKS, 4);
    let endpoint = serve(chain, Tamper::None).await?;
    let provider = shared(endpoint.provider);
    let cache = Mutex::new(RangeLogCache::default());
    let to = FIRST_BLOCK + WINDOW_BLOCKS - 1;

    let batch = run_window(&provider, &cache, FIRST_BLOCK, to, None).await?;

    assert_eq!(batch.blocks.len(), WINDOW_BLOCKS as usize);
    assert_eq!(endpoint.counts.get(BLOCK_BODY), 0);
    assert_eq!(endpoint.counts.get(BLOCK_RECEIPTS), 0);
    assert_eq!(endpoint.counts.get(EXACT_BLOCK_LOGS), 0);
    let selected_transactions = WINDOW_BLOCKS as usize / 4;
    assert_eq!(
        endpoint.counts.get("eth_getTransactionReceipt"),
        selected_transactions
    );
    assert_eq!(
        endpoint.counts.get("eth_getTransactionByHash"),
        selected_transactions
    );
    Ok(())
}

#[tokio::test]
async fn an_empty_range_reorg_is_retried_before_storing_old_headers() -> AnyResult<()> {
    let endpoint = serve(
        TestChain::synthetic(FIRST_BLOCK, 1, 1),
        Tamper::EmptyRangeAfterReorg(FIRST_BLOCK),
    )
    .await?;
    let provider = shared(endpoint.provider);
    let cache = Mutex::new(RangeLogCache::default());
    let result = run_window(&provider, &cache, FIRST_BLOCK, FIRST_BLOCK, None).await;
    assert!(
        result.is_err(),
        "old block with missing watched logs must not be accepted"
    );
    assert_eq!(result.unwrap_err().kind(), ErrorKind::Transient);
    assert_eq!(endpoint.counts.get("eth_getLogs"), 1);
    Ok(())
}

#[tokio::test]
async fn a_window_rechecks_every_block_while_loading_its_headers() -> AnyResult<()> {
    let chain = TestChain::synthetic(FIRST_BLOCK, WINDOW_BLOCKS, 4);
    let endpoint = serve(chain, Tamper::None).await?;
    let provider = shared(endpoint.provider);
    let cache = Mutex::new(RangeLogCache::default());
    let to = FIRST_BLOCK + WINDOW_BLOCKS - 1;

    run_window(&provider, &cache, FIRST_BLOCK, to, None).await?;

    assert_eq!(
        endpoint.counts.get("eth_getBlockByNumber"),
        (WINDOW_BLOCKS * 2) as usize,
        "one initial resolve plus one header lookup per block; no separate recheck"
    );
    assert_eq!(endpoint.counts.get("eth_getLogs"), 1);
    assert_eq!(endpoint.counts.get("eth_getBlockByHash"), 0);
    Ok(())
}

#[tokio::test]
async fn one_wide_range_query_answers_every_window_it_covers() -> AnyResult<()> {
    let windows = 4i64;
    let chain = TestChain::synthetic(FIRST_BLOCK, WINDOW_BLOCKS * windows, 4);
    let endpoint = serve(chain, Tamper::None).await?;
    let provider = shared(endpoint.provider);
    let cache = Mutex::new(RangeLogCache::default());
    let ceiling = FIRST_BLOCK + PREFETCH_RANGE_BLOCKS;

    let mut stored_logs = 0;
    for window in 0..windows {
        let from = FIRST_BLOCK + window * WINDOW_BLOCKS;
        let batch = run_window(
            &provider,
            &cache,
            from,
            from + WINDOW_BLOCKS - 1,
            Some(ceiling),
        )
        .await?;
        stored_logs += batch.logs.len();
    }

    assert_eq!(
        endpoint.counts.get("eth_getLogs"),
        1,
        "one wide-range query serves every window inside it"
    );
    assert_eq!(stored_logs, 3 * (windows * WINDOW_BLOCKS / 4) as usize);
    Ok(())
}

#[tokio::test]
async fn a_prefetched_log_on_a_hash_the_window_did_not_resolve_is_refetched() -> AnyResult<()> {
    let chain = TestChain::synthetic(FIRST_BLOCK, WINDOW_BLOCKS * 2, 4);
    let endpoint = serve(chain.clone(), Tamper::None).await?;
    let provider = shared(endpoint.provider);
    let cache = Mutex::new(RangeLogCache::default());
    let ceiling = FIRST_BLOCK + PREFETCH_RANGE_BLOCKS;

    let first = chain.resolved()[..WINDOW_BLOCKS as usize].to_vec();
    let prefetcher = Prefetcher::new(&cache, "test-provider".to_owned(), ceiling);
    let first_to = FIRST_BLOCK + WINDOW_BLOCKS - 1;
    let warm = prefetcher
        .logs(
            &provider,
            &first,
            &watch_query(FIRST_BLOCK, first_to),
            FIRST_BLOCK,
            first_to,
        )
        .await?;
    assert!(!warm.is_empty());
    assert_eq!(endpoint.counts.get("eth_getLogs"), 1);

    // The second window resolves a different lineage than the prefetch saw, so the cached
    // range must be dropped and the window's own range read instead.
    let mut reorged =
        chain.resolved()[WINDOW_BLOCKS as usize..(WINDOW_BLOCKS * 2) as usize].to_vec();
    for block in &mut reorged {
        block.hash = crate::test_chain::hash_of("forked", block.number);
    }
    let from = FIRST_BLOCK + WINDOW_BLOCKS;
    let error = prefetcher
        .logs(
            &provider,
            &reorged,
            &watch_query(from, from + WINDOW_BLOCKS - 1),
            from,
            from + WINDOW_BLOCKS - 1,
        )
        .await
        .expect_err("a log on an unresolved hash must not be accepted");

    assert_eq!(error.kind(), ErrorKind::Transient);
    assert!(
        endpoint.counts.get("eth_getLogs") >= 2,
        "the window refetched its own range"
    );
    Ok(())
}

async fn window_error(tamper: Tamper) -> AnyResult<crate::IngestError> {
    let chain = TestChain::synthetic(FIRST_BLOCK, WINDOW_BLOCKS, 4);
    let endpoint = serve(chain, tamper).await?;
    let provider = shared(endpoint.provider);
    let cache = Mutex::new(RangeLogCache::default());
    Ok(run_window(
        &provider,
        &cache,
        FIRST_BLOCK,
        FIRST_BLOCK + WINDOW_BLOCKS - 1,
        None,
    )
    .await
    .expect_err("the tampered window must fail"))
}

/// The block the fixture tampers with: logged, and inside the window.
const TAMPERED_BLOCK: i64 = FIRST_BLOCK + 8;

#[tokio::test]
async fn a_header_bloom_that_omits_a_stored_log_is_a_data_integrity_fault() -> AnyResult<()> {
    let error = window_error(Tamper::BloomOmitsWatchedAddress(TAMPERED_BLOCK)).await?;
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(
        error.to_string().contains("is not in the bloom of block"),
        "{error}"
    );
    Ok(())
}

#[tokio::test]
async fn a_receipt_log_that_differs_from_the_range_query_is_a_data_integrity_fault() -> AnyResult<()>
{
    let error = window_error(Tamper::ReceiptLogDiffers(TAMPERED_BLOCK)).await?;
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(
        error.to_string().contains("log identity differs"),
        "{error}"
    );
    Ok(())
}

#[tokio::test]
async fn a_watched_log_the_range_query_missed_is_a_data_integrity_fault() -> AnyResult<()> {
    let error = window_error(Tamper::RangeQueryDropsWatchedLog(TAMPERED_BLOCK)).await?;
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
    assert!(
        error
            .to_string()
            .contains("range log query missed watched log"),
        "{error}"
    );
    Ok(())
}

#[tokio::test]
async fn a_receipt_on_a_hash_the_window_did_not_resolve_is_transient() -> AnyResult<()> {
    let error = window_error(Tamper::ReceiptBlockHashMoved(TAMPERED_BLOCK)).await?;
    assert_eq!(error.kind(), ErrorKind::Transient);
    assert!(
        error.to_string().contains("outside resolved block"),
        "{error}"
    );
    Ok(())
}

#[tokio::test]
async fn a_null_receipt_is_transient() -> AnyResult<()> {
    let error = window_error(Tamper::NullReceipt(TAMPERED_BLOCK)).await?;
    assert_eq!(error.kind(), ErrorKind::Transient);
    assert!(error.to_string().contains("omitted receipt"), "{error}");
    Ok(())
}

#[tokio::test]
async fn a_null_transaction_is_transient() -> AnyResult<()> {
    let error = window_error(Tamper::NullTransaction(TAMPERED_BLOCK)).await?;
    assert_eq!(error.kind(), ErrorKind::Transient);
    Ok(())
}

/// Prints the per-window request profile the optimisation is judged on.
///
/// The worst case on mainnet: every block in the window carries watched activity.
#[tokio::test]
async fn the_window_request_profile_is_recorded() -> AnyResult<()> {
    let chain = TestChain::synthetic(FIRST_BLOCK, WINDOW_BLOCKS, 1);
    let endpoint = serve(chain, Tamper::None).await?;
    let provider = shared(endpoint.provider);
    let cache = Mutex::new(RangeLogCache::default());
    let to = FIRST_BLOCK + WINDOW_BLOCKS - 1;

    run_window(&provider, &cache, FIRST_BLOCK, to, Some(to)).await?;
    let profile: BTreeMap<String, usize> = endpoint.counts.snapshot();
    println!(
        "after:  calls={profile:?} http_requests={}",
        provider.verification_rpc_request_attempts()
    );

    assert_eq!(
        profile.get(BLOCK_BODY).copied().unwrap_or_default()
            + profile.get(BLOCK_RECEIPTS).copied().unwrap_or_default()
            + profile.get(EXACT_BLOCK_LOGS).copied().unwrap_or_default(),
        0
    );
    Ok(())
}

/// The same window through the whole-block path, so the saving is measured, not asserted.
#[tokio::test]
async fn the_block_bundle_window_request_profile_is_recorded() -> AnyResult<()> {
    let chain = TestChain::synthetic(FIRST_BLOCK, WINDOW_BLOCKS, 1);
    let endpoint = serve(chain, Tamper::None).await?;
    let provider = shared(endpoint.provider);
    let to = FIRST_BLOCK + WINDOW_BLOCKS - 1;

    // What the crate did before: resolve, then one range query per watch query each
    // followed by its own re-resolve, then headers, then a bundle per logged block.
    let resolved = provider
        .resolve(&((FIRST_BLOCK..=to).collect::<Vec<_>>()))
        .await?;
    let query = watch_query(FIRST_BLOCK, to);
    let logs = provider
        .range_logs(
            &resolved,
            FIRST_BLOCK,
            to,
            &query.addresses,
            &query.topic0s,
            &query.topic1s,
        )
        .await?;
    let logged = logs
        .iter()
        .map(|log| log.block_number)
        .collect::<std::collections::BTreeSet<_>>();
    provider
        .resolve(&logged.into_iter().collect::<Vec<_>>())
        .await?;
    let blocks = provider.headers(&resolved).await?;
    let reference = fetch_selected_bundles(&provider, &resolved, logs)
        .await?
        .into_batch(blocks);

    let profile: BTreeMap<String, usize> = endpoint.counts.snapshot();
    println!(
        "before: calls={profile:?} http_requests={}",
        provider.verification_rpc_request_attempts()
    );
    assert_eq!(
        profile.get(BLOCK_BODY).copied().unwrap_or_default(),
        WINDOW_BLOCKS as usize
    );
    assert_eq!(reference.logs.len(), 3 * WINDOW_BLOCKS as usize);
    Ok(())
}
