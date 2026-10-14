//! Every request the resolver arms serve returns the rows, order and counts of the plain
//! statement: `source.rs`'s source with the resolver condition.
use super::super::super::{
    HistorySummary,
    duplicates::push_product_history_duplicate_filter,
    paging::{push_history_filters, push_history_order},
    resolver_source::ResolverSource,
    source,
};
use super::*;

/// The rows the plain statement returns for `filter`, in its order, unpaged.
async fn plain_identities(pool: &PgPool, filter: &EventHistoryReadFilter) -> Result<Vec<String>> {
    let mut query = QueryBuilder::<Postgres>::new("SELECT ne.event_identity");
    source::push_history_source_for_filter(&mut query, filter, true, false, false);
    push_history_filters(&mut query, filter, true, ResolverSource::Condition);
    push_product_history_duplicate_filter(&mut query, filter, true);
    push_history_order(&mut query, filter.order);
    Ok(query.build_query_scalar().fetch_all(pool).await?)
}

async fn count(pool: &PgPool, filter: &EventHistoryReadFilter, limit: Option<i64>) -> Result<i64> {
    let mut query = QueryBuilder::<Postgres>::new("");
    push_history_count_query(&mut query, filter, true, limit);
    Ok(query.build_query_scalar().fetch_one(pool).await?)
}

/// Rows added to the resolver fixture for this test, for both resolvers, so that each arm's
/// limit has to get the order terms within a block (transaction index and its null
/// placement, log index, identity) and every pushed condition right. Only one canonical block
/// has each number here, so the block hash term never decides the order:
/// - a group of ten rows of the same arm in one block (6,000 + 100 per arm for resolver A,
///   6,500 + 100 per arm for B): three rows tied on transaction and log index that differ
///   only by identity, three rows of one transaction whose log indexes run opposite to their
///   identities, two rows with no transaction or log index, and two plain rows, plus one row
///   at the same block number in an orphaned block, which only the lineage condition drops.
///   An arm reads one row past the page, so a tie or a reversed term can change the page
///   only when three or more rows share the keys before it.
/// - three rows without a block per arm, tied on everything but identity.
/// - old `addr:60` record writes and an old record version change, below newer writes of
///   another key.
/// - the newest rows of each arm in another namespace (blocks 9,970 to 9,972) and the
///   newest emitted rows of another kind (9,975 to 9,977).
/// - the newest pointer rows emitted by one contract (blocks 9,980 and 9,981) above older
///   pointer rows emitted by another (3,000 and 3,001).
const ARM_ORDER_FIXTURE: &str = r#"
    CREATE TEMP TABLE fixture_arms (arm text, resolver text, base bigint);
    INSERT INTO fixture_arms VALUES
        ('emit', '0x00000000000000000000000000000000000000a1', 6000),
        ('set', '0x00000000000000000000000000000000000000a1', 6100),
        ('clear', '0x00000000000000000000000000000000000000a1', 6200),
        ('emit', '0x00000000000000000000000000000000000000b1', 6500),
        ('set', '0x00000000000000000000000000000000000000b1', 6600),
        ('clear', '0x00000000000000000000000000000000000000b1', 6700);
    CREATE TEMP TABLE fixture_rows (
        identity text, arm text, resolver text, namespace text, event_kind text,
        block_number bigint, block_hash text, transaction_index bigint, log_index bigint,
        record_key text, emitter text
    );
    INSERT INTO fixture_rows
    SELECT 'arm-order:' || arm || ':' || resolver || ':' || member, arm, resolver, 'ens',
           'RecordChanged', base, block_hash, tx, log, NULL, NULL
    FROM fixture_arms
    CROSS JOIN LATERAL (VALUES
        ('group-1', 'ethereum-sepolia-block-' || base, 2::bigint, 5::bigint),
        ('group-2', 'ethereum-sepolia-block-' || base, 2, 5),
        ('group-3', 'ethereum-sepolia-block-' || base, 2, 5),
        ('group-4', 'ethereum-sepolia-block-' || base, 1, 9),
        ('group-5', 'ethereum-sepolia-block-' || base, NULL, NULL),
        ('group-6', 'ethereum-sepolia-block-' || base, NULL, NULL),
        ('group-7', 'ethereum-sepolia-block-' || base, 3, 0),
        ('log-1', 'ethereum-sepolia-block-' || base, 4, 3),
        ('log-2', 'ethereum-sepolia-block-' || base, 4, 2),
        ('log-3', 'ethereum-sepolia-block-' || base, 4, 1),
        ('orphaned', 'ethereum-sepolia-orphan-' || base, 0, 0)
    ) member (member, block_hash, tx, log)
    UNION ALL
    SELECT 'arm-order:' || arm || ':' || resolver || ':blockless-' || n, arm, resolver, 'ens',
           'RecordChanged', NULL, NULL, NULL, NULL, NULL, NULL
    FROM fixture_arms CROSS JOIN generate_series(1, 3) n
    UNION ALL
    SELECT 'arm-order:' || arm || ':' || resolver || ':namespace-' || n, arm, resolver,
           'other', 'RecordChanged', 9969 + n, 'ethereum-sepolia-block-' || (9969 + n), 0, 7,
           NULL, NULL
    FROM fixture_arms CROSS JOIN generate_series(1, 3) n;
    INSERT INTO fixture_rows
    SELECT 'arm-order:emit:' || resolver || ':key-' || n, 'emit', resolver, 'ens',
           'RecordChanged', 10 * n, 'ethereum-sepolia-block-' || (10 * n), 0, 7, 'addr:60', NULL
    FROM (SELECT DISTINCT resolver FROM fixture_arms) r CROSS JOIN generate_series(2, 4) n
    UNION ALL
    SELECT 'arm-order:emit:' || resolver || ':version', 'emit', resolver, 'ens',
           'RecordVersionChanged', 50, 'ethereum-sepolia-block-50', 0, 7, NULL, NULL
    FROM (SELECT DISTINCT resolver FROM fixture_arms) r
    UNION ALL
    SELECT 'arm-order:emit:' || resolver || ':other-key-' || n, 'emit', resolver, 'ens',
           'RecordChanged', 9940 + n, 'ethereum-sepolia-block-' || (9940 + n), 0, 7,
           'text:avatar', NULL
    FROM (SELECT DISTINCT resolver FROM fixture_arms) r CROSS JOIN generate_series(1, 3) n
    UNION ALL
    SELECT 'arm-order:emit:' || resolver || ':kind-' || n, 'emit', resolver, 'ens',
           'AliasChanged', 9974 + n, 'ethereum-sepolia-block-' || (9974 + n), 0, 7, NULL, NULL
    FROM (SELECT DISTINCT resolver FROM fixture_arms) r CROSS JOIN generate_series(1, 3) n
    UNION ALL
    SELECT 'arm-order:' || arm || ':' || resolver || ':newer-contract-' || n, arm, resolver,
           'ens', 'RecordChanged', 9979 + n, 'ethereum-sepolia-block-' || (9979 + n), 0, 7,
           NULL, '0x00000000000000000000000000000000000000c1'
    FROM fixture_arms CROSS JOIN generate_series(1, 2) n WHERE arm <> 'emit'
    UNION ALL
    SELECT 'arm-order:' || arm || ':' || resolver || ':older-contract-' || n, arm, resolver,
           'ens', 'RecordChanged', 2999 + n, 'ethereum-sepolia-block-' || (2999 + n), 0, 7,
           NULL, '0x00000000000000000000000000000000000000c2'
    FROM fixture_arms CROSS JOIN generate_series(1, 2) n WHERE arm <> 'emit';
    INSERT INTO chain_lineage
        (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
    SELECT 'ethereum-sepolia', 'ethereum-sepolia-orphan-' || base, base, to_timestamp(base),
           'orphaned'
    FROM fixture_arms;
    INSERT INTO normalized_events
        (event_identity, namespace, event_kind, source_family, manifest_version,
         chain_id, block_hash, block_number, transaction_hash, transaction_index,
         log_index, derivation_kind, canonicality_state, after_state, before_state, raw_fact_ref)
    SELECT identity, namespace,
           CASE WHEN arm = 'emit' THEN event_kind ELSE 'ResolverChanged' END,
           CASE WHEN arm = 'emit' THEN 'ens_v1_resolver_l1' ELSE 'ens_v1_registry_l1' END,
           1, 'ethereum-sepolia', block_hash, block_number,
           CASE WHEN transaction_index IS NULL THEN NULL ELSE 'tx-' || identity END,
           transaction_index, log_index, 'ens_v1_unwrapped_authority', 'canonical',
           CASE arm
               WHEN 'emit' THEN jsonb_strip_nulls(jsonb_build_object(
                   'node', 'node-arm-order', 'record_key', record_key))
               WHEN 'set' THEN jsonb_build_object('node', 'node-arm-order', 'resolver', resolver)
               ELSE jsonb_build_object(
                   'node', 'node-arm-order',
                   'resolver', '0x0000000000000000000000000000000000000099')
           END,
           CASE arm
               WHEN 'emit' THEN '{}'::jsonb
               WHEN 'set' THEN jsonb_build_object(
                   'resolver', '0x0000000000000000000000000000000000000099')
               ELSE jsonb_build_object('resolver', resolver)
           END,
           CASE
               WHEN arm = 'emit' THEN jsonb_build_object('emitting_address', resolver)
               WHEN emitter IS NOT NULL THEN jsonb_build_object('emitting_address', emitter)
               ELSE '{}'::jsonb
           END
    FROM fixture_rows;
    DROP TABLE fixture_rows, fixture_arms;
    ANALYZE normalized_events;
    ANALYZE chain_lineage;
"#;

/// Only the pointer-set arm tests a `candidate` row's new resolver for equality.
const ARM_MARKER: &str = "lower(candidate.after_state ->> 'resolver') = ";

async fn full_summary(pool: &PgPool, filter: &EventHistoryReadFilter) -> Result<HistorySummary> {
    load_history_page(
        pool,
        filter.clone(),
        true,
        None,
        1,
        HistorySummaryMode::Full,
        false,
        None,
    )
    .await?
    .summary
    .context("a full summary must be returned")
}

/// The arm-shape variants an events request can send: with and without a namespace, with no
/// kinds, a block window per chain with both bounds (what a timestamp range resolves to), a
/// window over several chains, a window that leaves the resolver's chain out, an empty window,
/// an open range, block bounds, a lower block bound alone, a record key with and without
/// kinds, record kinds only, and no matching kinds. A contract filter is not in the arm shape
/// (`resolver_arm_source`), so it is not here. The bounded windows cut through the fixture's matches, which lie in blocks 20 to 1,656, 3,000
/// to 6,700 and 9,941 to 9,981.
fn arm_shape_variants(address: &str) -> Vec<(&'static str, EventHistoryReadFilter)> {
    let base = |order| resolver_filter(order, address);
    let mut variants = Vec::new();
    for order in [HistoryOrder::Desc, HistoryOrder::Asc] {
        variants.push(("namespace", base(order)));
        let mut filter = base(order);
        filter.namespace = None;
        variants.push(("no namespace", filter));
        let mut filter = base(order);
        filter.event_kinds.clear();
        variants.push(("no kinds", filter));
        let mut filter = base(order);
        filter.block_window = Some(window(&[(RESOLVER_CHAIN, Some(500), Some(1_200))]));
        variants.push(("window with both bounds", filter));
        let mut filter = base(order);
        filter.namespace = None;
        filter.block_window = Some(window(&[
            (RESOLVER_CHAIN, Some(200), Some(9_955)),
            ("other-chain", None, Some(9_500)),
        ]));
        variants.push(("window over two chains", filter));
        let mut filter = base(order);
        filter.block_window = Some(window(&[("other-chain", None, Some(9_500))]));
        variants.push(("window without the resolver chain", filter));
        let mut filter = base(order);
        filter.block_window = Some(window(&[]));
        variants.push(("empty window", filter));
        let mut filter = base(order);
        filter.block_window = Some(window(&[(RESOLVER_CHAIN, None, None)]));
        variants.push(("open window", filter));
        let mut filter = base(order);
        filter.from_block = Some(300);
        filter.to_block = Some(1_000);
        variants.push(("block bounds", filter));
        let mut filter = base(order);
        filter.record_key = Some("addr:60".to_owned());
        variants.push(("record key", filter));
        let mut filter = base(order);
        filter.match_no_events = true;
        variants.push(("no matching kinds", filter));
        let mut filter = base(order);
        filter.event_kinds.clear();
        filter.record_key = Some("addr:60".to_owned());
        variants.push(("record key, no kinds", filter));
        let mut filter = base(order);
        filter.event_kinds = vec!["RecordChanged".to_owned()];
        variants.push(("record kinds only", filter));
        let mut filter = base(order);
        filter.from_block = Some(6_000);
        variants.push(("from block", filter));
    }
    variants
}

/// Variants that match no row by design. Every other variant must return rows, so that it
/// can fail.
const EMPTY_VARIANTS: [&str; 3] = [
    "no matching kinds",
    "empty window",
    "window without the resolver chain",
];

#[tokio::test]
async fn arm_shape_requests_match_the_plain_statement() -> Result<()> {
    // The resolver fixture with 10,000 blocks of noise instead of 150,000: every match lies at
    // or below block 9,981, and the smaller copy keeps this test light beside the other users.
    let fixture = format!(
        "{}{ARM_ORDER_FIXTURE}",
        RESOLVER_FIXTURE.replace("generate_series(1, 150000)", "generate_series(1, 10000)")
    );
    let database = phase_database("history_resolver_arm_equivalence", &fixture).await?;
    let result = async {
        let pool = database.pool();
        let mut failures = Vec::new();
        for address in [RESOLVER_A, RESOLVER_B] {
            for (name, filter) in arm_shape_variants(address) {
                let label = format!("{address} {name} {:?}", filter.order);
                let mut probe = QueryBuilder::<Postgres>::new("");
                push_history_page_query(&mut probe, &filter, true, None, false, 2);
                if !probe.sql().contains(ARM_MARKER) {
                    failures.push(format!("{label}: the request does not take the arms"));
                }
                let expected = plain_identities(pool, &filter).await?;
                if expected.is_empty() && !EMPTY_VARIANTS.contains(&name) {
                    failures.push(format!("{label}: the fixture gives no rows, so it cannot fail"));
                }
                for page_size in [1u64, 3, 50] {
                    let paged = paged_identities(pool, &filter, page_size).await?;
                    if paged != expected {
                        failures.push(format!(
                            "{label}: pages of {page_size} gave {} rows, the plain statement gives {}",
                            paged.len(),
                            expected.len()
                        ));
                    }
                }
                // A cursor without its position resumes through the cursor-check statement.
                for page_size in [1u64, 3, 50] {
                    let mut unpositioned = Vec::new();
                    let mut cursor: Option<HistoryCursor> = None;
                    loop {
                        let (rows, next) =
                            page_identities(pool, &filter, cursor.as_ref(), page_size).await?;
                        unpositioned.extend(rows);
                        match next {
                            Some(mut next) => {
                                next.position = None;
                                cursor = Some(next);
                            }
                            None => break,
                        }
                    }
                    if unpositioned != expected {
                        failures.push(format!(
                            "{label}: pages of {page_size} through cursors without a position \
                             gave {} rows, the plain statement gives {}",
                            unpositioned.len(),
                            expected.len()
                        ));
                    }
                }
                let full = full_summary(pool, &filter).await?;
                // The same request with `diagnostics` set falls outside the arm shape and reads
                // the plain source with the resolver condition. With no selector, `diagnostics`
                // changes nothing else, so its summary is the plain statement's.
                let mut plain_filter = filter.clone();
                plain_filter.diagnostics = true;
                let plain_full = full_summary(pool, &plain_filter).await?;
                if full.total_count != u64::try_from(expected.len())? {
                    failures.push(format!(
                        "{label}: the full summary counted {}, the plain statement gives {}",
                        full.total_count,
                        expected.len()
                    ));
                }
                if full != plain_full {
                    failures.push(format!(
                        "{label}: the full summary differs from the plain statement's"
                    ));
                }
                let total = i64::try_from(expected.len())?;
                for (limit, want) in [(None, total), (Some(5), total.min(5))] {
                    let got = count(pool, &filter, limit).await?;
                    if got != want {
                        failures.push(format!(
                            "{label}: count with limit {limit:?} gave {got}, the plain statement gives {want}"
                        ));
                    }
                }
                eprintln!("{label}: {} rows", expected.len());
            }
        }
        ensure!(failures.is_empty(), "{}", failures.join("\n"));
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}
