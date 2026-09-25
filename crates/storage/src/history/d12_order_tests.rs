//! The history order under D12: block number, transaction index, log index, then
//! `event_identity`, with events that have no transaction position first in their block. The
//! transaction hash and the generated normalized event id never decide an order.

use std::collections::BTreeSet;

use anyhow::{Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::PgPool;
use uuid::Uuid;

use super::super::selectors::HistorySelector;
use super::super::{EventHistoryReadFilter, HistoryCursor, HistoryOrder, HistorySummaryMode};
use super::load_history_page;

async fn phase_database(name: &str, fixture: &str) -> Result<TestDatabase> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new(name).pool_max_connections(1)).await?;
    let mut connection = database.pool().acquire().await?;
    sqlx::raw_sql("CREATE SCHEMA bigname_phase; SET search_path TO bigname_phase, public")
        .execute(&mut *connection)
        .await?;
    for baseline in [
        include_str!("../../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../../schema-v2/baseline/02_raw_facts.sql"),
        include_str!("../../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../../schema-v2/baseline/04_manifests.sql"),
        include_str!("../../../../schema-v2/baseline/05_normalized_events.sql"),
        include_str!("../../../../schema-v2/baseline/06_projections.sql"),
        fixture,
    ] {
        sqlx::raw_sql(baseline).execute(&mut *connection).await?;
    }
    Ok(database)
}

/// Block 2 holds transaction A at index 1 with the greater hash and logs 3 and 7, transaction B
/// at index 2 with the smaller hash, and an event with no transaction position. Blocks 1 and 3
/// hold one event each.
const BLOCK_FIXTURE: &str = r#"
    INSERT INTO chain_lineage
        (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
    SELECT 'chain-a', 'chain-a-block-' || n, n, to_timestamp(n), 'canonical'
    FROM generate_series(1, 3) n;
    INSERT INTO normalized_events
        (event_identity, namespace, event_kind, source_family, manifest_version,
         chain_id, block_hash, block_number, transaction_hash, transaction_index,
         log_index, derivation_kind, canonicality_state, after_state)
    SELECT identity, 'ens', 'RecordChanged', 'ens_v1_registry_l1', 1, 'chain-a',
           'chain-a-block-' || block, block, tx_hash, tx_index, log, 'ens_v1_unwrapped_authority',
           'canonical', '{}'::jsonb
    FROM (VALUES
        ('d12:block-1', 1, '0x11', 0, 0),
        ('d12:a-log-3', 2, '0xff', 1, 3),
        ('d12:a-log-7', 2, '0xff', 1, 7),
        ('d12:b', 2, '0x00', 2, 9),
        ('d12:synthesised', 2, NULL, NULL, NULL),
        ('d12:block-3', 3, '0x33', 0, 0)
    ) AS event(identity, block, tx_hash, tx_index, log);
"#;

const NEWEST_FIRST: [&str; 6] = [
    "d12:block-3",
    "d12:b",
    "d12:a-log-7",
    "d12:a-log-3",
    "d12:synthesised",
    "d12:block-1",
];

fn events_filter(order: HistoryOrder) -> EventHistoryReadFilter {
    EventHistoryReadFilter {
        namespace: Some("ens".to_owned()),
        event_kinds: vec!["RecordChanged".to_owned()],
        order,
        ..EventHistoryReadFilter::default()
    }
}

fn expected(order: HistoryOrder) -> Vec<String> {
    let mut expected = NEWEST_FIRST.map(str::to_owned).to_vec();
    if order == HistoryOrder::Asc {
        expected.reverse();
    }
    expected
}

async fn page(
    pool: &PgPool,
    order: HistoryOrder,
    cursor: Option<&HistoryCursor>,
    page_size: u64,
) -> Result<(Vec<String>, Option<HistoryCursor>)> {
    filtered_page(pool, events_filter(order), cursor, page_size).await
}

async fn filtered_page(
    pool: &PgPool,
    filter: EventHistoryReadFilter,
    cursor: Option<&HistoryCursor>,
    page_size: u64,
) -> Result<(Vec<String>, Option<HistoryCursor>)> {
    let page = load_history_page(
        pool,
        filter,
        true,
        cursor,
        page_size,
        HistorySummaryMode::None,
        false,
        None,
    )
    .await?;
    Ok((
        page.rows
            .into_iter()
            .map(|row| row.event_identity)
            .collect(),
        page.next_cursor,
    ))
}

/// Every row from `cursor` on, one row per page.
async fn walk(
    pool: &PgPool,
    order: HistoryOrder,
    mut cursor: Option<HistoryCursor>,
) -> Result<Vec<String>> {
    let mut rows = Vec::new();
    loop {
        let (page_rows, next) = page(pool, order, cursor.as_ref(), 1).await?;
        rows.extend(page_rows);
        match next {
            Some(next) => cursor = Some(next),
            None => return Ok(rows),
        }
    }
}

/// The cursor a one-row page issues for `anchor`.
async fn cursor_after(pool: &PgPool, order: HistoryOrder, anchor: &str) -> Result<HistoryCursor> {
    let mut cursor = None;
    loop {
        let (rows, next) = page(pool, order, cursor.as_ref(), 1).await?;
        let next = next.ok_or_else(|| anyhow::anyhow!("{anchor} must be followed by a row"))?;
        if rows.iter().any(|row| row == anchor) {
            return Ok(next);
        }
        cursor = Some(next);
    }
}

#[tokio::test]
async fn one_block_orders_by_transaction_index_in_both_directions() -> Result<()> {
    let database = phase_database("history_d12_block_order", BLOCK_FIXTURE).await?;
    let result = async {
        let pool = database.pool();
        for order in [HistoryOrder::Desc, HistoryOrder::Asc] {
            let (unpaged, next) = page(pool, order, None, 100).await?;
            ensure!(next.is_none(), "the unpaged read must fit on one page");
            ensure!(
                unpaged == expected(order),
                "{order:?}: B (index 2) comes before A (index 1) newest first, and the event without a \
                 transaction first in its block, got {unpaged:?}"
            );
            let walked = walk(pool, order, None).await?;
            ensure!(walked == unpaged, "{order:?}: one-row pages {walked:?}");
        }
        // Mid-block continuations carry the transaction index. Newest first, after A's last
        // row the event without a transaction follows, never B; oldest first, after A's last
        // row B follows.
        let desc = cursor_after(pool, HistoryOrder::Desc, "d12:a-log-3").await?;
        let (rows, _) = page(pool, HistoryOrder::Desc, Some(&desc), 1).await?;
        ensure!(rows == ["d12:synthesised"], "descending after A: {rows:?}");
        let asc = cursor_after(pool, HistoryOrder::Asc, "d12:a-log-7").await?;
        let (rows, _) = page(pool, HistoryOrder::Asc, Some(&asc), 1).await?;
        ensure!(rows == ["d12:b"], "ascending after A: {rows:?}");
        // The position the cursor carries continues the same way once its anchor is gone.
        let mid = cursor_after(pool, HistoryOrder::Desc, "d12:a-log-7").await?;
        sqlx::query("DELETE FROM normalized_events WHERE event_identity = 'd12:a-log-7'")
            .execute(pool)
            .await?;
        let rest = walk(pool, HistoryOrder::Desc, Some(mid)).await?;
        ensure!(
            rest == ["d12:a-log-3", "d12:synthesised", "d12:block-1"],
            "descending after a deleted anchor: {rest:?}"
        );
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

const NAMEHASH: &str = "0x0000000000000000000000000000000000000000000000000000000000000a11";
const RESOURCE: u128 = 0xa11;

/// One name with a resource on blocks 1 to 5; `events` are inserted one statement at a time in
/// the order given, so their generated ids follow that order.
fn attribution_fixture(events: &[(&str, &str, &str, bool, i64, i64, &str)]) -> String {
    let mut sql = format!(
        r#"
        INSERT INTO chain_lineage
            (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
        SELECT 'ethereum-mainnet', 'block-' || n, n, to_timestamp(n), 'canonical'
        FROM generate_series(1, 5) n;
        INSERT INTO name_surfaces
            (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash,
             labelhashes, normalizer_version, visibility_state, chain_id, block_hash,
             block_number, canonicality_state)
        VALUES ('ens:{NAMEHASH}', 'ens', 'tie.eth', ARRAY['tie', 'eth'], '\x00'::bytea,
                '{NAMEHASH}', ARRAY['{NAMEHASH}', 'eth'], 'test', 'active', 'ethereum-mainnet',
                'block-1', 1, 'canonical');
        INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state)
        VALUES ('{resource}', 'ethereum-mainnet', 'block-1', 1, 'canonical');
        "#,
        resource = Uuid::from_u128(RESOURCE),
    );
    for (identity, kind, family, named, block, log, state) in events {
        let (name, resource) = if *named {
            (
                format!("'ens:{NAMEHASH}'"),
                format!("'{}'::uuid", Uuid::from_u128(RESOURCE)),
            )
        } else {
            ("NULL".to_owned(), "NULL".to_owned())
        };
        sql.push_str(&format!(
            "INSERT INTO normalized_events
                 (event_identity, namespace, logical_name_id, resource_id, event_kind,
                  source_family, manifest_version, chain_id, block_hash, block_number,
                  transaction_hash, transaction_index, log_index, derivation_kind,
                  canonicality_state, after_state)
             VALUES ('{identity}', 'ens', {name}, {resource}, '{kind}', '{family}', 1,
                     'ethereum-mainnet', 'block-{block}', {block}, 'tx-{block}', 0, {log},
                     'ens_v1_unwrapped_authority', 'canonical', '{state}'::jsonb);\n"
        ));
    }
    sql
}

async fn attributed(pool: &PgPool) -> Result<BTreeSet<String>> {
    let resource = Uuid::from_u128(RESOURCE);
    let ids = crate::load_bounded_record_attribution(pool, &[resource], None)
        .await?
        .remove(&resource)
        .unwrap_or_default();
    let identities: Vec<String> = sqlx::query_scalar(
        "SELECT event_identity FROM normalized_events WHERE normalized_event_id = ANY($1)",
    )
    .bind(ids.into_iter().collect::<Vec<_>>())
    .fetch_all(pool)
    .await?;
    Ok(identities.into_iter().collect())
}

/// Two resolver pointers at one position: the later by `event_identity` is the latest pointer,
/// whatever the generated ids say, so only its resolver's write is attributed.
#[tokio::test]
async fn pointer_attribution_breaks_exact_position_ties_by_identity() -> Result<()> {
    let pointer = |resolver: &str| format!(r#"{{"node": "{NAMEHASH}", "resolver": "{resolver}"}}"#);
    let resolver_a = "0x00000000000000000000000000000000000000aa";
    let resolver_b = "0x00000000000000000000000000000000000000bb";
    let (pointer_a, pointer_b) = (pointer(resolver_a), pointer(resolver_b));
    let fixture = attribution_fixture(&[
        // The later pointer by identity gets the smaller generated id.
        (
            "tie:b",
            "ResolverChanged",
            "ens_v1_registry_l1",
            true,
            2,
            0,
            &pointer_b,
        ),
        (
            "tie:a",
            "ResolverChanged",
            "ens_v1_registry_l1",
            true,
            2,
            0,
            &pointer_a,
        ),
        (
            "write:a",
            "RecordChanged",
            "ens_v1_resolver_l1",
            false,
            3,
            0,
            &pointer_a,
        ),
        (
            "write:b",
            "RecordChanged",
            "ens_v1_resolver_l1",
            false,
            3,
            1,
            &pointer_b,
        ),
    ]);
    let database = phase_database("history_d12_pointer_tie", &fixture).await?;
    let result = async {
        let pool = database.pool();
        let attributed = attributed(pool).await?;
        ensure!(
            attributed == BTreeSet::from(["write:b".to_owned()]),
            "the pointer that is later by identity attributes its resolver's write: {attributed:?}"
        );
        // The resource's history lists both pointers and the attributed write only, in the
        // same order unpaged and one row per page, in both directions; the tied pointers
        // follow their identities.
        for order in [HistoryOrder::Desc, HistoryOrder::Asc] {
            let filter = || EventHistoryReadFilter {
                selectors: vec![HistorySelector::Resources(vec![Uuid::from_u128(RESOURCE)])],
                order,
                ..EventHistoryReadFilter::default()
            };
            let mut expected = ["write:b", "tie:b", "tie:a"].map(str::to_owned).to_vec();
            if order == HistoryOrder::Asc {
                expected.reverse();
            }
            let (unpaged, next) = filtered_page(pool, filter(), None, 100).await?;
            ensure!(next.is_none(), "the unpaged read must fit on one page");
            ensure!(unpaged == expected, "{order:?}: unpaged {unpaged:?}");
            let mut walked = Vec::new();
            let mut cursor = None;
            loop {
                let (rows, next) = filtered_page(pool, filter(), cursor.as_ref(), 1).await?;
                walked.extend(rows);
                match next {
                    Some(next) => cursor = Some(next),
                    None => break,
                }
            }
            ensure!(walked == expected, "{order:?}: one-row pages {walked:?}");
        }
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

/// Two exact record links at one position: the later by `event_identity` selects the record
/// the span after both serves, whatever the generated ids say.
#[tokio::test]
async fn link_selection_breaks_exact_position_ties_by_identity() -> Result<()> {
    let resolver = "0x00000000000000000000000000000000000000cc";
    let pointer = format!(r#"{{"node": "{NAMEHASH}", "resolver": "{resolver}"}}"#);
    let link = |record: &str| {
        format!(
            r#"{{"node": "{NAMEHASH}", "resolver": "{resolver}", "storage_model": "resolver_record_id", "resolver_record_id": "{record}"}}"#
        )
    };
    let write = |record: &str| {
        format!(
            r#"{{"resolver": "{resolver}", "storage_model": "resolver_record_id", "resolver_record_id": "{record}"}}"#
        )
    };
    let (link_1, link_2, write_1, write_2) = (link("1"), link("2"), write("1"), write("2"));
    let fixture = attribution_fixture(&[
        (
            "pointer",
            "ResolverChanged",
            "ens_v1_registry_l1",
            true,
            2,
            0,
            &pointer,
        ),
        // The later link by identity gets the smaller generated id.
        (
            "link:b",
            "ResolverRecordLinked",
            "ens_v2_resolver_l1",
            false,
            3,
            0,
            &link_2,
        ),
        (
            "link:a",
            "ResolverRecordLinked",
            "ens_v2_resolver_l1",
            false,
            3,
            0,
            &link_1,
        ),
        (
            "write:1",
            "RecordChanged",
            "ens_v2_resolver_l1",
            false,
            4,
            0,
            &write_1,
        ),
        (
            "write:2",
            "RecordChanged",
            "ens_v2_resolver_l1",
            false,
            4,
            1,
            &write_2,
        ),
    ]);
    let database = phase_database("history_d12_link_tie", &fixture).await?;
    let result = async {
        let attributed = attributed(database.pool()).await?;
        let expected = ["link:a", "link:b", "write:2"]
            .map(str::to_owned)
            .into_iter()
            .collect::<BTreeSet<_>>();
        ensure!(
            attributed == expected,
            "the link that is later by identity selects the served record: {attributed:?}"
        );
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}
