//! The stored event count statement (`count_contract.rs`) against the event reads it counts, on
//! one seeded chain. For every (anchor, namespace, kind) the counts the statement gives equal the
//! count the `contract_address` read, the `resolver` read and the registry overview's read run for
//! that key at the published block, capped as the reads cap and uncapped, and the counts of
//! consecutive block ranges add up to the count of their union. The seed holds one row of each
//! input class the predicate decides: emitters across blocks, kinds and namespaces, the three
//! resolver arms, each fixed duplicate rule, registry-fallback handoff groups, candidate and
//! orphaned rows, a row without a block and a row above the published block.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgConnection, Postgres, QueryBuilder};
use uuid::Uuid;

use super::{
    ChainBlockRange, EventHistoryReadFilter, EventHistoryResolverFilter, HistoryBlockWindow,
    count_contract::{EMITTER, POINTER_CLEARED, POINTER_SET, block_event_counts_sql},
    summary::push_history_count_query,
};

const CHAIN: &str = "ethereum-sepolia";
const OTHER_CHAIN: &str = "base-mainnet";
/// The published block every read is bound to.
const PUBLISHED: i64 = 100;
/// The cap the event feed's `include=total_count` applies.
const CAP: u64 = 100_000;
const EMITTER_A: &str = "0x00000000000000000000000000000000000000e1";
const REGISTRY: &str = "0x00000000000000000000000000000000000000e2";
const RESOLVER_1: &str = "0x00000000000000000000000000000000000000c1";
const RESOLVER_2: &str = "0x00000000000000000000000000000000000000c2";
const RESOLVER_3: &str = "0x00000000000000000000000000000000000000c3";
const ZERO: &str = "0x0000000000000000000000000000000000000000";
/// The Sepolia PublicResolver, a proven address-pair generation (`count_contract.rs`).
const PUBLIC_RESOLVER: &str = "0xe99638b40e4fff0129d56f03b55b6bbc4bbe49b5";
const HANDOFF: &str = ":ResolverChanged:registry-fallback-handoff:";

type Key = (i16, String, String, String);

struct Row<'a> {
    identity: String,
    chain: &'a str,
    block: Option<i64>,
    log: i64,
    namespace: &'a str,
    kind: &'a str,
    family: &'a str,
    emitter: &'a str,
    before: Value,
    after: Value,
    manifest: Option<i64>,
}

impl<'a> Row<'a> {
    fn new(identity: impl Into<String>, block: i64, kind: &'a str, emitter: &'a str) -> Self {
        Self {
            identity: identity.into(),
            chain: CHAIN,
            block: Some(block),
            log: 0,
            namespace: "ens",
            kind,
            family: "ens_v1_registry_l1",
            emitter,
            before: json!({}),
            after: json!({}),
            manifest: None,
        }
    }
}

async fn insert(conn: &mut PgConnection, row: Row<'_>) -> Result<()> {
    let hash = row.block.map(|block| format!("b{block}"));
    sqlx::query(
        "INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
             manifest_version, source_manifest_id, chain_id, block_number, block_hash,
             transaction_hash, transaction_index, log_index, derivation_kind, canonicality_state,
             before_state, after_state, raw_fact_ref)
         VALUES ($1, $2, $3, $4, 1, $5, $6, $7, $8, CASE WHEN $8 IS NOT NULL THEN 'tx' END,
                 CASE WHEN $8 IS NOT NULL THEN 0 END, CASE WHEN $8 IS NOT NULL THEN $9 END,
                 'ens_v1_unwrapped_authority', 'canonical', $10, $11, $12)",
    )
    .bind(&row.identity)
    .bind(row.namespace)
    .bind(row.kind)
    .bind(row.family)
    .bind(row.manifest)
    .bind(row.chain)
    .bind(row.block)
    .bind(hash)
    .bind(row.log)
    .bind(row.before)
    .bind(row.after)
    .bind(json!({"emitting_address": row.emitter}))
    .execute(&mut *conn)
    .await?;
    Ok(())
}

async fn install(conn: &mut PgConnection) -> Result<()> {
    sqlx::raw_sql("CREATE SCHEMA bigname_phase; SET search_path = bigname_phase, public")
        .execute(&mut *conn)
        .await?;
    for schema in [
        include_str!("../../schema/baseline/01_chain.sql"),
        include_str!("../../schema/baseline/02_raw_facts.sql"),
        include_str!("../../schema/baseline/03_identity.sql"),
        include_str!("../../schema/baseline/04_manifests.sql"),
        include_str!("../../schema/baseline/05_normalized_events.sql"),
    ] {
        sqlx::raw_sql(schema).execute(&mut *conn).await?;
    }
    for chain in [CHAIN, OTHER_CHAIN] {
        sqlx::query(
            "INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp,
                 canonicality_state)
             SELECT $1, 'b' || block, block, to_timestamp(block), 'canonical'
             FROM generate_series(0, $2) block",
        )
        .bind(chain)
        .bind(PUBLISHED + 1)
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

/// The Sepolia PublicResolver as its manifest declares it, for the address-pair rule.
async fn declare_public_resolver(conn: &mut PgConnection) -> Result<i64> {
    let instance = Uuid::from_u128(1);
    sqlx::query(
        "INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
         VALUES ($1, $2, 'contract')",
    )
    .bind(instance)
    .bind(CHAIN)
    .execute(&mut *conn)
    .await?;
    let manifest: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions (manifest_version, namespace, source_family, chain_id,
             deployment_label, rollout_status, normalizer_version, file_path, manifest_payload)
         VALUES (1, 'ens', 'ens_v1_resolver_l1', $1, 'counts', 'shadow', 'test', 'counts', '{}')
         RETURNING manifest_id",
    )
    .bind(CHAIN)
    .fetch_one(&mut *conn)
    .await?;
    sqlx::query(
        "INSERT INTO manifest_contract_instances (manifest_id, chain_id, declaration_kind,
             declaration_name, contract_instance_id, declared_address, role, proxy_kind,
             start_block_number)
         VALUES ($1, $2, 'contract', 'public_resolver', $3, $4, 'public_resolver', 'none', 0)",
    )
    .bind(manifest)
    .bind(CHAIN)
    .bind(instance)
    .bind(PUBLIC_RESOLVER)
    .execute(&mut *conn)
    .await?;
    Ok(manifest)
}

fn address_write(block: i64, log: i64, source: &str, manifest: i64) -> Row<'static> {
    let mut row = Row::new(
        format!("pair-{block}-{log}"),
        block,
        "RecordChanged",
        PUBLIC_RESOLVER,
    );
    row.log = log;
    row.family = "ens_v1_resolver_l1";
    row.manifest = Some(manifest);
    row.after = json!({"source_event": source, "resolver": PUBLIC_RESOLVER,
        "resolver_contract_instance_id": Uuid::from_u128(1).to_string(),
        "node": format!("0x{:064x}", 1), "record_key": "addr:60", "record_family": "addr",
        "selector_key": "60", "value_retained": true,
        "value": "0x000000000000000000000000000000000000000a"});
    row
}

fn resolver_changed(
    identity: impl Into<String>,
    block: i64,
    emitter: &'static str,
    before: Option<&str>,
    after: &str,
) -> Row<'static> {
    let mut row = Row::new(identity, block, "ResolverChanged", emitter);
    row.after = json!({"resolver": after, "node": format!("0x{:064x}", 2)});
    if let Some(before) = before {
        row.before = json!({"resolver": before});
    }
    row
}

async fn seed(conn: &mut PgConnection) -> Result<()> {
    // Emitter rows across blocks, kinds and namespaces, one with a mixed-case emitter.
    insert(conn, Row::new("emitted-10", 10, "AliasChanged", EMITTER_A)).await?;
    insert(
        conn,
        Row::new("emitted-20", 20, "PermissionScopeChanged", EMITTER_A),
    )
    .await?;
    let mut basenames = Row::new("emitted-30", 30, "AliasChanged", EMITTER_A);
    basenames.namespace = "basenames";
    insert(conn, basenames).await?;
    insert(
        conn,
        Row::new(
            "emitted-40",
            40,
            "AliasChanged",
            "0x00000000000000000000000000000000000000E1",
        ),
    )
    .await?;
    // The same emitter on another chain, and above the published block.
    let mut elsewhere = Row::new("emitted-elsewhere", 10, "AliasChanged", EMITTER_A);
    elsewhere.chain = OTHER_CHAIN;
    insert(conn, elsewhere).await?;
    insert(
        conn,
        Row::new("emitted-101", 101, "AliasChanged", EMITTER_A),
    )
    .await?;
    // The three resolver arms: a resolver's own row, a pointer set to it and a pointer moved
    // from it, each on another contract's row. A pointer a resolver sets to itself is its own
    // row only, and a pointer set to the resolver it already held is a set only.
    insert(
        conn,
        Row::new("resolver-own", 20, "AliasChanged", RESOLVER_1),
    )
    .await?;
    insert(
        conn,
        resolver_changed("set", 30, REGISTRY, None, RESOLVER_1),
    )
    .await?;
    insert(
        conn,
        resolver_changed("moved", 40, REGISTRY, Some(RESOLVER_1), RESOLVER_2),
    )
    .await?;
    insert(
        conn,
        resolver_changed("self", 50, RESOLVER_1, None, RESOLVER_1),
    )
    .await?;
    insert(
        conn,
        resolver_changed("kept", 50, REGISTRY, Some(RESOLVER_2), RESOLVER_2),
    )
    .await?;
    // The fixed duplicate rules: a registrar snapshot, a token-state-absent expiry, a derived
    // wrapper mint transfer, a registry-read copy and the second row of a proven address pair.
    let mut snapshot = Row::new("snapshot", 50, "AliasChanged", EMITTER_A);
    snapshot.after = json!({"state_derived": true, "registrar_surface_snapshot": true});
    insert(conn, snapshot).await?;
    let mut expiry = Row::new("tokenless-expiry", 50, "ExpiryChanged", EMITTER_A);
    expiry.after = json!({"token_state_absent": true});
    insert(conn, expiry).await?;
    let mut mint = Row::new("wrapper-mint", 50, "TokenControlTransferred", EMITTER_A);
    mint.family = "ens_v1_wrapper_l1";
    mint.before = json!({"from": ZERO});
    mint.after = json!({"source_event": "NameWrapped", "to": RESOLVER_3});
    insert(conn, mint).await?;
    insert(
        conn,
        resolver_changed(
            "copy:ResolverChanged:registry-read:1",
            50,
            REGISTRY,
            None,
            RESOLVER_3,
        ),
    )
    .await?;
    let manifest = declare_public_resolver(conn).await?;
    insert(conn, address_write(55, 1, "AddressChanged", manifest)).await?;
    insert(conn, address_write(55, 2, "AddrChanged", manifest)).await?;
    // Registry-fallback handoff groups: one copy per retired resolver. The second group retires
    // the first resolver twice, so that resolver reads one of its two copies.
    for (block, before) in [
        (60, [RESOLVER_1, RESOLVER_2, RESOLVER_3]),
        (61, [RESOLVER_1, RESOLVER_1, RESOLVER_2]),
    ] {
        for (copy, before) in before.into_iter().enumerate() {
            let mut row = resolver_changed(
                format!("origin-{block}{HANDOFF}{copy}"),
                block,
                REGISTRY,
                Some(before),
                ZERO,
            );
            row.log = i64::try_from(copy)?;
            insert(conn, row).await?;
        }
    }
    // Rows no read counts: a candidate, an orphaned row, a row on an orphaned block and a row
    // without a block.
    insert(conn, Row::new("candidate", 70, "AliasChanged", EMITTER_A)).await?;
    sqlx::query(
        "UPDATE normalized_events SET consumer_visibility = 'candidate',
             migration_correlation_ids = ARRAY['count-test']
         WHERE event_identity = 'candidate'",
    )
    .execute(&mut *conn)
    .await?;
    insert(conn, Row::new("orphaned", 70, "AliasChanged", EMITTER_A)).await?;
    sqlx::query(
        "UPDATE normalized_events SET canonicality_state = 'orphaned'
         WHERE event_identity = 'orphaned'",
    )
    .execute(&mut *conn)
    .await?;
    sqlx::query(
        "INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp,
             canonicality_state)
         VALUES ($1, 'b71-uncle', 71, to_timestamp(71), 'orphaned')",
    )
    .bind(CHAIN)
    .execute(&mut *conn)
    .await?;
    insert(conn, Row::new("uncle", 71, "AliasChanged", EMITTER_A)).await?;
    sqlx::query(
        "UPDATE normalized_events SET block_hash = 'b71-uncle' WHERE event_identity = 'uncle'",
    )
    .execute(&mut *conn)
    .await?;
    let mut unpositioned = Row::new("unpositioned", 0, "AliasChanged", EMITTER_A);
    unpositioned.block = None;
    insert(conn, unpositioned).await?;
    Ok(())
}

async fn counted(conn: &mut PgConnection, after: i64, through: i64) -> Result<BTreeMap<Key, i64>> {
    let rows: Vec<(i16, String, String, String, i64)> = sqlx::query_as(&block_event_counts_sql())
        .bind(CHAIN)
        .bind(after)
        .bind(through)
        .fetch_all(&mut *conn)
        .await?;
    let mut counts = BTreeMap::new();
    for (kind, anchor, namespace, event_kind, delta) in rows {
        ensure!(delta > 0, "a zero count for {anchor}");
        ensure!(
            counts
                .insert((kind, anchor, namespace, event_kind), delta)
                .is_none(),
            "a key counted twice"
        );
    }
    Ok(counts)
}

fn window() -> Option<HistoryBlockWindow> {
    Some(HistoryBlockWindow {
        ranges: vec![ChainBlockRange {
            chain_id: CHAIN.to_owned(),
            from_block: None,
            to_block: Some(PUBLISHED),
        }],
    })
}

async fn read(
    conn: &mut PgConnection,
    filter: &EventHistoryReadFilter,
    cap: Option<u64>,
) -> Result<i64> {
    let limit = cap.map(|cap| i64::try_from(cap + 1)).transpose()?;
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_history_count_query(&mut builder, filter, true, limit);
    Ok(builder
        .build_query_scalar::<i64>()
        .fetch_one(&mut *conn)
        .await?)
}

fn contract(address: &str, namespace: Option<&str>, kind: &str) -> EventHistoryReadFilter {
    EventHistoryReadFilter {
        contract_address: Some(address.to_owned()),
        namespace: namespace.map(str::to_owned),
        event_kinds: vec![kind.to_owned()],
        block_window: window(),
        ..EventHistoryReadFilter::default()
    }
}

fn resolver(address: &str, namespace: &str, kind: &str) -> EventHistoryReadFilter {
    EventHistoryReadFilter {
        resolver: Some(EventHistoryResolverFilter {
            chain_id: CHAIN.to_owned(),
            address: address.to_owned(),
        }),
        namespace: Some(namespace.to_owned()),
        event_kinds: vec![kind.to_owned()],
        block_window: window(),
        ..EventHistoryReadFilter::default()
    }
}

fn stored(
    counts: &BTreeMap<Key, i64>,
    kind: i16,
    anchor: &str,
    namespace: &str,
    event: &str,
) -> i64 {
    counts
        .get(&(
            kind,
            anchor.to_owned(),
            namespace.to_owned(),
            event.to_owned(),
        ))
        .copied()
        .unwrap_or(0)
}

#[tokio::test]
async fn stored_counts_equal_the_reads_they_count() -> Result<()> {
    let database = TestDatabase::create(TestDatabaseConfig::new("history_count_contract")).await?;
    let result = async {
        let mut conn = database.pool().acquire().await?;
        install(&mut conn).await?;
        seed(&mut conn).await?;
        let counts = counted(&mut conn, -1, PUBLISHED).await?;
        // Consecutive block ranges, as consecutive family steps apply them, add up to the whole.
        let mut summed: BTreeMap<Key, i64> = BTreeMap::new();
        for (after, through) in [(-1, 35), (35, 60), (60, 61), (61, PUBLISHED)] {
            for (key, delta) in counted(&mut conn, after, through).await? {
                *summed.entry(key).or_default() += delta;
            }
        }
        ensure!(summed == counts, "ranges {summed:?} against {counts:?}");
        // The rows the seed's classes leave, by trace of the read predicate.
        let expected = [
            ((EMITTER, EMITTER_A, "ens", "AliasChanged"), 2),
            ((EMITTER, EMITTER_A, "ens", "PermissionScopeChanged"), 1),
            ((EMITTER, EMITTER_A, "basenames", "AliasChanged"), 1),
            ((EMITTER, RESOLVER_1, "ens", "AliasChanged"), 1),
            ((EMITTER, RESOLVER_1, "ens", "ResolverChanged"), 1),
            ((EMITTER, REGISTRY, "ens", "ResolverChanged"), 5),
            ((EMITTER, PUBLIC_RESOLVER, "ens", "RecordChanged"), 1),
            ((POINTER_SET, RESOLVER_1, "ens", "ResolverChanged"), 1),
            ((POINTER_SET, RESOLVER_2, "ens", "ResolverChanged"), 2),
            ((POINTER_SET, ZERO, "ens", "ResolverChanged"), 2),
            ((POINTER_CLEARED, RESOLVER_1, "ens", "ResolverChanged"), 3),
            ((POINTER_CLEARED, RESOLVER_2, "ens", "ResolverChanged"), 2),
            ((POINTER_CLEARED, RESOLVER_3, "ens", "ResolverChanged"), 1),
        ]
        .into_iter()
        .map(|((kind, anchor, namespace, event), count)| {
            (
                (
                    kind,
                    anchor.to_owned(),
                    namespace.to_owned(),
                    event.to_owned(),
                ),
                count,
            )
        })
        .collect::<BTreeMap<_, _>>();
        ensure!(counts == expected, "counted {counts:?}");
        // Every key of the seed, counted or not, against the three reads.
        let kinds: Vec<String> =
            sqlx::query_scalar("SELECT DISTINCT event_kind FROM normalized_events ORDER BY 1")
                .fetch_all(&mut *conn)
                .await?;
        let anchors = [
            EMITTER_A,
            REGISTRY,
            RESOLVER_1,
            RESOLVER_2,
            RESOLVER_3,
            ZERO,
            PUBLIC_RESOLVER,
        ];
        let mut failures = Vec::new();
        for anchor in anchors {
            for kind in &kinds {
                let mut overview = 0;
                for namespace in ["ens", "basenames"] {
                    let emitted = stored(&counts, EMITTER, anchor, namespace, kind);
                    overview += emitted;
                    let feed = contract(anchor, Some(namespace), kind);
                    let own = stored(&counts, POINTER_SET, anchor, namespace, kind)
                        + stored(&counts, POINTER_CLEARED, anchor, namespace, kind)
                        + emitted;
                    let pointers = resolver(anchor, namespace, kind);
                    for cap in [None, Some(CAP)] {
                        let walked = read(&mut conn, &feed, cap).await?;
                        if walked != emitted {
                            failures.push(format!(
                                "contract {anchor} {namespace} {kind} cap {cap:?}: \
                                 read {walked}, stored {emitted}"
                            ));
                        }
                        let walked = read(&mut conn, &pointers, cap).await?;
                        if walked != own {
                            failures.push(format!(
                                "resolver {anchor} {namespace} {kind} cap {cap:?}: \
                                 read {walked}, stored {own}"
                            ));
                        }
                    }
                }
                let walked = read(&mut conn, &contract(anchor, None, kind), Some(CAP)).await?;
                if walked != overview {
                    failures.push(format!(
                        "overview {anchor} {kind}: read {walked}, stored {overview}"
                    ));
                }
            }
        }
        ensure!(failures.is_empty(), "{failures:#?}");
        let namespaces: BTreeSet<String> = counts.keys().map(|key| key.2.clone()).collect();
        ensure!(namespaces.len() == 2, "{namespaces:?}");
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

/// The statement reads one block of 2,000 events, among 40,000 events of earlier blocks, through
/// the chain/block indexes in both plan modes, and every read of `normalized_events` it runs, the
/// handoff group and address pair probes included, is keyed. No access path reads the table
/// sequentially.
#[tokio::test]
async fn the_count_statement_reads_its_block_range_by_index() -> Result<()> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("history_count_contract_plan").pool_max_connections(1),
    )
    .await?;
    let result = async {
        let mut conn = database.pool().acquire().await?;
        install(&mut conn).await?;
        let manifest = declare_public_resolver(&mut conn).await?;
        sqlx::query(
            "INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp,
                 canonicality_state)
             SELECT $1, 'b' || block, block, to_timestamp(block), 'canonical'
             FROM generate_series($2, 1000) block",
        )
        .bind(CHAIN)
        .bind(PUBLISHED + 2)
        .execute(&mut *conn)
        .await?;
        sqlx::query(
            "INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
                 manifest_version, chain_id, block_number, block_hash, transaction_hash,
                 transaction_index, log_index, derivation_kind, canonicality_state, after_state,
                 raw_fact_ref)
             SELECT 'bulk-' || n, 'ens', 'AliasChanged', 'ens_v1_registry_l1', 1, $1,
                    1 + n % 999, 'b' || (1 + n % 999), 'tx', 0, n,
                    'ens_v1_unwrapped_authority', 'canonical', '{}',
                    jsonb_build_object('emitting_address', $2::text)
             FROM generate_series(1, 40000) n",
        )
        .bind(CHAIN)
        .bind(EMITTER_A)
        .execute(&mut *conn)
        .await?;
        for log in 0..1800 {
            insert(&mut conn, {
                let mut row = Row::new(
                    format!("busy-{log}"),
                    1000,
                    "PermissionScopeChanged",
                    EMITTER_A,
                );
                row.log = 100_000 + log;
                row
            })
            .await?;
        }
        for log in 0..100 {
            insert(
                &mut conn,
                address_write(1000, 2 * log, "AddressChanged", manifest),
            )
            .await?;
            insert(
                &mut conn,
                address_write(1000, 2 * log + 1, "AddrChanged", manifest),
            )
            .await?;
        }
        sqlx::raw_sql("ANALYZE").execute(&mut *conn).await?;
        sqlx::raw_sql(&format!(
            "PREPARE block_counts (text, bigint, bigint) AS {}",
            block_event_counts_sql()
        ))
        .execute(&mut *conn)
        .await?;
        let mut failures = Vec::new();
        for mode in ["force_generic_plan", "force_custom_plan"] {
            sqlx::raw_sql(&format!("SET plan_cache_mode = {mode}"))
                .execute(&mut *conn)
                .await?;
            let plan: Value = sqlx::query_scalar(&format!(
                "EXPLAIN (ANALYZE, FORMAT JSON) EXECUTE block_counts('{CHAIN}', 999, 1000)"
            ))
            .fetch_one(&mut *conn)
            .await?;
            let mut nodes = Vec::new();
            walk(&plan[0]["Plan"], &mut nodes);
            for node in &nodes {
                let table = node["Relation Name"] == "normalized_events";
                let index = node["Index Name"]
                    .as_str()
                    .is_some_and(|name| name.starts_with("normalized_events_"));
                if table && node["Node Type"] == "Seq Scan" {
                    failures.push(format!(
                        "{mode}: a sequential read of normalized_events: {node}"
                    ));
                }
                if index && node["Index Cond"].as_str().is_none_or(str::is_empty) {
                    failures.push(format!(
                        "{mode}: an unkeyed read of normalized_events: {node}"
                    ));
                }
            }
            let ranged = nodes.iter().any(|node| {
                let cond = node["Index Cond"].as_str().unwrap_or_default();
                matches!(
                    node["Index Name"].as_str(),
                    Some(
                        "normalized_events_chain_block_number_idx"
                            | "normalized_events_chain_block_number_desc_idx"
                    )
                ) && cond.contains("block_number >")
                    && cond.contains("block_number <=")
            });
            if !ranged {
                failures.push(format!(
                    "{mode}: the block range is not an index range: {plan}"
                ));
            }
            let counts: Vec<(i16, String, String, String, i64)> =
                sqlx::query_as(&format!("EXECUTE block_counts('{CHAIN}', 999, 1000)"))
                    .fetch_all(&mut *conn)
                    .await?;
            let busy = counts
                .iter()
                .find(|row| {
                    row.0 == EMITTER && row.1 == EMITTER_A && row.3 == "PermissionScopeChanged"
                })
                .map(|row| row.4);
            let resolver = counts
                .iter()
                .find(|row| row.0 == EMITTER && row.1 == PUBLIC_RESOLVER)
                .map(|row| row.4);
            if busy != Some(1800) || resolver != Some(100) {
                failures.push(format!("{mode}: counted {counts:?}"));
            }
        }
        ensure!(failures.is_empty(), "{failures:#?}");
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

fn walk<'a>(node: &'a Value, output: &mut Vec<&'a Value>) {
    output.push(node);
    for child in node["Plans"].as_array().into_iter().flatten() {
        walk(child, output);
    }
}
