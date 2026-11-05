//! Plan and row tests for the fenced catalogue probes (`catalogue_source::push_event_rows`).
//!
//! The planner estimates any block range above the `block_number` histogram of
//! `normalized_events` at about one row, and the newest blocks are always above it. The fixture
//! reproduces that: a dense bucket of unrelated events written after the last `ANALYZE` of
//! `normalized_events`, a thin bucket below it, and an address whose names, resource, node,
//! record id and link sources span both. The plan test checks that the previous statement still
//! scans the dense bucket on the chain-and-block index there, so the fixture keeps proving
//! something. It checks that the fenced statement probes every arm on that arm's key index, with
//! the key in the index condition and no chain-and-block index anywhere in a probe, in both plan
//! modes and with statistics stale and fresh. Two rejected fence shapes must fail the same check:
//! the chain id inside the fence for every source kind, and the fence without the readable terms
//! its partial key indexes need. The row test compares the fenced statements with the previous
//! ones on every input class, in order.

use std::{collections::BTreeMap, io::Write};

use anyhow::{Context, Result, anyhow, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{Execute, PgConnection, Postgres, QueryBuilder, Row, postgres::PgArguments, raw_sql};

use super::{AddressRead, Witness, catalogue_source, plan_tests};
use crate::history::{
    ChainBlockRange, EventHistoryReadFilter, HistoryBlockWindow, HistoryOrder, HistoryScope,
    keyset::HistoryKeyset, paging::push_history_order_terms,
};

const ADDRESS: &str = "0x0000000000000000000000000000000000000a11";
const WALLET_COLD: &str = "0x0000000000000000000000000000000000000b21";
const WALLET_HOT: &str = "0x0000000000000000000000000000000000000b22";
/// The operator (subject) of a long run of account approvals.
const WALLET_ACCOUNT: &str = "0x0000000000000000000000000000000000000b23";
/// The address of a long run of reverse claims.
const WALLET_REVERSE: &str = "0x0000000000000000000000000000000000000b24";
const CHAIN: &str = "ethereum-mainnet";
const DENSE: i64 = 1_000;
const THIN: i64 = 999;
const DENSE_EVENTS: i64 = 20_000;
const HOT_EVENTS: i64 = 3_000;
/// The bucket of the newest event of each hot history.
const HOT_LAST: i64 = (400 + (HOT_EVENTS - 1) * 80) / 256;
const PUBLISHED: i64 = (DENSE + 1) * 256;

/// The readable terms the fence holds before the key, as `push_event_rows` writes them.
const READABLE: &str = "ne.consumer_visibility = 'activated' AND ne.canonicality_state IN ('canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state)";

/// Each arm's key index and a term its index condition must name.
const KEY_INDEXES: [(&str, &str); 9] = [
    ("normalized_events_name_history_idx", "logical_name_id"),
    ("normalized_events_resource_history_idx", "resource_id"),
    ("normalized_events_project_node_history_idx", "'node'"),
    (
        "normalized_events_record_id_write_idx",
        "resolver_record_id",
    ),
    ("normalized_events_event_identity_key", "event_identity"),
    ("normalized_events_history_account_owner_idx", "owner"),
    ("normalized_events_history_account_subject_idx", "subject"),
    ("normalized_events_history_reverse_address_idx", "'address'"),
    ("normalized_events_address_root_permission_idx", "subject"),
];

/// The route's default product kinds (`product_history_event_kinds` in the API).
pub(super) const ROUTE_KINDS: [&str; 26] = [
    "AccountPermissionChanged",
    "AuthorityEpochChanged",
    "AuthorityTransferred",
    "EACRolesChanged",
    "ExpiryChanged",
    "LabelRegistered",
    "MigrationApplied",
    "ParentChanged",
    "PermissionChanged",
    "PermissionScopeChanged",
    "RecordChanged",
    "RecordVersionChanged",
    "RegistrationGranted",
    "RegistrationReleased",
    "RegistrationRenewed",
    "RegistrationReserved",
    "RegistryCreated",
    "ResolverChanged",
    "ResolverRecordLinked",
    "ReverseChanged",
    "RolesChanged",
    "RootPermissionChanged",
    "SubregistryChanged",
    "TokenControlTransferred",
    "TokenRegenerated",
    "Upgraded",
];

#[derive(Clone, Copy)]
enum Statement {
    Candidate {
        bucket: i64,
        limit: i64,
        /// The legacy row-only cursor's selected event.
        identity: Option<&'static str>,
    },
    Seek {
        after: Option<i64>,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Statistics {
    /// The dense bucket was written after the last `ANALYZE` of `normalized_events`.
    Stale,
    Fresh,
}

impl Statistics {
    fn label(self) -> &'static str {
        match self {
            Self::Stale => "stale statistics",
            Self::Fresh => "fresh statistics",
        }
    }
}

/// One input class: the address, the filter's shape and the statement.
struct Case {
    label: &'static str,
    address: &'static str,
    order: HistoryOrder,
    record_key: Option<&'static str>,
    kinds: Option<&'static [&'static str]>,
    window: Option<(i64, i64)>,
    /// Without the published block window, which excludes unpositioned events.
    unbounded: bool,
    /// A second published chain in the window.
    second_chain: bool,
    /// No namespace on the read.
    all_namespaces: bool,
    scope: HistoryScope,
    /// Every probe of the custom plan returns at most a handful of rows.
    few_rows: bool,
    /// Continue after the row at this index of the first page (limit 3) of the same statement.
    continue_after: Option<usize>,
    statement: Statement,
    /// The seek's expected next bucket.
    next_bucket: Option<Option<i64>>,
}

fn cases() -> Vec<Case> {
    let base = |label, statement| Case {
        label,
        address: ADDRESS,
        order: HistoryOrder::Desc,
        record_key: None,
        kinds: None,
        window: None,
        unbounded: false,
        second_chain: false,
        all_namespaces: false,
        scope: HistoryScope::Both,
        few_rows: false,
        continue_after: None,
        statement,
        next_bucket: None,
    };
    let page = Statement::Candidate {
        bucket: DENSE,
        limit: 21,
        identity: None,
    };
    vec![
        base("dense bucket", page),
        base(
            "thin bucket",
            Statement::Candidate {
                bucket: THIN,
                limit: 21,
                identity: None,
            },
        ),
        Case {
            continue_after: Some(2),
            ..base("page 2 inside the dense bucket", page)
        },
        Case {
            next_bucket: Some(Some(THIN)),
            ..base(
                "seek from the dense bucket",
                Statement::Seek { after: Some(DENSE) },
            )
        },
        Case {
            order: HistoryOrder::Asc,
            ..base("dense bucket asc", page)
        },
        Case {
            order: HistoryOrder::Asc,
            next_bucket: Some(Some(DENSE)),
            ..base(
                "seek asc into the dense bucket",
                Statement::Seek { after: Some(THIN) },
            )
        },
        Case {
            window: Some((DENSE * 256 + 10, DENSE * 256 + 30)),
            ..base("block window inside the dense bucket", page)
        },
        Case {
            record_key: Some("text:description"),
            ..base("record key", page)
        },
        Case {
            kinds: Some(&["RecordChanged"]),
            ..base("type subset", page)
        },
        Case {
            unbounded: true,
            ..base(
                "unpositioned bucket",
                Statement::Candidate {
                    bucket: -1,
                    limit: 21,
                    identity: None,
                },
            )
        },
        Case {
            order: HistoryOrder::Asc,
            unbounded: true,
            ..base(
                "unpositioned bucket asc",
                Statement::Candidate {
                    bucket: -1,
                    limit: 21,
                    identity: None,
                },
            )
        },
        Case {
            order: HistoryOrder::Asc,
            unbounded: true,
            next_bucket: Some(Some(-1)),
            ..base("first seek asc", Statement::Seek { after: None })
        },
        Case {
            next_bucket: Some(Some(DENSE)),
            ..base("first seek", Statement::Seek { after: None })
        },
        Case {
            address: WALLET_COLD,
            ..base("wallet with one anchor", page)
        },
        Case {
            address: WALLET_COLD,
            next_bucket: Some(Some(500)),
            ..base("wallet seek", Statement::Seek { after: Some(DENSE) })
        },
        Case {
            address: WALLET_HOT,
            few_rows: true,
            next_bucket: Some(Some(HOT_LAST)),
            ..base("hot source seek", Statement::Seek { after: Some(DENSE) })
        },
        // An ascending seek whose window ends before the hot source's tail: the window bound
        // inside the fence stops each probe at once instead of reading the tail.
        Case {
            address: WALLET_HOT,
            order: HistoryOrder::Asc,
            window: Some((0, 511)),
            few_rows: true,
            next_bucket: Some(None),
            ..base(
                "hot source seek asc under a window end",
                Statement::Seek { after: Some(1) },
            )
        },
        // A first seek reads the direct arms without a bucket: a wallet with a long approval or
        // reverse history must stop at its newest (or oldest) event.
        Case {
            address: WALLET_ACCOUNT,
            few_rows: true,
            next_bucket: Some(Some(HOT_LAST)),
            ..base("hot account first seek", Statement::Seek { after: None })
        },
        Case {
            address: WALLET_ACCOUNT,
            order: HistoryOrder::Asc,
            few_rows: true,
            next_bucket: Some(Some(400 / 256)),
            ..base(
                "hot account first seek asc",
                Statement::Seek { after: None },
            )
        },
        Case {
            address: WALLET_REVERSE,
            few_rows: true,
            next_bucket: Some(Some(HOT_LAST)),
            ..base("hot reverse first seek", Statement::Seek { after: None })
        },
        base(
            "selected event of a row-only cursor",
            Statement::Candidate {
                bucket: DENSE,
                limit: 21,
                identity: Some("fence:record:2"),
            },
        ),
        Case {
            order: HistoryOrder::Asc,
            unbounded: true,
            next_bucket: Some(Some(0)),
            ..base(
                "seek asc after the unpositioned bucket",
                Statement::Seek { after: Some(-1) },
            )
        },
        Case {
            unbounded: true,
            next_bucket: Some(None),
            ..base(
                "seek after the unpositioned bucket",
                Statement::Seek { after: Some(-1) },
            )
        },
        Case {
            all_namespaces: true,
            ..base("every namespace", page)
        },
        Case {
            scope: HistoryScope::Surface,
            ..base("names only", page)
        },
        Case {
            second_chain: true,
            ..base("two published chains", page)
        },
    ]
}

#[tokio::test]
async fn catalogue_probes_read_their_key_index() -> Result<()> {
    with_fixture("catalogue_fence_plans", async |connection| {
        let mut failures = Vec::new();
        let mut chain_inside_failed = false;
        for statistics in [Statistics::Stale, Statistics::Fresh] {
            if statistics == Statistics::Fresh {
                raw_sql("ANALYZE normalized_events")
                    .execute(&mut *connection)
                    .await?;
            }
            for case in cases() {
                let published = published();
                let (read, filter) = inputs(&case, &published);
                let cursor = continuation(connection, &case, &read, &filter).await?;
                let keyset = cursor.as_ref().map(|(cursor, block)| HistoryKeyset {
                    cursor,
                    block_number: *block,
                });
                let (sql, arguments) = render(&read, &filter, keyset.as_ref(), case.statement, true)?;
                let (previous, previous_arguments) =
                    render(&read, &filter, keyset.as_ref(), case.statement, false)?;
                // The rejected shapes, for statements with source probes.
                let variants = match chain_inside(&sql) {
                    Some(chain_variant) => {
                        let scope_variant = scope_shape(&chain_variant, case.order)?;
                        vec![("chain id inside", chain_variant), ("scope shape", scope_variant)]
                    }
                    None => Vec::new(),
                };
                for generic in [false, true] {
                    let mode = if generic { "generic plan" } else { "custom plan" };
                    let cell = format!("{}, {mode}, {}", statistics.label(), case.label);
                    let plan = explain(connection, &sql, arguments.clone(), generic).await?;
                    println!("{cell}: {}", json!(probes(&plan[0]["Plan"])));
                    failures.extend(guard(&cell, &plan, generic));
                    // A seek of a source with a long history stops at its first row in order,
                    // whatever the statistics say about the length of that history.
                    if case.few_rows {
                        // Written past the test harness's capture, so a passing run's log keeps
                        // each seek's plan shape and rows read.
                        let _ = writeln!(
                            std::io::stderr(),
                            "seek plan | {cell} | sorts {} | {}",
                            json!(sorts(&plan[0]["Plan"])),
                            json!(probes(&plan[0]["Plan"]))
                        );
                    }
                    if case.few_rows && !generic {
                        for probe in probes(&plan[0]["Plan"]) {
                            if probe["rows"].as_f64().unwrap_or(0.0) > 4.0 {
                                failures.push(format!(
                                    "{cell}: the seek read more than its first rows: {probe}"
                                ));
                            }
                        }
                    }
                    // The rejected shapes are compared where the fence has probes keyed by a
                    // partial index. A row-only cursor's selected event is keyed by the unique
                    // identity index, which is not partial, so it serves the scope shape too.
                    let keyed = !probes(&plan[0]["Plan"]).is_empty()
                        && !matches!(case.statement, Statement::Candidate { identity: Some(_), .. });
                    for (shape, variant) in &variants {
                        let plan = explain(connection, variant, arguments.clone(), generic).await?;
                        let found = guard(&cell, &plan, generic);
                        println!("{cell}, {shape}: {} guard failures", found.len());
                        if *shape == "chain id inside" {
                            chain_inside_failed |= !found.is_empty();
                        } else if keyed && found.is_empty() {
                            failures.push(format!(
                                "{cell}: the fence without its readable terms passed the guard"
                            ));
                        }
                    }
                    if statistics == Statistics::Stale && !generic && case.label == "dense bucket" {
                        let before =
                            explain(connection, &previous, previous_arguments.clone(), false)
                                .await?;
                        let scans = probes(&before[0]["Plan"]);
                        println!("{cell}, previous statement: {}", json!(scans));
                        if !scans.iter().any(|scan| {
                            index_names(scan).iter().any(|index| index.contains("chain_block_number"))
                                && scan["removed"].as_f64().unwrap_or(0.0) >= 1_000.0
                        }) {
                            failures.push(format!(
                                "{cell}: the previous statement no longer scans the dense bucket on \
                                 the chain-and-block index, so this fixture proves nothing about it"
                            ));
                        }
                    }
                }
            }
        }
        if !chain_inside_failed {
            failures.push(
                "the fence with the chain id inside for every source kind passed the guard in \
                 every cell"
                    .to_owned(),
            );
        }
        ensure!(
            failures.is_empty(),
            "catalogue probe plans:\n{}",
            failures.join("\n\n")
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn catalogue_fenced_probes_keep_rows() -> Result<()> {
    with_fixture("catalogue_fence_rows", async |connection| {
        let mut failures = Vec::new();
        for statistics in [Statistics::Stale, Statistics::Fresh] {
            if statistics == Statistics::Fresh {
                raw_sql("ANALYZE normalized_events")
                    .execute(&mut *connection)
                    .await?;
            }
            for case in cases() {
                let cell = format!("{}, {}", statistics.label(), case.label);
                let published = published();
                let (read, filter) = inputs(&case, &published);
                let cursor = continuation(connection, &case, &read, &filter).await?;
                let keyset = cursor.as_ref().map(|(cursor, block)| HistoryKeyset {
                    cursor,
                    block_number: *block,
                });
                let (sql, arguments) =
                    render(&read, &filter, keyset.as_ref(), case.statement, true)?;
                let (previous, previous_arguments) =
                    render(&read, &filter, keyset.as_ref(), case.statement, false)?;
                let rows = rows_of(connection, &sql, arguments).await?;
                let before = rows_of(connection, &previous, previous_arguments).await?;
                if rows != before {
                    failures.push(format!("{cell}: rows {rows}\nprevious {before}"));
                }
                match (case.statement, case.next_bucket) {
                    (Statement::Seek { .. }, Some(expected)) => {
                        let field = if case.order == HistoryOrder::Asc {
                            "min"
                        } else {
                            "max"
                        };
                        if rows[0][field] != json!(expected) {
                            failures
                                .push(format!("{cell}: next bucket {rows}, expected {expected:?}"));
                        }
                    }
                    (Statement::Candidate { .. }, _)
                        if rows.as_array().is_none_or(Vec::is_empty) =>
                    {
                        failures.push(format!("{cell}: no rows"));
                    }
                    _ => {}
                }
            }
        }
        ensure!(
            failures.is_empty(),
            "catalogue probe rows:\n{}",
            failures.join("\n\n")
        );
        Ok(())
    })
    .await
}

fn published() -> BTreeMap<String, i64> {
    BTreeMap::from([(CHAIN.to_owned(), PUBLISHED)])
}

fn inputs<'a>(
    case: &Case,
    published: &'a BTreeMap<String, i64>,
) -> (AddressRead<'a>, EventHistoryReadFilter) {
    let read = AddressRead {
        address: case.address,
        namespace: (!case.all_namespaces).then_some("ens"),
        relations: None,
        scope: case.scope,
        canonical_only: true,
        published: Some(published),
        catalogue: true,
    };
    let (from_block, to_block) = case
        .window
        .map_or((None, PUBLISHED), |(from, to)| (Some(from), to));
    let filter = EventHistoryReadFilter {
        order: case.order,
        event_kinds: case
            .kinds
            .unwrap_or(&ROUTE_KINDS)
            .iter()
            .map(|kind| (*kind).to_owned())
            .collect(),
        record_key: case.record_key.map(str::to_owned),
        block_window: (!case.unbounded).then(|| HistoryBlockWindow {
            ranges: [(CHAIN, to_block), ("ethereum-sepolia", 10)]
                .into_iter()
                .take(if case.second_chain { 2 } else { 1 })
                .map(|(chain_id, to_block)| ChainBlockRange {
                    chain_id: chain_id.to_owned(),
                    from_block,
                    to_block: Some(to_block),
                })
                .collect(),
        }),
        ..Default::default()
    };
    (read, filter)
}

/// The cursor after the `continue_after`-th row of the case's first page, read with limit 3.
async fn continuation(
    connection: &mut PgConnection,
    case: &Case,
    read: &AddressRead<'_>,
    filter: &EventHistoryReadFilter,
) -> Result<Option<(crate::history::HistoryCursor, Option<i64>)>> {
    let (Some(index), Statement::Candidate { bucket, .. }) = (case.continue_after, case.statement)
    else {
        return Ok(None);
    };
    let mut query = QueryBuilder::<Postgres>::new("");
    catalogue_source::push_candidate_query(&mut query, read, filter, None, bucket, None, 3);
    let rows: Vec<Witness> = query.build_query_as().fetch_all(&mut *connection).await?;
    let row = rows
        .get(index)
        .with_context(|| format!("{}: the first page has no row {index}", case.label))?;
    Ok(Some((row.cursor(), row.block_number)))
}

fn render<'a>(
    read: &'a AddressRead<'a>,
    filter: &'a EventHistoryReadFilter,
    keyset: Option<&HistoryKeyset<'a>>,
    statement: Statement,
    fence: bool,
) -> Result<(String, PgArguments)> {
    let mut builder = QueryBuilder::<Postgres>::new("");
    match statement {
        Statement::Candidate {
            bucket,
            limit,
            identity,
        } => catalogue_source::push_candidate_statement(
            &mut builder,
            read,
            filter,
            keyset,
            bucket,
            identity,
            limit,
            fence,
        ),
        Statement::Seek { after } => {
            catalogue_source::push_seek_statement(&mut builder, read, filter, after, fence);
        }
    }
    let mut query = builder.build();
    let sql = query.sql().to_owned();
    let arguments = query
        .take_arguments()
        .map_err(|error| anyhow!("statement arguments: {error}"))?
        .context("bound statement")?;
    Ok((sql, arguments))
}

/// The fence with `ne.chain_id = source.chain_id` inside for the name, resource and identity
/// keys too. The chain-and-block index can then serve the inner by chain and range. `None` for
/// a statement without source probes.
fn chain_inside(sql: &str) -> Option<String> {
    let mut sql = sql.to_owned();
    for key in [
        "ne.logical_name_id = source.source_key",
        "ne.resource_id = CASE WHEN source.source_kind = 1 THEN source.source_key::uuid END",
        "ne.event_identity = source.source_key",
    ] {
        let fenced = format!("{READABLE} AND {key}");
        if !sql.contains(&fenced) {
            return None;
        }
        sql = sql.replace(
            &fenced,
            &format!("{READABLE} AND ne.chain_id = source.chain_id AND {key}"),
        );
    }
    Some(sql)
}

/// The fence as chain id, key and range only: no readable terms, no order. No partial key
/// index is provable inside it.
fn scope_shape(chain_inside: &str, order: HistoryOrder) -> Result<String> {
    let readable = format!("{READABLE} AND ");
    ensure!(
        chain_inside.contains(&readable),
        "no readable terms in the fence"
    );
    let mut terms = QueryBuilder::<Postgres>::new("");
    push_history_order_terms(&mut terms, order);
    let ordered = format!(" ORDER BY {} OFFSET 0) ne", terms.sql());
    let without_readable = chain_inside.replace(&readable, "");
    if without_readable.contains(&ordered) {
        return Ok(without_readable.replace(&ordered, " OFFSET 0) ne"));
    }
    // A seek's fence ends in its own LIMIT; the probe's lineage join follows it. The probe's
    // own order and LIMIT stay.
    let limited = format!(" ORDER BY {} LIMIT ", terms.sql());
    let mut shape = String::new();
    let mut rest = without_readable.as_str();
    let mut found = false;
    while let Some(at) = rest.find(&limited) {
        let tail = &rest[at + limited.len()..];
        let fence_end = tail
            .find(") ne")
            .is_some_and(|end| tail[end + 4..].trim_start().starts_with("LEFT JOIN"));
        shape.push_str(&rest[..at]);
        shape.push_str(if fence_end { " LIMIT " } else { &limited });
        found |= fence_end;
        rest = tail;
    }
    shape.push_str(rest);
    ensure!(found, "no order in the fence");
    Ok(shape)
}

async fn explain(
    connection: &mut PgConnection,
    sql: &str,
    arguments: PgArguments,
    generic: bool,
) -> Result<Value> {
    if generic {
        raw_sql("SET plan_cache_mode = force_generic_plan")
            .execute(&mut *connection)
            .await?;
        // Unbound parameters: GENERIC_PLAN infers their types from the statement.
        let plan: Value = raw_sql(&format!("EXPLAIN (GENERIC_PLAN, FORMAT JSON) {sql}"))
            .fetch_one(&mut *connection)
            .await
            .context("generic plan")?
            .get(0);
        ensure!(
            plan.to_string().contains('$'),
            "generic plan substituted concrete parameters"
        );
        Ok(plan)
    } else {
        raw_sql("SET plan_cache_mode = force_custom_plan")
            .execute(&mut *connection)
            .await?;
        sqlx::query_scalar_with(
            &format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}"),
            arguments,
        )
        .fetch_one(&mut *connection)
        .await
        .context("custom plan")
    }
}

async fn rows_of(
    connection: &mut PgConnection,
    sql: &str,
    arguments: PgArguments,
) -> Result<Value> {
    sqlx::query_scalar_with(
        &format!("SELECT coalesce(jsonb_agg(to_jsonb(result)), '[]'::jsonb) FROM ({sql}) result"),
        arguments,
    )
    .fetch_one(&mut *connection)
    .await
    .context("statement rows")
}

/// Every `normalized_events` probe of the statement: the scans aliased `ne…`. The duplicate
/// filter's pair lookup reads `normalized_events` as `paired` and is not a probe.
fn probes(node: &Value) -> Vec<Value> {
    let mut found = Vec::new();
    if node["Relation Name"] == "normalized_events"
        && node["Alias"]
            .as_str()
            .is_some_and(|alias| alias.starts_with("ne"))
    {
        let mut indexes = Vec::new();
        let mut conditions = Vec::new();
        bitmap_indexes(node, &mut indexes, &mut conditions);
        found.push(
            json!({"type":node["Node Type"],"alias":node["Alias"],"indexes":indexes,
            "condition":conditions.join(" AND "),"rows":node["Actual Rows"],
            "removed":node["Rows Removed by Filter"],"loops":node["Actual Loops"],
            "shared_hit":node["Shared Hit Blocks"]}),
        );
    }
    for child in node["Plans"].as_array().into_iter().flatten() {
        found.extend(probes(child));
    }
    found
}

fn bitmap_indexes(node: &Value, indexes: &mut Vec<String>, conditions: &mut Vec<String>) {
    if let Some(index) = node["Index Name"].as_str() {
        indexes.push(index.to_owned());
        conditions.push(node["Index Cond"].as_str().unwrap_or_default().to_owned());
    }
    if node["Node Type"] == "Bitmap Heap Scan"
        || node["Node Type"] == "BitmapOr"
        || node["Node Type"] == "BitmapAnd"
    {
        for child in node["Plans"].as_array().into_iter().flatten() {
            bitmap_indexes(child, indexes, conditions);
        }
    }
}

fn index_names(probe: &Value) -> Vec<&str> {
    probe["indexes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect()
}

/// The sort nodes with a probe below them, innermost last.
fn sorts(node: &Value) -> Vec<Value> {
    let mut found = Vec::new();
    for child in node["Plans"].as_array().into_iter().flatten() {
        found.extend(sorts(child));
    }
    let kind = node["Node Type"].as_str().unwrap_or_default();
    if (kind == "Sort" || kind == "Incremental Sort") && !probes(node).is_empty() {
        found.insert(
            0,
            json!({"type":kind,"rows":node["Actual Rows"],"loops":node["Actual Loops"]}),
        );
    }
    found
}

/// Every probe reads its own key index with the key in the index condition, never a
/// chain-and-block index, and in a custom plan filters out at most a handful of rows per loop.
fn guard(cell: &str, plan: &Value, generic: bool) -> Vec<String> {
    let mut failures = Vec::new();
    for probe in probes(&plan[0]["Plan"]) {
        let indexes = index_names(&probe);
        let condition = probe["condition"].as_str().unwrap_or_default();
        if indexes.is_empty() {
            failures.push(format!("{cell}: a probe reads no index: {probe}"));
        }
        for index in indexes {
            match KEY_INDEXES.iter().find(|(name, _)| *name == index) {
                _ if index.contains("chain_block_number") => failures.push(format!(
                    "{cell}: a probe reads the chain-and-block index: {probe}"
                )),
                Some((_, term)) if condition.contains(term) => {}
                _ => failures.push(format!(
                    "{cell}: a probe is not on its key index with the key in the condition: {probe}"
                )),
            }
        }
        if !generic && probe["removed"].as_f64().unwrap_or(0.0) > 16.0 {
            failures.push(format!(
                "{cell}: a probe filtered out more than its key's rows: {probe}"
            ));
        }
    }
    failures
}

async fn with_fixture(
    name: &str,
    test: impl AsyncFnOnce(&mut PgConnection) -> Result<()>,
) -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new(name).pool_max_connections(1)).await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        plan_tests::install(&mut connection, 32, 320).await?;
        install(&mut connection).await?;
        raw_sql("SET jit = off").execute(&mut *connection).await?;
        test(&mut connection).await
    }
    .await;
    database.cleanup().await?;
    result
}

/// Adds the dense and thin buckets, the hot and cold wallets, the hot approval and reverse
/// histories and an unpositioned event, then
/// builds the catalogue rows. `normalized_events` is not analyzed again, so every new block lies
/// above its `block_number` histogram until a test analyzes it.
async fn install(connection: &mut PgConnection) -> Result<()> {
    let dense_lo = DENSE * 256;
    let thin_lo = THIN * 256;
    raw_sql(&format!(r#"
      ALTER TABLE normalized_events SET (autovacuum_enabled = off);
      INSERT INTO chain_lineage(chain_id,block_hash,block_number,block_timestamp,canonicality_state)
      SELECT '{CHAIN}','fence-block-'||b,b,to_timestamp(b),'canonical'::canonicality_state
      FROM (SELECT generate_series({dense_lo},{dense_lo}+255)
        UNION SELECT {thin_lo}+n*5 FROM generate_series(1,40) n
        UNION SELECT {thin_lo}+50+n FROM generate_series(1,2) n
        UNION SELECT 400+n*80 FROM generate_series(0,{HOT_EVENTS}-1) n
        UNION SELECT 128000) blocks(b);
      INSERT INTO normalized_events(event_identity,namespace,logical_name_id,resource_id,event_kind,source_family,manifest_version,chain_id,block_hash,block_number,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state)
      SELECT 'fence:dense:'||n,'ens','ens:0x'||lpad(to_hex(k),64,'0'),lpad(to_hex(k),32,'0')::uuid,'RegistrationRenewed','ens_v2_registry_l1',1,'{CHAIN}','fence-block-'||b,b,'fence-tx-'||n,0,n,'ens_v2_registry_resource_surface','canonical'::canonicality_state,'{{}}'::jsonb
      FROM generate_series(1,{DENSE_EVENTS}) n CROSS JOIN LATERAL (SELECT 33 + n % 307 AS k, {dense_lo} + n % 256 AS b) x
      UNION ALL
      SELECT 'fence:thin:'||n,'ens','ens:0x'||lpad(to_hex(33+n),64,'0'),lpad(to_hex(33+n),32,'0')::uuid,'RegistrationRenewed','ens_v2_registry_l1',1,'{CHAIN}','fence-block-'||({thin_lo}+n*5),{thin_lo}+n*5,'fence-tx-thin-'||n,0,n,'ens_v2_registry_resource_surface','canonical'::canonicality_state,'{{}}'::jsonb
      FROM generate_series(1,40) n
      UNION ALL
      SELECT 'fence:renew:'||n,'ens','ens:0x'||lpad(to_hex(n),64,'0'),lpad(to_hex(n),32,'0')::uuid,'RegistrationRenewed','ens_v2_registry_l1',1,'{CHAIN}','fence-block-'||({dense_lo}+n*7),{dense_lo}+n*7,'fence-tx-renew-'||n,0,100000+n,'ens_v2_registry_resource_surface','canonical'::canonicality_state,'{{}}'::jsonb
      FROM generate_series(1,4) n
      UNION ALL
      SELECT 'fence:thin-renew:'||n,'ens','ens:0x'||lpad(to_hex(n),64,'0'),lpad(to_hex(n),32,'0')::uuid,'RegistrationRenewed','ens_v2_registry_l1',1,'{CHAIN}','fence-block-'||({thin_lo}+50+n),{thin_lo}+50+n,'fence-tx-thin-renew-'||n,0,100000+n,'ens_v2_registry_resource_surface','canonical'::canonicality_state,'{{}}'::jsonb
      FROM generate_series(1,2) n
      UNION ALL
      SELECT 'fence:hot:'||n,'ens','ens:0x'||lpad(to_hex(340),64,'0'),lpad(to_hex(340),32,'0')::uuid,'RegistrationRenewed','ens_v2_registry_l1',1,'{CHAIN}','fence-block-'||(400+n*80),400+n*80,'fence-tx-hot-'||n,0,0,'ens_v2_registry_resource_surface','canonical'::canonicality_state,'{{}}'::jsonb
      FROM generate_series(0,{HOT_EVENTS}-1) n
      UNION ALL
      SELECT 'fence:hot:dense','ens','ens:0x'||lpad(to_hex(340),64,'0'),lpad(to_hex(340),32,'0')::uuid,'RegistrationRenewed','ens_v2_registry_l1',1,'{CHAIN}','fence-block-'||({dense_lo}+200),{dense_lo}+200,'fence-tx-hot-dense',0,1,'ens_v2_registry_resource_surface','canonical'::canonicality_state,'{{}}'::jsonb
      UNION ALL
      SELECT 'fence:cold:'||b,'ens','ens:0x'||lpad(to_hex(341),64,'0'),lpad(to_hex(341),32,'0')::uuid,'RegistrationRenewed','ens_v2_registry_l1',1,'{CHAIN}','fence-block-'||b,b,'fence-tx-cold-'||b,0,2,'ens_v2_registry_resource_surface','canonical'::canonicality_state,'{{}}'::jsonb
      FROM unnest(ARRAY[128000,{dense_lo}+100]) b
      UNION ALL
      SELECT 'fence:record:'||n,'ens',NULL,NULL,'RecordChanged','ens_v1_resolver_l1',1,'{CHAIN}','fence-block-'||({dense_lo}+n*7),{dense_lo}+n*7,'fence-tx-renew-'||n,0,100010+n,'ens_v1_unwrapped_authority','canonical'::canonicality_state,jsonb_build_object('resolver','0x0000000000000000000000000000000000000d11','node','0x'||lpad(to_hex(n),64,'0'),'record_key','text:description','value','fence fixture')
      FROM generate_series(1,4) n
      UNION ALL
      SELECT 'fence:id-record:'||n,'ens',NULL,NULL,'RecordChanged','ens_v2_resolver_l1',1,'{CHAIN}','fence-block-'||({dense_lo}+n*7),{dense_lo}+n*7,'fence-tx-renew-'||n,0,100020+n,'ens_v2_resolver','canonical'::canonicality_state,jsonb_build_object('resolver','0x0000000000000000000000000000000000000d11','storage_model','resolver_record_id','resolver_record_id','record-'||n,'record_key','text:description','value','fence fixture')
      FROM generate_series(1,4) n;
      INSERT INTO normalized_events(event_identity,namespace,logical_name_id,event_kind,source_family,manifest_version,chain_id,derivation_kind,canonicality_state)
      VALUES ('fence:unpositioned','ens','ens:0x'||lpad(to_hex(1),64,'0'),'RegistrationRenewed','ens_v1_registrar_l1',1,'{CHAIN}','ens_v1_unwrapped_authority','canonical');
      INSERT INTO normalized_events(event_identity,namespace,event_kind,source_family,manifest_version,chain_id,block_hash,block_number,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state)
      SELECT 'fence:hot-account:'||n,'ens','AccountPermissionChanged','ens_v1_registry_l1',1,'{CHAIN}','fence-block-'||(400+n*80),400+n*80,'fence-tx-hot-account-'||n,0,3,'ens_v1_unwrapped_authority','canonical'::canonicality_state,jsonb_build_object('subject','{WALLET_ACCOUNT}','relation_kind','operator','scope',jsonb_build_object('kind','account','owner','0x'||lpad(to_hex(n+2000000),40,'0')))
      FROM generate_series(0,{HOT_EVENTS}-1) n
      UNION ALL
      SELECT 'fence:hot-reverse:'||n,'ens','ReverseChanged','ens_v1_reverse_registrar_l1',1,'{CHAIN}','fence-block-'||(400+n*80),400+n*80,'fence-tx-hot-reverse-'||n,0,4,'ens_v1_reverse_claim','canonical'::canonicality_state,jsonb_build_object('address','{WALLET_REVERSE}','name','fence-'||n||'.eth')
      FROM generate_series(0,{HOT_EVENTS}-1) n;
      INSERT INTO project_history_source(chain_id,source_kind,source_key,first_bucket,last_bucket,bucket_range,event_mask,key_bloom)
      SELECT chain_id,kind,key,min(block_number/256),max(block_number/256),int8range(min(block_number/256),max(block_number/256)+1),9223372036854775807,~B'0'::bit(256)
      FROM (SELECT chain_id,0::smallint AS kind,logical_name_id AS key,block_number FROM normalized_events WHERE logical_name_id IS NOT NULL AND block_number IS NOT NULL
        UNION ALL SELECT chain_id,1::smallint,resource_id::text,block_number FROM normalized_events WHERE resource_id IS NOT NULL AND block_number IS NOT NULL
        UNION ALL SELECT chain_id,2::smallint,lower(after_state->>'node'),block_number FROM normalized_events WHERE event_kind IN ('RecordChanged','RecordVersionChanged') AND after_state->>'node' IS NOT NULL) sources
      GROUP BY chain_id,kind,key;
      INSERT INTO project_history_source(chain_id,source_kind,source_key,resolver_address,first_bucket,last_bucket,bucket_range,event_mask,key_bloom)
      SELECT chain_id,3,after_state->>'resolver_record_id',lower(after_state->>'resolver'),min(block_number/256),max(block_number/256),int8range(min(block_number/256),max(block_number/256)+1),9223372036854775807,~B'0'::bit(256)
      FROM normalized_events WHERE event_kind='RecordChanged' AND after_state->>'storage_model'='resolver_record_id' GROUP BY chain_id,after_state->>'resolver_record_id',lower(after_state->>'resolver');
      UPDATE project_history_source SET first_bucket=-1,bucket_range=int8range(-1,last_bucket+1) WHERE source_kind=0 AND source_key='ens:0x'||lpad(to_hex(1),64,'0');
      INSERT INTO project_address_history_anchor(chain_id,address,namespace,anchor_kind,anchor_id,current_mask,historical_mask,first_bucket,last_bucket,bucket_range,event_mask,key_bloom)
      SELECT event.chain_id,lower(event.after_state->>'registrant'),'ens',source.source_kind,source.source_key,0,1,source.first_bucket,source.last_bucket,source.bucket_range,source.event_mask,source.key_bloom FROM normalized_events event
        JOIN project_history_source source ON source.chain_id=event.chain_id AND ((source.source_kind=0 AND source.source_key=event.logical_name_id) OR (source.source_kind=1 AND source.source_key=event.resource_id::text)) WHERE event.event_kind='RegistrationGranted'
      UNION ALL
      SELECT source.chain_id,wallet.address,'ens',0,source.source_key,0,1,source.first_bucket,source.last_bucket,source.bucket_range,source.event_mask,source.key_bloom
      FROM (VALUES ('{WALLET_COLD}',341),('{WALLET_HOT}',340)) wallet(address,n)
      JOIN project_history_source source ON source.source_kind=0 AND source.source_key='ens:0x'||lpad(to_hex(wallet.n),64,'0');
      INSERT INTO project_history_source_edge(chain_id,resource_id,source_kind,source_key,pointer_event_identity,pointer_resolver,node,pointer_block_number,first_bucket,last_bucket,bucket_range,event_mask,key_bloom)
      SELECT event.chain_id,event.resource_id,2,source.source_key,event.event_identity,lower(event.after_state->>'resolver'),source.source_key,event.block_number,source.first_bucket,source.last_bucket,source.bucket_range,source.event_mask,source.key_bloom FROM normalized_events event JOIN project_history_source source ON source.chain_id=event.chain_id AND source.source_kind=2 AND source.source_key=lower(event.after_state->>'node') WHERE event.event_kind='ResolverChanged';
      INSERT INTO project_history_source_edge(chain_id,resource_id,source_kind,source_key,source_resolver,pointer_event_identity,link_event_identity,pointer_resolver,node,pointer_block_number,link_block_number,first_bucket,last_bucket,bucket_range,event_mask,key_bloom)
      SELECT pointer.chain_id,pointer.resource_id,3,source.source_key,source.resolver_address,pointer.event_identity,link.event_identity,source.resolver_address,lower(pointer.after_state->>'node'),pointer.block_number,link.block_number,source.first_bucket,source.last_bucket,source.bucket_range,source.event_mask,source.key_bloom
      FROM normalized_events pointer JOIN normalized_events link ON link.chain_id=pointer.chain_id AND link.event_kind='ResolverRecordLinked' AND lower(link.after_state->>'resolver')=lower(pointer.after_state->>'resolver') AND lower(link.after_state->>'node') IN (lower(pointer.after_state->>'node'),'0x0000000000000000000000000000000000000000000000000000000000000000')
      JOIN project_history_source source ON source.chain_id=link.chain_id AND source.source_kind=3 AND source.source_key=link.after_state->>'resolver_record_id' AND source.resolver_address=lower(link.after_state->>'resolver') WHERE pointer.event_kind='ResolverChanged';
      ANALYZE chain_lineage, project_history_source, project_address_history_anchor, project_history_source_edge;
    "#)).execute(&mut *connection).await?;
    Ok(())
}
