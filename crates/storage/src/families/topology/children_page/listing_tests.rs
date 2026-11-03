//! The stored child pages of TYR-455 over the 2,000-child fixture (`performance_tests.rs`,
//! `install`), with name summaries that leave a third of the children without an expiry and a
//! half without a registration time. Each case names its row of the TYR-455 table.
use anyhow::{Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::Value;
use sqlx::{PgConnection, raw_sql};

use super::super::listing_page::{count_query, page_query};
use super::{
    listing_support::{checked_page, compose_listing},
    performance_tests::{CHAIN, CHILDREN, PARENT, install},
    *,
};
use crate::{ChildrenCurrentOrder, ChildrenCurrentSort, ChildrenCurrentSortValue, NameQuery};

const PLAN_MODES: [&str; 2] = ["force_custom_plan", "force_generic_plan"];
const SORTS: [(ChildrenCurrentSort, &str); 3] = [
    (ChildrenCurrentSort::Name, "project_child_listing_name_idx"),
    (
        ChildrenCurrentSort::ExpiresAt,
        "project_child_listing_expires_idx",
    ),
    (
        ChildrenCurrentSort::RegisteredAt,
        "project_child_listing_registered_idx",
    ),
];
const ORDERS: [ChildrenCurrentOrder; 2] = [ChildrenCurrentOrder::Asc, ChildrenCurrentOrder::Desc];

/// The fixture with its children's name summaries and their stored listing.
async fn listed(conn: &mut PgConnection) -> Result<()> {
    install(conn).await?;
    raw_sql(&format!(
        "INSERT INTO project_name_summary (chain_id, logical_name_id, namespace, serving,
             zero_owner, expiry_listable, search_supported, resolver_listing_eligible,
             resolver_support_exempt, namehash, expires_at, registered_at)
         SELECT '{CHAIN}', 'ens:0x' || lpad(to_hex(n), 64, '0'), 'ens', false, false, false,
                false, false, false, '0x' || lpad(to_hex(n), 64, '0'),
                CASE WHEN n % 3 <> 0 THEN 1000 + n % 7 END,
                CASE WHEN n % 2 = 0 THEN to_timestamp(500 + n % 11) END
         FROM generate_series(1, {CHILDREN}) n"
    ))
    .execute(&mut *conn)
    .await?;
    compose_listing(conn).await?;
    raw_sql("VACUUM ANALYZE project_child_listing")
        .execute(&mut *conn)
        .await?;
    Ok(())
}

fn sorted(
    sort: ChildrenCurrentSort,
    order: ChildrenCurrentOrder,
) -> ChildrenCurrentPageFilter<'static> {
    ChildrenCurrentPageFilter {
        sort,
        order,
        ..ChildrenCurrentPageFilter::default()
    }
}

/// The node under a plan's `Limit` or `Aggregate` that reads the rows, past its init plans.
fn outer(node: &Value) -> &Value {
    node["Plans"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|child| child["Parent Relationship"] != "InitPlan")
        .unwrap_or(&Value::Null)
}

fn has_node(node: &Value, node_type: &str) -> bool {
    node["Node Type"] == node_type
        || node["Plans"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|child| has_node(child, node_type))
}

async fn plan(conn: &mut PgConnection, name: &str, execute: &str) -> Result<Value> {
    let plan: Value = sqlx::query_scalar(&format!(
        "EXPLAIN (ANALYZE, FORMAT JSON) EXECUTE {name} {execute}"
    ))
    .fetch_one(&mut *conn)
    .await?;
    Ok(plan[0]["Plan"].clone())
}

fn quoted(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

// Row 21: under both plan modes, every sort and order, a first page and a page after a cursor is
// a `Limit` of at most six rows directly over a scan of the sort's index with the parent in its
// condition, backwards for a descending page, with no `Sort`.
#[tokio::test]
async fn a_stored_page_is_a_limit_over_the_sorts_index() -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new("listing_page_plan").pool_max_connections(1))
            .await?;
    let result = async {
        let mut conn = database.pool().acquire().await?;
        listed(&mut conn).await?;
        let mut statement = 0;
        for (sort, index) in SORTS {
            for order in ORDERS {
                let filter = sorted(sort, order);
                let first = page(&mut conn, PARENT, &filter, None, None, 5).await?;
                let cursor = first.next_cursor.clone().expect("a second page");
                for cursor in [None, Some(&cursor)] {
                    statement += 1;
                    let name = format!("listing_page_{statement}");
                    let query = page_query(PARENT, &filter, None, cursor, 6);
                    let mut types = vec!["text", "text"];
                    let mut arguments = vec![quoted(PARENT), quoted(PARENT)];
                    if let Some(cursor) = cursor {
                        if let ChildrenCurrentSortValue::Timestamp(value) = cursor.sort_value {
                            types.push("numeric");
                            arguments.push(match value {
                                Some(value) => value.to_string(),
                                None => "NULL".to_owned(),
                            });
                        }
                        types.extend(["text", "text"]);
                        arguments.push(quoted(&cursor.canonical_display_name));
                        arguments.push(quoted(&cursor.child_logical_name_id));
                    }
                    types.push("bigint");
                    arguments.push("6".to_owned());
                    raw_sql(&format!(
                        "PREPARE {name} ({}) AS {}",
                        types.join(", "),
                        query.sql()
                    ))
                    .execute(&mut *conn)
                    .await?;
                    for mode in PLAN_MODES {
                        raw_sql(&format!("SET plan_cache_mode = {mode}"))
                            .execute(&mut *conn)
                            .await?;
                        let plan =
                            plan(&mut conn, &name, &format!("({})", arguments.join(", "))).await?;
                        let label =
                            format!("{mode} {sort:?} {order:?} cursor {}", cursor.is_some());
                        let scan = outer(&plan);
                        ensure!(plan["Node Type"] == "Limit", "{label}: {plan}");
                        ensure!(
                            matches!(
                                scan["Node Type"].as_str(),
                                Some("Index Scan" | "Index Only Scan")
                            ) && scan["Index Name"] == index
                                && scan["Index Cond"]
                                    .as_str()
                                    .is_some_and(|cond| cond.contains("parent_logical_name_id")),
                            "{label}: the page does not scan {index} by parent: {plan}"
                        );
                        let direction = match order {
                            ChildrenCurrentOrder::Asc => "Forward",
                            ChildrenCurrentOrder::Desc => "Backward",
                        };
                        ensure!(scan["Scan Direction"] == direction, "{label}: {plan}");
                        ensure!(!has_node(&plan, "Sort"), "{label}: the page sorts: {plan}");
                        ensure!(
                            scan["Actual Rows"].as_u64().is_some_and(|rows| rows <= 6),
                            "{label}: the scan reads more than the page: {plan}"
                        );
                    }
                }
            }
        }
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

// Rows 4, 7 and 8, and the descending ruling: a walk of every sort in pages of 37 lists every
// child once, in the stored keys' C order with missing timestamps first, with the exact total on
// every page; the descending walk is the ascending one reversed, ties included.
#[tokio::test]
async fn a_walk_lists_every_child_once_and_descending_is_the_exact_reverse() -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new("listing_page_walk").pool_max_connections(1))
            .await?;
    let result = async {
        let mut conn = database.pool().acquire().await?;
        listed(&mut conn).await?;
        for (sort, _) in SORTS {
            let key = match sort {
                ChildrenCurrentSort::Name => "",
                ChildrenCurrentSort::ExpiresAt => "expires_key, ",
                ChildrenCurrentSort::RegisteredAt => "registered_key, ",
            };
            let expected: Vec<String> = sqlx::query_scalar(&format!(
                "SELECT child_logical_name_id FROM project_child_listing
                 WHERE parent_logical_name_id = $1
                 ORDER BY {key}display_key, child_logical_name_id"
            ))
            .bind(PARENT)
            .fetch_all(&mut *conn)
            .await?;
            ensure!(expected.len() == CHILDREN as usize);
            let mut walks = Vec::new();
            for order in ORDERS {
                let filter = sorted(sort, order);
                let mut walked = Vec::new();
                let mut cursor = None;
                loop {
                    let page = page(&mut conn, PARENT, &filter, None, cursor.as_ref(), 37).await?;
                    ensure!(page.total_count == CHILDREN as u64, "{sort:?} {order:?}");
                    walked.extend(page.rows.into_iter().map(|row| row.child_logical_name_id));
                    cursor = page.next_cursor;
                    if cursor.is_none() {
                        break;
                    }
                }
                walks.push(walked);
            }
            ensure!(
                walks[0] == expected,
                "{sort:?}: the ascending walk is out of order"
            );
            let mut reversed = walks[1].clone();
            reversed.reverse();
            ensure!(
                reversed == expected,
                "{sort:?}: the descending walk is not the exact reverse"
            );
        }
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

// Rows 9 and 23: a prefix bounds the name index by a range of the C-collated key, and the page
// and its total equal the relation's.
#[tokio::test]
async fn a_prefix_reads_a_range_of_the_name_key() -> Result<()> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("listing_prefix_plan").pool_max_connections(1),
    )
    .await?;
    let result = async {
        let mut conn = database.pool().acquire().await?;
        listed(&mut conn).await?;
        // The sixteen children 0x10 through 0x1f.
        let prefix = format!("[{}", "0".repeat(62) + "1");
        let filter = ChildrenCurrentPageFilter {
            q: Some(NameQuery::prefix(&prefix)),
            ..ChildrenCurrentPageFilter::default()
        };
        let page = checked_page(&mut conn, PARENT, &filter, None, None, 50).await?;
        ensure!(
            page.total_count == 16 && page.rows.len() == 16,
            "{} rows of {}",
            page.rows.len(),
            page.total_count
        );
        let query = page_query(PARENT, &filter, None, None, 6);
        raw_sql(&format!(
            "PREPARE prefixed (text, text, text, text, text, bigint) AS {}",
            query.sql()
        ))
        .execute(&mut *conn)
        .await?;
        let floor = quoted(&prefix);
        let ceiling = quoted(&format!("{prefix}\u{10FFFF}"));
        let pattern = quoted(&format!("{prefix}%"));
        for mode in PLAN_MODES {
            raw_sql(&format!("SET plan_cache_mode = {mode}"))
                .execute(&mut *conn)
                .await?;
            let plan = plan(
                &mut conn,
                "prefixed",
                &format!("('{PARENT}', '{PARENT}', {floor}, {ceiling}, {pattern}, 6)"),
            )
            .await?;
            let scan = outer(&plan);
            let condition = scan["Index Cond"].as_str().unwrap_or_default();
            ensure!(
                scan["Index Name"] == "project_child_listing_name_idx"
                    && condition.contains("display_key >=")
                    && condition.contains("display_key <"),
                "{mode}: the prefix is not a range of the key: {plan}"
            );
        }
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

// Rows 22 and 26: a filtered total is an `Aggregate` over an index-only scan of the name index
// with the parent in its condition, no CTE and no composition; the unfiltered total is the
// stored count.
#[tokio::test]
async fn a_filtered_total_counts_the_name_index_only() -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new("listing_total_plan").pool_max_connections(1))
            .await?;
    let result = async {
        let mut conn = database.pool().acquire().await?;
        listed(&mut conn).await?;
        let fenced = ChildrenCurrentPageFilter {
            include_expired: false,
            ..ChildrenCurrentPageFilter::default()
        };
        let page = checked_page(&mut conn, PARENT, &fenced, None, None, CHILDREN as u64).await?;
        ensure!(page.total_count == CHILDREN as u64, "{}", page.total_count);
        // `checked_page` stores the listing again, so its pages are vacuumed again for the
        // index-only scan, and the sequential scan a 2,000-row table invites is ruled out.
        raw_sql("VACUUM ANALYZE project_child_listing")
            .execute(&mut *conn)
            .await?;
        raw_sql("SET enable_seqscan = off")
            .execute(&mut *conn)
            .await?;
        let query = count_query(PARENT, &fenced, None);
        raw_sql(&format!("PREPARE fenced (text, text) AS {}", query.sql()))
            .execute(&mut *conn)
            .await?;
        for mode in PLAN_MODES {
            raw_sql(&format!("SET plan_cache_mode = {mode}"))
                .execute(&mut *conn)
                .await?;
            let plan = plan(&mut conn, "fenced", &format!("('{PARENT}', '{PARENT}')")).await?;
            let scan = outer(&plan);
            ensure!(
                plan["Node Type"] == "Aggregate"
                    && scan["Node Type"] == "Index Only Scan"
                    && scan["Index Name"] == "project_child_listing_name_idx"
                    && !has_node(&plan, "CTE Scan"),
                "{mode}: {plan}"
            );
        }
        raw_sql("RESET enable_seqscan").execute(&mut *conn).await?;
        ensure!(count(&mut conn, PARENT, None).await? == CHILDREN as u64);
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

// Row 15: a label only the rainbow import spells keeps its placeholder key, so the child sorts
// and pages by it, and its served name is healed. The next cursor carries the stored key.
#[tokio::test]
async fn an_imported_label_serves_healed_and_pages_by_its_stored_key() -> Result<()> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("listing_imported_label").pool_max_connections(1),
    )
    .await?;
    let result = async {
        let mut conn = database.pool().acquire().await?;
        install(&mut conn).await?;
        let label = format!("0x{:064x}", 1);
        raw_sql(&format!(
            "INSERT INTO label_preimages (labelhash, raw_label, decoded_label,
                 normalizer_version, normalized_under_version, source_kind, source_priority)
             VALUES ('{label}', convert_to('healed', 'UTF8'), 'healed', 'fixture', true,
                 'ens_rainbow_import', 0)"
        ))
        .execute(&mut *conn)
        .await?;
        compose_listing(&mut conn).await?;
        let stored_key = format!("[{}].eth", &label[2..]);
        let filter = ChildrenCurrentPageFilter::default();
        let first = page(&mut conn, PARENT, &filter, None, None, 1).await?;
        ensure!(
            first.rows.len() == 1 && first.rows[0].canonical_display_name == "healed.eth",
            "{first:?}"
        );
        let cursor = first.next_cursor.clone().expect("a next page");
        ensure!(cursor.canonical_display_name == stored_key, "{cursor:?}");
        let second = page(&mut conn, PARENT, &filter, None, Some(&cursor), 1).await?;
        ensure!(
            second.rows.len() == 1
                && second.rows[0].child_logical_name_id == format!("ens:0x{:064x}", 2),
            "{second:?}"
        );
        let stored: String = sqlx::query_scalar(
            "SELECT display_name FROM project_child_listing WHERE child_logical_name_id = $1",
        )
        .bind(format!("ens:{label}"))
        .fetch_one(&mut *conn)
        .await?;
        ensure!(
            stored == stored_key,
            "the stored name reads no import: {stored}"
        );
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}
