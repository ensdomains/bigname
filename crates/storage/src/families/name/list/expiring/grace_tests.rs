//! Grace selectors use their stored deadline directly, including exact values and ties.
//! This fixture isolates selector cost; API tests separately exercise composed publications.
use super::*;
use crate::families::id_index_plan_tests::{PLAN_MODES, with_database};
use sqlx::raw_sql;

fn work(plan: &Value) -> (u64, f64) {
    let mut nodes = 1;
    let mut rows =
        plan["Actual Rows"].as_f64().unwrap_or(0.0) * plan["Actual Loops"].as_f64().unwrap_or(0.0);
    for child in plan["Plans"].as_array().into_iter().flatten() {
        let (n, r) = work(child);
        nodes += n;
        rows += r;
    }
    (nodes, rows)
}

fn assert_deadline_index(plan: &Value, index: &str) -> Result<bool> {
    let mut found = false;
    if plan["Index Name"] == index {
        let condition = plan["Index Cond"]
            .as_str()
            .context("grace index has no seek condition")?;
        ensure!(
            condition.contains("namespace") && condition.contains("grace_ends_at"),
            "grace index did not seek namespace and deadline: {condition}"
        );
        found = true;
    }
    for child in plan["Plans"].as_array().into_iter().flatten() {
        found |= assert_deadline_index(child, index)?;
    }
    Ok(found)
}

#[tokio::test]
async fn grace_selector_bounds_pages_and_uses_date_indexes() -> Result<()> {
    with_database("family_grace_selector", async |conn| {
        super::tests::install_fixture(conn).await?;
        // Keep the expected order, then deliberately separate expiry from grace. A query using
        // expiry accidentally has no matches, even when the two deadlines used to coincide.
        let ascending = super::tests::listed(conn, false).await?;
        let descending = super::tests::listed(conn, true).await?;
        raw_sql("UPDATE project_name_summary SET grace_ends_at = expires_at, expires_at = -100; ANALYZE project_name_summary; SET enable_seqscan = off")
            .execute(&mut *conn).await?;
        let bytes: (i64, i64, i64) = sqlx::query_as("SELECT (SELECT sum(pg_column_size(grace_ends_at))::bigint FROM project_name_summary), pg_relation_size('project_name_summary_grace_idx'), pg_relation_size('project_name_summary_authority_grace_idx')")
            .fetch_one(&mut *conn).await?;
        eprintln!("grace storage bytes: column={}, namespace_index={}, authority_index={}", bytes.0, bytes.1, bytes.2);
        for (order, all) in [(NameCurrentListOrder::Asc, &ascending), (NameCurrentListOrder::Desc, &descending)] {
            for count in [1, 32] {
                for (after, before) in [("1849999999", "1850000001"), ("1800200000.5", "1800201001"), ("1900000000", "1900000001"),
                    ("9223372036854775808", "9223372036854780000")] {
                    let mut windows = vec![NameCurrentExpiryWindow { expires_after: Some(after.parse()?), expires_before: Some(before.parse()?) }];
                    for i in 1..count { windows.push(NameCurrentExpiryWindow { expires_after: UnixSeconds::from_seconds(2_000_000_000 + i*2), expires_before: UnixSeconds::from_seconds(2_000_000_001 + i*2) }); }
                    for authority in [None, Some(vec!["ens_v1".to_owned()])] {
                        let filter = NameCurrentExpiringFilter { deadline: crate::NameCurrentDeadline::GraceEnds, namespace: "ens".to_owned(), windows: windows.clone(), authorities: authority, parent: None };
                        let expected: Vec<_> = all.iter().filter(|(id, at, ..)| {
                            let at: UnixSeconds = at.parse().unwrap();
                            let n = usize::from_str_radix(id.trim_start_matches("ens:0x"),16).unwrap();
                            windows.iter().any(|w| at >= w.expires_after.unwrap() && at < w.expires_before.unwrap()) && (filter.authorities.is_none() || n % 4 == 1)
                        }).collect();
                        for page_size in [1, 200] {
                            let mut cursor = None;
                            for offset in [0, page_size as usize] {
                                let actual: Vec<String> = expiring_union_query("", &filter, order, cursor.as_ref(), page_size+1)?.build_query_scalar().fetch_all(&mut *conn).await?;
                                let expected_page: Vec<_> = expected.iter().skip(offset).take(page_size as usize + 1).map(|(id, ..)| id.as_str()).collect();
                                assert_eq!(actual.iter().map(String::as_str).collect::<Vec<_>>(), expected_page);
                                if actual.len() <= page_size as usize { break; }
                                let (_, at, name, namehash) = expected[offset + page_size as usize - 1];
                                cursor = Some(NameCurrentListCursor { sort_value: NameCurrentListCursorValue::Timestamp(Some(at.parse()?)), namespace: "ens".to_owned(), normalized_name: name.clone(), namehash: namehash.clone() });
                            }
                            for mode in PLAN_MODES {
                                let evidence = super::measure::explain_expiring_selection(conn, &filter, order, None, page_size, mode).await?;
                                let plan = &evidence["plan"][0]["Plan"];
                                let text = plan.to_string();
                                let index = if filter.authorities.is_some() { "project_name_summary_authority_grace_idx" } else { "project_name_summary_grace_idx" };
                                ensure!(assert_deadline_index(plan, index)?, "grace selector did not use {index}: {text}");
                                ensure!(!text.contains("project_name_summary_expiry_idx"), "grace fell back to expiry index");
                                let (nodes, rows) = work(plan);
                                eprintln!("grace work: windows={count} page={page_size} bounds={after}..{before} authority={:?} order={order:?} mode={mode} statements=1 nodes={nodes} rows_across_nodes={rows}", filter.authorities);
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }).await
}

#[tokio::test]
async fn lifecycle_schema_migration_fences_old_payload_and_is_repeatable() -> Result<()> {
    with_database("lifecycle_schema_migration", async |conn| {
        super::tests::install_fixture(conn).await?;
        raw_sql("ALTER TABLE project_name_summary DROP COLUMN grace_ends_at; ALTER TABLE project_name_summary DROP CONSTRAINT project_name_summary_search_check; ALTER TABLE project_name_summary ADD CONSTRAINT project_name_summary_search_check CHECK (NOT search_supported OR search_fields ? 'registration_status')")
            .execute(&mut *conn).await?;
        let migration = include_str!("../../../../../../../migrations/20261008120000_registration_lifecycle.sql");
        raw_sql(migration).execute(&mut *conn).await?;
        let counts: (i64,i64) = sqlx::query_as("SELECT (SELECT count(*) FROM project_name_summary), (SELECT count(*) FROM project_family_marker)").fetch_one(&mut *conn).await?;
        assert_eq!(counts, (0,0));
        raw_sql("INSERT INTO project_name_summary (chain_id, logical_name_id, namespace, serving, zero_owner, expiry_listable, search_supported, search_fields) VALUES ('test', 'ens:rebuilt', 'ens', false, false, false, true, '{\"status\":\"unregistered\"}')")
            .execute(&mut *conn).await?;
        raw_sql(migration).execute(&mut *conn).await?;
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM project_name_summary").fetch_one(&mut *conn).await?;
        assert_eq!(count, 1);
        assert!(sqlx::query("UPDATE project_name_summary SET search_fields = '{\"registration_status\":\"registered\"}'").execute(&mut *conn).await.is_err());
        Ok(())
    }).await
}

fn summary_access(plan: &Value, accesses: &mut Vec<Value>) {
    if plan["Relation Name"] == "project_name_summary"
        || plan["Index Name"]
            .as_str()
            .is_some_and(|index| index.starts_with("project_name_summary_"))
    {
        accesses.push(serde_json::json!({
            "node": plan["Node Type"], "index": plan["Index Name"],
            "condition": plan["Index Cond"], "filter": plan["Filter"],
            "rows": plan["Actual Rows"], "loops": plan["Actual Loops"],
            "rows_removed": plan["Rows Removed by Filter"],
            "shared_hits": plan["Shared Hit Blocks"], "shared_reads": plan["Shared Read Blocks"],
        }));
    }
    for child in plan["Plans"].as_array().into_iter().flatten() {
        summary_access(child, accesses);
    }
}

#[tokio::test]
async fn grace_selector_unforced_plans_include_actual_continuations() -> Result<()> {
    with_database("family_grace_unforced", async |conn| {
        super::tests::install_fixture(conn).await?;
        let all = super::tests::listed(conn, false).await?;
        // Leave PostgreSQL free to choose sequential, bitmap or index access. The earlier
        // forced-index test establishes available paths, not the natural planner choice.
        raw_sql("UPDATE project_name_summary SET grace_ends_at = expires_at, expires_at = -100; ANALYZE project_name_summary; RESET enable_seqscan")
            .execute(&mut *conn).await?;
        let sequential: String = sqlx::query_scalar("SHOW enable_seqscan")
            .fetch_one(&mut *conn).await?;
        ensure!(sequential == "on", "unforced plans require sequential scans enabled");
        for count in [1, 32] {
            for (density, after, before) in [
                ("dense_ties", "1849999999", "1850000001"),
                ("sparse", "1800200000.5", "1800205001"),
            ] {
                let mut windows = vec![NameCurrentExpiryWindow {
                    expires_after: Some(after.parse()?), expires_before: Some(before.parse()?),
                }];
                for i in 1..count {
                    windows.push(NameCurrentExpiryWindow {
                        expires_after: UnixSeconds::from_seconds(2_000_000_000 + i*2),
                        expires_before: UnixSeconds::from_seconds(2_000_000_001 + i*2),
                    });
                }
                let filter = NameCurrentExpiringFilter {
                    deadline: crate::NameCurrentDeadline::GraceEnds,
                    namespace: "ens".to_owned(), windows, authorities: None, parent: None,
                };
                let expected: Vec<_> = all.iter().filter(|(_, at, ..)| {
                    let at: UnixSeconds = at.parse().unwrap();
                    filter.windows.iter().any(|window| at >= window.expires_after.unwrap()
                        && at < window.expires_before.unwrap())
                }).collect();
                let first: Vec<String> = expiring_union_query("", &filter,
                    NameCurrentListOrder::Asc, None, 2)?.build_query_scalar()
                    .fetch_all(&mut *conn).await?;
                ensure!(first.len() == 2, "fixture needs a real page-size-one continuation");
                assert_eq!(first[0], expected[0].0);
                // This is the cursor the real selector's first page of size one emits.
                // Continuation may change page_size, which is intentionally not cursor-bound.
                let (_, at, name, namehash) = expected[0];
                let cursor = NameCurrentListCursor {
                    sort_value: NameCurrentListCursorValue::Timestamp(Some(at.parse()?)),
                    namespace: "ens".to_owned(), normalized_name: name.clone(), namehash: namehash.clone(),
                };
                for page_size in [1, 200] {
                    for (shape, position, offset) in [("first", None, 0), ("continuation", Some(&cursor), 1)] {
                        let actual: Vec<String> = expiring_union_query("", &filter,
                            NameCurrentListOrder::Asc, position, page_size+1)?.build_query_scalar()
                            .fetch_all(&mut *conn).await?;
                        let wanted: Vec<_> = expected.iter().skip(offset).take(page_size as usize+1)
                            .map(|(id, ..)| id.as_str()).collect();
                        assert_eq!(actual.iter().map(String::as_str).collect::<Vec<_>>(), wanted);
                        for mode in PLAN_MODES {
                            let evidence = super::measure::explain_expiring_selection(conn, &filter,
                                NameCurrentListOrder::Asc, position, page_size, mode).await?;
                            assert_eq!(evidence["generic_plans"], u64::from(mode == "force_generic_plan"));
                            assert_eq!(evidence["custom_plans"], u64::from(mode == "force_custom_plan"));
                            let plan = &evidence["plan"][0]["Plan"];
                            let (nodes, rows) = work(plan);
                            let mut accesses = Vec::new();
                            summary_access(plan, &mut accesses);
                            ensure!(!accesses.is_empty(), "unforced plan must read the summary selector");
                            // A tiny representative table can legitimately favor a sequential
                            // scan. Record that choice; do not turn it into an index-forcing claim.
                            eprintln!("grace unforced: {}", serde_json::json!({
                                "windows":count, "page_size":page_size, "density":density,
                                "shape":shape, "mode":mode, "enable_seqscan":sequential,
                                "cursor_deadline": position.map(|_| at),
                                "cursor_name": position.map(|_| name),
                                "statements":1, "nodes":nodes, "rows_across_nodes":rows,
                                "returned_selector_keys":plan["Actual Rows"],
                                "execution_ms":evidence["plan"][0]["Execution Time"],
                                "shared_hits":plan["Shared Hit Blocks"],
                                "shared_reads":plan["Shared Read Blocks"],
                                "summary_access":accesses,
                            }));
                        }
                    }
                }
            }
        }
        Ok(())
    }).await
}
