//! Exercise the actual bounded union statement against the reader's existing selector fixture.
//! This checks correctness and available index paths; representative IO/latency measurements
//! remain a separate gate and must not infer production costs from a small fixture.
use sqlx::{Row, raw_sql};

use super::*;
use crate::families::id_index_plan_tests::{PLAN_MODES, with_database};

fn filter(count: usize) -> Result<NameCurrentExpiringFilter> {
    let mut windows = vec![NameCurrentExpiryWindow {
        expires_after: Some("1849999999".parse()?),
        expires_before: Some("1850000001".parse()?),
    }];
    if count > 1 {
        windows.extend([
            NameCurrentExpiryWindow {
                expires_after: Some("9223372036854775808".parse()?),
                expires_before: Some("9223372036854780000".parse()?),
            },
            NameCurrentExpiryWindow {
                expires_after: Some("1800200000.5".parse()?),
                expires_before: Some("1800350000".parse()?),
            },
        ]);
        for index in windows.len()..count {
            let after = 2_000_000_000 + index as i128 * 1000;
            windows.push(NameCurrentExpiryWindow {
                expires_after: UnixSeconds::from_seconds(after),
                expires_before: UnixSeconds::from_seconds(after + 1),
            });
        }
    }
    Ok(NameCurrentExpiringFilter {
        namespace: "ens".to_owned(),
        windows,
        authorities: None,
        parent: None,
    })
}

#[tokio::test]
async fn expiring_union_seeks_each_window_before_global_order_and_limit() -> Result<()> {
    with_database("family_expiring_union", async |connection| {
        super::tests::install_fixture(connection).await?;
        raw_sql("SET enable_seqscan = off").execute(&mut *connection).await?;
        for descending in [false, true] {
            let order = if descending { NameCurrentListOrder::Desc } else { NameCurrentListOrder::Asc };
            let all = super::tests::listed(connection, descending).await?;
            for count in [1, 7, 32] {
                for (authorities, parent) in [
                    (None, None),
                    (Some(vec!["ens_v1".to_owned()]), Some("eth")),
                    (Some(vec!["ens_v0".to_owned(), "ens_v2".to_owned()]), Some("p.eth")),
                    (None, Some("empty.eth")),
                ] {
                    let mut filter = filter(count)?;
                    filter.authorities = authorities;
                    filter.parent = parent.map(str::to_owned);
                    filter.validate_windows()?;
                    let expected = all.iter().filter(|(id, at, name, _)| {
                        let at: UnixSeconds = at.parse().unwrap();
                        let n = usize::from_str_radix(id.trim_start_matches("ens:0x"), 16).unwrap();
                        let authority = [Some("ens_v0"), Some("ens_v1"), Some("ens_v2"), None][n % 4];
                        filter.windows.iter().any(|window| window.expires_after.is_none_or(|after| at >= after)
                            && window.expires_before.is_none_or(|before| at < before))
                            && filter.authorities.as_ref().is_none_or(|values| authority.is_some_and(|authority| values.iter().any(|v| v == authority)))
                            && parent.is_none_or(|parent| name.strip_suffix(&format!(".{parent}")).is_some_and(|label| !label.contains('.')))
                    }).collect::<Vec<_>>();
                    for mode in PLAN_MODES {
                        raw_sql(&format!("SET plan_cache_mode = {mode}")).execute(&mut *connection).await?;
                        let plan: Vec<String> = expiring_union_query("EXPLAIN (COSTS OFF) ", &filter, order, None, 8)?
                            .build().fetch_all(&mut *connection).await?.iter()
                            .map(|row| row.try_get(0).map_err(anyhow::Error::from)).collect::<Result<_>>()?;
                        let plan = plan.join("\n");
                        ensure!(plan.contains("project_name_summary_") && !plan.contains("Seq Scan on project_name_summary"),
                            "{count} windows {mode} {order:?} did not use the selector indexes: {plan}");
                        let mut selected = Vec::new();
                        let mut cursor = None;
                        loop {
                            let names: Vec<String> = expiring_union_query("", &filter, order, cursor.as_ref(), 8)?
                                .build_query_scalar().fetch_all(&mut *connection).await?;
                            ensure!(names.len() <= 8, "union exceeded the global key bound");
                            let more = names.len() > 7;
                            selected.extend(names.into_iter().take(7));
                            if !more { break; }
                            let (_, at, name, namehash) = expected[selected.len() - 1];
                            cursor = Some(NameCurrentListCursor {
                                sort_value: NameCurrentListCursorValue::Timestamp(Some(at.parse()?)),
                                namespace: "ens".to_owned(), normalized_name: name.clone(), namehash: namehash.clone(),
                            });
                        }
                        let expected: Vec<_> = expected.iter().map(|(id, ..)| id.as_str()).collect();
                        ensure!(selected.iter().map(String::as_str).eq(expected.iter().copied()),
                            "{count} windows {mode} {order:?} {parent:?}: the union skipped, repeated or misordered a name");
                    }
                }
            }
        }
        Ok(())
    }).await
}

#[test]
fn expiring_union_rejects_unbounded_overlapping_or_excessive_storage_filters() -> Result<()> {
    let mut good = filter(7)?;
    good.validate_windows()?;
    good.windows.swap(0, 1);
    good.validate_windows()?;
    for windows in [
        Vec::new(),
        vec![NameCurrentExpiryWindow {
            expires_after: None,
            expires_before: None,
        }],
        vec![good.windows[0]; 2],
        vec![good.windows[0]; 33],
        vec![NameCurrentExpiryWindow {
            expires_after: Some("2".parse()?),
            expires_before: Some("1".parse()?),
        }],
        vec![
            NameCurrentExpiryWindow {
                expires_after: None,
                expires_before: Some("1".parse()?),
            },
            good.windows[0],
        ],
    ] {
        let bad = NameCurrentExpiringFilter {
            windows,
            ..good.clone()
        };
        assert!(bad.validate_windows().is_err(), "{bad:?}");
    }
    Ok(())
}
