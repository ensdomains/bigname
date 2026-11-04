//! `parent` on the expiring selection, against the selector fixture: a name is listed when its
//! labelhash path is one label followed by the parent's path, whatever text its labels are
//! served under. Every parent class is read in every window shape, both orders and both plan
//! modes, and paged to the end. Under custom plans a parent with few names below it must be
//! read from the parent key, and the hot parent `eth` from the expiry, each within the rows its
//! access path has to examine.
use sqlx::{Row, raw_sql};

use super::tests::{ROWS, fixture_labelhash, install_fixture};
use super::*;
use crate::families::id_index_plan_tests::{PLAN_MODES, with_database};

const PARENT_KEY_INDEX: &str = "name_surfaces_project_parent_key_idx";
const EXPIRY_INDEX: &str = "project_name_summary_expiry_idx";
const GRACE_INDEX: &str = "project_name_summary_grace_idx";
const PAGE: usize = 50;
const PLAN_LIMIT: u64 = 8;

/// One readable, listable name of the fixture: id, deadline, served name, namehash, public
/// authority and labelhash path in lowercase.
pub(super) struct Positioned {
    pub(super) id: String,
    pub(super) at: UnixSeconds,
    name: String,
    namehash: String,
    authority: Option<String>,
    pub(super) path: Vec<String>,
}

/// Every readable, listable name of the fixture in the public order, read without indexes,
/// keyset or parent filter, as [`super::tests::listed`] reads them.
pub(super) async fn positioned(
    connection: &mut PgConnection,
    deadline: crate::NameCurrentDeadline,
    descending: bool,
) -> Result<Vec<Positioned>> {
    let date = deadline.column();
    let rows = raw_sql(&format!(
        "SELECT summary.logical_name_id, summary.{date}::text AS at, {name} AS raw_name,
                surface.namehash, summary.public_authority, surface.labelhashes
         FROM project_name_summary summary
         JOIN name_surfaces surface USING (logical_name_id)
         JOIN chain_lineage lineage
           ON lineage.chain_id = surface.chain_id AND lineage.block_hash = surface.block_hash
         WHERE summary.expiry_listable AND summary.{date} IS NOT NULL
           AND surface.visibility_state = 'active'
           AND surface.block_number <= {ROWS}
           AND lineage.canonicality_state = 'canonical'
         ORDER BY summary.{date} {direction}, {name}, surface.namehash",
        name = rendered_name_sql("surface"),
        direction = if descending { "DESC" } else { "ASC" }
    ))
    .fetch_all(&mut *connection)
    .await?;
    rows.iter()
        .map(|row| {
            let at: String = row.try_get(1)?;
            let path: Vec<String> = row.try_get(5)?;
            Ok(Positioned {
                id: row.try_get(0)?,
                at: at.parse()?,
                name: row.try_get(2)?,
                namehash: row.try_get(3)?,
                authority: row.try_get(4)?,
                path: path.iter().map(|hash| hash.to_ascii_lowercase()).collect(),
            })
        })
        .collect()
}

/// The labelhash path of `parent`, label by label.
pub(super) fn parent_path(parent: &str) -> Vec<String> {
    parent.split('.').map(fixture_labelhash).collect()
}

/// Whether `path` is one label followed by `parent`.
pub(super) fn one_below(path: &[String], parent: &[String]) -> bool {
    path.len() == parent.len() + 1 && path[1..] == *parent
}

fn in_window(at: UnixSeconds, window: &NameCurrentExpiryWindow) -> bool {
    window.expires_after.is_none_or(|after| at >= after)
        && window.expires_before.is_none_or(|before| at < before)
}

fn expected<'a>(
    all: &'a [Positioned],
    window: &NameCurrentExpiryWindow,
    authorities: Option<&[String]>,
    parent: &[String],
) -> Vec<&'a Positioned> {
    all.iter()
        .filter(|row| {
            in_window(row.at, window)
                && one_below(&row.path, parent)
                && authorities.is_none_or(|listed| {
                    row.authority
                        .as_ref()
                        .is_some_and(|authority| listed.contains(authority))
                })
        })
        .collect()
}

fn cursor_after(row: &Positioned) -> NameCurrentListCursor {
    NameCurrentListCursor {
        sort_value: NameCurrentListCursorValue::Timestamp(Some(row.at)),
        namespace: "ens".to_owned(),
        normalized_name: row.name.clone(),
        namehash: row.namehash.clone(),
    }
}

/// Every name of `selection`, paged [`PAGE`] at a time from the first page.
async fn page_through(
    connection: &mut PgConnection,
    selection: &ExpiringSelection<'_>,
    expected: &[&Positioned],
) -> Result<Vec<String>> {
    let mut selected = Vec::new();
    let mut cursor: Option<NameCurrentListCursor> = None;
    loop {
        let paged = ExpiringSelection {
            cursor: cursor.as_ref(),
            ..*selection
        };
        let names = select_expiring_names(connection, &paged, PAGE as u64 + 1).await?;
        let more = names.len() > PAGE;
        selected.extend(names.into_iter().take(PAGE));
        if !more {
            return Ok(selected);
        }
        let last = expected
            .get(selected.len() - 1)
            .context("the selection continued past the expected names")?;
        cursor = Some(cursor_after(last));
    }
}

fn windows() -> Result<[(&'static str, NameCurrentExpiryWindow); 4]> {
    let window = |after: Option<&str>, before: Option<&str>| -> Result<NameCurrentExpiryWindow> {
        Ok(NameCurrentExpiryWindow {
            expires_after: after.map(str::parse).transpose()?,
            expires_before: before.map(str::parse).transpose()?,
        })
    };
    Ok([
        ("from a date", window(Some("1800500000.5"), None)?),
        ("until a date", window(None, Some("1850000001"))?),
        (
            "the shared second",
            window(Some("1849999999"), Some("1850000001"))?,
        ),
        ("past", window(Some("1000"), Some("2000"))?),
    ])
}

/// The parent classes of the fixture, by label.
fn parents() -> Vec<(&'static str, String)> {
    let opaque = format!("[{}].eth", "ab".repeat(32));
    let bracketed_p = format!("[{}].eth", &fixture_labelhash("p")[2..]);
    vec![
        ("no names below", "tlknown.eth".to_owned()),
        ("absent", "nobody.eth".to_owned()),
        ("one name below", "a_b.eth".to_owned()),
        ("a LIKE percent", "a%b.eth".to_owned()),
        ("few below, one in uppercase hex", "few.eth".to_owned()),
        ("many below", "p.eth".to_owned()),
        ("the hot parent", "eth".to_owned()),
        ("a bracketed parent", opaque.clone()),
        ("a three-label parent", format!("kid.{opaque}")),
        ("a known label spelled bracketed", bracketed_p),
        ("a label with no preimage", "zz.eth".to_owned()),
    ]
}

#[tokio::test]
async fn parent_selection_lists_the_names_one_label_below_by_tree_position() -> Result<()> {
    with_database("family_expiring_parent_rows", async |connection| {
        install_fixture(connection).await?;
        raw_sql("SET enable_seqscan = off")
            .execute(&mut *connection)
            .await?;
        let windows = windows()?;
        let one = ["ens_v1".to_owned()];
        let two = ["ens_v0".to_owned(), "ens_v2".to_owned()];
        let id = |n: i64| format!("ens:0x{n:064x}");
        for descending in [false, true] {
            let order = if descending {
                NameCurrentListOrder::Desc
            } else {
                NameCurrentListOrder::Asc
            };
            let all =
                positioned(connection, crate::NameCurrentDeadline::Expiry, descending).await?;
            // The fixture holds each class the labels claim, in the shared second.
            let shared = &windows[2].1;
            let below = |parent: &str| -> Vec<String> {
                expected(&all, shared, None, &parent_path(parent))
                    .iter()
                    .map(|row| row.id.clone())
                    .collect()
            };
            ensure!(below("tlknown.eth").is_empty() && below("nobody.eth").is_empty());
            ensure!(below("a_b.eth") == [id(3002)], "{:?}", below("a_b.eth"));
            let mut few = below("few.eth");
            few.sort();
            ensure!(few == [id(3010), id(3011), id(3012)], "{few:?}");
            ensure!(below("zz.eth") == [id(3015)], "{:?}", below("zz.eth"));
            ensure!(
                all.iter().any(|row| row.id == id(3015)
                    && row
                        .name
                        .ends_with(&format!(".[{}].eth", &fixture_labelhash("zz")[2..]))),
                "the name below zz.eth is not served with a bracketed zz"
            );
            let p = expected(&all, &windows[0].1, None, &parent_path("p.eth"));
            ensure!(p.len() > PAGE, "p.eth has {} names", p.len());
            for (label, parent) in parents() {
                for authorities in [None, Some(&one[..]), Some(&two[..])] {
                    for (shape, window) in &windows {
                        let wanted = expected(&all, window, authorities, &parent_path(&parent));
                        let wanted_ids: Vec<&str> =
                            wanted.iter().map(|row| row.id.as_str()).collect();
                        let selection = ExpiringSelection {
                            deadline: crate::NameCurrentDeadline::Expiry,
                            namespace: "ens",
                            expires_after: window.expires_after,
                            expires_before: window.expires_before,
                            authorities,
                            parent: Some(parent.as_str()),
                            order,
                            cursor: None,
                        };
                        for mode in PLAN_MODES {
                            raw_sql(&format!("SET plan_cache_mode = {mode}"))
                                .execute(&mut *connection)
                                .await?;
                            let selected = page_through(connection, &selection, &wanted).await?;
                            ensure!(
                                selected
                                    .iter()
                                    .map(String::as_str)
                                    .eq(wanted_ids.iter().copied()),
                                "{label} ({parent}) {authorities:?} {shape} {order:?} under \
                                 {mode}: selected {selected:?}, expected {wanted_ids:?}"
                            );
                        }
                    }
                }
            }
            // A known label spelled as its labelhash lists what its text lists.
            let bracketed_p = format!("[{}].eth", &fixture_labelhash("p")[2..]);
            ensure!(parent_path(&bracketed_p) == parent_path("p.eth"));
        }
        Ok(())
    })
    .await
}

/// The rows the scans of `relation` in `plan` examined: those they returned and those their
/// filters and rechecks removed, over every loop.
fn examined(plan: &Value, relation: &str) -> f64 {
    let own = if plan["Relation Name"] == relation {
        let loops = plan["Actual Loops"].as_f64().unwrap_or(0.0);
        [
            "Actual Rows",
            "Rows Removed by Filter",
            "Rows Removed by Index Recheck",
        ]
        .iter()
        .map(|key| plan[*key].as_f64().unwrap_or(0.0))
        .sum::<f64>()
            * loops
    } else {
        0.0
    };
    own + plan["Plans"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|child| examined(child, relation))
        .sum::<f64>()
}

fn reads_index(plan: &Value, index: &str) -> bool {
    plan["Index Name"] == index
        || plan["Plans"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|child| reads_index(child, index))
}

/// The summary rows of `window` an expiry-ordered scan reads before a page of [`PLAN_LIMIT`]
/// selected names is complete: through the first selected name after the last one served
/// that has another deadline, the row that ends that deadline's group, or the whole window.
fn expiry_walk(
    window_deadlines: &[UnixSeconds],
    wanted: &[&Positioned],
    descending: bool,
) -> usize {
    let limit = usize::try_from(PLAN_LIMIT).expect("small limit");
    let Some(last) = wanted.get(limit - 1) else {
        return window_deadlines.len();
    };
    let Some(next) = wanted[limit..].iter().find(|row| row.at != last.at) else {
        return window_deadlines.len();
    };
    window_deadlines
        .iter()
        .filter(|at| {
            if descending {
                **at >= next.at
            } else {
                **at <= next.at
            }
        })
        .count()
}

/// The custom plan of each parent class's first page under `deadline`: a parent with no more
/// surfaces below it than a page must read the parent key and examine at most those surfaces
/// and a page, `eth` must walk the deadline index and examine at most the rows
/// [`expiry_walk`] counts, and any other parent must stay within the bound of whichever path
/// it takes. A plan that reads both indexes reads at most every listable row of the window
/// once. Its bound is those rows on the summary, and the parent's surfaces and a page on the
/// surfaces. A nested loop over the parent's materialized surfaces also compares each walked
/// row with each of those surfaces in memory. This check does not count those comparisons.
async fn check_parent_plans(
    connection: &mut PgConnection,
    deadline: crate::NameCurrentDeadline,
    deadline_index: &str,
) -> Result<()> {
    let date = deadline.column();
    raw_sql("SET enable_seqscan = off; SET plan_cache_mode = force_custom_plan")
        .execute(&mut *connection)
        .await?;
    let surfaces: Vec<Vec<String>> = sqlx::query_scalar("SELECT labelhashes FROM name_surfaces")
        .fetch_all(&mut *connection)
        .await?;
    let below = |parent: &[String]| {
        surfaces
            .iter()
            .filter(|path| {
                let lower: Vec<String> =
                    path.iter().map(|hash| hash.to_ascii_lowercase()).collect();
                one_below(&lower, parent)
            })
            .count()
    };
    let windows = windows()?;
    let ens_v2 = ["ens_v2".to_owned()];
    let mut failures = Vec::new();
    for descending in [false, true] {
        let order = if descending {
            NameCurrentListOrder::Desc
        } else {
            NameCurrentListOrder::Asc
        };
        let all = positioned(connection, deadline, descending).await?;
        for (shape, window) in &windows[..3] {
            let deadlines: Vec<UnixSeconds> = sqlx::query_scalar::<_, String>(&format!(
                "SELECT {date}::text FROM project_name_summary
                 WHERE namespace = 'ens' AND expiry_listable AND {date} IS NOT NULL"
            ))
            .fetch_all(&mut *connection)
            .await?
            .iter()
            .map(|at| at.parse())
            .collect::<Result<Vec<UnixSeconds>, _>>()?
            .into_iter()
            .filter(|at| in_window(*at, window))
            .collect();
            for (label, parent) in parents() {
                let path = parent_path(&parent);
                let names_below = below(&path);
                for authorities in [None, Some(&ens_v2[..])] {
                    let hot = names_below > PLAN_LIMIT as usize;
                    if hot && authorities.is_some() {
                        continue;
                    }
                    let selection = ExpiringSelection {
                        deadline,
                        namespace: "ens",
                        expires_after: window.expires_after,
                        expires_before: window.expires_before,
                        authorities,
                        parent: Some(parent.as_str()),
                        order,
                        cursor: None,
                    };
                    let explained: Value = expiring_names_query(
                        "EXPLAIN (ANALYZE, FORMAT JSON) ",
                        &selection,
                        PLAN_LIMIT,
                    )?
                    .build_query_scalar()
                    .fetch_one(&mut *connection)
                    .await?;
                    let plan = &explained[0]["Plan"];
                    let summary_rows = examined(plan, "project_name_summary");
                    let surface_rows = examined(plan, "name_surfaces");
                    let by_key = reads_index(plan, PARENT_KEY_INDEX);
                    let key_bound = (names_below + PLAN_LIMIT as usize) as f64;
                    let wanted = expected(&all, window, authorities, &path);
                    let walk_bound = expiry_walk(&deadlines, &wanted, descending) as f64;
                    let fault = if parent == "eth" {
                        (by_key || !reads_index(plan, deadline_index) || summary_rows > walk_bound)
                            .then(|| format!("must walk {deadline_index} within {walk_bound} rows"))
                    } else if hot {
                        let (bound, surface_bound) =
                            match (by_key, reads_index(plan, deadline_index)) {
                                (true, true) => (deadlines.len() as f64, Some(key_bound)),
                                (true, false) => (key_bound, Some(key_bound)),
                                (false, _) => (walk_bound, None),
                            };
                        let surfaces_over = surface_bound.is_some_and(|limit| surface_rows > limit);
                        (summary_rows > bound || surfaces_over).then(|| match surface_bound {
                            Some(limit) => {
                                format!("must stay within {bound} summary and {limit} surface rows")
                            }
                            None => format!("must stay within {bound} summary rows"),
                        })
                    } else {
                        (!by_key || summary_rows > key_bound || surface_rows > key_bound).then(
                            || format!("must read {PARENT_KEY_INDEX} within {key_bound} rows"),
                        )
                    };
                    if let Some(fault) = fault {
                        failures.push(format!(
                            "{label} ({parent}) {authorities:?} {shape} {order:?}: {fault}; \
                             examined {summary_rows} summary and {surface_rows} surface rows\n{plan:#}"
                        ));
                    }
                }
            }
        }
    }
    ensure!(failures.is_empty(), "{}", failures.join("\n\n"));
    Ok(())
}

#[tokio::test]
async fn parent_selection_reads_the_parent_key_or_the_expiry_by_the_parent_statistics() -> Result<()>
{
    with_database("family_expiring_parent_plans", async |connection| {
        install_fixture(connection).await?;
        check_parent_plans(connection, crate::NameCurrentDeadline::Expiry, EXPIRY_INDEX).await
    })
    .await
}

#[tokio::test]
async fn grace_parent_selection_lists_by_tree_position_and_reads_the_parent_key() -> Result<()> {
    with_database("family_grace_parent", async |connection| {
        install_fixture(connection).await?;
        raw_sql("UPDATE project_name_summary SET grace_ends_at = expires_at; ANALYZE project_name_summary")
            .execute(&mut *connection)
            .await?;
        let windows = windows()?;
        for descending in [false, true] {
            let order = if descending {
                NameCurrentListOrder::Desc
            } else {
                NameCurrentListOrder::Asc
            };
            let all =
                positioned(connection, crate::NameCurrentDeadline::GraceEnds, descending).await?;
            for (label, parent) in parents() {
                for (shape, window) in &windows {
                    let wanted = expected(&all, window, None, &parent_path(&parent));
                    let wanted_ids: Vec<&str> = wanted.iter().map(|row| row.id.as_str()).collect();
                    let selection = ExpiringSelection {
                        deadline: crate::NameCurrentDeadline::GraceEnds,
                        namespace: "ens",
                        expires_after: window.expires_after,
                        expires_before: window.expires_before,
                        authorities: None,
                        parent: Some(parent.as_str()),
                        order,
                        cursor: None,
                    };
                    let selected = page_through(connection, &selection, &wanted).await?;
                    ensure!(
                        selected.iter().map(String::as_str).eq(wanted_ids.iter().copied()),
                        "grace {label} ({parent}) {shape} {order:?}: selected {selected:?}, \
                         expected {wanted_ids:?}"
                    );
                }
            }
        }
        check_parent_plans(connection, crate::NameCurrentDeadline::GraceEnds, GRACE_INDEX).await
    })
    .await
}

/// A generic plan cannot see the parent, so neither selection statement may be kept as a
/// prepared statement whose plan PostgreSQL would reuse across parents.
#[tokio::test]
async fn expiring_selections_are_planned_for_each_request() -> Result<()> {
    with_database("family_expiring_unprepared", async |connection| {
        install_fixture(connection).await?;
        let window = |after: &str, before: &str| -> Result<NameCurrentExpiryWindow> {
            Ok(NameCurrentExpiryWindow {
                expires_after: Some(after.parse()?),
                expires_before: Some(before.parse()?),
            })
        };
        let single = window("1800500000", "1850000001")?;
        for parent in ["nobody.eth", "a_b.eth"] {
            let selection = ExpiringSelection {
                deadline: crate::NameCurrentDeadline::Expiry,
                namespace: "ens",
                expires_after: single.expires_after,
                expires_before: single.expires_before,
                authorities: None,
                parent: Some(parent),
                order: NameCurrentListOrder::Asc,
                cursor: None,
            };
            let names = select_expiring_names(connection, &selection, 8).await?;
            ensure!(
                names.len() == usize::from(parent == "a_b.eth"),
                "{parent}: {names:?}"
            );
        }
        // Two windows run the union statement. Nothing is below the parent, so no row is
        // composed and no family publication is needed.
        let union = NameCurrentExpiringFilter {
            deadline: crate::NameCurrentDeadline::Expiry,
            namespace: "ens".to_owned(),
            windows: vec![
                window("1800500000", "1810000000")?,
                window("1849999999", "1850000001")?,
            ],
            authorities: None,
            parent: Some("nobody.eth".to_owned()),
        };
        let page = load_family_expiring_page(
            &mut *connection,
            &union,
            NameCurrentListOrder::Asc,
            None,
            7,
            &[],
        )
        .await?;
        ensure!(page.rows.is_empty(), "nobody.eth has no names below it");
        let kept: Vec<String> = sqlx::query_scalar(
            "SELECT statement FROM pg_prepared_statements
             WHERE statement LIKE '%storage:families.name.expiring_%'
               AND statement NOT LIKE '%pg_prepared_statements%'",
        )
        .fetch_all(&mut *connection)
        .await?;
        ensure!(
            kept.is_empty(),
            "the expiring selection is kept prepared: {kept:?}"
        );
        Ok(())
    })
    .await
}
