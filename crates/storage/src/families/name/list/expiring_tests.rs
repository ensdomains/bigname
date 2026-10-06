//! The selection statement on a table too small to need its indexes, with sequential scans
//! and plain sorts off to stand in for a large one: the assertions are about which access
//! paths the planner can use at all and about the rows returned, not about costs.
//!
//! Under a generic and a custom plan alike, for a first page and a continuation, the
//! statement must read a selector index with the namespace, the window and the cursor's
//! expiry as index conditions, and may sort only the names that share one expiry (an
//! incremental sort presorted by expiry), never the namespace.
use sqlx::{Row, raw_sql};

use super::*;
use crate::families::id_index_plan_tests::{PLAN_MODES, with_database};

const CHAIN: &str = "ethereum-sepolia";
const ROWS: i64 = 3_000;
const TIE: i64 = 1_850_000_000;
const EXPIRY_INDEX: &str = "project_name_summary_expiry_idx";
const AUTHORITY_INDEX: &str = "project_name_summary_authority_expiry_idx";

/// Name n of `ens`, n from 1: listable unless n is a multiple of 11; names with n % 4 == 0
/// share [`TIE`], every 97th expires half a second past its slot, and every 501st past the
/// largest bigint; the authority cycles ens_v0, ens_v1, ens_v2 and none; n % 9 == 0 is one
/// label below `p.eth`. Names with n % 50 == 0 are not active and every 70th block is
/// orphaned. Name ROWS + 1 sits on a block after the family marker.
///
/// Six more names, numbered from ROWS + 2 and all sharing [`TIE`]: one label below `a_b.eth`
/// and below `aXb.eth` and `a%b.eth`, which a `LIKE` taking the parent as a pattern would
/// also match; one label and two labels below a bracketed labelhash parent; and a name of
/// about 6.6 KB, longer than any index on names admits.
///
/// Two more, ROWS + 8 and ROWS + 9, store no raw bytes and also share [`TIE`]: `tlknown.eth`,
/// whose labels all have usable preimages, and a label with no preimage one label below
/// `p.eth`. Each is listed under the name built from its label hashes.
pub(super) async fn install_fixture(connection: &mut PgConnection) -> Result<()> {
    raw_sql(include_str!("../../../../schema/baseline/07_labels.sql"))
        .execute(&mut *connection)
        .await?;
    let opaque = format!("[{}].eth", "ab".repeat(32));
    raw_sql(&format!(
        "INSERT INTO chain_lineage
             (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         SELECT '{CHAIN}', 'block-' || n, n, to_timestamp(n),
                (CASE WHEN n % 70 = 0 THEN 'orphaned' ELSE 'canonical' END)::canonicality_state
         FROM generate_series(1, {ROWS} + 1) n;
         INSERT INTO project_family_marker (chain_id, current_block_number,
             current_block_hash, state)
         VALUES ('{CHAIN}', {ROWS}, 'block-{ROWS}', 'live');
         CREATE TEMP TABLE fixture_name AS
         SELECT n, substr(md5(n::text), 1, 8)
                    || CASE WHEN n % 9 = 0 THEN '.p.eth' ELSE '.eth' END AS raw_name,
                n AS block_number
         FROM generate_series(1, {ROWS} + 1) n
         UNION ALL
         SELECT {ROWS} + 1 + special.position, special.raw_name, 1
         FROM unnest(ARRAY['x.a_b.eth', 'x.aXb.eth', 'y.a%b.eth', 'kid.{opaque}',
                 'deep.kid.{opaque}',
                 (SELECT string_agg(md5(i::text), '.') FROM generate_series(1, 200) i)
                     || '.eth'])
             WITH ORDINALITY AS special(raw_name, position);
         INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state,
             deactivation_reason, deactivated_at, chain_id, block_hash, block_number,
             canonicality_state)
         SELECT 'ens:0x' || lpad(to_hex(n), 64, '0'), 'ens', raw_name, ARRAY[raw_name],
                '\\x00', '0x' || lpad(to_hex(n), 64, '0'),
                ARRAY['0x' || lpad(to_hex(n), 64, '0')], 'v1',
                CASE WHEN n % 50 = 0 THEN 'shadow' ELSE 'active' END,
                CASE WHEN n % 50 = 0 THEN 'invalid' END, CASE WHEN n % 50 = 0 THEN now() END,
                '{CHAIN}', 'block-' || block_number, block_number,
                (CASE WHEN block_number % 70 = 0 THEN 'orphaned' ELSE 'canonical' END)
                    ::canonicality_state
         FROM fixture_name WHERE raw_name IS NOT NULL;
         INSERT INTO label_preimages (labelhash, raw_label, decoded_label, normalizer_version,
             normalized_under_version, source_kind, source_priority)
         SELECT '0x' || repeat(digit, 64), convert_to(label, 'UTF8'), label, 'v1', true,
                'fixture', 0
         FROM (VALUES ('1', 'eth'), ('2', 'p'), ('3', 'tlknown')) known(digit, label);
         INSERT INTO fixture_name VALUES ({ROWS} + 8, NULL, 1), ({ROWS} + 9, NULL, 1);
         INSERT INTO name_surfaces (logical_name_id, namespace, namehash, labelhashes,
             normalizer_version, visibility_state, chain_id, block_hash, block_number,
             canonicality_state)
         SELECT 'ens:0x' || lpad(to_hex(n), 64, '0'), 'ens', '0x' || lpad(to_hex(n), 64, '0'),
                path, 'v1', 'active', '{CHAIN}', 'block-1', 1, 'canonical'
         FROM (VALUES
             ({ROWS} + 8, ARRAY['0x' || repeat('3', 64), '0x' || repeat('1', 64)]),
             ({ROWS} + 9, ARRAY['0x' || repeat('c', 64), '0x' || repeat('2', 64),
                                '0x' || repeat('1', 64)])) textless(n, path);
         INSERT INTO project_name_summary (chain_id, logical_name_id, namespace, serving,
             zero_owner, expires_at, expiry_listable, public_authority, search_supported)
         SELECT '{CHAIN}', 'ens:0x' || lpad(to_hex(n), 64, '0'), 'ens', TRUE, FALSE,
                CASE WHEN n > {ROWS} + 1 THEN {TIE}
                     WHEN n % 501 = 0 THEN 9223372036854775808 + n
                     WHEN n % 4 = 0 THEN {TIE}
                     ELSE 1800000000 + n * 1000 + CASE WHEN n % 97 = 0 THEN 0.5 ELSE 0 END
                END,
                n % 11 <> 0,
                (ARRAY['ens_v0', 'ens_v1', 'ens_v2', NULL])[n % 4 + 1], FALSE
         FROM fixture_name;
         ANALYZE name_surfaces; ANALYZE project_name_summary; ANALYZE chain_lineage;"
    ))
    .execute(&mut *connection)
    .await
    .context("failed to install the expiring selection fixture")?;
    Ok(())
}

/// What the selection must return, read without its indexes or keyset: every readable,
/// listable name of the fixture in the public order as (id, expiry, name, namehash).
pub(super) async fn listed(
    connection: &mut PgConnection,
    descending: bool,
) -> Result<Vec<(String, String, String, String)>> {
    let rows = raw_sql(&format!(
        "SELECT summary.logical_name_id, summary.expires_at::text AS at, {name} AS raw_name,
                surface.namehash
         FROM project_name_summary summary
         JOIN name_surfaces surface USING (logical_name_id)
         JOIN chain_lineage lineage
           ON lineage.chain_id = surface.chain_id AND lineage.block_hash = surface.block_hash
         WHERE summary.expiry_listable AND surface.visibility_state = 'active'
           AND surface.block_number <= {ROWS}
           AND lineage.canonicality_state = 'canonical'
         ORDER BY summary.expires_at {direction}, {name}, surface.namehash",
        name = rendered_name_sql("surface"),
        direction = if descending { "DESC" } else { "ASC" }
    ))
    .fetch_all(&mut *connection)
    .await?;
    rows.iter()
        .map(|row| {
            Ok((
                row.try_get(0)?,
                row.try_get(1)?,
                row.try_get(2)?,
                row.try_get(3)?,
            ))
        })
        .collect()
}

fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// `selection`'s statement prepared as `name`, and the values of one execution: the
/// parameter types and literals in the order [`expiring_names_query`] binds them.
fn prepared(name: &str, selection: &ExpiringSelection<'_>, limit: u64) -> Result<(String, String)> {
    let mut types = vec!["text"];
    let mut values = vec![literal(selection.namespace)];
    match selection.authorities {
        None => {}
        Some([authority]) => {
            types.push("text");
            values.push(literal(authority));
        }
        Some(authorities) => {
            types.push("text[]");
            values.push(format!(
                "ARRAY[{}]",
                authorities
                    .iter()
                    .map(|authority| literal(authority))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
    }
    if let Some(parent) = selection.parent {
        let (one_below, deeper) = crate::name_current::parent_like_patterns(parent);
        types.extend(["text", "text"]);
        values.extend([literal(&one_below), literal(&deeper)]);
    }
    for bound in [selection.expires_after, selection.expires_before]
        .into_iter()
        .flatten()
    {
        types.push("numeric");
        values.push(bound.to_string());
    }
    if let Some(cursor) = selection.cursor {
        let NameCurrentListCursorValue::Timestamp(Some(at)) = cursor.sort_value else {
            bail!("the fixture cursor carries an expiry");
        };
        types.extend(["numeric", "numeric", "numeric", "text", "text", "text"]);
        values.extend([at.to_string(), at.to_string(), at.to_string()]);
        values.extend([
            literal(&cursor.namespace),
            literal(&cursor.normalized_name),
            literal(&cursor.namehash),
        ]);
    }
    types.push("bigint");
    values.push(limit.to_string());
    let statement = expiring_names_query("", selection, limit)?.into_sql();
    Ok((
        format!("PREPARE {name} ({}) AS {statement}", types.join(", ")),
        values.join(", "),
    ))
}

/// The plan `mode` gives one execution of `selection`'s prepared statement.
async fn plan_under(
    connection: &mut PgConnection,
    mode: &str,
    selection: &ExpiringSelection<'_>,
) -> Result<Vec<String>> {
    let (prepare, values) = prepared("expiring_names", selection, 8)?;
    raw_sql(&prepare).execute(&mut *connection).await?;
    let plan = raw_sql(&format!(
        "SET plan_cache_mode = {mode}; EXPLAIN (COSTS OFF) EXECUTE expiring_names ({values})"
    ))
    .fetch_all(&mut *connection)
    .await?
    .iter()
    .map(|row| row.try_get(0).map_err(anyhow::Error::from))
    .collect::<Result<Vec<String>>>()?;
    raw_sql("DEALLOCATE expiring_names")
        .execute(&mut *connection)
        .await?;
    Ok(plan)
}

/// Why `plan` is not an acceptable plan of the selection, if it is not: it must scan one of
/// `indexes` on the summary with the namespace and every expiry bound of `selection` as
/// index conditions, and sort nothing but the names of one expiry.
fn plan_fault(
    plan: &[String],
    mode: &str,
    selection: &ExpiringSelection<'_>,
    indexes: &[&str],
) -> Option<String> {
    let generic = mode == "force_generic_plan";
    let text = plan.join("\n");
    if generic != text.contains("$1") {
        return Some(format!("is not a {mode} plan"));
    }
    let Some(condition) = plan.windows(2).find_map(|lines| {
        let scans = indexes
            .iter()
            .any(|index| lines[0].contains(&format!("using {index} on project_name_summary")));
        (scans && lines[1].trim_start().starts_with("Index Cond:")).then(|| lines[1].clone())
    }) else {
        return Some(format!("scans none of {indexes:?} with an index condition"));
    };
    let ascending = selection.order == NameCurrentListOrder::Asc;
    let mut lower = usize::from(selection.expires_after.is_some());
    let mut upper = usize::from(selection.expires_before.is_some());
    if selection.cursor.is_some() {
        *(if ascending { &mut lower } else { &mut upper }) += 1;
    }
    let cursor_upper = usize::from(selection.cursor.is_some() && !ascending);
    if !condition.contains("namespace = ")
        || condition.matches("expires_at >= ").count() != lower
        || condition.matches("expires_at < ").count() + condition.matches("expires_at <= ").count()
            != upper
        || condition.matches("expires_at <= ").count() != cursor_upper
    {
        return Some(format!(
            "does not bound the index by the window and cursor: {condition}"
        ));
    }
    let Some(presorted) = plan.windows(3).find_map(|lines| {
        lines[0]
            .contains("Incremental Sort")
            .then(|| lines[2].trim().to_owned())
    }) else {
        return Some("has no incremental sort".to_owned());
    };
    // The one presorted key is the expiry; PostgreSQL prints it with or without the direction.
    if !matches!(
        presorted.as_str(),
        "Presorted Key: summary.expires_at" | "Presorted Key: summary.expires_at DESC"
    ) {
        return Some(format!("sorts more than one expiry's names: {presorted}"));
    }
    plan.iter()
        .any(|line| {
            let node = line.trim_start().trim_start_matches("->").trim_start();
            node == "Sort" || node.contains("Seq Scan on project_name_summary")
        })
        .then(|| "sorts or scans the whole namespace".to_owned())
}

#[tokio::test]
async fn expiring_selection_reads_the_selector_indexes_in_the_public_order() -> Result<()> {
    with_database("family_expiring_selection", async |connection| {
        install_fixture(connection).await?;
        raw_sql("SET enable_seqscan = off; SET enable_sort = off")
            .execute(&mut *connection)
            .await?;
        let ens_v1 = ["ens_v1".to_owned()];
        let both = ["ens_v0".to_owned(), "ens_v2".to_owned()];
        let after: UnixSeconds = "1800500000.5".parse()?;
        let before: UnixSeconds = "9223372036854777000".parse()?;
        let opaque = format!("[{}].eth", "ab".repeat(32));
        for descending in [false, true] {
            let order = if descending {
                NameCurrentListOrder::Desc
            } else {
                NameCurrentListOrder::Asc
            };
            let all = listed(connection, descending).await?;
            ensure!(
                all.iter().any(|(_, _, name, _)| name.len() > 6_000),
                "the fixture's long name is not listed"
            );
            let textless_child = format!("[{}].p.eth", "c".repeat(64));
            ensure!(
                ["tlknown.eth", textless_child.as_str()]
                    .iter()
                    .all(|served| all.iter().any(|(_, _, name, _)| name == served)),
                "the fixture's names without bytes are not listed under their served names"
            );
            let authority_of = |id: &str| -> Option<&'static str> {
                let n =
                    usize::from_str_radix(id.trim_start_matches("ens:0x"), 16).expect("fixture id");
                [Some("ens_v0"), Some("ens_v1"), Some("ens_v2"), None][n % 4]
            };
            for (label, authorities, parent, indexes, least) in [
                ("plain", None, None, &[EXPIRY_INDEX][..], 15),
                (
                    "one authority",
                    Some(&ens_v1[..]),
                    None,
                    &[AUTHORITY_INDEX][..],
                    15,
                ),
                (
                    "two authorities",
                    Some(&both[..]),
                    None,
                    &[EXPIRY_INDEX, AUTHORITY_INDEX][..],
                    15,
                ),
                ("parent", None, Some("p.eth"), &[EXPIRY_INDEX][..], 15),
                // The parent is a literal: `_` and `%` in it match only themselves.
                (
                    "parent with a LIKE wildcard",
                    None,
                    Some("a_b.eth"),
                    &[EXPIRY_INDEX][..],
                    1,
                ),
                (
                    "parent with a LIKE percent",
                    None,
                    Some("a%b.eth"),
                    &[EXPIRY_INDEX][..],
                    1,
                ),
                // A bracketed labelhash label is an ordinary label of the parent.
                (
                    "opaque parent",
                    None,
                    Some(opaque.as_str()),
                    &[EXPIRY_INDEX][..],
                    1,
                ),
            ] {
                let expected: Vec<_> = all
                    .iter()
                    .filter(|(id, at, name, _)| {
                        let at: UnixSeconds = at.parse().expect("fixture expiry");
                        at >= after
                            && at < before
                            && authorities.is_none_or(|listed| {
                                authority_of(id).is_some_and(|authority| {
                                    listed.iter().any(|value| value == authority)
                                })
                            })
                            && parent.is_none_or(|parent| {
                                name.strip_suffix(&format!(".{parent}"))
                                    .is_some_and(|label| !label.contains('.'))
                            })
                    })
                    .cloned()
                    .collect();
                ensure!(
                    if least == 1 {
                        expected.len() == 1
                    } else {
                        expected.len() >= least
                    },
                    "{label}: the fixture lists {} names",
                    expected.len()
                );
                let selection = ExpiringSelection {
                    namespace: "ens",
                    expires_after: Some(after),
                    expires_before: Some(before),
                    authorities,
                    parent,
                    order,
                    cursor: None,
                };
                // A continuation from inside the shared second, where the fixture has one.
                let (_, at, name, namehash) = expected
                    .iter()
                    .find(|(_, at, ..)| at == &TIE.to_string())
                    .unwrap_or(&expected[0]);
                let continuation = NameCurrentListCursor {
                    sort_value: NameCurrentListCursorValue::Timestamp(Some(at.parse()?)),
                    namespace: "ens".to_owned(),
                    normalized_name: name.clone(),
                    namehash: namehash.clone(),
                };
                for mode in PLAN_MODES {
                    for (shape, cursor) in
                        [("first page", None), ("continuation", Some(&continuation))]
                    {
                        let shaped = ExpiringSelection {
                            cursor,
                            ..selection
                        };
                        let plan = plan_under(connection, mode, &shaped).await?;
                        if let Some(fault) = plan_fault(&plan, mode, &shaped, indexes) {
                            bail!(
                                "{label} {order:?} {shape} under {mode} {fault}:\n{}",
                                plan.join("\n")
                            );
                        }
                    }
                    // Paged seven at a time, the selection returns every listed name once,
                    // in order, through the shared second and the fractional and past-bigint
                    // expiries.
                    raw_sql(&format!("SET plan_cache_mode = {mode}"))
                        .execute(&mut *connection)
                        .await?;
                    let mut selected = Vec::new();
                    let mut cursor: Option<NameCurrentListCursor> = None;
                    loop {
                        let paged = ExpiringSelection {
                            cursor: cursor.as_ref(),
                            ..selection
                        };
                        let names = select_expiring_names(connection, &paged, 8).await?;
                        let more = names.len() > 7;
                        selected.extend(names.into_iter().take(7));
                        if !more {
                            break;
                        }
                        let (_, at, name, namehash) = &expected[selected.len() - 1];
                        cursor = Some(NameCurrentListCursor {
                            sort_value: NameCurrentListCursorValue::Timestamp(Some(at.parse()?)),
                            namespace: "ens".to_owned(),
                            normalized_name: name.clone(),
                            namehash: namehash.clone(),
                        });
                    }
                    let expected_ids: Vec<&str> =
                        expected.iter().map(|(id, ..)| id.as_str()).collect();
                    ensure!(
                        selected
                            .iter()
                            .map(String::as_str)
                            .eq(expected_ids.iter().copied()),
                        "{label} {order:?} under {mode} selected {} names, expected {}",
                        selected.len(),
                        expected_ids.len()
                    );
                }
            }
        }
        Ok(())
    })
    .await
}
