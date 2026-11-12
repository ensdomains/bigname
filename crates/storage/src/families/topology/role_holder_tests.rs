//! The role-holder page over a fixture of `project_grant` rows, one holder per input class:
//!
//! | Holder | Grants | Expected |
//! | --- | --- | --- |
//! | `SINGLE` | one EAC grant on one resolver | one row, count 1, the grant's powers |
//! | `RECORDS` | 30 EAC grants on 30 resources of one record resolver | one row, count 30, the union |
//! | `WIDE` | one EAC grant on each of 60 resolvers | every page size walks them in scope order, exact `has_more` |
//! | `SKEWED` | 5 EAC grants, and 40,000 derived ENSv1 rows on 2,000 resolvers, one of them an EAC resolver | 5 rows; the plan reads only the new index, 5 entries |
//! | `MIXED` | EAC and derived rows on one resolver, derived rows only on another, a revoked EAC grant on a third | one row, counting the EAC grant only |
//! | `ROOTS` | root only; root and two resources; a revoked root and a live resource; resources only | four rows; a revoked root counts and adds nothing |
//! | `REGISTRY` | ENSv2 registry and registry-root grants, which share the EAC source | no row |
//! | `TWO_CHAINS` | an EAC grant on each chain | both, Base first; one chain alone lists its own |
//! | `NONE` | none | no row |
//!
//! The plan test runs the statement as the reader sends it under both plan modes, for the
//! skewed holder and for a cursor page of the wide one, and requires the only read of
//! `project_grant` to be the new index, with the holder and the scope range in its condition,
//! reading only the rows the page groups. Dropping the index then shows the fixture would catch
//! a wrong plan: the same statement reads every one of the skewed holder's derived rows, and
//! returns the same pages.
use anyhow::{Context, Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::{PgConnection, Row, raw_sql};

use super::{RoleHolderResolver, STATEMENT, load_resolvers_by_role_holder};

const CHAIN: &str = "ethereum-mainnet";
const OTHER_CHAIN: &str = "base-mainnet";
const MARKER: i64 = 1_000;
const INDEX: &str = "project_grant_resolver_role_holder_idx";

const SINGLE: &str = "0x00000000000000000000000000000000000000a1";
const RECORDS: &str = "0x00000000000000000000000000000000000000a2";
const WIDE: &str = "0x00000000000000000000000000000000000000a3";
const SKEWED: &str = "0x00000000000000000000000000000000000000a4";
const MIXED: &str = "0x00000000000000000000000000000000000000a5";
const ROOTS: &str = "0x00000000000000000000000000000000000000a6";
const REGISTRY: &str = "0x00000000000000000000000000000000000000a7";
const TWO_CHAINS: &str = "0x00000000000000000000000000000000000000a8";
const NONE: &str = "0x00000000000000000000000000000000000000a9";
const WIDE_RESOLVERS: usize = 60;
const SKEWED_EAC: usize = 5;
const SKEWED_DERIVED: i64 = 40_000;

/// Resolver `n` as a lower-cased address.
fn resolver(n: u32) -> String {
    format!("0x{n:040x}")
}

fn chains() -> Vec<String> {
    vec![CHAIN.to_owned(), OTHER_CHAIN.to_owned()]
}

fn expect(chain: &str, n: u32, count: u64, powers: &[&str]) -> RoleHolderResolver {
    RoleHolderResolver {
        chain_id: chain.to_owned(),
        resolver_address: resolver(n),
        grant_count: count,
        powers: powers.iter().map(|power| (*power).to_owned()).collect(),
    }
}

async fn load(
    connection: &mut PgConnection,
    holder: &str,
    chains: &[String],
    after: Option<(&str, &str)>,
    limit: i64,
) -> Result<Vec<RoleHolderResolver>> {
    load_resolvers_by_role_holder(&mut *connection, holder, chains, after, limit).await
}

#[tokio::test]
async fn each_holder_class_lists_its_eac_resolvers() -> Result<()> {
    with_database("role_holder_classes", async |connection| {
        let all = chains();
        let cases: Vec<(&str, Vec<RoleHolderResolver>)> = vec![
            (SINGLE, vec![expect(CHAIN, 1, 1, &["set_addr"])]),
            (
                RECORDS,
                vec![expect(CHAIN, 2, 30, &["set_addr", "set_data", "set_text"])],
            ),
            (
                SKEWED,
                (0..SKEWED_EAC as u32)
                    .map(|i| expect(CHAIN, 400 + i, 1, &["set_text"]))
                    .collect(),
            ),
            (MIXED, vec![expect(CHAIN, 5, 1, &["set_addr"])]),
            (
                ROOTS,
                vec![
                    expect(CHAIN, 8, 1, &["admin_set_text", "set_text"]),
                    expect(CHAIN, 9, 3, &["link", "set_addr", "set_text"]),
                    expect(CHAIN, 10, 2, &["set_addr"]),
                    expect(CHAIN, 11, 2, &["set_addr", "set_text"]),
                ],
            ),
            (REGISTRY, Vec::new()),
            (
                TWO_CHAINS,
                vec![
                    expect(OTHER_CHAIN, 13, 1, &["set_text"]),
                    expect(CHAIN, 12, 1, &["set_addr"]),
                ],
            ),
            (NONE, Vec::new()),
        ];
        for (holder, expected) in cases {
            let page = load(connection, holder, &all, None, 51).await?;
            ensure!(
                page == expected,
                "{holder}: {page:?}\nexpected {expected:?}"
            );
        }
        // An upper-cased holder reads the lower-cased subject.
        let page = load(
            connection,
            &SINGLE.to_ascii_uppercase().replace("0X", "0x"),
            &all,
            None,
            51,
        )
        .await?;
        ensure!(page.len() == 1, "{page:?}");
        Ok(())
    })
    .await
}

#[tokio::test]
async fn one_chain_lists_only_its_own_resolvers() -> Result<()> {
    with_database("role_holder_chain", async |connection| {
        for (chain, n, power) in [(CHAIN, 12, "set_addr"), (OTHER_CHAIN, 13, "set_text")] {
            let page = load(connection, TWO_CHAINS, &[chain.to_owned()], None, 51).await?;
            ensure!(
                page == vec![expect(chain, n, 1, &[power])],
                "{chain}: {page:?}"
            );
            // A cursor on the other chain is outside this chain's range on either side.
            for after in [(CHAIN, resolver(12)), (OTHER_CHAIN, resolver(13))] {
                let page = load(
                    connection,
                    TWO_CHAINS,
                    &[chain.to_owned()],
                    Some((after.0, after.1.as_str())),
                    51,
                )
                .await?;
                let expected = if after.0 >= chain {
                    Vec::new()
                } else {
                    vec![expect(chain, n, 1, &[power])]
                };
                ensure!(page == expected, "{chain} after {after:?}: {page:?}");
            }
        }
        Ok(())
    })
    .await
}

#[tokio::test]
async fn every_page_size_walks_the_wide_holder_in_scope_order() -> Result<()> {
    with_database("role_holder_walk", async |connection| {
        let reference = reference(connection, WIDE).await?;
        ensure!(reference.len() == WIDE_RESOLVERS, "{reference:?}");
        for page_size in [1_usize, 7, 50, 59, 60, 61, 200] {
            let mut after: Option<(String, String)> = None;
            let mut walked = Vec::new();
            loop {
                let limit = i64::try_from(page_size + 1)?;
                let mut page = load(
                    connection,
                    WIDE,
                    &chains(),
                    after.as_ref().map(|(c, a)| (c.as_str(), a.as_str())),
                    limit,
                )
                .await?;
                let has_more = page.len() > page_size;
                page.truncate(page_size);
                walked.extend(page.iter().map(|row| row.resolver_address.clone()));
                ensure!(
                    has_more == (walked.len() < reference.len()),
                    "page size {page_size}: has_more {has_more} after {}",
                    walked.len()
                );
                if !has_more {
                    break;
                }
                let last = page.last().context("a page with more has a last row")?;
                after = Some((last.chain_id.clone(), last.resolver_address.clone()));
            }
            ensure!(walked == reference, "page size {page_size}: {walked:?}");
        }
        Ok(())
    })
    .await
}

#[tokio::test]
async fn the_page_reads_only_the_new_index_in_both_plan_modes() -> Result<()> {
    with_database("role_holder_plan", async |connection| {
        let reference = reference(connection, WIDE).await?;
        let resume = format!("resolver:{CHAIN}:{}", reference[49]);
        let mut failures = Vec::new();
        let cases = [
            ("skewed holder", SKEWED, "resolver:", SKEWED_EAC),
            (
                "wide holder after 50",
                WIDE,
                resume.as_str(),
                WIDE_RESOLVERS - 50,
            ),
        ];
        for (label, holder, start, rows) in cases {
            for mode in ["force_custom_plan", "force_generic_plan"] {
                let plan = explain(connection, mode, holder, start).await?;
                failures.extend(
                    plan_problems(&plan, rows)
                        .into_iter()
                        .map(|problem| format!("{label}, {mode}: {problem}\n{}", plan.join("\n"))),
                );
            }
        }
        ensure!(failures.is_empty(), "{}", failures.join("\n\n"));

        // Without the index the same statement reads every grant of the skewed holder, and the
        // pages are the same.
        let before = all_pages(connection).await?;
        raw_sql(&format!("DROP INDEX {INDEX}"))
            .execute(&mut *connection)
            .await?;
        let plan = explain(connection, "force_custom_plan", SKEWED, "resolver:").await?;
        let read = rows_read(&plan);
        ensure!(
            read >= SKEWED_DERIVED as f64,
            "without the index the skewed holder read {read} rows:\n{}",
            plan.join("\n")
        );
        ensure!(
            all_pages(connection).await? == before,
            "pages differ without the index"
        );
        Ok(())
    })
    .await
}

/// What keeps `plan` from reading `project_grant` only through the new index, with the holder
/// and the scope range in its condition and at most `rows` rows read: a sequential scan, another
/// index of the table, rows its own condition did not select, or a condition without those
/// terms.
fn plan_problems(plan: &[String], rows: usize) -> Vec<String> {
    let mut problems = Vec::new();
    if plan.iter().any(|line| line.contains("Seq Scan")) {
        problems.push("the plan scans a table sequentially".to_owned());
    }
    let indexes: Vec<&str> = plan.iter().filter_map(|line| scanned_index(line)).collect();
    if indexes.is_empty() {
        problems.push("project_grant is not read through an index".to_owned());
    }
    for index in &indexes {
        if *index != INDEX {
            problems.push(format!("project_grant is read through {index}"));
        }
    }
    let read = rows_read(plan);
    if read > rows as f64 {
        problems.push(format!("project_grant gave {read} rows, more than {rows}"));
    }
    let conditions = plan
        .iter()
        .filter(|line| line.contains("Index Cond:"))
        .collect::<Vec<_>>();
    if !conditions.iter().any(|line| {
        ["subject", "scope", " > ", " < "]
            .iter()
            .all(|term| line.contains(term))
    }) {
        problems.push(format!(
            "no index condition has the holder and both scope bounds: {conditions:?}"
        ));
    }
    problems
}

/// The `project_grant` index a plan line reads, if it reads one.
fn scanned_index(line: &str) -> Option<&str> {
    let node = line.trim_start().trim_start_matches("->").trim_start();
    let name = node
        .split_once(" using ")
        .map(|(_, rest)| rest)
        .or_else(|| node.strip_prefix("Bitmap Index Scan on "))?
        .split_whitespace()
        .next()?;
    name.starts_with("project_grant").then_some(name)
}

/// Every row the plan's `project_grant` scans returned or dropped by their own filter.
fn rows_read(plan: &[String]) -> f64 {
    let mut read = 0.0;
    for (index, line) in plan.iter().enumerate() {
        let node = line.trim_start().trim_start_matches("->").trim_start();
        if !(node.contains(" on project_grant ") && node.contains("Scan")) {
            continue;
        }
        read += actual_rows(line);
        read += plan[index + 1..]
            .iter()
            // A node's own properties, up to its first child. A filter can hold `->>`.
            .take_while(|line| !line.trim_start().starts_with("->"))
            .filter_map(|line| line.trim().strip_prefix("Rows Removed by Filter: "))
            .filter_map(|rows| rows.trim().parse::<f64>().ok())
            .sum::<f64>();
    }
    read
}

/// The `actual rows` a plan line reports, which recent PostgreSQL prints with decimals.
fn actual_rows(line: &str) -> f64 {
    line.split("actual rows=")
        .nth(1)
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|rows| rows.parse().ok())
        .unwrap_or(0.0)
}

async fn explain(
    connection: &mut PgConnection,
    mode: &str,
    holder: &str,
    start: &str,
) -> Result<Vec<String>> {
    // A name of its own for each call: DEALLOCATE ALL would also drop the statements sqlx has
    // prepared on this connection.
    static CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let name = format!(
        "role_holder_page_{}",
        CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    );
    raw_sql(&format!(
        "PREPARE {name}(text, text, text, bigint, text[]) AS {STATEMENT};
         SET plan_cache_mode = {mode};
         EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
             EXECUTE {name}('{holder}', '{start}', 'resolver;', 51,
                            ARRAY['{CHAIN}', '{OTHER_CHAIN}']);
         RESET plan_cache_mode;"
    ))
    .fetch_all(&mut *connection)
    .await
    .with_context(|| format!("{holder} {mode}"))?
    .iter()
    .map(|row| row.try_get(0).map_err(Into::into))
    .collect()
}

/// The first page of every fixture holder.
async fn all_pages(connection: &mut PgConnection) -> Result<Vec<Vec<RoleHolderResolver>>> {
    let mut pages = Vec::new();
    for holder in [
        SINGLE, RECORDS, WIDE, SKEWED, MIXED, ROOTS, REGISTRY, TWO_CHAINS, NONE,
    ] {
        pages.push(load(connection, holder, &chains(), None, 201).await?);
    }
    Ok(pages)
}

/// The holder's EAC resolvers in page order, from the table alone, so the database collation
/// decides the order.
async fn reference(connection: &mut PgConnection, holder: &str) -> Result<Vec<String>> {
    let scopes: Vec<String> = sqlx::query_scalar(
        r#"SELECT DISTINCT scope COLLATE "C" FROM project_grant
           WHERE subject = $1 AND NOT revoked
             AND grant_source ->> 'source_event' = 'EACRolesChanged'
             AND scope LIKE 'resolver:%'
           ORDER BY 1"#,
    )
    .bind(holder)
    .fetch_all(&mut *connection)
    .await?;
    Ok(scopes
        .iter()
        .filter_map(|scope| {
            scope
                .rsplit_once(':')
                .map(|(_, address)| address.to_owned())
        })
        .collect())
}

async fn with_database(
    prefix: &str,
    check: impl AsyncFnOnce(&mut PgConnection) -> Result<()>,
) -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new(prefix).pool_max_connections(1)).await?;
    let result = async {
        database.create_phase_schema().await?;
        let mut connection = database.pool().acquire().await?;
        for baseline in [
            include_str!("../../../schema/baseline/01_chain.sql"),
            include_str!("../../../schema/baseline/02_raw_facts.sql"),
            include_str!("../../../schema/baseline/03_identity.sql"),
            include_str!("../../../schema/baseline/04_manifests.sql"),
            include_str!("../../../schema/baseline/05_normalized_events.sql"),
            include_str!("../../../schema/baseline/06_projections.sql"),
            include_str!("../../../schema/baseline/07_labels.sql"),
            include_str!("../../../schema/baseline/08_heartbeats.sql"),
            include_str!("../../../schema/baseline/09_divergence.sql"),
            include_str!("../../../schema/baseline/10_phase_state.sql"),
        ] {
            raw_sql(baseline).execute(&mut *connection).await?;
        }
        install_fixture(&mut connection).await?;
        check(&mut connection).await
    }
    .await;
    database.cleanup().await?;
    result
}

/// Grants as the writer stores them. An EAC grant's source is the adapter's raw-log source
/// (crates/adapters/src/schema_v2/protocol/permissions.rs, `v2_states`), `{}` once revoked; a
/// derived ENSv1 grant's is the `ens_v1_authority` source of `v1_grant_states`. Resolver `n` on
/// a chain has scope `resolver:<chain>:<resolver(n)>`, and grant `k` of it EAC resource `k`,
/// the root when `k` is 0.
async fn install_fixture(connection: &mut PgConnection) -> Result<()> {
    let hash = bigname_content_hash::PROJECTION_CONTENT_HASH;
    raw_sql(&format!(
        r#"
        INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp,
            canonicality_state)
        SELECT chain, 'b' || n, n, to_timestamp(1700000000 + n), 'canonical'
        FROM unnest(ARRAY['{CHAIN}', '{OTHER_CHAIN}']) AS chains(chain), generate_series(0, {MARKER}) n;
        INSERT INTO project_family_marker (chain_id, current_block_number, current_block_hash,
            block_timestamp, input_content_hash, state)
        SELECT chain, {MARKER}, 'b{MARKER}', to_timestamp(1700000000 + {MARKER}), '{hash}', 'live'
        FROM unnest(ARRAY['{CHAIN}', '{OTHER_CHAIN}']) AS chains(chain);

        CREATE TEMP TABLE fixture_grant (chain_id text, holder text, resolver integer,
            eac integer, powers jsonb, live boolean, derived boolean, scope_kind text,
            scope text);
        INSERT INTO fixture_grant
        -- SINGLE
        VALUES ('{CHAIN}', '{SINGLE}', 1, 1, '["set_addr"]', true, false, NULL, NULL),
        -- MIXED: EAC and derived on 5, derived only on 6, a revoked EAC grant on 7.
               ('{CHAIN}', '{MIXED}', 5, 1, '["set_addr"]', true, false, NULL, NULL),
               ('{CHAIN}', '{MIXED}', 5, 2, '["resolver_control"]', true, true, NULL, NULL),
               ('{CHAIN}', '{MIXED}', 6, 1, '["resolver_control"]', true, true, NULL, NULL),
               ('{CHAIN}', '{MIXED}', 7, 1, '[]', false, false, NULL, NULL),
        -- ROOTS: root only on 8; root and two resources on 9; a revoked root and two live
        -- resources on 10; two resources on 11.
               ('{CHAIN}', '{ROOTS}', 8, 0, '["set_text", "admin_set_text"]', true, false, NULL, NULL),
               ('{CHAIN}', '{ROOTS}', 9, 0, '["link"]', true, false, NULL, NULL),
               ('{CHAIN}', '{ROOTS}', 9, 1, '["set_addr"]', true, false, NULL, NULL),
               ('{CHAIN}', '{ROOTS}', 9, 2, '["set_text"]', true, false, NULL, NULL),
               ('{CHAIN}', '{ROOTS}', 10, 0, '[]', false, false, NULL, NULL),
               ('{CHAIN}', '{ROOTS}', 10, 1, '["set_addr"]', true, false, NULL, NULL),
               ('{CHAIN}', '{ROOTS}', 10, 2, '["set_addr"]', true, false, NULL, NULL),
               ('{CHAIN}', '{ROOTS}', 11, 1, '["set_addr"]', true, false, NULL, NULL),
               ('{CHAIN}', '{ROOTS}', 11, 2, '["set_text"]', true, false, NULL, NULL),
        -- REGISTRY: an ENSv2 registry grant and a registry root grant.
               ('{CHAIN}', '{REGISTRY}', 20, 1, '["set_resolver"]', true, false, 'registry', 'registry'),
               ('{CHAIN}', '{REGISTRY}', 20, 0, '["registrar"]', true, false, 'root', 'root'),
        -- TWO_CHAINS
               ('{CHAIN}', '{TWO_CHAINS}', 12, 1, '["set_addr"]', true, false, NULL, NULL),
               ('{OTHER_CHAIN}', '{TWO_CHAINS}', 13, 1, '["set_text"]', true, false, NULL, NULL);
        -- RECORDS: 30 resources of record resolver 2.
        INSERT INTO fixture_grant
        SELECT '{CHAIN}', '{RECORDS}', 2, k,
               CASE k % 3 WHEN 0 THEN '["set_addr"]' WHEN 1 THEN '["set_text"]'
                   ELSE '["set_text", "set_data"]' END::jsonb,
               true, false, NULL, NULL
        FROM generate_series(1, 30) k;
        -- WIDE: one grant on each of 60 resolvers.
        INSERT INTO fixture_grant
        SELECT '{CHAIN}', '{WIDE}', 100 + r, 1, '["set_addr"]', true, false, NULL, NULL
        FROM generate_series(1, {WIDE_RESOLVERS}) r;
        -- SKEWED: 5 EAC grants on resolvers 400 to 404, and derived rows on 2,000 resolvers,
        -- 400 among them.
        INSERT INTO fixture_grant
        SELECT '{CHAIN}', '{SKEWED}', 400 + r, 1, '["set_text"]', true, false, NULL, NULL
        FROM generate_series(0, {SKEWED_EAC} - 1) r;
        INSERT INTO fixture_grant
        SELECT '{CHAIN}', '{SKEWED}', 400 + n % 2000, 1000 + n, '["resolver_control"]', true,
               true, NULL, NULL
        FROM generate_series(1, {SKEWED_DERIVED}) n;

        INSERT INTO project_grant (chain_id, resource_id, subject, scope, block_number,
            event_identity, event_kind, scope_kind, scope_detail, effective_powers,
            grant_source, inheritance_path, revoked)
        SELECT g.chain_id,
               md5(g.chain_id || g.holder || g.resolver || ':' || g.eac || g.derived)::uuid,
               g.holder,
               COALESCE(g.scope,
                        'resolver:' || g.chain_id || ':0x' || lpad(to_hex(g.resolver), 40, '0')),
               10, 'fixture:' || g.chain_id || g.holder || g.resolver || ':' || g.eac || g.derived,
               'PermissionChanged', COALESCE(g.scope_kind, 'resolver'),
               jsonb_build_object('kind', COALESCE(g.scope_kind, 'resolver'),
                   'chain_id', g.chain_id,
                   'resolver_address', '0x' || lpad(to_hex(g.resolver), 40, '0')),
               g.powers,
               CASE WHEN g.derived THEN jsonb_build_object('kind', 'ens_v1_authority',
                        'authority_kind', 'registry', 'authority_key', 'fixture',
                        'source_event_kind', 'ResolverChanged')
                    WHEN NOT g.live THEN '{{}}'::jsonb
                    ELSE jsonb_build_object('kind', 'raw_log', 'source_event', 'EACRolesChanged',
                        'upstream_resource', '0x' || lpad(to_hex(g.eac), 64, '0'),
                        'root_resource', g.eac = 0, 'changed_powers', g.powers) END,
               '[]'::jsonb, NOT g.live
        FROM fixture_grant g;
        -- Other holders' grants, so the subject is selective in the planner's statistics too.
        INSERT INTO project_grant (chain_id, resource_id, subject, scope, block_number,
            event_identity, event_kind, scope_kind, effective_powers, grant_source, revoked)
        SELECT '{CHAIN}', md5('filler' || n)::uuid, '0x' || lpad(to_hex(70000 + n % 5000), 40, '0'),
               'resolver:{CHAIN}:0x' || lpad(to_hex(n % 3000), 40, '0'), 10, 'filler:' || n,
               'PermissionChanged', 'resolver', '["resolver_control"]',
               CASE WHEN n % 10 = 0
                    THEN '{{"kind": "raw_log", "source_event": "EACRolesChanged"}}'::jsonb
                    ELSE '{{"kind": "ens_v1_authority"}}'::jsonb END,
               false
        FROM generate_series(1, 20000) n;
        DROP TABLE fixture_grant;
        ANALYZE chain_lineage, project_family_marker, project_grant;
        "#
    ))
    .execute(&mut *connection)
    .await
    .context("failed to install the role holder fixture")?;
    Ok(())
}
