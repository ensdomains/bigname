//! The registry role-holder page over a fixture of `project_grant` rows, one holder per input
//! class:
//!
//! | Holder | Grants | Expected |
//! | --- | --- | --- |
//! | `SINGLE` | one live root grant | one row, count 1, the grant's powers sorted |
//! | `REVOKED` | a revoked root grant on one registry, a live one on another | the live one only |
//! | `WIDE` | one root grant on each of 60 registries whose addresses differ at digit and letter positions | every page size walks them in byte order, exact `has_more` |
//! | `SKEWED` | 3 root grants, 2,000 per-name registry grants and 500 resolver grants | 3 rows; the plan reads 3 root rows |
//! | `CHURN` | 1 live and 200 revoked root grants | 1 row; the plan reads the 201 root rows, no other |
//! | `TWO_CHAINS` | a root grant on each chain | both, in chain slug order; one chain alone lists its own |
//! | `PER_NAME` | per-name registry grants only | no row |
//! | `MIXED_CASE` | addresses stored in upper case, one address stored in both cases by two instances | lower-case rows in lower-case byte order, the two instances in one row |
//! | `NONE` | none | no row |
//!
//! The plan test runs the statement as the reader sends it under both plan modes, and requires
//! the only read of `project_grant` to be `project_grant_registry_parent_idx`, with the holder
//! and the chains in its condition, reading only the holder's root rows on those chains.
//! Dropping the index then shows the fixture would catch a wrong plan: the same statement reads
//! the skewed holder's other grants too, and returns the same pages.
use anyhow::{Context, Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::{PgConnection, Row, raw_sql};

use super::{REGISTRY_STATEMENT, RoleHolderRegistry, load_registries_by_role_holder};

const CHAIN: &str = "ethereum-mainnet";
const OTHER_CHAIN: &str = "ethereum-sepolia";
const MARKER: i64 = 1_000;
const INDEX: &str = "project_grant_registry_parent_idx";

const SINGLE: &str = "0x00000000000000000000000000000000000000b1";
const REVOKED: &str = "0x00000000000000000000000000000000000000b2";
const WIDE: &str = "0x00000000000000000000000000000000000000b3";
const SKEWED: &str = "0x00000000000000000000000000000000000000b4";
const CHURN: &str = "0x00000000000000000000000000000000000000b5";
const TWO_CHAINS: &str = "0x00000000000000000000000000000000000000b6";
const PER_NAME: &str = "0x00000000000000000000000000000000000000b7";
const MIXED_CASE: &str = "0x00000000000000000000000000000000000000b8";
const NONE: &str = "0x00000000000000000000000000000000000000b9";
const HOLDERS: [&str; 9] = [
    SINGLE, REVOKED, WIDE, SKEWED, CHURN, TWO_CHAINS, PER_NAME, MIXED_CASE, NONE,
];
const WIDE_REGISTRIES: usize = 60;
const SKEWED_ROOT: usize = 3;
const SKEWED_OTHER: usize = 2_500;
const CHURN_REVOKED: usize = 200;

/// Registry `n` as a lower-cased address.
fn registry(n: u32) -> String {
    format!("0x{n:040x}")
}

fn chains() -> Vec<String> {
    vec![CHAIN.to_owned(), OTHER_CHAIN.to_owned()]
}

fn expect(chain: &str, n: u32, count: u64, powers: &[&str]) -> RoleHolderRegistry {
    RoleHolderRegistry {
        chain_id: chain.to_owned(),
        registry_address: registry(n),
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
) -> Result<Vec<RoleHolderRegistry>> {
    load_registries_by_role_holder(&mut *connection, holder, chains, after, limit).await
}

#[tokio::test]
async fn each_holder_class_lists_its_root_registries() -> Result<()> {
    with_database("registry_role_holder_classes", async |connection| {
        let all = chains();
        let cases: Vec<(&str, Vec<RoleHolderRegistry>)> = vec![
            (
                SINGLE,
                vec![expect(CHAIN, 1, 1, &["admin_registrar", "registrar"])],
            ),
            (REVOKED, vec![expect(CHAIN, 3, 1, &["renew"])]),
            (
                SKEWED,
                (0..SKEWED_ROOT as u32)
                    .map(|i| expect(CHAIN, 400 + i, 1, &["registrar"]))
                    .collect(),
            ),
            (CHURN, vec![expect(CHAIN, 500, 1, &["set_parent"])]),
            (
                TWO_CHAINS,
                vec![
                    expect(CHAIN, 12, 1, &["registrar"]),
                    expect(OTHER_CHAIN, 13, 1, &["renew"]),
                ],
            ),
            (PER_NAME, Vec::new()),
            (
                MIXED_CASE,
                vec![
                    expect(CHAIN, 0xa0, 1, &["registrar"]),
                    expect(CHAIN, 0xb0, 1, &["renew"]),
                    expect(CHAIN, 0xc0, 2, &["registrar", "set_parent"]),
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
async fn one_chain_lists_only_its_own_registries() -> Result<()> {
    with_database("registry_role_holder_chain", async |connection| {
        for (chain, n, power) in [(CHAIN, 12, "registrar"), (OTHER_CHAIN, 13, "renew")] {
            let page = load(connection, TWO_CHAINS, &[chain.to_owned()], None, 51).await?;
            ensure!(
                page == vec![expect(chain, n, 1, &[power])],
                "{chain}: {page:?}"
            );
        }
        // Across chains a cursor on the first chain's row continues into the second chain, and
        // one on the second chain's row ends the list.
        let page = load(
            connection,
            TWO_CHAINS,
            &chains(),
            Some((CHAIN, registry(12).as_str())),
            51,
        )
        .await?;
        ensure!(
            page == vec![expect(OTHER_CHAIN, 13, 1, &["renew"])],
            "{page:?}"
        );
        let page = load(
            connection,
            TWO_CHAINS,
            &chains(),
            Some((OTHER_CHAIN, registry(13).as_str())),
            51,
        )
        .await?;
        ensure!(page.is_empty(), "{page:?}");
        // A cursor on the first chain, past every address there, starts the second chain.
        let page = load(
            connection,
            TWO_CHAINS,
            &chains(),
            Some((CHAIN, "0xffffffffffffffffffffffffffffffffffffffff")),
            51,
        )
        .await?;
        ensure!(
            page == vec![expect(OTHER_CHAIN, 13, 1, &["renew"])],
            "{page:?}"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn every_page_size_walks_the_wide_and_mixed_case_holders_in_byte_order() -> Result<()> {
    with_database("registry_role_holder_walk", async |connection| {
        let mixed = vec![registry(0xa0), registry(0xb0), registry(0xc0)];
        for (holder, reference) in [(WIDE, wide_reference()), (MIXED_CASE, mixed)] {
            for page_size in [1_usize, 2, 7, 50, 59, 60, 61, 200] {
                let mut after: Option<(String, String)> = None;
                let mut walked = Vec::new();
                loop {
                    let limit = i64::try_from(page_size + 1)?;
                    let mut page = load(
                        connection,
                        holder,
                        &chains(),
                        after.as_ref().map(|(c, a)| (c.as_str(), a.as_str())),
                        limit,
                    )
                    .await?;
                    let has_more = page.len() > page_size;
                    page.truncate(page_size);
                    walked.extend(page.iter().map(|row| row.registry_address.clone()));
                    ensure!(
                        has_more == (walked.len() < reference.len()),
                        "{holder} page size {page_size}: has_more {has_more} after {}",
                        walked.len()
                    );
                    if !has_more {
                        break;
                    }
                    let last = page.last().context("a page with more has a last row")?;
                    after = Some((last.chain_id.clone(), last.registry_address.clone()));
                }
                ensure!(
                    walked == reference,
                    "{holder} page size {page_size}: {walked:?}"
                );
            }
        }
        Ok(())
    })
    .await
}

#[tokio::test]
async fn the_page_reads_only_the_holders_root_rows_in_both_plan_modes() -> Result<()> {
    with_database("registry_role_holder_plan", async |connection| {
        let resume = wide_reference()[49].clone();
        let mut failures = Vec::new();
        let cases = [
            ("skewed holder", SKEWED, None, SKEWED_ROOT),
            ("churned holder", CHURN, None, 1 + CHURN_REVOKED),
            (
                "wide holder after 50",
                WIDE,
                Some(resume.as_str()),
                WIDE_REGISTRIES,
            ),
        ];
        for (label, holder, after, rows) in cases {
            for mode in ["force_custom_plan", "force_generic_plan"] {
                let plan = explain(connection, mode, holder, after).await?;
                failures.extend(
                    plan_problems(&plan, rows)
                        .into_iter()
                        .map(|problem| format!("{label}, {mode}: {problem}\n{}", plan.join("\n"))),
                );
            }
        }
        ensure!(failures.is_empty(), "{}", failures.join("\n\n"));

        // Without the index the same statement reads the skewed holder's other grants too, and
        // the pages are the same.
        let before = all_pages(connection).await?;
        raw_sql(&format!("DROP INDEX {INDEX}"))
            .execute(&mut *connection)
            .await?;
        let plan = explain(connection, "force_custom_plan", SKEWED, None).await?;
        let read = rows_read(&plan);
        ensure!(
            read >= (SKEWED_ROOT + SKEWED_OTHER) as f64,
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

/// The wide holder's registries in byte order: addresses whose last three hex digits run from
/// `165` to `1a0`, so neighbours differ first at a digit in one and a letter in the other
/// (`…169`, `…16a`).
fn wide_reference() -> Vec<String> {
    let mut wide: Vec<String> = (0..WIDE_REGISTRIES as u32)
        .map(|r| registry(0x165 + r))
        .collect();
    wide.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    wide
}

/// What keeps `plan` from reading `project_grant` only through the index, with the holder and
/// the chains in its condition and at most `rows` rows read: a sequential scan, another index of
/// the table, more rows than that, or a condition without those terms.
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
        ["subject", "chain_id"]
            .iter()
            .all(|term| line.contains(term))
    }) {
        problems.push(format!(
            "no index condition has the holder and the chains: {conditions:?}"
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
    after: Option<&str>,
) -> Result<Vec<String>> {
    // A name of its own for each call: DEALLOCATE ALL would also drop the statements sqlx has
    // prepared on this connection.
    static CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let name = format!(
        "registry_role_holder_page_{}",
        CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    );
    let (after_chain, after_address) = match after {
        Some(address) => (format!("'{CHAIN}'"), format!("'{address}'")),
        None => ("NULL".to_owned(), "NULL".to_owned()),
    };
    raw_sql(&format!(
        "PREPARE {name}(text, text[], text, text, bigint) AS {REGISTRY_STATEMENT};
         SET plan_cache_mode = {mode};
         EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
             EXECUTE {name}('{holder}', ARRAY['{CHAIN}', '{OTHER_CHAIN}'], {after_chain},
                            {after_address}, 51);
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
async fn all_pages(connection: &mut PgConnection) -> Result<Vec<Vec<RoleHolderRegistry>>> {
    let mut pages = Vec::new();
    for holder in HOLDERS {
        pages.push(load(connection, holder, &chains(), None, 201).await?);
    }
    Ok(pages)
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

/// Grants as the writer stores them (crates/project/src/families/permissions.rs, `apply_grant`):
/// a registry root grant has scope `root` and the emitting registry in `scope_detail`, a
/// per-name grant scope `registry`, and an EAC resolver grant its resolver scope. A grant's
/// source is `{}` once revoked. `upper` stores the registry address in upper case, as the
/// adapter would from an upper-cased emitter; `instance` tells two registry instances at one
/// address apart.
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

        CREATE TEMP TABLE fixture_grant (chain_id text, holder text, registry integer,
            instance integer, powers jsonb, scope text, upper boolean);
        INSERT INTO fixture_grant
        VALUES ('{CHAIN}', '{SINGLE}', 1, 0, '["registrar", "admin_registrar"]', 'root', false),
        -- REVOKED: revoked on 2, live on 3.
               ('{CHAIN}', '{REVOKED}', 2, 0, '[]', 'root', false),
               ('{CHAIN}', '{REVOKED}', 3, 0, '["renew"]', 'root', false),
        -- CHURN: live on 500, then 200 revoked below.
               ('{CHAIN}', '{CHURN}', 500, 0, '["set_parent"]', 'root', false),
        -- TWO_CHAINS
               ('{CHAIN}', '{TWO_CHAINS}', 12, 0, '["registrar"]', 'root', false),
               ('{OTHER_CHAIN}', '{TWO_CHAINS}', 13, 0, '["renew"]', 'root', false),
        -- PER_NAME: per-name grants on two registries.
               ('{CHAIN}', '{PER_NAME}', 20, 0, '["set_resolver"]', 'registry', false),
               ('{CHAIN}', '{PER_NAME}', 21, 0, '["renew"]', 'registry', false),
        -- MIXED_CASE: b0 stored upper-cased, so it sorts before a0 as stored; c0 held through two
        -- instances, one stored upper-cased.
               ('{CHAIN}', '{MIXED_CASE}', 160, 0, '["registrar"]', 'root', false),
               ('{CHAIN}', '{MIXED_CASE}', 176, 0, '["renew"]', 'root', true),
               ('{CHAIN}', '{MIXED_CASE}', 192, 0, '["registrar"]', 'root', true),
               ('{CHAIN}', '{MIXED_CASE}', 192, 1, '["set_parent"]', 'root', false);
        INSERT INTO fixture_grant
        SELECT '{CHAIN}', '{CHURN}', 600 + r, 0, '[]', 'root', false
        FROM generate_series(1, {CHURN_REVOKED}) r;
        -- WIDE: one root grant on each of 60 registries.
        INSERT INTO fixture_grant
        SELECT '{CHAIN}', '{WIDE}', 0x165 + r, 0, '["registrar"]', 'root', false
        FROM generate_series(0, {WIDE_REGISTRIES} - 1) r;
        -- SKEWED: 3 root grants, 2,000 per-name grants across 40 registries and 500 resolver
        -- grants across 100 resolvers.
        INSERT INTO fixture_grant
        SELECT '{CHAIN}', '{SKEWED}', 400 + r, 0, '["registrar"]', 'root', false
        FROM generate_series(0, {SKEWED_ROOT} - 1) r;
        INSERT INTO fixture_grant
        SELECT '{CHAIN}', '{SKEWED}', 400 + n % 40, n, '["renew"]', 'registry', false
        FROM generate_series(1, 2000) n;
        INSERT INTO fixture_grant
        SELECT '{CHAIN}', '{SKEWED}', 800 + n % 100, n, '["set_addr"]', 'resolver', false
        FROM generate_series(1, 500) n;

        INSERT INTO project_grant (chain_id, resource_id, subject, scope, block_number,
            event_identity, event_kind, scope_kind, scope_detail, effective_powers,
            grant_source, inheritance_path, revoked)
        SELECT g.chain_id,
               md5(g.chain_id || g.holder || g.registry || ':' || g.instance || g.scope)::uuid,
               g.holder,
               CASE g.scope WHEN 'resolver'
                   THEN 'resolver:' || g.chain_id || ':' || fixture.address ELSE g.scope END,
               10, 'fixture:' || g.chain_id || g.holder || g.registry || ':' || g.instance || g.scope,
               CASE g.scope WHEN 'root' THEN 'RootPermissionChanged' ELSE 'PermissionChanged' END,
               g.scope,
               CASE g.scope WHEN 'resolver'
                   THEN jsonb_build_object('kind', 'resolver', 'chain_id', g.chain_id,
                       'resolver_address', fixture.address)
                   ELSE jsonb_build_object('kind', g.scope, 'chain_id', g.chain_id,
                       'registry_address', fixture.stored) END,
               g.powers,
               CASE WHEN g.powers = '[]'::jsonb THEN '{{}}'::jsonb
                    ELSE jsonb_build_object('kind', 'raw_log', 'source_event', 'EACRolesChanged',
                        'root_resource', g.scope = 'root', 'changed_powers', g.powers) END,
               '[]'::jsonb, g.powers = '[]'::jsonb
        FROM fixture_grant g
        CROSS JOIN LATERAL (
            SELECT '0x' || lpad(to_hex(g.registry), 40, '0') AS address,
                   CASE WHEN g.upper THEN '0x' || upper(lpad(to_hex(g.registry), 40, '0'))
                        ELSE '0x' || lpad(to_hex(g.registry), 40, '0') END AS stored
        ) fixture;
        -- Other holders' grants, half of them root grants, so the subject is selective and the
        -- root rows are many in the planner's statistics too.
        INSERT INTO project_grant (chain_id, resource_id, subject, scope, block_number,
            event_identity, event_kind, scope_kind, scope_detail, effective_powers,
            grant_source, revoked)
        SELECT '{CHAIN}', md5('filler' || n)::uuid, '0x' || lpad(to_hex(70000 + n % 5000), 40, '0'),
               CASE WHEN n % 2 = 0 THEN 'root' ELSE 'registry' END, 10, 'filler:' || n,
               'PermissionChanged', CASE WHEN n % 2 = 0 THEN 'root' ELSE 'registry' END,
               jsonb_build_object('kind', 'root', 'chain_id', '{CHAIN}',
                   'registry_address', '0x' || lpad(to_hex(n % 3000), 40, '0')),
               '["registrar"]',
               '{{"kind": "raw_log", "source_event": "EACRolesChanged"}}'::jsonb, false
        FROM generate_series(1, 20000) n;
        DROP TABLE fixture_grant;
        ANALYZE chain_lineage, project_family_marker, project_grant;
        "#
    ))
    .execute(&mut *connection)
    .await
    .context("failed to install the registry role holder fixture")?;
    Ok(())
}
