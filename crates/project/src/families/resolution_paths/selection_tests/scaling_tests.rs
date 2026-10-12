//! How many name surfaces a selection reads.
//!
//! A publication that changes no ENSv2 registry reads the surfaces of the names it touched, of
//! their ancestors and descendants, and of the names that hold a changed entry's label. That
//! number must not grow with the number of surfaces on the chain. Four cases read every
//! observed ENS surface by design and are not measured here:
//!
//! - A registry created, upgraded or announced in the publication.
//! - A touched name that is, or sits under, a name whose retained migration evidence holds a
//!   created registry, whatever block the migration was in.
//! - A change of the fallback resolver's classification.
//! - A changed path of zero labels (the root registry itself).
//!
//! The measure is taken from `EXPLAIN (ANALYZE, FORMAT JSON)`: over every scan of
//! `name_surfaces` and every join above one, the rows returned plus the rows a filter, an index
//! recheck or a join filter removed, times the loops. Row counts follow the plan and the data,
//! not the buffer cache or the clock.
//!
//! The fixture is a tree of second, third and fourth level names under `eth`, with the root,
//! `eth` and some second-level entries mounted, and one publication at block 10 that touches
//! 100 names and changes 20 entries. It holds 50,000 surfaces. Below about 25,000 the planner
//! reads the whole table to look up the touched names' own surfaces by primary key, which is
//! cheaper there and says nothing about a chain with more names.
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, ensure};
use serde_json::Value;
use sqlx::{PgConnection, raw_sql};

use super::{CHAIN, ETH, PLAN_MODES, ROOT, current, hash, oracle};
use crate::families::{
    derived::SUMMARY_WORK_LIST,
    guard_tests::{database, executed_scans, indexes_read, reads_through},
};

const ETH_LABEL: &str = "0x4f5b812789fc606be1b3b16908db13fc7a9adf7ca72641f84d75b47069d3d7f0";
const SURFACES: &str = "name_surfaces";
/// The CTEs of the statement that look names up by label hash.
const ARMS: [&str; 3] = ["source_ancestors", "candidates", "affected"];
const PATH_INDEX: &str = "name_surfaces_project_path_hash_idx";
const KEY_INDEX: &str = "name_surfaces_project_entry_keys_idx";

/// The tree's size: second, third and fourth level names.
#[derive(Clone, Copy)]
struct Tree {
    second: i64,
    third: i64,
    fourth: i64,
}

impl Tree {
    fn of(surfaces: i64) -> Self {
        let (second, third) = (surfaces * 8 / 100, surfaces * 32 / 100);
        Self {
            second,
            third,
            fourth: surfaces - second - third,
        }
    }
}

/// The label hash of the `n`th label of a level, as SQL over the series column `n`.
fn label(level: char, n: &str) -> String {
    format!("'0x' || md5('{level}' || {n}) || md5({n} || '{level}')")
}

/// The registry the second-level name `s` points at, as SQL.
fn registry(s: &str) -> String {
    format!("'0x' || lpad(to_hex(4096 + {s}), 40, '0')")
}

/// Surfaces at block 0: second-level name `s` is `[S(s), eth]`, third-level name `t` is under
/// second-level name `t % second`, and fourth-level name `u` under third-level name
/// `u % third`. The root's `eth` entry points at `ETH`, and the last quarter of the
/// second-level names each point at a registry of their own.
async fn install(connection: &mut PgConnection, tree: Tree) -> Result<()> {
    let Tree {
        second,
        third,
        fourth,
    } = tree;
    let (s, t, u) = (label('s', "n"), label('t', "n"), label('u', "n"));
    let t_parent = label('s', &format!("(n % {second})"));
    let u_parent = label('t', &format!("(n % {third})"));
    let u_grandparent = label('s', &format!("((n % {third}) % {second})"));
    let mounted = format!("generate_series({second} * 3 / 4, {second} - 1) n");
    let registry = registry("n");
    raw_sql(&format!(
        "INSERT INTO name_surfaces (logical_name_id, namespace, namehash, labelhashes,
             normalizer_version, visibility_state, chain_id, block_hash, block_number,
             canonicality_state)
         SELECT 'ens:' || node, 'ens', node, labels, 'fixture', 'active', '{CHAIN}', '{block}',
                0, 'canonical'
         FROM (
             SELECT '0x' || md5('n2' || n) || md5(n || 'n2'), ARRAY[{s}, '{ETH_LABEL}']
             FROM generate_series(0, {second} - 1) n
             UNION ALL
             SELECT '0x' || md5('n3' || n) || md5(n || 'n3'),
                    ARRAY[{t}, {t_parent}, '{ETH_LABEL}']
             FROM generate_series(0, {third} - 1) n
             UNION ALL
             SELECT '0x' || md5('n4' || n) || md5(n || 'n4'),
                    ARRAY[{u}, {u_parent}, {u_grandparent}, '{ETH_LABEL}']
             FROM generate_series(0, {fourth} - 1) n
         ) surface (node, labels);

         CREATE TEMP TABLE mount ON COMMIT DROP AS
         SELECT '{ROOT}' AS registry, '{ETH_LABEL}' AS label, '{ETH}' AS subregistry
         UNION ALL
         SELECT '{ETH}', {s}, {registry} FROM {mounted};

         INSERT INTO resources (resource_id, chain_id, block_hash, block_number,
             canonicality_state)
         SELECT md5(registry || label)::uuid, '{CHAIN}', '{block}', 0, 'canonical' FROM mount;

         INSERT INTO project_ens_v2_entry_owner (chain_id, registry, entry_key, token_id,
             resource_id, status, block_number, event_identity)
         SELECT '{CHAIN}', registry, left(label, 58) || '00000000', label,
                md5(registry || label)::uuid, 'registered', 1, 'mount:' || registry || label
         FROM mount;

         INSERT INTO normalized_events (event_identity, namespace, resource_id, event_kind,
             source_family, manifest_version, chain_id, block_number, block_hash,
             derivation_kind, canonicality_state, after_state, raw_fact_ref)
         SELECT 'mount:' || registry || label, 'ens', md5(registry || label)::uuid,
                'SubregistryChanged', 'ens_v2_registry_l1', 1, '{CHAIN}', 1, '{first}',
                'ens_v2_registry_resource_surface', 'canonical',
                jsonb_build_object('subregistry', subregistry, 'token_id', label),
                jsonb_build_object('emitting_address', registry)
         FROM mount",
        block = hash(0),
        first = hash(1),
    ))
    .execute(&mut *connection)
    .await
    .context("install the tree")?;
    Ok(())
}

/// `count` more surfaces of two to four labels that share no label but `eth` with the tree.
async fn grow(connection: &mut PgConnection, count: i64) -> Result<()> {
    let own = label('g', "n");
    let parent = label('h', "(n % 500)");
    let grandparent = label('i', "(n % 700)");
    raw_sql(&format!(
        "INSERT INTO name_surfaces (logical_name_id, namespace, namehash, labelhashes,
             normalizer_version, visibility_state, chain_id, block_hash, block_number,
             canonicality_state)
         SELECT 'ens:' || node, 'ens', node, labels, 'fixture', 'active', '{CHAIN}', '{block}',
                0, 'canonical'
         FROM (
             SELECT '0x' || md5('g' || n) || md5(n || 'g'),
                    CASE n % 3
                        WHEN 0 THEN ARRAY[{own}, '{ETH_LABEL}']
                        WHEN 1 THEN ARRAY[{own}, {parent}, '{ETH_LABEL}']
                        ELSE ARRAY[{own}, {grandparent}, {parent}, '{ETH_LABEL}']
                    END
             FROM generate_series(0, {count} - 1) n
         ) surface (node, labels)",
        block = hash(0),
    ))
    .execute(&mut *connection)
    .await
    .context("grow the namespace")?;
    Ok(())
}

/// What the publication at a block changed.
#[derive(Clone, Copy)]
struct Stimulus {
    block: i64,
    /// Touched names: a fifth at the second level, two fifths at each of the third and fourth.
    touched: i64,
    /// Changed entries: half in `ETH`, half in the registries second-level names point at.
    entries: i64,
    /// Whether a registry a second-level name points at is created in the block.
    created: bool,
}

async fn stimulate(connection: &mut PgConnection, tree: Tree, stimulus: Stimulus) -> Result<()> {
    let Tree { second, third, .. } = tree;
    let Stimulus {
        block,
        touched,
        entries,
        created,
    } = stimulus;
    let (at_second, at_third) = (touched / 5, touched * 2 / 5);
    let in_eth = entries / 2;
    ensure!(
        at_second <= second / 4 && entries - in_eth <= second / 4,
        "the tree is too small for the stimulus"
    );
    let (s, t) = (label('s', "n"), label('t', "n"));
    let registry = registry("n");
    raw_sql(&format!(
        "INSERT INTO project_family_undo (chain_id, block_number, block_hash, family, key)
         SELECT '{CHAIN}', {block}, '{hash}', 'project_name_state',
                jsonb_build_array('{CHAIN}', 'ens', 'ens:0x' || md5(tag || n) || md5(n || tag))::text
         FROM (
             SELECT 'n2', n FROM generate_series(0, {at_second} - 1) n
             UNION ALL
             SELECT 'n3', {second} / 4 + n FROM generate_series(0, {at_third} - 1) n
             UNION ALL
             SELECT 'n4', {third} / 2 + n
             FROM generate_series(0, {touched} - {at_second} - {at_third} - 1) n
         ) name (tag, n);

         INSERT INTO project_family_undo (chain_id, block_number, block_hash, family, key)
         SELECT '{CHAIN}', {block}, '{hash}', 'project_ens_v2_entry_owner',
                jsonb_build_array('{CHAIN}', registry, left(label, 58) || '00000000')::text
         FROM (
             SELECT '{ETH}', {s} FROM generate_series({second} / 2, {second} / 2 + {in_eth} - 1) n
             UNION ALL
             SELECT {registry}, {t}
             FROM generate_series({second} * 3 / 4, {second} * 3 / 4 + {entries} - {in_eth} - 1) n
         ) entry (registry, label)",
        hash = hash(block),
    ))
    .execute(&mut *connection)
    .await
    .context("journal the stimulus")?;
    if created {
        raw_sql(&format!(
            "INSERT INTO normalized_events (event_identity, namespace, event_kind,
                 source_family, manifest_version, chain_id, block_number, block_hash,
                 derivation_kind, canonicality_state, after_state)
             SELECT 'created:{block}', 'ens', 'RegistryCreated', 'ens_v2_registry_l1', 1,
                    '{CHAIN}', {block}, '{hash}', 'ens_v2_registry_resource_surface', 'canonical',
                    jsonb_build_object('registry', {registry})
             FROM (SELECT {second} * 3 / 4) mounted (n)",
            hash = hash(block),
        ))
        .execute(&mut *connection)
        .await
        .context("create a registry")?;
    }
    Ok(())
}

/// Prepare the current statement and the one before the probes against a session work table.
async fn prepare_statements(connection: &mut PgConnection) -> Result<()> {
    raw_sql(
        "CREATE TEMP TABLE bigname_resolution_path_work (logical_name_id text PRIMARY KEY);
         SET jit = off",
    )
    .execute(&mut *connection)
    .await?;
    for (name, statement) in [
        ("path_work", current()),
        ("path_work_before", oracle::statement(SUMMARY_WORK_LIST)),
    ] {
        raw_sql(&format!(
            "PREPARE {name} (text, bigint, bigint, bigint, text) AS {statement}"
        ))
        .execute(&mut *connection)
        .await
        .with_context(|| format!("prepare {name}"))?;
    }
    Ok(())
}

fn arguments(block: i64) -> String {
    format!(
        "('{CHAIN}', {block}, {}, {}, '{ROOT}')",
        1_800_000_000 + block * 12,
        block - 1
    )
}

/// One measured run of the current statement for the publication at `block`.
struct Run {
    plan: Value,
    selected: BTreeSet<String>,
}

impl Run {
    /// Visited rows of `name_surfaces` by the CTE they were read under, `""` for the last step.
    fn visited(&self) -> BTreeMap<String, i64> {
        let mut totals = BTreeMap::new();
        visit(&self.plan["Plan"], "", &mut totals);
        totals
    }

    fn total(&self) -> i64 {
        self.visited().values().sum()
    }

    /// Shared buffers the whole statement touched.
    fn buffers(&self) -> i64 {
        ["Shared Hit Blocks", "Shared Read Blocks"]
            .into_iter()
            .filter_map(|field| self.plan["Plan"][field].as_i64())
            .sum()
    }

    fn scans(&self) -> Vec<(&str, &Value)> {
        let mut scans = Vec::new();
        executed_scans(&self.plan["Plan"], "", &mut scans);
        scans
            .into_iter()
            .filter(|(_, node)| node["Relation Name"] == SURFACES)
            .collect()
    }
}

/// Add the visited rows of `name_surfaces` at and below `node` to `totals`, and say whether the
/// node reads the table itself, below no CTE of its own.
fn visit(node: &Value, cte: &str, totals: &mut BTreeMap<String, i64>) -> bool {
    let own = node["Subplan Name"]
        .as_str()
        .and_then(|name| name.strip_prefix("CTE "));
    let cte = own.unwrap_or(cte);
    let loops = node["Actual Loops"].as_i64().unwrap_or(0);
    let rows = |field: &str| node[field].as_i64().unwrap_or(0) * loops;
    let mut reads = false;
    if node["Relation Name"] == SURFACES {
        reads = true;
        *totals.entry(cte.to_owned()).or_default() += rows("Actual Rows")
            + rows("Rows Removed by Filter")
            + rows("Rows Removed by Index Recheck");
    }
    for child in node["Plans"].as_array().into_iter().flatten() {
        let below = visit(child, cte, totals);
        // A CTE's own scans do not make the node that holds it a reader of the table.
        reads |= below
            && !child["Subplan Name"]
                .as_str()
                .is_some_and(|name| name.starts_with("CTE "));
    }
    if reads {
        *totals.entry(cte.to_owned()).or_default() += rows("Rows Removed by Join Filter");
    }
    reads
}

async fn run(connection: &mut PgConnection, statement: &str, block: i64) -> Result<Run> {
    raw_sql("TRUNCATE pg_temp.bigname_resolution_path_work")
        .execute(&mut *connection)
        .await?;
    // A GIN lookup reads the index's whole pending list, whose size follows recent inserts
    // and vacuum timing. Every measured run starts with the list empty.
    raw_sql(&format!("SELECT gin_clean_pending_list('{KEY_INDEX}')"))
        .execute(&mut *connection)
        .await
        .context("flush the entry key index's pending list")?;
    let plan: Value = sqlx::query_scalar(&format!(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE {statement} {}",
        arguments(block)
    ))
    .fetch_one(&mut *connection)
    .await
    .with_context(|| format!("explain {statement}"))?;
    let selected =
        sqlx::query_scalar("SELECT logical_name_id FROM pg_temp.bigname_resolution_path_work")
            .fetch_all(&mut *connection)
            .await?
            .into_iter()
            .collect();
    Ok(Run {
        plan: plan[0].clone(),
        selected,
    })
}

async fn set_mode(connection: &mut PgConnection, mode: &str) -> Result<()> {
    raw_sql(&format!("SET plan_cache_mode = {mode}"))
        .execute(connection)
        .await?;
    Ok(())
}

/// Surfaces in the fixture, and surfaces added to it to grow the namespace.
const SMALL: i64 = 50_000;
const GROWTH: i64 = 50_000;
const PUBLICATION: Stimulus = Stimulus {
    block: 10,
    touched: 100,
    entries: 20,
    created: false,
};

// With no registry change, the surfaces a selection reads follow the names the publication
// touched, not the number of surfaces on the chain. The same publication is measured over the
// fixture and again after as many more surfaces that share no label with it.
#[tokio::test]
async fn prepare_visits_the_same_surfaces_when_the_namespace_grows() -> Result<()> {
    let (database, pool) = database().await?;
    let result = async {
        let mut connection = pool.acquire().await?;
        let tree = Tree::of(SMALL);
        install(&mut connection, tree).await?;
        stimulate(&mut connection, tree, PUBLICATION).await?;
        raw_sql("ANALYZE").execute(&mut *connection).await?;
        prepare_statements(&mut connection).await?;
        let mut small = Vec::new();
        for mode in PLAN_MODES {
            set_mode(&mut connection, mode).await?;
            small.push(run(&mut connection, "path_work", PUBLICATION.block).await?);
        }
        grow(&mut connection, GROWTH).await?;
        raw_sql("ANALYZE").execute(&mut *connection).await?;
        let mut failures = Vec::new();
        for (mode, small) in PLAN_MODES.into_iter().zip(&small) {
            set_mode(&mut connection, mode).await?;
            let large = run(&mut connection, "path_work", PUBLICATION.block).await?;
            let before = run(&mut connection, "path_work_before", PUBLICATION.block).await?;
            ensure!(
                !large.selected.is_empty() && large.selected == before.selected,
                "{mode}: the statement selects {} names, the one before the probes {}",
                large.selected.len(),
                before.selected.len()
            );
            ensure!(
                large.selected == small.selected,
                "{mode}: the added surfaces changed the selection"
            );
            let (few, many) = (small.visited(), large.visited());
            for arm in ARMS {
                let (few, many) = (
                    few.get(arm).copied().unwrap_or(0),
                    many.get(arm).copied().unwrap_or(0),
                );
                if many > few + 16 {
                    failures.push(format!(
                        "{mode}: {arm} visits {few} surfaces of {SMALL} and {many} of {}",
                        SMALL + GROWTH
                    ));
                }
            }
            let (few, many, selected) = (small.total(), large.total(), large.selected.len());
            if many > few + 16 || many > 20 * i64::try_from(selected)? {
                failures.push(format!(
                    "{mode}: the statement visits {few} surfaces of {SMALL} and {many} of {} to \
                     select {selected} names",
                    SMALL + GROWTH
                ));
            }
            let (few, many) = (small.buffers(), large.buffers());
            if 2 * many >= 3 * few {
                failures.push(format!(
                    "{mode}: the statement reads {few} buffers over {SMALL} surfaces and {many} \
                     over {}",
                    SMALL + GROWTH
                ));
            }
        }
        ensure!(
            failures.is_empty(),
            "the selection's reads grow with the namespace:\n{}",
            failures.join("\n")
        );
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

// Each arm that looks names up by label hash reaches `name_surfaces` through its index,
// under a custom and a generic plan, and no part of the statement scans the whole table.
#[tokio::test]
async fn the_prepare_probes_read_their_indexes() -> Result<()> {
    let (database, pool) = database().await?;
    let result = async {
        let mut connection = pool.acquire().await?;
        let tree = Tree::of(SMALL);
        install(&mut connection, tree).await?;
        stimulate(&mut connection, tree, PUBLICATION).await?;
        raw_sql("ANALYZE").execute(&mut *connection).await?;
        prepare_statements(&mut connection).await?;
        let mut failures = Vec::new();
        for mode in PLAN_MODES {
            set_mode(&mut connection, mode).await?;
            let run = run(&mut connection, "path_work", PUBLICATION.block).await?;
            let scans = run.scans();
            for (cte, alias, index) in [
                ("source_ancestors", "ancestor", PATH_INDEX),
                ("candidates", "surface", KEY_INDEX),
                ("affected", "child", KEY_INDEX),
            ] {
                let probes: Vec<&Value> = scans
                    .iter()
                    .filter(|(under, node)| {
                        *under == cte
                            && node["Alias"]
                                .as_str()
                                .is_some_and(|name| name.split('_').next() == Some(alias))
                    })
                    .map(|(_, node)| *node)
                    .collect();
                if probes.is_empty() {
                    failures.push(format!("{mode}: {cte} never read {SURFACES} {alias}"));
                }
                for node in probes {
                    if !reads_through(node, &[index]) {
                        failures.push(format!(
                            "{mode}: {cte} reads {SURFACES} {alias} by {} over {:?}, not {index}",
                            node["Node Type"],
                            indexes_read(node)
                        ));
                    }
                }
            }
            for (cte, node) in &scans {
                if node["Node Type"] == "Seq Scan" {
                    failures.push(format!(
                        "{mode}: a sequential scan of {SURFACES} {} under {cte:?}",
                        node["Alias"]
                    ));
                }
            }
        }
        ensure!(
            failures.is_empty(),
            "the selection's access paths:\n{}",
            failures.join("\n")
        );
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

// A publication that changes no row still runs the selection, for the names whose summary
// clock is due. With an empty journal and no event in its window, every arm that reads
// `name_surfaces` is skipped: the statement reads the surfaces of the two due names and no
// other.
#[tokio::test]
async fn a_publication_that_changes_nothing_visits_only_due_names() -> Result<()> {
    let (database, pool) = database().await?;
    let result = async {
        let mut connection = pool.acquire().await?;
        install(&mut connection, Tree::of(SMALL)).await?;
        // Names 0 and 1 are due at block 10. Name 2 is due one second later.
        raw_sql(&format!(
            "INSERT INTO project_name_summary (chain_id, logical_name_id, namespace, serving,
                 zero_owner, recompose_at, expiry_listable, search_supported)
             SELECT '{CHAIN}', 'ens:0x' || md5('n2' || n) || md5(n || 'n2'), 'ens', false, false,
                    1800000000 + 10 * 12 + CASE WHEN n = 2 THEN 1 ELSE 0 END, false, false
             FROM generate_series(0, 2) n;
             ANALYZE"
        ))
        .execute(&mut *connection)
        .await?;
        prepare_statements(&mut connection).await?;
        let due: BTreeSet<String> = sqlx::query_scalar(
            "SELECT 'ens:0x' || md5('n2' || n) || md5(n || 'n2') FROM generate_series(0, 1) n",
        )
        .fetch_all(&mut *connection)
        .await?
        .into_iter()
        .collect();
        for mode in PLAN_MODES {
            set_mode(&mut connection, mode).await?;
            let run = run(&mut connection, "path_work", 10).await?;
            let before = run_before(&mut connection, 10).await?;
            ensure!(
                run.selected == due && before == due,
                "{mode}: selected {:?}, before the probes {before:?}",
                run.selected
            );
            ensure!(
                run.total() == 2,
                "{mode}: the statement visits {:?} surfaces for two due names",
                run.visited()
            );
        }
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

async fn run_before(connection: &mut PgConnection, block: i64) -> Result<BTreeSet<String>> {
    Ok(run(connection, "path_work_before", block).await?.selected)
}

// The statement's cost at scale, run by hand:
//
//     PREPARE_COST_SURFACES=100000 cargo test -p bigname-project --lib \
//         prepare_cost_at_scale -- --ignored --nocapture
//
// It prints the execution time, shared buffers and visited surfaces of three publications
// that each touch 130 names: one with no ENSv2 change, one that also changes 140 entries, and
// one in which a registry is created instead.
#[tokio::test]
#[ignore = "measurement; run by hand"]
async fn prepare_cost_at_scale() -> Result<()> {
    let surfaces: i64 = std::env::var("PREPARE_COST_SURFACES")
        .unwrap_or_else(|_| "100000".to_owned())
        .parse()?;
    let (database, pool) = database().await?;
    let result = async {
        let mut connection = pool.acquire().await?;
        let tree = Tree::of(surfaces);
        install(&mut connection, tree).await?;
        let publications = [
            ("no ENSv2 change", 10, 0, false),
            ("entry changes", 11, 140, false),
            ("a created registry", 12, 0, true),
        ];
        for (_, block, entries, created) in publications {
            let stimulus = Stimulus {
                block,
                touched: 130,
                entries,
                created,
            };
            stimulate(&mut connection, tree, stimulus).await?;
        }
        raw_sql("ANALYZE").execute(&mut *connection).await?;
        prepare_statements(&mut connection).await?;
        for (label, block, ..) in publications {
            for mode in PLAN_MODES {
                set_mode(&mut connection, mode).await?;
                // The second run reads warm buffers.
                run(&mut connection, "path_work", block).await?;
                let run = run(&mut connection, "path_work", block).await?;
                println!(
                    "PREPARE_COST surfaces={surfaces} publication={label:?} mode={mode} \
                     execution_ms={} shared_buffers={} visited_surfaces={} selected={}",
                    run.plan["Execution Time"],
                    run.buffers(),
                    run.total(),
                    run.selected.len()
                );
            }
        }
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}
