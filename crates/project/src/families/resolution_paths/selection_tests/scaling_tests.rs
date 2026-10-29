//! How many name surfaces a selection reads.
//!
//! A publication reads the surfaces of the names it touched, of their ancestors and
//! descendants, of the names that hold a changed entry's label directly above a label of an
//! entry that points at its registry, and of the names under the entries that point at a
//! changed registry. That number must not grow with the number of surfaces on the chain. Two
//! cases read every observed ENS surface by design and are not measured here:
//!
//! - A change of the fallback resolver's classification.
//! - A changed path of zero labels (the root registry itself).
//!
//! One more reads every surface that holds a label: a change of an entry whose key equals the
//! key of every slot on a route up to the root, such as the `eth` entry of the registry the
//! root's `eth` entry points at (`eth.eth`). The probe then holds that key alone.
//! `an_eth_keyed_entry_of_the_eth_registry_reads_every_eth_surface` pins the `eth.eth` case.
//!
//! Past `FEW_AFFECTED_NAMES`, 5% of the surfaces, the last step reads every surface once
//! instead of probing each name. Only the two tests of the last step select that many.
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
use crate::families::derived::SUMMARY_WORK_LIST;
use crate::families::guard_tests::{database, executed_scans, indexes_read, reads_through};

const ETH_LABEL: &str = "0x4f5b812789fc606be1b3b16908db13fc7a9adf7ca72641f84d75b47069d3d7f0";
const SURFACES: &str = "name_surfaces";
/// The CTEs of the statement that look names up by label hash.
const ARMS: [&str; 3] = ["source_ancestors", "candidates", "affected"];
const PATH_INDEX: &str = "name_surfaces_project_path_hash_idx";
const KEY_INDEX: &str = "name_surfaces_project_entry_keys_idx";
const STATE_KEY_INDEX: &str = "normalized_events_v2_key_probe_idx";
const EVENTS: &str = "normalized_events";
const ENTRIES: &str = "project_ens_v2_entry_owner";
/// The owner and owner getter of a touched name's registry node state.
const HOLDER: &str = "0x00000000000000000000000000000000000000aa";

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
         ) entry (registry, label);

         INSERT INTO project_registry_pointer (chain_id, namespace, node, block_number,
             event_identity, resolver_address, source_family)
         VALUES ('{CHAIN}', 'ens', '0x' || md5('n2' || 0) || md5(0 || 'n2'), {block},
                 'pointer:0', '0x00000000000000000000000000000000000000c1',
                 'ens_v1_registry_l1');

         INSERT INTO project_family_undo (chain_id, block_number, block_hash, family, key,
             before_image)
         VALUES ('{CHAIN}', {block}, '{hash}', 'project_registry_pointer',
                 jsonb_build_array('{CHAIN}', 'ens',
                     '0x' || md5('n2' || 0) || md5(0 || 'n2'))::text,
                 '{{\"resolver_address\": \"0x00000000000000000000000000000000000000c0\"}}'::jsonb)",
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

/// How a publication comes to hold a changed registry.
#[derive(Clone, Copy)]
enum Route {
    /// A `RegistryCreated` event of the registry in the publication's block.
    Created,
    /// An `Upgraded` event of the registry in the block.
    Upgraded,
    /// A `ContractDiscovered` event of the registry in the block.
    Announced,
    /// The block touches a third-level name. Its unmounted parent has an ENSv1→ENSv2 migration
    /// at block 2, whose evidence holds the registry's creation at block 1.
    Evidence,
}

const ROUTES: [(Route, &str); 4] = [
    (Route::Created, "created"),
    (Route::Upgraded, "upgraded"),
    (Route::Announced, "announced"),
    (Route::Evidence, "migration evidence"),
];

/// The publication at `block` holds, by `route`, the registry the `nth` mounted second-level
/// name points at. It changes nothing else.
async fn change_registry(
    connection: &mut PgConnection,
    tree: Tree,
    route: Route,
    block: i64,
    nth: i64,
) -> Result<()> {
    let mounted = tree.second * 3 / 4 + nth;
    let registry = registry(&mounted.to_string());
    let event = |identity: &str, kind: &str, family: &str, at: i64, name: &str, after: &str| {
        format!(
            "INSERT INTO normalized_events (event_identity, namespace, logical_name_id,
                 event_kind, source_family, manifest_version, chain_id, block_number, block_hash,
                 derivation_kind, canonicality_state, after_state)
             VALUES ('{identity}', 'ens', {name}, '{kind}', '{family}', 1, '{CHAIN}', {at},
                     '{hash}', 'ens_v2_registry_resource_surface', 'canonical', {after});",
            hash = hash(at),
        )
    };
    let carried = |field: &str| format!("jsonb_build_object('{field}', {registry})");
    let statement = match route {
        Route::Created => event(
            &format!("created:{block}"),
            "RegistryCreated",
            "ens_v2_registry_l1",
            block,
            "NULL",
            &carried("registry"),
        ),
        Route::Upgraded => event(
            &format!("upgraded:{block}"),
            "Upgraded",
            "ens_v2_registry_l1",
            block,
            "NULL",
            &carried("proxy_address"),
        ),
        Route::Announced => event(
            &format!("announced:{block}"),
            "ContractDiscovered",
            "ens_v2_migration_l1",
            block,
            "NULL",
            &carried("proxy_address"),
        ),
        Route::Evidence => {
            // Second-level name `nth` is not mounted, and third-level name `nth` is under it.
            ensure!(
                nth < tree.second * 3 / 4,
                "the tree is too small for the route"
            );
            let node =
                |tag: &str| format!("'0x' || md5('{tag}' || {nth}) || md5({nth} || '{tag}')");
            let name = |tag: &str| format!("'ens:' || {}", node(tag));
            let creation = format!("evidence:{block}");
            let evidence = format!(
                "jsonb_build_object('evidence', jsonb_build_array(jsonb_build_object(
                     'event_identity', '{creation}', 'event_kind', 'RegistryCreated')))"
            );
            [
                event(
                    &creation,
                    "RegistryCreated",
                    "ens_v2_registry_l1",
                    1,
                    "NULL",
                    &carried("registry"),
                ),
                event(
                    &format!("migration:{block}"),
                    "MigrationApplied",
                    "ens_v2_migration_l1",
                    2,
                    &name("n2"),
                    &evidence,
                ),
                // The touch is a change that can move the names under the touched name, so the
                // statement reads its ancestors' migration evidence: its registry node state
                // gains a holder.
                format!(
                    "INSERT INTO project_registry_node_state (chain_id, namespace, node,
                         block_number, event_identity, owner, owner_getter, owner_word_unmasked)
                     VALUES ('{CHAIN}', 'ens', {touched}, {block}, 'node:{block}', '{HOLDER}',
                             '{HOLDER}', false);
                     INSERT INTO project_family_undo (chain_id, block_number, block_hash, family,
                         key)
                     VALUES ('{CHAIN}', {block}, '{hash}', 'project_registry_node_state',
                             jsonb_build_array('{CHAIN}', 'ens', {touched})::text)",
                    hash = hash(block),
                    touched = node("n3"),
                ),
            ]
            .concat()
        }
    };
    raw_sql(&statement)
        .execute(&mut *connection)
        .await
        .context("change a registry")?;
    Ok(())
}

/// Prepare the current statement and the frozen one before it (`oracle.rs`) against a session
/// work table.
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
    // and vacuum timing. Every measured run starts with both lists empty.
    raw_sql(&format!(
        "SELECT gin_clean_pending_list('{KEY_INDEX}'), gin_clean_pending_list('{STATE_KEY_INDEX}')"
    ))
    .execute(&mut *connection)
    .await
    .context("flush the pending lists of the two inverted indexes")?;
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

/// The names the frozen statement before this one selects for the publication at `block`.
async fn run_before(connection: &mut PgConnection, block: i64) -> Result<BTreeSet<String>> {
    Ok(run(connection, "path_work_before", block).await?.selected)
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
            ensure!(
                !large.selected.is_empty(),
                "{mode}: the statement selects no names"
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
                 zero_owner, recompose_at, expiry_listable, search_supported,
                 resolver_listing_eligible, resolver_support_exempt, namehash)
             SELECT '{CHAIN}', 'ens:0x' || md5('n2' || n) || md5(n || 'n2'), 'ens', false, false,
                    1800000000 + 10 * 12 + CASE WHEN n = 2 THEN 1 ELSE 0 END, false, false,
                    false, false, '0x' || md5('n2' || n) || md5(n || 'n2')
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
            ensure!(
                run.selected == due,
                "{mode}: selected {:?}, not the due names",
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

// A publication that touches one second-level name only in its own state selects that
// name alone, however many descendants and unrelated surfaces the chain holds, and the
// surfaces it visits do not grow with the namespace.
#[tokio::test]
async fn a_self_only_parent_change_visits_the_same_surfaces_when_the_namespace_grows() -> Result<()>
{
    let (database, pool) = database().await?;
    let result = async {
        let mut connection = pool.acquire().await?;
        let tree = Tree::of(SMALL);
        install(&mut connection, tree).await?;
        let parent = "'ens:0x' || md5('n2' || 0) || md5(0 || 'n2')";
        raw_sql(&format!(
            "INSERT INTO project_family_undo (chain_id, block_number, block_hash, family, key)
             VALUES ('{CHAIN}', 10, '{hash}', 'project_name_state',
                     jsonb_build_array('{CHAIN}', 'ens', {parent})::text)",
            hash = hash(10),
        ))
        .execute(&mut *connection)
        .await?;
        raw_sql("ANALYZE").execute(&mut *connection).await?;
        prepare_statements(&mut connection).await?;
        let expected: BTreeSet<String> = BTreeSet::from([sqlx::query_scalar::<_, String>(
            &format!("SELECT {parent}"),
        )
        .fetch_one(&mut *connection)
        .await?]);
        let mut small = Vec::new();
        for mode in PLAN_MODES {
            set_mode(&mut connection, mode).await?;
            let run = run(&mut connection, "path_work", 10).await?;
            ensure!(
                run.selected == expected,
                "{mode}: a self-only change selects {:?}, not the parent alone",
                run.selected
            );
            small.push(run);
        }
        grow(&mut connection, GROWTH).await?;
        raw_sql("ANALYZE").execute(&mut *connection).await?;
        for (mode, small) in PLAN_MODES.into_iter().zip(&small) {
            set_mode(&mut connection, mode).await?;
            let large = run(&mut connection, "path_work", 10).await?;
            ensure!(
                large.selected == expected,
                "{mode}: the added surfaces changed the selection"
            );
            ensure!(
                large.total() <= small.total() + 16,
                "{mode}: the statement visits {} surfaces of {SMALL} and {} of {} for one \
                 self-only parent",
                small.total(),
                large.total(),
                SMALL + GROWTH
            );
        }
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

/// The node of the fixture's `eth` surface.
const ETH_NODE: &str = "0x00000000000000000000000000000000000000000000000000000000000e7e7e";
const POINTER_INDEX: &str = "project_registry_pointer_pkey";

// Row L. A pointer-only change at `eth`, at its most skewed, and at a second-level name, its
// least, selects the node and exactly the names of three or more labels under it that have no
// resolver of their own. A third of the third-level names and a fifth of the fourth-level
// names have one, and a third and a fifth have a pointer row that clears. Half the
// second-level names have a resolver, which plays no part. In both plan modes, before and
// after ANALYZE, the statement reads `project_registry_pointer` under `affected` only through
// its primary key, at most once per name of three or more labels under the node.
#[tokio::test]
async fn a_pointer_change_probes_each_deeper_name_once_by_its_own_pointer() -> Result<()> {
    let (database, pool) = database().await?;
    let result = async {
        let mut connection = pool.acquire().await?;
        let tree = Tree::of(SMALL);
        let Tree {
            second,
            third,
            fourth,
        } = tree;
        install(&mut connection, tree).await?;
        let s1 = "'0x' || md5('n2' || 1) || md5(1 || 'n2')";
        raw_sql(&format!(
            "INSERT INTO name_surfaces (logical_name_id, namespace, namehash, labelhashes,
                 normalizer_version, visibility_state, chain_id, block_hash, block_number,
                 canonicality_state)
             VALUES ('ens:{ETH_NODE}', 'ens', '{ETH_NODE}', ARRAY['{ETH_LABEL}'], 'fixture',
                     'active', '{CHAIN}', '{zero}', 0, 'canonical');

             INSERT INTO project_registry_pointer (chain_id, namespace, node, block_number,
                 event_identity, resolver_address, source_family)
             SELECT '{CHAIN}', 'ens', node, 1, 'pointer', resolver, 'ens_v1_registry_l1'
             FROM (
                 SELECT '0x' || md5('n2' || n) || md5(n || 'n2'),
                        '0x00000000000000000000000000000000000000d2'
                 FROM generate_series(0, {second} - 1) n WHERE n % 2 = 0
                 UNION ALL
                 SELECT '0x' || md5('n3' || n) || md5(n || 'n3'),
                        CASE n % 3 WHEN 0 THEN '0x00000000000000000000000000000000000000d3'
                             ELSE '0x0000000000000000000000000000000000000000' END
                 FROM generate_series(0, {third} - 1) n WHERE n % 3 < 2
                 UNION ALL
                 SELECT '0x' || md5('n4' || n) || md5(n || 'n4'),
                        CASE n % 5 WHEN 0 THEN '0x00000000000000000000000000000000000000d4'
                             ELSE '' END
                 FROM generate_series(0, {fourth} - 1) n WHERE n % 5 < 2
                 UNION ALL
                 SELECT '{ETH_NODE}', '0x00000000000000000000000000000000000000c1'
                 UNION ALL
                 SELECT {s1}, '0x00000000000000000000000000000000000000c1'
             ) pointer (node, resolver);

             INSERT INTO project_family_undo (chain_id, block_number, block_hash, family, key,
                 before_image)
             VALUES ('{CHAIN}', 10, '{ten}', 'project_registry_pointer',
                     jsonb_build_array('{CHAIN}', 'ens', '{ETH_NODE}')::text,
                     '{{\"resolver_address\": \"0x00000000000000000000000000000000000000c0\"}}'::jsonb),
                    ('{CHAIN}', 11, '{eleven}', 'project_registry_pointer',
                     jsonb_build_array('{CHAIN}', 'ens', {s1})::text,
                     '{{\"resolver_address\": \"0x00000000000000000000000000000000000000c0\"}}'::jsonb)",
            zero = hash(0),
            ten = hash(10),
            eleven = hash(11),
        ))
        .execute(&mut *connection)
        .await
        .context("seed the pointers and their changes")?;
        // The expected names follow the seeding rule, not the tables.
        let deeper = |filter3: &str, filter4: &str| {
            format!(
                "SELECT 'ens:0x' || md5('n3' || n) || md5(n || 'n3')
                 FROM generate_series(0, {third} - 1) n WHERE n % 3 <> 0 AND {filter3}
                 UNION ALL
                 SELECT 'ens:0x' || md5('n4' || n) || md5(n || 'n4')
                 FROM generate_series(0, {fourth} - 1) n WHERE n % 5 <> 0 AND {filter4}"
            )
        };
        let mut publications = Vec::new();
        for (label, block, own, filter3, filter4, under) in [
            (
                "eth",
                10,
                format!("'ens:{ETH_NODE}'"),
                "true".to_owned(),
                "true".to_owned(),
                third + fourth,
            ),
            (
                "a second-level name",
                11,
                format!("'ens:' || {s1}"),
                format!("n % {second} = 1"),
                format!("(n % {third}) % {second} = 1"),
                0,
            ),
        ] {
            let expected: BTreeSet<String> = sqlx::query_scalar(&format!(
                "SELECT {own} UNION ALL {}",
                deeper(&filter3, &filter4)
            ))
            .fetch_all(&mut *connection)
            .await?
            .into_iter()
            .collect();
            let under: i64 = if under > 0 {
                under
            } else {
                sqlx::query_scalar(&format!(
                    "SELECT (SELECT count(*) FROM generate_series(0, {third} - 1) n WHERE {filter3})
                          + (SELECT count(*) FROM generate_series(0, {fourth} - 1) n WHERE {filter4})"
                ))
                .fetch_one(&mut *connection)
                .await?
            };
            ensure!(
                usize::try_from(under)? + 1 > expected.len() && expected.len() > 1,
                "{label}: the fixture gives no name under the node a resolver, or every one"
            );
            publications.push((label, block, expected, under));
        }
        prepare_statements(&mut connection).await?;
        let mut failures = Vec::new();
        for analyzed in [false, true] {
            if analyzed {
                raw_sql("ANALYZE").execute(&mut *connection).await?;
            }
            for (label, block, expected, under) in &publications {
                for mode in PLAN_MODES {
                    set_mode(&mut connection, mode).await?;
                    let run = run(&mut connection, "path_work", *block).await?;
                    let leg = format!("{label}, {mode}, analyzed {analyzed}");
                    if run.selected != *expected {
                        failures.push(format!(
                            "{leg}: selects {} names, not the {} without a resolver of their own",
                            run.selected.len(),
                            expected.len()
                        ));
                    }
                    let mut scans = Vec::new();
                    executed_scans(&run.plan["Plan"], "", &mut scans);
                    let probes: Vec<&Value> = scans
                        .into_iter()
                        .filter(|(cte, node)| {
                            *cte == "affected" && node["Relation Name"] == "project_registry_pointer"
                        })
                        .map(|(_, node)| node)
                        .collect();
                    if probes.is_empty() {
                        failures.push(format!("{leg}: affected never reads a name's own pointer"));
                    }
                    let mut loops = 0;
                    for node in probes {
                        loops += node["Actual Loops"].as_i64().unwrap_or(0);
                        if !reads_through(node, &[POINTER_INDEX]) {
                            failures.push(format!(
                                "{leg}: affected reads project_registry_pointer by {} over {:?}",
                                node["Node Type"],
                                indexes_read(node)
                            ));
                        }
                    }
                    if loops > *under {
                        failures.push(format!(
                            "{leg}: {loops} pointer probes for {under} names under the node"
                        ));
                    }
                }
            }
        }
        ensure!(
            failures.is_empty(),
            "the pointer-only expansion:\n{}",
            failures.join("\n")
        );
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

// A publication that holds a changed registry reads the surfaces of the names under the
// entries that point at the registry, not every surface on the chain. Each way a registry comes
// to be changed is one publication, measured over the fixture and again after as many more
// surfaces that share no label with it.
#[tokio::test]
async fn a_changed_registry_visits_the_names_under_its_mounts() -> Result<()> {
    let (database, pool) = database().await?;
    let result = async {
        let mut connection = pool.acquire().await?;
        let tree = Tree::of(SMALL);
        install(&mut connection, tree).await?;
        let mut publications = Vec::new();
        for (block, (route, name)) in (9..).zip(ROUTES) {
            change_registry(&mut connection, tree, route, block, 100 + block).await?;
            publications.push((name, block));
        }
        raw_sql("ANALYZE").execute(&mut *connection).await?;
        prepare_statements(&mut connection).await?;
        let mut small = Vec::new();
        for &(_, block) in &publications {
            for mode in PLAN_MODES {
                set_mode(&mut connection, mode).await?;
                small.push(run(&mut connection, "path_work", block).await?);
            }
        }
        grow(&mut connection, GROWTH).await?;
        raw_sql("ANALYZE").execute(&mut *connection).await?;
        let mut failures = Vec::new();
        let mut small = small.iter();
        for &(name, block) in &publications {
            for mode in PLAN_MODES {
                let small = small.next().context("a run over the fixture")?;
                set_mode(&mut connection, mode).await?;
                let large = run(&mut connection, "path_work", block).await?;
                let before = run(&mut connection, "path_work_before", block).await?;
                ensure!(
                    !large.selected.is_empty() && large.selected == before.selected,
                    "{name}, {mode}: the statement selects {} names, the one before the probes {}",
                    large.selected.len(),
                    before.selected.len()
                );
                ensure!(
                    large.selected == small.selected,
                    "{name}, {mode}: the added surfaces changed the selection"
                );
                let (few, many) = (small.visited(), large.visited());
                for arm in ARMS {
                    let (few, many) = (
                        few.get(arm).copied().unwrap_or(0),
                        many.get(arm).copied().unwrap_or(0),
                    );
                    if many > few + 16 {
                        failures.push(format!(
                            "{name}, {mode}: {arm} visits {few} surfaces of {SMALL} and {many} of {}",
                            SMALL + GROWTH
                        ));
                    }
                }
                let (few, many, selected) = (small.total(), large.total(), large.selected.len());
                if many > few + 16 || many > 20 * i64::try_from(selected)? {
                    failures.push(format!(
                        "{name}, {mode}: the statement visits {few} surfaces of {SMALL} and {many} \
                         of {} to select {selected} names",
                        SMALL + GROWTH
                    ));
                }
            }
        }
        ensure!(
            failures.is_empty(),
            "the reads of a selection that holds a changed registry grow with the namespace:\n{}",
            failures.join("\n")
        );
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

// The lookups that start from a changed registry reach their tables through indexes, under a
// custom and a generic plan. The events that name the registry as a subregistry come from the
// ENSv2 state key index, the entries from their resource index or primary key, the events of an
// entry's resource from the resource history index, and the ENSv1→ENSv2 migration events of a
// touched name's ancestors from the name history index. No part of the statement scans every
// name surface or every normalized event. The publication holds a created registry and a
// touched name whose parent's migration evidence holds another.
#[tokio::test]
async fn the_registry_route_probes_read_their_indexes() -> Result<()> {
    const RESOURCE_HISTORY: &str = "normalized_events_resource_history_idx";
    const NAME_HISTORY: &str = "normalized_events_name_history_idx";
    const ENTRY_RESOURCE: &str = "project_ens_v2_entry_owner_resource_idx";
    const ENTRY_KEY: &str = "project_ens_v2_entry_owner_pkey";
    let (database, pool) = database().await?;
    let result = async {
        let mut connection = pool.acquire().await?;
        let tree = Tree::of(SMALL);
        install(&mut connection, tree).await?;
        change_registry(&mut connection, tree, Route::Created, 10, 0).await?;
        change_registry(&mut connection, tree, Route::Evidence, 10, 1).await?;
        raw_sql("ANALYZE").execute(&mut *connection).await?;
        prepare_statements(&mut connection).await?;
        let mut failures = Vec::new();
        for mode in PLAN_MODES {
            set_mode(&mut connection, mode).await?;
            let run = run(&mut connection, "path_work", 10).await?;
            let before = run_before(&mut connection, 10).await?;
            ensure!(
                !run.selected.is_empty() && run.selected == before,
                "{mode}: the statement selects {} names, the one before the probes {}",
                run.selected.len(),
                before.len()
            );
            let mut scans = Vec::new();
            executed_scans(&run.plan["Plan"], "", &mut scans);
            for (cte, table, alias, indexes) in [
                ("slots", EVENTS, "pointer", &[STATE_KEY_INDEX][..]),
                ("slots", ENTRIES, "entry", &[ENTRY_RESOURCE, ENTRY_KEY][..]),
                ("slots", EVENTS, "origin", &[RESOURCE_HISTORY][..]),
                (
                    "changed_registries",
                    EVENTS,
                    "migration",
                    &[NAME_HISTORY][..],
                ),
            ] {
                let probes: Vec<&Value> = scans
                    .iter()
                    .filter(|(under, node)| {
                        *under == cte
                            && node["Relation Name"] == table
                            && node["Alias"]
                                .as_str()
                                .is_some_and(|name| name.split('_').next() == Some(alias))
                    })
                    .map(|(_, node)| *node)
                    .collect();
                if probes.is_empty() {
                    failures.push(format!("{mode}: {cte} never read {table} {alias}"));
                }
                for node in &probes {
                    if !reads_through(node, indexes) {
                        failures.push(format!(
                            "{mode}: {cte} reads {table} {alias} by {} over {:?}, not {indexes:?}",
                            node["Node Type"],
                            indexes_read(node)
                        ));
                    }
                }
                if table == ENTRIES
                    && !probes
                        .iter()
                        .any(|node| reads_through(node, &[ENTRY_RESOURCE]))
                {
                    failures.push(format!(
                        "{mode}: {cte} never read {table} {alias} through {ENTRY_RESOURCE}"
                    ));
                }
            }
            for (cte, node) in &scans {
                if node["Node Type"] == "Seq Scan"
                    && [SURFACES, EVENTS]
                        .iter()
                        .any(|table| node["Relation Name"] == *table)
                {
                    failures.push(format!(
                        "{mode}: a sequential scan of {} {} under {cte:?}",
                        node["Relation Name"], node["Alias"]
                    ));
                }
            }
        }
        ensure!(
            failures.is_empty(),
            "the access paths of a selection that holds a changed registry:\n{}",
            failures.join("\n")
        );
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

// A changed registry that no entry points at moves no name, and neither does an `Upgraded`
// event that carries no address. The publication holds one of each and nothing else. It selects
// nothing and reads no name surface.
#[tokio::test]
async fn a_changed_registry_no_slot_names_reads_no_surface() -> Result<()> {
    let (database, pool) = database().await?;
    let result = async {
        let mut connection = pool.acquire().await?;
        install(&mut connection, Tree::of(SMALL)).await?;
        raw_sql(&format!(
            "INSERT INTO normalized_events (event_identity, namespace, event_kind,
                 source_family, manifest_version, chain_id, block_number, block_hash,
                 derivation_kind, canonicality_state, after_state)
             SELECT identity, 'ens', kind, 'ens_v2_registry_l1', 1, '{CHAIN}', 10, '{hash}',
                    'ens_v2_registry_resource_surface', 'canonical', after
             FROM (VALUES
                 ('unmounted', 'RegistryCreated',
                  jsonb_build_object('registry', '0x' || repeat('d', 40))),
                 ('addressless', 'Upgraded', '{{}}'::jsonb)
             ) event (identity, kind, after);
             ANALYZE",
            hash = hash(10),
        ))
        .execute(&mut *connection)
        .await?;
        prepare_statements(&mut connection).await?;
        let mut failures = Vec::new();
        for mode in PLAN_MODES {
            set_mode(&mut connection, mode).await?;
            let run = run(&mut connection, "path_work", 10).await?;
            let before = run_before(&mut connection, 10).await?;
            ensure!(
                run.selected.is_empty() && before.is_empty(),
                "{mode}: selected {:?}, before the probes {before:?}",
                run.selected
            );
            if run.total() != 0 {
                failures.push(format!(
                    "{mode}: the statement visits {:?} surfaces of {SMALL}",
                    run.visited()
                ));
            }
        }
        ensure!(
            failures.is_empty(),
            "a changed registry that no entry points at reads name surfaces:\n{}",
            failures.join("\n")
        );
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

/// A registry no entry points at.
const UNMOUNTED: &str = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
/// The registry the `eth` entry of a mounted second-level name's registry points at.
const NESTED: &str = "0xffffffffffffffffffffffffffffffffffffffff";

/// The publication at `block` changes entries of registries below `.eth` whose labels many
/// surfaces hold, or only the surfaces `grow` adds. With `m` the first mounted second-level name:
///
/// - The `eth` entry of the registry at `m`, mounted by `m`'s entry in `ETH`.
/// - The `eth` entry of a registry no entry points at.
/// - The `eth` entry of `NESTED`, which the `eth` entry of the registry at `m + 1` points at. The
///   entry's key and its slot's are both `eth`, and `m + 1`'s label is the first other key above.
/// - 500 entries of the registry at `m + 2`, whose labels only the surfaces `grow` adds hold.
/// - The entry of second-level name `m + 3` in `ETH`, so that the publication selects names.
async fn change_user_registry_entries(
    connection: &mut PgConnection,
    tree: Tree,
    block: i64,
) -> Result<()> {
    let m = tree.second * 3 / 4;
    let mounted = |offset: i64| registry(&(m + offset).to_string());
    let (first, nested_parent, many) = (mounted(0), mounted(1), mounted(2));
    let second_level = label('s', &(m + 3).to_string());
    let late = label('h', "n");
    raw_sql(&format!(
        "INSERT INTO resources (resource_id, chain_id, block_hash, block_number,
             canonicality_state)
         VALUES (md5({nested_parent} || '{ETH_LABEL}')::uuid, '{CHAIN}', '{zero}', 0,
                 'canonical');

         INSERT INTO project_ens_v2_entry_owner (chain_id, registry, entry_key, token_id,
             resource_id, status, block_number, event_identity)
         VALUES ('{CHAIN}', {nested_parent}, left('{ETH_LABEL}', 58) || '00000000',
                 '{ETH_LABEL}', md5({nested_parent} || '{ETH_LABEL}')::uuid, 'registered', 1,
                 'mount:nested');

         INSERT INTO normalized_events (event_identity, namespace, resource_id, event_kind,
             source_family, manifest_version, chain_id, block_number, block_hash,
             derivation_kind, canonicality_state, after_state, raw_fact_ref)
         VALUES ('mount:nested', 'ens', md5({nested_parent} || '{ETH_LABEL}')::uuid,
                 'SubregistryChanged', 'ens_v2_registry_l1', 1, '{CHAIN}', 1, '{one}',
                 'ens_v2_registry_resource_surface', 'canonical',
                 jsonb_build_object('subregistry', '{NESTED}', 'token_id', '{ETH_LABEL}'),
                 jsonb_build_object('emitting_address', {nested_parent}));

         INSERT INTO project_family_undo (chain_id, block_number, block_hash, family, key)
         SELECT '{CHAIN}', {block}, '{hash}', 'project_ens_v2_entry_owner',
                jsonb_build_array('{CHAIN}', registry, left(label, 58) || '00000000')::text
         FROM (
             SELECT {first}, '{ETH_LABEL}'
             UNION ALL SELECT '{UNMOUNTED}', '{ETH_LABEL}'
             UNION ALL SELECT '{NESTED}', '{ETH_LABEL}'
             UNION ALL SELECT '{ETH}', {second_level}
             UNION ALL SELECT {many}, {late} FROM generate_series(0, 499) n
         ) entry (registry, label)",
        zero = hash(0),
        one = hash(1),
        hash = hash(block),
    ))
    .execute(&mut *connection)
    .await
    .context("change entries of user registries")?;
    Ok(())
}

// Rows V1 and C22. A changed entry of a registry below `.eth` reads the surfaces that hold its
// label directly above a label of an entry that points at the registry, not every surface that
// holds its label. The publication of `change_user_registry_entries` changes entries whose
// labels are `eth`, which every surface holds, or labels only the added surfaces hold. It is
// measured over the fixture and again after as many more surfaces.
#[tokio::test]
async fn entries_of_user_registries_visit_the_same_surfaces_when_the_namespace_grows() -> Result<()>
{
    let (database, pool) = database().await?;
    let result = async {
        let mut connection = pool.acquire().await?;
        let tree = Tree::of(SMALL);
        install(&mut connection, tree).await?;
        change_user_registry_entries(&mut connection, tree, 10).await?;
        raw_sql("ANALYZE").execute(&mut *connection).await?;
        prepare_statements(&mut connection).await?;
        let mut small = Vec::new();
        for mode in PLAN_MODES {
            set_mode(&mut connection, mode).await?;
            small.push(run(&mut connection, "path_work", 10).await?);
        }
        grow(&mut connection, GROWTH).await?;
        raw_sql("ANALYZE").execute(&mut *connection).await?;
        let mut failures = Vec::new();
        for (mode, small) in PLAN_MODES.into_iter().zip(&small) {
            set_mode(&mut connection, mode).await?;
            let large = run(&mut connection, "path_work", 10).await?;
            let before = run_before(&mut connection, 10).await?;
            ensure!(
                !large.selected.is_empty()
                    && large.selected == before
                    && large.selected == small.selected,
                "{mode}: the statement selects {} names over {SMALL} surfaces and {} over {}, \
                 the one before the probes {}",
                small.selected.len(),
                large.selected.len(),
                SMALL + GROWTH,
                before.len()
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
            let (few, many) = (small.total(), large.total());
            if many > few + 16 {
                failures.push(format!(
                    "{mode}: the statement visits {few} surfaces of {SMALL} and {many} of {}",
                    SMALL + GROWTH
                ));
            }
        }
        ensure!(
            failures.is_empty(),
            "the reads of entry changes in user registries grow with the namespace:\n{}",
            failures.join("\n")
        );
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

// Row C6. An entry of a registry that no entry points at moves no name. The publication changes
// the `eth` entry and a second-level label's entry of such a registry, and nothing else. It
// selects nothing and reads no name surface.
#[tokio::test]
async fn an_entry_of_a_registry_no_slot_names_reads_no_surface() -> Result<()> {
    let (database, pool) = database().await?;
    let result = async {
        let mut connection = pool.acquire().await?;
        install(&mut connection, Tree::of(SMALL)).await?;
        let second_level = label('s', "0");
        raw_sql(&format!(
            "INSERT INTO project_family_undo (chain_id, block_number, block_hash, family, key)
             SELECT '{CHAIN}', 10, '{hash}', 'project_ens_v2_entry_owner',
                    jsonb_build_array('{CHAIN}', '{UNMOUNTED}',
                        left(label, 58) || '00000000')::text
             FROM (SELECT '{ETH_LABEL}' UNION ALL SELECT {second_level}) entry (label);
             ANALYZE",
            hash = hash(10),
        ))
        .execute(&mut *connection)
        .await?;
        prepare_statements(&mut connection).await?;
        let mut failures = Vec::new();
        for mode in PLAN_MODES {
            set_mode(&mut connection, mode).await?;
            let run = run(&mut connection, "path_work", 10).await?;
            let before = run_before(&mut connection, 10).await?;
            ensure!(
                run.selected.is_empty() && before.is_empty(),
                "{mode}: selected {:?}, before the probes {before:?}",
                run.selected
            );
            if run.total() != 0 {
                failures.push(format!(
                    "{mode}: the statement visits {:?} surfaces of {SMALL}",
                    run.visited()
                ));
            }
        }
        ensure!(
            failures.is_empty(),
            "an entry of a registry that no entry points at reads name surfaces:\n{}",
            failures.join("\n")
        );
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

// Row V3, a pin. The registry the root's `eth` entry points at changes its own `eth` entry. The
// entry's key and its slot's are both `eth` and the root is directly above, so the probe holds
// `eth` alone: the `candidates` step reads every surface that holds `eth`, and its reads grow by
// every surface added. The selection still equals the frozen statement's.
#[tokio::test]
async fn an_eth_keyed_entry_of_the_eth_registry_reads_every_eth_surface() -> Result<()> {
    let (database, pool) = database().await?;
    let result = async {
        let mut connection = pool.acquire().await?;
        install(&mut connection, Tree::of(SMALL)).await?;
        raw_sql(&format!(
            "INSERT INTO project_family_undo (chain_id, block_number, block_hash, family, key)
             VALUES ('{CHAIN}', 10, '{hash}', 'project_ens_v2_entry_owner',
                     jsonb_build_array('{CHAIN}', '{ETH}',
                         left('{ETH_LABEL}', 58) || '00000000')::text);
             ANALYZE",
            hash = hash(10),
        ))
        .execute(&mut *connection)
        .await?;
        prepare_statements(&mut connection).await?;
        let mut small = Vec::new();
        for mode in PLAN_MODES {
            set_mode(&mut connection, mode).await?;
            small.push(run(&mut connection, "path_work", 10).await?);
        }
        grow(&mut connection, GROWTH).await?;
        raw_sql("ANALYZE").execute(&mut *connection).await?;
        for (mode, small) in PLAN_MODES.into_iter().zip(&small) {
            set_mode(&mut connection, mode).await?;
            let large = run(&mut connection, "path_work", 10).await?;
            let before = run_before(&mut connection, 10).await?;
            ensure!(
                large.selected == before && large.selected == small.selected,
                "{mode}: the statement selects {} names, the one before the probes {}",
                large.selected.len(),
                before.len()
            );
            let candidates = |run: &Run| run.visited().get("candidates").copied().unwrap_or(0);
            let (few, many) = (candidates(small), candidates(&large));
            ensure!(
                many >= few + GROWTH,
                "{mode}: candidates visits {few} surfaces of {SMALL} and {many} of {}, not every \
                 surface that holds eth",
                SMALL + GROWTH
            );
        }
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

/// The index conditions a scan node reads its table by: its own, or those of the bitmap index
/// scans that feed it.
fn index_conditions(node: &Value) -> Vec<&str> {
    let mut conditions: Vec<&str> = node["Index Cond"].as_str().into_iter().collect();
    for child in node["Plans"].as_array().into_iter().flatten() {
        if !matches!(
            child["Parent Relationship"].as_str(),
            Some("SubPlan" | "InitPlan")
        ) && child["Node Type"]
            .as_str()
            .is_some_and(|kind| kind.starts_with("Bitmap"))
        {
            conditions.extend(index_conditions(child));
        }
    }
    conditions
}

// Row P1. The lookups that start from a changed entry's registry reach their tables through
// indexes, under a custom and a generic plan, for the publication of
// `change_user_registry_entries`. The events that name a registry as a subregistry come from the
// ENSv2 state key index, for the registries of the changed entries (`slots`) and for the
// registries above an entry whose key is its slot's (`chains`). The entries come from their
// resource index or primary key, and the events of an entry's resource from the resource history
// index. Every probe of `name_surfaces` in `candidates` reads the entry key index, and one holds
// a changed entry's key and its slot's together. The last step reads each selected name's
// surface by key. No part of the statement scans every name surface or every normalized event.
//
// It is also the skewed row of the last step's table: a few affected names, below steps whose
// row estimates follow arms that can read every surface. Red by run on the plain join the last
// step had before, which read `name_surfaces` by a sequential scan under `""`.
#[tokio::test]
async fn the_changed_entry_probes_read_their_indexes() -> Result<()> {
    const RESOURCE_HISTORY: &str = "normalized_events_resource_history_idx";
    const ENTRY_RESOURCE: &str = "project_ens_v2_entry_owner_resource_idx";
    const ENTRY_KEY: &str = "project_ens_v2_entry_owner_pkey";
    let (database, pool) = database().await?;
    let result = async {
        let mut connection = pool.acquire().await?;
        let tree = Tree::of(SMALL);
        install(&mut connection, tree).await?;
        change_user_registry_entries(&mut connection, tree, 10).await?;
        raw_sql("ANALYZE").execute(&mut *connection).await?;
        prepare_statements(&mut connection).await?;
        let mut failures = Vec::new();
        for mode in PLAN_MODES {
            set_mode(&mut connection, mode).await?;
            let run = run(&mut connection, "path_work", 10).await?;
            let before = run_before(&mut connection, 10).await?;
            ensure!(
                !run.selected.is_empty() && run.selected == before,
                "{mode}: the statement selects {} names, the one before the probes {}",
                run.selected.len(),
                before.len()
            );
            let mut scans = Vec::new();
            executed_scans(&run.plan["Plan"], "", &mut scans);
            for cte in ["slots", "chains"] {
                for (table, alias, indexes) in [
                    (EVENTS, "pointer", &[STATE_KEY_INDEX][..]),
                    (ENTRIES, "entry", &[ENTRY_RESOURCE, ENTRY_KEY][..]),
                    (EVENTS, "origin", &[RESOURCE_HISTORY][..]),
                ] {
                    let probes: Vec<&Value> = scans
                        .iter()
                        .filter(|(under, node)| {
                            *under == cte
                                && node["Relation Name"] == table
                                && node["Alias"]
                                    .as_str()
                                    .is_some_and(|name| name.split('_').next() == Some(alias))
                        })
                        .map(|(_, node)| *node)
                        .collect();
                    if probes.is_empty() {
                        failures.push(format!("{mode}: {cte} never read {table} {alias}"));
                    }
                    for node in &probes {
                        if !reads_through(node, indexes) {
                            failures.push(format!(
                                "{mode}: {cte} reads {table} {alias} by {} over {:?}, not \
                                 {indexes:?}",
                                node["Node Type"],
                                indexes_read(node)
                            ));
                        }
                    }
                    if table == ENTRIES
                        && !probes
                            .iter()
                            .any(|node| reads_through(node, &[ENTRY_RESOURCE]))
                    {
                        failures.push(format!(
                            "{mode}: {cte} never read {table} {alias} through {ENTRY_RESOURCE}"
                        ));
                    }
                }
            }
            let probes: Vec<&Value> = scans
                .iter()
                .filter(|(under, node)| *under == "candidates" && node["Relation Name"] == SURFACES)
                .map(|(_, node)| *node)
                .collect();
            for node in &probes {
                if !reads_through(node, &[KEY_INDEX]) {
                    failures.push(format!(
                        "{mode}: candidates reads {SURFACES} by {} over {:?}, not {KEY_INDEX}",
                        node["Node Type"],
                        indexes_read(node)
                    ));
                }
            }
            if !probes.iter().any(|node| {
                index_conditions(node)
                    .iter()
                    .any(|condition| condition.matches("hashtextextended(").count() >= 2)
            }) {
                failures.push(format!(
                    "{mode}: no candidates probe of {SURFACES} reads {KEY_INDEX} by two keys"
                ));
            }
            // The last step reads each affected name's surface by key, whatever the row
            // estimate of the steps above it.
            last_step_reads_by_key(mode, &scans, &mut failures);
            for (cte, node) in &scans {
                if node["Node Type"] == "Seq Scan"
                    && [SURFACES, EVENTS]
                        .iter()
                        .any(|table| node["Relation Name"] == *table)
                {
                    failures.push(format!(
                        "{mode}: a sequential scan of {} {} under {cte:?}",
                        node["Relation Name"], node["Alias"]
                    ));
                }
            }
        }
        ensure!(
            failures.is_empty(),
            "the access paths of a selection that holds changed entries of user registries:\n{}",
            failures.join("\n")
        );
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

/// The primary key and the unique key of `name_surfaces`, the keys the last step reads a few
/// affected names' surfaces by.
const SURFACE_KEYS: [&str; 2] = [
    "name_surfaces_pkey",
    "name_surfaces_chain_id_logical_name_id_key",
];

/// `FEW_AFFECTED_NAMES` of the fix's `selection.rs`, which the base's `selection.rs` this red
/// variant keeps does not have.
const FEW_AFFECTED_NAMES: &str = "(SELECT floor(0.05::float8 * CASE
            WHEN surfaces.reltuples > 0 THEN surfaces.reltuples::float8
            ELSE (SELECT count(*) FROM (SELECT 1 FROM name_surfaces LIMIT 1000000) counted)::float8
        END)::bigint
        FROM pg_class surfaces WHERE surfaces.oid='name_surfaces'::regclass)";

/// Push a failure unless the last step read `name_surfaces`, and read it only by key.
fn last_step_reads_by_key(mode: &str, scans: &[(&str, &Value)], failures: &mut Vec<String>) {
    let last: Vec<&Value> = scans
        .iter()
        .filter(|(under, node)| under.is_empty() && node["Relation Name"] == SURFACES)
        .map(|(_, node)| *node)
        .collect();
    if last.is_empty() {
        failures.push(format!("{mode}: the last step never read {SURFACES}"));
    }
    for node in last {
        if !reads_through(node, &SURFACE_KEYS) {
            failures.push(format!(
                "{mode}: the last step reads {SURFACES} by {} over {:?}, not \
                 {SURFACE_KEYS:?}",
                node["Node Type"],
                indexes_read(node)
            ));
        }
    }
}

/// The plan nodes right below `node` in its own query: no CTE, initplan or subplan.
fn own_children(node: &Value) -> impl Iterator<Item = &Value> {
    node["Plans"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|child| {
            !matches!(
                child["Parent Relationship"].as_str(),
                Some("InitPlan" | "SubPlan")
            )
        })
}

/// Whether `node`, or a node below it in its own query, ran and is one `kind` accepts.
fn ran_below(node: &Value, kind: &dyn Fn(&Value) -> bool) -> bool {
    (node["Actual Loops"].as_i64().unwrap_or(0) > 0 && kind(node))
        || own_children(node).any(|child| ran_below(child, kind))
}

/// The lowest node of the statement's own query that ran and joins a scan of `affected` with a
/// scan of `name_surfaces`.
fn last_join(node: &Value) -> Option<&Value> {
    if let Some(join) = own_children(node).find_map(last_join) {
        return Some(join);
    }
    let affected = |node: &Value| node["Node Type"] == "CTE Scan" && node["CTE Name"] == "affected";
    let surfaces = |node: &Value| node["Relation Name"] == SURFACES;
    (ran_below(node, &affected) && ran_below(node, &surfaces)).then_some(node)
}

/// Journal a change of the name state of the first `count` fourth-level names at `block`. Such a
/// change recomposes only the name it names.
async fn touch_names(connection: &mut PgConnection, block: i64, count: i64) -> Result<()> {
    raw_sql(&format!(
        "INSERT INTO project_family_undo (chain_id, block_number, block_hash, family, key)
         SELECT '{CHAIN}', {block}, '{hash}', 'project_name_state',
                jsonb_build_array('{CHAIN}', 'ens',
                    'ens:0x' || md5('n4' || n) || md5(n || 'n4'))::text
         FROM generate_series(0, {count} - 1) n",
        hash = hash(block),
    ))
    .execute(&mut *connection)
    .await
    .context("touch names")?;
    Ok(())
}

/// The bound of the last step's probe arm on this database, as the statement computes it.
async fn few_affected_names(connection: &mut PgConnection) -> Result<i64> {
    let few: i64 = sqlx::query_scalar(&format!("SELECT {FEW_AFFECTED_NAMES}"))
        .fetch_one(&mut *connection)
        .await
        .context("compute the bound of the last step")?;
    Ok(few)
}

/// The planner's row count of `table`: 0 or -1 when it was never analyzed.
async fn tuples_by_statistics(connection: &mut PgConnection, table: &str) -> Result<f32> {
    let tuples: f32 =
        sqlx::query_scalar("SELECT reltuples FROM pg_class WHERE oid = $1::text::regclass")
            .bind(table)
            .fetch_one(&mut *connection)
            .await?;
    Ok(tuples)
}

const LINEAGE: &str = "chain_lineage";
/// The primary key of `chain_lineage` and the unique key that leads with it, the keys the last
/// step looks a surface's block up by.
const LINEAGE_KEYS: [&str; 2] = [
    "chain_lineage_pkey",
    "chain_lineage_chain_id_block_hash_block_number_key",
];

/// Blocks 13 to 10,012 above the 13 the database starts with. With 13 blocks the lineage is one
/// page, and a sequential scan of it is the cheapest lookup there is. With these, a scan of the
/// lineage costs more than a lookup by key, as it does on a chain.
async fn grow_lineage(connection: &mut PgConnection) -> Result<()> {
    raw_sql(&format!(
        "INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state)
         SELECT '{CHAIN}', '0x' || lpad(to_hex(block), 64, '0'),
                '0x' || lpad(to_hex(block - 1), 64, '0'), block,
                to_timestamp(1800000000 + block * 12), 'canonical'
         FROM generate_series(13, 10012) block"
    ))
    .execute(&mut *connection)
    .await
    .context("grow the lineage")?;
    Ok(())
}

/// Run each `(row, block, count)` publication under both plan modes and push a failure for each
/// last step that does not read `name_surfaces` as its count says: by key up to `few` names, in
/// one pass beyond. Either way it must read `chain_lineage` only by key. Each publication must
/// select what the frozen statement before it selects, and `count` names.
async fn check_last_step(
    connection: &mut PgConnection,
    rows: &[(&str, i64, i64)],
    few: i64,
    failures: &mut Vec<String>,
) -> Result<()> {
    for &(row, block, count) in rows {
        for mode in PLAN_MODES {
            set_mode(connection, mode).await?;
            let run = run(connection, "path_work", block).await?;
            let before = run_before(connection, block).await?;
            ensure!(
                run.selected == before && i64::try_from(run.selected.len())? == count,
                "{row}, {mode}: the statement selects {} names, the one before it {}, not \
                 {count}",
                run.selected.len(),
                before.len()
            );
            let mut read = Vec::new();
            executed_scans(&run.plan["Plan"], "", &mut read);
            let lineage: Vec<&Value> = read
                .iter()
                .filter(|(under, node)| under.is_empty() && node["Relation Name"] == LINEAGE)
                .map(|(_, node)| *node)
                .collect();
            if lineage.is_empty() {
                failures.push(format!("{row}, {mode}: the last step never read {LINEAGE}"));
            }
            for node in lineage {
                if !reads_through(node, &LINEAGE_KEYS) {
                    failures.push(format!(
                        "{row}, {mode}: the last step reads {LINEAGE} by {} over {:?}, not \
                         {LINEAGE_KEYS:?}",
                        node["Node Type"],
                        indexes_read(node)
                    ));
                }
            }
            let scans = run.scans();
            if count <= few {
                last_step_reads_by_key(&format!("{row}, {mode}"), &scans, failures);
                continue;
            }
            let last: Vec<&Value> = scans
                .iter()
                .filter(|(under, _)| under.is_empty())
                .map(|(_, node)| *node)
                .collect();
            if last.is_empty() {
                failures.push(format!(
                    "{row}, {mode}: the last step never read {SURFACES}"
                ));
            }
            for node in last {
                let loops = node["Actual Loops"].as_i64().unwrap_or(0);
                if loops != 1 {
                    failures.push(format!(
                        "{row}, {mode}: the last step reads {SURFACES} by {} in {loops} loops, \
                         not one",
                        node["Node Type"]
                    ));
                }
            }
            match last_join(&run.plan["Plan"]) {
                Some(join)
                    if matches!(join["Node Type"].as_str(), Some("Hash Join" | "Merge Join")) => {}
                Some(join) => failures.push(format!(
                    "{row}, {mode}: the last step joins affected to {SURFACES} by {}",
                    join["Node Type"]
                )),
                None => failures.push(format!(
                    "{row}, {mode}: no join of affected and {SURFACES} ran"
                )),
            }
        }
    }
    Ok(())
}

// The last step reads the surfaces of the affected names, and their count against `few`, 5% of
// the surfaces, picks how. The rows are the count's cardinality classes, each under a custom
// and a generic plan, after ANALYZE, where `few` is 5% of the planner's row count:
//
// | Row          | Publication                          | Affected      | Last step |
// |--------------|--------------------------------------|---------------|-----------|
// | one          | one touched name                     | 1             | by key    |
// | skewed       | P1's changed user registry entries   | a few         | by key    |
// | one under    | `few - 1` touched names              | few - 1       | by key    |
// | at the bound | `few` touched names                  | few           | by key    |
// | one over     | `few + 1` touched names              | few + 1       | one pass  |
// | most names   | the `.eth` registry upgraded         | every surface | one pass  |
//
// The skewed row is `the_changed_entry_probes_read_their_indexes` (P1). A table never analyzed
// takes the bound from a count instead (`a_never_analyzed_table_bounds_the_last_step_by_a_count`).
//
// "By key" is every read of `name_surfaces` through its primary or unique key. "One pass" is
// every read of it in one loop, joined to `affected` by a hash or merge join, never a nested
// loop that reads it again. Each readable surface still meets `chain_lineage` by a lookup of its
// own, and every read of `chain_lineage` in the last step must go through its primary key or the
// unique key that leads with it. That guards the lookup against an index that bounds only the
// chain. Each row also selects what the frozen statement before it selects, and as many names
// as the row says.
//
// Red, by trace. The rows up to the bound fail on the plain join the last step had before only
// when the planner's estimates make it scan `name_surfaces`. The one run that shows it is P1's
// fixture under a custom plan. One over and most names fail on a last step that probes by key
// whatever the count, which reads `name_surfaces` once per name. At the bound fails when the
// count's bound excludes the bound itself, and one over when it admits one more.
#[tokio::test]
async fn the_last_step_reads_few_names_by_key_and_many_in_one_pass() -> Result<()> {
    let (database, pool) = database().await?;
    let result = async {
        let mut connection = pool.acquire().await?;
        install(&mut connection, Tree::of(SMALL)).await?;
        grow_lineage(&mut connection).await?;
        raw_sql("ANALYZE name_surfaces")
            .execute(&mut *connection)
            .await?;
        let few = few_affected_names(&mut connection).await?;
        let tuples = tuples_by_statistics(&mut connection, SURFACES).await?;
        ensure!(
            few == SMALL / 20,
            "the bound is {few} over {tuples} surfaces by statistics, not 5% of {SMALL}"
        );
        // Each publication changes only its own block, so none sees another's change.
        let rows = [
            ("one", 5, 1),
            ("one under", 6, few - 1),
            ("at the bound", 7, few),
            ("one over", 8, few + 1),
        ];
        for (_, block, count) in rows {
            touch_names(&mut connection, block, count).await?;
        }
        raw_sql(&format!(
            "INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
                 manifest_version, chain_id, block_number, block_hash, derivation_kind,
                 canonicality_state, after_state)
             VALUES ('upgraded:9', 'ens', 'Upgraded', 'ens_v2_registry_l1', 1, '{CHAIN}', 9,
                     '{hash}', 'ens_v2_registry_resource_surface', 'canonical',
                     jsonb_build_object('proxy_address', '{ETH}'))",
            hash = hash(9),
        ))
        .execute(&mut *connection)
        .await
        .context("upgrade the .eth registry")?;
        raw_sql("ANALYZE").execute(&mut *connection).await?;
        ensure!(
            few_affected_names(&mut connection).await? == few,
            "the second ANALYZE moved the bound"
        );
        prepare_statements(&mut connection).await?;
        let mut failures = Vec::new();
        let rows = [rows[0], rows[1], rows[2], rows[3], ("most names", 9, SMALL)];
        check_last_step(&mut connection, &rows, few, &mut failures).await?;
        ensure!(
            failures.is_empty(),
            "the last step's reads of the affected names' surfaces:\n{}",
            failures.join("\n")
        );
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

// A table that was never analyzed has no row count, and the bound is 5% of a count of the
// surfaces. The rows at the bound and one over it read `name_surfaces` by key and in one pass,
// and `chain_lineage` by key, under both plan modes. Autovacuum is off for both tables, so no
// ANALYZE runs under the test.
// Red, by trace: on a fresh table `reltuples` is -1, so a bound read from the row count alone
// is floor(5% of -1), also -1, and the row at the bound then takes the one pass. Red by run on
// a last step that joined `chain_lineage` and filtered the key lookup in one subquery: with no
// statistics the probe read `name_surfaces_visibility_idx` and the full join read
// `name_surfaces` in 13 loops, by trace one for each of the 13 blocks the fixture had then.
#[tokio::test]
async fn a_never_analyzed_table_bounds_the_last_step_by_a_count() -> Result<()> {
    let (database, pool) = database().await?;
    let result = async {
        let mut connection = pool.acquire().await?;
        raw_sql(
            "ALTER TABLE name_surfaces SET (autovacuum_enabled = false);
             ALTER TABLE chain_lineage SET (autovacuum_enabled = false)",
        )
        .execute(&mut *connection)
        .await?;
        install(&mut connection, Tree::of(SMALL)).await?;
        grow_lineage(&mut connection).await?;
        for table in [SURFACES, LINEAGE] {
            let tuples = tuples_by_statistics(&mut connection, table).await?;
            ensure!(
                tuples <= 0.0,
                "{table} has {tuples} rows by statistics, so the test has statistics"
            );
        }
        let few = few_affected_names(&mut connection).await?;
        ensure!(
            few == SMALL / 20,
            "the bound is {few}, not 5% of the {SMALL} surfaces counted"
        );
        let rows = [("at the bound", 7, few), ("one over", 8, few + 1)];
        for (_, block, count) in rows {
            touch_names(&mut connection, block, count).await?;
        }
        prepare_statements(&mut connection).await?;
        let mut failures = Vec::new();
        check_last_step(&mut connection, &rows, few, &mut failures).await?;
        for table in [SURFACES, LINEAGE] {
            ensure!(
                tuples_by_statistics(&mut connection, table).await? <= 0.0,
                "{table} was analyzed during the test"
            );
        }
        ensure!(
            failures.is_empty(),
            "the last step's reads of the affected names' surfaces, never analyzed:\n{}",
            failures.join("\n")
        );
        Ok(())
    }
    .await;
    database.cleanup().await?;
    result
}

// The statement's cost at scale, run by hand:
//
//     PREPARE_COST_SURFACES=100000 cargo test -p bigname-project --lib \
//         prepare_cost_at_scale -- --ignored --nocapture
//
// It prints the execution time, shared buffers and visited surfaces of six publications. Three
// each touch 130 names: one with no ENSv2 change, one that also changes 140 entries, and one in
// which a registry is created instead. Three change nothing but a registry: one upgraded, one
// announced, and one held by the ENSv1→ENSv2 migration evidence of a touched name's parent.
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
        let mut publications: Vec<(&str, i64)> = publications
            .into_iter()
            .map(|(label, block, ..)| (label, block))
            .collect();
        // Blocks below the three publications above, and names none of them touches.
        for (block, (route, label)) in (6..).zip(ROUTES).skip(1) {
            change_registry(&mut connection, tree, route, block, 100 + block).await?;
            publications.push((label, block));
        }
        raw_sql("ANALYZE").execute(&mut *connection).await?;
        prepare_statements(&mut connection).await?;
        for (label, block) in publications {
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
