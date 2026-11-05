//! Plan-shape tests for the address listing family step's reads of its own table and of the
//! address index (`project_address_listing`, `project_address_name_index`), in both plan modes.
//!
//! The fixture holds 200,000 stored rows and as many index rows with sequential scans enabled:
//! one address holds a fifth of them, one resource carries a tenth across every address, 2,000
//! registry-child rows sit under 13 parents and 2,000 rows leave a label unhealed. The step's
//! work tables hold a 1,000-name chunk. Each read must probe the index its axis names, so a name
//! with many holders, a heavy address or a resource shared by many addresses never turns a read
//! into a scan of the table:
//!
//! - W1, the stored rows of a scoped name (`CHANGES`): `project_address_listing_name_work_idx`,
//!   and the probe of each changed key there: the primary key;
//! - W2, the holders of an observed label (`REGISTRY_HOLDERS`, the respell over `LABEL_TARGET`):
//!   a bitmap probe of `project_address_listing_unhealed_idx`;
//! - W3, a touched (address, resource) group (`LEADS`): `project_address_listing_resource_idx`
//!   on all three of its columns;
//! - W4, the address index gate of a composed row (`address_listing_statement`): the index's
//!   primary key on (address, name);
//! - W5, the registry children of a parent derived whole (`CHANGES`):
//!   `project_address_listing_registry_parent_idx`.
//!
//! The respell over `KEY_TARGET` (an undo's restored keys) probes the primary key. The tests pin
//! access paths only: no row or buffer assertions. The write statements (`DELETE`, `INSERT`) join
//! the change table by the primary key, as the child listing's do, and are not pinned here.
use anyhow::{Context, Result, ensure};
use bigname_storage::families::records::{
    ADDRESS_LISTING_COLUMNS, AddressListingRows, address_listing_respell_sql,
    address_listing_statement,
};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::Value;
use sqlx::{PgConnection, postgres::PgArguments, raw_sql};

use super::statements::{CHANGES, KEY_TARGET, LABEL_TARGET, LEADS, PREPARE, REGISTRY_HOLDERS};
use crate::families::derived::summary_plan_tests::{Probe, collect_scans, probe_failure};

const CHAIN: &str = "ethereum-sepolia";
const ROWS: i64 = 200_000;
const PLAN_MODES: [&str; 2] = ["force_generic_plan", "force_custom_plan"];
const HEAVY: &str = "0x00000000000000000000000000000000000000ff";
const SHARED: &str = "00000000-0000-0000-0000-00000000aaaa";

const NAME_WORK: Probe = (
    "project_address_listing_name_work_idx",
    &["(chain_id = ", "(logical_name_id = "],
);
const KEY: Probe = (
    "project_address_listing_pkey",
    &["(chain_id = ", "(address = ", "(logical_name_id = "],
);
const REGISTRY_PARENT: Probe = (
    "project_address_listing_registry_parent_idx",
    &["(chain_id = ", "(registry_parent_logical_name_id = "],
);
const UNHEALED: Probe = (
    "project_address_listing_unhealed_idx",
    &["(unhealed_labelhashes && "],
);
const GROUP: Probe = (
    "project_address_listing_resource_idx",
    &["(chain_id = ", "(resource_id = ", "(address = "],
);
const INDEX_GATE: Probe = (
    "project_address_name_index_pkey",
    &["(address = ", "(logical_name_id = "],
);

#[tokio::test]
async fn the_address_listing_step_probes_its_indexes() -> Result<()> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("family_address_listing_plan").pool_max_connections(1),
    )
    .await?;
    let result = async {
        let mut connection = database.pool().acquire().await?;
        install_schema(&mut connection).await?;
        install_fixture(&mut connection).await?;
        // The work tables drop at commit: the checks run in one transaction.
        raw_sql("BEGIN").execute(&mut *connection).await?;
        let checked = async {
            fill_work_tables(&mut connection).await?;
            check_plans(&mut connection).await
        }
        .await;
        raw_sql("ROLLBACK").execute(&mut *connection).await?;
        checked
    }
    .await;
    database.cleanup().await?;
    result
}

/// One statement whose scans of `relation` under `alias` must all probe one of `probes`.
struct Check {
    label: &'static str,
    sql: String,
    arguments: fn() -> PgArguments,
    scans: &'static [(&'static str, &'static str, &'static [Probe<'static>])],
}

fn chain() -> PgArguments {
    use sqlx::Arguments;
    let mut arguments = PgArguments::default();
    arguments.add(CHAIN).expect("a text argument");
    arguments
}

fn chain_and_labels() -> PgArguments {
    use sqlx::Arguments;
    let mut arguments = chain();
    arguments
        .add(vec![label(3), label(103)])
        .expect("a text array argument");
    arguments
}

fn chain_and_keys() -> PgArguments {
    use sqlx::Arguments;
    let mut arguments = chain();
    arguments
        .add(vec![address(3), address(103)])
        .expect("a text array argument");
    arguments
        .add(vec![name(3), name(103)])
        .expect("a text array argument");
    arguments
}

async fn check_plans(connection: &mut PgConnection) -> Result<()> {
    let checks = [
        Check {
            label: "changes",
            sql: CHANGES.to_owned(),
            arguments: chain,
            scans: &[(
                "project_address_listing",
                "listing",
                &[NAME_WORK, REGISTRY_PARENT, KEY],
            )],
        },
        Check {
            label: "registry holders",
            sql: REGISTRY_HOLDERS.to_owned(),
            arguments: chain_and_labels,
            scans: &[("project_address_listing", "listing", &[UNHEALED])],
        },
        Check {
            label: "respell labels",
            sql: address_listing_respell_sql(LABEL_TARGET),
            arguments: chain_and_labels,
            scans: &[("project_address_listing", "listing", &[UNHEALED])],
        },
        Check {
            label: "respell keys",
            sql: address_listing_respell_sql(KEY_TARGET),
            arguments: chain_and_keys,
            scans: &[("project_address_listing", "listing", &[KEY])],
        },
        Check {
            label: "leads",
            sql: LEADS.to_owned(),
            arguments: chain,
            scans: &[("project_address_listing", "listing", &[GROUP])],
        },
    ];
    let mut failures = Vec::new();
    for mode in PLAN_MODES {
        raw_sql(&format!("SET LOCAL plan_cache_mode = {mode}"))
            .execute(&mut *connection)
            .await?;
        for check in &checks {
            let plan: Value = sqlx::query_scalar_with(
                &format!("EXPLAIN (FORMAT JSON, COSTS OFF) {}", check.sql),
                (check.arguments)(),
            )
            .fetch_one(&mut *connection)
            .await
            .with_context(|| format!("explain {} {mode}", check.label))?;
            failures.extend(failures_of(&plan, check.label, mode, check.scans)?);
        }
        let plan = staged_plan(connection).await?;
        failures.extend(failures_of(
            &plan,
            "stage",
            mode,
            &[("project_address_name_index", "indexed", &[INDEX_GATE])],
        )?);
    }
    ensure!(
        failures.is_empty(),
        "address listing plans:\n{}",
        failures.join("\n")
    );
    Ok(())
}

/// The plan of the staging statement, bound as the step binds it. Its row sets are record sets of
/// bound JSON, which the planner estimates alike whatever they hold.
async fn staged_plan(connection: &mut PgConnection) -> Result<Value> {
    let head =
        format!("INSERT INTO pg_temp.bigname_address_listing_fresh ({ADDRESS_LISTING_COLUMNS})");
    let (staged, arguments) =
        address_listing_statement(&head, CHAIN, &AddressListingRows::default())?;
    sqlx::query_scalar_with(
        &format!("EXPLAIN (FORMAT JSON, COSTS OFF) {staged}"),
        arguments,
    )
    .fetch_one(&mut *connection)
    .await
    .context("explain stage")
}

fn failures_of(
    plan: &Value,
    label: &str,
    mode: &str,
    checks: &[(&str, &str, &[Probe])],
) -> Result<Vec<String>> {
    let mut scans = Vec::new();
    collect_scans(&plan[0]["Plan"], &mut scans);
    let mut failures: Vec<String> = checks
        .iter()
        .filter_map(|(relation, alias, probes)| probe_failure(&scans, relation, alias, probes))
        .map(|failure| format!("{label} {mode}: {failure}"))
        .collect();
    let scanned: Vec<String> = scans
        .iter()
        .filter(|node| {
            node["Node Type"] == "Seq Scan"
                && checks
                    .iter()
                    .any(|(relation, _, _)| node["Relation Name"] == *relation)
        })
        .map(|node| {
            format!(
                "{label} {mode}: a sequential scan of {}",
                node["Relation Name"]
            )
        })
        .collect();
    failures.extend(scanned);
    if !failures.is_empty() {
        failures.push(serde_json::to_string_pretty(plan)?);
    }
    Ok(failures)
}

fn name(n: i64) -> String {
    format!("ens:0x{n:064x}")
}

fn address(n: i64) -> String {
    if n % 5 == 0 {
        HEAVY.to_owned()
    } else {
        format!("0x{:040x}", n % 5_000)
    }
}

fn label(n: i64) -> String {
    format!("0x{:064x}", n + 0x1_0000_0000)
}

async fn install_schema(connection: &mut PgConnection) -> Result<()> {
    raw_sql("CREATE SCHEMA bigname_phase; SET search_path TO bigname_phase, public")
        .execute(&mut *connection)
        .await?;
    for baseline in [
        include_str!("../../../../../storage/schema/baseline/01_chain.sql"),
        include_str!("../../../../../storage/schema/baseline/02_raw_facts.sql"),
        include_str!("../../../../../storage/schema/baseline/03_identity.sql"),
        include_str!("../../../../../storage/schema/baseline/04_manifests.sql"),
        include_str!("../../../../../storage/schema/baseline/05_normalized_events.sql"),
        include_str!("../../../../../storage/schema/baseline/06_projections.sql"),
        include_str!("../../../../../storage/schema/baseline/07_labels.sql"),
    ] {
        raw_sql(baseline).execute(&mut *connection).await?;
    }
    raw_sql("SET jit = off").execute(&mut *connection).await?;
    Ok(())
}

/// Row `i` of the listing and of the index: name `i`, held by `address(i)`; on the shared
/// resource when `i % 10 = 1`; a registry child of one of 13 parents when `i % 100 = 7`; leaving
/// label `i` unhealed when `i % 100 = 3`.
async fn install_fixture(connection: &mut PgConnection) -> Result<()> {
    raw_sql(&format!(
        "INSERT INTO project_address_listing
             (chain_id, address, logical_name_id, namespace, display_key, display_name,
              normalized_name, parent_normalized_name, namehash, resource_id, token_holder,
              effective_controller, role_holder, listed, former_owner, registry_child,
              registry_parent_logical_name_id, registration_lead, public_authority, migrated,
              expires_key, registered_key, created_key, former_expires_key,
              unhealed_labelhashes)
         SELECT '{CHAIN}',
                CASE WHEN i % 5 = 0 THEN '{HEAVY}' ELSE '0x' || lpad(to_hex(i % 5000), 40, '0') END,
                'ens:0x' || lpad(to_hex(i), 64, '0'), 'ens',
                'n' || i || '.eth', 'n' || i || '.eth', 'n' || i || '.eth', 'eth',
                '0x' || lpad(to_hex(i), 64, '0'),
                CASE WHEN i % 10 = 1 THEN '{SHARED}'::uuid
                     ELSE ('00000000-0000-0000-0000-' || lpad(to_hex(i), 12, '0'))::uuid END,
                TRUE, TRUE, FALSE, TRUE, FALSE, i % 100 = 7,
                CASE WHEN i % 100 = 7 THEN 'ens:0x' || lpad(to_hex(i % 13), 64, '0') END,
                FALSE, 'ens_v1', FALSE, -1, -1, -1, -1,
                CASE WHEN i % 100 = 3
                     THEN ARRAY['0x' || lpad(to_hex(i + 4294967296), 64, '0')]
                     ELSE '{{}}'::text[] END
         FROM generate_series(1, {ROWS}) i;
         INSERT INTO project_address_name_index (address, logical_name_id, relation, chain_id)
         SELECT CASE WHEN i % 5 = 0 THEN '{HEAVY}' ELSE '0x' || lpad(to_hex(i % 5000), 40, '0') END,
                'ens:0x' || lpad(to_hex(i), 64, '0'), 'effective_controller', '{CHAIN}'
         FROM generate_series(1, {ROWS}) i;
         ANALYZE project_address_listing;
         ANALYZE project_address_name_index;"
    ))
    .execute(&mut *connection)
    .await
    .context("install the address listing fixture")?;
    Ok(())
}

/// A 1,000-name chunk scoped whole, 50 registry children, 5 parents derived whole, 1,000 changed
/// keys and 1,000 touched groups, the shared resource's heavy address among them. The work
/// tables are not analyzed, as the step leaves them.
async fn fill_work_tables(connection: &mut PgConnection) -> Result<()> {
    for statement in PREPARE {
        raw_sql(statement).execute(&mut *connection).await?;
    }
    raw_sql(&format!(
        "INSERT INTO pg_temp.bigname_address_listing_scope (kind, id)
         SELECT 0, 'ens:0x' || lpad(to_hex(i), 64, '0') FROM generate_series(1, 1000) i
         UNION ALL
         SELECT 1, 'ens:0x' || lpad(to_hex(i * 100 + 7), 64, '0') FROM generate_series(1, 50) i
         UNION ALL
         SELECT 2, 'ens:0x' || lpad(to_hex(i), 64, '0') FROM generate_series(1, 5) i;
         INSERT INTO pg_temp.bigname_address_listing_change (address, logical_name_id)
         SELECT address, logical_name_id FROM project_address_listing
         WHERE logical_name_id <= 'ens:0x' || lpad(to_hex(1000), 64, '0');
         INSERT INTO pg_temp.bigname_address_listing_group (address, resource_id)
         SELECT DISTINCT address, resource_id FROM project_address_listing
         WHERE logical_name_id <= 'ens:0x' || lpad(to_hex(1000), 64, '0');
         INSERT INTO pg_temp.bigname_address_listing_group (address, resource_id)
         VALUES ('{HEAVY}', '{SHARED}'::uuid)"
    ))
    .execute(&mut *connection)
    .await
    .context("fill the address listing work tables")?;
    Ok(())
}
