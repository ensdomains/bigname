//! TYR-428: a registry's role counts read `normalized_events_registry_roles_idx`, which holds
//! only the registry permission rows the role-count statements keep, and return what the
//! unindexed read returns.
use std::collections::BTreeMap;

use super::*;
use crate::v2::registry_role_counts_test_hooks as role_counts;
use sqlx::PgConnection;

const INDEX: &str = "normalized_events_registry_roles_idx";
const EMITTER_INDEX: &str = "normalized_events_emitter_history_idx";
const CHAIN: &str = "ethereum-mainnet";
/// A root registry, an ordinary registry, a registry with no roles and another registry.
const ROOT: &str = "0x00000000000000000000000000000000000000b0";
const REGISTRY: &str = "0x00000000000000000000000000000000000000b1";
const NO_ROLES: &str = "0x00000000000000000000000000000000000000b2";
const OTHER: &str = "0x00000000000000000000000000000000000000b3";
/// The ordinary registry's label resource at token version 1, the same label's resource at
/// version 0, and its registry-wide resource.
const LABEL: u128 = 0xB100;
const LABEL_OLD: u128 = 0xB101;
const REGISTRY_WIDE: u128 = 0xB102;
/// The root registry's own resource and one of its label resources.
const ROOT_WIDE: u128 = 0xB000;
const ROOT_LABEL: u128 = 0xB001;
const ORPHANED_BLOCK: i64 = 109;

/// One `EACRolesChanged` row: its identity, source family, emitter, resource, subject, the
/// on-chain resource word (label id shifted above the 32-bit version, or zero for the
/// registry-wide resource), role bitmap, block, log and canonicality.
struct Role {
    id: String,
    family: &'static str,
    emitter: &'static str,
    resource: u128,
    subject: u32,
    upstream: u128,
    bitmap: u64,
    block: i64,
    log: i64,
    state: &'static str,
}

#[allow(clippy::too_many_arguments)]
fn role(
    id: &str,
    family: &'static str,
    emitter: &'static str,
    resource: u128,
    subject: u32,
    upstream: u128,
    bitmap: u64,
    block: i64,
    log: i64,
) -> Role {
    Role {
        id: id.to_owned(),
        family,
        emitter,
        resource,
        subject,
        upstream,
        bitmap,
        block,
        log,
        state: "canonical",
    }
}

const V2_REGISTRY: &str = "ens_v2_registry_l1";
const V2_ROOT: &str = "ens_v2_root_l1";
const LABEL_V1: u128 = (1 << 32) | 1;
const LABEL_V0: u128 = 1 << 32;
const ROOT_LABEL_V0: u128 = 2 << 32;

/// Every input class of the role counts, on one chain:
/// - the ordinary registry mixes label `PermissionChanged` rows and registry-wide
///   `RootPermissionChanged` rows, supersedes a subject's grant with more bits and another's with
///   a revocation, has a grant only after block 104, a grant on the label's older token version,
///   an orphaned row and a row on an orphaned block;
/// - the root registry (`ens_v2_root_l1`) has a registry-wide and a label grant;
/// - the registry with no roles emits only non-permission events;
/// - another registry grants on the ordinary registry's label resource.
fn roles() -> Vec<Role> {
    vec![
        role(
            "label-s1-grant",
            V2_REGISTRY,
            REGISTRY,
            LABEL,
            1,
            LABEL_V1,
            0x11,
            101,
            0,
        ),
        role(
            "label-s1-more-bits",
            V2_REGISTRY,
            REGISTRY,
            LABEL,
            1,
            LABEL_V1,
            0x101,
            102,
            0,
        ),
        role(
            "label-s2-grant",
            V2_REGISTRY,
            REGISTRY,
            LABEL,
            2,
            LABEL_V1,
            1,
            102,
            1,
        ),
        role(
            "label-s2-revoke",
            V2_REGISTRY,
            REGISTRY,
            LABEL,
            2,
            LABEL_V1,
            0,
            103,
            0,
        ),
        role(
            "label-s3-late-grant",
            V2_REGISTRY,
            REGISTRY,
            LABEL,
            3,
            LABEL_V1,
            1,
            105,
            0,
        ),
        role(
            "label-old-version",
            V2_REGISTRY,
            REGISTRY,
            LABEL_OLD,
            9,
            LABEL_V0,
            1,
            101,
            1,
        ),
        role(
            "registry-wide-s1",
            V2_REGISTRY,
            REGISTRY,
            REGISTRY_WIDE,
            1,
            0,
            1,
            101,
            2,
        ),
        Role {
            state: "orphaned",
            ..role(
                "label-s5-orphaned",
                V2_REGISTRY,
                REGISTRY,
                LABEL,
                5,
                LABEL_V1,
                1,
                104,
                0,
            )
        },
        role(
            "label-s7-orphaned-block",
            V2_REGISTRY,
            REGISTRY,
            LABEL,
            7,
            LABEL_V1,
            1,
            ORPHANED_BLOCK,
            0,
        ),
        role("root-wide-s1", V2_ROOT, ROOT, ROOT_WIDE, 1, 0, 1, 101, 3),
        role(
            "root-label-s4",
            V2_ROOT,
            ROOT,
            ROOT_LABEL,
            4,
            ROOT_LABEL_V0,
            1,
            101,
            4,
        ),
        role(
            "other-registry-s8",
            V2_REGISTRY,
            OTHER,
            LABEL,
            8,
            LABEL_V1,
            1,
            101,
            5,
        ),
    ]
}

fn word(value: u128) -> String {
    format!("0x{value:064x}")
}

fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

async fn seed(database: &TestDatabase, roles: &[Role]) -> Result<()> {
    let blocks = (100..=110)
        .map(|block| {
            raw_block(
                CHAIN,
                &format!("0xroles{block}"),
                None,
                block,
                1_700_000_000 + block,
            )
        })
        .collect::<Vec<_>>();
    upsert_phase_raw_blocks(&database.pool, &blocks).await?;
    let mut resources: Vec<u128> = roles.iter().map(|role| role.resource).collect();
    resources.sort_unstable();
    resources.dedup();
    let resource_rows = resources
        .iter()
        .map(|id| {
            format!(
                "('{}', {}, '0xroles100', 100, 'canonical')",
                Uuid::from_u128(*id),
                quote(CHAIN)
            )
        })
        .collect::<Vec<_>>()
        .join(",\n");
    let event_rows = roles
        .iter()
        .map(|role| {
            let kind = if role.upstream == 0 {
                "RootPermissionChanged"
            } else {
                "PermissionChanged"
            };
            let after = json!({
                "source_event": "EACRolesChanged",
                "subject": format!("0x{:040x}", role.subject),
                "upstream_resource": word(role.upstream),
                "role_bitmap": format!("0x{:064x}", role.bitmap),
            });
            let raw = json!({
                "kind": "raw_log",
                "event_identity": role.id,
                "emitting_address": role.emitter,
            });
            let block = role.block;
            format!(
                "({}, 'ens', '{}', '{kind}', {}, 1, {}, {block}, '0xroles{block}', \
                 '0xtx{block}', 0, {}, 'ens_v2_permissions', '{}', {}::jsonb, {}::jsonb)",
                quote(&role.id),
                Uuid::from_u128(role.resource),
                quote(role.family),
                quote(CHAIN),
                role.log,
                role.state,
                quote(&after.to_string()),
                quote(&raw.to_string()),
            )
        })
        .collect::<Vec<_>>()
        .join(",\n");
    sqlx::raw_sql(&format!(
        "INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state)
         VALUES {resource_rows};
         INSERT INTO normalized_events (event_identity, namespace, resource_id, event_kind,
             source_family, manifest_version, chain_id, block_number, block_hash, transaction_hash,
             transaction_index, log_index, derivation_kind, canonicality_state, after_state,
             raw_fact_ref)
         VALUES {event_rows};
         UPDATE chain_lineage SET canonicality_state = 'orphaned'
         WHERE chain_id = {} AND block_hash = '0xroles{ORPHANED_BLOCK}';",
        quote(CHAIN),
    ))
    .execute(&database.pool)
    .await?;
    Ok(())
}

/// `count` registry events from `emitter` that are not role changes: what the emitter-history
/// index holds besides the permission rows, and what the role-count index leaves out.
async fn seed_noise(database: &TestDatabase, emitter: &str, count: i64) -> Result<()> {
    sqlx::raw_sql(&format!(
        "INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
             manifest_version, chain_id, block_number, block_hash, transaction_hash,
             transaction_index, log_index, derivation_kind, canonicality_state, after_state,
             raw_fact_ref)
         SELECT 'noise-' || {emitter} || '-' || n, 'ens', 'RegistrationGranted', 'ens_v2_registry_l1', 1,
             {chain}, 100 + n % 10, '0xroles' || (100 + n % 10), '0xnoise' || n, n, 0,
             'ens_v2_registry_resource_surface', 'canonical', '{{}}'::jsonb,
             jsonb_build_object('kind', 'raw_log', 'emitting_address', {emitter})
         FROM generate_series(1, {count}) n;",
        emitter = quote(emitter),
        chain = quote(CHAIN),
    ))
    .execute(&database.pool)
    .await?;
    Ok(())
}

/// Each registry's role count and label role counts at three bounds, as the routes read them.
type Counts = Vec<(&'static str, Option<i64>, u64, BTreeMap<Uuid, u64>)>;

async fn counts(connection: &mut PgConnection) -> Result<Counts> {
    let labels = [LABEL, LABEL_OLD, REGISTRY_WIDE, ROOT_WIDE, ROOT_LABEL].map(Uuid::from_u128);
    let mut result = Vec::new();
    for registry in [ROOT, REGISTRY, NO_ROLES, OTHER] {
        for at in [None, Some(102), Some(104)] {
            let count = role_counts::registry_role_count(connection, CHAIN, registry, at)
                .await
                .map_err(|error| anyhow::anyhow!("{error:?}"))?;
            let per_label =
                role_counts::label_role_counts(connection, CHAIN, registry, &labels, at)
                    .await
                    .map_err(|error| anyhow::anyhow!("{error:?}"))?;
            result.push((registry, at, count, per_label));
        }
    }
    Ok(result)
}

async fn index_exists(connection: &mut PgConnection) -> Result<bool> {
    Ok(sqlx::query_scalar(&format!(
        "SELECT to_regclass('bigname_phase.{INDEX}') IS NOT NULL"
    ))
    .fetch_one(&mut *connection)
    .await?)
}

#[tokio::test]
async fn registry_role_counts_match_the_unindexed_read() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed(&database, &roles()).await?;
    seed_noise(&database, NO_ROLES, 20).await?;
    let mut connection = database.pool.acquire().await?;
    assert!(
        index_exists(&mut connection).await?,
        "{INDEX} must be installed"
    );
    let indexed = counts(&mut connection).await?;
    sqlx::raw_sql(&format!("BEGIN; DROP INDEX bigname_phase.{INDEX}"))
        .execute(&mut *connection)
        .await?;
    let unindexed = counts(&mut connection).await?;
    sqlx::raw_sql("ROLLBACK").execute(&mut *connection).await?;
    assert!(
        index_exists(&mut connection).await?,
        "the rollback must restore {INDEX}"
    );
    assert_eq!(indexed, unindexed, "the index must not change any count");

    let label = Uuid::from_u128(LABEL);
    let find = |registry: &str, at: Option<i64>| {
        indexed
            .iter()
            .find(|row| row.0 == registry && row.1 == at)
            .map(|row| (row.2, row.3.clone()))
            .expect("counted bound")
    };
    // Subject 1 counts once with its newest bitmap, subject 2's revocation hides its grant, the
    // older token version, the orphaned row, the orphaned block and the other registry's grant
    // never count, and the registry-wide grant counts for the registry but no label.
    assert_eq!(find(REGISTRY, None), (3, BTreeMap::from([(label, 2)])));
    assert_eq!(find(REGISTRY, Some(104)), (2, BTreeMap::from([(label, 1)])));
    // At block 102 subject 2's grant is current: its revocation comes at block 103.
    assert_eq!(find(REGISTRY, Some(102)), (3, BTreeMap::from([(label, 2)])));
    assert_eq!(
        find(ROOT, None),
        (2, BTreeMap::from([(Uuid::from_u128(ROOT_LABEL), 1)]))
    );
    assert_eq!(find(NO_ROLES, None), (0, BTreeMap::new()));
    assert_eq!(find(OTHER, None), (1, BTreeMap::from([(label, 1)])));

    drop(connection);
    database.cleanup().await
}

/// Every `normalized_events` scan node of a plan.
fn normalized_event_scans<'a>(plan: &'a Value, scans: &mut Vec<&'a Value>) {
    if plan["Relation Name"] == "normalized_events" {
        scans.push(plan);
    }
    for child in plan["Plans"].as_array().into_iter().flatten() {
        normalized_event_scans(child, scans);
    }
}

#[tokio::test]
async fn registry_role_counts_read_the_roles_index() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let mut fixture = roles();
    // A label with many holders, so the scan has rows to order, and an emitter history mostly
    // made of other events, as a large registry's is.
    for subject in 100..500 {
        fixture.push(role(
            &format!("label-holder-{subject}"),
            V2_REGISTRY,
            REGISTRY,
            LABEL,
            subject,
            LABEL_V1,
            1,
            106,
            i64::from(subject),
        ));
    }
    seed(&database, &fixture).await?;
    seed_noise(&database, REGISTRY, 20_000).await?;
    seed_noise(&database, NO_ROLES, 2_000).await?;
    let mut connection = database.pool.acquire().await?;
    // The deployment's planner cost settings (docker-compose.yml).
    sqlx::raw_sql(
        "ANALYZE normalized_events; ANALYZE chain_lineage; SET jit = off;
         SET random_page_cost = 1.1",
    )
    .execute(&mut *connection)
    .await?;
    let labels = [LABEL, LABEL_OLD, REGISTRY_WIDE].map(Uuid::from_u128);
    // Prepare both statements on this connection as the routes do.
    role_counts::registry_role_count(&mut connection, CHAIN, REGISTRY, Some(110))
        .await
        .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    role_counts::label_role_counts(&mut connection, CHAIN, REGISTRY, &labels, Some(110))
        .await
        .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    let prepared: Vec<(String, String)> =
        sqlx::query_as("SELECT name, statement FROM pg_prepared_statements")
            .fetch_all(&mut *connection)
            .await?;
    let label_array = labels
        .iter()
        .map(|id| quote(&id.to_string()))
        .collect::<Vec<_>>()
        .join(",");
    let statements = [
        ("FROM current_roles", String::new()),
        (
            "GROUP BY resource_id",
            format!(", ARRAY[{label_array}]::uuid[]"),
        ),
    ];
    for (suffix, extra) in &statements {
        let (name, _) = prepared
            .iter()
            .find(|(_, statement)| {
                statement.starts_with(role_counts::CURRENT_ROLES)
                    && statement.trim_end().ends_with(suffix)
            })
            .unwrap_or_else(|| {
                panic!("prepared role-count statement ending {suffix}: {prepared:?}")
            });
        for mode in ["force_custom_plan", "force_generic_plan"] {
            sqlx::raw_sql(&format!("SET plan_cache_mode = {mode}"))
                .execute(&mut *connection)
                .await?;
            let plan: Value = sqlx::query_scalar(&format!(
                "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE \"{}\" ({}, {}, 110{extra})",
                name.replace('"', "\"\""),
                quote(CHAIN),
                quote(REGISTRY),
            ))
            .fetch_one(&mut *connection)
            .await?;
            let mut scans = Vec::new();
            normalized_event_scans(&plan[0]["Plan"], &mut scans);
            assert!(
                !scans.is_empty(),
                "{suffix} {mode}: no normalized_events scan: {plan}"
            );
            for scan in scans {
                assert_eq!(scan["Node Type"], "Index Scan", "{suffix} {mode}: {scan}");
                assert_eq!(scan["Index Name"], INDEX, "{suffix} {mode}: {scan}");
                assert_ne!(scan["Index Name"], EMITTER_INDEX, "{suffix} {mode}: {scan}");
                // The index holds only the rows the statement keeps.
                assert_eq!(
                    scan["Rows Removed by Filter"].as_u64().unwrap_or(0),
                    0,
                    "{suffix} {mode}: {scan}"
                );
            }
        }
        sqlx::raw_sql("RESET plan_cache_mode")
            .execute(&mut *connection)
            .await?;
    }
    drop(connection);
    database.cleanup().await
}

const WALK_INSTALL: &str = include_str!("../../../../ops/walk-index-set/install.sql");
const WALK_DROP: &str = include_str!("../../../../ops/walk-index-set/drop.sql");
const WALK_README: &str = include_str!("../../../../ops/walk-index-set/README.md");
const STORAGE_DOCS: &str = include_str!("../../../../docs/storage.md");

/// The index serves only the API, so the walk index set drops it before a from-zero walk and
/// rebuilds it before Project runs. Both scripts, the install script's definition check and
/// receipt, and the documented counts must name it.
#[test]
fn registry_roles_index_is_in_the_walk_index_set() {
    let qualified = format!("bigname_phase.{INDEX}");
    // The paragraph of docs/storage.md § Walk index set that lists the dropped indexes.
    let walk_docs = STORAGE_DOCS
        .split_once("serve only Project, the API and `phase-runner inspect`")
        .and_then(|(_, rest)| rest.split_once("\n\n"))
        .map(|(list, _)| list)
        .expect("docs/storage.md lists the dropped walk indexes");
    let dropped = WALK_DROP
        .lines()
        .filter(|line| line.starts_with("DROP INDEX CONCURRENTLY IF EXISTS "))
        .count();
    for (place, present) in [
        (
            "drop.sql checked names",
            WALK_DROP.contains(&format!("        '{INDEX}',")),
        ),
        (
            "drop.sql drops",
            WALK_DROP.contains(&format!("DROP INDEX CONCURRENTLY IF EXISTS {qualified};")),
        ),
        (
            "install.sql definition check",
            WALK_INSTALL.contains(&format!("('{INDEX}',"))
                && WALK_INSTALL.contains(&format!(
                    "$def$CREATE INDEX {INDEX} ON bigname_phase.normalized_events "
                )),
        ),
        (
            "install.sql builds",
            WALK_INSTALL.contains(&format!(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS {INDEX}\n"
            )),
        ),
        (
            "install.sql receipt",
            WALK_INSTALL.contains(&format!("to_regclass('{qualified}'),")),
        ),
        (
            "docs/storage.md drop list",
            walk_docs.contains(&format!("`{INDEX}`")),
        ),
    ] {
        assert!(present, "{place} must name {INDEX}");
    }
    for (place, text) in [
        ("drop.sql", format!("allow for all {dropped} before")),
        (
            "README.md",
            format!("`drop.sql` drops the other {dropped} before"),
        ),
        ("README.md", format!("Allow for all {dropped} drops")),
        (
            "docs/storage.md",
            format!("The other {dropped} serve only Project"),
        ),
    ] {
        let source = match place {
            "drop.sql" => WALK_DROP,
            "README.md" => WALK_README,
            _ => STORAGE_DOCS,
        };
        assert!(
            source.contains(&text),
            "{place} must count the {dropped} dropped indexes: {text}"
        );
    }
}
