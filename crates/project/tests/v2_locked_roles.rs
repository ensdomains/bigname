//! ENSv2 `locked_roles` converge across full, incremental, and redo builds: a registration's
//! summary reads the admin roles held on its registry root, so a root permission change outside
//! the registration's own event window must still rebuild the registration.
//! (upstream: .refs/ens_v2/contracts/src/access-control/EnhancedAccessControl.sol:L418-L424 @ ens_v2@a971bd64)
//! (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L560-L572 @ ens_v2@a971bd64)
//! (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L24-L45 @ ens_v2@a971bd64)

use anyhow::Result;
use bigname_project::{BatchRequest, Engine, Marker, RunMode};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgPool, raw_sql};

const CHAIN: &str = "ethereum-sepolia";
const REGISTRY: &str = "0x00000000000000000000000000000000000021aa";
const INSTANCE: &str = "00000000-0000-0000-0000-000000000001";
const ROOT: &str = "0e3f3a4a-1e2c-5b1a-9c7d-0000000000aa";
const CHILD: &str = "0e3f3a4a-1e2c-5b1a-9c7d-0000000000bb";
const CHILD_LINEAGE: &str = "0e3f3a4a-1e2c-5b1a-9c7d-0000000000cc";
const ZERO_WORD: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";
const CHILD_WORD: &str = "0x0000000000000000000000000000000000000000000000000000000000001389";
const HOLDER: &str = "0x0000000000000000000000000000000000002011";
const ROOT_ADMIN: &str = "0x0000000000000000000000000000000000002022";
const CHILD_ADMIN: &str = "0x0000000000000000000000000000000000002033";
const ALL_LOCKED: &[&str] = &[
    "unregister",
    "renew",
    "set_subregistry",
    "set_resolver",
    "transfer",
];
const TABLES: &str = "permissions_current permissions_current_resource_summary";

fn hash(block: i64) -> String {
    format!("0x{block:064x}")
}

#[rustfmt::skip]
async fn database(prefix: &str) -> Result<(TestDatabase, PgPool)> {
    let database = TestDatabase::create(TestDatabaseConfig::new(prefix)).await?;
    let pool = database.pool().clone(); let name: String = sqlx::query_scalar("SELECT current_database()").fetch_one(&pool).await?; let mut tx = pool.begin().await?;
    sqlx::query("CREATE SCHEMA bigname_phase").execute(&mut *tx).await?;
    raw_sql(&format!("ALTER DATABASE \"{}\" SET search_path TO bigname_phase, public", name.replace('"', r#""""#))).execute(&mut *tx).await?;
    sqlx::query("SET LOCAL search_path TO bigname_phase, public").execute(&mut *tx).await?;
    for script in [
        include_str!("../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../schema-v2/baseline/02_raw_facts.sql"),
        include_str!("../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../schema-v2/baseline/04_manifests.sql"),
        include_str!("../../../schema-v2/baseline/05_normalized_events.sql"),
        include_str!("../../../schema-v2/baseline/06_projections.sql"),
        include_str!("../../../schema-v2/baseline/07_labels.sql"),
        include_str!("../../../schema-v2/baseline/08_heartbeats.sql"),
        include_str!("../../../schema-v2/baseline/09_divergence.sql"),
        include_str!("../../../schema-v2/baseline/10_phase_state.sql"),
    ] {
        raw_sql(script).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    pool.set_connect_options(pool.connect_options().as_ref().clone().options([("search_path", "bigname_phase,public")]));
    let mut connections = Vec::new();
    for _ in 0..pool.options().get_max_connections() {
        connections.push(pool.acquire().await?);
    }
    for connection in &mut connections {
        sqlx::query("SET search_path TO bigname_phase, public").execute(&mut **connection).await?;
    }
    Ok((database, pool))
}

/// One `EACRolesChanged` row: `root` selects `RootPermissionChanged` on the registry root.
async fn permission(
    pool: &PgPool,
    root: bool,
    block: i64,
    subject: &str,
    old_powers: &[&str],
    powers: &[&str],
) -> Result<()> {
    let (resource, upstream) = if root {
        (ROOT, ZERO_WORD)
    } else {
        (CHILD, CHILD_WORD)
    };
    let kind = if root {
        "RootPermissionChanged"
    } else {
        "PermissionChanged"
    };
    let source = json!({
        "kind": "raw_log",
        "source_event": "EACRolesChanged",
        "upstream_resource": upstream,
        "root_resource": root,
        "changed_powers": powers,
        "registry_contract_instance_id": INSTANCE,
    });
    let scope_kind = if root { "registry_root" } else { "registry" };
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, resource_id, event_kind,
             source_family, manifest_version, chain_id, block_number, block_hash,
             transaction_hash, transaction_index, log_index, derivation_kind,
             canonicality_state, before_state, after_state
         ) VALUES ($1, 'ens', NULL, $2::uuid, $3, 'ens_v2_registry_l1', 1, $4, $5, $6,
                   $7, 0, 0, 'ens_v2_permissions', 'canonical', $8, $9)",
    )
    .bind(format!("{block}:{kind}:{subject}"))
    .bind(resource)
    .bind(kind)
    .bind(CHAIN)
    .bind(block)
    .bind(hash(block))
    .bind(format!("0x{:064x}", 7_000 + block))
    .bind(json!({"subject": subject, "effective_powers": old_powers}))
    .bind(json!({
        "subject": subject,
        "scope": {"kind": scope_kind, "chain_id": CHAIN, "registry_address": REGISTRY},
        "effective_powers": powers,
        "grant_source": if powers.is_empty() { json!({}) } else { source.clone() },
        "revocation_source": if powers.is_empty() { source } else { Value::Null },
        "inheritance_path": if root {
            json!([{"kind": "registry_root_fallback", "chain_id": CHAIN,
                    "registry_address": REGISTRY, "upstream_resource": upstream}])
        } else {
            json!([])
        },
        "transfer_behavior": {},
        "source_event": "EACRolesChanged",
        "upstream_resource": upstream,
        "resource": upstream,
        "root_resource": root,
        "registry_contract_instance_id": INSTANCE,
    }))
    .execute(pool)
    .await?;
    Ok(())
}

#[rustfmt::skip]
async fn seed(pool: &PgPool) -> Result<()> {
    for block in 100..=103 {
        sqlx::query(
            "INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
             VALUES ($1, $2, $3, to_timestamp($4), 'canonical')",
        )
        .bind(CHAIN).bind(hash(block)).bind(block).bind(1_700_000_000_f64 + block as f64)
        .execute(pool)
        .await?;
    }
    let h100 = hash(100);
    let provenance = |upstream: &str| json!({
        "adapter": "ens_v2_permissions", "chain_id": CHAIN, "source_family": "ens_v2_registry_l1",
        "registry_address": REGISTRY, "registry_contract_instance_id": INSTANCE,
        "upstream_resource": upstream, "manifest_version": 1, "source_manifest_id": 1,
    });
    sqlx::query(
        "INSERT INTO token_lineages (token_lineage_id, chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1::uuid, $2, $3, 100, 'canonical')",
    )
    .bind(CHILD_LINEAGE).bind(CHAIN).bind(&h100)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO resources (resource_id, token_lineage_id, chain_id, block_hash, block_number, provenance, canonicality_state)
         VALUES ($1::uuid, NULL, $3, $4, 100, $5, 'canonical'),
                ($2::uuid, $6::uuid, $3, $4, 100, $7, 'canonical')",
    )
    .bind(ROOT).bind(CHILD).bind(CHAIN).bind(&h100).bind(provenance(ZERO_WORD)).bind(CHILD_LINEAGE).bind(provenance(CHILD_WORD))
    .execute(pool)
    .await?;
    // 100: the registration's holder gets `unregister`; 101: a root admin gains `admin_renew`;
    // 102: that root admin is revoked; 103: the registration gains its own `admin_set_resolver`.
    permission(pool, false, 100, HOLDER, &[], &["unregister"]).await?;
    permission(pool, true, 101, ROOT_ADMIN, &[], &["admin_renew"]).await?;
    permission(pool, true, 102, ROOT_ADMIN, &["admin_renew"], &[]).await?;
    permission(pool, false, 103, CHILD_ADMIN, &[], &["admin_set_resolver"]).await?;
    Ok(())
}

async fn run(pool: &PgPool, target: i64, resume: Option<Marker>) -> Result<Marker> {
    Ok(Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block: target,
            affected_from_block: resume.as_ref().map_or(100, |marker| marker.number + 1),
            affected_to_block: target,
            resume_current: resume,
            mode: RunMode::Normal,
        })
        .await?
        .current)
}

async fn redo(pool: &PgPool, target: i64, block: i64) -> Result<()> {
    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block: target,
            affected_from_block: block,
            affected_to_block: block,
            resume_current: None,
            mode: RunMode::Redo,
        })
        .await?;
    Ok(())
}

async fn snapshot(pool: &PgPool) -> Result<Value> {
    let mut snapshot = serde_json::Map::new();
    for table in TABLES.split_whitespace() {
        let rows: Value = sqlx::query_scalar(&format!(
            "SELECT COALESCE(jsonb_agg(value ORDER BY value::text), '[]'::jsonb)
             FROM (SELECT to_jsonb(row) - 'last_recomputed_at' - 'inserted_at'
                          - 'canonicality_summary' - 'chain_positions' AS value
                   FROM {table} row) canonical"
        ))
        .fetch_one(pool)
        .await?;
        snapshot.insert(table.to_owned(), rows);
    }
    Ok(Value::Object(snapshot))
}

async fn child_summary(pool: &PgPool) -> Result<(Option<String>, Option<Value>)> {
    Ok(sqlx::query_as(
        "SELECT root_resource_id::text, resource_restrictions
         FROM permissions_current_resource_summary
         WHERE resource_id = $1::uuid",
    )
    .bind(CHILD)
    .fetch_one(pool)
    .await?)
}

fn locked(roles: &[&str]) -> Option<Value> {
    Some(json!({"kind": "ens_v2_registry", "locked_roles": roles}))
}

fn without(role: &str) -> Vec<&'static str> {
    ALL_LOCKED
        .iter()
        .copied()
        .filter(|candidate| *candidate != role)
        .collect()
}

#[tokio::test]
async fn locked_roles_follow_root_admin_changes_across_build_modes() -> Result<()> {
    let (full_database, full) = database("v2_locked_roles_full").await?;
    seed(&full).await?;
    run(&full, 103, None).await?;
    let full_snapshot = snapshot(&full).await?;
    assert_eq!(
        child_summary(&full).await?,
        (Some(ROOT.to_owned()), locked(&without("set_resolver")))
    );

    let (incremental_database, incremental) = database("v2_locked_roles_incremental").await?;
    seed(&incremental).await?;
    let mut marker = run(&incremental, 100, None).await?;
    assert_eq!(
        child_summary(&incremental).await?,
        (Some(ROOT.to_owned()), locked(ALL_LOCKED))
    );

    // The root's admin grant lies in a window that touches none of the registration's events.
    marker = run(&incremental, 101, Some(marker)).await?;
    assert_eq!(
        child_summary(&incremental).await?,
        (Some(ROOT.to_owned()), locked(&without("renew")))
    );

    marker = run(&incremental, 102, Some(marker)).await?;
    assert_eq!(
        child_summary(&incremental).await?,
        (Some(ROOT.to_owned()), locked(ALL_LOCKED))
    );

    // The registration's own admin grant is judged against the live (unchanged) root rows.
    run(&incremental, 103, Some(marker)).await?;
    assert_eq!(
        child_summary(&incremental).await?,
        (Some(ROOT.to_owned()), locked(&without("set_resolver")))
    );
    assert_eq!(snapshot(&incremental).await?, full_snapshot);

    for block in [101, 102, 103] {
        redo(&full, 103, block).await?;
        assert_eq!(
            snapshot(&full).await?,
            full_snapshot,
            "redo of block {block}"
        );
    }

    full_database.cleanup().await?;
    incremental_database.cleanup().await?;
    Ok(())
}
