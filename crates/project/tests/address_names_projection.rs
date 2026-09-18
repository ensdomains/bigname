//! Builder-level coverage for the archived-registry masked owner word: an
//! `AuthorityTransferred` whose `after_state` carries `owner_word_unmasked`
//! authenticates no caller, so it must clear the effective controller with the
//! same shape a zero-owner transition produces, and must never publish the
//! masked low-20-byte tail as a controller.

use anyhow::Result;
use bigname_project::{BatchRequest, Engine, RunMode};
use bigname_storage::load_record_inventory_current_with_anchor_fallback;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::json;
use sqlx::{PgPool, raw_sql};

const CHAIN: &str = "ethereum-mainnet";
const MASKED_NAMEHASH: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const CONTROL_NAMEHASH: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const MASKED_LOGICAL: &str =
    "ens:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const CONTROL_LOGICAL: &str =
    "ens:0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const MASKED_RESOURCE: &str = "11111111-1111-1111-1111-111111111111";
const CONTROL_RESOURCE: &str = "22222222-2222-2222-2222-222222222222";
const MASKED_BINDING: &str = "33333333-3333-3333-3333-333333333333";
const CONTROL_BINDING: &str = "44444444-4444-4444-4444-444444444444";
const PRIOR_CONTROLLER: &str = "0x11111111111111111111111111111111111111Aa";
const CONTROL_OWNER: &str = "0x22222222222222222222222222222222222222Bb";
const OWNERLESS_NAMEHASH: &str =
    "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
const OWNERLESS_LOGICAL: &str =
    "ens:0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
const OWNERLESS_PARENT_HASH: &str =
    "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
const OWNERLESS_PARENT_LOGICAL: &str =
    "ens:0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
const OWNERLESS_RESOURCE: &str = "dddddddd-dddd-dddd-dddd-dddddddddddd";
const OWNERLESS_BINDING: &str = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee";
const REGISTRY_ADDRESS: &str = "0x9999999999999999999999999999999999999999";
const RESOLVER_ADDRESS: &str = "0x8888888888888888888888888888888888888888";
// Low-20-byte tail of the archived registry's dirty NewOwner log on mainnet.
const MASKED_TAIL: &str = "0x3831343865616130313363333864316330663339";
const MASKED_RAW: &str = "0x6330363834636235336331363831343865616130313363333864316330663339";

fn block_hash(number: i64) -> String {
    format!("0x{number:064x}")
}

async fn migrated_pool() -> Result<(TestDatabase, PgPool)> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new("address_names_projection")).await?;
    let pool = database.pool().clone();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    let mut transaction = pool.begin().await?;
    sqlx::query("CREATE SCHEMA bigname_phase")
        .execute(&mut *transaction)
        .await?;
    raw_sql(&format!(
        "ALTER DATABASE {} SET search_path TO bigname_phase, public",
        quote_identifier(&database_name)
    ))
    .execute(&mut *transaction)
    .await?;
    sqlx::query("SET LOCAL search_path TO bigname_phase, public")
        .execute(&mut *transaction)
        .await?;
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
        raw_sql(script).execute(&mut *transaction).await?;
    }
    transaction.commit().await?;

    pool.set_connect_options(
        pool.connect_options()
            .as_ref()
            .clone()
            .options([("search_path", "bigname_phase,public")]),
    );
    let mut connections = Vec::new();
    for _ in 0..pool.options().get_max_connections() {
        connections.push(pool.acquire().await?);
    }
    for connection in &mut connections {
        sqlx::query("SET search_path TO bigname_phase, public")
            .execute(&mut **connection)
            .await?;
    }
    Ok((database, pool))
}

async fn run_project(
    pool: &PgPool,
    target_block: i64,
    affected_from_block: i64,
    resume_number: Option<i64>,
) -> Result<()> {
    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block,
            affected_from_block,
            affected_to_block: target_block,
            resume_current: resume_number.map(|number| bigname_project::Marker {
                number,
                hash: block_hash(number),
            }),
            mode: RunMode::Normal,
        })
        .await?;
    Ok(())
}

async fn run_project_redo(pool: &PgPool, target_block: i64, affected_block: i64) -> Result<()> {
    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block,
            affected_from_block: affected_block,
            affected_to_block: affected_block,
            resume_current: None,
            mode: RunMode::Redo,
        })
        .await?;
    Ok(())
}

fn quote_identifier(identifier: &str) -> String {
    format!(r#""{}""#, identifier.replace('"', r#""""#))
}

async fn seed_blocks(pool: &PgPool, numbers: impl IntoIterator<Item = i64>) -> Result<()> {
    for number in numbers {
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, block_number, block_timestamp, canonicality_state
             ) VALUES ($1, $2, $3, $4::timestamptz, 'canonical')",
        )
        .bind(CHAIN)
        .bind(block_hash(number))
        .bind(number)
        .bind(format!("2026-08-01T00:00:{number:02}Z"))
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn seed_chain(pool: &PgPool) -> Result<()> {
    seed_blocks(pool, [8, 9, 10]).await
}

async fn seed_surface(
    pool: &PgPool,
    namehash: &str,
    raw_name: &str,
    resource: &str,
    binding: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
             namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             $1, 'ens', $2, $3, '\\x00', $4, $5, 'test', 'active',
             $6, $7, 8, 'canonical'
         )",
    )
    .bind(format!("ens:{namehash}"))
    .bind(raw_name)
    .bind(vec![
        raw_name.strip_suffix(".eth").unwrap_or(raw_name),
        "eth",
    ])
    .bind(namehash)
    .bind(vec![
        format!("0x{:064x}", 1_u64),
        format!("0x{:064x}", 2_u64),
    ])
    .bind(CHAIN)
    .bind(block_hash(8))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1::uuid, $2, $3, 8, 'canonical')",
    )
    .bind(resource)
    .bind(CHAIN)
    .bind(block_hash(8))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind,
             authority_arm, active_from, chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             $1::uuid, $2, $3::uuid, 'declared_registry_path', 'ens_v1',
             '2026-07-01T00:00:00Z', $4, $5, 8, 'canonical'
         )",
    )
    .bind(binding)
    .bind(format!("ens:{namehash}"))
    .bind(resource)
    .bind(CHAIN)
    .bind(block_hash(8))
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_authority_transferred(
    pool: &PgPool,
    identity: &str,
    namehash: &str,
    resource: &str,
    block_number: i64,
    log_index: i64,
    after_state: serde_json::Value,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, resource_id, event_kind,
             source_family, manifest_version, chain_id, block_number, block_hash,
             transaction_hash, transaction_index, log_index, derivation_kind,
             canonicality_state, after_state
         ) VALUES (
             $1, 'ens', $2, $3::uuid, 'AuthorityTransferred',
             'ens_v1_registry_l1', 1, $4, $5, $6,
             $7, 0, $8, 'ens_v1_unwrapped_authority',
             'canonical', $9
         )",
    )
    .bind(identity)
    .bind(format!("ens:{namehash}"))
    .bind(resource)
    .bind(CHAIN)
    .bind(block_number)
    .bind(block_hash(block_number))
    .bind(format!("0x{:064x}", 900 + log_index))
    .bind(log_index)
    .bind(after_state)
    .execute(pool)
    .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn seed_normalized_event(
    pool: &PgPool,
    identity: &str,
    logical_name_id: Option<&str>,
    resource: Option<&str>,
    event_kind: &str,
    source_family: &str,
    block_number: i64,
    log_index: i64,
    after_state: serde_json::Value,
    raw_fact_ref: serde_json::Value,
) -> Result<()> {
    seed_namespaced_normalized_event(
        pool,
        "ens",
        identity,
        logical_name_id,
        resource,
        event_kind,
        source_family,
        block_number,
        log_index,
        after_state,
        raw_fact_ref,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn seed_namespaced_normalized_event(
    pool: &PgPool,
    namespace: &str,
    identity: &str,
    logical_name_id: Option<&str>,
    resource: Option<&str>,
    event_kind: &str,
    source_family: &str,
    block_number: i64,
    log_index: i64,
    after_state: serde_json::Value,
    raw_fact_ref: serde_json::Value,
) -> Result<()> {
    let derivation_kind = if event_kind == "AccountPermissionChanged" {
        "standard_approval"
    } else {
        "ens_v1_unwrapped_authority"
    };
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, resource_id, event_kind,
             source_family, manifest_version, chain_id, block_number, block_hash,
             transaction_hash, transaction_index, log_index, derivation_kind,
             canonicality_state, after_state, raw_fact_ref
         ) VALUES (
             $1, $2, $3, $4::uuid, $5, $6, 1, $7, $8, $9,
             $10, 0, $11, $14, 'canonical', $12, $13
         )",
    )
    .bind(identity)
    .bind(namespace)
    .bind(logical_name_id)
    .bind(resource)
    .bind(event_kind)
    .bind(source_family)
    .bind(CHAIN)
    .bind(block_number)
    .bind(block_hash(block_number))
    .bind(format!("0x{:064x}", 1_000 + log_index))
    .bind(log_index)
    .bind(after_state)
    .bind(raw_fact_ref)
    .bind(derivation_kind)
    .execute(pool)
    .await?;
    Ok(())
}

async fn serving_projection_snapshot(pool: &PgPool) -> Result<Vec<(String, serde_json::Value)>> {
    let tables = [
        ("name_current", "logical_name_id"),
        (
            "children_current",
            "parent_logical_name_id, child_logical_name_id, surface_class",
        ),
        ("permissions_current", "resource_id, subject, scope"),
        (
            "account_permission_state_current",
            "chain_id, authority_kind, authority_contract, owner, subject, relation_kind",
        ),
        ("permissions_current_resource_summary", "resource_id"),
        (
            "record_inventory_current",
            "resource_id, record_version_boundary_key",
        ),
        ("resolver_current", "chain_id, resolver_address"),
        (
            "address_names_current",
            "address, logical_name_id, relation",
        ),
        ("primary_names_current", "address, coin_type, namespace"),
    ];
    let mut snapshot = Vec::with_capacity(tables.len());
    for (table, order) in tables {
        let statement = format!(
            "SELECT COALESCE(jsonb_agg(
                 to_jsonb(row) - 'last_recomputed_at' - 'inserted_at'
                 ORDER BY {order}
             ), '[]'::jsonb)
             FROM {table} row"
        );
        snapshot.push((
            table.to_owned(),
            sqlx::query_scalar(&statement).fetch_one(pool).await?,
        ));
    }
    Ok(snapshot)
}

#[tokio::test]
async fn registry_operator_state_and_binding_converge_after_revocation() -> Result<()> {
    const NAMEHASH: &str = "0x6050000000000000000000000000000000000000000000000000000000000001";
    const RESOURCE: &str = "00000000-0000-0000-0000-000000000605";
    const BINDING: &str = "00000000-0000-0000-0000-000000000606";
    const OWNER: &str = "0x0000000000000000000000000000000000000a11";
    const OPERATOR: &str = "0x0000000000000000000000000000000000000b22";
    const REGISTRY: &str = "0x0000000000000000000000000000000000000c33";
    let (database, pool) = migrated_pool().await?;
    seed_chain(&pool).await?;
    seed_surface(&pool, NAMEHASH, "operator.eth", RESOURCE, BINDING).await?;
    seed_normalized_event(
        &pool,
        "fixture:registry-owner",
        Some(&format!("ens:{NAMEHASH}")),
        Some(RESOURCE),
        "AuthorityTransferred",
        "ens_v1_registry_l1",
        8,
        1,
        json!({
            "owner": OWNER,
            "owner_getter": OWNER,
            "authority_kind": "registry_only"
        }),
        json!({"emitting_address": REGISTRY}),
    )
    .await?;
    for (identity, block, approved) in [
        ("fixture:operator-grant", 9, true),
        ("fixture:operator-revoke", 10, false),
    ] {
        seed_normalized_event(
            &pool,
            identity,
            None,
            None,
            "AccountPermissionChanged",
            "ens_v1_registry_l1",
            block,
            1,
            json!({
                "subject": OPERATOR,
                "relation_kind": "operator",
                "approved": approved,
                "scope": {
                    "authority_kind": "registry",
                    "authority_contract": REGISTRY,
                    "authority_contract_instance_id":
                        "00000000-0000-0000-0000-000000000607",
                    "owner": OWNER
                },
                "effective_powers": if approved { json!(["registry_control"]) } else { json!([]) },
                "grant_source": if approved { json!({"kind":"raw_log"}) } else { json!({}) },
                "revocation_source": if approved { serde_json::Value::Null } else { json!({"kind":"raw_log"}) },
                "inheritance_path": [],
                "transfer_behavior": {"mode":"owner_scoped"}
            }),
            json!({"emitting_address": REGISTRY}),
        )
        .await?;
        if approved {
            run_project(&pool, 9, 8, None).await?;
            let active: (bool, String, String, String) = sqlx::query_as(
                "SELECT account.approved, summary.registry_owner,
                        summary.registry_contract, summary.unsupported_reason
                 FROM account_permission_state_current account
                 JOIN permissions_current_resource_summary summary
                   ON summary.resource_id = $1::uuid",
            )
            .bind(RESOURCE)
            .fetch_one(&pool)
            .await?;
            assert!(active.0);
            assert_eq!(active.1, OWNER);
            assert_eq!(active.2, REGISTRY);
            assert_eq!(active.3, "operator_approval_surfaces_not_ingested");
        }
    }
    run_project(&pool, 10, 10, Some(9)).await?;
    let approved: bool = sqlx::query_scalar(
        "SELECT approved FROM account_permission_state_current WHERE subject = $1",
    )
    .bind(OPERATOR)
    .fetch_one(&pool)
    .await?;
    assert!(!approved, "the latest revocation must remain projected");
    const CURRENT_STATE: &str = "SELECT jsonb_build_object('account', to_jsonb(account) - 'last_recomputed_at' - 'inserted_at', 'registry_owner', summary.registry_owner, 'registry_contract', summary.registry_contract, 'unsupported_reason', summary.unsupported_reason) FROM account_permission_state_current account JOIN permissions_current_resource_summary summary ON summary.resource_id = $1::uuid";
    let incremental: serde_json::Value = sqlx::query_scalar(CURRENT_STATE)
        .bind(RESOURCE)
        .fetch_one(&pool)
        .await?;
    run_project(&pool, 10, 8, None).await?;
    let rebuilt: serde_json::Value = sqlx::query_scalar(CURRENT_STATE)
        .bind(RESOURCE)
        .fetch_one(&pool)
        .await?;
    assert_eq!(incremental, rebuilt);
    database.cleanup().await?;
    Ok(())
}
#[tokio::test]
async fn registry_operator_reorg_restores_losing_grant_and_revoke() -> Result<()> {
    const OWNER: &str = "0x0000000000000000000000000000000000000a51";
    const GRANT_LOSER: &str = "0x0000000000000000000000000000000000000b51";
    const REVOKE_LOSER: &str = "0x0000000000000000000000000000000000000b52";
    const REGISTRY: &str = "0x0000000000000000000000000000000000000c51";
    let (database, pool) = migrated_pool().await?;
    seed_chain(&pool).await?;
    for (identity, subject, block, approved) in [
        ("fixture:surviving-revoke", GRANT_LOSER, 8, false),
        ("fixture:losing-grant", GRANT_LOSER, 9, true),
        ("fixture:surviving-grant", REVOKE_LOSER, 8, true),
        ("fixture:losing-revoke", REVOKE_LOSER, 9, false),
    ] {
        seed_normalized_event(
            &pool,
            identity,
            None,
            None,
            "AccountPermissionChanged",
            "ens_v1_registry_l1",
            block,
            if subject == GRANT_LOSER { 1 } else { 2 },
            json!({
                "subject": subject,
                "relation_kind": "operator",
                "approved": approved,
                "scope": {
                    "authority_kind": "registry",
                    "authority_contract": REGISTRY,
                    "authority_contract_instance_id":
                        "00000000-0000-0000-0000-000000000657",
                    "owner": OWNER
                },
                "effective_powers": if approved { json!(["registry_control"]) } else { json!([]) },
                "grant_source": if approved { json!({"kind":"raw_log"}) } else { json!({}) },
                "revocation_source": if approved { serde_json::Value::Null } else { json!({"kind":"raw_log"}) },
                "inheritance_path": [],
                "transfer_behavior": {"mode":"owner_scoped"}
            }),
            json!({"emitting_address": REGISTRY}),
        )
        .await?;
    }
    run_project(&pool, 9, 8, None).await?;
    let before: Vec<(String, bool)> = sqlx::query_as(
        "SELECT subject, approved FROM account_permission_state_current ORDER BY subject",
    )
    .fetch_all(&pool)
    .await?;
    assert_eq!(
        before,
        vec![
            (GRANT_LOSER.to_owned(), true),
            (REVOKE_LOSER.to_owned(), false)
        ]
    );
    sqlx::query(
        "UPDATE normalized_events SET canonicality_state = 'orphaned'
         WHERE event_identity IN ('fixture:losing-grant', 'fixture:losing-revoke')",
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        "UPDATE chain_lineage SET canonicality_state = 'orphaned'
         WHERE chain_id = $1 AND block_number = 9",
    )
    .bind(CHAIN)
    .execute(&pool)
    .await?;
    run_project_redo(&pool, 10, 10).await?;
    let restored: Vec<(String, bool)> = sqlx::query_as(
        "SELECT subject, approved FROM account_permission_state_current ORDER BY subject",
    )
    .fetch_all(&pool)
    .await?;
    assert_eq!(
        restored,
        vec![
            (GRANT_LOSER.to_owned(), false),
            (REVOKE_LOSER.to_owned(), true)
        ]
    );
    database.cleanup().await?;
    Ok(())
}
#[tokio::test]
async fn registry_binding_tracks_generation_rotation_release_and_zero_clear() -> Result<()> {
    const RESOURCE: &str = "00000000-0000-0000-0000-000000000615";
    const RESTORED_RESOURCE: &str = "00000000-0000-0000-0000-000000000616";
    const OWNER: &str = "0x0000000000000000000000000000000000000a11";
    const ZERO: &str = "0x0000000000000000000000000000000000000000";
    const OLD_REGISTRY: &str = "0x0000000000000000000000000000000000000c31";
    const CURRENT_REGISTRY: &str = "0x0000000000000000000000000000000000000c32";
    let (database, pool) = migrated_pool().await?;
    seed_chain(&pool).await?;
    seed_blocks(&pool, [11]).await?;
    seed_surface(&pool, CONTROL_NAMEHASH, "x.eth", RESOURCE, CONTROL_BINDING).await?;
    sqlx::query(
        "INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1::uuid, $2, $3, 8, 'canonical')",
    )
    .bind(RESTORED_RESOURCE)
    .bind(CHAIN)
    .bind(block_hash(8))
    .execute(&pool)
    .await?;
    for (identity, logical_name_id, kind, block, registry) in [
        ("old", None, "AuthorityTransferred", 8, OLD_REGISTRY),
        (
            "current",
            Some(CONTROL_LOGICAL),
            "SubregistryChanged",
            9,
            CURRENT_REGISTRY,
        ),
    ] {
        seed_normalized_event(
            &pool,
            identity,
            logical_name_id,
            Some(RESOURCE),
            kind,
            "ens_v1_registry_l1",
            block,
            1,
            json!({"owner": OWNER, "owner_getter": OWNER, "authority_kind": "registrar", "source_event": "NewOwner"}),
            json!({"emitting_address": registry}),
        )
        .await?;
    }
    run_project(&pool, 9, 8, None).await?;
    let binding: (Option<String>, Option<String>) = sqlx::query_as(
        "SELECT registry_contract, authority_kind FROM permissions_current_resource_summary WHERE resource_id = $1::uuid",
    )
    .bind(RESOURCE)
    .fetch_one(&pool)
    .await?;
    assert_eq!(binding.0.as_deref(), Some(CURRENT_REGISTRY));
    assert_eq!(binding.1.as_deref(), Some("registrar"));
    seed_normalized_event(
        &pool,
        "fixture:current-registry-clear",
        None,
        Some(RESOURCE),
        "SubregistryChanged",
        "ens_v1_registry_l1",
        10,
        1,
        json!({"owner": ZERO, "owner_getter": ZERO, "authority_kind": "registrar", "source_event": "NewOwner"}),
        json!({"emitting_address": CURRENT_REGISTRY}),
    )
    .await?;
    run_project(&pool, 10, 10, Some(9)).await?;
    let cleared: (Option<String>, Option<String>) = sqlx::query_as(
        "SELECT registry_owner, registry_contract FROM permissions_current_resource_summary WHERE resource_id = $1::uuid",
    )
    .bind(RESOURCE)
    .fetch_one(&pool)
    .await?;
    assert_eq!(cleared, (None, None));
    sqlx::query(
        "DELETE FROM normalized_events
         WHERE event_identity = 'fixture:current-registry-clear'",
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        "UPDATE chain_lineage SET canonicality_state = 'orphaned'
         WHERE chain_id = $1 AND block_number = 10",
    )
    .bind(CHAIN)
    .execute(&pool)
    .await?;
    run_project_redo(&pool, 11, 10).await?;
    let restored: (Option<String>, Option<String>) = sqlx::query_as(
        "SELECT registry_owner, registry_contract
         FROM permissions_current_resource_summary WHERE resource_id = $1::uuid",
    )
    .bind(RESOURCE)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        restored,
        (Some(OWNER.to_owned()), Some(CURRENT_REGISTRY.to_owned()))
    );

    const RESTORED_REGISTRY: &str = "0x0000000000000000000000000000000000000c34";
    const LATER_REGISTRY: &str = "0x0000000000000000000000000000000000000c35";
    seed_normalized_event(
        &pool,
        "fixture:same-block-log",
        None,
        Some(RESTORED_RESOURCE),
        "SubregistryChanged",
        "ens_v1_registry_l1",
        11,
        2,
        json!({"source_event":"NewOwner", "owner_getter":OWNER}),
        json!({"kind":"raw_log", "emitting_address":LATER_REGISTRY}),
    )
    .await?;
    for (identity, resource, kind, family, after, raw_fact) in [
        (
            "fixture:detached-registry",
            RESOURCE,
            "SurfaceUnbound",
            "ens_v1_registrar_l1",
            json!({"source_event":"NameRegistered"}),
            json!({"kind":"raw_log", "emitting_address":"0x0000000000000000000000000000000000000c33"}),
        ),
        (
            "fixture:same-block-boundary",
            RESTORED_RESOURCE,
            "SurfaceBound",
            "ens_v1_registry_l1",
            json!({"source_event":"RegistrationReleased", "owner_getter":OWNER,
                "registry_contract":RESTORED_REGISTRY}),
            json!({"kind":"raw_block"}),
        ),
        (
            "fixture:expiry-restored-registry",
            RESTORED_RESOURCE,
            "SurfaceBound",
            "ens_v1_registrar_l1",
            json!({"source_event":"Transfer", "authority_kind":"registry_only",
                "owner_getter":OWNER, "registry_contract":RESTORED_REGISTRY}),
            json!({"kind":"raw_log", "emitting_address":"0x0000000000000000000000000000000000000c33"}),
        ),
    ] {
        seed_normalized_event(
            &pool,
            identity,
            None,
            Some(resource),
            kind,
            family,
            11,
            1,
            after,
            raw_fact,
        )
        .await?;
    }
    sqlx::query(
        "UPDATE normalized_events SET transaction_hash = NULL,
                 transaction_index = NULL, log_index = NULL
                 WHERE event_identity = 'fixture:same-block-boundary'",
    )
    .execute(&pool)
    .await?;
    run_project_redo(&pool, 11, 11).await?;
    let incremental = serving_projection_snapshot(&pool).await?;
    let rotated: (Option<String>, Option<String>) = sqlx::query_as(
        "SELECT detached.registry_contract, restored.registry_contract
         FROM permissions_current_resource_summary detached
         CROSS JOIN permissions_current_resource_summary restored
         WHERE detached.resource_id = $1::uuid AND restored.resource_id = $2::uuid",
    )
    .bind(RESOURCE)
    .bind(RESTORED_RESOURCE)
    .fetch_one(&pool)
    .await?;
    assert_eq!(rotated, (None, Some(LATER_REGISTRY.to_owned())));
    run_project(&pool, 11, 8, None).await?;
    assert_eq!(incremental, serving_projection_snapshot(&pool).await?);
    database.cleanup().await?;
    Ok(())
}
#[tokio::test]
async fn registry_binding_reorg_restores_surviving_observation() -> Result<()> {
    const NAMEHASH: &str = "0x6050000000000000000000000000000000000000000000000000000000000021";
    const RESOURCE: &str = "00000000-0000-0000-0000-000000000625";
    const BINDING: &str = "00000000-0000-0000-0000-000000000626";
    const OWNER: &str = "0x0000000000000000000000000000000000000a11";
    const SURVIVING: &str = "0x0000000000000000000000000000000000000c41";
    const LOSING: &str = "0x0000000000000000000000000000000000000c42";
    let (database, pool) = migrated_pool().await?;
    seed_chain(&pool).await?;
    seed_surface(&pool, NAMEHASH, "reorg.eth", RESOURCE, BINDING).await?;
    for (identity, block, registry) in [
        ("fixture:surviving-binding", 8, SURVIVING),
        ("fixture:losing-binding", 9, LOSING),
    ] {
        seed_normalized_event(
            &pool,
            identity,
            Some(&format!("ens:{NAMEHASH}")),
            Some(RESOURCE),
            "AuthorityTransferred",
            "ens_v1_registry_l1",
            block,
            1,
            json!({"owner": OWNER, "owner_getter": OWNER, "authority_kind": "registry_only"}),
            json!({"emitting_address": registry}),
        )
        .await?;
    }
    let losing_id: i64 = sqlx::query_scalar(
        "SELECT normalized_event_id FROM normalized_events
         WHERE event_identity = 'fixture:losing-binding'",
    )
    .fetch_one(&pool)
    .await?;
    sqlx::query(
        "INSERT INTO permissions_current_resource_summary (
             resource_id, authority_kind, registry_owner, registry_contract,
             registry_binding_provenance, registry_binding_chain_positions,
             support_status, unsupported_reason, provenance, chain_positions,
             canonicality_summary, manifest_version
         ) VALUES (
             $1::uuid, 'registry_only', $2, $3,
             jsonb_build_object('normalized_event_ids', jsonb_build_array($4::bigint),
                                'chain_id', $5::text),
             jsonb_build_object('block_number', 9), 'unsupported',
             'resolver_approval_and_delegation_surfaces_not_interpreted',
             jsonb_build_object('chain_id', $5::text),
             jsonb_build_object('block_number', 8), '{}'::jsonb, 1
         )",
    )
    .bind(RESOURCE)
    .bind(OWNER)
    .bind(LOSING)
    .bind(losing_id)
    .bind(CHAIN)
    .execute(&pool)
    .await?;
    sqlx::query("UPDATE normalized_events SET canonicality_state = 'orphaned' WHERE event_identity = 'fixture:losing-binding'")
        .execute(&pool)
        .await?;
    sqlx::query("UPDATE chain_lineage SET canonicality_state = 'orphaned' WHERE chain_id = $1 AND block_number = 9")
        .bind(CHAIN)
        .execute(&pool)
        .await?;
    run_project_redo(&pool, 10, 10).await?;
    let contract: Option<String> = sqlx::query_scalar(
        "SELECT registry_contract FROM permissions_current_resource_summary WHERE resource_id = $1::uuid",
    )
    .bind(RESOURCE)
    .fetch_one(&pool)
    .await?;
    assert_eq!(contract.as_deref(), Some(SURVIVING));
    database.cleanup().await?;
    Ok(())
}
async fn ownerless_serving_projection_snapshot(
    pool: &PgPool,
    logical_name_id: &str,
) -> Result<Vec<(String, serde_json::Value)>> {
    let mut snapshot = serving_projection_snapshot(pool).await?;
    for (table, rows) in &mut snapshot {
        let Some(rows) = rows.as_array_mut() else {
            continue;
        };
        rows.retain(|row| match table.as_str() {
            "name_current" => row["logical_name_id"] == logical_name_id,
            "children_current" => row["child_logical_name_id"] == logical_name_id,
            "permissions_current"
            | "permissions_current_resource_summary"
            | "record_inventory_current" => row["resource_id"] == OWNERLESS_RESOURCE,
            "resolver_current" => row["resolver_address"] == RESOLVER_ADDRESS,
            "address_names_current" => {
                row["logical_name_id"] == logical_name_id
                    || row["resource_id"] == OWNERLESS_RESOURCE
            }
            "primary_names_current" => false,
            unexpected => panic!("unexpected serving projection table {unexpected}"),
        });
    }
    Ok(snapshot)
}

#[tokio::test]
async fn masked_owner_word_clears_the_effective_controller() -> Result<()> {
    let (database, pool) = migrated_pool().await?;
    seed_chain(&pool).await?;
    seed_surface(
        &pool,
        MASKED_NAMEHASH,
        "masked-fixture.eth",
        MASKED_RESOURCE,
        MASKED_BINDING,
    )
    .await?;
    seed_surface(
        &pool,
        CONTROL_NAMEHASH,
        "control-fixture.eth",
        CONTROL_RESOURCE,
        CONTROL_BINDING,
    )
    .await?;
    seed_authority_transferred(
        &pool,
        "fixture:clean-prior",
        MASKED_NAMEHASH,
        MASKED_RESOURCE,
        8,
        1,
        json!({
            "node": MASKED_NAMEHASH,
            "owner": PRIOR_CONTROLLER,
            "authority_kind": "registry_only"
        }),
    )
    .await?;
    seed_authority_transferred(
        &pool,
        "fixture:masked",
        MASKED_NAMEHASH,
        MASKED_RESOURCE,
        9,
        2,
        json!({
            "node": MASKED_NAMEHASH,
            "owner": MASKED_TAIL,
            "owner_word_unmasked": true,
            "owner_word_raw": MASKED_RAW
        }),
    )
    .await?;
    seed_authority_transferred(
        &pool,
        "fixture:control",
        CONTROL_NAMEHASH,
        CONTROL_RESOURCE,
        8,
        3,
        json!({
            "node": CONTROL_NAMEHASH,
            "owner": CONTROL_OWNER,
            "authority_kind": "registry_only"
        }),
    )
    .await?;

    run_project(&pool, 10, 8, None).await?;

    // Anti-vacuity: both names staged and projected.
    let staged_names: i64 = sqlx::query_scalar("SELECT count(*) FROM name_current")
        .fetch_one(&pool)
        .await?;
    assert_eq!(staged_names, 2);

    // The masked event clears the prior controller with the zero-owner shape:
    // no relation row remains for the name at all.
    let masked_rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT address, relation FROM address_names_current WHERE logical_name_id = $1",
    )
    .bind(MASKED_LOGICAL)
    .fetch_all(&pool)
    .await?;
    assert_eq!(masked_rows, Vec::<(String, String)>::new());

    // Neither the cleared prior controller nor the masked tail leaks in for it.
    let leaked: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM address_names_current
         WHERE lower(address) IN (lower($1), lower($2))",
    )
    .bind(PRIOR_CONTROLLER)
    .bind(MASKED_TAIL)
    .fetch_one(&pool)
    .await?;
    assert_eq!(leaked, 0);

    // The exact-name control summary clears the masked tail as well.
    let masked_control: serde_json::Value = sqlx::query_scalar(
        "SELECT declared_summary -> 'control' FROM name_current WHERE logical_name_id = $1",
    )
    .bind(MASKED_LOGICAL)
    .fetch_one(&pool)
    .await?;
    assert_eq!(masked_control["registry_owner"], serde_json::Value::Null);
    assert!(masked_control.get("owner").is_none());

    // The marker-less path is unchanged: the control name keeps its controller.
    let control_rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT address, relation FROM address_names_current WHERE logical_name_id = $1",
    )
    .bind(CONTROL_LOGICAL)
    .fetch_all(&pool)
    .await?;
    assert_eq!(
        control_rows,
        vec![(
            CONTROL_OWNER.to_lowercase(),
            "effective_controller".to_owned()
        )]
    );
    let control_summary: serde_json::Value = sqlx::query_scalar(
        "SELECT declared_summary -> 'control' FROM name_current WHERE logical_name_id = $1",
    )
    .bind(CONTROL_LOGICAL)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        control_summary["registry_owner"],
        json!(CONTROL_OWNER.to_lowercase())
    );
    assert!(control_summary.get("owner").is_none());

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn pre_surface_zero_owner_projects_as_supported_unregistered() -> Result<()> {
    let (database, pool) = migrated_pool().await?;
    seed_chain(&pool).await?;
    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1::uuid, $2, $3, 8, 'canonical')",
    )
    .bind(OWNERLESS_RESOURCE)
    .bind(CHAIN)
    .bind(block_hash(8))
    .execute(&pool)
    .await?;
    seed_normalized_event(
        &pool,
        "fixture:pre-surface-ownerless",
        None,
        Some(OWNERLESS_RESOURCE),
        "AuthorityTransferred",
        "ens_v1_registry_l1",
        8,
        1,
        json!({
            "node": OWNERLESS_NAMEHASH,
            "owner": "0x0000000000000000000000000000000000000000",
            "owner_getter": "0x0000000000000000000000000000000000000000",
            "owner_getter_reason": "literal_zero",
            "authority_kind": null
        }),
        json!({"emitting_address": REGISTRY_ADDRESS}),
    )
    .await?;
    run_project(&pool, 8, 8, None).await?;

    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
             namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             $1, 'ens', 'pre-surface-ownerless.eth',
             ARRAY['pre-surface-ownerless', 'eth'], '\\x00', $2,
             ARRAY[
                 '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                 '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
             ], 'test', 'active', $3, $4, 9, 'canonical'
         )",
    )
    .bind(OWNERLESS_LOGICAL)
    .bind(OWNERLESS_NAMEHASH)
    .bind(CHAIN)
    .bind(block_hash(9))
    .execute(&pool)
    .await?;

    run_project(&pool, 9, 9, Some(8)).await?;

    let owner_event_name: Option<String> = sqlx::query_scalar(
        "SELECT logical_name_id
         FROM normalized_events
         WHERE event_identity = 'fixture:pre-surface-ownerless'",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(owner_event_name, None, "fixture must remain pre-surface");
    let (support_status, unsupported_reason, registration_status, has_control): (
        String,
        Option<String>,
        Option<String>,
        bool,
    ) = sqlx::query_as(
        "SELECT support_status, unsupported_reason,
                declared_summary #>> '{registration,status}',
                resource_id IS NOT NULL OR surface_binding_id IS NOT NULL
         FROM name_current
         WHERE logical_name_id = $1",
    )
    .bind(OWNERLESS_LOGICAL)
    .fetch_one(&pool)
    .await?;
    assert_eq!(support_status, "supported");
    assert_eq!(unsupported_reason, None);
    assert_eq!(registration_status.as_deref(), Some("unregistered"));
    assert!(!has_control);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn pre_surface_owner_order_matches_full_rebuild_after_surface_activation() -> Result<()> {
    const LATER_OWNER: &str = "0x7777777777777777777777777777777777777777";
    let (database, pool) = migrated_pool().await?;
    seed_chain(&pool).await?;
    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1::uuid, $2, $3, 8, 'canonical')",
    )
    .bind(OWNERLESS_RESOURCE)
    .bind(CHAIN)
    .bind(block_hash(8))
    .execute(&pool)
    .await?;
    seed_normalized_event(
        &pool,
        "fixture:pre-surface-zero-owner",
        None,
        Some(OWNERLESS_RESOURCE),
        "AuthorityTransferred",
        "ens_v1_registry_l1",
        8,
        1,
        json!({
            "node": OWNERLESS_NAMEHASH,
            "owner": "0x0000000000000000000000000000000000000000",
            "owner_getter": "0x0000000000000000000000000000000000000000",
            "owner_getter_reason": "literal_zero",
            "authority_kind": null
        }),
        json!({"emitting_address": REGISTRY_ADDRESS}),
    )
    .await?;
    seed_normalized_event(
        &pool,
        "fixture:pre-surface-later-owner",
        None,
        Some(OWNERLESS_RESOURCE),
        "AuthorityTransferred",
        "ens_v1_registry_l1",
        8,
        2,
        json!({
            "node": OWNERLESS_NAMEHASH,
            "owner": LATER_OWNER,
            "owner_getter": LATER_OWNER,
            "authority_kind": "registry_only"
        }),
        json!({"emitting_address": REGISTRY_ADDRESS}),
    )
    .await?;
    run_project(&pool, 8, 8, None).await?;

    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
             namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             $1, 'ens', 'pre-surface-owner-order.eth',
             ARRAY['pre-surface-owner-order', 'eth'], '\\x00', $2,
             ARRAY[
                 '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                 '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
             ], 'test', 'active', $3, $4, 9, 'canonical'
         )",
    )
    .bind(OWNERLESS_LOGICAL)
    .bind(OWNERLESS_NAMEHASH)
    .bind(CHAIN)
    .bind(block_hash(9))
    .execute(&pool)
    .await?;

    run_project(&pool, 9, 9, Some(8)).await?;
    let incremental: (String, Option<String>) = sqlx::query_as(
        "SELECT support_status, unsupported_reason
         FROM name_current
         WHERE logical_name_id = $1",
    )
    .bind(OWNERLESS_LOGICAL)
    .fetch_one(&pool)
    .await?;

    run_project(&pool, 9, 8, None).await?;
    let full: (String, Option<String>) = sqlx::query_as(
        "SELECT support_status, unsupported_reason
         FROM name_current
         WHERE logical_name_id = $1",
    )
    .bind(OWNERLESS_LOGICAL)
    .fetch_one(&pool)
    .await?;
    assert_eq!(incremental, full);
    assert_eq!(full.0, "unsupported");
    assert_eq!(full.1.as_deref(), Some("current_authority_not_projected"));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn registry_self_with_linked_resolver_serves_without_control() -> Result<()> {
    let (database, pool) = migrated_pool().await?;
    seed_chain(&pool).await?;
    seed_surface(
        &pool,
        OWNERLESS_NAMEHASH,
        "ownerless-fixture.eth",
        OWNERLESS_RESOURCE,
        OWNERLESS_BINDING,
    )
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
             namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             $1, 'ens', 'eth', ARRAY['eth'], '\\x00', $2, ARRAY[$2],
             'test', 'active', $3, $4, 8, 'canonical'
         )",
    )
    .bind(OWNERLESS_PARENT_LOGICAL)
    .bind(OWNERLESS_PARENT_HASH)
    .bind(CHAIN)
    .bind(block_hash(8))
    .execute(&pool)
    .await?;
    sqlx::query(
        "UPDATE surface_bindings
         SET active_to = '2026-08-01T00:00:09Z'
         WHERE surface_binding_id = $1::uuid",
    )
    .bind(OWNERLESS_BINDING)
    .execute(&pool)
    .await?;
    seed_normalized_event(
        &pool,
        "fixture:ownerless-resolver",
        Some(OWNERLESS_LOGICAL),
        Some(OWNERLESS_RESOURCE),
        "ResolverChanged",
        "ens_v1_registry_l1",
        8,
        1,
        json!({"node": OWNERLESS_NAMEHASH, "resolver": RESOLVER_ADDRESS}),
        json!({"emitting_address": REGISTRY_ADDRESS}),
    )
    .await?;
    seed_normalized_event(
        &pool,
        "fixture:ownerless-child",
        Some(OWNERLESS_LOGICAL),
        Some(OWNERLESS_RESOURCE),
        "SubregistryChanged",
        "ens_v1_registry_l1",
        9,
        2,
        json!({
            "node": OWNERLESS_PARENT_HASH,
            "child_node": OWNERLESS_NAMEHASH,
            "labelhash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "owner": CONTROL_OWNER,
            "owner_getter": CONTROL_OWNER
        }),
        json!({"emitting_address": REGISTRY_ADDRESS}),
    )
    .await?;
    seed_normalized_event(
        &pool,
        "fixture:ownerless-record",
        Some(OWNERLESS_LOGICAL),
        None,
        "RecordChanged",
        "ens_v1_resolver_l1",
        8,
        2,
        json!({
            "node": OWNERLESS_NAMEHASH,
            "record_family": "text",
            "record_key": "text:description",
            "selector_key": "description",
            "value": "still readable"
        }),
        json!({"emitting_address": RESOLVER_ADDRESS}),
    )
    .await?;
    seed_normalized_event(
        &pool,
        "fixture:ownerless-self",
        None,
        Some(OWNERLESS_RESOURCE),
        "AuthorityTransferred",
        "ens_v1_registry_l1",
        9,
        1,
        json!({
            "node": OWNERLESS_NAMEHASH,
            "owner": REGISTRY_ADDRESS,
            "owner_getter": "0x0000000000000000000000000000000000000000",
            "owner_getter_reason": "registry_self",
            "authority_kind": null
        }),
        json!({"emitting_address": REGISTRY_ADDRESS}),
    )
    .await?;
    run_project(&pool, 9, 8, None).await?;
    let initial_value: String = sqlx::query_scalar(
        "SELECT entries -> 0 ->> 'value'
         FROM record_inventory_current
         WHERE resource_id = $1::uuid",
    )
    .bind(OWNERLESS_RESOURCE)
    .fetch_one(&pool)
    .await?;
    assert_eq!(initial_value, "still readable");

    seed_normalized_event(
        &pool,
        "fixture:ownerless-version",
        None,
        None,
        "RecordVersionChanged",
        "ens_v1_resolver_l1",
        10,
        1,
        json!({"node": OWNERLESS_NAMEHASH, "record_version": "1"}),
        json!({"emitting_address": RESOLVER_ADDRESS}),
    )
    .await?;
    seed_normalized_event(
        &pool,
        "fixture:ownerless-record-after-version",
        None,
        None,
        "RecordChanged",
        "ens_v1_resolver_l1",
        10,
        2,
        json!({
            "node": OWNERLESS_NAMEHASH,
            "record_family": "text",
            "record_key": "text:description",
            "selector_key": "description",
            "value": "readable after version"
        }),
        json!({"emitting_address": RESOLVER_ADDRESS}),
    )
    .await?;
    run_project(&pool, 10, 10, Some(9)).await?;

    let row_matches_contract: bool = sqlx::query_scalar(
        "SELECT surface_binding_id IS NULL
             AND resource_id IS NULL
             AND binding_kind IS NULL
             AND serving_resource_id = $2::uuid
             AND support_status = 'supported'
             AND unsupported_reason IS NULL
             AND jsonb_typeof(declared_summary -> 'topology') = 'object'
             AND declared_summary @> $3::jsonb
             AND provenance @> $4::jsonb
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(OWNERLESS_LOGICAL)
    .bind(OWNERLESS_RESOURCE)
    .bind(json!({
        "registration":{"status":"unregistered"},
        "control":{"status":"unregistered"},
        "resolver":{"address":RESOLVER_ADDRESS},
        "coverage":{
            "status":"projected", "exhaustiveness":"not_asserted",
            "enumeration_basis":"event_linked_registry_resolver"
        }
    }))
    .bind(json!({"read_reachability":{
        "basis":"retained_registry_resolver_pointer",
        "owner_getter_reason":"registry_self"
    }}))
    .fetch_one(&pool)
    .await?;
    assert!(row_matches_contract);

    let (inventory_boundary, topology_boundary, inventory): (
        serde_json::Value,
        serde_json::Value,
        serde_json::Value,
    ) = sqlx::query_as(
        "SELECT inventory.record_version_boundary,
                name.declared_summary #> '{topology,version_boundaries,record_version_boundary}',
                inventory.entries
         FROM record_inventory_current inventory
         JOIN name_current name
           ON name.serving_resource_id = inventory.resource_id
         WHERE inventory.resource_id = $1::uuid",
    )
    .bind(OWNERLESS_RESOURCE)
    .fetch_one(&pool)
    .await?;
    assert_eq!(topology_boundary, inventory_boundary);
    assert_eq!(inventory[0]["record_key"], "text:description");
    assert_eq!(inventory[0]["value"], "readable after version");
    let loaded = load_record_inventory_current_with_anchor_fallback(
        &pool,
        OWNERLESS_RESOURCE.parse()?,
        &topology_boundary,
    )
    .await?
    .expect("ownerless topology boundary loads its current inventory");
    assert_eq!(loaded.entries[0]["value"], "readable after version");
    let incremental_record_change =
        ownerless_serving_projection_snapshot(&pool, OWNERLESS_LOGICAL).await?;
    run_project(&pool, 10, 8, None).await?;
    assert_eq!(
        incremental_record_change,
        ownerless_serving_projection_snapshot(&pool, OWNERLESS_LOGICAL).await?,
        "resource-less ownerless record changes diverged from a fresh Project rebuild across the eight serving tables"
    );
    let address_relations: i64 =
        sqlx::query_scalar("SELECT count(*) FROM address_names_current WHERE logical_name_id = $1")
            .bind(OWNERLESS_LOGICAL)
            .fetch_one(&pool)
            .await?;
    assert_eq!(address_relations, 0);
    let effective_permissions: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM permissions_current
         WHERE resource_id = $1::uuid AND jsonb_array_length(effective_powers) > 0",
    )
    .bind(OWNERLESS_RESOURCE)
    .fetch_one(&pool)
    .await?;
    assert_eq!(effective_permissions, 0);

    let child: (String, Option<String>) = sqlx::query_as(
        "SELECT owner, registrant FROM children_current
         WHERE parent_logical_name_id = $1 AND child_logical_name_id = $2",
    )
    .bind(OWNERLESS_PARENT_LOGICAL)
    .bind(OWNERLESS_LOGICAL)
    .fetch_one(&pool)
    .await?;
    assert_eq!(child.0, "0x0000000000000000000000000000000000000000");
    assert_eq!(child.1, None);

    seed_blocks(&pool, [11, 12]).await?;
    seed_normalized_event(
        &pool,
        "fixture:ownerless-resolver-clear",
        Some(OWNERLESS_LOGICAL),
        Some(OWNERLESS_RESOURCE),
        "ResolverChanged",
        "ens_v1_registry_l1",
        11,
        1,
        json!({
            "node": OWNERLESS_NAMEHASH,
            "resolver": "0x0000000000000000000000000000000000000000"
        }),
        json!({"emitting_address": REGISTRY_ADDRESS}),
    )
    .await?;
    run_project(&pool, 11, 11, Some(10)).await?;
    let cleared: (Option<String>, Option<String>, i64, i64) = sqlx::query_as(
        "SELECT serving_resource_id::text,
                declared_summary #>> '{resolver,address}',
                (SELECT count(*) FROM children_current
                 WHERE child_logical_name_id = $1),
                -- A cleared pointer may still publish the history-only row that carries
                -- provenance.attributed_event_ids; it serves no records, and that is what this
                -- counts.
                (SELECT count(*) FROM record_inventory_current
                 WHERE resource_id = $2::uuid
                   AND provenance ->> 'record_serving' IS DISTINCT FROM 'false')
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(OWNERLESS_LOGICAL)
    .bind(OWNERLESS_RESOURCE)
    .fetch_one(&pool)
    .await?;
    assert_eq!(cleared, (None, None, 0, 0));

    seed_normalized_event(
        &pool,
        "fixture:ownerless-resolver-reselected",
        Some(OWNERLESS_LOGICAL),
        Some(OWNERLESS_RESOURCE),
        "ResolverChanged",
        "ens_v1_registry_l1",
        12,
        1,
        json!({"node": OWNERLESS_NAMEHASH, "resolver": RESOLVER_ADDRESS}),
        json!({"emitting_address": REGISTRY_ADDRESS}),
    )
    .await?;
    run_project(&pool, 12, 12, Some(11)).await?;
    let restored: (Option<String>, Option<String>, i64, i64) = sqlx::query_as(
        "SELECT serving_resource_id::text,
                declared_summary #>> '{resolver,address}',
                (SELECT count(*) FROM children_current
                 WHERE child_logical_name_id = $1),
                (SELECT count(*) FROM record_inventory_current
                 WHERE resource_id = $2::uuid)
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(OWNERLESS_LOGICAL)
    .bind(OWNERLESS_RESOURCE)
    .fetch_one(&pool)
    .await?;
    assert_eq!(restored.0.as_deref(), Some(OWNERLESS_RESOURCE));
    assert_eq!(restored.1.as_deref(), Some(RESOLVER_ADDRESS));
    assert_eq!((restored.2, restored.3), (1, 1));
    let incremental = serving_projection_snapshot(&pool).await?;
    run_project(&pool, 12, 8, None).await?;
    assert_eq!(
        incremental,
        serving_projection_snapshot(&pool).await?,
        "incremental ownerless clear/reselection diverged from a fresh Project rebuild across the eight serving tables"
    );

    seed_blocks(&pool, [13]).await?;
    for (identity, resolver) in [
        ("fixture:ownerless-resolver-z", RESOLVER_ADDRESS),
        (
            "fixture:ownerless-resolver-a",
            "0x7777777777777777777777777777777777777777",
        ),
    ] {
        seed_normalized_event(
            &pool,
            identity,
            Some(OWNERLESS_LOGICAL),
            Some(OWNERLESS_RESOURCE),
            "ResolverChanged",
            "ens_v1_registry_l1",
            13,
            1,
            json!({"node": OWNERLESS_NAMEHASH, "resolver": resolver}),
            json!({"emitting_address": REGISTRY_ADDRESS}),
        )
        .await?;
    }
    run_project(&pool, 13, 13, Some(12)).await?;
    let selected_resolver: String = sqlx::query_scalar(
        "SELECT declared_summary #>> '{resolver,address}'
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(OWNERLESS_LOGICAL)
    .fetch_one(&pool)
    .await?;
    assert_eq!(selected_resolver, RESOLVER_ADDRESS);

    seed_blocks(&pool, [14, 15]).await?;
    for (identity, owner, getter, reason) in [
        (
            "fixture:ownerless-owner-z",
            REGISTRY_ADDRESS,
            "0x0000000000000000000000000000000000000000",
            Some("registry_self"),
        ),
        (
            "fixture:ownerless-owner-a",
            CONTROL_OWNER,
            CONTROL_OWNER,
            None,
        ),
    ] {
        seed_normalized_event(
            &pool,
            identity,
            Some(OWNERLESS_LOGICAL),
            Some(OWNERLESS_RESOURCE),
            "AuthorityTransferred",
            "ens_v1_registry_l1",
            14,
            1,
            json!({
                "node": OWNERLESS_NAMEHASH, "owner": owner,
                "owner_getter": getter, "owner_getter_reason": reason
            }),
            json!({"emitting_address": REGISTRY_ADDRESS}),
        )
        .await?;
    }
    raw_sql(
        "ALTER TABLE normalized_events ALTER COLUMN normalized_event_id DROP IDENTITY;
         UPDATE normalized_events
         SET normalized_event_id = CASE event_identity
             WHEN 'fixture:ownerless-owner-a'
                 THEN 900002
             ELSE 900001 END
         WHERE event_identity IN (
             'fixture:ownerless-owner-a', 'fixture:ownerless-owner-z'
         );
         ALTER TABLE normalized_events ALTER COLUMN normalized_event_id
             ADD GENERATED ALWAYS AS IDENTITY (START WITH 900003)",
    )
    .execute(&pool)
    .await?;
    run_project(&pool, 14, 14, Some(13)).await?;
    sqlx::query(
        "UPDATE name_current SET provenance = provenance || '{\"scope_poison\":true}'::jsonb
         WHERE logical_name_id = $1",
    )
    .bind(OWNERLESS_PARENT_LOGICAL)
    .execute(&pool)
    .await?;
    seed_normalized_event(
        &pool,
        "fixture:ownerless-resolver-parent-scope",
        Some(OWNERLESS_LOGICAL),
        Some(OWNERLESS_RESOURCE),
        "ResolverChanged",
        "ens_v1_registry_l1",
        15,
        1,
        json!({"node": OWNERLESS_NAMEHASH, "resolver": RESOLVER_ADDRESS}),
        json!({"emitting_address": REGISTRY_ADDRESS}),
    )
    .await?;
    run_project(&pool, 15, 15, Some(14)).await?;
    let parent_rebuilt: bool = sqlx::query_scalar(
        "SELECT NOT provenance ? 'scope_poison' FROM name_current WHERE logical_name_id = $1",
    )
    .bind(OWNERLESS_PARENT_LOGICAL)
    .fetch_one(&pool)
    .await?;
    assert!(
        parent_rebuilt,
        "ownerless resolver change omitted its parent from incremental scope"
    );
    let incremental = serving_projection_snapshot(&pool).await?;
    run_project(&pool, 15, 8, None).await?;
    assert_eq!(incremental, serving_projection_snapshot(&pool).await?);

    database.cleanup().await?;
    Ok(())
}

/// Basenames record writes remain possible after ownership clears because the resolver separately
/// authorizes its registrar controller and reverse registrar.
/// (upstream: .refs/basenames/src/L2/L2Resolver.sol:L193-L199 @ basenames@1809bbc)
#[tokio::test]
async fn basenames_node_only_record_after_owner_clear_rebuilds_inventory() -> Result<()> {
    const LOGICAL_NAME_ID: &str =
        "basenames:0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
    let (database, pool) = migrated_pool().await?;
    seed_chain(&pool).await?;
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
             namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             $1, 'basenames', 'ownerless-fixture.base.eth',
             ARRAY['ownerless-fixture', 'base', 'eth'], '\\x00', $2,
             ARRAY[
                 '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                 '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
                 '0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'
             ], 'test', 'active', $3, $4, 8, 'canonical'
         )",
    )
    .bind(LOGICAL_NAME_ID)
    .bind(OWNERLESS_NAMEHASH)
    .bind(CHAIN)
    .bind(block_hash(8))
    .execute(&pool)
    .await?;
    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1::uuid, $2, $3, 8, 'canonical')",
    )
    .bind(OWNERLESS_RESOURCE)
    .bind(CHAIN)
    .bind(block_hash(8))
    .execute(&pool)
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind,
             authority_arm, active_from, active_to, chain_id, block_hash, block_number,
             canonicality_state
         ) VALUES (
             $1::uuid, $2, $3::uuid, 'declared_registry_path', 'basenames',
             '2026-08-01T00:00:08Z', '2026-08-01T00:00:09Z', $4, $5, 8, 'canonical'
         )",
    )
    .bind(OWNERLESS_BINDING)
    .bind(LOGICAL_NAME_ID)
    .bind(OWNERLESS_RESOURCE)
    .bind(CHAIN)
    .bind(block_hash(8))
    .execute(&pool)
    .await?;
    for (identity, logical_name_id, resource, event_kind, source_family, block, log, state) in [
        (
            "fixture:basenames-ownerless-resolver",
            Some(LOGICAL_NAME_ID),
            Some(OWNERLESS_RESOURCE),
            "ResolverChanged",
            "basenames_base_registry",
            8,
            1,
            json!({"node": OWNERLESS_NAMEHASH, "resolver": RESOLVER_ADDRESS}),
        ),
        (
            "fixture:basenames-ownerless-record",
            Some(LOGICAL_NAME_ID),
            None,
            "RecordChanged",
            "basenames_base_resolver",
            8,
            2,
            json!({
                "node": OWNERLESS_NAMEHASH,
                "record_family": "text",
                "record_key": "text:description",
                "selector_key": "description",
                "value": "still readable"
            }),
        ),
        (
            "fixture:basenames-ownerless-clear",
            None,
            Some(OWNERLESS_RESOURCE),
            "AuthorityTransferred",
            "basenames_base_registry",
            9,
            1,
            json!({
                "node": OWNERLESS_NAMEHASH,
                "owner": "0x0000000000000000000000000000000000000000",
                "owner_getter": "0x0000000000000000000000000000000000000000",
                "owner_getter_reason": "literal_zero",
                "authority_kind": null
            }),
        ),
    ] {
        seed_namespaced_normalized_event(
            &pool,
            "basenames",
            identity,
            logical_name_id,
            resource,
            event_kind,
            source_family,
            block,
            log,
            state,
            json!({"emitting_address": if event_kind == "RecordChanged" {
                RESOLVER_ADDRESS
            } else {
                REGISTRY_ADDRESS
            }}),
        )
        .await?;
    }
    run_project(&pool, 9, 8, None).await?;
    let initial: String = sqlx::query_scalar(
        "SELECT entries -> 0 ->> 'value'
         FROM record_inventory_current WHERE resource_id = $1::uuid",
    )
    .bind(OWNERLESS_RESOURCE)
    .fetch_one(&pool)
    .await?;
    assert_eq!(initial, "still readable");
    let ownerless_serving: (Option<String>, Option<String>, String) = sqlx::query_as(
        "SELECT serving_resource_id::text,
                declared_summary #>> '{registration,status}', support_status
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(LOGICAL_NAME_ID)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        ownerless_serving,
        (
            Some(OWNERLESS_RESOURCE.to_owned()),
            Some("unregistered".to_owned()),
            "supported".to_owned()
        )
    );

    for (identity, event_kind, log, state) in [
        (
            "fixture:basenames-ownerless-version",
            "RecordVersionChanged",
            1,
            json!({"node": OWNERLESS_NAMEHASH, "record_version": "1"}),
        ),
        (
            "fixture:basenames-ownerless-record-after-version",
            "RecordChanged",
            2,
            json!({
                "node": OWNERLESS_NAMEHASH,
                "record_family": "text",
                "record_key": "text:description",
                "selector_key": "description",
                "value": "readable after version"
            }),
        ),
    ] {
        seed_namespaced_normalized_event(
            &pool,
            "basenames",
            identity,
            None,
            None,
            event_kind,
            "basenames_base_resolver",
            10,
            log,
            state,
            json!({"emitting_address": RESOLVER_ADDRESS}),
        )
        .await?;
    }
    run_project(&pool, 10, 10, Some(9)).await?;
    let incremental_value: String = sqlx::query_scalar(
        "SELECT entries -> 0 ->> 'value'
         FROM record_inventory_current WHERE resource_id = $1::uuid",
    )
    .bind(OWNERLESS_RESOURCE)
    .fetch_one(&pool)
    .await?;
    let incremental = ownerless_serving_projection_snapshot(&pool, LOGICAL_NAME_ID).await?;
    run_project(&pool, 10, 8, None).await?;
    let fresh = ownerless_serving_projection_snapshot(&pool, LOGICAL_NAME_ID).await?;
    assert_eq!(
        incremental, fresh,
        "Basenames node-only records diverged between incremental and fresh rebuilds for the published inventory and name topology"
    );
    let fresh_value: String = sqlx::query_scalar(
        "SELECT entries -> 0 ->> 'value'
         FROM record_inventory_current WHERE resource_id = $1::uuid",
    )
    .bind(OWNERLESS_RESOURCE)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        (incremental_value.as_str(), fresh_value.as_str()),
        ("readable after version", "readable after version"),
        "Basenames node-only records must replace 'still readable' with 'readable after version' in incremental and fresh rebuilds"
    );

    database.cleanup().await?;
    Ok(())
}

const DIVERGENT_NAMEHASH: &str =
    "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const DIVERGENT_LOGICAL: &str =
    "ens:0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const REGISTRAR_RESOURCE: &str = "55555555-5555-5555-5555-555555555555";
const REGISTRY_RESOURCE: &str = "66666666-6666-6666-6666-666666666666";
const REGISTRAR_BINDING: &str = "77777777-7777-7777-7777-777777777777";
const REGISTRY_BINDING: &str = "88888888-8888-8888-8888-888888888888";
const DIVERGENT_OWNER: &str = "0x33333333333333333333333333333333333333Cc";

/// Binds a further same-arm resource to an existing name, closing whichever binding is still
/// open so the chain stays non-overlapping.
async fn seed_next_binding(
    pool: &PgPool,
    namehash: &str,
    resource: &str,
    binding: &str,
    block_number: i64,
    active_from: &str,
) -> Result<()> {
    seed_next_arm_binding(
        pool,
        namehash,
        resource,
        binding,
        block_number,
        active_from,
        "ens_v1",
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn seed_next_arm_binding(
    pool: &PgPool,
    namehash: &str,
    resource: &str,
    binding: &str,
    block_number: i64,
    active_from: &str,
    authority_arm: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1::uuid, $2, $3, $4, 'canonical')",
    )
    .bind(resource)
    .bind(CHAIN)
    .bind(block_hash(block_number))
    .bind(block_number)
    .execute(pool)
    .await?;
    sqlx::query(
        "UPDATE surface_bindings SET active_to = $2::timestamptz
         WHERE logical_name_id = $1 AND active_to IS NULL",
    )
    .bind(format!("ens:{namehash}"))
    .bind(active_from)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind,
             authority_arm, active_from, chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             $1::uuid, $2, $3::uuid, 'declared_registry_path', $8,
             $4::timestamptz, $5, $6, $7, 'canonical'
         )",
    )
    .bind(binding)
    .bind(format!("ens:{namehash}"))
    .bind(resource)
    .bind(active_from)
    .bind(CHAIN)
    .bind(block_hash(block_number))
    .bind(block_number)
    .bind(authority_arm)
    .execute(pool)
    .await?;
    Ok(())
}

/// Records a real intra-block position on a binding. Interpret provenances every binding with the
/// transaction and log index of the event that created it; fixtures that leave it NULL cannot tell
/// an inclusive position bound from an exclusive one.
async fn seed_binding_provenance(
    pool: &PgPool,
    binding: &str,
    transaction_index: i64,
    log_index: i64,
) -> Result<()> {
    sqlx::query("UPDATE surface_bindings SET provenance = $2 WHERE surface_binding_id = $1::uuid")
        .bind(binding)
        .bind(json!({
            "transaction_index": transaction_index,
            "log_index": log_index
        }))
        .execute(pool)
        .await?;
    Ok(())
}

/// Binds a second same-arm resource to an existing name, superseding the first.
async fn seed_successor_binding(
    pool: &PgPool,
    namehash: &str,
    resource: &str,
    binding: &str,
    block_number: i64,
) -> Result<()> {
    seed_next_binding(
        pool,
        namehash,
        resource,
        binding,
        block_number,
        "2026-07-02T00:00:00Z",
    )
    .await
}

async fn seed_authority_epoch_changed(
    pool: &PgPool,
    identity: &str,
    namehash: &str,
    resource: &str,
    block_number: i64,
    authority_kind: &str,
) -> Result<()> {
    seed_authority_epoch(
        pool,
        identity,
        namehash,
        resource,
        block_number,
        7,
        json!({"authority_kind": authority_kind}),
    )
    .await
}

/// Seeds an `AuthorityEpochChanged` with the full `after_state` the adapter would carry.
async fn seed_authority_epoch(
    pool: &PgPool,
    identity: &str,
    namehash: &str,
    resource: &str,
    block_number: i64,
    log_index: i64,
    after_state: serde_json::Value,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, resource_id, event_kind,
             source_family, manifest_version, chain_id, block_number, block_hash,
             transaction_hash, transaction_index, log_index, derivation_kind,
             canonicality_state, after_state
         ) VALUES (
             $1, 'ens', $2, $3::uuid, 'AuthorityEpochChanged',
             'ens_v1_registry_l1', 1, $4, $5, $6,
             $7, 0, $9, 'ens_v1_unwrapped_authority',
             'canonical', $8
         )",
    )
    .bind(identity)
    .bind(format!("ens:{namehash}"))
    .bind(resource)
    .bind(CHAIN)
    .bind(block_number)
    .bind(block_hash(block_number))
    .bind(format!("0x{:064x}", 700 + block_number))
    .bind(after_state)
    .bind(log_index)
    .execute(pool)
    .await?;
    Ok(())
}

/// ENSv1 keeps registry ownership on a different resource from the registrar leasehold: the
/// registrar's ERC721 transfer writes no registry state, and after registration only `reclaim`
/// writes the registry owner
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172-L174
/// @ ens_v1@91c966f). So a registry-only binding that superseded the registrar resource must
/// still publish the divergent owner it left behind. This is the preservation direction of the re-scoped controller fold: the
/// same-arm predecessor survives, which is a different question from whether a superseded
/// other-arm event can win.
#[tokio::test]
async fn registry_only_binding_preserves_the_same_arm_divergent_owner() -> Result<()> {
    let (database, pool) = migrated_pool().await?;
    seed_chain(&pool).await?;
    seed_surface(
        &pool,
        DIVERGENT_NAMEHASH,
        "divergent-fixture.eth",
        REGISTRAR_RESOURCE,
        REGISTRAR_BINDING,
    )
    .await?;
    // Registry ownership was set on the registrar resource and never reclaimed.
    seed_authority_transferred(
        &pool,
        "fixture:divergent-owner",
        DIVERGENT_NAMEHASH,
        REGISTRAR_RESOURCE,
        8,
        1,
        json!({
            "node": DIVERGENT_NAMEHASH,
            "owner": DIVERGENT_OWNER,
            "authority_kind": "registry_only"
        }),
    )
    .await?;
    seed_successor_binding(
        &pool,
        DIVERGENT_NAMEHASH,
        REGISTRY_RESOURCE,
        REGISTRY_BINDING,
        9,
    )
    .await?;
    seed_authority_epoch_changed(
        &pool,
        "fixture:divergent-epoch",
        DIVERGENT_NAMEHASH,
        REGISTRY_RESOURCE,
        9,
        "registry_only",
    )
    .await?;

    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block: 10,
            affected_from_block: 8,
            affected_to_block: 10,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;

    // Anti-vacuity: the registry-only resource is the selected one, not the registrar resource.
    let selected: Option<String> =
        sqlx::query_scalar("SELECT resource_id::text FROM name_current WHERE logical_name_id = $1")
            .bind(DIVERGENT_LOGICAL)
            .fetch_one(&pool)
            .await?;
    assert_eq!(selected.as_deref(), Some(REGISTRY_RESOURCE));

    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT address, relation FROM address_names_current WHERE logical_name_id = $1",
    )
    .bind(DIVERGENT_LOGICAL)
    .fetch_all(&pool)
    .await?;
    assert_eq!(
        rows,
        vec![(
            DIVERGENT_OWNER.to_lowercase(),
            "effective_controller".to_owned()
        )],
        "the same-arm divergent registry owner lost its relation"
    );

    database.cleanup().await?;
    Ok(())
}

const HANDOFF_NAMEHASH: &str = "0x1212121212121212121212121212121212121212121212121212121212121212";
const HANDOFF_LOGICAL: &str =
    "ens:0x1212121212121212121212121212121212121212121212121212121212121212";
const HANDOFF_PARENT_HASH: &str =
    "0x93cdeb708b7545dc668eb9280176169d1c33cfd8ed6f04690a0bcc88a93fc4ae";
const HANDOFF_LABELHASH: &str =
    "0x3434343434343434343434343434343434343434343434343434343434343434";
const HANDOFF_TOKEN_LINEAGE: &str = "30000000-0000-0000-0000-000000000099";
const HANDOFF_REGISTRAR_RESOURCE: &str = "30000000-0000-0000-0000-000000000001";
const HANDOFF_REGISTRY_RESOURCE: &str = "30000000-0000-0000-0000-000000000002";
const HANDOFF_REGISTRAR_BINDING: &str = "30000000-0000-0000-0000-000000000011";
const HANDOFF_REGISTRY_BINDING: &str = "30000000-0000-0000-0000-000000000012";
const HANDOFF_REGISTRAR_MANIFEST: i64 = 912;
/// The BaseRegistrar emits the ERC-721 `Transfer`; the controller emits `NameRegistered`.
const HANDOFF_REGISTRAR_ADDRESS: &str = "0x5757575757575757575757575757575757575757";
const HANDOFF_CONTROLLER_ADDRESS: &str = "0x2828282828282828282828282828282828282828";
const RETAINED_OWNER: &str = "0x99999999999999999999999999999999999999Aa";
const SECOND_HOLDER: &str = "0x99999999999999999999999999999999999999Bb";
const THIRD_HOLDER: &str = "0x99999999999999999999999999999999999999Cc";

fn handoff_registrar_authority_key() -> String {
    format!(
        "registrar:{CHAIN}:{HANDOFF_REGISTRAR_MANIFEST}:{HANDOFF_LABELHASH}:{}:2",
        block_hash(8)
    )
}

fn handoff_registry_authority_key() -> String {
    format!("registry-only:{CHAIN}:{HANDOFF_NAMEHASH}")
}

/// The `raw_fact_ref` the adapter attaches to every event it derives from one log: the log's
/// position and the contract that emitted it.
fn handoff_fact_ref(
    emitting_address: &str,
    block_number: i64,
    log_index: i64,
) -> serde_json::Value {
    json!({
        "kind": "raw_log",
        "chain_id": CHAIN,
        "block_hash": block_hash(block_number),
        "block_number": block_number,
        "transaction_hash": format!("0x{:064x}", 1_000 + log_index),
        "transaction_index": 0,
        "log_index": log_index,
        "emitting_address": emitting_address
    })
}

/// Binds a resource with the provenance the adapter records for a binding created by one log.
#[allow(clippy::too_many_arguments)]
async fn seed_handoff_binding(
    pool: &PgPool,
    resource: &str,
    binding: &str,
    block_number: i64,
    log_index: i64,
    active_from: &str,
    active_to: Option<&str>,
    emitting_address: &str,
    source_event: &str,
    source_manifest_id: i64,
) -> Result<()> {
    let mut provenance = handoff_fact_ref(emitting_address, block_number, log_index);
    provenance["source"] = json!("raw_log");
    provenance["source_event"] = json!(source_event);
    provenance["source_manifest_id"] = json!(source_manifest_id);
    provenance
        .as_object_mut()
        .expect("provenance is an object")
        .remove("kind");
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind,
             authority_arm, active_from, active_to, chain_id, block_hash, block_number,
             canonicality_state, provenance
         ) VALUES (
             $1::uuid, $2, $3::uuid, 'declared_registry_path', 'ens_v1',
             $4::timestamptz, $5::timestamptz, $6, $7, $8, 'canonical', $9
         )",
    )
    .bind(binding)
    .bind(HANDOFF_LOGICAL)
    .bind(resource)
    .bind(active_from)
    .bind(active_to)
    .bind(CHAIN)
    .bind(block_hash(block_number))
    .bind(block_number)
    .bind(provenance)
    .execute(pool)
    .await?;
    Ok(())
}

/// Seeds one event of the handoff scenario with the provenance the adapter gives it: the
/// source family of the manifest whose log produced it and that log's emitting contract.
#[allow(clippy::too_many_arguments)]
async fn seed_handoff_event(
    pool: &PgPool,
    identity: &str,
    resource: &str,
    event_kind: &str,
    source_family: &str,
    emitting_address: &str,
    block_number: i64,
    log_index: i64,
    after_state: serde_json::Value,
) -> Result<()> {
    seed_normalized_event(
        pool,
        identity,
        Some(HANDOFF_LOGICAL),
        Some(resource),
        event_kind,
        source_family,
        block_number,
        log_index,
        after_state,
        handoff_fact_ref(emitting_address, block_number, log_index),
    )
    .await
}

/// Seeds a live ENSv1 registrar name whose registry owner and token holder are both R, then the
/// registrar transfer R -> S without reclaim, with the provenance and payloads the adapter
/// emits for that history (copied from the adapter test
/// `registrar_transfers_without_reclaim_keep_the_retained_registry_owner_on_the_epoch`):
///
/// - block 8, log 0, registry contract: the registration's `NewOwner` lands on the registrar
///   resource as `AuthorityTransferred` with owner R;
/// - block 8, log 2, registrar controller: `RegistrationGranted`, `ExpiryChanged`,
///   `SurfaceBound` and `AuthorityEpochChanged` on the registrar resource under the registrar
///   source family, and the registrar binding;
/// - block 9, log 0, BaseRegistrar: `TokenControlTransferred` to S on the registrar resource,
///   `SurfaceUnbound` on it, then `SurfaceBound` and `AuthorityEpochChanged` on the registry-only
///   resource carrying `registry_owner` R, and the registry-only binding.
///
/// No registry event follows the handoff, because a registrar token transfer writes no registry
/// state (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172-L175
/// @ ens_v1@91c966f). The adapter's `PermissionChanged`, `SubregistryChanged` and
/// `PreimageObserved` rows and the `registrar_surface_evidence` payload are left out: the
/// exact-name control fold does not read them.
async fn seed_registrar_handoff_without_reclaim(pool: &PgPool) -> Result<()> {
    seed_blocks(pool, [8, 9, 10]).await?;
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
             namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             $1, 'ens', 'handoff.eth', ARRAY['handoff', 'eth'], '\\x00', $2,
             ARRAY[$3, $4], 'test', 'active', $5, $6, 8, 'canonical'
         )",
    )
    .bind(HANDOFF_LOGICAL)
    .bind(HANDOFF_NAMEHASH)
    .bind(HANDOFF_LABELHASH)
    .bind(format!("0x{:064x}", 2_u64))
    .bind(CHAIN)
    .bind(block_hash(8))
    .execute(pool)
    .await?;
    for resource in [HANDOFF_REGISTRY_RESOURCE, HANDOFF_REGISTRAR_RESOURCE] {
        sqlx::query(
            "INSERT INTO resources (
                 resource_id, chain_id, block_hash, block_number, canonicality_state
             ) VALUES ($1::uuid, $2, $3, 8, 'canonical')",
        )
        .bind(resource)
        .bind(CHAIN)
        .bind(block_hash(8))
        .execute(pool)
        .await?;
    }
    let registrar_key = handoff_registrar_authority_key();
    let registry_key = handoff_registry_authority_key();
    let registration = json!({
        "authority_key": registrar_key,
        "authority_kind": "registrar",
        "cost": "7",
        "decoded_label": "handoff",
        "expiry": 4_102_444_800_i64,
        "labelhash": HANDOFF_LABELHASH,
        "namehash": HANDOFF_NAMEHASH,
        "raw_label_hex": "68616e646f6666",
        "registrant": RETAINED_OWNER,
        "source_event": "NameRegistered",
        "surface_known": true,
        "token_lineage_id": HANDOFF_TOKEN_LINEAGE
    });

    // Block 8: the registration transaction. The registry NewOwner names R.
    seed_handoff_event(
        pool,
        "fixture:handoff-registered-owner",
        HANDOFF_REGISTRAR_RESOURCE,
        "AuthorityTransferred",
        "ens_v1_registry_l1",
        REGISTRY_ADDRESS,
        8,
        0,
        json!({
            "child_node": HANDOFF_NAMEHASH,
            "emitter_role": "registry",
            "labelhash": HANDOFF_LABELHASH,
            "node": HANDOFF_PARENT_HASH,
            "owner": RETAINED_OWNER,
            "owner_getter": RETAINED_OWNER,
            "source_event": "NewOwner"
        }),
    )
    .await?;
    let mut granted = registration.clone();
    granted["authority_owner"] = json!(RETAINED_OWNER);
    seed_handoff_event(
        pool,
        "fixture:handoff-registration",
        HANDOFF_REGISTRAR_RESOURCE,
        "RegistrationGranted",
        "ens_v1_registrar_l1",
        HANDOFF_CONTROLLER_ADDRESS,
        8,
        2,
        granted,
    )
    .await?;
    seed_handoff_event(
        pool,
        "fixture:handoff-expiry",
        HANDOFF_REGISTRAR_RESOURCE,
        "ExpiryChanged",
        "ens_v1_registrar_l1",
        HANDOFF_CONTROLLER_ADDRESS,
        8,
        2,
        registration.clone(),
    )
    .await?;
    seed_handoff_binding(
        pool,
        HANDOFF_REGISTRAR_RESOURCE,
        HANDOFF_REGISTRAR_BINDING,
        8,
        2,
        "2026-08-01T00:00:08.000002Z",
        Some("2026-08-01T00:00:09Z"),
        HANDOFF_CONTROLLER_ADDRESS,
        "NameRegistered",
        HANDOFF_REGISTRAR_MANIFEST,
    )
    .await?;
    let mut bound = registration.clone();
    bound["active_from"] = json!(8);
    bound["binding_kind"] = json!("declared_registry_path");
    bound["owner_getter"] = json!(RETAINED_OWNER);
    bound["registry_contract"] = json!(REGISTRY_ADDRESS);
    seed_handoff_event(
        pool,
        "fixture:handoff-registrar-bound",
        HANDOFF_REGISTRAR_RESOURCE,
        "SurfaceBound",
        "ens_v1_registrar_l1",
        HANDOFF_CONTROLLER_ADDRESS,
        8,
        2,
        bound,
    )
    .await?;
    seed_handoff_event(
        pool,
        "fixture:handoff-registrar-epoch",
        HANDOFF_REGISTRAR_RESOURCE,
        "AuthorityEpochChanged",
        "ens_v1_registrar_l1",
        HANDOFF_CONTROLLER_ADDRESS,
        8,
        2,
        registration,
    )
    .await?;

    // Block 9: the BaseRegistrar Transfer R -> S without reclaim. Every row below comes from
    // that one registrar log.
    seed_handoff_event(
        pool,
        "fixture:handoff-token-transfer",
        HANDOFF_REGISTRAR_RESOURCE,
        "TokenControlTransferred",
        "ens_v1_registrar_l1",
        HANDOFF_REGISTRAR_ADDRESS,
        9,
        0,
        json!({
            "namehash": HANDOFF_NAMEHASH,
            "source_event": "Transfer",
            "to": SECOND_HOLDER,
            "token_id": HANDOFF_LABELHASH,
            "token_lineage_id": HANDOFF_TOKEN_LINEAGE
        }),
    )
    .await?;
    seed_handoff_event(
        pool,
        "fixture:handoff-unbound",
        HANDOFF_REGISTRAR_RESOURCE,
        "SurfaceUnbound",
        "ens_v1_registrar_l1",
        HANDOFF_REGISTRAR_ADDRESS,
        9,
        0,
        json!({
            "active_to": 9,
            "authority_key": registrar_key,
            "authority_kind": "registrar",
            "registry_owner": RETAINED_OWNER,
            "source_event": "Transfer"
        }),
    )
    .await?;
    seed_handoff_binding(
        pool,
        HANDOFF_REGISTRY_RESOURCE,
        HANDOFF_REGISTRY_BINDING,
        9,
        0,
        "2026-08-01T00:00:09Z",
        None,
        HANDOFF_REGISTRAR_ADDRESS,
        "Transfer",
        HANDOFF_REGISTRAR_MANIFEST,
    )
    .await?;
    seed_handoff_event(
        pool,
        "fixture:handoff-bound",
        HANDOFF_REGISTRY_RESOURCE,
        "SurfaceBound",
        "ens_v1_registrar_l1",
        HANDOFF_REGISTRAR_ADDRESS,
        9,
        0,
        json!({
            "active_from": 9,
            "authority_key": registry_key,
            "authority_kind": "registry_only",
            "binding_kind": "declared_registry_path",
            "owner_getter": RETAINED_OWNER,
            "registry_contract": REGISTRY_ADDRESS,
            "registry_owner": RETAINED_OWNER,
            "source_event": "Transfer"
        }),
    )
    .await?;
    seed_handoff_event(
        pool,
        "fixture:handoff-epoch",
        HANDOFF_REGISTRY_RESOURCE,
        "AuthorityEpochChanged",
        "ens_v1_registrar_l1",
        HANDOFF_REGISTRAR_ADDRESS,
        9,
        0,
        json!({
            "authority_key": registry_key,
            "authority_kind": "registry_only",
            "registry_owner": RETAINED_OWNER,
            "source_event": "Transfer"
        }),
    )
    .await
}

/// Seeds the later BaseRegistrar transfer S -> T, still without reclaim: the adapter emits only
/// the token transfer on the registrar resource because the registry-only authority stays
/// selected, so no binding, `SurfaceBound` or `AuthorityEpochChanged` accompanies it.
async fn seed_later_registrar_transfer(pool: &PgPool) -> Result<()> {
    seed_handoff_event(
        pool,
        "fixture:handoff-later-token-transfer",
        HANDOFF_REGISTRAR_RESOURCE,
        "TokenControlTransferred",
        "ens_v1_registrar_l1",
        HANDOFF_REGISTRAR_ADDRESS,
        10,
        0,
        json!({
            "namehash": HANDOFF_NAMEHASH,
            "source_event": "Transfer",
            "to": THIRD_HOLDER,
            "token_id": HANDOFF_LABELHASH,
            "token_lineage_id": HANDOFF_TOKEN_LINEAGE
        }),
    )
    .await
}

async fn handoff_control(pool: &PgPool) -> Result<serde_json::Value> {
    Ok(sqlx::query_scalar(
        "SELECT declared_summary -> 'control' FROM name_current WHERE logical_name_id = $1",
    )
    .bind(HANDOFF_LOGICAL)
    .fetch_one(pool)
    .await?)
}

fn assert_handoff_control_keeps_the_registry_owner(control: &serde_json::Value, stage: &str) {
    assert_eq!(
        control["registry_owner"],
        json!(RETAINED_OWNER.to_lowercase()),
        "{stage}: the registry still names R after a transfer without reclaim, got {control}"
    );
    assert!(
        control.get("owner").is_none(),
        "{stage}: the exact-name control summary publishes the registry owner under registry_owner"
    );
}

/// After the adapter's real registry-only handoff (issue #923), the exact-name owner must stay
/// the retained registry owner R, and a later token transfer S -> T must not clear it. The
/// registrant follows the token to S at the handoff; whether it follows the later transfer
/// to T is left to the registration-event supplement (#911). Checked incrementally and as a
/// rebuild from zero.
#[tokio::test]
async fn registrar_handoff_without_reclaim_keeps_the_registry_owner_across_a_later_transfer()
-> Result<()> {
    let (database, pool) = migrated_pool().await?;
    seed_registrar_handoff_without_reclaim(&pool).await?;
    run_project(&pool, 9, 8, None).await?;
    let selected: Option<String> =
        sqlx::query_scalar("SELECT resource_id::text FROM name_current WHERE logical_name_id = $1")
            .bind(HANDOFF_LOGICAL)
            .fetch_one(&pool)
            .await?;
    assert_eq!(selected.as_deref(), Some(HANDOFF_REGISTRY_RESOURCE));
    let after_handoff = handoff_control(&pool).await?;
    assert_handoff_control_keeps_the_registry_owner(&after_handoff, "incremental handoff");
    assert_eq!(
        after_handoff["registrant"],
        json!(SECOND_HOLDER.to_lowercase()),
        "incremental handoff: the registrant follows the token"
    );

    seed_later_registrar_transfer(&pool).await?;
    run_project(&pool, 10, 10, Some(9)).await?;
    let incremental = handoff_control(&pool).await?;
    assert_handoff_control_keeps_the_registry_owner(&incremental, "incremental later transfer");
    // The registrant after a post-handoff token transfer is not pinned here: main still serves
    // the handoff holder S because the later transfer sits after the selected binding's
    // position, and moving it to T is the registration-event supplement's job (#911).
    assert!(
        incremental["registrant"].is_string(),
        "incremental later transfer: a registrant is still served, got {incremental}"
    );
    let incremental_snapshot = serving_projection_snapshot(&pool).await?;
    database.cleanup().await?;

    let (database, pool) = migrated_pool().await?;
    seed_registrar_handoff_without_reclaim(&pool).await?;
    seed_later_registrar_transfer(&pool).await?;
    run_project(&pool, 10, 8, None).await?;
    let rebuilt = handoff_control(&pool).await?;
    assert_handoff_control_keeps_the_registry_owner(&rebuilt, "rebuild from zero");
    assert_eq!(rebuilt, incremental);
    assert_eq!(
        serving_projection_snapshot(&pool).await?,
        incremental_snapshot,
        "a rebuild from zero must serve what the incremental run served"
    );
    database.cleanup().await?;
    Ok(())
}

const SUPERSEDED_NAMEHASH: &str =
    "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
const SUPERSEDED_LOGICAL: &str =
    "ens:0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
const SUPERSEDED_REGISTRAR_RESOURCE: &str = "99999999-9999-9999-9999-999999999999";
const SUPERSEDED_REGISTRY_RESOURCE: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
const SUPERSEDED_REGISTRAR_BINDING: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
const SUPERSEDED_REGISTRY_BINDING: &str = "cccccccc-cccc-cccc-cccc-cccccccccccc";
const CURRENT_OWNER: &str = "0x44444444444444444444444444444444444444Dd";

/// The fold must stay chronological across the union rather than preferring either side of it,
/// so a reclaim recorded on the selected registry-only resource still outranks the older owner on
/// the predecessor resource. This pins the ordering, not the readmission itself -- the winning
/// event is on the selected resource and so is admitted either way.
#[tokio::test]
async fn a_later_selected_resource_owner_outranks_the_readmitted_predecessor() -> Result<()> {
    let (database, pool) = migrated_pool().await?;
    seed_chain(&pool).await?;
    seed_surface(
        &pool,
        SUPERSEDED_NAMEHASH,
        "superseded-fixture.eth",
        SUPERSEDED_REGISTRAR_RESOURCE,
        SUPERSEDED_REGISTRAR_BINDING,
    )
    .await?;
    seed_authority_transferred(
        &pool,
        "fixture:superseded-old-owner",
        SUPERSEDED_NAMEHASH,
        SUPERSEDED_REGISTRAR_RESOURCE,
        8,
        1,
        json!({
            "node": SUPERSEDED_NAMEHASH,
            "owner": DIVERGENT_OWNER,
            "authority_kind": "registry_only"
        }),
    )
    .await?;
    seed_successor_binding(
        &pool,
        SUPERSEDED_NAMEHASH,
        SUPERSEDED_REGISTRY_RESOURCE,
        SUPERSEDED_REGISTRY_BINDING,
        9,
    )
    .await?;
    seed_authority_epoch_changed(
        &pool,
        "fixture:superseded-epoch",
        SUPERSEDED_NAMEHASH,
        SUPERSEDED_REGISTRY_RESOURCE,
        9,
        "registry_only",
    )
    .await?;
    // A later reclaim on the selected resource itself.
    seed_authority_transferred(
        &pool,
        "fixture:superseded-current-owner",
        SUPERSEDED_NAMEHASH,
        SUPERSEDED_REGISTRY_RESOURCE,
        10,
        2,
        json!({
            "node": SUPERSEDED_NAMEHASH,
            "owner": CURRENT_OWNER,
            "authority_kind": "registry_only"
        }),
    )
    .await?;

    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block: 10,
            affected_from_block: 8,
            affected_to_block: 10,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;

    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT address, relation FROM address_names_current WHERE logical_name_id = $1",
    )
    .bind(SUPERSEDED_LOGICAL)
    .fetch_all(&pool)
    .await?;
    assert_eq!(
        rows,
        vec![(
            CURRENT_OWNER.to_lowercase(),
            "effective_controller".to_owned()
        )],
        "the readmitted predecessor outranked the selected resource's later owner"
    );

    database.cleanup().await?;
    Ok(())
}

const RESIDUE_NAMEHASH: &str = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
const RESIDUE_LOGICAL: &str =
    "ens:0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
const RESIDUE_REGISTRAR_RESOURCE: &str = "dddddddd-dddd-dddd-dddd-dddddddddddd";
const RESIDUE_REGISTRY_RESOURCE: &str = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee";
const RESIDUE_REGISTRAR_BINDING: &str = "ffffffff-ffff-ffff-ffff-ffffffffffff";
const RESIDUE_REGISTRY_BINDING: &str = "12121212-1212-1212-1212-121212121212";
const RESIDUE_OWNER: &str = "0x55555555555555555555555555555555555555Ee";

/// Readmission is bounded to the predecessor era. An ownership event landing on the superseded
/// resource *after* the selected binding opened is out of scope for the divergence this restores,
/// so it must not become the controller — the divergent owner recorded before the binding stands.
#[tokio::test]
async fn a_post_binding_event_on_the_superseded_resource_is_not_readmitted() -> Result<()> {
    let (database, pool) = migrated_pool().await?;
    seed_chain(&pool).await?;
    seed_surface(
        &pool,
        RESIDUE_NAMEHASH,
        "residue-fixture.eth",
        RESIDUE_REGISTRAR_RESOURCE,
        RESIDUE_REGISTRAR_BINDING,
    )
    .await?;
    seed_authority_transferred(
        &pool,
        "fixture:residue-divergent",
        RESIDUE_NAMEHASH,
        RESIDUE_REGISTRAR_RESOURCE,
        8,
        1,
        json!({
            "node": RESIDUE_NAMEHASH,
            "owner": DIVERGENT_OWNER,
            "authority_kind": "registry_only"
        }),
    )
    .await?;
    seed_successor_binding(
        &pool,
        RESIDUE_NAMEHASH,
        RESIDUE_REGISTRY_RESOURCE,
        RESIDUE_REGISTRY_BINDING,
        9,
    )
    .await?;
    seed_authority_epoch_changed(
        &pool,
        "fixture:residue-epoch",
        RESIDUE_NAMEHASH,
        RESIDUE_REGISTRY_RESOURCE,
        9,
        "registry_only",
    )
    .await?;
    // Residue on the resource the binding already superseded.
    seed_authority_transferred(
        &pool,
        "fixture:residue-late",
        RESIDUE_NAMEHASH,
        RESIDUE_REGISTRAR_RESOURCE,
        10,
        2,
        json!({
            "node": RESIDUE_NAMEHASH,
            "owner": RESIDUE_OWNER,
            "authority_kind": "registry_only"
        }),
    )
    .await?;

    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block: 10,
            affected_from_block: 8,
            affected_to_block: 10,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;

    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT address, relation FROM address_names_current WHERE logical_name_id = $1",
    )
    .bind(RESIDUE_LOGICAL)
    .fetch_all(&pool)
    .await?;
    assert_eq!(
        rows,
        vec![(
            DIVERGENT_OWNER.to_lowercase(),
            "effective_controller".to_owned()
        )],
        "a post-binding event on the superseded resource was readmitted"
    );

    database.cleanup().await?;
    Ok(())
}

const STALE_NAMEHASH: &str = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
const STALE_LOGICAL: &str =
    "ens:0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
const STALE_REGISTRAR_RESOURCE: &str = "dddddddd-dddd-dddd-dddd-dddddddddddd";
const STALE_WRAPPER_RESOURCE: &str = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee";
const STALE_REGISTRY_RESOURCE: &str = "ffffffff-ffff-ffff-ffff-ffffffffffff";
const STALE_REGISTRAR_BINDING: &str = "10000000-0000-0000-0000-000000000001";
const STALE_WRAPPER_BINDING: &str = "10000000-0000-0000-0000-000000000002";
const STALE_REGISTRY_BINDING: &str = "10000000-0000-0000-0000-000000000003";
const STALE_OWNER: &str = "0x55555555555555555555555555555555555555Ee";
const PRE_BINDING_OWNER: &str = "0x66666666666666666666666666666666666666Ff";
const OFF_PATH_OWNER: &str = "0x77777777777777777777777777777777777777Aa";

/// Readmission exists to recover the owner the *immediate* predecessor binding left behind, so it
/// must stop at that binding's start. A name that moved registrar -> wrapper -> registry-only has
/// an ownership event from the registrar era that no longer describes anyone's authority; folding
/// it back in would publish a long-superseded address, and for a name that expired while wrapped
/// the same widening would publish the wrapper contract itself as the controller.
#[tokio::test]
async fn a_stale_event_from_before_the_predecessor_binding_is_not_readmitted() -> Result<()> {
    let (database, pool) = migrated_pool().await?;
    seed_chain(&pool).await?;
    seed_surface(
        &pool,
        STALE_NAMEHASH,
        "stale-fixture.eth",
        STALE_REGISTRAR_RESOURCE,
        STALE_REGISTRAR_BINDING,
    )
    .await?;
    seed_authority_transferred(
        &pool,
        "fixture:stale-registrar-owner",
        STALE_NAMEHASH,
        STALE_REGISTRAR_RESOURCE,
        8,
        1,
        json!({
            "node": STALE_NAMEHASH,
            "owner": STALE_OWNER,
            "authority_kind": "registry_only"
        }),
    )
    .await?;
    seed_next_binding(
        &pool,
        STALE_NAMEHASH,
        STALE_WRAPPER_RESOURCE,
        STALE_WRAPPER_BINDING,
        9,
        "2026-07-02T00:00:00Z",
    )
    .await?;
    // Isolates the lower bound: on the predecessor resource, but from before that binding opened.
    seed_authority_transferred(
        &pool,
        "fixture:stale-wrapper-pre-binding",
        STALE_NAMEHASH,
        STALE_WRAPPER_RESOURCE,
        8,
        2,
        json!({
            "node": STALE_NAMEHASH,
            "owner": PRE_BINDING_OWNER,
            "authority_kind": "registry_only"
        }),
    )
    .await?;
    // Isolates the resource restriction: inside the position window, but on a resource the
    // immediate predecessor is not.
    seed_authority_transferred(
        &pool,
        "fixture:stale-registrar-in-window",
        STALE_NAMEHASH,
        STALE_REGISTRAR_RESOURCE,
        9,
        3,
        json!({
            "node": STALE_NAMEHASH,
            "owner": OFF_PATH_OWNER,
            "authority_kind": "registry_only"
        }),
    )
    .await?;
    seed_next_binding(
        &pool,
        STALE_NAMEHASH,
        STALE_REGISTRY_RESOURCE,
        STALE_REGISTRY_BINDING,
        10,
        "2026-07-03T00:00:00Z",
    )
    .await?;
    seed_authority_epoch_changed(
        &pool,
        "fixture:stale-epoch",
        STALE_NAMEHASH,
        STALE_REGISTRY_RESOURCE,
        10,
        "registry_only",
    )
    .await?;

    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block: 10,
            affected_from_block: 8,
            affected_to_block: 10,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;

    // Anti-vacuity: the readmission gate is armed -- the selected resource is the registry-only
    // one, and the registrar resource is still a same-arm binding candidate of this name.
    let selected: Option<String> =
        sqlx::query_scalar("SELECT resource_id::text FROM name_current WHERE logical_name_id = $1")
            .bind(STALE_LOGICAL)
            .fetch_one(&pool)
            .await?;
    assert_eq!(selected.as_deref(), Some(STALE_REGISTRY_RESOURCE));
    let candidates: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM surface_bindings
         WHERE logical_name_id = $1 AND resource_id = $2::uuid AND authority_arm = 'ens_v1'",
    )
    .bind(STALE_LOGICAL)
    .bind(STALE_REGISTRAR_RESOURCE)
    .fetch_one(&pool)
    .await?;
    assert_eq!(candidates, 1);

    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT address, relation FROM address_names_current WHERE logical_name_id = $1",
    )
    .bind(STALE_LOGICAL)
    .fetch_all(&pool)
    .await?;
    assert!(
        rows.is_empty(),
        "readmission reached past the immediate predecessor binding and published {rows:?}"
    );

    database.cleanup().await?;
    Ok(())
}

const BOUNDARY_NAMEHASH: &str =
    "0x1111111111111111111111111111111111111111111111111111111111111111";
const BOUNDARY_LOGICAL: &str =
    "ens:0x1111111111111111111111111111111111111111111111111111111111111111";
const BOUNDARY_REGISTRAR_RESOURCE: &str = "20000000-0000-0000-0000-000000000001";
const BOUNDARY_REGISTRY_RESOURCE: &str = "20000000-0000-0000-0000-000000000002";
const BOUNDARY_REGISTRAR_BINDING: &str = "20000000-0000-0000-0000-000000000011";
const BOUNDARY_REGISTRY_BINDING: &str = "20000000-0000-0000-0000-000000000012";
const BOUNDARY_OWNER: &str = "0x88888888888888888888888888888888888888Bb";

/// The transfer that opens the divergence lands in the same transaction as the binding that
/// records it, at the same log position. That event is the whole point of the readmission, so the
/// upper bound has to include its own position rather than stop just short of it.
#[tokio::test]
async fn an_authority_transfer_at_the_selected_binding_position_is_readmitted() -> Result<()> {
    let (database, pool) = migrated_pool().await?;
    seed_chain(&pool).await?;
    seed_surface(
        &pool,
        BOUNDARY_NAMEHASH,
        "boundary-fixture.eth",
        BOUNDARY_REGISTRAR_RESOURCE,
        BOUNDARY_REGISTRAR_BINDING,
    )
    .await?;
    seed_next_binding(
        &pool,
        BOUNDARY_NAMEHASH,
        BOUNDARY_REGISTRY_RESOURCE,
        BOUNDARY_REGISTRY_BINDING,
        9,
        "2026-07-02T00:00:00Z",
    )
    .await?;
    seed_binding_provenance(&pool, BOUNDARY_REGISTRY_BINDING, 0, 5).await?;
    // Exactly at the selected binding's position, on the superseded registrar resource.
    seed_authority_transferred(
        &pool,
        "fixture:boundary-owner",
        BOUNDARY_NAMEHASH,
        BOUNDARY_REGISTRAR_RESOURCE,
        9,
        5,
        json!({
            "node": BOUNDARY_NAMEHASH,
            "owner": BOUNDARY_OWNER,
            "authority_kind": "registry_only"
        }),
    )
    .await?;
    seed_authority_epoch_changed(
        &pool,
        "fixture:boundary-epoch",
        BOUNDARY_NAMEHASH,
        BOUNDARY_REGISTRY_RESOURCE,
        9,
        "registry_only",
    )
    .await?;

    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block: 10,
            affected_from_block: 8,
            affected_to_block: 10,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;

    // Anti-vacuity: the bound is a real position comparison, not (block, -1, -1) on both sides.
    let provenance: Option<serde_json::Value> = sqlx::query_scalar(
        "SELECT provenance FROM surface_bindings WHERE surface_binding_id = $1::uuid",
    )
    .bind(BOUNDARY_REGISTRY_BINDING)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        provenance,
        Some(json!({"transaction_index": 0, "log_index": 5}))
    );

    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT address, relation FROM address_names_current WHERE logical_name_id = $1",
    )
    .bind(BOUNDARY_LOGICAL)
    .fetch_all(&pool)
    .await?;
    assert_eq!(
        rows,
        vec![(
            BOUNDARY_OWNER.to_lowercase(),
            "effective_controller".to_owned()
        )],
        "the transfer at the selected binding's own position was dropped"
    );

    database.cleanup().await?;
    Ok(())
}

const CROSS_ARM_NAMEHASH: &str =
    "0x2222222222222222222222222222222222222222222222222222222222222222";
const CROSS_ARM_LOGICAL: &str =
    "ens:0x2222222222222222222222222222222222222222222222222222222222222222";
const CROSS_ARM_REGISTRAR_RESOURCE: &str = "30000000-0000-0000-0000-000000000001";
const CROSS_ARM_OTHER_RESOURCE: &str = "30000000-0000-0000-0000-000000000002";
const CROSS_ARM_REGISTRY_RESOURCE: &str = "30000000-0000-0000-0000-000000000003";
const CROSS_ARM_REGISTRAR_BINDING: &str = "30000000-0000-0000-0000-000000000011";
const CROSS_ARM_OTHER_BINDING: &str = "30000000-0000-0000-0000-000000000012";
const CROSS_ARM_REGISTRY_BINDING: &str = "30000000-0000-0000-0000-000000000013";
const CROSS_ARM_OWNER: &str = "0x99999999999999999999999999999999999999Cc";

/// "Immediate predecessor" means the immediate predecessor *on the selected arm*. A binding from
/// another arm sitting between the superseded resource and the selection must not stand in for it,
/// or the same-arm divergence stops being recoverable the moment a name has any other-arm history.
#[tokio::test]
async fn an_other_arm_binding_does_not_stand_in_for_the_same_arm_predecessor() -> Result<()> {
    let (database, pool) = migrated_pool().await?;
    seed_chain(&pool).await?;
    seed_surface(
        &pool,
        CROSS_ARM_NAMEHASH,
        "cross-arm-fixture.eth",
        CROSS_ARM_REGISTRAR_RESOURCE,
        CROSS_ARM_REGISTRAR_BINDING,
    )
    .await?;
    seed_authority_transferred(
        &pool,
        "fixture:cross-arm-owner",
        CROSS_ARM_NAMEHASH,
        CROSS_ARM_REGISTRAR_RESOURCE,
        8,
        1,
        json!({
            "node": CROSS_ARM_NAMEHASH,
            "owner": CROSS_ARM_OWNER,
            "authority_kind": "registry_only"
        }),
    )
    .await?;
    seed_next_arm_binding(
        &pool,
        CROSS_ARM_NAMEHASH,
        CROSS_ARM_OTHER_RESOURCE,
        CROSS_ARM_OTHER_BINDING,
        9,
        "2026-07-02T00:00:00Z",
        "ens_v2",
    )
    .await?;
    seed_next_binding(
        &pool,
        CROSS_ARM_NAMEHASH,
        CROSS_ARM_REGISTRY_RESOURCE,
        CROSS_ARM_REGISTRY_BINDING,
        10,
        "2026-07-03T00:00:00Z",
    )
    .await?;
    seed_authority_epoch_changed(
        &pool,
        "fixture:cross-arm-epoch",
        CROSS_ARM_NAMEHASH,
        CROSS_ARM_REGISTRY_RESOURCE,
        10,
        "registry_only",
    )
    .await?;

    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block: 10,
            affected_from_block: 8,
            affected_to_block: 10,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;

    // Anti-vacuity: the other-arm binding really is the most recent one before the selection.
    let nearest_arm: String = sqlx::query_scalar(
        "SELECT authority_arm FROM surface_bindings
         WHERE logical_name_id = $1 AND block_number < 10
         ORDER BY block_number DESC LIMIT 1",
    )
    .bind(CROSS_ARM_LOGICAL)
    .fetch_one(&pool)
    .await?;
    assert_eq!(nearest_arm, "ens_v2");

    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT address, relation FROM address_names_current WHERE logical_name_id = $1",
    )
    .bind(CROSS_ARM_LOGICAL)
    .fetch_all(&pool)
    .await?;
    assert_eq!(
        rows,
        vec![(
            CROSS_ARM_OWNER.to_lowercase(),
            "effective_controller".to_owned()
        )],
        "an other-arm binding displaced the same-arm predecessor"
    );

    database.cleanup().await?;
    Ok(())
}

const BASENAMES_NAMEHASH: &str =
    "0x3333333333333333333333333333333333333333333333333333333333333333";
const BASENAMES_LOGICAL: &str =
    "ens:0x3333333333333333333333333333333333333333333333333333333333333333";
const BASENAMES_REGISTRAR_RESOURCE: &str = "40000000-0000-0000-0000-000000000001";
const BASENAMES_REGISTRY_RESOURCE: &str = "40000000-0000-0000-0000-000000000002";
const BASENAMES_REGISTRAR_BINDING: &str = "40000000-0000-0000-0000-000000000011";
const BASENAMES_REGISTRY_BINDING: &str = "40000000-0000-0000-0000-000000000012";
const BASENAMES_OWNER: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaDd";

/// Basenames reaches this exception with a real shape, not just a fabricated one: its registrar
/// and its registry are both admitted source families, its registrar creates a non-registry
/// predecessor binding, and its registrar writes the registry owner before emitting the event the
/// binding is provenanced to, exactly as ENSv1 does
/// (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L423-L425 @ basenames@1809bbc). So the
/// recovered owner has to be correct for the Basenames arm too, not only for ENSv1.
#[tokio::test]
async fn a_basenames_registry_only_binding_preserves_its_divergent_owner() -> Result<()> {
    let (database, pool) = migrated_pool().await?;
    seed_chain(&pool).await?;
    seed_surface(
        &pool,
        BASENAMES_NAMEHASH,
        "divergent-basename.eth",
        BASENAMES_REGISTRAR_RESOURCE,
        BASENAMES_REGISTRAR_BINDING,
    )
    .await?;
    sqlx::query(
        "UPDATE surface_bindings SET authority_arm = 'basenames'
         WHERE surface_binding_id = $1::uuid",
    )
    .bind(BASENAMES_REGISTRAR_BINDING)
    .execute(&pool)
    .await?;
    seed_authority_transferred(
        &pool,
        "fixture:basenames-divergent-owner",
        BASENAMES_NAMEHASH,
        BASENAMES_REGISTRAR_RESOURCE,
        8,
        1,
        json!({
            "node": BASENAMES_NAMEHASH,
            "owner": BASENAMES_OWNER,
            "authority_kind": "registry_only"
        }),
    )
    .await?;
    seed_next_arm_binding(
        &pool,
        BASENAMES_NAMEHASH,
        BASENAMES_REGISTRY_RESOURCE,
        BASENAMES_REGISTRY_BINDING,
        9,
        "2026-07-02T00:00:00Z",
        "basenames",
    )
    .await?;
    seed_authority_epoch_changed(
        &pool,
        "fixture:basenames-divergent-epoch",
        BASENAMES_NAMEHASH,
        BASENAMES_REGISTRY_RESOURCE,
        9,
        "registry_only",
    )
    .await?;

    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block: 10,
            affected_from_block: 8,
            affected_to_block: 10,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;

    // Anti-vacuity: this is the Basenames arm end to end, not an ENSv1 selection in disguise.
    let selected_arm: String = sqlx::query_scalar(
        "SELECT binding.authority_arm
         FROM name_current name
         JOIN surface_bindings binding
           ON binding.logical_name_id = name.logical_name_id
          AND binding.resource_id = name.resource_id
         WHERE name.logical_name_id = $1",
    )
    .bind(BASENAMES_LOGICAL)
    .fetch_one(&pool)
    .await?;
    assert_eq!(selected_arm, "basenames");

    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT address, relation FROM address_names_current WHERE logical_name_id = $1",
    )
    .bind(BASENAMES_LOGICAL)
    .fetch_all(&pool)
    .await?;
    assert_eq!(
        rows,
        vec![(
            BASENAMES_OWNER.to_lowercase(),
            "effective_controller".to_owned()
        )],
        "the Basenames divergent registry owner lost its relation"
    );

    database.cleanup().await?;
    Ok(())
}
