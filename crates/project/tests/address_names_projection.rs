//! Builder-level coverage for the archived-registry masked owner word: an
//! `AuthorityTransferred` whose `after_state` carries `owner_word_unmasked`
//! authenticates no caller, so it must clear the effective controller with the
//! same shape a zero-owner transition produces, and must never publish the
//! masked low-20-byte tail as a controller.

#[path = "support/bounded_registration.rs"]
mod bounded_registration;

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
const LAPSED_LAST_HOLDER: &str = "0x33333333333333333333333333333333333333Cc";
const OWNERLESS_NAMEHASH: &str =
    "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
const OWNERLESS_LOGICAL: &str =
    "ens:0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
const OWNERLESS_PARENT_HASH: &str =
    "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
const OWNERLESS_PARENT_LOGICAL: &str =
    "ens:0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
const OWNERLESS_RESOURCE: &str = "dddddddd-dddd-dddd-dddd-dddddddddddd";
const OLD_REGISTRAR_RESOURCE: &str = "abababab-abab-abab-abab-abababababab";
const OWNERLESS_BINDING: &str = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee";
const RELEASE_REGISTRY_RESOURCE: &str = "edededed-eded-eded-eded-edededededed";
const RELEASE_REGISTRY_BINDING: &str = "efefefef-efef-efef-efef-efefefefefef";
const REWRAPPED_RESOURCE: &str = "acacacac-acac-acac-acac-acacacacacac";
const REWRAPPED_BINDING: &str = "adadadad-adad-adad-adad-adadadadadad";
const UNWRAPPED_BINDING: &str = "afafafaf-afaf-afaf-afaf-afafafafafaf";
const REWRAPPED_LINEAGE: &str = "aeaeaeae-aeae-aeae-aeae-aeaeaeaeaeae";
const WRAPPER_LINEAGE: &str = "cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcdcd";
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
    bounded_registration::assert_selected_registrations_are_bounded(pool).await?;
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
    bounded_registration::assert_selected_registrations_are_bounded(pool).await?;
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
         ) VALUES ($1::uuid, $2, $3, 8, 'canonical')
         ON CONFLICT (chain_id, resource_id) DO NOTHING",
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

/// The registry-only handoff scenario of issue #923, interpreted by the real adapter and persisted
/// the way Interpret persists adapter output, so Project consumes exactly what production would.
mod handoff_scenario {
    use super::{CHAIN, block_hash};
    use alloy_primitives::{Address, B256, U256, keccak256};
    use alloy_sol_types::{SolEvent, sol};
    use anyhow::Result;
    use bigname_adapters::schema_v2::{
        AdapterSession, AddressAdmissionInput, BatchInput, BatchOutput, ManifestInput,
        RawBlockInput, RawLogInput, StateCacheCapacity, prepare_schema_v2_batch_incremental,
    };
    use bigname_manifests::load_repository;
    use sqlx::PgPool;
    use time::OffsetDateTime;
    use uuid::Uuid;

    sol! {
        event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
        event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
    }

    mod registrar_lifecycle {
        alloy_sol_types::sol! {
            event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
        }
    }

    mod legacy_controller {
        alloy_sol_types::sol! {
            event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 cost, uint256 expires);
        }
    }

    pub const REGISTRY_MANIFEST: i64 = 911;
    pub const REGISTRAR_MANIFEST: i64 = 912;
    pub const REGISTRY_ADDRESS: &str = "0x0000000000000000000000000000000000000091";
    /// The BaseRegistrar is the ERC-721 token and emits `Transfer` and its numeric
    /// `NameRegistered`
    /// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L8 @ ens_v1@91c966f)
    /// (upstream: .refs/ens_v1/contracts/ethregistrar/IBaseRegistrar.sol:L15-L19 @ ens_v1@91c966f);
    /// the legacy controller emits the label-bearing, cost-carrying `NameRegistered`
    /// (upstream: .refs/ens_v1/deployments/archive/ETHRegistrarController_mainnet_9380471.sol/ETHRegistrarController_mainnet_9380471.json:L33-L68 @ ens_v1@91c966f)
    /// (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L333-L341 @ ens_v1@91c966f).
    pub const REGISTRAR_ADDRESS: &str = "0x0000000000000000000000000000000000000042";
    pub const CONTROLLER_ADDRESS: &str = "0x0000000000000000000000000000000000000092";
    pub const RETAINED_OWNER: &str = "0x00000000000000000000000000000000000000ab";
    pub const SECOND_HOLDER: &str = "0x00000000000000000000000000000000000000cd";
    pub const THIRD_HOLDER: &str = "0x00000000000000000000000000000000000000ef";
    pub const LABEL: &str = "handoff";
    pub const REGISTRATION_BLOCK: i64 = 8;
    pub const HANDOFF_BLOCK: i64 = 9;
    pub const LATER_BLOCK: i64 = 10;

    fn namehash(labels: &[&str]) -> B256 {
        labels.iter().rev().fold(B256::ZERO, |node, label| {
            keccak256([node.as_slice(), keccak256(label.as_bytes()).as_slice()].concat())
        })
    }

    pub fn name_namehash() -> B256 {
        namehash(&[LABEL, "eth"])
    }

    pub fn logical_name_id() -> String {
        format!("ens:{:#x}", name_namehash())
    }

    /// The adapter's stable registry-only resource identity for a node (its `stable_uuid`).
    pub fn registry_only_resource() -> Uuid {
        let hash = keccak256(format!(
            "resource:registry-only:{CHAIN}:{:#x}",
            name_namehash()
        ));
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&hash[..16]);
        bytes[6] = (bytes[6] & 0x0f) | 0x50;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes)
    }

    fn block_timestamp(block_number: i64) -> OffsetDateTime {
        // The same instants `seed_blocks` writes: 2026-08-01T00:00:<block>Z.
        time::Date::from_calendar_date(2026, time::Month::August, 1)
            .expect("fixture date")
            .midnight()
            .assume_utc()
            + time::Duration::seconds(block_number)
    }

    fn raw_log(
        encoded: alloy_primitives::LogData,
        block_number: i64,
        log_index: i64,
        emitting_address: &str,
    ) -> RawLogInput {
        RawLogInput {
            chain_id: CHAIN.to_owned(),
            block_hash: block_hash(block_number),
            block_number,
            block_timestamp: block_timestamp(block_number),
            canonicality_state: "canonical".to_owned(),
            transaction_hash: format!("0x{:064x}", 900 + block_number),
            transaction_index: 0,
            log_index,
            emitting_address: emitting_address.to_owned(),
            topics: encoded
                .topics()
                .iter()
                .map(|topic| format!("{topic:#x}"))
                .collect(),
            data: encoded.data.to_vec(),
        }
    }

    fn admission(
        manifest_id: i64,
        instance: u128,
        role: &str,
        address: &str,
    ) -> AddressAdmissionInput {
        AddressAdmissionInput {
            address: address.to_owned(),
            contract_instance_id: Uuid::from_u128(instance),
            source_manifest_id: Some(manifest_id),
            role: Some(role.to_owned()),
            discovery_edge_kind: None,
            discovery_from_contract_instance_id: None,
            discovery_observation_key: None,
            active_from_block: Some(0),
            active_to_block: None,
        }
    }

    /// The checked-in production ENS mainnet manifests, loaded from `manifests/mainnet`: the
    /// active registry manifest (`ens_v1_registry_l1` v3) and the registrar manifest
    /// (`ens_v1_registrar_l1` v1), whose registrar numeric `NameRegistered` only releases, whose
    /// legacy controller `NameRegistered` grants, and whose ERC-721 `Transfer` comes from the
    /// registrar. The fixture manifest ids replace the checked-in ones.
    pub fn manifests() -> Vec<ManifestInput> {
        let repository = load_repository(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/mainnet"),
        )
        .expect("the checked-in mainnet manifests must load");
        [
            (REGISTRY_MANIFEST, "ens_v1_registry_l1", "v3"),
            (REGISTRAR_MANIFEST, "ens_v1_registrar_l1", "v1"),
        ]
        .into_iter()
        .map(|(manifest_id, source_family, version_tag)| {
            let loaded = repository
                .manifests()
                .iter()
                .find(|loaded| {
                    loaded.manifest.chain == CHAIN
                        && loaded.manifest.source_family == source_family
                        && loaded.version_tag == version_tag
                })
                .unwrap_or_else(|| {
                    panic!("the checked-in {source_family} {version_tag} manifest must exist")
                });
            ManifestInput {
                manifest_id,
                manifest_version: i64::try_from(loaded.manifest.manifest_version)
                    .expect("manifest version fits i64"),
                namespace: loaded.manifest.namespace.clone(),
                source_family: loaded.manifest.source_family.clone(),
                chain_id: loaded.manifest.chain.clone(),
                deployment_label: loaded.manifest.deployment_epoch.clone(),
                normalizer_version: loaded.manifest.normalizer_version.clone(),
                payload_json: serde_json::to_string(&loaded.manifest)
                    .expect("the checked-in manifest serializes"),
            }
        })
        .collect()
    }

    fn batch(block_number: i64, raw_logs: Vec<RawLogInput>) -> BatchInput {
        BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: manifests(),
            discovery_rules: Vec::new(),
            admissions: vec![
                admission(REGISTRY_MANIFEST, 911, "registry", REGISTRY_ADDRESS),
                admission(REGISTRAR_MANIFEST, 912, "registrar", REGISTRAR_ADDRESS),
                admission(
                    REGISTRAR_MANIFEST,
                    9121,
                    "legacy_registrar_controller",
                    CONTROLLER_ADDRESS,
                ),
            ],
            prior_events: Vec::new(),
            blocks: vec![RawBlockInput {
                chain_id: CHAIN.to_owned(),
                block_hash: block_hash(block_number),
                block_number,
                block_timestamp: block_timestamp(block_number),
                canonicality_state: "canonical".to_owned(),
            }],
            raw_logs,
        }
    }

    fn address(value: &str) -> Address {
        value.parse().expect("fixture address")
    }

    /// The registration transaction in the order the ENSv1 contracts emit it. The controller
    /// calls `base.register`
    /// (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L288-L298 @ ens_v1@91c966f);
    /// inside `_register` the registrar mints the token (the ERC-721 `Transfer` from zero),
    /// sets the registry owner, which makes the registry emit `NewOwner`, and then emits its
    /// numeric `NameRegistered`
    /// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L131-L153 @ ens_v1@91c966f)
    /// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L84 @ ens_v1@91c966f);
    /// the controller emits its label-bearing `NameRegistered` after the call returns
    /// (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L333-L341 @ ens_v1@91c966f).
    pub fn registration() -> BatchInput {
        let label = keccak256(LABEL.as_bytes());
        batch(
            REGISTRATION_BLOCK,
            vec![
                raw_log(
                    Transfer {
                        from: Address::ZERO,
                        to: address(RETAINED_OWNER),
                        tokenId: U256::from_be_bytes(*label),
                    }
                    .encode_log_data(),
                    REGISTRATION_BLOCK,
                    0,
                    REGISTRAR_ADDRESS,
                ),
                raw_log(
                    NewOwner {
                        node: namehash(&["eth"]),
                        label,
                        owner: address(RETAINED_OWNER),
                    }
                    .encode_log_data(),
                    REGISTRATION_BLOCK,
                    1,
                    REGISTRY_ADDRESS,
                ),
                raw_log(
                    registrar_lifecycle::NameRegistered {
                        id: U256::from_be_bytes(*label),
                        owner: address(RETAINED_OWNER),
                        expires: U256::from(4_102_444_800_u64),
                    }
                    .encode_log_data(),
                    REGISTRATION_BLOCK,
                    2,
                    REGISTRAR_ADDRESS,
                ),
                raw_log(
                    legacy_controller::NameRegistered {
                        name: LABEL.to_owned(),
                        label,
                        owner: address(RETAINED_OWNER),
                        cost: U256::from(7),
                        expires: U256::from(4_102_444_800_u64),
                    }
                    .encode_log_data(),
                    REGISTRATION_BLOCK,
                    3,
                    CONTROLLER_ADDRESS,
                ),
            ],
        )
    }

    /// A registrar token transfer without `reclaim`: one ERC-721 `Transfer` from the registrar
    /// and nothing from the registry. The registrar inherits the ERC-721 transfer unchanged
    /// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L8 @ ens_v1@91c966f)
    /// and writes the registry owner only from `_register` and `reclaim`
    /// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L148-L150 @ ens_v1@91c966f)
    /// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L171-L175 @ ens_v1@91c966f),
    /// and the registry emits `NewOwner` and `Transfer` only from its own owner writes
    /// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L63-L69 @ ens_v1@91c966f)
    /// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L84 @ ens_v1@91c966f).
    pub fn token_transfer(block_number: i64, from: &str, to: &str) -> BatchInput {
        batch(
            block_number,
            vec![raw_log(
                Transfer {
                    from: address(from),
                    to: address(to),
                    tokenId: U256::from_be_bytes(*keccak256(LABEL.as_bytes())),
                }
                .encode_log_data(),
                block_number,
                0,
                REGISTRAR_ADDRESS,
            )],
        )
    }

    pub fn interpret(
        input: BatchInput,
        session: Option<AdapterSession>,
    ) -> Result<(BatchOutput, AdapterSession)> {
        let (output, session) =
            prepare_schema_v2_batch_incremental(input, session, StateCacheCapacity::Unlimited)?
                .finish(Vec::new())?;
        assert!(
            output.decode_skips.is_empty(),
            "every fixture log must decode: {:?}",
            output.decode_skips
        );
        Ok((output, session))
    }

    /// Persists adapter output the way Interpret does for the rows Project reads: the manifest
    /// versions the events cite, token lineages, resources, name surfaces, surface bindings
    /// with their closures, and normalized events with every column Interpret writes. Label
    /// preimages, contract identity and discovery rows are not persisted; the exact-name
    /// control fold does not read them.
    pub async fn persist(pool: &PgPool, output: &BatchOutput) -> Result<()> {
        for manifest in manifests() {
            sqlx::query(
                "INSERT INTO manifest_versions (
                     manifest_id, manifest_version, namespace, source_family, chain_id,
                     deployment_label, rollout_status, normalizer_version, file_path,
                     manifest_payload
                 ) OVERRIDING SYSTEM VALUE
                 VALUES ($1, $2, $3, $4, $5, $6, 'active', $7, $8, $9::jsonb)
                 ON CONFLICT (manifest_id) DO NOTHING",
            )
            .bind(manifest.manifest_id)
            .bind(manifest.manifest_version)
            .bind(&manifest.namespace)
            .bind(&manifest.source_family)
            .bind(&manifest.chain_id)
            .bind(&manifest.deployment_label)
            .bind(&manifest.normalizer_version)
            .bind(format!("fixture/handoff/{}.toml", manifest.source_family))
            .bind(&manifest.payload_json)
            .execute(pool)
            .await?;
        }
        for lineage in &output.token_lineages {
            sqlx::query(
                "INSERT INTO token_lineages (
                     token_lineage_id, chain_id, block_hash, block_number, provenance,
                     canonicality_state
                 ) VALUES ($1, $2, $3, $4, $5, $6::canonicality_state)
                 ON CONFLICT (token_lineage_id) DO NOTHING",
            )
            .bind(lineage.token_lineage_id)
            .bind(&lineage.chain_id)
            .bind(&lineage.block_hash)
            .bind(lineage.block_number)
            .bind(&lineage.provenance)
            .bind(&lineage.canonicality_state)
            .execute(pool)
            .await?;
        }
        for resource in &output.resources {
            sqlx::query(
                "INSERT INTO resources (
                     resource_id, token_lineage_id, chain_id, block_hash, block_number,
                     provenance, canonicality_state
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7::canonicality_state)
                 ON CONFLICT (resource_id) DO NOTHING",
            )
            .bind(resource.resource_id)
            .bind(resource.token_lineage_id)
            .bind(&resource.chain_id)
            .bind(&resource.block_hash)
            .bind(resource.block_number)
            .bind(&resource.provenance)
            .bind(&resource.canonicality_state)
            .execute(pool)
            .await?;
        }
        for surface in &output.name_surfaces {
            sqlx::query(
                "INSERT INTO name_surfaces (
                     logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
                     namehash, labelhashes, normalizer_version, visibility_state,
                     normalization_errors, deactivation_reason, deactivated_at, chain_id,
                     block_hash, block_number, provenance, canonicality_state
                 ) VALUES (
                     $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                     $17::canonicality_state
                 )
                 ON CONFLICT (logical_name_id) DO NOTHING",
            )
            .bind(&surface.logical_name_id)
            .bind(&surface.namespace)
            .bind(&surface.raw_name)
            .bind(&surface.raw_labels)
            .bind(&surface.dns_encoded_name)
            .bind(&surface.namehash)
            .bind(&surface.labelhashes)
            .bind(&surface.normalizer_version)
            .bind(&surface.visibility_state)
            .bind(&surface.normalization_errors)
            .bind(&surface.deactivation_reason)
            .bind(surface.deactivated_at)
            .bind(&surface.chain_id)
            .bind(&surface.block_hash)
            .bind(surface.block_number)
            .bind(&surface.provenance)
            .bind(&surface.canonicality_state)
            .execute(pool)
            .await?;
        }
        for closure in &output.binding_closures {
            sqlx::query(
                "UPDATE surface_bindings
                 SET active_to = $2
                 WHERE logical_name_id = $1
                   AND chain_id = $3
                   AND authority_arm = $4
                   AND ($5::uuid IS NULL OR surface_binding_id <> $5)
                   AND (
                       block_number < $6
                       OR (
                           block_number = $6
                           AND (
                               COALESCE((provenance ->> 'transaction_index')::bigint, -1),
                               COALESCE((provenance ->> 'log_index')::bigint, -1)
                           ) < ($7, $8)
                       )
                   )
                   AND (active_to IS NULL OR active_to > $2)",
            )
            .bind(&closure.logical_name_id)
            .bind(closure.active_to)
            .bind(&closure.chain_id)
            .bind(&closure.authority_arm)
            .bind(closure.except_surface_binding_id)
            .bind(closure.block_number)
            .bind(closure.transaction_index)
            .bind(closure.log_index)
            .execute(pool)
            .await?;
        }
        for binding in &output.surface_bindings {
            sqlx::query(
                "INSERT INTO surface_bindings (
                     surface_binding_id, logical_name_id, resource_id, binding_kind,
                     authority_arm, active_from, chain_id, block_hash, block_number,
                     provenance, canonicality_state
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::canonicality_state)",
            )
            .bind(binding.surface_binding_id)
            .bind(&binding.logical_name_id)
            .bind(binding.resource_id)
            .bind(&binding.binding_kind)
            .bind(&binding.authority_arm)
            .bind(binding.active_from)
            .bind(&binding.chain_id)
            .bind(&binding.block_hash)
            .bind(binding.block_number)
            .bind(&binding.provenance)
            .bind(&binding.canonicality_state)
            .execute(pool)
            .await?;
        }
        for event in &output.normalized_events {
            sqlx::query(
                "INSERT INTO normalized_events (
                     event_identity, namespace, logical_name_id, resource_id, event_kind,
                     source_family, manifest_version, source_manifest_id, chain_id,
                     block_number, block_hash, transaction_hash, transaction_index, log_index,
                     raw_fact_ref, derivation_kind, canonicality_state, before_state,
                     after_state, migration_correlation_ids, consumer_visibility
                 ) VALUES (
                     $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                     $17::canonicality_state, $18, $19, $20, $21
                 )",
            )
            .bind(&event.event_identity)
            .bind(&event.namespace)
            .bind(&event.logical_name_id)
            .bind(event.resource_id)
            .bind(&event.event_kind)
            .bind(&event.source_family)
            .bind(event.manifest_version)
            .bind(event.source_manifest_id)
            .bind(&event.chain_id)
            .bind(event.block_number)
            .bind(&event.block_hash)
            .bind(&event.transaction_hash)
            .bind(event.transaction_index)
            .bind(event.log_index)
            .bind(&event.raw_fact_ref)
            .bind(&event.derivation_kind)
            .bind(&event.canonicality_state)
            .bind(&event.before_state)
            .bind(&event.after_state)
            .bind(&event.migration_correlation_ids)
            .bind(&event.consumer_visibility)
            .execute(pool)
            .await?;
        }
        Ok(())
    }
}

async fn selected_resource(pool: &PgPool) -> Result<Option<String>> {
    Ok(
        sqlx::query_scalar("SELECT resource_id::text FROM name_current WHERE logical_name_id = $1")
            .bind(handoff_scenario::logical_name_id())
            .fetch_one(pool)
            .await?,
    )
}

async fn handoff_control(pool: &PgPool) -> Result<serde_json::Value> {
    Ok(sqlx::query_scalar(
        "SELECT declared_summary -> 'control' FROM name_current WHERE logical_name_id = $1",
    )
    .bind(handoff_scenario::logical_name_id())
    .fetch_one(pool)
    .await?)
}

fn assert_handoff_control_keeps_the_registry_owner(control: &serde_json::Value, stage: &str) {
    assert_eq!(
        control["registry_owner"],
        json!(handoff_scenario::RETAINED_OWNER),
        "{stage}: the registry still names R after a transfer without reclaim, got {control}"
    );
    assert!(
        control.get("owner").is_none(),
        "{stage}: the exact-name control summary publishes the registry owner under registry_owner"
    );
}

/// Issue #923: a live ENSv1 name whose registry owner and registrar token holder are both R,
/// transferred R -> S and then S -> T without `reclaim`. A registrar token transfer writes no
/// registry state; after registration only `reclaim` writes the registry owner
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172-L175
/// @ ens_v1@91c966f). The three batches go through the real adapter, are persisted as
/// Interpret persists them, and Project must keep serving R as the registry owner: after the
/// handoff and after the later transfer, incrementally and rebuilt from zero. The registrant
/// follows the token to S at the handoff; whether it follows the later transfer to T is left to
/// the registration-event supplement (#911).
#[tokio::test]
async fn registrar_handoff_without_reclaim_keeps_the_registry_owner_across_a_later_transfer()
-> Result<()> {
    use handoff_scenario::{
        HANDOFF_BLOCK, LATER_BLOCK, REGISTRATION_BLOCK, RETAINED_OWNER, SECOND_HOLDER, THIRD_HOLDER,
    };
    let (registration, session) =
        handoff_scenario::interpret(handoff_scenario::registration(), None)?;
    let (handoff, session) = handoff_scenario::interpret(
        handoff_scenario::token_transfer(HANDOFF_BLOCK, RETAINED_OWNER, SECOND_HOLDER),
        Some(session),
    )?;
    let (later, _) = handoff_scenario::interpret(
        handoff_scenario::token_transfer(LATER_BLOCK, SECOND_HOLDER, THIRD_HOLDER),
        Some(session),
    )?;

    // Anti-vacuity: this is the adapter output the issue is about. The handoff selects the
    // registry-only resource with an epoch carrying R, and the later transfer emits its token
    // and permission rows but no epoch and no binding.
    let registry_resource = handoff_scenario::registry_only_resource();
    let handoff_epochs = handoff
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "AuthorityEpochChanged")
        .collect::<Vec<_>>();
    assert_eq!(handoff_epochs.len(), 1);
    assert_eq!(handoff_epochs[0].resource_id, Some(registry_resource));
    assert_eq!(
        handoff_epochs[0].after_state["registry_owner"],
        RETAINED_OWNER
    );
    assert!(
        handoff
            .normalized_events
            .iter()
            .all(|event| event.event_kind != "AuthorityTransferred"),
        "a registrar token transfer must not produce a registry ownership observation"
    );
    let later_kinds = later
        .normalized_events
        .iter()
        .map(|event| event.event_kind.as_str())
        .collect::<Vec<_>>();
    assert!(
        later_kinds.contains(&"TokenControlTransferred"),
        "{later_kinds:?}"
    );
    assert!(
        later_kinds.contains(&"PermissionChanged"),
        "{later_kinds:?}"
    );
    assert!(
        !later_kinds.contains(&"AuthorityEpochChanged"),
        "{later_kinds:?}"
    );
    assert!(later.surface_bindings.is_empty() && later.binding_closures.is_empty());

    let (database, pool) = migrated_pool().await?;
    seed_blocks(&pool, [REGISTRATION_BLOCK, HANDOFF_BLOCK, LATER_BLOCK]).await?;
    handoff_scenario::persist(&pool, &registration).await?;
    run_project(&pool, REGISTRATION_BLOCK, REGISTRATION_BLOCK, None).await?;
    let registered = handoff_control(&pool).await?;
    let registrar_resource = registration
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationGranted")
        .and_then(|event| event.resource_id)
        .expect("the registration mints the registrar resource");
    assert_eq!(
        selected_resource(&pool).await?.as_deref(),
        Some(registrar_resource.to_string().as_str())
    );
    assert_eq!(registered["registrant"], json!(RETAINED_OWNER));
    // A fresh registration serves no control owner: the registration's own registry setup is
    // not projected as a later control transfer, which the end-to-end suite pins in
    // `tests/e2e/src/scenarios/registry_driven_reads.rs`. The handoff below is what first
    // publishes a registry owner for this name.
    assert!(
        registered["registry_owner"].is_null(),
        "registration: first-ownership setup is not a control transfer, got {registered}"
    );

    // The handoff arrives as its own batch and resumes the materialized projection.
    handoff_scenario::persist(&pool, &handoff).await?;
    assert!(
        bigname_storage::resource_is_registry_control_for_registrar_lease(&pool, registry_resource)
            .await?,
        "the real registrar-transfer epoch must classify the registry-only control resource"
    );
    assert!(
        !bigname_storage::resource_is_registry_control_for_registrar_lease(
            &pool,
            registrar_resource
        )
        .await?,
        "the transferred lease must remain a registration handle"
    );
    run_project(
        &pool,
        HANDOFF_BLOCK,
        HANDOFF_BLOCK,
        Some(REGISTRATION_BLOCK),
    )
    .await?;
    assert_eq!(
        selected_resource(&pool).await?.as_deref(),
        Some(registry_resource.to_string().as_str())
    );
    let after_handoff = handoff_control(&pool).await?;
    assert_handoff_control_keeps_the_registry_owner(&after_handoff, "incremental handoff");
    assert_eq!(
        after_handoff["registrant"],
        json!(SECOND_HOLDER),
        "incremental handoff: the registrant follows the token"
    );

    handoff_scenario::persist(&pool, &later).await?;
    run_project(&pool, LATER_BLOCK, LATER_BLOCK, Some(HANDOFF_BLOCK)).await?;
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
    seed_blocks(&pool, [REGISTRATION_BLOCK, HANDOFF_BLOCK, LATER_BLOCK]).await?;
    for output in [&registration, &handoff, &later] {
        handoff_scenario::persist(&pool, output).await?;
    }
    run_project(&pool, LATER_BLOCK, REGISTRATION_BLOCK, None).await?;
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

async fn registrar_reveal_projection(
    split: bool,
) -> Result<(
    (String, i64, String, String, i64),
    Vec<(String, serde_json::Value)>,
)> {
    let (database, pool) = migrated_pool().await?;
    seed_chain(&pool).await?;
    sqlx::query(
        "INSERT INTO resources (resource_id, chain_id, block_hash, block_number,
             canonicality_state)
         VALUES ($1::uuid, $2, $3, 8, 'canonical')",
    )
    .bind(OWNERLESS_RESOURCE)
    .bind(CHAIN)
    .bind(block_hash(8))
    .execute(&pool)
    .await?;
    for (kind, index) in [("RegistrationGranted", 1), ("ExpiryChanged", 2)] {
        seed_normalized_event(
            &pool,
            &format!("fixture:surface-less-{kind}"),
            None,
            Some(OWNERLESS_RESOURCE),
            kind,
            "ens_v1_registrar_l1",
            8,
            index,
            json!({
                "source_event": "NameRegistered",
                "authority_kind": "registrar",
                "authority_key": "registrar:fixture",
                "registrant": CONTROL_OWNER,
                "expiry": 4242,
                "namehash": OWNERLESS_NAMEHASH,
            }),
            json!({}),
        )
        .await?;
    }
    if split {
        run_project(&pool, 8, 8, None).await?;
    }
    seed_surface(
        &pool,
        OWNERLESS_NAMEHASH,
        "revealed.eth",
        OWNERLESS_RESOURCE,
        OWNERLESS_BINDING,
    )
    .await?;
    seed_normalized_event(
        &pool,
        "fixture:revealed-resolver",
        Some(OWNERLESS_LOGICAL),
        Some(OWNERLESS_RESOURCE),
        "ResolverChanged",
        "ens_v1_registry_l1",
        9,
        1,
        json!({"source_event":"NewResolver","node":OWNERLESS_NAMEHASH,"resolver":RESOLVER_ADDRESS}),
        json!({"emitting_address":REGISTRY_ADDRESS}),
    )
    .await?;
    seed_normalized_event(
        &pool,
        "fixture:revealed-record",
        Some(OWNERLESS_LOGICAL),
        None,
        "RecordChanged",
        "ens_v1_resolver_l1",
        9,
        2,
        json!({
            "node": OWNERLESS_NAMEHASH,
            "record_family": "text",
            "record_key": "text:description",
            "selector_key": "description",
            "value": "revealed incrementally",
        }),
        json!({"emitting_address":RESOLVER_ADDRESS}),
    )
    .await?;
    run_project(&pool, 9, 8, split.then_some(8)).await?;
    let summary = sqlx::query_as(
        "SELECT declared_summary #>> '{registration,status}',
             (declared_summary #>> '{registration,expiry}')::bigint, resource_id::text,
             declared_summary #>> '{resolver,address}', (SELECT count(*)
         FROM record_inventory_current
         WHERE resource_id = $2::uuid)
         FROM name_current
         WHERE logical_name_id = $1",
    )
    .bind(OWNERLESS_LOGICAL)
    .bind(OWNERLESS_RESOURCE)
    .fetch_one(&pool)
    .await?;
    let snapshot = serving_projection_snapshot(&pool).await?;
    database.cleanup().await?;
    Ok((summary, snapshot))
}

#[tokio::test]
async fn registrar_only_then_enrichment_projects_name_addressable_registration() -> Result<()> {
    let (summary, _) = registrar_reveal_projection(false).await?;
    assert_eq!(
        summary,
        (
            "active".to_owned(),
            4242,
            OWNERLESS_RESOURCE.to_owned(),
            RESOLVER_ADDRESS.to_lowercase(),
            1
        )
    );
    Ok(())
}

#[tokio::test]
async fn registrar_only_then_enrichment_converges_across_project_batches() -> Result<()> {
    assert_eq!(
        registrar_reveal_projection(false).await?,
        registrar_reveal_projection(true).await?
    );
    Ok(())
}

#[tokio::test]
async fn resource_keyed_registrar_event_does_not_backfill_a_different_surface_on_shared_resource()
-> Result<()> {
    let (database, pool) = migrated_pool().await?;
    seed_chain(&pool).await?;
    seed_surface(
        &pool,
        CONTROL_NAMEHASH,
        "control.eth",
        OWNERLESS_RESOURCE,
        CONTROL_BINDING,
    )
    .await?;
    seed_normalized_event(
        &pool,
        "fixture:unrelated-resource-registration",
        None,
        Some(OWNERLESS_RESOURCE),
        "RegistrationGranted",
        "ens_v1_registrar_l1",
        8,
        1,
        json!({
            "source_event": "NameRegistered",
            "authority_kind": "registrar",
            "authority_key": "registrar:unrelated",
            "registrant": CONTROL_OWNER,
            "expiry": 4242,
            "namehash": OWNERLESS_NAMEHASH,
        }),
        json!({}),
    )
    .await?;
    run_project(&pool, 8, 8, None).await?;
    let expiry: Option<i64> = sqlx::query_scalar(
        "SELECT (declared_summary #>> '{registration,expiry}')::bigint
         FROM name_current
         WHERE logical_name_id = $1",
    )
    .bind(CONTROL_LOGICAL)
    .fetch_one(&pool)
    .await?;
    assert_eq!(expiry, None);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn later_wrapper_projection_joins_only_the_wrapped_registrar_lineage() -> Result<()> {
    const LATEST_WRAPPER_OWNER: &str = "0x7777777777777777777777777777777777777777";
    const WRAPPER_CONTRACT: &str = "0x9999999999999999999999999999999999999999";
    let (database, pool) = migrated_pool().await?;
    seed_chain(&pool).await?;
    for resource in [OLD_REGISTRAR_RESOURCE, OWNERLESS_RESOURCE] {
        sqlx::query(
            "INSERT INTO resources (resource_id, chain_id, block_hash, block_number,
                 canonicality_state)
             VALUES ($1::uuid, $2, $3, 8, 'canonical')",
        )
        .bind(resource)
        .bind(CHAIN)
        .bind(block_hash(8))
        .execute(&pool)
        .await?;
    }
    seed_surface(
        &pool,
        OWNERLESS_NAMEHASH,
        "wrapped.eth",
        CONTROL_RESOURCE,
        CONTROL_BINDING,
    )
    .await?;
    sqlx::query(
        "INSERT INTO token_lineages (token_lineage_id, chain_id, block_hash, block_number,
             canonicality_state)
         VALUES ($1::uuid, $2, $3, 8, 'canonical')",
    )
    .bind(WRAPPER_LINEAGE)
    .bind(CHAIN)
    .bind(block_hash(8))
    .execute(&pool)
    .await?;
    sqlx::query("UPDATE resources SET token_lineage_id = $1::uuid WHERE resource_id = $2::uuid")
        .bind(WRAPPER_LINEAGE)
        .bind(CONTROL_RESOURCE)
        .execute(&pool)
        .await?;
    for (identity, logical, resource, kind, family, block, log, state) in [
        (
            "fixture:old-registration",
            None,
            OLD_REGISTRAR_RESOURCE,
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            8,
            0,
            json!({
                "source_event": "NameRegistered",
                "authority_kind": "registrar",
                "authority_key": "registrar:old",
                "registrant": PRIOR_CONTROLLER,
                "expiry": 1111,
                "namehash": OWNERLESS_NAMEHASH,
            }),
        ),
        (
            "fixture:wrapped-registration",
            None,
            OWNERLESS_RESOURCE,
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            8,
            1,
            json!({
                "source_event": "NameRegistered",
                "authority_kind": "registrar",
                "authority_key": "registrar:fixture",
                "registrant": PRIOR_CONTROLLER,
                "expiry": 4242,
                "namehash": OWNERLESS_NAMEHASH,
            }),
        ),
        (
            "fixture:wrapped-expiry",
            None,
            OWNERLESS_RESOURCE,
            "ExpiryChanged",
            "ens_v1_registrar_l1",
            8,
            2,
            json!({
                "source_event": "NameRegistered",
                "authority_kind": "registrar",
                "authority_key": "registrar:fixture",
                "registrant": CONTROL_OWNER,
                "expiry": 4242,
                "namehash": OWNERLESS_NAMEHASH,
            }),
        ),
        (
            "fixture:wrapped-registrar-transfer",
            None,
            OWNERLESS_RESOURCE,
            "TokenControlTransferred",
            "ens_v1_registrar_l1",
            9,
            1,
            json!({
                "source_event": "Transfer",
                "from": PRIOR_CONTROLLER,
                "to": CONTROL_OWNER,
                "namehash": OWNERLESS_NAMEHASH,
            }),
        ),
        (
            "fixture:wrapped-binding",
            Some(OWNERLESS_LOGICAL),
            CONTROL_RESOURCE,
            "SurfaceBound",
            "ens_v1_wrapper_l1",
            9,
            3,
            json!({
                "source_event": "NameWrapped",
                "node": OWNERLESS_NAMEHASH,
                "wrapped_registrar_resource_id": OWNERLESS_RESOURCE,
            }),
        ),
        (
            "fixture:wrapped-scope",
            Some(OWNERLESS_LOGICAL),
            CONTROL_RESOURCE,
            "PermissionScopeChanged",
            "ens_v1_wrapper_l1",
            9,
            3,
            json!({
                "source_event": "NameWrapped",
                "node": OWNERLESS_NAMEHASH,
                "wrapper_state": "wrapped",
                "fuses": 0,
            }),
        ),
        (
            "fixture:wrapper-expiry",
            Some(OWNERLESS_LOGICAL),
            CONTROL_RESOURCE,
            "ExpiryChanged",
            "ens_v1_wrapper_l1",
            9,
            3,
            json!({"source_event":"NameWrapped","node":OWNERLESS_NAMEHASH,"expiry":5252}),
        ),
        (
            "fixture:wrapped-renewal",
            None,
            OWNERLESS_RESOURCE,
            "RegistrationRenewed",
            "ens_v1_registrar_l1",
            10,
            1,
            json!({
                "source_event": "NameRenewed",
                "authority_kind": "registrar",
                "registrant": CONTROL_OWNER,
                "expiry": 5252,
                "namehash": OWNERLESS_NAMEHASH,
            }),
        ),
        (
            "fixture:wrapped-renewed-expiry",
            None,
            OWNERLESS_RESOURCE,
            "ExpiryChanged",
            "ens_v1_registrar_l1",
            10,
            2,
            json!({
                "source_event": "NameRenewed",
                "authority_kind": "registrar",
                "registrant": CONTROL_OWNER,
                "expiry": 5252,
                "namehash": OWNERLESS_NAMEHASH,
            }),
        ),
    ] {
        seed_normalized_event(
            &pool,
            identity,
            logical,
            Some(resource),
            kind,
            family,
            block,
            log,
            state,
            json!({}),
        )
        .await?;
    }
    sqlx::query(
        "UPDATE normalized_events
         SET transaction_hash = (SELECT transaction_hash
         FROM normalized_events
         WHERE event_identity = 'fixture:wrapped-binding')
         WHERE event_identity = 'fixture:wrapped-registrar-transfer'",
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        "UPDATE normalized_events
         SET raw_fact_ref = jsonb_build_object('emitting_address', lower($1))
         WHERE event_identity = 'fixture:wrapped-binding'",
    )
    .bind(WRAPPER_CONTRACT)
    .execute(&pool)
    .await?;
    seed_normalized_event(
        &pool,
        "fixture:later-wrapper-transfer",
        Some(OWNERLESS_LOGICAL),
        Some(CONTROL_RESOURCE),
        "TokenControlTransferred",
        "ens_v1_wrapper_l1",
        9,
        3,
        json!({"source_event":"NameWrapped","to":PRIOR_CONTROLLER}),
        json!({}),
    )
    .await?;
    seed_normalized_event(
        &pool,
        "fixture:wrap-registrar-transfer",
        Some(OWNERLESS_LOGICAL),
        Some(OWNERLESS_RESOURCE),
        "TokenControlTransferred",
        "ens_v1_registrar_l1",
        9,
        3,
        json!({
            "source_event": "Transfer",
            "from": CONTROL_OWNER,
            "to": WRAPPER_CONTRACT,
            "namehash": OWNERLESS_NAMEHASH,
        }),
        json!({}),
    )
    .await?;
    run_project(&pool, 8, 8, None).await?;
    run_project(&pool, 10, 9, Some(8)).await?;
    let summary: (String, i64) = sqlx::query_as(
        "SELECT declared_summary #>> '{registration,status}',
             (declared_summary #>> '{registration,expiry}')::bigint
         FROM name_current
         WHERE logical_name_id = $1",
    )
    .bind(OWNERLESS_LOGICAL)
    .fetch_one(&pool)
    .await?;
    let registrants: Vec<String> = sqlx::query_scalar(
        "SELECT address
         FROM address_names_current
         WHERE logical_name_id = $1
         AND relation = 'registrant'
         ORDER BY address",
    )
    .bind(OWNERLESS_LOGICAL)
    .fetch_all(&pool)
    .await?;
    assert_eq!(summary, ("active".to_owned(), 5252));
    assert_eq!(
        registrants,
        vec![PRIOR_CONTROLLER.to_lowercase()],
        "a wrapped name serves the NameWrapped owner as registrant, as the chain records it"
    );
    seed_blocks(&pool, [11]).await?;
    seed_normalized_event(
        &pool,
        "fixture:later-wrapper-holder-transfer",
        Some(OWNERLESS_LOGICAL),
        Some(CONTROL_RESOURCE),
        "TokenControlTransferred",
        "ens_v1_wrapper_l1",
        11,
        1,
        json!({"source_event":"TransferSingle","from":PRIOR_CONTROLLER,"to":LATEST_WRAPPER_OWNER}),
        json!({}),
    )
    .await?;
    run_project(&pool, 11, 8, None).await?;
    let current_registrant: String = sqlx::query_scalar(
        "SELECT declared_summary #>> '{registration,registrant}'
         FROM name_current
         WHERE logical_name_id = $1",
    )
    .bind(OWNERLESS_LOGICAL)
    .fetch_one(&pool)
    .await?;
    let later_relations: Vec<String> = sqlx::query_scalar(
        "SELECT relation
         FROM address_names_current
         WHERE logical_name_id = $1
         AND address = lower($2)
         ORDER BY relation",
    )
    .bind(OWNERLESS_LOGICAL)
    .bind(LATEST_WRAPPER_OWNER)
    .fetch_all(&pool)
    .await?;
    assert_eq!(
        current_registrant,
        LATEST_WRAPPER_OWNER.to_lowercase(),
        "a later wrapper transfer must replace the NameWrapped owner"
    );
    assert_eq!(
        later_relations,
        vec![
            "effective_controller".to_owned(),
            "registrant".to_owned(),
            "token_holder".to_owned()
        ]
    );
    database.cleanup().await?;
    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum LaterWrapperDelta {
    HolderTransfer,
    Rewrap,
    ResolverUpdate,
    RegistrarRenewal,
    RegistrarRelease,
    RegistryUpdateAfterRelease,
}

#[derive(Debug, PartialEq)]
struct LaterWrapperProjection {
    registration_status: Option<String>,
    selected_registration_kind: Option<String>,
    expiry: Option<i64>,
    registrant: Option<String>,
    registration_resource_id: Option<String>,
    registered_at: Option<String>,
    created_at: Option<String>,
    address_registrant: Option<String>,
    registrant_event_identity: Option<String>,
    serving: Vec<(String, serde_json::Value)>,
}

type LaterWrapperRegistrationRow = (
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

async fn later_wrapper_serving_snapshot(pool: &PgPool) -> Result<Vec<(String, serde_json::Value)>> {
    let mut snapshot = serving_projection_snapshot(pool).await?;
    for (table, rows) in &mut snapshot {
        if table != "name_current" && table != "address_names_current" {
            continue;
        }
        if let Some(rows) = rows.as_array_mut() {
            rows.retain(|row| row["logical_name_id"] == OWNERLESS_LOGICAL);
        }
    }
    snapshot.retain(|(table, _)| table == "name_current" || table == "address_names_current");
    Ok(snapshot)
}

async fn project_later_wrapper_delta(
    delta: LaterWrapperDelta,
    incremental: bool,
    retract_delta: bool,
    born_wrapped: bool,
) -> Result<LaterWrapperProjection> {
    const LATEST_WRAPPER_OWNER: &str = "0x7777777777777777777777777777777777777777";
    const WRAPPER_CONTRACT: &str = "0x9999999999999999999999999999999999999999";

    let (database, pool) = migrated_pool().await?;
    seed_chain(&pool).await?;
    seed_blocks(&pool, [11]).await?;
    for resource in [OLD_REGISTRAR_RESOURCE, OWNERLESS_RESOURCE] {
        sqlx::query(
            "INSERT INTO resources (
                 resource_id, chain_id, block_hash, block_number, canonicality_state
             ) VALUES ($1::uuid, $2, $3, 8, 'canonical')",
        )
        .bind(resource)
        .bind(CHAIN)
        .bind(block_hash(8))
        .execute(&pool)
        .await?;
    }
    seed_surface(
        &pool,
        OWNERLESS_NAMEHASH,
        "wrapped-incremental.eth",
        OWNERLESS_RESOURCE,
        OWNERLESS_BINDING,
    )
    .await?;
    seed_next_binding(
        &pool,
        OWNERLESS_NAMEHASH,
        CONTROL_RESOURCE,
        CONTROL_BINDING,
        9,
        "2026-08-01T00:00:09Z",
    )
    .await?;
    seed_binding_provenance(&pool, CONTROL_BINDING, 0, 3).await?;
    sqlx::query("DELETE FROM surface_bindings WHERE surface_binding_id = $1::uuid")
        .bind(OWNERLESS_BINDING)
        .execute(&pool)
        .await?;
    sqlx::query(
        "UPDATE name_surfaces SET block_number = 9, block_hash = $2
         WHERE logical_name_id = $1",
    )
    .bind(OWNERLESS_LOGICAL)
    .bind(block_hash(9))
    .execute(&pool)
    .await?;
    sqlx::query(
        "INSERT INTO token_lineages (
             token_lineage_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1::uuid, $2, $3, 8, 'canonical')",
    )
    .bind(WRAPPER_LINEAGE)
    .bind(CHAIN)
    .bind(block_hash(8))
    .execute(&pool)
    .await?;
    sqlx::query("UPDATE resources SET token_lineage_id = $1::uuid WHERE resource_id = $2::uuid")
        .bind(WRAPPER_LINEAGE)
        .bind(CONTROL_RESOURCE)
        .execute(&pool)
        .await?;

    for (identity, resource, kind, block, log, state) in [
        (
            "fixture:incremental-old-registration",
            OLD_REGISTRAR_RESOURCE,
            "RegistrationGranted",
            // A later grant on an unrelated lineage must not supply registered_at.
            if matches!(delta, LaterWrapperDelta::RegistryUpdateAfterRelease) {
                10
            } else {
                8
            },
            0,
            json!({
                "source_event": "NameRegistered",
                "authority_kind": "registrar",
                "authority_key": "registrar:old",
                "registrant": PRIOR_CONTROLLER,
                "expiry": 1111,
                "namehash": OWNERLESS_NAMEHASH,
            }),
        ),
        (
            "fixture:incremental-registration",
            OWNERLESS_RESOURCE,
            "RegistrationGranted",
            if born_wrapped { 9 } else { 8 },
            1,
            json!({
                "source_event": "NameRegistered",
                "authority_kind": "registrar",
                "authority_key": "registrar:current",
                "registrant": if born_wrapped { WRAPPER_CONTRACT } else { CONTROL_OWNER },
                "expiry": 4242,
                "namehash": OWNERLESS_NAMEHASH,
            }),
        ),
        (
            "fixture:incremental-expiry",
            OWNERLESS_RESOURCE,
            "ExpiryChanged",
            if born_wrapped { 9 } else { 8 },
            if born_wrapped { 1 } else { 2 },
            json!({
                "source_event": "NameRegistered",
                "authority_kind": "registrar",
                "authority_key": "registrar:current",
                "registrant": if born_wrapped { WRAPPER_CONTRACT } else { CONTROL_OWNER },
                "expiry": 4242,
                "namehash": OWNERLESS_NAMEHASH,
            }),
        ),
    ] {
        seed_normalized_event(
            &pool,
            identity,
            (resource == OLD_REGISTRAR_RESOURCE
                && matches!(delta, LaterWrapperDelta::RegistryUpdateAfterRelease))
            .then_some(OWNERLESS_LOGICAL),
            Some(resource),
            kind,
            "ens_v1_registrar_l1",
            block,
            log,
            state,
            json!({}),
        )
        .await?;
    }
    for (identity, resource, kind, family, log, state, raw_fact_ref) in [
        (
            "fixture:incremental-wrap-transfer",
            OWNERLESS_RESOURCE,
            "TokenControlTransferred",
            "ens_v1_registrar_l1",
            2,
            json!({
                "source_event": "Transfer",
                "from": CONTROL_OWNER,
                "to": WRAPPER_CONTRACT,
                "namehash": OWNERLESS_NAMEHASH,
            }),
            json!({}),
        ),
        (
            "fixture:incremental-wrapper-binding",
            CONTROL_RESOURCE,
            "SurfaceBound",
            "ens_v1_wrapper_l1",
            3,
            json!({
                "source_event": "NameWrapped",
                "node": OWNERLESS_NAMEHASH,
                "wrapped_registrar_resource_id": OWNERLESS_RESOURCE,
            }),
            json!({"emitting_address":WRAPPER_CONTRACT}),
        ),
        (
            "fixture:incremental-wrapper-scope",
            CONTROL_RESOURCE,
            "PermissionScopeChanged",
            "ens_v1_wrapper_l1",
            3,
            json!({
                "source_event": "NameWrapped",
                "node": OWNERLESS_NAMEHASH,
                "wrapper_state": "wrapped",
                "fuses": 0,
            }),
            json!({}),
        ),
        (
            "fixture:incremental-wrapper-expiry",
            CONTROL_RESOURCE,
            "ExpiryChanged",
            "ens_v1_wrapper_l1",
            3,
            json!({"source_event":"NameWrapped","node":OWNERLESS_NAMEHASH,"expiry":5252}),
            json!({}),
        ),
        (
            "fixture:incremental-wrapper-holder",
            CONTROL_RESOURCE,
            "TokenControlTransferred",
            "ens_v1_wrapper_l1",
            3,
            json!({"source_event":"NameWrapped","to":PRIOR_CONTROLLER}),
            json!({}),
        ),
    ] {
        if born_wrapped && identity == "fixture:incremental-wrap-transfer" {
            continue;
        }
        seed_normalized_event(
            &pool,
            identity,
            Some(OWNERLESS_LOGICAL),
            Some(resource),
            kind,
            family,
            9,
            log,
            state,
            raw_fact_ref,
        )
        .await?;
    }
    sqlx::query(
        "UPDATE normalized_events
         SET transaction_hash = (
             SELECT transaction_hash FROM normalized_events
             WHERE event_identity = 'fixture:incremental-wrapper-binding'
         )
         WHERE event_identity = 'fixture:incremental-wrap-transfer'
            OR ($1 AND event_identity IN (
                 'fixture:incremental-registration', 'fixture:incremental-expiry'
             ))",
    )
    .bind(born_wrapped)
    .execute(&pool)
    .await?;

    let (registrar_bindings, grant_is_resource_only): (i64, bool) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM surface_bindings WHERE resource_id = $1::uuid),
                logical_name_id IS NULL FROM normalized_events
         WHERE event_identity = 'fixture:incremental-registration'",
    )
    .bind(OWNERLESS_RESOURCE)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        registrar_bindings, 0,
        "wrapping must not invent a registrar binding"
    );
    assert!(
        grant_is_resource_only,
        "the original numeric grant must remain resource-only"
    );

    if incremental {
        run_project(&pool, 9, 8, None).await?;
    }

    match delta {
        LaterWrapperDelta::HolderTransfer => {
            seed_normalized_event(
                &pool,
                "fixture:incremental-holder-transfer",
                Some(OWNERLESS_LOGICAL),
                Some(CONTROL_RESOURCE),
                "TokenControlTransferred",
                "ens_v1_wrapper_l1",
                11,
                1,
                json!({
                    "source_event": "TransferSingle",
                    "from": PRIOR_CONTROLLER,
                    "to": LATEST_WRAPPER_OWNER,
                }),
                json!({}),
            )
            .await?;
        }
        LaterWrapperDelta::Rewrap => {
            sqlx::query(
                "UPDATE surface_bindings SET active_to = '2026-08-01T00:00:10Z'
                 WHERE logical_name_id = $1 AND active_to IS NULL",
            )
            .bind(OWNERLESS_LOGICAL)
            .execute(&pool)
            .await?;
            sqlx::query(
                "INSERT INTO surface_bindings (
                     surface_binding_id, logical_name_id, resource_id, binding_kind,
                     authority_arm, active_from, chain_id, block_hash, block_number,
                     canonicality_state
                 ) VALUES (
                     $1::uuid, $2, $3::uuid, 'declared_registry_path', 'ens_v1',
                     '2026-08-01T00:00:10Z', $4, $5, 10, 'canonical'
                 )",
            )
            .bind(UNWRAPPED_BINDING)
            .bind(OWNERLESS_LOGICAL)
            .bind(OWNERLESS_RESOURCE)
            .bind(CHAIN)
            .bind(block_hash(10))
            .execute(&pool)
            .await?;
            seed_binding_provenance(&pool, UNWRAPPED_BINDING, 0, 2).await?;
            seed_normalized_event(
                &pool,
                "fixture:incremental-unwrap",
                Some(OWNERLESS_LOGICAL),
                Some(CONTROL_RESOURCE),
                "SurfaceUnbound",
                "ens_v1_wrapper_l1",
                10,
                1,
                json!({"source_event":"NameUnwrapped","node":OWNERLESS_NAMEHASH}),
                json!({}),
            )
            .await?;
            seed_normalized_event(
                &pool,
                "fixture:incremental-unwrapped-holder",
                Some(OWNERLESS_LOGICAL),
                Some(OWNERLESS_RESOURCE),
                "TokenControlTransferred",
                "ens_v1_registrar_l1",
                10,
                2,
                json!({
                    "source_event": "Transfer",
                    "from": WRAPPER_CONTRACT,
                    "to": CONTROL_OWNER,
                    "namehash": OWNERLESS_NAMEHASH,
                }),
                json!({}),
            )
            .await?;
            if incremental {
                run_project(&pool, 10, 10, Some(9)).await?;
                let unwrapped_registration: Option<String> = sqlx::query_scalar(
                    "SELECT declared_summary #>> '{registration,resource_id}'
                     FROM name_current WHERE logical_name_id = $1",
                )
                .bind(OWNERLESS_LOGICAL)
                .fetch_one(&pool)
                .await?;
                assert_eq!(
                    unwrapped_registration.as_deref(),
                    Some(OWNERLESS_RESOURCE),
                    "the registrar lease handle changed while the lifecycle was unwrapped"
                );
            }
            seed_next_binding(
                &pool,
                OWNERLESS_NAMEHASH,
                REWRAPPED_RESOURCE,
                REWRAPPED_BINDING,
                11,
                "2026-08-01T00:00:11Z",
            )
            .await?;
            seed_binding_provenance(&pool, REWRAPPED_BINDING, 0, 1).await?;
            sqlx::query(
                "INSERT INTO token_lineages (
                     token_lineage_id, chain_id, block_hash, block_number,
                     canonicality_state
                 ) VALUES ($1::uuid, $2, $3, 11, 'canonical')",
            )
            .bind(REWRAPPED_LINEAGE)
            .bind(CHAIN)
            .bind(block_hash(11))
            .execute(&pool)
            .await?;
            sqlx::query(
                "UPDATE resources SET token_lineage_id = $1::uuid
                 WHERE resource_id = $2::uuid",
            )
            .bind(REWRAPPED_LINEAGE)
            .bind(REWRAPPED_RESOURCE)
            .execute(&pool)
            .await?;
            seed_normalized_event(
                &pool,
                "fixture:incremental-rewrap-binding",
                Some(OWNERLESS_LOGICAL),
                Some(REWRAPPED_RESOURCE),
                "SurfaceBound",
                "ens_v1_wrapper_l1",
                11,
                1,
                json!({
                    "source_event": "NameWrapped",
                    "node": OWNERLESS_NAMEHASH,
                    "wrapped_registrar_resource_id": OWNERLESS_RESOURCE,
                }),
                json!({"emitting_address":WRAPPER_CONTRACT}),
            )
            .await?;
            seed_normalized_event(
                &pool,
                "fixture:incremental-rewrap-holder",
                Some(OWNERLESS_LOGICAL),
                Some(REWRAPPED_RESOURCE),
                "TokenControlTransferred",
                "ens_v1_wrapper_l1",
                11,
                2,
                json!({
                    "source_event": "TransferSingle",
                    "from": WRAPPER_CONTRACT,
                    "to": LATEST_WRAPPER_OWNER,
                }),
                json!({}),
            )
            .await?;
            seed_normalized_event(
                &pool,
                "fixture:incremental-rewrap-scope",
                Some(OWNERLESS_LOGICAL),
                Some(REWRAPPED_RESOURCE),
                "PermissionScopeChanged",
                "ens_v1_wrapper_l1",
                11,
                1,
                json!({
                    "source_event": "NameWrapped",
                    "node": OWNERLESS_NAMEHASH,
                    "wrapper_state": "wrapped",
                    "fuses": 0,
                }),
                json!({}),
            )
            .await?;
            seed_normalized_event(
                &pool,
                "fixture:incremental-rewrap-expiry",
                Some(OWNERLESS_LOGICAL),
                Some(REWRAPPED_RESOURCE),
                "ExpiryChanged",
                "ens_v1_wrapper_l1",
                11,
                1,
                json!({"source_event":"NameWrapped","node":OWNERLESS_NAMEHASH,"expiry":5252}),
                json!({}),
            )
            .await?;
        }
        LaterWrapperDelta::ResolverUpdate => {
            seed_normalized_event(
                &pool,
                "fixture:incremental-resolver-update",
                Some(OWNERLESS_LOGICAL),
                Some(CONTROL_RESOURCE),
                "ResolverChanged",
                "ens_v1_registry_l1",
                11,
                1,
                json!({
                    "source_event": "NewResolver",
                    "node": OWNERLESS_NAMEHASH,
                    "resolver": RESOLVER_ADDRESS,
                }),
                json!({"emitting_address":REGISTRY_ADDRESS}),
            )
            .await?;
        }
        LaterWrapperDelta::RegistrarRenewal => {
            for (kind, log) in [("RegistrationRenewed", 1), ("ExpiryChanged", 2)] {
                seed_normalized_event(
                    &pool,
                    &format!("fixture:incremental-renewal-{kind}"),
                    None,
                    Some(OWNERLESS_RESOURCE),
                    kind,
                    "ens_v1_registrar_l1",
                    11,
                    log,
                    json!({
                        "source_event": "NameRenewed",
                        "authority_kind": "registrar",
                        "registrant": CONTROL_OWNER,
                        "expiry": 6262,
                        "namehash": OWNERLESS_NAMEHASH,
                    }),
                    json!({}),
                )
                .await?;
            }
        }
        LaterWrapperDelta::RegistrarRelease | LaterWrapperDelta::RegistryUpdateAfterRelease => {
            seed_normalized_event(
                &pool,
                "fixture:release-wrapper-holder-transfer",
                Some(OWNERLESS_LOGICAL),
                Some(CONTROL_RESOURCE),
                "TokenControlTransferred",
                "ens_v1_wrapper_l1",
                10,
                1,
                json!({
                    "source_event": "TransferSingle",
                    "from": PRIOR_CONTROLLER,
                    "to": LATEST_WRAPPER_OWNER,
                }),
                json!({}),
            )
            .await?;
            seed_next_binding(
                &pool,
                OWNERLESS_NAMEHASH,
                RELEASE_REGISTRY_RESOURCE,
                RELEASE_REGISTRY_BINDING,
                11,
                "2026-08-01T00:00:11Z",
            )
            .await?;
            seed_binding_provenance(&pool, RELEASE_REGISTRY_BINDING, 0, 1).await?;
            seed_authority_epoch_changed(
                &pool,
                "fixture:release-registry-only-epoch",
                OWNERLESS_NAMEHASH,
                RELEASE_REGISTRY_RESOURCE,
                11,
                "registry_only",
            )
            .await?;
            seed_normalized_event(
                &pool,
                "fixture:incremental-registrar-release",
                Some(OWNERLESS_LOGICAL),
                Some(OWNERLESS_RESOURCE),
                "RegistrationReleased",
                "ens_v1_registrar_l1",
                11,
                1,
                json!({
                    "source_event": "RegistrationReleased",
                    "authority_kind": "registrar",
                    "expiry": 4242,
                    "namehash": OWNERLESS_NAMEHASH,
                }),
                json!({}),
            )
            .await?;
            sqlx::query(
                "UPDATE normalized_events
                 SET before_state = jsonb_build_object(
                     'registrant', lower($1), 'expiry', 4242
                 )
                 WHERE event_identity = 'fixture:incremental-registrar-release'",
            )
            .bind(WRAPPER_CONTRACT)
            .execute(&pool)
            .await?;
        }
    }

    if incremental && retract_delta {
        run_project(&pool, 11, 11, Some(9)).await?;
    }
    if retract_delta {
        sqlx::query(
            "UPDATE normalized_events
             SET canonicality_state = 'orphaned'
             WHERE event_identity = 'fixture:incremental-holder-transfer'",
        )
        .execute(&pool)
        .await?;
    }
    if incremental && retract_delta {
        Engine::new(pool.clone())
            .run_batch(BatchRequest {
                chain_id: CHAIN.to_owned(),
                target_block: 11,
                affected_from_block: 11,
                affected_to_block: 11,
                resume_current: Some(bigname_project::Marker {
                    number: 9,
                    hash: block_hash(9),
                }),
                mode: RunMode::Redo,
            })
            .await?;
    } else if incremental || !matches!(delta, LaterWrapperDelta::RegistryUpdateAfterRelease) {
        run_project(
            &pool,
            11,
            if incremental {
                match delta {
                    LaterWrapperDelta::RegistrarRelease
                    | LaterWrapperDelta::RegistryUpdateAfterRelease => 10,
                    _ => 11,
                }
            } else {
                8
            },
            incremental.then_some(if matches!(delta, LaterWrapperDelta::Rewrap) {
                10
            } else {
                9
            }),
        )
        .await?;
    }
    if matches!(delta, LaterWrapperDelta::RegistryUpdateAfterRelease) {
        seed_blocks(&pool, [12]).await?;
        seed_normalized_event(
            &pool,
            "fixture:registry-update-after-wrapper-release",
            Some(OWNERLESS_LOGICAL),
            Some(RELEASE_REGISTRY_RESOURCE),
            "ResolverChanged",
            "ens_v1_registry_l1",
            12,
            1,
            json!({
                "source_event": "NewResolver",
                "node": OWNERLESS_NAMEHASH,
                "resolver": RESOLVER_ADDRESS,
            }),
            json!({"emitting_address":REGISTRY_ADDRESS}),
        )
        .await?;
        run_project(
            &pool,
            12,
            if incremental { 12 } else { 8 },
            incremental.then_some(11),
        )
        .await?;
    }
    let (
        registration_status,
        selected_registration_kind,
        expiry,
        registrant,
        registration_resource_id,
        registered_at,
        created_at,
    ): LaterWrapperRegistrationRow = sqlx::query_as(
        "SELECT declared_summary #>> '{registration,status}',
                declared_summary #>> '{registration,latest_event_kind}',
                (declared_summary #>> '{registration,expiry}')::bigint,
                declared_summary #>> '{registration,registrant}',
                declared_summary #>> '{registration,resource_id}',
                declared_summary #>> '{registration,registered_at}',
                declared_summary #>> '{registration,created_at}'
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(OWNERLESS_LOGICAL)
    .fetch_one(&pool)
    .await?;
    let registrant_event_identity: Option<String> = sqlx::query_scalar(
        "SELECT event.event_identity
         FROM address_names_current relation
         JOIN normalized_events event
           ON event.normalized_event_id =
              (relation.provenance ->> 'normalized_event_id')::bigint
         WHERE relation.logical_name_id = $1
           AND relation.relation = 'registrant'",
    )
    .bind(OWNERLESS_LOGICAL)
    .fetch_optional(&pool)
    .await?;
    let address_registrant: Option<String> = sqlx::query_scalar(
        "SELECT address FROM address_names_current
         WHERE logical_name_id = $1 AND relation = 'registrant'",
    )
    .bind(OWNERLESS_LOGICAL)
    .fetch_optional(&pool)
    .await?;
    let serving = later_wrapper_serving_snapshot(&pool).await?;
    database.cleanup().await?;
    Ok(LaterWrapperProjection {
        registration_status,
        selected_registration_kind,
        expiry,
        registrant,
        registration_resource_id,
        registered_at,
        created_at,
        address_registrant,
        registrant_event_identity,
        serving,
    })
}

#[tokio::test]
async fn later_wrapper_deltas_project_identically_incrementally_and_from_zero() -> Result<()> {
    for delta in [
        LaterWrapperDelta::RegistrarRelease,
        LaterWrapperDelta::HolderTransfer,
        LaterWrapperDelta::ResolverUpdate,
        LaterWrapperDelta::RegistrarRenewal,
    ] {
        let incremental = project_later_wrapper_delta(delta, true, false, false).await?;
        let from_zero = project_later_wrapper_delta(delta, false, false, false).await?;
        assert_eq!(
            incremental.expiry, from_zero.expiry,
            "{delta:?} produced a different registrar expiry incrementally"
        );
        assert_eq!(
            incremental.registrant, from_zero.registrant,
            "{delta:?} selected a different registrant incrementally"
        );
        assert_eq!(
            incremental.registrant_event_identity, from_zero.registrant_event_identity,
            "{delta:?} selected different registration-event input incrementally"
        );
        // A wrapper-scoped batch stages the wrapped registrar's rows too, so created_at is the
        // same whether the name is projected batch by batch or rebuilt from zero.
        assert!(
            incremental.created_at.is_some(),
            "{delta:?} served no created_at"
        );
        assert_eq!(
            incremental.created_at, from_zero.created_at,
            "{delta:?} produced a different created_at incrementally"
        );
        if matches!(delta, LaterWrapperDelta::RegistrarRelease) {
            assert_eq!(
                incremental.registration_status.as_deref(),
                Some("released"),
                "the registrar release left the wrapped lease active"
            );
            assert_eq!(
                incremental.selected_registration_kind.as_deref(),
                Some("RegistrationReleased"),
                "the registrar release did not become the selected registration lifecycle row"
            );
            assert_eq!(incremental.expiry, Some(4242));
            assert_eq!(
                incremental.registrant.as_deref(),
                Some("0x7777777777777777777777777777777777777777"),
                "the registrar release replaced the last wrapper holder with NameWrapper custody"
            );
            // After the release the name is bound to a registry-only resource, which lists
            // nobody under the registrant address relation.
            assert_eq!(incremental.registrant_event_identity, None);
        }
        assert_eq!(
            incremental.registration_resource_id.as_deref(),
            Some(OWNERLESS_RESOURCE),
            "{delta:?} did not retain the wrapped registrar lifecycle handle"
        );
        assert_eq!(
            incremental.serving, from_zero.serving,
            "{delta:?} diverged elsewhere between incremental projection and a from-zero rebuild"
        );
        if !matches!(delta, LaterWrapperDelta::RegistrarRelease) {
            assert_eq!(
                incremental.expiry,
                Some(match delta {
                    LaterWrapperDelta::RegistrarRenewal => 6262,
                    _ => 4242,
                })
            );
        }
        // Served state follows the chain: the NameWrapped owner is the registrant until a
        // later NameWrapper transfer names another holder.
        let expected_registrant = match delta {
            LaterWrapperDelta::RegistrarRelease | LaterWrapperDelta::HolderTransfer => {
                "0x7777777777777777777777777777777777777777".to_owned()
            }
            _ => PRIOR_CONTROLLER.to_lowercase(),
        };
        assert_eq!(
            incremental.registrant.as_deref(),
            Some(expected_registrant.as_str()),
            "{delta:?} did not serve the current NameWrapper holder as registrant"
        );
        if matches!(delta, LaterWrapperDelta::RegistrarRelease) {
            assert_eq!(
                incremental.address_registrant, None,
                "a registry-only binding lists nobody under relation=registrant"
            );
        } else {
            assert_eq!(
                incremental.registrant, incremental.address_registrant,
                "{delta:?} made name_current disagree with the address-name registrant fold"
            );
        }
        assert_ne!(
            incremental.registrant_event_identity.as_deref(),
            Some("fixture:incremental-old-registration"),
            "the selected registration rows admitted an older same-label registrar lineage"
        );
    }
    Ok(())
}

/// A name registered through the NameWrapper is identified by its BaseRegistrar lease, the same
/// as a name wrapped later; a re-wrap mints a new NameWrapper resource and changes nothing.
#[tokio::test]
async fn rewrap_of_a_name_wrapped_at_registration_keeps_the_registrar_lease_identity() -> Result<()>
{
    let incremental =
        project_later_wrapper_delta(LaterWrapperDelta::Rewrap, true, false, true).await?;
    let from_zero =
        project_later_wrapper_delta(LaterWrapperDelta::Rewrap, false, false, true).await?;
    assert_eq!(
        incremental, from_zero,
        "re-wrap projection diverged between an incremental batch and from-zero rebuild"
    );
    assert_eq!(
        incremental.registration_resource_id.as_deref(),
        Some(OWNERLESS_RESOURCE),
        "a name wrapped at registration must be identified by its registrar lease, not a \
         NameWrapper resource"
    );
    assert_eq!(
        incremental.registrant.as_deref(),
        Some("0x7777777777777777777777777777777777777777")
    );
    assert_eq!(incremental.registrant, incremental.address_registrant);
    Ok(())
}

#[tokio::test]
async fn release_of_a_name_wrapped_at_registration_keeps_the_registrar_lease_identity() -> Result<()>
{
    let incremental =
        project_later_wrapper_delta(LaterWrapperDelta::RegistrarRelease, true, false, true).await?;
    let from_zero =
        project_later_wrapper_delta(LaterWrapperDelta::RegistrarRelease, false, false, true)
            .await?;
    assert_eq!(incremental, from_zero);
    assert_eq!(incremental.registration_status.as_deref(), Some("released"));
    assert_eq!(
        incremental.registration_resource_id.as_deref(),
        Some(OWNERLESS_RESOURCE)
    );
    assert_eq!(
        incremental.address_registrant, None,
        "a registry-only binding lists nobody under relation=registrant"
    );
    Ok(())
}

#[tokio::test]
async fn registry_update_after_wrapper_release_preserves_registration_history() -> Result<()> {
    for born_wrapped in [false, true] {
        let incremental = project_later_wrapper_delta(
            LaterWrapperDelta::RegistryUpdateAfterRelease,
            true,
            false,
            born_wrapped,
        )
        .await?;
        let from_zero = project_later_wrapper_delta(
            LaterWrapperDelta::RegistryUpdateAfterRelease,
            false,
            false,
            born_wrapped,
        )
        .await?;
        assert_eq!(
            incremental, from_zero,
            "post-release registry update diverged"
        );
        assert_eq!(incremental.registration_status.as_deref(), Some("released"));
        assert_eq!(
            incremental.registration_resource_id.as_deref(),
            Some(OWNERLESS_RESOURCE)
        );
        // Born-wrapped registration starts in the wrapping block; later wrapping
        // retains the earlier start. The unrelated block-10 grant cannot win.
        assert_eq!(
            incremental.registered_at.as_deref(),
            Some(if born_wrapped {
                "2026-08-01T00:00:09+00:00"
            } else {
                "2026-08-01T00:00:08+00:00"
            })
        );
        assert_eq!(
            incremental.registrant.as_deref(),
            Some("0x7777777777777777777777777777777777777777")
        );
        assert_eq!(
            incremental.address_registrant, None,
            "a registry-only binding lists nobody under relation=registrant"
        );
        assert_ne!(
            incremental.registrant_event_identity.as_deref(),
            Some("fixture:incremental-old-registration"),
            "an unrelated registrar lineage was admitted"
        );
    }
    Ok(())
}

#[tokio::test]
async fn later_wrapper_retraction_projects_identically_incrementally_and_from_zero() -> Result<()> {
    let incremental =
        project_later_wrapper_delta(LaterWrapperDelta::HolderTransfer, true, true, false).await?;
    let from_zero =
        project_later_wrapper_delta(LaterWrapperDelta::HolderTransfer, false, true, false).await?;
    assert_eq!(incremental, from_zero);
    assert_eq!(incremental.expiry, Some(4242));
    // With the holder transfer retracted, the NameWrapped owner is the registrant again.
    assert_eq!(
        incremental.registrant.as_deref(),
        Some(PRIOR_CONTROLLER.to_lowercase().as_str())
    );
    Ok(())
}

type EnrichedRegistryOnlyRow = (
    Option<i64>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    serde_json::Value,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

/// The state-derived release of the earlier lease of the name, carrying the name.
async fn seed_earlier_lease_release(
    pool: &PgPool,
    block_number: i64,
    log_index: i64,
) -> Result<()> {
    seed_normalized_event(
        pool,
        "fixture:enriched-earlier-lease-release",
        Some(OWNERLESS_LOGICAL),
        Some(EARLIER_LEASE_RESOURCE),
        "RegistrationReleased",
        "ens_v1_registrar_l1",
        block_number,
        log_index,
        json!({
            "source_event": "RegistrationReleased",
            "released_at": 1_600_000_000,
            "expiry": 1_592_224_000,
            "namehash": OWNERLESS_NAMEHASH,
        }),
        json!({}),
    )
    .await?;
    // Match the release producer: the ended lease carries its former holder.
    let updated = sqlx::query("UPDATE normalized_events SET before_state = $1 WHERE event_identity = 'fixture:enriched-earlier-lease-release'")
        .bind(json!({"registrant": "0x00000000000000000000000000000000000000aa"}))
        .execute(pool)
        .await?;
    assert_eq!(updated.rows_affected(), 1);
    Ok(())
}

/// Seeds the successor lease's batch at `block` when `stage` reaches it: the grant at block 12,
/// the renewal at block 13, the release at block 14. With `transferred` the block-13 batch
/// starts with the successor token's transfer to a later holder, again without `reclaim`, ahead
/// of the renewal when the stage reaches that. Every row carries the name, as the adapter names
/// them when the surface is known. Returns whether the block was seeded.
async fn seed_successor_lease_batch(
    pool: &PgPool,
    block: i64,
    stage: SuccessorStage,
    reclaimed: bool,
    transferred: bool,
) -> Result<bool> {
    let reached = match block {
        12 => true,
        13 => stage >= SuccessorStage::Renewed || transferred,
        14 => stage >= SuccessorStage::Released,
        _ => false,
    };
    if !reached {
        return Ok(false);
    }
    seed_blocks(pool, [block]).await?;
    let authority_key = format!(
        "registrar:{CHAIN}:ens_v1_registrar_l1:0x{:064x}:{}:2",
        1_u64,
        block_hash(12)
    );
    match block {
        12 if reclaimed => {
            // `register` writes the registry owner in the grant's transaction, so the successor
            // lease's resource takes over the name: a binding of its own, the registry owner it
            // wrote, and its registrar epoch.
            // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L148-L150 @ ens_v1@91c966f)
            seed_next_binding(
                pool,
                OWNERLESS_NAMEHASH,
                SUCCESSOR_LEASE_RESOURCE,
                SUCCESSOR_LEASE_BINDING,
                12,
                "2026-08-01T00:00:12Z",
            )
            .await?;
            seed_binding_provenance(pool, SUCCESSOR_LEASE_BINDING, 0, 2).await?;
            seed_authority_transferred(
                pool,
                "fixture:enriched-successor-registry-owner",
                OWNERLESS_NAMEHASH,
                SUCCESSOR_LEASE_RESOURCE,
                12,
                4,
                json!({
                    "node": OWNERLESS_NAMEHASH,
                    "owner": SUCCESSOR_OWNER,
                    "owner_getter": SUCCESSOR_OWNER,
                    "authority_kind": "registrar",
                }),
            )
            .await?;
            seed_authority_epoch_changed(
                pool,
                "fixture:enriched-successor-epoch",
                OWNERLESS_NAMEHASH,
                SUCCESSOR_LEASE_RESOURCE,
                12,
                "registrar",
            )
            .await?;
        }
        12 => {
            // `registerOnly` mints the token and writes the expiry without touching the
            // registry: a new registrar resource, no binding, no registry row.
            // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L118-L152 @ ens_v1@91c966f)
            sqlx::query(
                "INSERT INTO resources (
                     resource_id, chain_id, block_hash, block_number, canonicality_state
                 ) VALUES ($1::uuid, $2, $3, 12, 'canonical')",
            )
            .bind(SUCCESSOR_LEASE_RESOURCE)
            .bind(CHAIN)
            .bind(block_hash(12))
            .execute(pool)
            .await?;
        }
        _ => {}
    }
    let rows: Vec<(&str, i64, serde_json::Value)> = match block {
        12 => [("RegistrationGranted", 2), ("ExpiryChanged", 3)]
            .into_iter()
            .map(|(kind, log)| {
                (
                    kind,
                    log,
                    json!({
                        "source_event": "NameRegistered",
                        "namehash": OWNERLESS_NAMEHASH,
                        "labelhash": format!("0x{:064x}", 1_u64),
                        "token_id": format!("0x{:064x}", 1_u64),
                        "registrant": SUCCESSOR_OWNER,
                        "authority_owner": SUCCESSOR_OWNER,
                        "expiry": SUCCESSOR_EXPIRY,
                        "surface_known": true,
                        "authority_kind": "registrar",
                        "authority_key": authority_key,
                        "registration_window": "whole_transaction",
                    }),
                )
            })
            .collect(),
        13 => {
            let holder = if transferred {
                LATER_HOLDER
            } else {
                SUCCESSOR_OWNER
            };
            let transfer = transferred.then(|| {
                (
                    "TokenControlTransferred",
                    0,
                    json!({
                        "source_event": "Transfer",
                        "authority_kind": "registrar",
                        "from": SUCCESSOR_OWNER,
                        "to": LATER_HOLDER,
                        "namehash": OWNERLESS_NAMEHASH,
                    }),
                )
            });
            let renewal = (stage >= SuccessorStage::Renewed)
                .then_some([("RegistrationRenewed", 1), ("ExpiryChanged", 2)])
                .into_iter()
                .flatten()
                .map(|(kind, log)| {
                    (
                        kind,
                        log,
                        json!({
                            "source_event": "NameRenewed",
                            "namehash": OWNERLESS_NAMEHASH,
                            "labelhash": format!("0x{:064x}", 1_u64),
                            "registrant": holder,
                            "expiry": SUCCESSOR_RENEWED_EXPIRY,
                            "surface_known": true,
                            "authority_kind": "registrar",
                            "authority_key": authority_key.clone(),
                        }),
                    )
                });
            transfer.into_iter().chain(renewal).collect()
        }
        _ => vec![(
            "RegistrationReleased",
            1,
            json!({
                "source_event": "RegistrationReleased",
                "released_at": SUCCESSOR_RELEASED_AT,
                "expiry": SUCCESSOR_RENEWED_EXPIRY,
                "namehash": OWNERLESS_NAMEHASH,
            }),
        )],
    };
    for (kind, log, after_state) in rows {
        seed_normalized_event(
            pool,
            &format!("fixture:enriched-successor-{block}-{kind}"),
            Some(OWNERLESS_LOGICAL),
            Some(SUCCESSOR_LEASE_RESOURCE),
            kind,
            "ens_v1_registrar_l1",
            block,
            log,
            after_state,
            json!({}),
        )
        .await?;
    }
    Ok(true)
}

#[derive(Debug, PartialEq)]
struct EnrichedRegistryOnlyProjection {
    expiry: Option<i64>,
    registered_at: Option<String>,
    serving: Vec<(String, serde_json::Value)>,
    registration_resource_id: Option<String>,
    registrant: Option<String>,
    /// The identity of the event `provenance.registrant_event_id` names.
    registrant_event_identity: Option<String>,
    address_registrant: Option<String>,
    /// The selected binding and its resource, as `name_current` serves them.
    selected_binding: (Option<String>, Option<String>),
    /// `declared_summary.control` without its `expiry`, which repeats the lease expiry.
    control: serde_json::Value,
    registration_status: Option<String>,
    authority_kind: Option<String>,
    released_at: Option<String>,
    /// The resolver `name_current` serves for the name, if any.
    resolver: Option<String>,
    /// `released_tombstone` from the selection's resource authority context.
    released_tombstone: Option<String>,
    /// Every `(address, relation)` row the address listing holds for the name.
    address_relations: Vec<(String, String)>,
}

async fn project_enriched_registry_only(
    controller_registered: bool,
    incremental: bool,
) -> Result<EnrichedRegistryOnlyProjection> {
    project_enriched_registry_only_batches(controller_registered, incremental, None).await
}

/// A batch at block 11, after the registrar resource's binding has closed.
#[derive(Clone, Copy)]
enum LaterBatch {
    /// Touches only the registry-only resource.
    RegistryOwner,
    /// Touches only the registrar resource, with a renewal that carries no name.
    RenewalWithoutName,
    /// The same renewal, followed in the block by later and longer renewals of two other leases
    /// that carry this name: an earlier lease whose binding to the name closed before the
    /// retained lease was granted, and a registrar resource the name was never bound to.
    RenewalAndOtherLeases,
    /// The retained lease lapses past grace: the registrar resource's state-derived release.
    ReleaseOfRetainedLease,
    /// An earlier lease of the name is released. Before the re-registration it is released in
    /// the on-chain order, ahead of the retained lease's grant, and the block-11 batch renews
    /// the retained lease; otherwise its release is observed at block 11, after the handoff.
    ReleaseOfAnEarlierLease { before_reregistration: bool },
    /// The retained lease lapses at block 11, and at block 12 a controller grants the name
    /// again to a new owner. With `reclaimed` false the grant is `registerOnly`: the registrar
    /// writes the expiry and mints the token but does not touch the registry, so the
    /// registry-only binding stays the name's only open one and the successor lease has no
    /// binding of its own. With `reclaimed` true the grant is `register`: the registry write
    /// opens a binding on the successor lease's resource. Later stages renew the successor lease
    /// at block 13 and release it at block 14, each in a batch of its own.
    /// With `transferred` the successor token changes hands again at block 13, without
    /// `reclaim`, ahead of the renewal in that batch when the stage reaches it.
    /// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L118-L152 @ ens_v1@91c966f)
    SuccessorLease {
        stage: SuccessorStage,
        reclaimed: bool,
        transferred: bool,
    },
    /// The retained lease's token is transferred again at block 11, without `reclaim`, to a
    /// holder who is neither the registry owner nor the holder the handoff left it with. At
    /// `Renewed` the same batch renews the lease after the transfer; at `Released` a block-12
    /// batch releases it.
    TransferOfRetainedLease { stage: TransferStage },
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
enum SuccessorStage {
    Granted,
    Renewed,
    Released,
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
enum TransferStage {
    Transferred,
    Renewed,
    Released,
}

const EARLIER_LEASE_RESOURCE: &str = "60000000-0000-0000-0000-000000000001";
const EARLIER_LEASE_BINDING: &str = "60000000-0000-0000-0000-000000000011";
const UNBOUND_LEASE_RESOURCE: &str = "60000000-0000-0000-0000-000000000002";
const SUCCESSOR_LEASE_RESOURCE: &str = "60000000-0000-0000-0000-000000000003";
const SUCCESSOR_LEASE_BINDING: &str = "60000000-0000-0000-0000-000000000013";
/// The successor lease's registrar owner: neither the registry owner the handoff left behind nor
/// the retained lease's last holder.
const SUCCESSOR_OWNER: &str = "0x7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a";
/// The holder a lease's token is transferred to under the registry-only binding, again without
/// `reclaim`: neither the registry owner nor any earlier holder.
const LATER_HOLDER: &str = "0x7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c";
const SUCCESSOR_EXPIRY: i64 = 1_700_020_000;
const SUCCESSOR_RENEWED_EXPIRY: i64 = 1_700_030_000;
const SUCCESSOR_RELEASED_AT: i64 = SUCCESSOR_RENEWED_EXPIRY + 7_776_001;
/// When the retained lease, renewed once after its second transfer, lapses past grace.
const TRANSFERRED_RELEASED_AT: i64 = 1_700_002_100 + 7_776_001;

async fn project_enriched_registry_only_batches(
    controller_registered: bool,
    incremental: bool,
    later_batch: Option<LaterBatch>,
) -> Result<EnrichedRegistryOnlyProjection> {
    const ALICE: &str = "0x5555555555555555555555555555555555555555";
    const BOB: &str = "0x6666666666666666666666666666666666666666";
    const REGISTRY_RESOURCE: &str = "edededed-eded-eded-eded-edededededed";
    const REGISTRY_BINDING: &str = "efefefef-efef-efef-efef-efefefefefef";
    const EXPIRY: i64 = 1_700_001_100;
    let (database, pool) = migrated_pool().await?;
    seed_chain(&pool).await?;
    seed_surface(
        &pool,
        OWNERLESS_NAMEHASH,
        "enriched-later.eth",
        OWNERLESS_RESOURCE,
        OWNERLESS_BINDING,
    )
    .await?;
    if !controller_registered {
        sqlx::query(
            "UPDATE surface_bindings
             SET block_number = 9, block_hash = $2, active_from = '2026-08-01T00:00:09Z'
             WHERE surface_binding_id = $1::uuid",
        )
        .bind(OWNERLESS_BINDING)
        .bind(block_hash(9))
        .execute(&pool)
        .await?;
        seed_binding_provenance(&pool, OWNERLESS_BINDING, 0, 1).await?;
    }
    if matches!(
        later_batch,
        Some(LaterBatch::RenewalAndOtherLeases | LaterBatch::ReleaseOfAnEarlierLease { .. })
    ) {
        for resource in [EARLIER_LEASE_RESOURCE, UNBOUND_LEASE_RESOURCE] {
            sqlx::query(
                "INSERT INTO resources (
                     resource_id, chain_id, block_hash, block_number, canonicality_state
                 ) VALUES ($1::uuid, $2, $3, 8, 'canonical')",
            )
            .bind(resource)
            .bind(CHAIN)
            .bind(block_hash(8))
            .execute(&pool)
            .await?;
        }
        sqlx::query(
            "INSERT INTO surface_bindings (
                 surface_binding_id, logical_name_id, resource_id, binding_kind,
                 authority_arm, active_from, active_to, chain_id, block_hash, block_number,
                 canonicality_state
             ) VALUES (
                 $1::uuid, $2, $3::uuid, 'declared_registry_path', 'ens_v1',
                 '2026-06-01T00:00:00Z', '2026-06-02T00:00:00Z', $4, $5, 8, 'canonical'
             )",
        )
        .bind(EARLIER_LEASE_BINDING)
        .bind(OWNERLESS_LOGICAL)
        .bind(EARLIER_LEASE_RESOURCE)
        .bind(CHAIN)
        .bind(block_hash(8))
        .execute(&pool)
        .await?;
        seed_binding_provenance(&pool, EARLIER_LEASE_BINDING, 0, 0).await?;
    }
    if matches!(
        later_batch,
        Some(LaterBatch::ReleaseOfAnEarlierLease {
            before_reregistration: true
        })
    ) {
        seed_earlier_lease_release(&pool, 8, 0).await?;
    }
    for (identity, kind, block, log, expiry) in [
        (
            "fixture:enriched-grant",
            "RegistrationGranted",
            8,
            1,
            1_700_000_100,
        ),
        (
            "fixture:enriched-initial-expiry",
            "ExpiryChanged",
            8,
            2,
            1_700_000_100,
        ),
        (
            "fixture:enriched-renewal",
            "RegistrationRenewed",
            9,
            0,
            EXPIRY,
        ),
        (
            "fixture:enriched-renewal-expiry",
            "ExpiryChanged",
            9,
            0,
            EXPIRY,
        ),
    ] {
        seed_normalized_event(
            &pool,
            identity,
            controller_registered.then_some(OWNERLESS_LOGICAL),
            Some(OWNERLESS_RESOURCE),
            kind,
            "ens_v1_registrar_l1",
            block,
            log,
            json!({
                "source_event": if block == 8 { "NameRegistered" } else { "NameRenewed" },
                "authority_kind": "registrar",
                "registrant": ALICE,
                "expiry": expiry,
                "namehash": OWNERLESS_NAMEHASH,
            }),
            json!({}),
        )
        .await?;
    }
    if incremental {
        run_project(&pool, 9, 8, None).await?;
    }
    seed_next_binding(
        &pool,
        OWNERLESS_NAMEHASH,
        REGISTRY_RESOURCE,
        REGISTRY_BINDING,
        10,
        "2026-08-01T00:00:10Z",
    )
    .await?;
    seed_binding_provenance(&pool, REGISTRY_BINDING, 0, 0).await?;
    seed_authority_epoch_changed(
        &pool,
        "fixture:enriched-registry-only-epoch",
        OWNERLESS_NAMEHASH,
        REGISTRY_RESOURCE,
        10,
        "registry_only",
    )
    .await?;
    // The registry owner the transfer left behind: the registrar wrote it when registering and a
    // token transfer without `reclaim` does not touch it.
    // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L148-L150 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172-L175 @ ens_v1@91c966f)
    seed_authority_transferred(
        &pool,
        "fixture:enriched-registry-only-owner",
        OWNERLESS_NAMEHASH,
        REGISTRY_RESOURCE,
        10,
        8,
        json!({
            "node": OWNERLESS_NAMEHASH,
            "owner": ALICE,
            "owner_getter": ALICE,
            "authority_kind": "registry_only",
        }),
    )
    .await?;
    seed_normalized_event(
        &pool,
        "fixture:enriched-registry-only-resolver",
        Some(OWNERLESS_LOGICAL),
        Some(REGISTRY_RESOURCE),
        "ResolverChanged",
        "ens_v1_registry_l1",
        10,
        2,
        json!({
            "source_event": "NewResolver",
            "node": OWNERLESS_NAMEHASH,
            "resolver": RESOLVER_ADDRESS,
        }),
        json!({"emitting_address": REGISTRY_ADDRESS}),
    )
    .await?;
    seed_normalized_event(
        &pool,
        "fixture:enriched-unreclaimed-transfer",
        Some(OWNERLESS_LOGICAL),
        Some(OWNERLESS_RESOURCE),
        "TokenControlTransferred",
        "ens_v1_registrar_l1",
        10,
        0,
        json!({
            "source_event": "Transfer",
            "authority_kind": "registrar",
            "from": ALICE,
            "to": BOB,
            "namehash": OWNERLESS_NAMEHASH,
        }),
        json!({}),
    )
    .await?;
    if incremental || later_batch.is_none() {
        run_project(
            &pool,
            10,
            if incremental { 10 } else { 8 },
            incremental.then_some(9),
        )
        .await?;
    }
    if let Some(later_batch) = later_batch {
        seed_blocks(&pool, [11]).await?;
        match later_batch {
            LaterBatch::RegistryOwner => {
                seed_authority_transferred(
                    &pool,
                    "fixture:enriched-later-registry-owner",
                    OWNERLESS_NAMEHASH,
                    REGISTRY_RESOURCE,
                    11,
                    1,
                    json!({
                        "node": OWNERLESS_NAMEHASH,
                        "owner": ALICE,
                        "owner_getter": ALICE,
                        "authority_kind": "registry_only",
                    }),
                )
                .await?;
            }
            LaterBatch::ReleaseOfRetainedLease | LaterBatch::SuccessorLease { .. } => {
                seed_normalized_event(
                    &pool,
                    "fixture:enriched-later-release",
                    None,
                    Some(OWNERLESS_RESOURCE),
                    "RegistrationReleased",
                    "ens_v1_registrar_l1",
                    11,
                    1,
                    json!({
                        "source_event": "RegistrationReleased",
                        "released_at": 1_700_001_100 + 7_776_001,
                        "expiry": EXPIRY,
                        "namehash": OWNERLESS_NAMEHASH,
                    }),
                    json!({}),
                )
                .await?;
            }
            LaterBatch::ReleaseOfAnEarlierLease {
                before_reregistration: false,
            } => {
                seed_earlier_lease_release(&pool, 11, 3).await?;
            }
            LaterBatch::TransferOfRetainedLease { stage } => {
                // The adapter names the transfer: the surface is known by now.
                seed_normalized_event(
                    &pool,
                    "fixture:enriched-retained-transfer",
                    Some(OWNERLESS_LOGICAL),
                    Some(OWNERLESS_RESOURCE),
                    "TokenControlTransferred",
                    "ens_v1_registrar_l1",
                    11,
                    1,
                    json!({
                        "source_event": "Transfer",
                        "authority_kind": "registrar",
                        "from": BOB,
                        "to": LATER_HOLDER,
                        "namehash": OWNERLESS_NAMEHASH,
                    }),
                    json!({}),
                )
                .await?;
                if stage >= TransferStage::Renewed {
                    for (kind, log) in [("RegistrationRenewed", 2), ("ExpiryChanged", 3)] {
                        seed_normalized_event(
                            &pool,
                            &format!("fixture:enriched-later-{kind}"),
                            None,
                            Some(OWNERLESS_RESOURCE),
                            kind,
                            "ens_v1_registrar_l1",
                            11,
                            log,
                            json!({
                                "source_event": "NameRenewed",
                                "authority_kind": "registrar",
                                "registrant": LATER_HOLDER,
                                "expiry": EXPIRY + 1_000,
                                "namehash": OWNERLESS_NAMEHASH,
                            }),
                            json!({}),
                        )
                        .await?;
                    }
                }
            }
            LaterBatch::RenewalWithoutName
            | LaterBatch::RenewalAndOtherLeases
            | LaterBatch::ReleaseOfAnEarlierLease {
                before_reregistration: true,
            } => {
                for (kind, log) in [("RegistrationRenewed", 1), ("ExpiryChanged", 2)] {
                    seed_normalized_event(
                        &pool,
                        &format!("fixture:enriched-later-{kind}"),
                        None,
                        Some(OWNERLESS_RESOURCE),
                        kind,
                        "ens_v1_registrar_l1",
                        11,
                        log,
                        json!({
                            "source_event": "NameRenewed",
                            "authority_kind": "registrar",
                            "registrant": BOB,
                            "expiry": EXPIRY + 1_000,
                            "namehash": OWNERLESS_NAMEHASH,
                        }),
                        json!({}),
                    )
                    .await?;
                }
                if matches!(later_batch, LaterBatch::RenewalAndOtherLeases) {
                    for (resource, kind, log, expiry) in [
                        (
                            EARLIER_LEASE_RESOURCE,
                            "RegistrationRenewed",
                            3,
                            EXPIRY + 5_000,
                        ),
                        (EARLIER_LEASE_RESOURCE, "ExpiryChanged", 4, EXPIRY + 5_000),
                        (
                            UNBOUND_LEASE_RESOURCE,
                            "RegistrationRenewed",
                            5,
                            EXPIRY + 9_000,
                        ),
                        (UNBOUND_LEASE_RESOURCE, "ExpiryChanged", 6, EXPIRY + 9_000),
                    ] {
                        seed_normalized_event(
                            &pool,
                            &format!("fixture:enriched-other-lease-{log}"),
                            Some(OWNERLESS_LOGICAL),
                            Some(resource),
                            kind,
                            "ens_v1_registrar_l1",
                            11,
                            log,
                            json!({
                                "source_event": "NameRenewed",
                                "authority_kind": "registrar",
                                "registrant": BOB,
                                "expiry": expiry,
                                "namehash": OWNERLESS_NAMEHASH,
                            }),
                            json!({}),
                        )
                        .await?;
                    }
                }
            }
        }
        let mut target_block = 11;
        if incremental {
            run_project(&pool, 11, 11, Some(10)).await?;
        }
        if let LaterBatch::TransferOfRetainedLease {
            stage: TransferStage::Released,
        } = later_batch
        {
            seed_blocks(&pool, [12]).await?;
            seed_normalized_event(
                &pool,
                "fixture:enriched-transferred-release",
                None,
                Some(OWNERLESS_RESOURCE),
                "RegistrationReleased",
                "ens_v1_registrar_l1",
                12,
                1,
                json!({
                    "source_event": "RegistrationReleased",
                    "released_at": TRANSFERRED_RELEASED_AT,
                    "expiry": EXPIRY + 1_000,
                    "namehash": OWNERLESS_NAMEHASH,
                }),
                json!({}),
            )
            .await?;
            target_block = 12;
            if incremental {
                run_project(&pool, 12, 12, Some(11)).await?;
            }
        }
        if let LaterBatch::SuccessorLease {
            stage,
            reclaimed,
            transferred,
        } = later_batch
        {
            for block in [12, 13, 14] {
                let seeded =
                    seed_successor_lease_batch(&pool, block, stage, reclaimed, transferred).await?;
                if !seeded {
                    break;
                }
                target_block = block;
                if incremental {
                    run_project(&pool, block, block, Some(block - 1)).await?;
                }
            }
        }
        if !incremental {
            run_project(&pool, target_block, 8, None).await?;
        }
    }
    let (
        expiry,
        registered_at,
        registration_resource_id,
        registrant,
        registrant_event_identity,
        selected_binding_id,
        selected_resource_id,
        control,
        registration_status,
        authority_kind,
        released_at,
        resolver,
        released_tombstone,
    ): EnrichedRegistryOnlyRow = sqlx::query_as(
        "SELECT (declared_summary #>> '{registration,expiry}')::bigint,
             declared_summary #>> '{registration,registered_at}',
             declared_summary #>> '{registration,resource_id}',
             declared_summary #>> '{registration,registrant}',
             (SELECT event.event_identity FROM normalized_events event
              WHERE event.normalized_event_id =
                    (name_current.provenance ->> 'registrant_event_id')::bigint),
             surface_binding_id::text, resource_id::text,
             (declared_summary -> 'control') - 'expiry',
             declared_summary #>> '{registration,status}',
             declared_summary #>> '{registration,authority_kind}',
             declared_summary #>> '{registration,released_at}',
             declared_summary #>> '{resolver,address}',
             provenance #>> '{authority_selection,resource_authority_context,released_tombstone}'
         FROM name_current
         WHERE logical_name_id = $1",
    )
    .bind(OWNERLESS_LOGICAL)
    .fetch_one(&pool)
    .await?;
    if matches!(later_batch, Some(LaterBatch::ReleaseOfRetainedLease)) {
        let lapsed_authority: Option<String> = sqlx::query_scalar(
            "SELECT declared_summary #>> '{registration,lapsed_registration,authority_kind}' FROM name_current WHERE logical_name_id = $1",
        ).bind(OWNERLESS_LOGICAL).fetch_one(&pool).await?;
        assert_eq!(
            lapsed_authority.as_deref(),
            Some("registrar"),
            "the registry-only binding must preserve the released lease's holding contract"
        );
    }
    let address_registrant = sqlx::query_scalar(
        "SELECT address
         FROM address_names_current
         WHERE logical_name_id = $1
         AND relation = 'registrant'",
    )
    .bind(OWNERLESS_LOGICAL)
    .fetch_optional(&pool)
    .await?;
    let address_relations = sqlx::query_as(
        "SELECT address, relation
         FROM address_names_current
         WHERE logical_name_id = $1
         ORDER BY relation, address",
    )
    .bind(OWNERLESS_LOGICAL)
    .fetch_all(&pool)
    .await?;
    let mut serving = serving_projection_snapshot(&pool).await?;
    if matches!(
        later_batch,
        Some(
            LaterBatch::ReleaseOfAnEarlierLease { .. }
                | LaterBatch::SuccessorLease { .. }
                | LaterBatch::TransferOfRetainedLease {
                    stage: TransferStage::Released
                }
        )
    ) {
        // A resource that no later batch touches keeps, in its permission summary row, the
        // target block of the batch that last projected it, as in
        // `project_released_then_reregistered`: the earlier lease's resource when it was
        // released before the re-registration, the retained lease's when the block-11 batch
        // touches only the earlier lease's or when the later batches touch only the successor
        // lease's, and the registry resource's when the block-12 batch touches only the
        // transferred lease's.
        serving.retain(|(table, _)| table != "permissions_current_resource_summary");
    }
    database.cleanup().await?;
    Ok(EnrichedRegistryOnlyProjection {
        expiry,
        registered_at,
        serving,
        registration_resource_id,
        registrant,
        registrant_event_identity,
        address_registrant,
        selected_binding: (selected_binding_id, selected_resource_id),
        control,
        registration_status,
        authority_kind,
        released_at,
        resolver,
        released_tombstone,
        address_relations,
    })
}

#[tokio::test]
async fn enrich_later_registration_keeps_lease_through_registry_only_fallback() -> Result<()> {
    let incremental = project_enriched_registry_only(false, true).await?;
    let from_zero = project_enriched_registry_only(false, false).await?;
    assert_eq!(incremental, from_zero);
    assert_eq!(
        incremental.expiry,
        Some(1_700_001_100),
        "plaintext enrichment left the live registrar expiry behind its binding"
    );
    assert_eq!(
        incremental.registration_resource_id.as_deref(),
        Some(OWNERLESS_RESOURCE)
    );
    assert_eq!(
        incremental.registrant.as_deref(),
        Some("0x6666666666666666666666666666666666666666")
    );
    assert_eq!(
        incremental.address_registrant, None,
        "a registry-only binding lists nobody under relation=registrant"
    );
    let controller_control = project_enriched_registry_only(true, false).await?;
    assert_eq!(controller_control.expiry, incremental.expiry);
    assert_eq!(
        controller_control.registration_resource_id,
        incremental.registration_resource_id
    );
    assert_eq!(controller_control.registrant, incremental.registrant);
    assert_eq!(
        controller_control.address_registrant,
        incremental.address_registrant
    );
    Ok(())
}

/// Once the registry-only binding is selected the registrar resource's binding is closed. A later
/// batch that touches only the registry resource must still stage the lease rows that carry no
/// name, as a rebuild does; otherwise the name loses its expiry and registration date until the
/// next rebuild.
#[tokio::test]
async fn closed_registrar_binding_keeps_its_lease_in_a_later_incremental_batch() -> Result<()> {
    let later = Some(LaterBatch::RegistryOwner);
    let incremental = project_enriched_registry_only_batches(false, true, later).await?;
    let from_zero = project_enriched_registry_only_batches(false, false, later).await?;
    assert_eq!(
        (incremental.expiry, &incremental.registered_at),
        (from_zero.expiry, &from_zero.registered_at),
        "a batch that touched only the registry resource dropped the closed binding's lease"
    );
    assert_eq!(incremental, from_zero);
    assert_eq!(from_zero.expiry, Some(1_700_001_100));
    assert!(from_zero.registered_at.is_some());
    Ok(())
}

/// The other direction: a renewal without a name arrives on the registrar resource after its
/// binding closed. A rebuild names the row through the closed binding, so the batch must rebuild
/// the name too. The renewal extends the lease the name still has: `renew` writes only the
/// expiry, so the registry-only binding keeps control while the registration takes the new
/// expiry.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L157-L169 @ ens_v1@91c966f)
#[tokio::test]
async fn renewal_without_a_name_on_a_closed_binding_rebuilds_its_name() -> Result<()> {
    let handed_off = project_enriched_registry_only_batches(false, false, None).await?;
    let later = Some(LaterBatch::RenewalWithoutName);
    let incremental = project_enriched_registry_only_batches(false, true, later).await?;
    let from_zero = project_enriched_registry_only_batches(false, false, later).await?;
    assert_eq!(incremental, from_zero);
    for (mode, projection) in [("incremental", &incremental), ("from zero", &from_zero)] {
        assert_renewed_after_handoff(mode, projection, &handed_off);
    }
    Ok(())
}

/// Only the lease the name kept can extend its registration. Renewals that carry the name but
/// sit on another registrar resource, whether an earlier lease of the name or a resource the name
/// was never bound to, are later in the block and longer, and still change nothing.
#[tokio::test]
async fn renewal_of_another_lease_does_not_extend_a_handed_off_name() -> Result<()> {
    let handed_off = project_enriched_registry_only_batches(false, false, None).await?;
    let later = Some(LaterBatch::RenewalAndOtherLeases);
    let incremental = project_enriched_registry_only_batches(false, true, later).await?;
    let from_zero = project_enriched_registry_only_batches(false, false, later).await?;
    assert_eq!(incremental, from_zero);
    for (mode, projection) in [("incremental", &incremental), ("from zero", &from_zero)] {
        assert_renewed_after_handoff(mode, projection, &handed_off);
    }
    Ok(())
}

/// The retained lease also ends under the registry-only binding, and its release releases the
/// name like any other lapse: the registrar's `ownerOf` reverts and the name is available again,
/// whatever the registry still records. The name serves a released tombstone on the registry-only
/// binding that stands for the lapsed lease: `released` with its `released_at`, no registrant,
/// authority, expiry, control or resolver, and no row in the address listing.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L71-L76 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L100-L103 @ ens_v1@91c966f)
#[tokio::test]
async fn release_of_the_retained_lease_reaches_a_handed_off_name() -> Result<()> {
    let handed_off = project_enriched_registry_only_batches(false, false, None).await?;
    assert_eq!(handed_off.registration_status.as_deref(), Some("active"));
    assert_eq!(
        handed_off.resolver.as_deref(),
        Some(RESOLVER_ADDRESS),
        "the handed-off name must serve the resolver its registry owner set"
    );
    assert!(
        !handed_off.address_relations.is_empty(),
        "the handed-off name must be listed under an address before the lease lapses"
    );
    let later = Some(LaterBatch::ReleaseOfRetainedLease);
    let incremental = project_enriched_registry_only_batches(false, true, later).await?;
    let from_zero = project_enriched_registry_only_batches(false, false, later).await?;
    assert_eq!(incremental, from_zero);
    for (mode, projection) in [("incremental", &incremental), ("from zero", &from_zero)] {
        assert_eq!(
            projection.registration_status.as_deref(),
            Some("released"),
            "{mode}: the retained lease's release did not reach the name"
        );
        assert_eq!(
            projection.released_at.as_deref(),
            Some("1707777101"),
            "{mode}: the release's released_at was not served"
        );
        assert_eq!(
            projection.released_tombstone.as_deref(),
            Some("ens_v1"),
            "{mode}: the lapsed lease did not leave a released tombstone"
        );
        assert_eq!(
            projection.selected_binding, handed_off.selected_binding,
            "{mode}: the tombstone must stand on the registry-only binding"
        );
        assert_eq!(
            projection.registration_resource_id.as_deref(),
            Some(OWNERLESS_RESOURCE),
            "{mode}: the released registration must keep its lease identity"
        );
        assert_eq!(projection.registered_at, handed_off.registered_at, "{mode}");
        assert_eq!(
            projection.expiry,
            Some(1_700_001_100),
            "{mode}: a released name keeps the lapsed lease's own expiry"
        );
        assert_eq!(
            projection.registrant, None,
            "{mode}: a released name has no registrant"
        );
        assert_eq!(
            projection.authority_kind, None,
            "{mode}: a released name has no authority"
        );
        assert_eq!(
            projection.control,
            json!({"status": "unregistered"}),
            "{mode}: a released name has no current control"
        );
        assert_eq!(
            projection.resolver, None,
            "{mode}: a released name has no resolver"
        );
        assert_eq!(
            projection.address_relations,
            Vec::<(String, String)>::new(),
            "{mode}: a released name is listed under no address"
        );
        assert_eq!(projection.address_registrant, None, "{mode}");
    }
    Ok(())
}

/// A name released on an earlier lease and registered again on a new one, which is then handed
/// off without `reclaim`: the earlier lease's release must not tombstone the name. In the
/// on-chain order the release precedes the new grant; a release of that lease observed after the
/// handoff is not the retained lease's either.
#[tokio::test]
async fn release_of_an_earlier_lease_does_not_tombstone_a_handed_off_name() -> Result<()> {
    let handed_off = project_enriched_registry_only_batches(false, false, None).await?;
    for before_reregistration in [true, false] {
        let later = Some(LaterBatch::ReleaseOfAnEarlierLease {
            before_reregistration,
        });
        let incremental = project_enriched_registry_only_batches(false, true, later).await?;
        let from_zero = project_enriched_registry_only_batches(false, false, later).await?;
        assert_eq!(
            incremental, from_zero,
            "before_reregistration={before_reregistration}"
        );
        for (mode, projection) in [("incremental", &incremental), ("from zero", &from_zero)] {
            let shape = format!("{mode}, before_reregistration={before_reregistration}");
            assert_eq!(
                projection.released_tombstone, None,
                "{shape}: the earlier lease's release tombstoned the live registration"
            );
            assert_eq!(projection.released_at, None, "{shape}");
            assert_eq!(projection.resolver, handed_off.resolver, "{shape}");
            assert_eq!(projection.control, handed_off.control, "{shape}");
            assert_eq!(
                projection.address_relations, handed_off.address_relations,
                "{shape}"
            );
            if before_reregistration {
                assert_renewed_after_handoff(&shape, projection, &handed_off);
            } else {
                assert_eq!(
                    projection.registration_status.as_deref(),
                    Some("active"),
                    "{shape}"
                );
                assert_eq!(projection.expiry, Some(1_700_001_100), "{shape}");
                assert_eq!(
                    projection.selected_binding, handed_off.selected_binding,
                    "{shape}"
                );
                assert_eq!(projection.registrant, handed_off.registrant, "{shape}");
                assert_eq!(
                    projection.registered_at, handed_off.registered_at,
                    "{shape}"
                );
            }
        }
    }
    Ok(())
}

/// After the retained lease lapsed under the registry-only binding, a controller grants the name
/// again with `registerOnly`: the registrar mints a new token and writes the expiry but leaves
/// the registry alone, so the registry-only binding stays the name's only open one and the
/// successor lease never gets a binding. The name is then that successor lease under the same
/// registry control: its registration identity, `registered_at`, expiry and registrant, active,
/// with nothing inherited from the lapsed lease's release, and the registry owner the handoff
/// left behind still served. When the successor lease lapses in turn, its release tombstones the
/// registry-only binding exactly as the retained lease's release did.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L118-L152 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L71-L76 @ ens_v1@91c966f)
#[tokio::test]
async fn successor_lease_by_register_only_is_served_under_the_registry_only_binding() -> Result<()>
{
    let handed_off = project_enriched_registry_only_batches(false, false, None).await?;
    let lapsed = project_enriched_registry_only_batches(
        false,
        false,
        Some(LaterBatch::ReleaseOfRetainedLease),
    )
    .await?;
    assert_eq!(lapsed.released_tombstone.as_deref(), Some("ens_v1"));
    for stage in [SuccessorStage::Granted, SuccessorStage::Renewed] {
        let later = Some(LaterBatch::SuccessorLease {
            stage,
            reclaimed: false,
            transferred: false,
        });
        let incremental = project_enriched_registry_only_batches(false, true, later).await?;
        let from_zero = project_enriched_registry_only_batches(false, false, later).await?;
        assert_eq!(incremental, from_zero, "{stage:?}");
        for (mode, projection) in [("incremental", &incremental), ("from zero", &from_zero)] {
            let shape = format!("{mode}, {stage:?}");
            assert_eq!(
                projection.registration_resource_id.as_deref(),
                Some(SUCCESSOR_LEASE_RESOURCE),
                "{shape}: the registration is not the successor lease"
            );
            assert_eq!(
                projection.registration_status.as_deref(),
                Some("active"),
                "{shape}: the successor lease is live"
            );
            assert_eq!(
                projection.released_at, None,
                "{shape}: the lapsed lease's released_at was inherited"
            );
            assert_eq!(
                projection.released_tombstone, None,
                "{shape}: the lapsed lease's tombstone outlived the successor grant"
            );
            assert_eq!(
                projection.registered_at.as_deref(),
                Some("2026-08-01T00:00:12+00:00"),
                "{shape}: registered_at is not the successor grant's"
            );
            assert_eq!(
                projection.expiry,
                Some(match stage {
                    SuccessorStage::Granted => SUCCESSOR_EXPIRY,
                    _ => SUCCESSOR_RENEWED_EXPIRY,
                }),
                "{shape}: the expiry is not the successor lease's"
            );
            assert_eq!(
                projection.registrant.as_deref(),
                Some(SUCCESSOR_OWNER),
                "{shape}: the registrant is not the successor lease's owner"
            );
            assert_eq!(
                projection.selected_binding, handed_off.selected_binding,
                "{shape}: the registry-only binding is no longer the selected one"
            );
            assert_eq!(
                projection.authority_kind, handed_off.authority_kind,
                "{shape}: the successor grant changed the authority kind"
            );
            // `control.registrant` repeats the registration's registrant; every other control
            // field, the registry owner above all, is the handoff's.
            let mut expected_control = handed_off.control.clone();
            expected_control["registrant"] = json!(SUCCESSOR_OWNER);
            assert_eq!(
                projection.control, expected_control,
                "{shape}: the successor grant changed control beyond its registrant"
            );
            assert_eq!(projection.resolver, handed_off.resolver, "{shape}");
            assert_eq!(
                projection.address_relations, handed_off.address_relations,
                "{shape}"
            );
            assert_eq!(projection.address_registrant, None, "{shape}");
        }
    }
    let later = Some(LaterBatch::SuccessorLease {
        stage: SuccessorStage::Released,
        reclaimed: false,
        transferred: false,
    });
    let incremental = project_enriched_registry_only_batches(false, true, later).await?;
    let from_zero = project_enriched_registry_only_batches(false, false, later).await?;
    assert_eq!(incremental, from_zero, "released");
    for (mode, projection) in [("incremental", &incremental), ("from zero", &from_zero)] {
        assert_released_under_the_registry_only_binding(
            mode,
            projection,
            &handed_off,
            SUCCESSOR_LEASE_RESOURCE,
            "2026-08-01T00:00:12+00:00",
            SUCCESSOR_RELEASED_AT,
            1_700_030_000,
        );
    }
    Ok(())
}

/// The released tombstone a lease leaves on the registry-only binding it lapsed under: the
/// lease's identity, `registered_at`, `released_at` and its own expiry are kept, and nothing
/// else is served as current.
fn assert_released_under_the_registry_only_binding(
    mode: &str,
    projection: &EnrichedRegistryOnlyProjection,
    handed_off: &EnrichedRegistryOnlyProjection,
    lease_resource: &str,
    registered_at: &str,
    released_at: i64,
    expiry: i64,
) {
    assert_eq!(
        projection.registration_status.as_deref(),
        Some("released"),
        "{mode}: the lease's release did not reach the name"
    );
    assert_eq!(
        projection.released_at.as_deref(),
        Some(released_at.to_string().as_str()),
        "{mode}: the release's released_at was not served"
    );
    assert_eq!(
        projection.released_tombstone.as_deref(),
        Some("ens_v1"),
        "{mode}: the lease's release did not leave a released tombstone"
    );
    assert_eq!(
        projection.selected_binding, handed_off.selected_binding,
        "{mode}: the tombstone must stand on the registry-only binding"
    );
    assert_eq!(
        projection.registration_resource_id.as_deref(),
        Some(lease_resource),
        "{mode}: the released registration must keep the lease's identity"
    );
    assert_eq!(
        projection.registered_at.as_deref(),
        Some(registered_at),
        "{mode}"
    );
    assert_eq!(
        projection.expiry,
        Some(expiry),
        "{mode}: a released name keeps the lapsed lease's own expiry"
    );
    assert_eq!(
        projection.registrant, None,
        "{mode}: a released name has no registrant"
    );
    assert_eq!(
        projection.authority_kind, None,
        "{mode}: a released name has no authority"
    );
    assert_eq!(
        projection.control,
        json!({"status": "unregistered"}),
        "{mode}: a released name has no current control"
    );
    assert_eq!(
        projection.resolver, None,
        "{mode}: a released name has no resolver"
    );
    assert_eq!(
        projection.address_relations,
        Vec::<(String, String)>::new(),
        "{mode}: a released name is listed under no address"
    );
    assert_eq!(projection.address_registrant, None, "{mode}");
}

/// The lease a registry-only binding stands for is live under it, so its token can be
/// transferred again without `reclaim`. That changes the token holder, and the token holder is
/// the registrant, while the registry owner the handoff left behind still controls the name.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172-L175 @ ens_v1@91c966f)
fn assert_transferred_under_the_registry_only_binding(
    mode: &str,
    projection: &EnrichedRegistryOnlyProjection,
    handed_off: &EnrichedRegistryOnlyProjection,
    transfer_identity: &str,
) {
    assert_eq!(
        projection.registrant.as_deref(),
        Some(LATER_HOLDER),
        "{mode}: the registrant is not the holder the token was transferred to"
    );
    assert_eq!(
        projection.registrant_event_identity.as_deref(),
        Some(transfer_identity),
        "{mode}: the registrant provenance is not the transfer"
    );
    assert_eq!(
        projection.registration_status.as_deref(),
        Some("active"),
        "{mode}"
    );
    assert_eq!(projection.released_at, None, "{mode}");
    assert_eq!(projection.released_tombstone, None, "{mode}");
    assert_eq!(
        projection.selected_binding, handed_off.selected_binding,
        "{mode}: the registry-only binding is no longer the selected one"
    );
    assert_eq!(
        projection.authority_kind, handed_off.authority_kind,
        "{mode}: the transfer changed the authority kind"
    );
    // `control.registrant` repeats the registration's registrant; every other control field,
    // the registry owner above all, is the handoff's.
    let mut expected_control = handed_off.control.clone();
    expected_control["registrant"] = json!(LATER_HOLDER);
    assert_eq!(
        projection.control, expected_control,
        "{mode}: the transfer changed control beyond its registrant"
    );
    assert_eq!(projection.resolver, handed_off.resolver, "{mode}");
    assert_eq!(
        projection.address_relations, handed_off.address_relations,
        "{mode}"
    );
    assert_eq!(projection.address_registrant, None, "{mode}");
}

/// The retained lease changes hands again under the registry-only binding, without `reclaim`.
/// The name's registrant is then the new holder, with the transfer as its provenance; the
/// registry owner, control, the selected binding, the resolver, the lease's identity and its
/// dates stay as the handoff left them. A renewal in the same batch extends the lease as
/// before, and the lease's later release tombstones the binding as before.
#[tokio::test]
async fn transfer_of_the_retained_lease_reaches_the_registrant_of_a_handed_off_name() -> Result<()>
{
    let handed_off = project_enriched_registry_only_batches(false, false, None).await?;
    assert_eq!(
        handed_off.registrant_event_identity.as_deref(),
        Some("fixture:enriched-unreclaimed-transfer"),
        "the handoff's own transfer names the registrant before the lease moves again"
    );
    for stage in [TransferStage::Transferred, TransferStage::Renewed] {
        let later = Some(LaterBatch::TransferOfRetainedLease { stage });
        let incremental = project_enriched_registry_only_batches(false, true, later).await?;
        let from_zero = project_enriched_registry_only_batches(false, false, later).await?;
        assert_eq!(incremental, from_zero, "{stage:?}");
        for (mode, projection) in [("incremental", &incremental), ("from zero", &from_zero)] {
            let shape = format!("{mode}, {stage:?}");
            assert_transferred_under_the_registry_only_binding(
                &shape,
                projection,
                &handed_off,
                "fixture:enriched-retained-transfer",
            );
            assert_eq!(
                projection.registration_resource_id.as_deref(),
                Some(OWNERLESS_RESOURCE),
                "{shape}: the registration is no longer the retained lease"
            );
            assert_eq!(
                projection.registered_at, handed_off.registered_at,
                "{shape}: the original registration date was not kept"
            );
            assert_eq!(
                projection.expiry,
                Some(match stage {
                    TransferStage::Transferred => 1_700_001_100,
                    _ => 1_700_002_100,
                }),
                "{shape}: the expiry is not the retained lease's"
            );
        }
    }
    let later = Some(LaterBatch::TransferOfRetainedLease {
        stage: TransferStage::Released,
    });
    let incremental = project_enriched_registry_only_batches(false, true, later).await?;
    let from_zero = project_enriched_registry_only_batches(false, false, later).await?;
    assert_eq!(incremental, from_zero, "released");
    for (mode, projection) in [("incremental", &incremental), ("from zero", &from_zero)] {
        assert_released_under_the_registry_only_binding(
            mode,
            projection,
            &handed_off,
            OWNERLESS_RESOURCE,
            "2026-08-01T00:00:08+00:00",
            TRANSFERRED_RELEASED_AT,
            1_700_002_100,
        );
    }
    Ok(())
}

/// The same for the successor lease: once `registerOnly` granted it under the registry-only
/// binding, its token can be transferred without `reclaim` too. The registrant follows the
/// token, with the transfer as its provenance, while the registry owner, control, the selected
/// binding, the resolver and the successor lease's identity and dates are unchanged; a renewal
/// in the same batch and the later release behave as they do without the transfer.
#[tokio::test]
async fn transfer_of_the_successor_lease_reaches_the_registrant_under_the_registry_only_binding()
-> Result<()> {
    let handed_off = project_enriched_registry_only_batches(false, false, None).await?;
    for stage in [SuccessorStage::Granted, SuccessorStage::Renewed] {
        let later = Some(LaterBatch::SuccessorLease {
            stage,
            reclaimed: false,
            transferred: true,
        });
        let incremental = project_enriched_registry_only_batches(false, true, later).await?;
        let from_zero = project_enriched_registry_only_batches(false, false, later).await?;
        assert_eq!(incremental, from_zero, "{stage:?}");
        for (mode, projection) in [("incremental", &incremental), ("from zero", &from_zero)] {
            let shape = format!("{mode}, {stage:?}");
            assert_transferred_under_the_registry_only_binding(
                &shape,
                projection,
                &handed_off,
                "fixture:enriched-successor-13-TokenControlTransferred",
            );
            assert_eq!(
                projection.registration_resource_id.as_deref(),
                Some(SUCCESSOR_LEASE_RESOURCE),
                "{shape}: the registration is not the successor lease"
            );
            assert_eq!(
                projection.registered_at.as_deref(),
                Some("2026-08-01T00:00:12+00:00"),
                "{shape}: registered_at is not the successor grant's"
            );
            assert_eq!(
                projection.expiry,
                Some(match stage {
                    SuccessorStage::Granted => SUCCESSOR_EXPIRY,
                    _ => SUCCESSOR_RENEWED_EXPIRY,
                }),
                "{shape}: the expiry is not the successor lease's"
            );
        }
    }
    let later = Some(LaterBatch::SuccessorLease {
        stage: SuccessorStage::Released,
        reclaimed: false,
        transferred: true,
    });
    let incremental = project_enriched_registry_only_batches(false, true, later).await?;
    let from_zero = project_enriched_registry_only_batches(false, false, later).await?;
    assert_eq!(incremental, from_zero, "released");
    for (mode, projection) in [("incremental", &incremental), ("from zero", &from_zero)] {
        assert_released_under_the_registry_only_binding(
            mode,
            projection,
            &handed_off,
            SUCCESSOR_LEASE_RESOURCE,
            "2026-08-01T00:00:12+00:00",
            SUCCESSOR_RELEASED_AT,
            1_700_030_000,
        );
    }
    Ok(())
}

/// The control case: the successor grant is `register`, which writes the registry owner in the
/// same transaction, so the successor lease's resource takes over the name with a binding of its
/// own. That binding is selected and the registration is the successor lease's, as for any
/// re-registration; the registry-only binding plays no part.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L148-L150 @ ens_v1@91c966f)
#[tokio::test]
async fn successor_lease_that_writes_the_registry_opens_its_own_binding() -> Result<()> {
    let later = Some(LaterBatch::SuccessorLease {
        stage: SuccessorStage::Granted,
        reclaimed: true,
        transferred: false,
    });
    let incremental = project_enriched_registry_only_batches(false, true, later).await?;
    let from_zero = project_enriched_registry_only_batches(false, false, later).await?;
    assert_eq!(incremental, from_zero);
    for (mode, projection) in [("incremental", &incremental), ("from zero", &from_zero)] {
        assert_eq!(
            projection.selected_binding,
            (
                Some(SUCCESSOR_LEASE_BINDING.to_owned()),
                Some(SUCCESSOR_LEASE_RESOURCE.to_owned())
            ),
            "{mode}: the successor lease's own binding must be selected"
        );
        assert_eq!(
            projection.registration_resource_id.as_deref(),
            Some(SUCCESSOR_LEASE_RESOURCE),
            "{mode}"
        );
        assert_eq!(
            projection.registration_status.as_deref(),
            Some("active"),
            "{mode}"
        );
        assert_eq!(projection.expiry, Some(SUCCESSOR_EXPIRY), "{mode}");
        assert_eq!(
            projection.registered_at.as_deref(),
            Some("2026-08-01T00:00:12+00:00"),
            "{mode}"
        );
        assert_eq!(projection.released_at, None, "{mode}");
        assert_eq!(projection.released_tombstone, None, "{mode}");
        assert_eq!(
            projection.registrant.as_deref(),
            Some(SUCCESSOR_OWNER),
            "{mode}"
        );
        assert_eq!(
            projection.authority_kind.as_deref(),
            Some("registrar"),
            "{mode}"
        );
    }
    Ok(())
}

fn assert_renewed_after_handoff(
    mode: &str,
    projection: &EnrichedRegistryOnlyProjection,
    handed_off: &EnrichedRegistryOnlyProjection,
) {
    assert_eq!(
        projection.expiry,
        Some(1_700_002_100),
        "{mode}: the expiry served is not the retained lease's renewed expiry"
    );
    assert_eq!(
        projection.selected_binding,
        (
            Some(RELEASE_REGISTRY_BINDING.to_owned()),
            Some(RELEASE_REGISTRY_RESOURCE.to_owned())
        ),
        "{mode}: the registry-only binding is no longer the selected one"
    );
    assert_eq!(
        projection.control, handed_off.control,
        "{mode}: the renewal changed control"
    );
    assert_eq!(
        projection.registered_at.as_deref(),
        Some("2026-08-01T00:00:08+00:00"),
        "{mode}: the original registration date was not kept"
    );
    assert_eq!(projection.registered_at, handed_off.registered_at);
    assert_eq!(
        projection.registration_resource_id.as_deref(),
        Some(OWNERLESS_RESOURCE)
    );
    assert_eq!(projection.registration_status.as_deref(), Some("active"));
    assert_eq!(projection.registrant, handed_off.registrant);
    assert_eq!(projection.address_registrant, handed_off.address_registrant);
    assert_eq!(projection.released_tombstone, None, "{mode}");
    assert_eq!(projection.resolver, handed_off.resolver, "{mode}");
    assert_eq!(
        projection.address_relations, handed_off.address_relations,
        "{mode}"
    );
}

/// Today's mainnet manifest shape: controller events grant leases, so registrar rows carry the
/// name, while the registry adapter may have written rows without one before the label was known.
/// A name is released and registered again on a new resource; a later batch touches only the new
/// resource. The closed binding's resource is out of that batch's scope, so anything a rebuild
/// attached through it would be missing incrementally.
async fn project_released_then_reregistered(
    incremental: bool,
) -> Result<Vec<(String, serde_json::Value)>> {
    const SECOND_RESOURCE: &str = "50000000-0000-0000-0000-000000000001";
    const SECOND_BINDING: &str = "50000000-0000-0000-0000-000000000011";
    let (database, pool) = migrated_pool().await?;
    seed_blocks(&pool, [8, 9, 10, 11]).await?;
    seed_surface(
        &pool,
        OWNERLESS_NAMEHASH,
        "registered-twice.eth",
        OWNERLESS_RESOURCE,
        OWNERLESS_BINDING,
    )
    .await?;
    seed_binding_provenance(&pool, OWNERLESS_BINDING, 0, 1).await?;
    seed_normalized_event(
        &pool,
        "fixture:twice-registry-row-before-label",
        None,
        Some(OWNERLESS_RESOURCE),
        "AuthorityTransferred",
        "ens_v1_registry_l1",
        8,
        0,
        json!({
            "node": OWNERLESS_NAMEHASH,
            "owner": CONTROL_OWNER,
            "owner_getter": CONTROL_OWNER,
            "authority_kind": "registrar",
        }),
        json!({"emitting_address": REGISTRY_ADDRESS}),
    )
    .await?;
    seed_normalized_event(
        &pool,
        "fixture:twice-registry-resolver-before-label",
        None,
        Some(OWNERLESS_RESOURCE),
        "ResolverChanged",
        "ens_v1_registry_l1",
        8,
        3,
        json!({
            "source_event": "NewResolver",
            "node": OWNERLESS_NAMEHASH,
            "resolver": RESOLVER_ADDRESS,
        }),
        json!({"emitting_address": REGISTRY_ADDRESS}),
    )
    .await?;
    let lease = |registrant: &str, expiry: i64| {
        json!({
            "source_event": "NameRegistered",
            "authority_kind": "registrar",
            "registrant": registrant,
            "expiry": expiry,
            "namehash": OWNERLESS_NAMEHASH,
        })
    };
    for (kind, log) in [("RegistrationGranted", 1), ("ExpiryChanged", 2)] {
        seed_normalized_event(
            &pool,
            &format!("fixture:twice-first-{kind}"),
            Some(OWNERLESS_LOGICAL),
            Some(OWNERLESS_RESOURCE),
            kind,
            "ens_v1_registrar_l1",
            8,
            log,
            lease(CONTROL_OWNER, 4242),
            json!({}),
        )
        .await?;
    }
    seed_normalized_event(
        &pool,
        "fixture:twice-release",
        Some(OWNERLESS_LOGICAL),
        Some(OWNERLESS_RESOURCE),
        "RegistrationReleased",
        "ens_v1_registrar_l1",
        9,
        1,
        json!({
            "source_event": "NameReleased",
            "authority_kind": "registrar",
            "status": "released",
            "namehash": OWNERLESS_NAMEHASH,
        }),
        json!({}),
    )
    .await?;
    if incremental {
        run_project(&pool, 9, 8, None).await?;
    }
    seed_next_binding(
        &pool,
        OWNERLESS_NAMEHASH,
        SECOND_RESOURCE,
        SECOND_BINDING,
        10,
        "2026-08-01T00:00:10Z",
    )
    .await?;
    seed_binding_provenance(&pool, SECOND_BINDING, 0, 1).await?;
    for (kind, log) in [("RegistrationGranted", 1), ("ExpiryChanged", 2)] {
        seed_normalized_event(
            &pool,
            &format!("fixture:twice-second-{kind}"),
            Some(OWNERLESS_LOGICAL),
            Some(SECOND_RESOURCE),
            kind,
            "ens_v1_registrar_l1",
            10,
            log,
            lease(PRIOR_CONTROLLER, 9_999_999_999),
            json!({}),
        )
        .await?;
    }
    if incremental {
        run_project(&pool, 10, 10, Some(9)).await?;
    }
    for (kind, log) in [("RegistrationRenewed", 1), ("ExpiryChanged", 2)] {
        seed_normalized_event(
            &pool,
            &format!("fixture:twice-renewal-{kind}"),
            Some(OWNERLESS_LOGICAL),
            Some(SECOND_RESOURCE),
            kind,
            "ens_v1_registrar_l1",
            11,
            log,
            lease(PRIOR_CONTROLLER, 19_999_999_999),
            json!({}),
        )
        .await?;
    }
    run_project(
        &pool,
        11,
        if incremental { 11 } else { 8 },
        incremental.then_some(10),
    )
    .await?;
    // The first resource's permission summary row is left out: a resource that no later batch
    // touches keeps the target block of the batch that last projected it, with or without
    // resource-keyed registrar rows.
    let mut serving = serving_projection_snapshot(&pool).await?;
    serving.retain(|(table, _)| table != "permissions_current_resource_summary");
    database.cleanup().await?;
    Ok(serving)
}

#[tokio::test]
async fn released_then_reregistered_name_projects_identically_incrementally_and_from_zero()
-> Result<()> {
    let incremental = project_released_then_reregistered(true).await?;
    let from_zero = project_released_then_reregistered(false).await?;
    assert_eq!(incremental, from_zero);
    let name_current = &from_zero[0].1[0];
    assert_eq!(
        name_current["declared_summary"]["registration"]["expiry"],
        json!(19_999_999_999_i64),
        "{name_current}"
    );
    Ok(())
}

/// A BaseRegistrar token transferred without `reclaim` leaves the registry owner behind, so the
/// name is bound to a registry-only resource while the lease stays on the registrar's. The new
/// token holder is served as the registration's registrant, but a registry-only binding lists
/// nobody under the `registrant` or `token_holder` address relations. Seeded the way today's
/// mainnet manifest produces events (named, controller-granted); this is what Project served
/// before registrar rows were joined by resource identity, and the end-to-end scenario
/// `transfer_without_reclaim_keeps_registry_owner_divergent` asserts the same.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L171-L175 @ ens_v1@91c966f)
#[tokio::test]
async fn unreclaimed_transfer_serves_the_holder_without_a_registrant_relation() -> Result<()> {
    for incremental in [false, true] {
        let projection = project_enriched_registry_only(true, incremental).await?;
        assert_eq!(
            projection.registrant.as_deref(),
            Some("0x6666666666666666666666666666666666666666"),
            "incremental={incremental}"
        );
        assert_eq!(projection.expiry, Some(1_700_001_100));
        assert_eq!(
            projection.address_registrant, None,
            "incremental={incremental}: a registry-only binding listed the token holder under \
             relation=registrant"
        );
    }
    Ok(())
}

// The two tests below seed events in the shape today's mainnet manifest produces: a controller
// event grants the lease, so registrar rows carry the name. What they assert is what Project
// served before registrar rows were joined by resource identity; it must not change.

#[tokio::test]
async fn controller_granted_born_wrapped_name_keeps_its_registrar_lease() -> Result<()> {
    let (database, pool) = migrated_pool().await?;
    seed_chain(&pool).await?;
    sqlx::query(
        "INSERT INTO resources (resource_id, chain_id, block_hash, block_number,
             canonicality_state)
         VALUES ($1::uuid, $2, $3, 8, 'canonical')",
    )
    .bind(OWNERLESS_RESOURCE)
    .bind(CHAIN)
    .bind(block_hash(8))
    .execute(&pool)
    .await?;
    seed_surface(
        &pool,
        OWNERLESS_NAMEHASH,
        "bornwrapped.eth",
        CONTROL_RESOURCE,
        CONTROL_BINDING,
    )
    .await?;
    seed_binding_provenance(&pool, CONTROL_BINDING, 0, 1).await?;
    sqlx::query(
        "INSERT INTO token_lineages (token_lineage_id, chain_id, block_hash, block_number,
             canonicality_state)
         VALUES ($1::uuid, $2, $3, 8, 'canonical')",
    )
    .bind(WRAPPER_LINEAGE)
    .bind(CHAIN)
    .bind(block_hash(8))
    .execute(&pool)
    .await?;
    sqlx::query("UPDATE resources SET token_lineage_id = $1::uuid WHERE resource_id = $2::uuid")
        .bind(WRAPPER_LINEAGE)
        .bind(CONTROL_RESOURCE)
        .execute(&pool)
        .await?;
    for (identity, resource, kind, family, log, state) in [
        // NameWrapped precedes the controller's NameRegistered, so no registrar lease is current yet.
        (
            "fixture:born-binding",
            CONTROL_RESOURCE,
            "SurfaceBound",
            "ens_v1_wrapper_l1",
            1,
            json!({
                "source_event": "NameWrapped",
                "node": OWNERLESS_NAMEHASH,
                "authority_kind": "wrapper",
                "wrapped_registrar_resource_id": null,
            }),
        ),
        (
            "fixture:born-scope",
            CONTROL_RESOURCE,
            "PermissionScopeChanged",
            "ens_v1_wrapper_l1",
            1,
            json!({
                "source_event": "NameWrapped",
                "node": OWNERLESS_NAMEHASH,
                "wrapper_state": "wrapped",
                "fuses": 0,
                "wrapped_registrar_resource_id": null,
            }),
        ),
        (
            "fixture:born-wrapper-expiry",
            CONTROL_RESOURCE,
            "ExpiryChanged",
            "ens_v1_wrapper_l1",
            1,
            json!({
                "source_event": "NameWrapped",
                "node": OWNERLESS_NAMEHASH,
                "expiry": 7_780_242,
                "wrapped_registrar_resource_id": null,
            }),
        ),
        (
            "fixture:born-wrapper-transfer",
            CONTROL_RESOURCE,
            "TokenControlTransferred",
            "ens_v1_wrapper_l1",
            1,
            json!({
                "source_event": "NameWrapped",
                "to": CONTROL_OWNER,
                "wrapped_registrar_resource_id": null,
            }),
        ),
        (
            "fixture:born-grant",
            OWNERLESS_RESOURCE,
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            3,
            json!({
                "source_event": "NameRegistered",
                "authority_kind": "registrar",
                "authority_key": "registrar:born",
                "registrant": CONTROL_OWNER,
                "expiry": 4242,
                "namehash": OWNERLESS_NAMEHASH,
            }),
        ),
        (
            "fixture:born-expiry",
            OWNERLESS_RESOURCE,
            "ExpiryChanged",
            "ens_v1_registrar_l1",
            3,
            json!({
                "source_event": "NameRegistered",
                "authority_kind": "registrar",
                "authority_key": "registrar:born",
                "registrant": CONTROL_OWNER,
                "expiry": 4242,
                "namehash": OWNERLESS_NAMEHASH,
            }),
        ),
    ] {
        seed_normalized_event(
            &pool,
            identity,
            Some(OWNERLESS_LOGICAL),
            Some(resource),
            kind,
            family,
            8,
            log,
            state,
            json!({}),
        )
        .await?;
    }
    sqlx::query(
        "UPDATE normalized_events
         SET transaction_hash = '0xborn'
         WHERE event_identity LIKE 'fixture:born-%'",
    )
    .execute(&pool)
    .await?;
    run_project(&pool, 8, 8, None).await?;
    let summary: (Option<String>, Option<i64>, Option<String>) = sqlx::query_as(
        "SELECT declared_summary #>> '{registration,status}',
             (declared_summary #>> '{registration,expiry}')::bigint,
             declared_summary #>> '{registration,registered_at}'
         FROM name_current
         WHERE logical_name_id = $1",
    )
    .bind(OWNERLESS_LOGICAL)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        (summary.0.as_deref(), summary.1),
        (Some("active"), Some(4242)),
        "a controller-granted born-wrapped name lost its registrar lease: {summary:?}"
    );
    assert!(summary.2.is_some(), "{summary:?}");
    assert_eq!(
        name_origin(&pool, OWNERLESS_LOGICAL).await?,
        json!({
            "created_at": "2026-08-01T00:00:08+00:00",
            "selected_event_ids": [
                "fixture:born-binding",
                "fixture:born-expiry",
                "fixture:born-grant",
                "fixture:born-scope",
                "fixture:born-wrapper-expiry",
                "fixture:born-wrapper-transfer",
            ],
            "raw_fact_refs": 6,
            "manifest_versions": 6,
            "control": {
                "expiry": "1970-01-01T01:10:42Z",
                "latest_event_kind": "TokenControlTransferred",
                "registrant": CONTROL_OWNER.to_lowercase(),
                "registry_owner": null,
                "status": null,
            },
        }),
        "created_at, provenance or control changed for a controller-granted born-wrapped name"
    );
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn controller_granted_later_wrapped_name_serves_the_same_registrant_as_before() -> Result<()>
{
    const WRAPPER_CONTRACT: &str = "0x9999999999999999999999999999999999999999";
    let (database, pool) = migrated_pool().await?;
    seed_chain(&pool).await?;
    seed_surface(
        &pool,
        OWNERLESS_NAMEHASH,
        "laterwrapped.eth",
        OWNERLESS_RESOURCE,
        OWNERLESS_BINDING,
    )
    .await?;
    seed_binding_provenance(&pool, OWNERLESS_BINDING, 0, 1).await?;
    seed_successor_binding(
        &pool,
        OWNERLESS_NAMEHASH,
        CONTROL_RESOURCE,
        CONTROL_BINDING,
        9,
    )
    .await?;
    seed_binding_provenance(&pool, CONTROL_BINDING, 0, 3).await?;
    sqlx::query(
        "INSERT INTO token_lineages (token_lineage_id, chain_id, block_hash, block_number,
             canonicality_state)
         VALUES ($1::uuid, $2, $3, 9, 'canonical')",
    )
    .bind(WRAPPER_LINEAGE)
    .bind(CHAIN)
    .bind(block_hash(9))
    .execute(&pool)
    .await?;
    sqlx::query("UPDATE resources SET token_lineage_id = $1::uuid WHERE resource_id = $2::uuid")
        .bind(WRAPPER_LINEAGE)
        .bind(CONTROL_RESOURCE)
        .execute(&pool)
        .await?;
    for (identity, resource, kind, family, block, log, state) in [
        (
            "fixture:later-grant",
            OWNERLESS_RESOURCE,
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            8,
            1,
            json!({
                "source_event": "NameRegistered",
                "authority_kind": "registrar",
                "authority_key": "registrar:later",
                "registrant": CONTROL_OWNER,
                "expiry": 4242,
                "namehash": OWNERLESS_NAMEHASH,
            }),
        ),
        (
            "fixture:later-expiry",
            OWNERLESS_RESOURCE,
            "ExpiryChanged",
            "ens_v1_registrar_l1",
            8,
            1,
            json!({
                "source_event": "NameRegistered",
                "authority_kind": "registrar",
                "authority_key": "registrar:later",
                "registrant": CONTROL_OWNER,
                "expiry": 4242,
                "namehash": OWNERLESS_NAMEHASH,
            }),
        ),
        (
            "fixture:later-custody",
            OWNERLESS_RESOURCE,
            "TokenControlTransferred",
            "ens_v1_registrar_l1",
            9,
            1,
            json!({
                "source_event": "Transfer",
                "from": CONTROL_OWNER,
                "to": WRAPPER_CONTRACT,
                "namehash": OWNERLESS_NAMEHASH,
            }),
        ),
        (
            "fixture:later-binding",
            CONTROL_RESOURCE,
            "SurfaceBound",
            "ens_v1_wrapper_l1",
            9,
            3,
            json!({
                "source_event": "NameWrapped",
                "node": OWNERLESS_NAMEHASH,
                "authority_kind": "wrapper",
                "wrapped_registrar_resource_id": OWNERLESS_RESOURCE,
            }),
        ),
        (
            "fixture:later-scope",
            CONTROL_RESOURCE,
            "PermissionScopeChanged",
            "ens_v1_wrapper_l1",
            9,
            3,
            json!({
                "source_event": "NameWrapped",
                "node": OWNERLESS_NAMEHASH,
                "wrapper_state": "wrapped",
                "fuses": 0,
                "wrapped_registrar_resource_id": OWNERLESS_RESOURCE,
            }),
        ),
        (
            "fixture:later-wrapper-expiry",
            CONTROL_RESOURCE,
            "ExpiryChanged",
            "ens_v1_wrapper_l1",
            9,
            3,
            json!({
                "source_event": "NameWrapped",
                "node": OWNERLESS_NAMEHASH,
                "expiry": 7_780_242,
                "wrapped_registrar_resource_id": OWNERLESS_RESOURCE,
            }),
        ),
        // wrapETH2LD lets the caller name a wrapped owner other than the registrant.
        (
            "fixture:later-wrapper-transfer",
            CONTROL_RESOURCE,
            "TokenControlTransferred",
            "ens_v1_wrapper_l1",
            9,
            3,
            json!({
                "source_event": "NameWrapped",
                "to": PRIOR_CONTROLLER,
                "wrapped_registrar_resource_id": OWNERLESS_RESOURCE,
            }),
        ),
    ] {
        seed_normalized_event(
            &pool,
            identity,
            Some(OWNERLESS_LOGICAL),
            Some(resource),
            kind,
            family,
            block,
            log,
            state,
            json!({"emitting_address":WRAPPER_CONTRACT}),
        )
        .await?;
    }
    sqlx::query(
        "UPDATE normalized_events
         SET transaction_hash = '0xwrap'
         WHERE event_identity LIKE 'fixture:later-%'
         AND block_number = 9",
    )
    .execute(&pool)
    .await?;
    run_project(&pool, 9, 8, None).await?;
    let summary: (Option<String>, Option<i64>, Option<String>) = sqlx::query_as(
        "SELECT declared_summary #>> '{registration,status}',
             (declared_summary #>> '{registration,expiry}')::bigint,
             declared_summary #>> '{registration,registrant}'
         FROM name_current
         WHERE logical_name_id = $1",
    )
    .bind(OWNERLESS_LOGICAL)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        (summary.0.as_deref(), summary.1),
        (Some("active"), Some(4242)),
        "{summary:?}"
    );
    assert_eq!(
        summary.2,
        Some(PRIOR_CONTROLLER.to_lowercase()),
        "a later-wrapped name serves the NameWrapped owner as registrant"
    );
    assert_eq!(
        name_origin(&pool, OWNERLESS_LOGICAL).await?,
        json!({
            "created_at": "2026-08-01T00:00:08+00:00",
            "selected_event_ids": [
                "fixture:later-binding",
                "fixture:later-custody",
                "fixture:later-expiry",
                "fixture:later-grant",
                "fixture:later-scope",
                "fixture:later-wrapper-expiry",
                "fixture:later-wrapper-transfer",
            ],
            "raw_fact_refs": 7,
            "manifest_versions": 7,
            "control": {
                "expiry": "1970-01-01T01:10:42Z",
                "latest_event_kind": "TokenControlTransferred",
                "registrant": PRIOR_CONTROLLER.to_lowercase(),
                "registry_owner": null,
                "status": null,
            },
        }),
        "created_at, provenance or control changed for a controller-granted later-wrapped name"
    );
    database.cleanup().await?;
    Ok(())
}

#[derive(Clone, Copy, PartialEq)]
enum BornWrappedShape {
    /// The BaseRegistrar's own event granted the lease before `NameWrapped`: registrar rows carry
    /// no name and the wrap recorded the lease in `wrapped_registrar_resource_id`.
    LinkRecorded,
    /// A controller event granted the lease after `NameWrapped` in the same transaction (today's
    /// mainnet manifest): registrar rows carry the name and the wrap recorded no lease.
    ControllerGranted,
    /// The registrar lease was renewed on the BaseRegistrar directly, so only the NameWrapper's
    /// own expiry has passed (issue #908). The lease is live and nothing was released.
    WrapperExpiryOnly,
}

/// A name registered through the NameWrapper. The only binding is the wrapper's; the registrar
/// lease lives on its own resource, which never has a binding. When the lease lapses past grace
/// the release is on the registrar resource while the unbind and the closing epoch are on the
/// wrapper's.
async fn born_wrapped_projection(
    shape: BornWrappedShape,
    incremental: bool,
) -> Result<serde_json::Value> {
    let controller_granted = shape == BornWrappedShape::ControllerGranted;
    let registrar_name = controller_granted.then_some(OWNERLESS_LOGICAL);
    let link = if controller_granted {
        serde_json::Value::Null
    } else {
        json!(OWNERLESS_RESOURCE)
    };
    // The numeric grant precedes NameWrapped; a controller grant follows it.
    let registrar_log = if controller_granted { 3 } else { 1 };
    const WRAPPER_CONTRACT: &str = "0x9999999999999999999999999999999999999999";
    let (database, pool) = migrated_pool().await?;
    seed_chain(&pool).await?;
    seed_blocks(&pool, [11]).await?;
    sqlx::query(
        "INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1::uuid, $2, $3, 8, 'canonical')",
    )
    .bind(OWNERLESS_RESOURCE)
    .bind(CHAIN)
    .bind(block_hash(8))
    .execute(&pool)
    .await?;
    seed_surface(
        &pool,
        OWNERLESS_NAMEHASH,
        "lapsed-born-wrapped.eth",
        CONTROL_RESOURCE,
        CONTROL_BINDING,
    )
    .await?;
    seed_binding_provenance(&pool, CONTROL_BINDING, 0, 2).await?;
    sqlx::query(
        "INSERT INTO token_lineages (
             token_lineage_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1::uuid, $2, $3, 8, 'canonical')",
    )
    .bind(WRAPPER_LINEAGE)
    .bind(CHAIN)
    .bind(block_hash(8))
    .execute(&pool)
    .await?;
    sqlx::query("UPDATE resources SET token_lineage_id = $1::uuid WHERE resource_id = $2::uuid")
        .bind(WRAPPER_LINEAGE)
        .bind(CONTROL_RESOURCE)
        .execute(&pool)
        .await?;
    // The NameWrapper registers the lease to itself, so the BaseRegistrar's numeric grant names
    // the NameWrapper contract. The controller's event names the wrapped user instead, while
    // the adapter keeps the NameWrapper as the lease's token owner.
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L297 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/deployments/mainnet/WrappedETHRegistrarController.json:L656 @ ens_v1@91c966f)
    let grant_registrant = if controller_granted {
        CONTROL_OWNER
    } else {
        WRAPPER_CONTRACT
    };
    let registrar = json!({
        "source_event": "NameRegistered",
        "authority_kind": "registrar",
        "authority_key": "registrar:born",
        "registrant": grant_registrant,
        "authority_owner": WRAPPER_CONTRACT,
        "expiry": 4242,
        "namehash": OWNERLESS_NAMEHASH,
    });
    let wrapper = json!({
        "source_event": "NameWrapped",
        "node": OWNERLESS_NAMEHASH,
        "authority_kind": "wrapper",
        "authority_key": "wrapper:born",
        "wrapped_registrar_resource_id": link,
        "wrapper_state": "wrapped",
        "fuses": 0,
        "expiry": 7_780_242,
    });
    for (identity, logical, resource, kind, family, log, state) in [
        (
            "fixture:lapsed-grant",
            registrar_name,
            OWNERLESS_RESOURCE,
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            registrar_log,
            registrar.clone(),
        ),
        (
            "fixture:lapsed-expiry",
            registrar_name,
            OWNERLESS_RESOURCE,
            "ExpiryChanged",
            "ens_v1_registrar_l1",
            registrar_log,
            registrar.clone(),
        ),
        (
            "fixture:lapsed-wrapper-holder",
            Some(OWNERLESS_LOGICAL),
            CONTROL_RESOURCE,
            "TokenControlTransferred",
            "ens_v1_wrapper_l1",
            2,
            json!({
                "source_event": "NameWrapped",
                "node": OWNERLESS_NAMEHASH,
                "to": CONTROL_OWNER,
                "wrapped_registrar_resource_id": link,
            }),
        ),
        (
            "fixture:lapsed-wrapper-expiry",
            Some(OWNERLESS_LOGICAL),
            CONTROL_RESOURCE,
            "ExpiryChanged",
            "ens_v1_wrapper_l1",
            2,
            wrapper.clone(),
        ),
        (
            "fixture:lapsed-wrapper-scope",
            Some(OWNERLESS_LOGICAL),
            CONTROL_RESOURCE,
            "PermissionScopeChanged",
            "ens_v1_wrapper_l1",
            2,
            wrapper.clone(),
        ),
        (
            "fixture:lapsed-wrapper-binding",
            Some(OWNERLESS_LOGICAL),
            CONTROL_RESOURCE,
            "SurfaceBound",
            "ens_v1_wrapper_l1",
            2,
            wrapper.clone(),
        ),
        (
            "fixture:lapsed-wrapper-epoch",
            Some(OWNERLESS_LOGICAL),
            CONTROL_RESOURCE,
            "AuthorityEpochChanged",
            "ens_v1_wrapper_l1",
            2,
            wrapper.clone(),
        ),
    ] {
        seed_normalized_event(
            &pool,
            identity,
            logical,
            Some(resource),
            kind,
            family,
            8,
            log,
            state,
            json!({"emitting_address":WRAPPER_CONTRACT}),
        )
        .await?;
    }
    sqlx::query(
        "UPDATE normalized_events SET transaction_hash = '0xbornwrapped'
         WHERE event_identity LIKE 'fixture:lapsed-%'",
    )
    .execute(&pool)
    .await?;
    if incremental {
        run_project(&pool, 9, 8, None).await?;
        let live: (Option<String>, Option<i64>) = sqlx::query_as(
            "SELECT declared_summary #>> '{registration,status}',
                    (declared_summary #>> '{registration,expiry}')::bigint
             FROM name_current WHERE logical_name_id = $1",
        )
        .bind(OWNERLESS_LOGICAL)
        .fetch_one(&pool)
        .await?;
        assert_eq!(live, (Some("active".to_owned()), Some(4242)));
    }

    if shape == BornWrappedShape::WrapperExpiryOnly {
        // Renewing on the BaseRegistrar directly extends the lease but not the NameWrapper's
        // own expiry.
        // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L157-L169 @ ens_v1@91c966f)
        for (kind, log) in [("RegistrationRenewed", 1), ("ExpiryChanged", 2)] {
            seed_normalized_event(
                &pool,
                &format!("fixture:direct-renewal-{kind}"),
                registrar_name,
                Some(OWNERLESS_RESOURCE),
                kind,
                "ens_v1_registrar_l1",
                10,
                log,
                json!({
                    "source_event": "NameRenewed",
                    "authority_kind": "registrar",
                    "expiry": 99_999_999,
                    "namehash": OWNERLESS_NAMEHASH,
                }),
                json!({}),
            )
            .await?;
        }
    }
    // The wrapped name changes hands before it lapses: a NameWrapper `TransferSingle`.
    seed_normalized_event(
        &pool,
        "fixture:lapsed-wrapper-transfer",
        Some(OWNERLESS_LOGICAL),
        Some(CONTROL_RESOURCE),
        "TokenControlTransferred",
        "ens_v1_wrapper_l1",
        10,
        0,
        json!({"source_event":"TransferSingle","to":LAPSED_LAST_HOLDER,"namehash":OWNERLESS_NAMEHASH,"value":"1"}),
        json!({"emitting_address":WRAPPER_CONTRACT}),
    )
    .await?;
    // The boundary rows carry no log position. When the lease lapses past grace the registrar
    // releases it. When only the NameWrapper's expiry passed there is no release; whatever the
    // interpreter emits for that state, the hardest case for the tombstone rule is the same
    // closed wrapper binding with no open binding left.
    for (identity, resource, kind, family, state) in [
        (
            "fixture:lapsed-release",
            OWNERLESS_RESOURCE,
            "RegistrationReleased",
            "ens_v1_registrar_l1",
            json!({
                "source_event": "RegistrationReleased",
                "expiry": 4242,
                "namehash": OWNERLESS_NAMEHASH,
                "released_at": 7_780_243,
            }),
        ),
        (
            "fixture:lapsed-unbound",
            CONTROL_RESOURCE,
            "SurfaceUnbound",
            "ens_v1_wrapper_l1",
            json!({
                "source_event": "RegistrationReleased",
                "authority_kind": "wrapper",
                "authority_key": "wrapper:born",
                "active_to": 7_780_243,
            }),
        ),
        (
            "fixture:lapsed-closing-epoch",
            CONTROL_RESOURCE,
            "AuthorityEpochChanged",
            "ens_v1_wrapper_l1",
            json!({
                "source_event": "RegistrationReleased",
                "authority_kind": null,
                "authority_key": null,
                "owner": null,
            }),
        ),
    ] {
        if shape == BornWrappedShape::WrapperExpiryOnly && identity == "fixture:lapsed-release" {
            continue;
        }
        seed_normalized_event(
            &pool,
            identity,
            Some(OWNERLESS_LOGICAL),
            Some(resource),
            kind,
            family,
            11,
            0,
            state,
            json!({}),
        )
        .await?;
    }
    sqlx::query(
        "UPDATE normalized_events
         SET transaction_hash = NULL, transaction_index = NULL, log_index = NULL
         WHERE event_identity IN (
             'fixture:lapsed-release', 'fixture:lapsed-unbound', 'fixture:lapsed-closing-epoch'
         )",
    )
    .execute(&pool)
    .await?;
    // The release records the BaseRegistrar token owner it ended, which for a wrapped lease is
    // the NameWrapper contract.
    sqlx::query(
        "UPDATE normalized_events
         SET before_state = jsonb_build_object('registrant', $1::text, 'expiry', 4242)
         WHERE event_identity = 'fixture:lapsed-release'",
    )
    .bind(WRAPPER_CONTRACT)
    .execute(&pool)
    .await?;
    sqlx::query(
        "UPDATE surface_bindings SET active_to = '2026-08-01T00:00:11Z'
         WHERE surface_binding_id = $1::uuid",
    )
    .bind(CONTROL_BINDING)
    .execute(&pool)
    .await?;
    if incremental {
        run_project(&pool, 11, 11, Some(9)).await?;
    } else {
        run_project(&pool, 11, 8, None).await?;
    }
    let row: serde_json::Value = sqlx::query_scalar(
        "SELECT jsonb_build_object(
             'support_status', support_status, 'unsupported_reason', unsupported_reason,
             'surface_binding_id', surface_binding_id, 'resource_id', resource_id,
             'declared_summary', declared_summary,
             'authority_selection', provenance -> 'authority_selection')
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(OWNERLESS_LOGICAL)
    .fetch_one(&pool)
    .await?;
    database.cleanup().await?;
    Ok(row)
}

fn assert_released_tombstone(row: &serde_json::Value) {
    assert_eq!(row["support_status"], "supported", "{row:#}");
    assert!(row["unsupported_reason"].is_null(), "{row:#}");
    let registration = &row["declared_summary"]["registration"];
    assert_eq!(registration["status"], "released", "{row:#}");
    assert!(registration["registrant"].is_null(), "{row:#}");
    assert!(registration["authority_kind"].is_null(), "{row:#}");
    assert!(registration["authority_key"].is_null(), "{row:#}");
    // The lapsed lease keeps its own expiry, not the NameWrapper's later one.
    assert_eq!(registration["expiry"], 4242, "{row:#}");
    assert_eq!(registration["resource_id"], OWNERLESS_RESOURCE, "{row:#}");
    // The previous holder is served only inside the lapsed block.
    let lapsed = &registration["lapsed_registration"];
    assert_eq!(lapsed["authority_kind"], "wrapper", "{row:#}");
    assert_eq!(lapsed["authority_key"], "wrapper:born", "{row:#}");
    assert_eq!(lapsed["released_at"], 7_780_243, "{row:#}");
    // Follow the chain: the lapsed holder is the NameWrapper token owner at the release, not
    // the NameWrapper contract that held the BaseRegistrar token, nor an earlier token owner.
    assert_eq!(
        lapsed["registrant"],
        LAPSED_LAST_HOLDER.to_lowercase(),
        "{row:#}"
    );
    assert_eq!(
        row["declared_summary"]["control"],
        json!({"status": "unregistered"})
    );
    assert_eq!(
        row["authority_selection"]["resource_authority_context"]["released_tombstone"],
        "ens_v1"
    );
}

/// Once the registrar lease lapses past grace the name is available again: `ownerOf` reverts and
/// `available` is true. The wrapper binding stands for the released lease.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L71-L76 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L101-L104 @ ens_v1@91c966f)
#[tokio::test]
async fn lapsed_born_wrapped_lease_serves_a_released_tombstone() -> Result<()> {
    let incremental = born_wrapped_projection(BornWrappedShape::LinkRecorded, true).await?;
    let from_zero = born_wrapped_projection(BornWrappedShape::LinkRecorded, false).await?;
    assert_eq!(incremental, from_zero);
    assert_released_tombstone(&incremental);
    Ok(())
}

/// The same lapse where a controller event granted the lease after `NameWrapped`, so the wrap
/// recorded no lease and the named grant in the wrap's transaction identifies it.
#[tokio::test]
async fn lapsed_controller_granted_born_wrapped_lease_serves_a_released_tombstone() -> Result<()> {
    let incremental = born_wrapped_projection(BornWrappedShape::ControllerGranted, true).await?;
    let from_zero = born_wrapped_projection(BornWrappedShape::ControllerGranted, false).await?;
    assert_eq!(incremental, from_zero);
    assert_released_tombstone(&incremental);
    Ok(())
}

/// A lease that was never wrapped becomes a released tombstone when no ENSv1 registry owner can
/// take the node over at the release: `registerOnly` registers without writing the registry, and
/// an owner word the adapter cannot authenticate leaves no owner either. The adapter then closes
/// the registrar binding and opens none. The lapsed block names the BaseRegistrar token owner at
/// the release and `registrar` as what held the lease.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L118-L127 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L147-L150 @ ens_v1@91c966f)
async fn unwrapped_lapse_projection(incremental: bool) -> Result<serde_json::Value> {
    let (database, pool) = migrated_pool().await?;
    seed_chain(&pool).await?;
    seed_blocks(&pool, [11]).await?;
    seed_surface(
        &pool,
        OWNERLESS_NAMEHASH,
        "lapsed-unwrapped.eth",
        CONTROL_RESOURCE,
        CONTROL_BINDING,
    )
    .await?;
    seed_binding_provenance(&pool, CONTROL_BINDING, 0, 1).await?;
    let registrar = json!({"source_event":"NameRegistered","authority_kind":"registrar","authority_key":"registrar:plain","registrant":CONTROL_OWNER,"authority_owner":CONTROL_OWNER,"expiry":4242,"namehash":OWNERLESS_NAMEHASH});
    for (identity, kind, block, state) in [
        (
            "fixture:plain-grant",
            "RegistrationGranted",
            8,
            registrar.clone(),
        ),
        (
            "fixture:plain-expiry",
            "ExpiryChanged",
            8,
            registrar.clone(),
        ),
        (
            "fixture:plain-binding",
            "SurfaceBound",
            8,
            registrar.clone(),
        ),
        (
            "fixture:plain-epoch",
            "AuthorityEpochChanged",
            8,
            registrar.clone(),
        ),
        // The BaseRegistrar token changes hands before the lease lapses.
        (
            "fixture:plain-transfer",
            "TokenControlTransferred",
            10,
            json!({"source_event":"Transfer","to":LAPSED_LAST_HOLDER,"namehash":OWNERLESS_NAMEHASH}),
        ),
    ] {
        seed_normalized_event(
            &pool,
            identity,
            Some(OWNERLESS_LOGICAL),
            Some(CONTROL_RESOURCE),
            kind,
            "ens_v1_registrar_l1",
            block,
            1,
            state,
            json!({}),
        )
        .await?;
    }
    if incremental {
        run_project(&pool, 9, 8, None).await?;
    }
    for (identity, kind, state) in [
        (
            "fixture:plain-release",
            "RegistrationReleased",
            json!({"source_event":"RegistrationReleased","expiry":4242,"namehash":OWNERLESS_NAMEHASH,"released_at":7_780_243}),
        ),
        (
            "fixture:plain-unbound",
            "SurfaceUnbound",
            json!({"source_event":"RegistrationReleased","authority_kind":"registrar","authority_key":"registrar:plain","active_to":7_780_243}),
        ),
        (
            "fixture:plain-closing-epoch",
            "AuthorityEpochChanged",
            json!({"source_event":"RegistrationReleased","authority_kind":null,"authority_key":null,"owner":null}),
        ),
    ] {
        seed_normalized_event(
            &pool,
            identity,
            Some(OWNERLESS_LOGICAL),
            Some(CONTROL_RESOURCE),
            kind,
            "ens_v1_registrar_l1",
            11,
            0,
            state,
            json!({}),
        )
        .await?;
    }
    sqlx::query(
        "UPDATE normalized_events
         SET transaction_hash = NULL, transaction_index = NULL, log_index = NULL
         WHERE event_identity IN (
             'fixture:plain-release', 'fixture:plain-unbound', 'fixture:plain-closing-epoch'
         )",
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        "UPDATE normalized_events
         SET before_state = jsonb_build_object('registrant', $1::text, 'expiry', 4242)
         WHERE event_identity = 'fixture:plain-release'",
    )
    .bind(LAPSED_LAST_HOLDER)
    .execute(&pool)
    .await?;
    sqlx::query(
        "UPDATE surface_bindings SET active_to = '2026-08-01T00:00:11Z'
         WHERE surface_binding_id = $1::uuid",
    )
    .bind(CONTROL_BINDING)
    .execute(&pool)
    .await?;
    if incremental {
        run_project(&pool, 11, 11, Some(9)).await?;
    } else {
        run_project(&pool, 11, 8, None).await?;
    }
    let row: serde_json::Value = sqlx::query_scalar(
        "SELECT jsonb_build_object(
             'support_status', support_status, 'resource_id', resource_id,
             'declared_summary', declared_summary,
             'authority_selection', provenance -> 'authority_selection')
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(OWNERLESS_LOGICAL)
    .fetch_one(&pool)
    .await?;
    database.cleanup().await?;
    Ok(row)
}

#[tokio::test]
async fn lapsed_unwrapped_lease_with_no_registry_owner_names_the_registrar() -> Result<()> {
    let incremental = unwrapped_lapse_projection(true).await?;
    let from_zero = unwrapped_lapse_projection(false).await?;
    assert_eq!(incremental, from_zero);
    let row = &incremental;
    assert_eq!(row["support_status"], "supported", "{row:#}");
    let registration = &row["declared_summary"]["registration"];
    assert_eq!(registration["status"], "released", "{row:#}");
    assert!(registration["registrant"].is_null(), "{row:#}");
    assert_eq!(registration["expiry"], 4242, "{row:#}");
    assert_eq!(
        registration["lapsed_registration"],
        json!({
            "registrant": LAPSED_LAST_HOLDER.to_lowercase(),
            "authority_kind": "registrar",
            "authority_key": "registrar:plain",
            "released_at": 7_780_243,
        }),
        "{row:#}"
    );
    assert_eq!(
        row["authority_selection"]["resource_authority_context"]["released_tombstone"],
        "ens_v1"
    );
    Ok(())
}

/// Issue #908: only the NameWrapper's own expiry has passed; the registrar lease was renewed and
/// is live. Nothing was released, so the wrapper binding must not become a released tombstone.
#[tokio::test]
async fn wrapper_expiry_alone_does_not_select_a_released_tombstone() -> Result<()> {
    let incremental = born_wrapped_projection(BornWrappedShape::WrapperExpiryOnly, true).await?;
    let from_zero = born_wrapped_projection(BornWrappedShape::WrapperExpiryOnly, false).await?;
    assert_eq!(incremental, from_zero);
    assert!(
        incremental["authority_selection"]["resource_authority_context"]["released_tombstone"]
            .is_null(),
        "{incremental:#}"
    );
    assert_ne!(
        incremental["declared_summary"]["registration"]["status"], "released",
        "{incremental:#}"
    );
    assert!(
        incremental["declared_summary"]["registration"]["released_at"].is_null(),
        "{incremental:#}"
    );
    assert!(
        incremental["declared_summary"]["registration"]
            .get("lapsed_registration")
            .is_none(),
        "a name that is not released carries no lapsed block: {incremental:#}"
    );
    Ok(())
}

type SnapshotRegistrationRow = (
    Option<String>,
    Option<i64>,
    Option<String>,
    Option<bool>,
    Option<String>,
);

/// The registrar surface snapshot and the resource join describe the same registration. A
/// name-less numeric grant whose resource is later bound, plus the state-derived snapshot grant
/// emitted when another source disclosed the label, must serve one registration dated at the
/// original grant.
#[tokio::test]
async fn snapshot_grant_and_bound_original_grant_serve_one_registration() -> Result<()> {
    let (database, pool) = migrated_pool().await?;
    seed_chain(&pool).await?;
    sqlx::query(
        "INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1::uuid, $2, $3, 8, 'canonical')",
    )
    .bind(OWNERLESS_RESOURCE)
    .bind(CHAIN)
    .bind(block_hash(8))
    .execute(&pool)
    .await?;
    let original_registered_at: i64 = sqlx::query_scalar(
        "SELECT extract(epoch FROM block_timestamp)::bigint FROM chain_lineage
         WHERE chain_id = $1 AND block_number = 8",
    )
    .bind(CHAIN)
    .fetch_one(&pool)
    .await?;
    let lease = json!({
        "source_event": "NameRegistered",
        "authority_kind": "registrar",
        "authority_key": "registrar:snapshot",
        "registrant": CONTROL_OWNER,
        "expiry": 4242,
        "namehash": OWNERLESS_NAMEHASH,
    });
    for (kind, log) in [("RegistrationGranted", 1), ("ExpiryChanged", 2)] {
        seed_normalized_event(
            &pool,
            &format!("fixture:snapshot-original-{kind}"),
            None,
            Some(OWNERLESS_RESOURCE),
            kind,
            "ens_v1_registrar_l1",
            8,
            log,
            lease.clone(),
            json!({}),
        )
        .await?;
    }
    seed_surface(
        &pool,
        OWNERLESS_NAMEHASH,
        "disclosed.eth",
        OWNERLESS_RESOURCE,
        OWNERLESS_BINDING,
    )
    .await?;
    // The registrar lease is an ERC-721 token; the registrant relation is served for tokens.
    sqlx::query(
        "INSERT INTO token_lineages (
             token_lineage_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1::uuid, $2, $3, 8, 'canonical')",
    )
    .bind(WRAPPER_LINEAGE)
    .bind(CHAIN)
    .bind(block_hash(8))
    .execute(&pool)
    .await?;
    sqlx::query("UPDATE resources SET token_lineage_id = $1::uuid WHERE resource_id = $2::uuid")
        .bind(WRAPPER_LINEAGE)
        .bind(OWNERLESS_RESOURCE)
        .execute(&pool)
        .await?;
    let mut snapshot = lease.clone();
    for (key, value) in [
        ("state_derived", json!(true)),
        ("surface_materialization", json!(true)),
        ("registrar_surface_snapshot", json!(true)),
        ("original_registered_at", json!(original_registered_at)),
    ] {
        snapshot[key] = value;
    }
    for (kind, log) in [("RegistrationGranted", 1), ("ExpiryChanged", 2)] {
        seed_normalized_event(
            &pool,
            &format!("fixture:snapshot-{kind}"),
            Some(OWNERLESS_LOGICAL),
            Some(OWNERLESS_RESOURCE),
            kind,
            "ens_v1_registrar_l1",
            9,
            log,
            snapshot.clone(),
            json!({}),
        )
        .await?;
    }
    run_project(&pool, 9, 8, None).await?;
    let (status, expiry, resource, registered_matches, registrant): SnapshotRegistrationRow =
        sqlx::query_as(
            "SELECT declared_summary #>> '{registration,status}',
                (declared_summary #>> '{registration,expiry}')::bigint,
                declared_summary #>> '{registration,resource_id}',
                (declared_summary #>> '{registration,registered_at}')::timestamptz =
                    to_timestamp($2),
                declared_summary #>> '{registration,registrant}'
         FROM name_current WHERE logical_name_id = $1",
        )
        .bind(OWNERLESS_LOGICAL)
        .bind(original_registered_at as f64)
        .fetch_one(&pool)
        .await?;
    assert_eq!(status.as_deref(), Some("active"));
    assert_eq!(expiry, Some(4242));
    assert_eq!(resource.as_deref(), Some(OWNERLESS_RESOURCE));
    assert_eq!(
        registered_matches,
        Some(true),
        "registered_at must be the original grant's block time"
    );
    assert_eq!(registrant, Some(CONTROL_OWNER.to_lowercase()));
    let registrant_relations: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM address_names_current
         WHERE logical_name_id = $1 AND relation = 'registrant'",
    )
    .bind(OWNERLESS_LOGICAL)
    .fetch_one(&pool)
    .await?;
    assert_eq!(registrant_relations, 1);
    database.cleanup().await?;
    Ok(())
}

#[derive(Debug, PartialEq)]
struct WrapperLapseStep {
    status: Option<String>,
    expiry: Option<i64>,
    registrant: Option<String>,
    wrapper_state: Option<String>,
    has_lapsed_block: bool,
    released_at: Option<String>,
    relations: Vec<(String, String)>,
}

/// Issue #908, the four steps of its scenario. Renewing a wrapped `.eth` name directly on the
/// BaseRegistrar extends the lease but not the NameWrapper's own expiry. Once that expiry passes
/// the NameWrapper reports no owner for the name, although the lease is live and nothing was
/// released; a later renewal through the NameWrapper restores the owner.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843-L856 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1013 @ ens_v1@91c966f)
async fn wrapper_only_lapse_projection(step: i64, incremental: bool) -> Result<WrapperLapseStep> {
    const GRACE: i64 = 7_776_000;
    const EMANCIPATED_DOT_ETH: i64 = 65_536 + 131_072;
    const WRAPPER_CONTRACT: &str = "0x9999999999999999999999999999999999999999";
    let (database, pool) = migrated_pool().await?;
    seed_chain(&pool).await?;
    seed_blocks(&pool, [11]).await?;
    let block_time: i64 = sqlx::query_scalar(
        "SELECT extract(epoch FROM block_timestamp)::bigint FROM chain_lineage
         WHERE chain_id = $1 AND block_number = 9",
    )
    .bind(CHAIN)
    .fetch_one(&pool)
    .await?;
    // The NameWrapper expiry falls between blocks 9 and 10; the renewed lease outlives it.
    let wrapper_expiry = block_time;
    let renewed_lease = wrapper_expiry + 9 * GRACE;
    sqlx::query(
        "INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1::uuid, $2, $3, 8, 'canonical')",
    )
    .bind(OWNERLESS_RESOURCE)
    .bind(CHAIN)
    .bind(block_hash(8))
    .execute(&pool)
    .await?;
    seed_surface(
        &pool,
        OWNERLESS_NAMEHASH,
        "wrapper-only-lapse.eth",
        CONTROL_RESOURCE,
        CONTROL_BINDING,
    )
    .await?;
    seed_binding_provenance(&pool, CONTROL_BINDING, 0, 2).await?;
    sqlx::query(
        "INSERT INTO token_lineages (
             token_lineage_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1::uuid, $2, $3, 8, 'canonical')",
    )
    .bind(WRAPPER_LINEAGE)
    .bind(CHAIN)
    .bind(block_hash(8))
    .execute(&pool)
    .await?;
    sqlx::query("UPDATE resources SET token_lineage_id = $1::uuid WHERE resource_id = $2::uuid")
        .bind(WRAPPER_LINEAGE)
        .bind(CONTROL_RESOURCE)
        .execute(&pool)
        .await?;
    let lease = |expiry: i64| json!({"source_event":"NameRegistered","authority_kind":"registrar","authority_key":"registrar:lapse","registrant":WRAPPER_CONTRACT,"expiry":expiry,"namehash":OWNERLESS_NAMEHASH});
    let wrapper = |expiry: i64| json!({"source_event":"NameWrapped","node":OWNERLESS_NAMEHASH,"authority_kind":"wrapper","authority_key":"wrapper:lapse","wrapped_registrar_resource_id":OWNERLESS_RESOURCE,"wrapper_state":"emancipated","fuses":EMANCIPATED_DOT_ETH,"expiry":expiry});
    let mut holder = wrapper(wrapper_expiry);
    holder["to"] = json!(CONTROL_OWNER);
    // Step 1, block 8: the name is registered through the NameWrapper.
    let mut events = vec![
        (
            "grant",
            None,
            OWNERLESS_RESOURCE,
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            8,
            1,
            lease(wrapper_expiry - GRACE),
        ),
        (
            "lease-expiry",
            None,
            OWNERLESS_RESOURCE,
            "ExpiryChanged",
            "ens_v1_registrar_l1",
            8,
            1,
            lease(wrapper_expiry - GRACE),
        ),
        (
            "holder",
            Some(OWNERLESS_LOGICAL),
            CONTROL_RESOURCE,
            "TokenControlTransferred",
            "ens_v1_wrapper_l1",
            8,
            2,
            holder,
        ),
        (
            "wrapper-expiry",
            Some(OWNERLESS_LOGICAL),
            CONTROL_RESOURCE,
            "ExpiryChanged",
            "ens_v1_wrapper_l1",
            8,
            2,
            wrapper(wrapper_expiry),
        ),
        (
            "scope",
            Some(OWNERLESS_LOGICAL),
            CONTROL_RESOURCE,
            "PermissionScopeChanged",
            "ens_v1_wrapper_l1",
            8,
            2,
            wrapper(wrapper_expiry),
        ),
        (
            "binding",
            Some(OWNERLESS_LOGICAL),
            CONTROL_RESOURCE,
            "SurfaceBound",
            "ens_v1_wrapper_l1",
            8,
            2,
            wrapper(wrapper_expiry),
        ),
        (
            "epoch",
            Some(OWNERLESS_LOGICAL),
            CONTROL_RESOURCE,
            "AuthorityEpochChanged",
            "ens_v1_wrapper_l1",
            8,
            2,
            wrapper(wrapper_expiry),
        ),
    ];
    // Step 2, block 9: a renewal on the BaseRegistrar alone.
    // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L157-L169 @ ens_v1@91c966f)
    let renewal = json!({"source_event":"NameRenewed","authority_kind":"registrar","expiry":renewed_lease,"namehash":OWNERLESS_NAMEHASH});
    events.push((
        "direct-renewal",
        None,
        OWNERLESS_RESOURCE,
        "RegistrationRenewed",
        "ens_v1_registrar_l1",
        9,
        1,
        renewal.clone(),
    ));
    events.push((
        "direct-renewal-expiry",
        None,
        OWNERLESS_RESOURCE,
        "ExpiryChanged",
        "ens_v1_registrar_l1",
        9,
        2,
        renewal,
    ));
    // Step 3, block 10, has no events: only the NameWrapper expiry passes.
    // Step 4, block 11: a renewal through the NameWrapper restores its stored expiry.
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L312-L340 @ ens_v1@91c966f)
    if step == 4 {
        events.push(("wrapped-renewal", Some(OWNERLESS_LOGICAL), CONTROL_RESOURCE, "ExpiryChanged", "ens_v1_registrar_l1", 11, 1,
            json!({"source_event":"NameRenewed","node":OWNERLESS_NAMEHASH,"authority_kind":"wrapper","emitter_role":"wrapped_registrar_controller","expiry":renewed_lease + GRACE,"registrar_expiry":renewed_lease})));
    }
    for (identity, logical, resource, kind, family, block, log, state) in events {
        seed_normalized_event(
            &pool,
            &format!("fixture:wrapper-lapse-{identity}"),
            logical,
            Some(resource),
            kind,
            family,
            block,
            log,
            state,
            json!({"emitting_address":WRAPPER_CONTRACT}),
        )
        .await?;
    }
    let target = match step {
        1 => 8,
        2 => 9,
        3 => 10,
        _ => 11,
    };
    if incremental {
        let mut previous = None;
        for block in 8..=target {
            run_project(&pool, block, previous.map_or(8, |_| block), previous).await?;
            previous = Some(block);
        }
    } else {
        run_project(&pool, target, 8, None).await?;
    }
    let (status, expiry, registrant, wrapper_state, has_lapsed_block, released_at) =
        sqlx::query_as(
            "SELECT declared_summary #>> '{registration,status}',
                (declared_summary #>> '{registration,expiry}')::bigint,
                declared_summary #>> '{registration,registrant}',
                declared_summary ->> 'wrapper_state',
                (declared_summary -> 'registration') ? 'lapsed_registration',
                declared_summary #>> '{registration,released_at}'
         FROM name_current WHERE logical_name_id = $1",
        )
        .bind(OWNERLESS_LOGICAL)
        .fetch_one(&pool)
        .await?;
    let relations = sqlx::query_as(
        "SELECT relation, address FROM address_names_current
         WHERE logical_name_id = $1 ORDER BY relation, address",
    )
    .bind(OWNERLESS_LOGICAL)
    .fetch_all(&pool)
    .await?;
    database.cleanup().await?;
    Ok(WrapperLapseStep {
        status,
        expiry,
        registrant,
        wrapper_state,
        has_lapsed_block,
        released_at,
        relations,
    })
}

#[tokio::test]
async fn wrapper_only_lapse_serves_no_registrant_until_a_wrapper_renewal() -> Result<()> {
    const GRACE: i64 = 7_776_000;
    let holder = CONTROL_OWNER.to_lowercase();
    let mut steps = Vec::new();
    for step in 1..=4 {
        let incremental = wrapper_only_lapse_projection(step, true).await?;
        let from_zero = wrapper_only_lapse_projection(step, false).await?;
        assert_eq!(
            incremental, from_zero,
            "step {step} diverged from a rebuild"
        );
        steps.push(incremental);
    }
    let lease_expiry = steps[0].expiry.expect("step 1 lease expiry");
    let renewed = lease_expiry + 10 * GRACE;
    for (index, step) in steps.iter().enumerate() {
        let number = index + 1;
        assert_eq!(
            step.status.as_deref(),
            Some("active"),
            "step {number}: {step:?}"
        );
        assert_eq!(
            step.released_at, None,
            "step {number}: nothing was released"
        );
        assert!(
            !step.has_lapsed_block,
            "step {number}: the name is not released"
        );
        assert_eq!(
            step.expiry,
            Some(if number == 1 { lease_expiry } else { renewed }),
            "step {number} serves the live registrar expiry"
        );
        let lapsed = number == 3;
        assert_eq!(
            step.registrant.as_deref(),
            (!lapsed).then_some(holder.as_str()),
            "step {number}: {step:?}"
        );
        assert_eq!(
            step.wrapper_state.as_deref(),
            (!lapsed).then_some("emancipated"),
            "step {number}: {step:?}"
        );
        assert_eq!(
            step.relations
                .iter()
                .any(|(relation, address)| relation == "registrant" && *address == holder),
            !lapsed,
            "step {number}: {step:?}"
        );
        assert_eq!(
            step.relations
                .iter()
                .any(|(relation, _)| relation == "token_holder"),
            !lapsed,
            "step {number}: {step:?}"
        );
    }
    Ok(())
}

/// What `name_current` publishes about where a name came from: `created_at`, the provenance event
/// ids and the control fields.
async fn name_origin(pool: &PgPool, logical_name_id: &str) -> Result<serde_json::Value> {
    Ok(sqlx::query_scalar(
        "SELECT jsonb_build_object(
             'created_at', (declared_summary #>> '{registration,created_at}')::timestamptz,
             'selected_event_ids', (
                 SELECT COALESCE(jsonb_agg(event.event_identity ORDER BY event.event_identity),
                                 '[]'::jsonb)
                 FROM normalized_events event
                 WHERE to_jsonb(event.normalized_event_id) <@
                       (name_current.provenance -> 'selected_event_ids')
             ),
             'raw_fact_refs', jsonb_array_length(provenance -> 'raw_fact_refs'),
             'manifest_versions', jsonb_array_length(provenance -> 'manifest_versions'),
             'control', declared_summary -> 'control'
         )
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(logical_name_id)
    .fetch_one(pool)
    .await?)
}

/// The ENSv1 registry adapter writes rows that carry a resource but no name while a label is
/// unknown. Naming rows by resource identity exists for `.eth` BaseRegistrar lifecycle rows only:
/// a registry row written before the label was known must stay out of the name's `created_at` and
/// provenance lists, as it did before registrar rows were joined by resource identity.
#[tokio::test]
async fn registry_rows_without_a_name_stay_out_of_the_names_origin() -> Result<()> {
    const OWNER: &str = "0x7777777777777777777777777777777777777777";
    let (database, pool) = migrated_pool().await?;
    seed_chain(&pool).await?;
    seed_surface(
        &pool,
        OWNERLESS_NAMEHASH,
        "label-learned-later.eth",
        OWNERLESS_RESOURCE,
        OWNERLESS_BINDING,
    )
    .await?;
    let owner_state = json!({
        "node": OWNERLESS_NAMEHASH,
        "owner": OWNER,
        "owner_getter": OWNER,
        "authority_kind": "registry_only"
    });
    seed_normalized_event(
        &pool,
        "fixture:registry-row-before-label",
        None,
        Some(OWNERLESS_RESOURCE),
        "AuthorityTransferred",
        "ens_v1_registry_l1",
        8,
        1,
        owner_state.clone(),
        json!({"emitting_address": REGISTRY_ADDRESS}),
    )
    .await?;
    seed_normalized_event(
        &pool,
        "fixture:registry-row-after-label",
        Some(OWNERLESS_LOGICAL),
        Some(OWNERLESS_RESOURCE),
        "AuthorityTransferred",
        "ens_v1_registry_l1",
        9,
        1,
        owner_state,
        json!({"emitting_address": REGISTRY_ADDRESS}),
    )
    .await?;
    run_project(&pool, 9, 8, None).await?;

    let origin = name_origin(&pool, OWNERLESS_LOGICAL).await?;
    assert_eq!(
        origin["selected_event_ids"],
        json!(["fixture:registry-row-after-label"]),
        "a registry row written without a name entered the name's provenance: {origin}"
    );
    assert_eq!(origin["raw_fact_refs"], json!(1), "{origin}");
    assert_eq!(origin["manifest_versions"], json!(1), "{origin}");
    let created_at_block: i64 = sqlx::query_scalar(
        "SELECT block_number FROM chain_lineage
         WHERE chain_id = $1 AND block_timestamp = ($2 #>> '{}')::timestamptz",
    )
    .bind(CHAIN)
    .bind(&origin["created_at"])
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        created_at_block, 9,
        "created_at moved back to a registry row written without a name: {origin}"
    );
    database.cleanup().await?;
    Ok(())
}
