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
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, resource_id, event_kind,
             source_family, manifest_version, chain_id, block_number, block_hash,
             transaction_hash, transaction_index, log_index, derivation_kind,
             canonicality_state, after_state, raw_fact_ref
         ) VALUES (
             $1, $2, $3, $4::uuid, $5, $6, 1, $7, $8, $9,
             $10, 0, $11, 'ens_v1_unwrapped_authority', 'canonical', $12, $13
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

async fn ownerless_serving_projection_snapshot(
    pool: &PgPool,
) -> Result<Vec<(String, serde_json::Value)>> {
    let mut snapshot = serving_projection_snapshot(pool).await?;
    for (table, rows) in &mut snapshot {
        let Some(rows) = rows.as_array_mut() else {
            continue;
        };
        rows.retain(|row| match table.as_str() {
            "name_current" => row["logical_name_id"] == OWNERLESS_LOGICAL,
            "children_current" => row["child_logical_name_id"] == OWNERLESS_LOGICAL,
            "permissions_current"
            | "permissions_current_resource_summary"
            | "record_inventory_current" => row["resource_id"] == OWNERLESS_RESOURCE,
            "resolver_current" => row["resolver_address"] == RESOLVER_ADDRESS,
            "address_names_current" => {
                row["logical_name_id"] == OWNERLESS_LOGICAL
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
    let incremental_record_change = ownerless_serving_projection_snapshot(&pool).await?;
    run_project(&pool, 10, 8, None).await?;
    assert_eq!(
        incremental_record_change,
        ownerless_serving_projection_snapshot(&pool).await?,
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
                (SELECT count(*) FROM record_inventory_current
                 WHERE resource_id = $2::uuid)
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
    let incremental: String = sqlx::query_scalar(
        "SELECT entries -> 0 ->> 'value'
         FROM record_inventory_current WHERE resource_id = $1::uuid",
    )
    .bind(OWNERLESS_RESOURCE)
    .fetch_one(&pool)
    .await?;
    run_project(&pool, 10, 8, None).await?;
    let fresh: String = sqlx::query_scalar(
        "SELECT entries -> 0 ->> 'value'
         FROM record_inventory_current WHERE resource_id = $1::uuid",
    )
    .bind(OWNERLESS_RESOURCE)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        (incremental.as_str(), fresh.as_str()),
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
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, resource_id, event_kind,
             source_family, manifest_version, chain_id, block_number, block_hash,
             transaction_hash, transaction_index, log_index, derivation_kind,
             canonicality_state, after_state
         ) VALUES (
             $1, 'ens', $2, $3::uuid, 'AuthorityEpochChanged',
             'ens_v1_registry_l1', 1, $4, $5, $6,
             $7, 0, 7, 'ens_v1_unwrapped_authority',
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
    .bind(json!({"authority_kind": authority_kind}))
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
