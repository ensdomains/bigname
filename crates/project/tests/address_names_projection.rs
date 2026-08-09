//! Builder-level coverage for the archived-registry masked owner word: an
//! `AuthorityTransferred` whose `after_state` carries `owner_word_unmasked`
//! authenticates no caller, so it must clear the effective controller with the
//! same shape a zero-owner transition produces, and must never publish the
//! masked low-20-byte tail as a controller.

use anyhow::Result;
use bigname_project::{BatchRequest, Engine, RunMode};
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

fn quote_identifier(identifier: &str) -> String {
    format!(r#""{}""#, identifier.replace('"', r#""""#))
}

async fn seed_chain(pool: &PgPool) -> Result<()> {
    for number in [8_i64, 9, 10] {
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
             active_from, chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             $1::uuid, $2, $3::uuid, 'declared_registry_path',
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
