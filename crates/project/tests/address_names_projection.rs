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
const OLD_REGISTRAR_RESOURCE: &str = "abababab-abab-abab-abab-abababababab";
const OWNERLESS_BINDING: &str = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee";
const RELEASE_REGISTRY_RESOURCE: &str = "edededed-eded-eded-eded-edededededed";
const RELEASE_REGISTRY_BINDING: &str = "efefefef-efef-efef-efef-efefefefefef";
const REWRAPPED_RESOURCE: &str = "acacacac-acac-acac-acac-acacacacacac";
const REWRAPPED_BINDING: &str = "adadadad-adad-adad-adad-adadadadadad";
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

#[rustfmt::skip]
async fn registrar_reveal_projection(split: bool) -> Result<((String, i64, String, String, i64), Vec<(String, serde_json::Value)>)> {
    let (database, pool) = migrated_pool().await?;
    seed_chain(&pool).await?;
    sqlx::query("INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state) VALUES ($1::uuid, $2, $3, 8, 'canonical')")
        .bind(OWNERLESS_RESOURCE).bind(CHAIN).bind(block_hash(8)).execute(&pool).await?;
    for (kind, index) in [("RegistrationGranted", 1), ("ExpiryChanged", 2)] {
        seed_normalized_event(&pool, &format!("fixture:surface-less-{kind}"), None, Some(OWNERLESS_RESOURCE), kind, "ens_v1_registrar_l1", 8, index, json!({"source_event":"NameRegistered","authority_kind":"registrar","authority_key":"registrar:fixture","registrant":CONTROL_OWNER,"expiry":4242,"namehash":OWNERLESS_NAMEHASH}), json!({})).await?;
    }
    if split { run_project(&pool, 8, 8, None).await?; }
    seed_surface(&pool, OWNERLESS_NAMEHASH, "revealed.eth", OWNERLESS_RESOURCE, OWNERLESS_BINDING).await?;
    seed_normalized_event(&pool, "fixture:revealed-resolver", Some(OWNERLESS_LOGICAL), Some(OWNERLESS_RESOURCE), "ResolverChanged", "ens_v1_registry_l1", 9, 1, json!({"source_event":"NewResolver","node":OWNERLESS_NAMEHASH,"resolver":RESOLVER_ADDRESS}), json!({"emitting_address":REGISTRY_ADDRESS})).await?;
    seed_normalized_event(&pool, "fixture:revealed-record", Some(OWNERLESS_LOGICAL), None, "RecordChanged", "ens_v1_resolver_l1", 9, 2, json!({"node":OWNERLESS_NAMEHASH,"record_family":"text","record_key":"text:description","selector_key":"description","value":"revealed incrementally"}), json!({"emitting_address":RESOLVER_ADDRESS})).await?;
    run_project(&pool, 9, 8, split.then_some(8)).await?;
    let summary = sqlx::query_as("SELECT declared_summary #>> '{registration,status}', (declared_summary #>> '{registration,expiry}')::bigint, resource_id::text, declared_summary #>> '{resolver,address}', (SELECT count(*) FROM record_inventory_current WHERE resource_id = $2::uuid) FROM name_current WHERE logical_name_id = $1")
        .bind(OWNERLESS_LOGICAL).bind(OWNERLESS_RESOURCE).fetch_one(&pool).await?;
    let snapshot = serving_projection_snapshot(&pool).await?;
    database.cleanup().await?;
    Ok((summary, snapshot))
}

#[tokio::test]
#[rustfmt::skip]
async fn registrar_only_then_enrichment_projects_name_addressable_registration() -> Result<()> { let (summary, _) = registrar_reveal_projection(false).await?; assert_eq!(summary, ("active".to_owned(), 4242, OWNERLESS_RESOURCE.to_owned(), RESOLVER_ADDRESS.to_lowercase(), 1)); Ok(()) }

#[tokio::test]
#[rustfmt::skip]
async fn registrar_only_then_enrichment_converges_across_project_batches() -> Result<()> { assert_eq!(registrar_reveal_projection(false).await?, registrar_reveal_projection(true).await?); Ok(()) }

#[tokio::test]
#[rustfmt::skip]
async fn resource_keyed_registrar_event_does_not_backfill_a_different_surface_on_shared_resource() -> Result<()> {
    let (database, pool) = migrated_pool().await?; seed_chain(&pool).await?;
    seed_surface(&pool, CONTROL_NAMEHASH, "control.eth", OWNERLESS_RESOURCE, CONTROL_BINDING).await?;
    seed_normalized_event(&pool, "fixture:unrelated-resource-registration", None, Some(OWNERLESS_RESOURCE), "RegistrationGranted", "ens_v1_registrar_l1", 8, 1, json!({"source_event":"NameRegistered","authority_kind":"registrar","authority_key":"registrar:unrelated","registrant":CONTROL_OWNER,"expiry":4242,"namehash":OWNERLESS_NAMEHASH}), json!({})).await?;
    run_project(&pool, 8, 8, None).await?;
    let expiry: Option<i64> = sqlx::query_scalar("SELECT (declared_summary #>> '{registration,expiry}')::bigint FROM name_current WHERE logical_name_id = $1").bind(CONTROL_LOGICAL).fetch_one(&pool).await?;
    assert_eq!(expiry, None); database.cleanup().await?; Ok(())
}

#[tokio::test]
#[rustfmt::skip]
async fn later_wrapper_projection_joins_only_the_wrapped_registrar_lineage() -> Result<()> {
    const LATEST_WRAPPER_OWNER: &str = "0x7777777777777777777777777777777777777777";
    const WRAPPER_CONTRACT: &str = "0x9999999999999999999999999999999999999999";
    let (database, pool) = migrated_pool().await?; seed_chain(&pool).await?;
    for resource in [OLD_REGISTRAR_RESOURCE, OWNERLESS_RESOURCE] { sqlx::query("INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state) VALUES ($1::uuid, $2, $3, 8, 'canonical')").bind(resource).bind(CHAIN).bind(block_hash(8)).execute(&pool).await?; }
    seed_surface(&pool, OWNERLESS_NAMEHASH, "wrapped.eth", CONTROL_RESOURCE, CONTROL_BINDING).await?;
    sqlx::query("INSERT INTO token_lineages (token_lineage_id, chain_id, block_hash, block_number, canonicality_state) VALUES ($1::uuid, $2, $3, 8, 'canonical')").bind(WRAPPER_LINEAGE).bind(CHAIN).bind(block_hash(8)).execute(&pool).await?;
    sqlx::query("UPDATE resources SET token_lineage_id = $1::uuid WHERE resource_id = $2::uuid").bind(WRAPPER_LINEAGE).bind(CONTROL_RESOURCE).execute(&pool).await?;
    for (identity, logical, resource, kind, family, block, log, state) in [
        ("fixture:old-registration", None, OLD_REGISTRAR_RESOURCE, "RegistrationGranted", "ens_v1_registrar_l1", 8, 0, json!({"source_event":"NameRegistered","authority_kind":"registrar","authority_key":"registrar:old","registrant":PRIOR_CONTROLLER,"expiry":1111,"namehash":OWNERLESS_NAMEHASH})),
        ("fixture:wrapped-registration", None, OWNERLESS_RESOURCE, "RegistrationGranted", "ens_v1_registrar_l1", 8, 1, json!({"source_event":"NameRegistered","authority_kind":"registrar","authority_key":"registrar:fixture","registrant":PRIOR_CONTROLLER,"expiry":4242,"namehash":OWNERLESS_NAMEHASH})),
        ("fixture:wrapped-expiry", None, OWNERLESS_RESOURCE, "ExpiryChanged", "ens_v1_registrar_l1", 8, 2, json!({"source_event":"NameRegistered","authority_kind":"registrar","authority_key":"registrar:fixture","registrant":CONTROL_OWNER,"expiry":4242,"namehash":OWNERLESS_NAMEHASH})),
        ("fixture:wrapped-registrar-transfer", None, OWNERLESS_RESOURCE, "TokenControlTransferred", "ens_v1_registrar_l1", 9, 1, json!({"source_event":"Transfer","from":PRIOR_CONTROLLER,"to":CONTROL_OWNER,"namehash":OWNERLESS_NAMEHASH})),
        ("fixture:wrapped-binding", Some(OWNERLESS_LOGICAL), CONTROL_RESOURCE, "SurfaceBound", "ens_v1_wrapper_l1", 9, 3, json!({"source_event":"NameWrapped","node":OWNERLESS_NAMEHASH,"wrapped_registrar_resource_id":OWNERLESS_RESOURCE})),
        ("fixture:wrapped-scope", Some(OWNERLESS_LOGICAL), CONTROL_RESOURCE, "PermissionScopeChanged", "ens_v1_wrapper_l1", 9, 3, json!({"source_event":"NameWrapped","node":OWNERLESS_NAMEHASH,"wrapper_state":"wrapped","fuses":0})),
        ("fixture:wrapper-expiry", Some(OWNERLESS_LOGICAL), CONTROL_RESOURCE, "ExpiryChanged", "ens_v1_wrapper_l1", 9, 3, json!({"source_event":"NameWrapped","node":OWNERLESS_NAMEHASH,"expiry":5252})),
        ("fixture:wrapped-renewal", None, OWNERLESS_RESOURCE, "RegistrationRenewed", "ens_v1_registrar_l1", 10, 1, json!({"source_event":"NameRenewed","authority_kind":"registrar","registrant":CONTROL_OWNER,"expiry":5252,"namehash":OWNERLESS_NAMEHASH})),
        ("fixture:wrapped-renewed-expiry", None, OWNERLESS_RESOURCE, "ExpiryChanged", "ens_v1_registrar_l1", 10, 2, json!({"source_event":"NameRenewed","authority_kind":"registrar","registrant":CONTROL_OWNER,"expiry":5252,"namehash":OWNERLESS_NAMEHASH})),
    ] { seed_normalized_event(&pool, identity, logical, Some(resource), kind, family, block, log, state, json!({})).await?; }
    sqlx::query("UPDATE normalized_events SET transaction_hash = (SELECT transaction_hash FROM normalized_events WHERE event_identity = 'fixture:wrapped-binding') WHERE event_identity = 'fixture:wrapped-registrar-transfer'").execute(&pool).await?;
    sqlx::query("UPDATE normalized_events SET raw_fact_ref = jsonb_build_object('emitting_address', lower($1)) WHERE event_identity = 'fixture:wrapped-binding'").bind(WRAPPER_CONTRACT).execute(&pool).await?;
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
    seed_normalized_event(&pool, "fixture:wrap-registrar-transfer", Some(OWNERLESS_LOGICAL), Some(OWNERLESS_RESOURCE), "TokenControlTransferred", "ens_v1_registrar_l1", 9, 3, json!({"source_event":"Transfer","from":CONTROL_OWNER,"to":WRAPPER_CONTRACT,"namehash":OWNERLESS_NAMEHASH}), json!({})).await?;
    run_project(&pool, 8, 8, None).await?;
    run_project(&pool, 10, 9, Some(8)).await?;
    let summary: (String, i64) = sqlx::query_as("SELECT declared_summary #>> '{registration,status}', (declared_summary #>> '{registration,expiry}')::bigint FROM name_current WHERE logical_name_id = $1").bind(OWNERLESS_LOGICAL).fetch_one(&pool).await?;
    let registrants: Vec<String> = sqlx::query_scalar("SELECT address FROM address_names_current WHERE logical_name_id = $1 AND relation = 'registrant' ORDER BY address").bind(OWNERLESS_LOGICAL).fetch_all(&pool).await?;
    assert_eq!(summary, ("active".to_owned(), 5252));
    assert_eq!(registrants, vec![CONTROL_OWNER.to_lowercase()], "the wrapper selected a registrant from a different registrar lineage");
    seed_blocks(&pool, [11]).await?;
    seed_normalized_event(&pool, "fixture:later-wrapper-holder-transfer", Some(OWNERLESS_LOGICAL), Some(CONTROL_RESOURCE), "TokenControlTransferred", "ens_v1_wrapper_l1", 11, 1, json!({"source_event":"TransferSingle","from":PRIOR_CONTROLLER,"to":LATEST_WRAPPER_OWNER}), json!({})).await?;
    run_project(&pool, 11, 8, None).await?;
    let current_registrant: String = sqlx::query_scalar("SELECT declared_summary #>> '{registration,registrant}' FROM name_current WHERE logical_name_id = $1").bind(OWNERLESS_LOGICAL).fetch_one(&pool).await?;
    let later_relations: Vec<String> = sqlx::query_scalar("SELECT relation FROM address_names_current WHERE logical_name_id = $1 AND address = lower($2) ORDER BY relation").bind(OWNERLESS_LOGICAL).bind(LATEST_WRAPPER_OWNER).fetch_all(&pool).await?;
    assert_eq!(current_registrant, LATEST_WRAPPER_OWNER.to_lowercase(), "a later wrapper transfer must replace the registrar-derived initial registrant");
    assert_eq!(later_relations, vec!["effective_controller".to_owned(), "registrant".to_owned(), "token_holder".to_owned()]);
    database.cleanup().await?; Ok(())
}

#[derive(Clone, Copy, Debug)]
enum LaterWrapperDelta {
    HolderTransfer,
    Rewrap,
    ResolverUpdate,
    RegistrarRenewal,
    RegistrarRelease,
}

#[derive(Debug, PartialEq)]
struct LaterWrapperProjection {
    registration_status: Option<String>,
    selected_registration_kind: Option<String>,
    expiry: Option<i64>,
    registrant: Option<String>,
    registration_resource_id: Option<String>,
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
            8,
            0,
            json!({"source_event":"NameRegistered","authority_kind":"registrar","authority_key":"registrar:old","registrant":PRIOR_CONTROLLER,"expiry":1111,"namehash":OWNERLESS_NAMEHASH}),
        ),
        (
            "fixture:incremental-registration",
            OWNERLESS_RESOURCE,
            "RegistrationGranted",
            8,
            1,
            json!({"source_event":"NameRegistered","authority_kind":"registrar","authority_key":"registrar:current","registrant":CONTROL_OWNER,"expiry":4242,"namehash":OWNERLESS_NAMEHASH}),
        ),
        (
            "fixture:incremental-expiry",
            OWNERLESS_RESOURCE,
            "ExpiryChanged",
            8,
            2,
            json!({"source_event":"NameRegistered","authority_kind":"registrar","authority_key":"registrar:current","registrant":CONTROL_OWNER,"expiry":4242,"namehash":OWNERLESS_NAMEHASH}),
        ),
    ] {
        seed_normalized_event(
            &pool,
            identity,
            None,
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
            1,
            json!({"source_event":"Transfer","from":CONTROL_OWNER,"to":WRAPPER_CONTRACT,"namehash":OWNERLESS_NAMEHASH}),
            json!({}),
        ),
        (
            "fixture:incremental-wrapper-binding",
            CONTROL_RESOURCE,
            "SurfaceBound",
            "ens_v1_wrapper_l1",
            3,
            json!({"source_event":"NameWrapped","node":OWNERLESS_NAMEHASH,"wrapped_registrar_resource_id":OWNERLESS_RESOURCE}),
            json!({"emitting_address":WRAPPER_CONTRACT}),
        ),
        (
            "fixture:incremental-wrapper-scope",
            CONTROL_RESOURCE,
            "PermissionScopeChanged",
            "ens_v1_wrapper_l1",
            3,
            json!({"source_event":"NameWrapped","node":OWNERLESS_NAMEHASH,"wrapper_state":"wrapped","fuses":0}),
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
            OR ($1 AND event_identity = 'fixture:incremental-registration')",
    )
    .bind(born_wrapped)
    .execute(&pool)
    .await?;

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
                json!({"source_event":"TransferSingle","from":PRIOR_CONTROLLER,"to":LATEST_WRAPPER_OWNER}),
                json!({}),
            )
            .await?;
        }
        LaterWrapperDelta::Rewrap => {
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
                json!({"source_event":"NameWrapped","node":OWNERLESS_NAMEHASH,"wrapped_registrar_resource_id":OWNERLESS_RESOURCE}),
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
                json!({"source_event":"TransferSingle","from":WRAPPER_CONTRACT,"to":LATEST_WRAPPER_OWNER}),
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
                json!({"source_event":"NameWrapped","node":OWNERLESS_NAMEHASH,"wrapper_state":"wrapped","fuses":0}),
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
                json!({"source_event":"NewResolver","node":OWNERLESS_NAMEHASH,"resolver":RESOLVER_ADDRESS}),
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
                    json!({"source_event":"NameRenewed","authority_kind":"registrar","registrant":CONTROL_OWNER,"expiry":6262,"namehash":OWNERLESS_NAMEHASH}),
                    json!({}),
                )
                .await?;
            }
        }
        LaterWrapperDelta::RegistrarRelease => {
            seed_normalized_event(
                &pool,
                "fixture:release-wrapper-holder-transfer",
                Some(OWNERLESS_LOGICAL),
                Some(CONTROL_RESOURCE),
                "TokenControlTransferred",
                "ens_v1_wrapper_l1",
                10,
                1,
                json!({"source_event":"TransferSingle","from":PRIOR_CONTROLLER,"to":LATEST_WRAPPER_OWNER}),
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
                json!({"source_event":"RegistrationReleased","authority_kind":"registrar","expiry":4242,"namehash":OWNERLESS_NAMEHASH}),
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
    } else {
        run_project(
            &pool,
            11,
            if incremental {
                match delta {
                    LaterWrapperDelta::RegistrarRelease => 10,
                    _ => 11,
                }
            } else {
                8
            },
            incremental.then_some(9),
        )
        .await?;
    }
    let (
        registration_status,
        selected_registration_kind,
        expiry,
        registrant,
        registration_resource_id,
    ): LaterWrapperRegistrationRow = sqlx::query_as(
        "SELECT declared_summary #>> '{registration,status}',
                declared_summary #>> '{registration,latest_event_kind}',
                (declared_summary #>> '{registration,expiry}')::bigint,
                declared_summary #>> '{registration,registrant}',
                declared_summary #>> '{registration,resource_id}'
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
            assert_eq!(
                incremental.registrant_event_identity.as_deref(),
                Some("fixture:release-wrapper-holder-transfer")
            );
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
        let initial_wrapper_holder = PRIOR_CONTROLLER.to_lowercase();
        assert_ne!(
            incremental.registrant.as_deref(),
            Some(initial_wrapper_holder.as_str()),
            "the initial NameWrapped holder replaced the wrapped registrar's registrant"
        );
        assert_eq!(
            incremental.registrant, incremental.address_registrant,
            "{delta:?} made name_current disagree with the address-name registrant fold"
        );
        assert_ne!(
            incremental.registrant_event_identity.as_deref(),
            Some("fixture:incremental-old-registration"),
            "the selected registration rows admitted an older same-label registrar lineage"
        );
    }
    Ok(())
}

#[tokio::test]
#[rustfmt::skip]
async fn born_wrapped_rewrap_keeps_the_first_wrapper_registration_identity() -> Result<()> {
    let incremental = project_later_wrapper_delta(LaterWrapperDelta::Rewrap, true, false, true).await?;
    let from_zero = project_later_wrapper_delta(LaterWrapperDelta::Rewrap, false, false, true).await?;
    assert_eq!(incremental, from_zero, "re-wrap projection diverged between an incremental batch and from-zero rebuild");
    assert_eq!(incremental.registration_resource_id.as_deref(), Some(CONTROL_RESOURCE), "a later wrapper split the registrar-born lifecycle away from its first wrapper handle");
    assert_eq!(incremental.registrant.as_deref(), Some("0x7777777777777777777777777777777777777777"));
    assert_eq!(incremental.registrant, incremental.address_registrant);
    Ok(())
}

#[tokio::test] #[rustfmt::skip]
async fn born_wrapped_release_keeps_the_wrapper_registration_identity() -> Result<()> {
    let incremental = project_later_wrapper_delta(LaterWrapperDelta::RegistrarRelease, true, false, true).await?;
    let from_zero = project_later_wrapper_delta(LaterWrapperDelta::RegistrarRelease, false, false, true).await?;
    assert_eq!(incremental, from_zero); assert_eq!(incremental.registration_status.as_deref(), Some("released"));
    assert_eq!(incremental.registration_resource_id.as_deref(), Some(CONTROL_RESOURCE)); assert_eq!(incremental.registrant, incremental.address_registrant); Ok(())
}

#[tokio::test]
async fn later_wrapper_retraction_projects_identically_incrementally_and_from_zero() -> Result<()> {
    let incremental =
        project_later_wrapper_delta(LaterWrapperDelta::HolderTransfer, true, true, false).await?;
    let from_zero =
        project_later_wrapper_delta(LaterWrapperDelta::HolderTransfer, false, true, false).await?;
    assert_eq!(incremental, from_zero);
    assert_eq!(incremental.expiry, Some(4242));
    assert_eq!(
        incremental.registrant.as_deref(),
        Some(CONTROL_OWNER.to_lowercase().as_str())
    );
    Ok(())
}

#[derive(Debug, PartialEq)]
struct EnrichedRegistryOnlyProjection {
    expiry: Option<i64>,
    registration_resource_id: Option<String>,
    registrant: Option<String>,
    address_registrant: Option<String>,
}

#[rustfmt::skip]
async fn project_enriched_registry_only(controller_registered: bool, incremental: bool) -> Result<EnrichedRegistryOnlyProjection> {
    const ALICE: &str = "0x5555555555555555555555555555555555555555"; const BOB: &str = "0x6666666666666666666666666666666666666666";
    const REGISTRY_RESOURCE: &str = "edededed-eded-eded-eded-edededededed"; const REGISTRY_BINDING: &str = "efefefef-efef-efef-efef-efefefefefef"; const EXPIRY: i64 = 1_700_001_100;
    let (database, pool) = migrated_pool().await?; seed_chain(&pool).await?;
    seed_surface(&pool, OWNERLESS_NAMEHASH, "enriched-later.eth", OWNERLESS_RESOURCE, OWNERLESS_BINDING).await?;
    if !controller_registered {
        sqlx::query("UPDATE surface_bindings SET block_number = 9, block_hash = $2, active_from = '2026-08-01T00:00:09Z' WHERE surface_binding_id = $1::uuid").bind(OWNERLESS_BINDING).bind(block_hash(9)).execute(&pool).await?;
        seed_binding_provenance(&pool, OWNERLESS_BINDING, 0, 1).await?;
    }
    for (identity, kind, block, log, expiry) in [
        ("fixture:enriched-grant", "RegistrationGranted", 8, 1, 1_700_000_100),
        ("fixture:enriched-initial-expiry", "ExpiryChanged", 8, 2, 1_700_000_100),
        ("fixture:enriched-renewal", "RegistrationRenewed", 9, 0, EXPIRY),
        ("fixture:enriched-renewal-expiry", "ExpiryChanged", 9, 0, EXPIRY),
    ] { seed_normalized_event(&pool, identity, controller_registered.then_some(OWNERLESS_LOGICAL), Some(OWNERLESS_RESOURCE), kind, "ens_v1_registrar_l1", block, log, json!({"source_event":if block == 8 { "NameRegistered" } else { "NameRenewed" },"authority_kind":"registrar","registrant":ALICE,"expiry":expiry,"namehash":OWNERLESS_NAMEHASH}), json!({})).await?; }
    if incremental { run_project(&pool, 9, 8, None).await?; }
    seed_next_binding(&pool, OWNERLESS_NAMEHASH, REGISTRY_RESOURCE, REGISTRY_BINDING, 10, "2026-08-01T00:00:10Z").await?;
    seed_binding_provenance(&pool, REGISTRY_BINDING, 0, 0).await?;
    seed_authority_epoch_changed(&pool, "fixture:enriched-registry-only-epoch", OWNERLESS_NAMEHASH, REGISTRY_RESOURCE, 10, "registry_only").await?;
    seed_normalized_event(&pool, "fixture:enriched-unreclaimed-transfer", Some(OWNERLESS_LOGICAL), Some(OWNERLESS_RESOURCE), "TokenControlTransferred", "ens_v1_registrar_l1", 10, 0, json!({"source_event":"Transfer","authority_kind":"registrar","from":ALICE,"to":BOB,"namehash":OWNERLESS_NAMEHASH}), json!({})).await?;
    run_project(&pool, 10, if incremental { 10 } else { 8 }, incremental.then_some(9)).await?;
    let (expiry, registration_resource_id, registrant) = sqlx::query_as("SELECT (declared_summary #>> '{registration,expiry}')::bigint, declared_summary #>> '{registration,resource_id}', declared_summary #>> '{registration,registrant}' FROM name_current WHERE logical_name_id = $1").bind(OWNERLESS_LOGICAL).fetch_one(&pool).await?;
    let address_registrant = sqlx::query_scalar("SELECT address FROM address_names_current WHERE logical_name_id = $1 AND relation = 'registrant'").bind(OWNERLESS_LOGICAL).fetch_optional(&pool).await?;
    database.cleanup().await?; Ok(EnrichedRegistryOnlyProjection { expiry, registration_resource_id, registrant, address_registrant })
}

#[tokio::test]
#[rustfmt::skip]
async fn enrich_later_registration_keeps_lease_through_registry_only_fallback() -> Result<()> {
    let incremental = project_enriched_registry_only(false, true).await?; let from_zero = project_enriched_registry_only(false, false).await?;
    assert_eq!(incremental, from_zero); assert_eq!(incremental.expiry, Some(1_700_001_100), "plaintext enrichment left the live registrar expiry behind its binding");
    assert_eq!(incremental.registration_resource_id.as_deref(), Some(OWNERLESS_RESOURCE)); assert_eq!(incremental.registrant.as_deref(), Some("0x6666666666666666666666666666666666666666")); assert_eq!(incremental.registrant, incremental.address_registrant);
    let controller_control = project_enriched_registry_only(true, false).await?;
    assert_eq!(controller_control.expiry, incremental.expiry); assert_eq!(controller_control.registration_resource_id, incremental.registration_resource_id); assert_eq!(controller_control.registrant, incremental.registrant); assert_eq!(controller_control.address_registrant, incremental.address_registrant); Ok(())
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
