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
