use alloy_primitives::keccak256;
use anyhow::{Context, Result};
use bigname_project::{
    BatchRequest, DUAL_CURRENT_CHILD_AUTHORITY, DUAL_CURRENT_EXACT_NAME_AUTHORITY, Engine, RunMode,
};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgPool, raw_sql};

const CHAIN: &str = "ethereum-sepolia";
const HASH: &str = "0x0000000000000000000000000000000000000000000000000000000000000503";

async fn database(prefix: &str) -> Result<(TestDatabase, PgPool)> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new(prefix).pool_max_connections(1)).await?;
    let pool = database.pool().clone();
    let name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    let mut tx = pool.begin().await?;
    sqlx::query("CREATE SCHEMA bigname_phase")
        .execute(&mut *tx)
        .await?;
    raw_sql(&format!(
        "ALTER DATABASE \"{}\" SET search_path TO bigname_phase, public",
        name.replace('"', r#""""#)
    ))
    .execute(&mut *tx)
    .await?;
    sqlx::query("SET LOCAL search_path TO bigname_phase, public")
        .execute(&mut *tx)
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
        raw_sql(script).execute(&mut *tx).await?;
    }
    tx.commit().await?;
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
    drop(connections);
    sqlx::query("INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state) VALUES ($1, $2, 10, '2026-08-26T00:00:00Z', 'canonical')")
        .bind(CHAIN).bind(HASH).execute(&pool).await?;
    Ok((database, pool))
}

fn uuid(kind: u8, index: u16) -> String {
    format!("{kind:08x}-0000-0000-0000-{index:012x}")
}

fn labelhash(label: &str) -> String {
    format!("{:#x}", keccak256(label.as_bytes()))
}

async fn surface(pool: &PgPool, index: u16, raw_name: &str, arms: &[&str]) -> Result<String> {
    let labels: Vec<_> = if raw_name.is_empty() {
        vec![]
    } else {
        raw_name.split('.').collect()
    };
    let hash = format!(
        "{:#x}",
        bigname_storage::ens_namehash_label_bytes(
            &labels
                .iter()
                .map(|label| label.as_bytes())
                .collect::<Vec<_>>()
        )
    );
    let logical = format!("ens:{hash}");
    let labelhashes: Vec<_> = labels.iter().map(|label| labelhash(label)).collect();
    sqlx::query("INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state, chain_id, block_hash, block_number, canonicality_state) VALUES ($1, 'ens', $2, $3, '\\x00', $4, $5, 'ensip15', 'active', $6, $7, 10, 'canonical')")
        .bind(&logical).bind(raw_name).bind(labels).bind(&hash).bind(labelhashes).bind(CHAIN).bind(HASH).execute(pool).await?;
    for (offset, arm) in arms.iter().enumerate() {
        let resource = uuid(1 + offset as u8, index);
        let binding = uuid(3 + offset as u8, index);
        sqlx::query("INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state) VALUES ($1::uuid, $2, $3, 10, 'canonical')")
            .bind(&resource).bind(CHAIN).bind(HASH).execute(pool).await?;
        sqlx::query("INSERT INTO surface_bindings (surface_binding_id, logical_name_id, resource_id, binding_kind, authority_arm, active_from, chain_id, block_hash, block_number, provenance, canonicality_state) VALUES ($1::uuid, $2, $3::uuid, 'declared_registry_path', $4, '2026-08-25T00:00:00Z', $5, $6, 10, '{\"transaction_index\":0,\"log_index\":0}', 'canonical')")
            .bind(binding).bind(&logical).bind(resource).bind(arm).bind(CHAIN).bind(HASH).execute(pool).await?;
    }
    Ok(logical)
}

struct Event<'a> {
    family: &'a str,
    kind: &'a str,
    log: i64,
    after: Value,
}

#[derive(sqlx::FromRow)]
struct CapturedAuthority {
    selected_authority_arm: Option<String>,
    authority_epoch_start_position: Option<Value>,
    authority_proof_kind: Option<String>,
    authority_proof_event_id: Option<i64>,
    authority_proof_event_identity: Option<String>,
    authority_transition_id: Option<String>,
}

async fn event(
    pool: &PgPool,
    identity: &str,
    logical: &str,
    resource: Option<&str>,
    event: Event<'_>,
) -> Result<i64> {
    event_in_tx(pool, identity, logical, resource, "0x503", event).await
}

/// Like `event`, in the transaction `tx`.
async fn event_in_tx(
    pool: &PgPool,
    identity: &str,
    logical: &str,
    resource: Option<&str>,
    tx: &str,
    event: Event<'_>,
) -> Result<i64> {
    Ok(sqlx::query_scalar("INSERT INTO normalized_events (event_identity, namespace, logical_name_id, resource_id, event_kind, source_family, manifest_version, chain_id, block_number, block_hash, transaction_hash, transaction_index, log_index, derivation_kind, canonicality_state, after_state, migration_correlation_ids) VALUES ($1, 'ens', $2, $3::uuid, $4, $5, 1, $6, 10, $7, $10, 0, $8, CASE WHEN $4 = 'MigrationApplied' THEN 'ens_v2_migration' ELSE 'ens_v2_registry_resource_surface' END, 'canonical', $9, CASE WHEN $4 = 'MigrationApplied' THEN ARRAY['issue-503'] ELSE ARRAY[]::text[] END) RETURNING normalized_event_id")
        .bind(identity).bind(logical).bind(resource).bind(event.kind).bind(event.family).bind(CHAIN).bind(HASH).bind(event.log).bind(event.after).bind(tx).fetch_one(pool).await?)
}

/// What the name serves: its resource and binding and the registration, control and resolver
/// sections of its summary.
async fn served(pool: &PgPool, logical: &str) -> Result<Value> {
    Ok(sqlx::query_scalar(
        "SELECT jsonb_build_object(
                'resource_id', resource_id, 'surface_binding_id', surface_binding_id,
                'registration', declared_summary -> 'registration',
                'control', declared_summary -> 'control',
                'resolver', declared_summary -> 'resolver')
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(logical)
    .fetch_one(pool)
    .await?)
}

async fn run(pool: &PgPool) -> bigname_project::Result<()> {
    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.into(),
            target_block: 10,
            affected_from_block: 10,
            affected_to_block: 10,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await
        .map(|_| ())
}

async fn authority(
    pool: &PgPool,
    logical: &str,
) -> Result<(
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
)> {
    Ok(sqlx::query_as("SELECT provenance #>> '{authority_selection,authority_arm}', provenance #>> '{authority_selection,unsupported_reason}', provenance #>> '{authority_selection,proof_kind}', provenance #>> '{authority_selection,transition_id}' FROM name_current WHERE logical_name_id = $1")
        .bind(logical).fetch_one(pool).await?)
}

async fn optional_authority(
    pool: &PgPool,
    logical: &str,
) -> Result<
    Option<(
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    )>,
> {
    Ok(sqlx::query_as("SELECT provenance #>> '{authority_selection,authority_arm}', provenance #>> '{authority_selection,unsupported_reason}', provenance #>> '{authority_selection,proof_kind}', provenance #>> '{authority_selection,transition_id}' FROM name_current WHERE logical_name_id = $1")
        .bind(logical).fetch_optional(pool).await?)
}

async fn authority_evidence(
    pool: &PgPool,
    logical: &str,
) -> Result<(
    Option<String>,
    Option<String>,
    Option<String>,
    Option<serde_json::Value>,
)> {
    Ok(sqlx::query_as("SELECT provenance #>> '{authority_selection,proof_kind}', provenance #>> '{authority_selection,proof_event_id}', provenance #>> '{authority_selection,proof_event_identity}', provenance #> '{authority_selection,epoch_start_position}' FROM name_current WHERE logical_name_id = $1")
        .bind(logical).fetch_one(pool).await?)
}

async fn capture_staged_authority(pool: &PgPool) -> Result<()> {
    sqlx::query("CREATE TABLE issue503_authority_capture (logical_name_id text, selected_authority_arm text, authority_epoch_start_position jsonb, authority_proof_kind text, authority_proof_event_id bigint, authority_proof_event_identity text, authority_transition_id text)")
        .execute(pool)
        .await?;
    sqlx::query("CREATE FUNCTION capture_issue503_authority() RETURNS trigger LANGUAGE plpgsql AS $capture$ BEGIN INSERT INTO issue503_authority_capture SELECT logical_name_id, selected_authority_arm, authority_epoch_start_position, authority_proof_kind, authority_proof_event_id, authority_proof_event_identity, authority_transition_id FROM project_name_authority; RETURN NULL; END $capture$")
        .execute(pool)
        .await?;
    sqlx::query("CREATE TRIGGER capture_issue503_authority AFTER INSERT ON name_current FOR EACH STATEMENT EXECUTE FUNCTION capture_issue503_authority()")
        .execute(pool)
        .await?;
    Ok(())
}

// Follow the chain: a current ENSv2 registration holds the name whatever ENSv1 holds, without a
// migration proof. The live ENSv1 binding beside it is chain state, not a post-proof
// contradiction, so the generation publishes.
#[tokio::test]
async fn a_current_v2_registration_holds_a_live_v1_name_without_proof() -> Result<()> {
    let (db, pool) = database("issue503_no_proof").await?;
    let logical = surface(&pool, 1, "ordinary.eth", &["ens_v1", "ens_v2"]).await?;
    run(&pool).await?;
    assert_eq!(
        authority(&pool, &logical).await?,
        (Some("ens_v2".into()), None, None, None)
    );
    assert_eq!(
        authority_evidence(&pool, &logical).await?,
        (
            None,
            None,
            None,
            Some(json!({"block_number": 10, "transaction_index": 0, "log_index": 0}))
        )
    );
    db.cleanup().await?;
    Ok(())
}

/// Adds a binding of `arm` that is still open at the target block, opened at `log`.
async fn open_binding(
    pool: &PgPool,
    logical: &str,
    index: u16,
    arm: &str,
    log: i64,
) -> Result<String> {
    let resource = uuid(8, index);
    sqlx::query("INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state) VALUES ($1::uuid, $2, $3, 10, 'canonical')")
        .bind(&resource).bind(CHAIN).bind(HASH).execute(pool).await?;
    sqlx::query("INSERT INTO surface_bindings (surface_binding_id, logical_name_id, resource_id, binding_kind, authority_arm, active_from, chain_id, block_hash, block_number, provenance, canonicality_state) VALUES ($1::uuid, $2, $3::uuid, 'declared_registry_path', $4, '2026-08-25T00:00:00Z', $5, $6, 10, jsonb_build_object('transaction_index', 0, 'log_index', $7::bigint), 'canonical')")
        .bind(uuid(9, index)).bind(logical).bind(&resource).bind(arm).bind(CHAIN).bind(HASH).bind(log).execute(pool).await?;
    Ok(resource)
}

// The remaining risk case: a label registered on ENSv1 after the premigration snapshot, so it
// has no reservation, and then registered on ENSv2. The ENSv2 registration decides, which is
// also what the Universal Resolver answers.
// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/universalResolver/libraries/LibResolution.sol:L58-L85 @ ens_v2_sepolia_20260916@366de741)
// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/resolver/ENSV1Resolver.sol:L40-L43 @ ens_v2_sepolia_20260916@366de741)
#[tokio::test]
async fn a_v2_registration_after_an_unreserved_v1_registration_selects_v2() -> Result<()> {
    let (db, pool) = database("overlap_unreserved_v1").await?;
    let logical = surface(&pool, 62, "late-v1.eth", &["ens_v1"]).await?;
    event(
        &pool,
        "overlap-late-v1-grant",
        &logical,
        Some(&uuid(1, 62)),
        Event {
            family: "ens_v1_registrar_l1",
            kind: "RegistrationGranted",
            log: 1,
            after: json!({"status":"registered","registrant":"0x0000000000000000000000000000000000000001"}),
        },
    )
    .await?;
    let v2_resource = open_binding(&pool, &logical, 62, "ens_v2", 5).await?;
    event(
        &pool,
        "overlap-late-v2-grant",
        &logical,
        Some(&v2_resource),
        Event {
            family: "ens_v2_registry_l1",
            kind: "RegistrationGranted",
            log: 5,
            after: json!({"status":"registered","registrant":"0x0000000000000000000000000000000000000002"}),
        },
    )
    .await?;
    run(&pool).await?;
    assert_eq!(
        authority(&pool, &logical).await?,
        (Some("ens_v2".into()), None, None, None)
    );
    db.cleanup().await?;
    Ok(())
}

// Sepolia group D: a wrapped name moved to ENSv2, and an ENSv1 registry owner change opened a
// newer ENSv1 binding afterwards. No migration proof was seen. The ENSv2 registration still
// decides; neither arm's recency does.
#[tokio::test]
async fn a_later_v1_registry_owner_does_not_displace_a_current_v2_registration() -> Result<()> {
    let (db, pool) = database("overlap_later_v1_owner").await?;
    let logical = surface(&pool, 63, "moved.eth", &["ens_v2"]).await?;
    let v1_resource = open_binding(&pool, &logical, 63, "ens_v1", 5).await?;
    event(
        &pool,
        "overlap-later-v1-owner",
        &logical,
        Some(&v1_resource),
        Event {
            family: "ens_v1_registry_l1",
            kind: "AuthorityTransferred",
            log: 5,
            after: json!({"owner":"0x0000000000000000000000000000000000000002"}),
        },
    )
    .await?;
    run(&pool).await?;
    assert_eq!(
        authority(&pool, &logical).await?,
        (Some("ens_v2".into()), None, None, None)
    );
    db.cleanup().await?;
    Ok(())
}

async fn lifecycle_state(pool: &PgPool, logical: &str) -> Result<Option<String>> {
    Ok(sqlx::query_scalar("SELECT provenance #>> '{authority_selection,lifecycle_state}' FROM name_current WHERE logical_name_id = $1")
        .bind(logical).fetch_one(pool).await?)
}

// A premigration reservation is not ENSv2 authority: it defers to ENSv1, whose live
// registration keeps the name.
#[tokio::test]
async fn a_v2_reservation_defers_to_a_live_v1_registration() -> Result<()> {
    let (db, pool) = database("overlap_reserved_live_v1").await?;
    let logical = surface(&pool, 64, "reserved-live.eth", &["ens_v1"]).await?;
    event(
        &pool,
        "overlap-reserved-live",
        &logical,
        None,
        Event {
            family: "ens_v2_registry_l1",
            kind: "RegistrationReserved",
            log: 2,
            after: json!({"status":"reserved"}),
        },
    )
    .await?;
    run(&pool).await?;
    assert_eq!(
        authority(&pool, &logical).await?,
        (Some("ens_v1".into()), None, None, None)
    );
    assert_eq!(
        lifecycle_state(&pool, &logical).await?.as_deref(),
        Some("registered")
    );
    db.cleanup().await?;
    Ok(())
}

// A reservation over an ENSv1 lease that has ended leaves nothing current: ENSv1 decides and
// serves its released lease.
#[tokio::test]
async fn a_v2_reservation_over_an_ended_v1_lease_serves_nothing_current() -> Result<()> {
    let (db, pool) = database("overlap_reserved_ended_v1").await?;
    let logical = surface(&pool, 65, "reserved-ended.eth", &[]).await?;
    let v1_resource = closed_binding(&pool, &logical, 65, "ens_v1").await?;
    for (log, kind, after) in [
        (
            1,
            "RegistrationGranted",
            json!({"status":"registered","registrant":"0x0000000000000000000000000000000000000001"}),
        ),
        (2, "RegistrationReleased", json!({"status":"unregistered"})),
    ] {
        event(
            &pool,
            &format!("overlap-reserved-ended-{kind}"),
            &logical,
            Some(&v1_resource),
            Event {
                family: "ens_v1_registrar_l1",
                kind,
                log,
                after,
            },
        )
        .await?;
    }
    event(
        &pool,
        "overlap-reserved-ended-reservation",
        &logical,
        None,
        Event {
            family: "ens_v2_registry_l1",
            kind: "RegistrationReserved",
            log: 3,
            after: json!({"status":"reserved"}),
        },
    )
    .await?;
    run(&pool).await?;
    assert_eq!(
        authority(&pool, &logical).await?,
        (Some("ens_v1".into()), None, None, None)
    );
    assert_eq!(
        lifecycle_state(&pool, &logical).await?.as_deref(),
        Some("unregistered")
    );
    db.cleanup().await?;
    Ok(())
}

// Nothing is open on either arm and the name has only ENSv1 history. ENSv1 then decides from its
// history, and the lifecycle state reads the latest ENSv1 lifecycle row. Here that row is a grant,
// so the name reads `registered` rather than the `unregistered` default of a selection with no
// binding.
#[tokio::test]
async fn ensv1_history_gives_the_lifecycle_state() -> Result<()> {
    let (db, pool) = database("overlap_history_lifecycle").await?;
    let sole = surface(&pool, 72, "history-sole.eth", &[]).await?;
    let v1_resource = closed_binding(&pool, &sole, 72, "ens_v1").await?;
    event(
        &pool,
        "history-lifecycle-v1-grant-72",
        &sole,
        Some(&v1_resource),
        Event {
            family: "ens_v1_registrar_l1",
            kind: "RegistrationGranted",
            log: 1,
            after: json!({"status":"registered","registrant":"0x0000000000000000000000000000000000000001"}),
        },
    )
    .await?;
    run(&pool).await?;
    assert_eq!(
        authority(&pool, &sole).await?,
        (
            Some("ens_v1".into()),
            Some("current_authority_not_projected".into()),
            None,
            None
        )
    );
    assert_eq!(
        lifecycle_state(&pool, &sole).await?.as_deref(),
        Some("registered")
    );
    db.cleanup().await?;
    Ok(())
}

// Expected delta (TYR-36 step 6). An ENSv1 lease whose binding has ended, then an ENSv2
// registration that was granted and released; nothing is open on either arm.
// Before: ENSv1 decided because ENSv1 facts preceded the release (arm `ens_v1`, unsupported
// `current_authority_not_projected`, lifecycle `registered`).
// After: the ENSv2 release is the latest lifecycle fact, so the name is served as the released
// ENSv2 tombstone (arm `ens_v2`, supported, lifecycle `unregistered`, registration `released`).
// Chain fact: `unregister` burns the ENSv2 token and sets its expiry to now, and the ended ENSv1
// lease no longer answers `ownerOf`; neither arm holds the name.
// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L195-L207 @ ens_v2@a971bd64)
// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L71-L76 @ ens_v1@91c966f)
#[tokio::test]
async fn expected_delta_v2_release_after_an_ended_v1_lease_is_a_v2_tombstone() -> Result<()> {
    let (db, pool) = database("delta_v2_release_after_v1_lease").await?;
    let mixed = surface(&pool, 71, "history-mixed.eth", &[]).await?;
    let v1_resource = closed_binding(&pool, &mixed, 71, "ens_v1").await?;
    event(
        &pool,
        "history-lifecycle-v1-grant-71",
        &mixed,
        Some(&v1_resource),
        Event {
            family: "ens_v1_registrar_l1",
            kind: "RegistrationGranted",
            log: 1,
            after: json!({"status":"registered","registrant":"0x0000000000000000000000000000000000000001"}),
        },
    )
    .await?;
    let v2_resource = closed_v2_binding_at(&pool, &mixed, 71, 2, 0).await?;
    for (log, kind, after) in [
        (
            2,
            "RegistrationGranted",
            json!({"status":"registered","registrant":"0x0000000000000000000000000000000000000002"}),
        ),
        (3, "RegistrationReleased", json!({"status":"unregistered"})),
    ] {
        event(
            &pool,
            &format!("history-lifecycle-v2-{kind}"),
            &mixed,
            Some(&v2_resource),
            Event {
                family: "ens_v2_registry_l1",
                kind,
                log,
                after,
            },
        )
        .await?;
    }
    run(&pool).await?;
    assert_eq!(
        authority(&pool, &mixed).await?,
        (Some("ens_v2".into()), None, None, None)
    );
    assert_eq!(
        lifecycle_state(&pool, &mixed).await?.as_deref(),
        Some("unregistered")
    );
    let (resource, status): (Option<String>, Option<String>) = sqlx::query_as(
        "SELECT resource_id::text, declared_summary #>> '{registration,status}'
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&mixed)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        (resource.as_deref(), status.as_deref()),
        (Some(v2_resource.as_str()), Some("released"))
    );
    db.cleanup().await?;
    Ok(())
}

const EARLIER_HASH: &str = "0x0000000000000000000000000000000000000000000000000000000000000502";

/// Adds block 9 to the lineage, before the target block 10.
async fn earlier_block(pool: &PgPool) -> Result<()> {
    sqlx::query("INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state) VALUES ($1, $2, 9, '2026-08-25T00:00:00Z', 'canonical')")
        .bind(CHAIN).bind(EARLIER_HASH).execute(pool).await?;
    Ok(())
}

/// Adds a resource and an `arm` binding at block 9 that closed before the target block.
async fn closed_binding_at_block_9(
    pool: &PgPool,
    logical: &str,
    index: u16,
    arm: &str,
) -> Result<(String, String)> {
    let resource = uuid(15, index);
    let binding = uuid(16, index);
    sqlx::query("INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state) VALUES ($1::uuid, $2, $3, 9, 'canonical')")
        .bind(&resource).bind(CHAIN).bind(EARLIER_HASH).execute(pool).await?;
    sqlx::query("INSERT INTO surface_bindings (surface_binding_id, logical_name_id, resource_id, binding_kind, authority_arm, active_from, active_to, chain_id, block_hash, block_number, provenance, canonicality_state) VALUES ($1::uuid, $2, $3::uuid, 'declared_registry_path', $4, '2026-08-25T00:00:00Z', '2026-08-25T12:00:00Z', $5, $6, 9, '{\"transaction_index\":0,\"log_index\":0}', 'canonical')")
        .bind(&binding).bind(logical).bind(&resource).bind(arm).bind(CHAIN).bind(EARLIER_HASH).execute(pool).await?;
    Ok((resource, binding))
}

// Equal-position tie (TYR-36 step 6, Pro review question 1). An ENSv1 lease and an ENSv2
// registration were both granted in block 9. In block 10 the ENSv1 lease lapses and the ENSv2
// registration is released, and Interpret writes both releases at the block boundary with no
// transaction or log index, so they share the position (10, NULL, NULL). Nothing is open on
// either arm. The ENSv1 release is a holding-changing fact, unlike the expiry maintenance in
// `expected_delta_equal_position_v1_expiry_residue_keeps_the_v2_tombstone` (phase-runner
// production_project), which does not take part.
// Rule: a released ENSv2 registration stays with ENSv2 whatever ENSv1 holds (product ruling of
// 2026-09-25), so ENSv1 fact positions no longer take part and the tie leaves the released ENSv2
// tombstone. Before the ruling the tie went to ENSv1, which served its released lease.
#[tokio::test]
async fn an_equal_position_v1_lease_release_leaves_the_v2_release_in_place() -> Result<()> {
    let (db, pool) = database("tie_v1_release_v2_release").await?;
    earlier_block(&pool).await?;
    let logical = surface(&pool, 76, "tie-release.eth", &[]).await?;
    let (v1_resource, _) = closed_binding_at_block_9(&pool, &logical, 76, "ens_v1").await?;
    let (v2_resource, v2_binding) =
        closed_binding_at_block_9(&pool, &logical, 77, "ens_v2").await?;
    for (identity, resource, family, kind, log, after) in [
        (
            "tie-v1-grant",
            &v1_resource,
            "ens_v1_registrar_l1",
            "RegistrationGranted",
            1,
            json!({"status":"registered","registrant":"0x0000000000000000000000000000000000000001"}),
        ),
        (
            "tie-v2-grant",
            &v2_resource,
            "ens_v2_registry_l1",
            "RegistrationGranted",
            2,
            json!({"status":"registered","registrant":"0x0000000000000000000000000000000000000002"}),
        ),
    ] {
        event(
            &pool,
            identity,
            &logical,
            Some(resource),
            Event {
                family,
                kind,
                log,
                after,
            },
        )
        .await?;
    }
    sqlx::query("UPDATE normalized_events SET block_number = 9, block_hash = $1 WHERE event_identity IN ('tie-v1-grant', 'tie-v2-grant')")
        .bind(EARLIER_HASH).execute(&pool).await?;
    for (identity, resource, family) in [
        ("tie-v2-release", &v2_resource, "ens_v2_registry_l1"),
        ("tie-v1-release", &v1_resource, "ens_v1_registrar_l1"),
    ] {
        event(
            &pool,
            identity,
            &logical,
            Some(resource),
            Event {
                family,
                kind: "RegistrationReleased",
                log: 0,
                after: json!({"status":"released"}),
            },
        )
        .await?;
    }
    sqlx::query("UPDATE normalized_events SET transaction_hash = NULL, transaction_index = NULL, log_index = NULL WHERE event_identity IN ('tie-v2-release', 'tie-v1-release')")
        .execute(&pool).await?;
    run(&pool).await?;
    assert_eq!(
        authority(&pool, &logical).await?,
        (Some("ens_v2".into()), None, None, None)
    );
    assert_eq!(
        lifecycle_state(&pool, &logical).await?.as_deref(),
        Some("unregistered")
    );
    let (resource, binding, status): (Option<String>, Option<String>, Option<String>) =
        sqlx::query_as(
            "SELECT resource_id::text, surface_binding_id::text,
                    declared_summary #>> '{registration,status}'
             FROM name_current WHERE logical_name_id = $1",
        )
        .bind(&logical)
        .fetch_one(&pool)
        .await?;
    assert_eq!(
        (resource.as_deref(), binding.as_deref(), status.as_deref()),
        (
            Some(v2_resource.as_str()),
            Some(v2_binding.as_str()),
            Some("released")
        )
    );
    db.cleanup().await?;
    Ok(())
}

// An ENSv2 registration was released, and an ENSv1 lease granted after the release is live now.
// The released ENSv2 registration stays with ENSv2 (product ruling of 2026-09-25): the name is
// the released ENSv2 tombstone (arm `ens_v2`, registration `released`), as it was before step 6.
// Chain fact: `unregister` writes the release time as the entry's expiry and the registry returns
// no resolver for an expired entry; nothing on ENSv2 reads the ENSv1 lease.
// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L196-L207 @ ens_v2@a971bd64)
// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L255-L258 @ ens_v2@a971bd64)
#[tokio::test]
async fn a_live_v1_lease_after_a_v2_release_leaves_the_v2_tombstone() -> Result<()> {
    let (db, pool) = database("delta_live_v1_after_v2_release").await?;
    let logical = surface(&pool, 73, "live-after-release.eth", &[]).await?;
    let v2_resource = closed_v2_binding_at(&pool, &logical, 73, 1, 0).await?;
    for (log, kind, after) in [
        (
            1,
            "RegistrationGranted",
            json!({"status":"registered","registrant":"0x0000000000000000000000000000000000000002"}),
        ),
        (2, "RegistrationReleased", json!({"status":"unregistered"})),
    ] {
        event(
            &pool,
            &format!("live-after-release-v2-{kind}"),
            &logical,
            Some(&v2_resource),
            Event {
                family: "ens_v2_registry_l1",
                kind,
                log,
                after,
            },
        )
        .await?;
    }
    let v1_resource = uuid(1, 73);
    sqlx::query("INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state) VALUES ($1::uuid, $2, $3, 10, 'canonical')")
        .bind(&v1_resource).bind(CHAIN).bind(HASH).execute(&pool).await?;
    sqlx::query("INSERT INTO surface_bindings (surface_binding_id, logical_name_id, resource_id, binding_kind, authority_arm, active_from, chain_id, block_hash, block_number, provenance, canonicality_state) VALUES ($1::uuid, $2, $3::uuid, 'declared_registry_path', 'ens_v1', '2026-08-25T02:00:00Z', $4, $5, 10, '{\"transaction_index\":0,\"log_index\":3}', 'canonical')")
        .bind(uuid(3, 73)).bind(&logical).bind(&v1_resource).bind(CHAIN).bind(HASH).execute(&pool).await?;
    event(
        &pool,
        "live-after-release-v1-grant",
        &logical,
        Some(&v1_resource),
        Event {
            family: "ens_v1_registrar_l1",
            kind: "RegistrationGranted",
            log: 3,
            after: json!({"status":"registered","registrant":"0x0000000000000000000000000000000000000001"}),
        },
    )
    .await?;
    run(&pool).await?;
    assert_eq!(
        authority(&pool, &logical).await?,
        (Some("ens_v2".into()), None, None, None)
    );
    let (resource, status): (Option<String>, Option<String>) = sqlx::query_as(
        "SELECT resource_id::text, declared_summary #>> '{registration,status}'
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical)
    .fetch_one(&pool)
    .await?;
    assert_ne!(resource.as_deref(), Some(v1_resource.as_str()));
    assert_eq!(
        (resource.as_deref(), status.as_deref()),
        (Some(v2_resource.as_str()), Some("released"))
    );
    db.cleanup().await?;
    Ok(())
}

// A lease registered through the NameWrapper lapses, so the closed NameWrapper binding stands
// for it as the released ENSv1 tombstone. That binding's resource has no lifecycle rows of its
// own, so the lifecycle state reads the released lease from the ENSv1 history, also when the
// name has ENSv2 history whose release does not qualify. The wrap carries the facts the
// NameWrapper producer emits, so the registration fold admits the lease's lifecycle rows and the
// payload agrees with the lifecycle state.
#[tokio::test]
async fn a_wrapped_v1_tombstone_with_v2_history_reads_unregistered() -> Result<()> {
    let (db, pool) = database("overlap_wrapped_tombstone").await?;
    let logical = surface(&pool, 73, "wrapped-lapsed.eth", &[]).await?;
    let wrapper_resource = closed_binding(&pool, &logical, 73, "ens_v1").await?;
    let lease_resource = uuid(15, 73);
    sqlx::query("INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state) VALUES ($1::uuid, $2, $3, 10, 'canonical')")
        .bind(&lease_resource).bind(CHAIN).bind(HASH).execute(&pool).await?;
    let holder = "0x0000000000000000000000000000000000000001";
    let name_wrapper = "0x0000000000000000000000000000000000000a11";
    let authority_key = "wrapper:wrapped-lapsed";
    // The BaseRegistrar lease names the NameWrapper as registrant; the holder is reached only
    // through the wrapper's own transfer below.
    event(
        &pool,
        "wrapped-tombstone-lease-grant",
        &logical,
        Some(&lease_resource),
        Event {
            family: "ens_v1_registrar_l1",
            kind: "RegistrationGranted",
            log: 1,
            after: json!({"authority_kind":"registrar","registrant":name_wrapper,"status":"registered","expiry":4}),
        },
    )
    .await?;
    // NameWrapped, in its own transaction so only the lease it records (not a same-transaction
    // grant) links it to the lease: the wrapper binding, its holder and authority, the token
    // transfer to the holder, the wrapper expiry, and its fuse scope, all on the wrapper resource.
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L264-L268 @ ens_v1@91c966f)
    let wrap = json!({
        "source_event": "NameWrapped",
        "owner": holder,
        "fuses": 0,
        "wrapper_state": "wrapped",
        "expiry": 4,
        "wrapped_registrar_resource_id": lease_resource,
        "authority_kind": "wrapper",
        "authority_key": authority_key,
    });
    let mut transfer = wrap.clone();
    transfer["to"] = json!(holder);
    transfer
        .as_object_mut()
        .expect("wrap payload is an object")
        .remove("owner");
    for (log, kind, after) in [
        (2, "SurfaceBound", wrap.clone()),
        (3, "AuthorityEpochChanged", wrap.clone()),
        (4, "TokenControlTransferred", transfer),
        (5, "ExpiryChanged", wrap.clone()),
        (6, "PermissionScopeChanged", wrap.clone()),
    ] {
        event_in_tx(
            &pool,
            &format!("wrapped-tombstone-wrap-{kind}"),
            &logical,
            Some(&wrapper_resource),
            "0x5031",
            Event {
                family: "ens_v1_wrapper_l1",
                kind,
                log,
                after,
            },
        )
        .await?;
    }
    // A later reservation of the ENSv2 label keeps this ENSv2 release from standing as a
    // tombstone: the reservation is the name's latest ENSv2 lifecycle fact and defers to ENSv1.
    // It has Interpret's shape: `unregister` bumped the token version, so the reservation names
    // the label but carries no resource.
    // (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L201-L205 @ ens_v2@a971bd64)
    let v2_resource = closed_v2_binding_at(&pool, &logical, 73, 7, 0).await?;
    for (log, kind, after) in [
        (
            7,
            "RegistrationGranted",
            json!({"status":"registered","registrant":"0x0000000000000000000000000000000000000002"}),
        ),
        (8, "RegistrationReleased", json!({"status":"unregistered"})),
        (9, "RegistrationReserved", json!({"status":"reserved"})),
    ] {
        let resource = (kind != "RegistrationReserved").then_some(v2_resource.as_str());
        event(
            &pool,
            &format!("wrapped-tombstone-v2-{kind}"),
            &logical,
            resource,
            Event {
                family: "ens_v2_registry_l1",
                kind,
                log,
                after,
            },
        )
        .await?;
    }
    // The lease lapses past grace, in a later transaction of its own.
    event_in_tx(
        &pool,
        "wrapped-tombstone-lease-release",
        &logical,
        Some(&lease_resource),
        "0x5032",
        Event {
            family: "ens_v1_registrar_l1",
            kind: "RegistrationReleased",
            log: 10,
            after: json!({"source_event":"RegistrationReleased","released_at":5,"namehash":logical.trim_start_matches("ens:"),"expiry":4}),
        },
    )
    .await?;
    run(&pool).await?;
    let (summary, selection): (Value, Value) = sqlx::query_as(
        "SELECT declared_summary, provenance -> 'authority_selection'
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical)
    .fetch_one(&pool)
    .await?;
    let registration = &summary["registration"];
    assert_eq!(registration["status"], "released");
    assert_eq!(registration["released_at"], 5);
    assert_eq!(registration["resource_id"], json!(lease_resource));
    // The lapsed lease keeps its own expiry; its last holder and authority are served only
    // inside the lapsed block.
    assert_eq!(registration["expiry"], 4);
    assert!(registration["registrant"].is_null());
    assert!(registration["authority_kind"].is_null());
    assert!(registration["authority_key"].is_null());
    assert_eq!(
        registration["lapsed_registration"],
        json!({
            "registrant": holder,
            "authority_kind": "wrapper",
            "authority_key": authority_key,
            "released_at": 5,
        })
    );
    assert_eq!(summary["control"], json!({"status": "unregistered"}));
    assert_eq!(
        (
            &selection["authority_arm"],
            &selection["surface_binding_id"],
            &selection["resource_authority_context"]["released_tombstone"],
            &selection["lifecycle_state"],
        ),
        (
            &json!("ens_v1"),
            &json!(uuid(7, 73)),
            &json!("ens_v1"),
            &json!("unregistered")
        )
    );
    db.cleanup().await?;
    Ok(())
}

/// Adds a binding of `arm` that closed before the target block, with its own resource.
async fn closed_binding(pool: &PgPool, logical: &str, index: u16, arm: &str) -> Result<String> {
    let resource = uuid(6, index);
    sqlx::query("INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state) VALUES ($1::uuid, $2, $3, 10, 'canonical')")
        .bind(&resource).bind(CHAIN).bind(HASH).execute(pool).await?;
    sqlx::query("INSERT INTO surface_bindings (surface_binding_id, logical_name_id, resource_id, binding_kind, authority_arm, active_from, active_to, chain_id, block_hash, block_number, provenance, canonicality_state) VALUES ($1::uuid, $2, $3::uuid, 'declared_registry_path', $4, '2026-08-25T00:00:00Z', '2026-08-25T12:00:00Z', $5, $6, 10, '{\"transaction_index\":0,\"log_index\":0}', 'canonical')")
        .bind(uuid(7, index)).bind(logical).bind(&resource).bind(arm).bind(CHAIN).bind(HASH).execute(pool).await?;
    Ok(resource)
}

// Expected delta (product ruling of 2026-09-25). Sepolia group A: the name is live on ENSv1, and
// its ENSv2 label was reserved, granted and released again.
// Before: ENSv2 held nothing now, so ENSv1 kept the name (arm `ens_v1`).
// After: the released ENSv2 registration stays with ENSv2 as the released tombstone whatever
// ENSv1 holds (arm `ens_v2`, registration `released`).
// Chain fact: `unregister` writes the release time as the entry's expiry and the registry returns
// no resolver for an expired entry, so nothing on ENSv2 routes the label back to ENSv1.
// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L196-L207 @ ens_v2@a971bd64)
// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L255-L258 @ ens_v2@a971bd64)
#[tokio::test]
async fn expected_delta_a_live_v1_name_whose_v2_grant_was_released_stays_released_on_v2()
-> Result<()> {
    let (db, pool) = database("overlap_released_v2_grant").await?;
    let logical = surface(&pool, 60, "released-grant.eth", &["ens_v1"]).await?;
    let v2_resource = closed_binding(&pool, &logical, 60, "ens_v2").await?;
    for (log, kind, after) in [
        (1, "RegistrationReserved", json!({"status":"reserved"})),
        (
            2,
            "RegistrationGranted",
            json!({"status":"registered","registrant":"0x0000000000000000000000000000000000000001"}),
        ),
        (3, "RegistrationReleased", json!({"status":"unregistered"})),
    ] {
        event(
            &pool,
            &format!("overlap-a-{kind}"),
            &logical,
            Some(&v2_resource),
            Event {
                family: "ens_v2_registry_l1",
                kind,
                log,
                after,
            },
        )
        .await?;
    }
    run(&pool).await?;
    assert_eq!(
        authority(&pool, &logical).await?,
        (Some("ens_v2".into()), None, None, None)
    );
    assert_eq!(
        lifecycle_state(&pool, &logical).await?.as_deref(),
        Some("unregistered")
    );
    let (resource, status): (Option<String>, Option<String>) = sqlx::query_as(
        "SELECT resource_id::text, declared_summary #>> '{registration,status}'
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        (resource.as_deref(), status.as_deref()),
        (Some(v2_resource.as_str()), Some("released"))
    );
    db.cleanup().await?;
    Ok(())
}

// Sepolia group B: the ENSv1 registration ended at expiry plus grace before a fresh ENSv2
// registration. ENSv1 holds nothing now, so its history is not a current candidate.
#[tokio::test]
async fn a_v2_registration_after_the_v1_lease_ended_selects_v2() -> Result<()> {
    let (db, pool) = database("overlap_released_v1_lease").await?;
    let logical = surface(&pool, 61, "after-v1.eth", &["ens_v2"]).await?;
    let v1_resource = closed_binding(&pool, &logical, 61, "ens_v1").await?;
    for (log, kind, after) in [
        (
            1,
            "RegistrationGranted",
            json!({"status":"registered","registrant":"0x0000000000000000000000000000000000000001"}),
        ),
        (2, "RegistrationReleased", json!({"status":"unregistered"})),
    ] {
        event(
            &pool,
            &format!("overlap-b-{kind}"),
            &logical,
            Some(&v1_resource),
            Event {
                family: "ens_v1_registrar_l1",
                kind,
                log,
                after,
            },
        )
        .await?;
    }
    run(&pool).await?;
    assert_eq!(
        authority(&pool, &logical).await?,
        (Some("ens_v2".into()), None, None, None)
    );
    db.cleanup().await?;
    Ok(())
}

// Expected delta (product ruling of 2026-09-25). Nothing is open on either arm, the name's ENSv2
// registration was released, and its latest ENSv1 registry owner, written after the release, is a
// known zero.
// Before: the later ENSv1 fact let ENSv1 decide, so the name served the ownerless-registry profile.
// After: the released ENSv2 registration stays with ENSv2 (arm `ens_v2`, registration `released`).
// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L196-L207 @ ens_v2@a971bd64)
#[tokio::test]
async fn expected_delta_an_ownerless_v1_registry_name_with_a_released_v2_registration_stays_on_v2()
-> Result<()> {
    let (db, pool) = database("overlap_ownerless_both_history").await?;
    let logical = surface(&pool, 66, "ownerless-both.eth", &[]).await?;
    let v1_resource = closed_binding(&pool, &logical, 66, "ens_v1").await?;
    event(
        &pool,
        "overlap-ownerless-v1-owner",
        &logical,
        Some(&v1_resource),
        Event {
            family: "ens_v1_registry_l1",
            kind: "AuthorityTransferred",
            log: 1,
            after: json!({"owner":"0x0000000000000000000000000000000000000001","owner_getter":"0x0000000000000000000000000000000000000001"}),
        },
    )
    .await?;
    let v2_resource = uuid(10, 66);
    sqlx::query("INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state) VALUES ($1::uuid, $2, $3, 10, 'canonical')")
        .bind(&v2_resource).bind(CHAIN).bind(HASH).execute(&pool).await?;
    sqlx::query("INSERT INTO surface_bindings (surface_binding_id, logical_name_id, resource_id, binding_kind, authority_arm, active_from, active_to, chain_id, block_hash, block_number, provenance, canonicality_state) VALUES ($1::uuid, $2, $3::uuid, 'declared_registry_path', 'ens_v2', '2026-08-25T00:00:00Z', '2026-08-25T12:00:00Z', $4, $5, 10, '{\"transaction_index\":0,\"log_index\":2}', 'canonical')")
        .bind(uuid(11, 66)).bind(&logical).bind(&v2_resource).bind(CHAIN).bind(HASH).execute(&pool).await?;
    for (log, kind, after) in [
        (
            2,
            "RegistrationGranted",
            json!({"status":"registered","registrant":"0x0000000000000000000000000000000000000002"}),
        ),
        (3, "RegistrationReleased", json!({"status":"unregistered"})),
    ] {
        event(
            &pool,
            &format!("overlap-ownerless-v2-{kind}"),
            &logical,
            Some(&v2_resource),
            Event {
                family: "ens_v2_registry_l1",
                kind,
                log,
                after,
            },
        )
        .await?;
    }
    event(
        &pool,
        "overlap-ownerless-v1-zero",
        &logical,
        Some(&v1_resource),
        Event {
            family: "ens_v1_registry_l1",
            kind: "AuthorityTransferred",
            log: 4,
            after: json!({"owner":"0x0000000000000000000000000000000000000000","owner_getter":"0x0000000000000000000000000000000000000000"}),
        },
    )
    .await?;
    run(&pool).await?;
    // Before the product ruling of 2026-09-25 the later ENSv1 owner clear let ENSv1 decide and the
    // name served the ownerless-registry profile; the released ENSv2 registration now stays with
    // ENSv2 whatever ENSv1 records, and the ownerless profile never applies under ENSv2.
    assert_eq!(
        authority(&pool, &logical).await?,
        (Some("ens_v2".into()), None, None, None)
    );
    let (support, reason, status, resource): (
        String,
        Option<String>,
        Option<String>,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT support_status, unsupported_reason,
                declared_summary #>> '{registration,status}', resource_id::text
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        (
            support.as_str(),
            reason,
            status.as_deref(),
            resource.as_deref()
        ),
        (
            "supported",
            None,
            Some("released"),
            Some(v2_resource.as_str())
        )
    );
    db.cleanup().await?;
    Ok(())
}

/// Adds an `ens_v2` binding that opened at `log` and at hour `from` of the day before the target
/// block, and closed an hour later.
async fn closed_v2_binding_at(
    pool: &PgPool,
    logical: &str,
    index: u16,
    log: i64,
    from: i32,
) -> Result<String> {
    let resource = uuid(12, index);
    sqlx::query("INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state) VALUES ($1::uuid, $2, $3, 10, 'canonical')")
        .bind(&resource).bind(CHAIN).bind(HASH).execute(pool).await?;
    sqlx::query("INSERT INTO surface_bindings (surface_binding_id, logical_name_id, resource_id, binding_kind, authority_arm, active_from, active_to, chain_id, block_hash, block_number, provenance, canonicality_state) VALUES ($1::uuid, $2, $3::uuid, 'declared_registry_path', 'ens_v2', '2026-08-25T00:00:00Z'::timestamptz + make_interval(hours => $7), '2026-08-25T00:00:00Z'::timestamptz + make_interval(hours => $7 + 1), $4, $5, 10, jsonb_build_object('transaction_index', 0, 'log_index', $6::bigint), 'canonical')")
        .bind(uuid(13, index)).bind(logical).bind(&resource).bind(CHAIN).bind(HASH).bind(log).bind(from).execute(pool).await?;
    Ok(resource)
}

// Expected delta (TYR-36 step 6). A first ENSv2 registration was released, an ENSv1 registry owner
// acquired the name, set a resolver and was cleared to zero, and a second ENSv2 registration was
// granted and released after that. Nothing is open on either arm.
// Before: the released ENSv2 regime held the name on ENSv2 with no selected binding (arm `ens_v2`,
// unsupported `current_authority_not_projected`).
// After: the second release is the latest lifecycle fact, so the name is served as the released
// ENSv2 tombstone (arm `ens_v2`, supported). It still serves no ENSv1 resolver under ENSv2.
// Chain fact: `unregister` burns the token and sets its expiry to now; the ENSv1 registry owner is
// zero, so no ENSv1 holder remains.
// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L195-L207 @ ens_v2@a971bd64)
// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L123-L131 @ ens_v1@91c966f)
#[tokio::test]
async fn expected_delta_a_second_v2_release_after_an_ownerless_v1_registry_is_a_v2_tombstone()
-> Result<()> {
    let (db, pool) = database("overlap_regime_ownerless_v1").await?;
    let logical = surface(&pool, 67, "regime-ownerless.eth", &[]).await?;
    let v2 = |kind: &'static str, log: i64, after: Value| Event {
        family: "ens_v2_registry_l1",
        kind,
        log,
        after,
    };
    let registrant = "0x0000000000000000000000000000000000000002";
    // A: the first ENSv2 registration; B: its qualifying release.
    let first = closed_v2_binding_at(&pool, &logical, 67, 1, 0).await?;
    event(
        &pool,
        "regime-a-grant",
        &logical,
        Some(&first),
        v2(
            "RegistrationGranted",
            1,
            json!({"status":"registered","registrant":registrant}),
        ),
    )
    .await?;
    event(
        &pool,
        "regime-b-release",
        &logical,
        Some(&first),
        v2("RegistrationReleased", 2, json!({"status":"unregistered"})),
    )
    .await?;
    // C: ENSv1 acquires the name and sets a resolver; D: its registry owner is cleared.
    let v1_resource = uuid(14, 67);
    sqlx::query("INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state) VALUES ($1::uuid, $2, $3, 10, 'canonical')")
        .bind(&v1_resource).bind(CHAIN).bind(HASH).execute(&pool).await?;
    for (log, kind, after) in [
        (
            3,
            "AuthorityTransferred",
            json!({"owner":"0x0000000000000000000000000000000000000001","owner_getter":"0x0000000000000000000000000000000000000001"}),
        ),
        (
            4,
            "ResolverChanged",
            json!({"resolver":"0x00000000000000000000000000000000000000c1"}),
        ),
        (
            5,
            "AuthorityTransferred",
            json!({"owner":"0x0000000000000000000000000000000000000000","owner_getter":"0x0000000000000000000000000000000000000000"}),
        ),
    ] {
        event(
            &pool,
            &format!("regime-v1-{log}"),
            &logical,
            Some(&v1_resource),
            Event {
                family: "ens_v1_registry_l1",
                kind,
                log,
                after,
            },
        )
        .await?;
    }
    // E: ENSv2 registers the name again; F: that registration is released too.
    let second = closed_v2_binding_at(&pool, &logical, 68, 6, 2).await?;
    event(
        &pool,
        "regime-e-grant",
        &logical,
        Some(&second),
        v2(
            "RegistrationGranted",
            6,
            json!({"status":"registered","registrant":registrant}),
        ),
    )
    .await?;
    event(
        &pool,
        "regime-f-release",
        &logical,
        Some(&second),
        v2("RegistrationReleased", 7, json!({"status":"unregistered"})),
    )
    .await?;

    run(&pool).await?;
    assert_eq!(
        authority(&pool, &logical).await?,
        (Some("ens_v2".into()), None, None, None)
    );
    let (resource, status): (Option<String>, Option<String>) = sqlx::query_as(
        "SELECT resource_id::text, declared_summary #>> '{registration,status}'
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        (resource.as_deref(), status.as_deref()),
        (Some(second.as_str()), Some("released"))
    );
    let serving: (Option<String>, Option<String>) = sqlx::query_as(
        "SELECT serving_resource_id::text, declared_summary #>> '{resolver,address}'
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(&logical)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        serving,
        (None, None),
        "no ENSv1 resolver serves under ENSv2"
    );
    db.cleanup().await?;
    Ok(())
}

// The root, `eth`, `reverse` and `addr.reverse` follow the ordinary rule: an open ENSv2 binding
// decides whatever ENSv1 holds, no proof is fabricated, and the authority epoch starts at that
// binding like any other. The deployment registers `eth` and `reverse` in the root registry; this
// fixture gives all four names a binding to show the rule, not that all four are root-registry
// registrations, and the root's empty surface is staged but not served.
// (upstream: .refs/ens_v2_sepolia_20260916/contracts/deploy/01_ETHRegistry.ts:L36-L48 @ ens_v2_sepolia_20260916@366de741)
// (upstream: .refs/ens_v2_sepolia_20260916/contracts/deploy/01_ReverseMirror.ts:L25-L37 @ ens_v2_sepolia_20260916@366de741)
#[tokio::test]
async fn root_registry_names_follow_the_ordinary_rule_beside_ensv1() -> Result<()> {
    let (db, pool) = database("issue503_root_registry_names").await?;
    let mut logicals = Vec::new();
    for (index, name) in ["", "eth", "reverse", "addr.reverse"]
        .into_iter()
        .enumerate()
    {
        logicals.push(surface(&pool, index as u16 + 10, name, &["ens_v1", "ens_v2"]).await?);
    }
    capture_staged_authority(&pool).await?;
    run(&pool).await?;
    let binding_position = json!({"block_number": 10, "transaction_index": 0, "log_index": 0});
    let root_authority: CapturedAuthority = sqlx::query_as("SELECT selected_authority_arm, authority_epoch_start_position, authority_proof_kind, authority_proof_event_id, authority_proof_event_identity, authority_transition_id FROM issue503_authority_capture WHERE logical_name_id = $1")
        .bind(&logicals[0])
        .fetch_one(&pool)
        .await?;
    assert_eq!(
        root_authority.selected_authority_arm.as_deref(),
        Some("ens_v2")
    );
    assert_eq!(
        root_authority.authority_epoch_start_position,
        Some(binding_position.clone())
    );
    assert_eq!(root_authority.authority_proof_kind, None);
    assert_eq!(root_authority.authority_proof_event_id, None);
    assert_eq!(root_authority.authority_proof_event_identity, None);
    assert_eq!(root_authority.authority_transition_id, None);
    // The exact root participates in authority selection, while the current
    // name projection intentionally omits its empty surface.
    assert_eq!(optional_authority(&pool, &logicals[0]).await?, None);
    for logical in &logicals[1..] {
        assert_eq!(
            authority(&pool, logical).await?,
            (Some("ens_v2".into()), None, None, None)
        );
        assert_eq!(
            authority_evidence(&pool, logical).await?,
            (None, None, None, Some(binding_position.clone()))
        );
    }
    db.cleanup().await?;
    Ok(())
}

// Expected delta (product ruling of 2026-09-25). `eth` had an ENSv2 registration that was
// released, and has an open ENSv1 binding.
// Before: the released ENSv2 registration was history and the open ENSv1 binding kept `eth`.
// After: `eth` follows the same rule as every other name, so the released registration stays with
// ENSv2 and `eth` is the released ENSv2 tombstone beside its ENSv1 binding.
// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L255-L258 @ ens_v2@a971bd64)
#[tokio::test]
async fn expected_delta_eth_with_a_released_v2_registration_stays_released_on_v2() -> Result<()> {
    let (db, pool) = database("issue503_shared_historical_v2").await?;
    let logical = surface(&pool, 15, "eth", &["ens_v1"]).await?;
    let v2_resource = uuid(2, 15);
    let v2_binding = uuid(4, 15);
    sqlx::query("INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state) VALUES ($1::uuid, $2, $3, 10, 'canonical')")
        .bind(&v2_resource).bind(CHAIN).bind(HASH).execute(&pool).await?;
    sqlx::query("INSERT INTO surface_bindings (surface_binding_id, logical_name_id, resource_id, binding_kind, authority_arm, active_from, active_to, chain_id, block_hash, block_number, provenance, canonicality_state) VALUES ($1::uuid, $2, $3::uuid, 'declared_registry_path', 'ens_v2', '2026-08-25T00:00:00Z', '2026-08-25T12:00:00Z', $4, $5, 10, '{\"transaction_index\":0,\"log_index\":0}', 'canonical')")
        .bind(v2_binding).bind(&logical).bind(&v2_resource).bind(CHAIN).bind(HASH).execute(&pool).await?;
    event(
        &pool,
        "issue503-shared-v2-release",
        &logical,
        Some(&v2_resource),
        Event {
            family: "ens_v2_registrar_l1",
            kind: "RegistrationReleased",
            log: 2,
            after: json!({"status":"unregistered"}),
        },
    )
    .await?;

    run(&pool).await?;
    assert_eq!(
        authority(&pool, &logical).await?,
        (Some("ens_v2".into()), None, None, None)
    );
    assert_eq!(
        lifecycle_state(&pool, &logical).await?.as_deref(),
        Some("unregistered")
    );
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn eth_and_reverse_current_v2_with_historical_or_absent_v1_evidence() -> Result<()> {
    let (db, pool) = database("issue503_shared_current_v2").await?;
    let historical_v1 = surface(&pool, 16, "eth", &["ens_v2"]).await?;
    let v2_only = surface(&pool, 17, "reverse", &["ens_v2"]).await?;
    event(
        &pool,
        "issue503-shared-v1-history",
        &historical_v1,
        None,
        Event {
            family: "ens_v1_registry_l1",
            kind: "AuthorityTransferred",
            log: 1,
            after: json!({"owner":"0x0000000000000000000000000000000000000001"}),
        },
    )
    .await?;

    run(&pool).await?;
    assert_eq!(
        authority(&pool, &historical_v1).await?,
        (Some("ens_v2".into()), None, None, None)
    );
    assert_eq!(
        authority_evidence(&pool, &historical_v1).await?,
        (
            None,
            None,
            None,
            Some(json!({
                "block_number": 10,
                "transaction_index": 0,
                "log_index": 0
            }))
        )
    );
    assert_eq!(
        authority(&pool, &v2_only).await?,
        (Some("ens_v2".into()), None, None, None)
    );
    assert_eq!(
        authority_evidence(&pool, &v2_only).await?,
        (
            None,
            None,
            None,
            Some(json!({
                "block_number": 10,
                "transaction_index": 0,
                "log_index": 0
            }))
        )
    );
    db.cleanup().await?;
    Ok(())
}

// Descendants follow the same rule: their current ENSv2 registration decides and they publish its
// authority epoch.
#[tokio::test]
async fn reverse_descendants_follow_the_ordinary_rule() -> Result<()> {
    let (db, pool) = database("issue503_reverse_descendants").await?;
    let a = surface(&pool, 20, "alice.addr.reverse", &["ens_v1", "ens_v2"]).await?;
    let b = surface(&pool, 21, "default.reverse", &["ens_v1", "ens_v2"]).await?;
    run(&pool).await?;
    for logical in [a, b] {
        assert_eq!(
            authority(&pool, &logical).await?,
            (Some("ens_v2".into()), None, None, None)
        );
        assert!(authority_evidence(&pool, &logical).await?.3.is_some());
    }
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn proven_sepolia_dual_current_exact_name_is_fatal() -> Result<()> {
    let (db, pool) = database("issue503_exact_fatal").await?;
    let logical = surface(&pool, 30, "proven.eth", &["ens_v1", "ens_v2"]).await?;
    let successor_binding = uuid(4, 30);
    let successor_resource = uuid(2, 30);
    event(
        &pool,
        "issue503-exact-proof",
        &logical,
        None,
        Event {
            family: "ens_v2_migration_l1",
            kind: "MigrationApplied",
            log: 1,
            after: json!({"migration_path":"unlocked_wrapped","successor_binding":{"binding_id":successor_binding,"resource_id":successor_resource}}),
        },
    )
    .await?;
    let error = run(&pool)
        .await
        .expect_err("proven Sepolia conflict must fail");
    let evidence = error
        .generation_failure_evidence()
        .context("failure evidence")?;
    assert_eq!(evidence.failure_kind, DUAL_CURRENT_EXACT_NAME_AUTHORITY);
    assert_eq!(evidence.logical_name_id, logical);
    assert_eq!(
        evidence.payload["boundary"]["event_identity"],
        "issue503-exact-proof"
    );
    assert_eq!(evidence.payload["target"]["block_number"], 10);
    assert_eq!(evidence.failure_fingerprint.len(), 64);
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn proven_sepolia_dual_current_child_is_fatal() -> Result<()> {
    let (db, pool) = database("issue503_child_fatal").await?;
    let parent = surface(&pool, 40, "parent.eth", &["ens_v2"]).await?;
    let child = surface(&pool, 41, "child.parent.eth", &["ens_v2"]).await?;
    let registry = uuid(8, 40);
    sqlx::query("INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind) VALUES ($1::uuid, $2, 'contract')")
        .bind(&registry).bind(CHAIN).execute(&pool).await?;
    sqlx::query("INSERT INTO contract_instance_addresses (contract_instance_id, chain_id, address, active_from_block_number) VALUES ($1::uuid, $2, '0x0000000000000000000000000000000000000503', 10)")
        .bind(&registry).bind(CHAIN).execute(&pool).await?;
    event(
        &pool,
        "issue503-parent-registry",
        &parent,
        None,
        Event {
            family: "ens_v2_registry_l1",
            kind: "SubregistryChanged",
            log: 1,
            after: json!({"subregistry":"0x0000000000000000000000000000000000000503"}),
        },
    )
    .await?;
    event(
        &pool,
        "issue503-v2-child",
        &child,
        Some(&uuid(1, 41)),
        Event {
            family: "ens_v2_registry_l1",
            kind: "RegistrationGranted",
            log: 2,
            after: json!({"registry_contract_instance_id":registry,"status":"registered","registrant":"0x0000000000000000000000000000000000000001"}),
        },
    )
    .await?;
    event(
        &pool,
        "issue503-child-proof",
        &child,
        None,
        Event {
            family: "ens_v2_migration_l1",
            kind: "MigrationApplied",
            log: 3,
            after: json!({"migration_path":"locked_wrapped","successor_binding":{"binding_id":uuid(3, 41),"resource_id":uuid(1, 41)}}),
        },
    )
    .await?;
    event(
        &pool,
        "issue503-v1-child",
        &child,
        None,
        Event {
            family: "ens_v1_registry_l1",
            kind: "SubregistryChanged",
            log: 4,
            after: json!({"node":parent.trim_start_matches("ens:"),"child_node":child.trim_start_matches("ens:"),"labelhash":labelhash("child"),"owner":"0x0000000000000000000000000000000000000002"}),
        },
    )
    .await?;
    let error = run(&pool)
        .await
        .expect_err("proven Sepolia child conflict must fail");
    let evidence = error
        .generation_failure_evidence()
        .context("failure evidence")?;
    assert_eq!(evidence.failure_kind, DUAL_CURRENT_CHILD_AUTHORITY);
    assert_eq!(evidence.payload["parent_logical_name_id"], parent);
    assert_eq!(evidence.logical_name_id, child);
    assert_eq!(
        evidence.payload["authority_proof_event_identity"],
        "issue503-child-proof"
    );
    assert_eq!(
        evidence.payload["predecessor"]["event_identity"],
        "issue503-v1-child"
    );
    assert_eq!(evidence.target_block_number, 10);
    assert_eq!(evidence.failure_fingerprint.len(), 64);
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn eth_on_both_arms_without_proof_is_not_integrity_fatal() -> Result<()> {
    let (db, pool) = database("issue503_eth_nonfatal").await?;
    let logical = surface(&pool, 50, "eth", &["ens_v1", "ens_v2"]).await?;
    run(&pool).await?;
    let selected = authority(&pool, &logical).await?;
    assert_eq!(selected, (Some("ens_v2".into()), None, None, None));
    db.cleanup().await?;
    Ok(())
}

// On Sepolia `eth` and `reverse` are registered in the ENSv2 root registry, so their ENSv2 facts come
// from the root family and no ETHRegistrar event exists for them. A registration in the admitted
// root registry is served like any other, with no proof fabricated for it.
// (upstream: .refs/ens_v2_sepolia_20260916/contracts/deploy/01_ETHRegistry.ts:L36-L48 @ ens_v2_sepolia_20260916@366de741)
// (upstream: .refs/ens_v2_sepolia_20260916/contracts/deploy/01_ReverseMirror.ts:L25-L37 @ ens_v2_sepolia_20260916@366de741)
#[tokio::test]
async fn root_registry_eth_and_reverse_serve_without_a_registrar_event() -> Result<()> {
    let (db, pool) = database("issue503_root_family").await?;
    let root_owner = "0x0000000000000000000000000000000000000660";
    for (index, name) in [(60_u16, "eth"), (61, "reverse")] {
        let logical = surface(&pool, index, name, &["ens_v1", "ens_v2"]).await?;
        event(
            &pool,
            &format!("issue503-{name}-v1-owner"),
            &logical,
            Some(&uuid(1, index)),
            Event {
                family: "ens_v1_registry_l1",
                kind: "AuthorityTransferred",
                log: 1,
                after: json!({"owner":"0x0000000000000000000000000000000000000001"}),
            },
        )
        .await?;
        event(
            &pool,
            &format!("issue503-{name}-root-grant"),
            &logical,
            Some(&uuid(2, index)),
            Event {
                family: "ens_v2_root_l1",
                kind: "RegistrationGranted",
                log: 2,
                after: json!({"label":name,"registrant":root_owner,"owner":root_owner,"expiry":u64::MAX}),
            },
        )
        .await?;
        run(&pool).await?;
        assert_eq!(
            authority(&pool, &logical).await?,
            (Some("ens_v2".into()), None, None, None),
            "{name}"
        );
        let (status, reason, coverage_reason, registrant): (
            String,
            Option<String>,
            Value,
            Option<String>,
        ) = sqlx::query_as("SELECT support_status, unsupported_reason, declared_summary #> '{coverage,unsupported_reason}', declared_summary #>> '{registration,registrant}' FROM name_current WHERE logical_name_id = $1")
            .bind(&logical)
            .fetch_one(&pool)
            .await?;
        assert_eq!(
            (status.as_str(), reason, coverage_reason),
            ("supported", None, Value::Null),
            "{name}"
        );
        assert_eq!(registrant.as_deref(), Some(root_owner), "{name}");
    }
    let registrar_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events WHERE source_family = 'ens_v2_registrar_l1'",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(registrar_events, 0);
    db.cleanup().await?;
    Ok(())
}

fn ops_event(family: &'static str, kind: &'static str, log: i64, after: Value) -> Event<'static> {
    Event {
        family,
        kind,
        log,
        after,
    }
}

// These are explicit normalized fixtures, not an RPC or adapter replay proof.
async fn ops_project(
    pool: &PgPool,
    target: i64,
    previous: Option<bigname_project::Marker>,
    mode: RunMode,
) -> Result<bigname_project::Marker> {
    let request = BatchRequest {
        chain_id: CHAIN.into(),
        target_block: target,
        affected_from_block: previous.as_ref().map_or(10, |_| 11),
        affected_to_block: target,
        resume_current: previous,
        mode,
    };
    Ok(Engine::new(pool.clone()).run_batch(request).await?.current)
}

async fn ops_summary(pool: &PgPool, logical: &str) -> Result<Value> {
    let query = "SELECT declared_summary FROM name_current WHERE logical_name_id = $1";
    Ok(sqlx::query_scalar(query)
        .bind(logical)
        .fetch_one(pool)
        .await?)
}

#[tokio::test]
async fn selected_v2_token_owner_incremental_rebuild_and_fixture_redo() -> Result<()> {
    let (db, pool) = database("ops_owner_project").await?;
    let logical = surface(&pool, 70, "ops-owner.eth", &["ens_v2"]).await?;
    let resource = uuid(1, 70);
    let alice = "0x0000000000000000000000000000000000000001";
    let bob = "0x0000000000000000000000000000000000000002";
    for (log, (kind, after)) in [
        (
            "RegistrationGranted",
            json!({"status":"registered","registrant":alice,"token_id":"70"}),
        ),
        (
            "AuthorityTransferred",
            json!({"owner":alice,"token_id":"70"}),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        event(
            &pool,
            kind,
            &logical,
            Some(&resource),
            ops_event("ens_v2_registry_l1", kind, log as i64 + 1, after),
        )
        .await?;
    }
    let prefix = ops_project(&pool, 10, None, RunMode::Normal).await?;
    let before = ops_summary(&pool, &logical).await?;
    assert_eq!(before["control"]["registry_owner"], alice);
    assert_eq!(before["registration"]["registrant"], alice);
    sqlx::query("INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state) VALUES ($1, '0x50311', 11, '2026-08-26T00:00:12Z', 'canonical')")
        .bind(CHAIN).execute(&pool).await?;
    let sale = event(
        &pool,
        "ops-sale",
        &logical,
        Some(&resource),
        ops_event(
            "ens_v2_registry_l1",
            "TokenControlTransferred",
            3,
            json!({"from":alice,"to":bob,"token_id":"70"}),
        ),
    )
    .await?;
    // Place the fixture suffix before Project consumes it; never repair a projection.
    sqlx::query("UPDATE normalized_events SET block_number = 11, block_hash = '0x50311' WHERE normalized_event_id = $1")
        .bind(sale).execute(&pool).await?;
    let current = ops_project(&pool, 11, Some(prefix.clone()), RunMode::Normal).await?;
    let transferred = ops_summary(&pool, &logical).await?;
    assert_eq!(transferred["registration"]["registrant"], bob);
    let owner = &transferred["control"]["registry_owner"];
    assert_eq!(owner, bob, "selected ENSv2 buyer must own the name");
    let selected: Option<String> =
        sqlx::query_scalar("SELECT resource_id::text FROM name_current WHERE logical_name_id = $1")
            .bind(&logical)
            .fetch_one(&pool)
            .await?;
    assert_eq!(selected.as_deref(), Some(resource.as_str()));
    ops_project(&pool, 11, None, RunMode::Normal).await?;
    assert_eq!(ops_summary(&pool, &logical).await?, transferred);
    // Explicit normalized-fixture retraction/reapplication exercises Project Redo,
    // not a claim about RPC reorg handling or adapter restoration.
    sqlx::query("UPDATE normalized_events SET canonicality_state = 'orphaned' WHERE normalized_event_id = $1")
        .bind(sale).execute(&pool).await?;
    ops_project(&pool, 11, Some(current.clone()), RunMode::Redo).await?;
    let retracted = ops_summary(&pool, &logical).await?;
    assert_eq!(retracted["registration"]["registrant"], alice);
    assert_eq!(retracted["control"]["registry_owner"], alice);
    sqlx::query("UPDATE normalized_events SET canonicality_state = 'canonical' WHERE normalized_event_id = $1")
        .bind(sale).execute(&pool).await?;
    ops_project(&pool, 11, Some(current), RunMode::Redo).await?;
    assert_eq!(ops_summary(&pool, &logical).await?, transferred);
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn token_owner_selection_preserves_authority_and_lifecycle_boundaries() -> Result<()> {
    let (db, pool) = database("ops_owner_controls").await?;
    let alice = "0x0000000000000000000000000000000000000001";
    let bob = "0x0000000000000000000000000000000000000002";
    for (index, mode) in "role rival orphan v1 same release reserved replacement order"
        .split_whitespace()
        .enumerate()
    {
        let index = 80 + index as u16;
        let family = if mode == "v1" {
            "ens_v1_registrar_l1"
        } else {
            "ens_v2_registry_l1"
        };
        let logical = surface(
            &pool,
            index,
            &format!("ops-{mode}.eth"),
            &[if mode == "v1" { "ens_v1" } else { "ens_v2" }],
        )
        .await?;
        let resource = uuid(1, index);
        event(&pool, &format!("{mode}-grant"), &logical, Some(&resource), Event {
            family, kind: "RegistrationGranted", log: 1,
            after: json!({"status":"registered","registrant":alice,"token_id":index.to_string()}),
        }).await?;
        event(
            &pool,
            &format!("{mode}-owner"),
            &logical,
            Some(&resource),
            ops_event(
                if mode == "v1" {
                    "ens_v1_registry_l1"
                } else {
                    family
                },
                "AuthorityTransferred",
                2,
                json!({"owner":alice}),
            ),
        )
        .await?;
        let rival = uuid(9, index);
        if matches!(mode, "rival" | "replacement") {
            sqlx::query("INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state) VALUES ($1::uuid, $2, $3, 10, 'canonical')")
                .bind(&rival).bind(CHAIN).bind(HASH).execute(&pool).await?;
        }
        let (kind, after) = match mode {
            "role" => (
                "PermissionChanged",
                json!({"subject":bob,"roles":["owner"]}),
            ),
            "release" => ("RegistrationReleased", json!({"status":"unregistered"})),
            "reserved" => (
                "RegistrationReserved",
                json!({"status":"reserved","token_id":index.to_string()}),
            ),
            _ => (
                "TokenControlTransferred",
                json!({"from":alice,"to":if mode == "same" { alice } else { bob },"token_id":index.to_string()}),
            ),
        };
        let suffix = event(
            &pool,
            &format!("{mode}-suffix"),
            &logical,
            Some(if mode == "rival" { &rival } else { &resource }),
            ops_event(family, kind, 3, after),
        )
        .await?;
        if mode == "orphan" {
            sqlx::query("UPDATE normalized_events SET canonicality_state = 'orphaned' WHERE normalized_event_id = $1")
                .bind(suffix).execute(&pool).await?;
        }
        if mode == "replacement" {
            sqlx::query(
                "UPDATE surface_bindings SET resource_id = $1::uuid WHERE logical_name_id = $2",
            )
            .bind(&rival)
            .bind(&logical)
            .execute(&pool)
            .await?;
            event(&pool, "replacement-next-grant", &logical, Some(&rival), Event {
                family, kind: "RegistrationGranted", log: 4,
                after: json!({"status":"registered","registrant":alice,"token_id":"replacement"}),
            }).await?;
            sqlx::query(
                "UPDATE normalized_events SET log_index = 6 WHERE normalized_event_id = $1",
            )
            .bind(suffix)
            .execute(&pool)
            .await?;
        }
        if matches!(mode, "replacement" | "order") {
            let (target, kind, log, after) = if mode == "replacement" {
                (&rival, "AuthorityTransferred", 5, json!({"owner":alice}))
            } else {
                (
                    &resource,
                    "TokenControlTransferred",
                    3,
                    json!({"from":bob,"to":alice,"token_id":index.to_string()}),
                )
            };
            event(
                &pool,
                &format!("{mode}-last"),
                &logical,
                Some(target),
                ops_event(family, kind, log, after),
            )
            .await?;
        }
        run(&pool).await?;
        let summary = ops_summary(&pool, &logical).await?;
        if mode == "release" {
            assert_eq!(summary["registration"]["status"], "released");
            assert!(summary["control"]["registry_owner"].is_null());
        } else {
            let owner = &summary["control"]["registry_owner"];
            assert_eq!(owner, alice, "{mode}: {summary}");
            if mode == "v1" {
                assert_eq!(summary["registration"]["registrant"], bob);
            }
            if mode == "reserved" {
                assert_eq!(summary["registration"]["status"], "reserved");
            }
        }
    }
    db.cleanup().await?;
    Ok(())
}

#[path = "issue_503/migration_profile.rs"]
mod migration_profile;

#[path = "issue_503/child_cutoff.rs"]
mod child_cutoff;

#[path = "issue_503/migration_readers.rs"]
mod migration_readers;

#[path = "issue_503/wrapper_control.rs"]
mod wrapper_control;

#[path = "issue_503/nameless_expiry.rs"]
mod nameless_expiry;

#[path = "issue_503/expired_reservation.rs"]
mod expired_reservation;

#[path = "issue_503/reservation_resource.rs"]
mod reservation_resource;
