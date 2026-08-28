use anyhow::{Context, Result};
use bigname_project::{BatchRequest, Engine, Marker, RunMode};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgPool, raw_sql};

const CHAIN: &str = "ethereum-sepolia";
const MAIN: &str = "ens:0x787192fc5378cc32aa956ddfdedbf26b24e8d78e40109add0eea2c1a012c3dec";
const GENERIC: &str = "ens:0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const RESERVATION: &str = "ens:0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
const MIXED: &str = "ens:0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
const RESOURCE: &str = "b64d3841-80ce-5f90-bb3c-575c05361e16";
const LINEAGE: &str = "27c01db8-d04a-5c85-af28-dfed7797bc79";
const VERSION_RESOURCE: &str = "cccccccc-cccc-cccc-cccc-cccccccccccc";
const VERSION_LINEAGE: &str = "dddddddd-dddd-dddd-dddd-dddddddddddd";
const TOKEN: &str = "0x0000000000000000000000000000000000000000000000000000000000000065";
const VERSION_TOKEN: &str = "0x0000000100000000000000000000000000000000000000000000000000000065";
const REGISTRY: &str = "0x00000000000000000000000000000000000020aa";
const OWNER: &str = "0x0000000000000000000000000000000000002011";
const SUBJECT: &str = "0x0000000000000000000000000000000000002033";
const FIXTURE: &str =
    include_str!("../../adapters/tests/fixtures/interpreters/v2-expiry-retirement.json");
const TABLES: &str = "name_current children_current permissions_current \
    permissions_current_resource_summary record_inventory_current resolver_current \
    address_names_current primary_names_current";

fn hash(block: i64) -> &'static str {
    match block {
        100 => "0xb1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1",
        101 => "0xb3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3",
        102 => "0xb5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5",
        103 => "0xc3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3",
        _ => unreachable!(),
    }
}

async fn database(prefix: &str) -> Result<(TestDatabase, PgPool)> {
    let database = TestDatabase::create(TestDatabaseConfig::new(prefix)).await?;
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
    Ok((database, pool))
}

async fn event(
    pool: &PgPool,
    logical: &str,
    resource: Option<&str>,
    block: i64,
    log: Option<i64>,
    kind: &str,
    after: Value,
) -> Result<()> {
    let family = if logical == MIXED && kind == "AuthorityTransferred" {
        "ens_v1_registry_l1"
    } else {
        "ens_v2_registry_l1"
    };
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, resource_id, event_kind,
             source_family, manifest_version, chain_id, block_number, block_hash,
             transaction_hash, transaction_index, log_index, derivation_kind,
             canonicality_state, after_state
         ) VALUES ($1, 'ens', $2, $3::uuid, $4, $5, 1, $6, $7, $8,
                   CASE WHEN $9::bigint IS NULL THEN NULL ELSE $10 END,
                   CASE WHEN $9::bigint IS NULL THEN NULL ELSE 0 END, $9,
                   'ens_v2_registry_resource_surface', 'canonical', $11)",
    )
    .bind(format!("{block}:{logical}:{kind}:{log:?}"))
    .bind(logical)
    .bind(resource)
    .bind(kind)
    .bind(family)
    .bind(CHAIN)
    .bind(block)
    .bind(hash(block))
    .bind(log)
    .bind(format!("0x{:064x}", block))
    .bind(after)
    .execute(pool)
    .await?;
    Ok(())
}

fn expired(expiry: i64) -> Value {
    json!({
        "source_event":"RegistryPathExpired",
        "derived_from":"interpreter_state",
        "terminal_reason":"registry_name_binding_expired",
        "registry":REGISTRY,
        "token_id":TOKEN,
        "registry_contract_instance_id":"00000000-0000-0000-0000-000000000001",
        "expiry":expiry,
        "status":"released",
        "released_at":expiry,
    })
}

#[rustfmt::skip]
async fn seed(pool: &PgPool) -> Result<()> {
    let fixture: Value = serde_json::from_str(FIXTURE)?;
    let expected = fixture.get("expected_project").context("expiry fixture expected_project")?;
    assert_eq!(expected["logical_name_id"], MAIN); assert_eq!(expected["resource_id"], RESOURCE);
    assert_eq!(expected["token_lineage_id"], LINEAGE); assert_eq!(expected["token_id"], TOKEN);
    assert_eq!(expected["registry"], REGISTRY); assert_eq!(expected["expiry"], 1_800_000_000_i64);

    for (block, timestamp) in [
        (100, 1_700_000_100_i64), (101, 1_800_000_000), (102, 1_800_000_001), (103, 1_900_000_000),
    ] {
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, block_number, block_timestamp, canonicality_state
             ) VALUES ($1, $2, $3, to_timestamp($4), 'canonical')",
        )
        .bind(CHAIN)
        .bind(hash(block))
        .bind(block)
        .bind(timestamp)
        .execute(pool)
        .await?;
    }
    let (h100, h102, h103) = (hash(100), hash(102), hash(103)); let (main_hash, generic_hash) = (MAIN.trim_start_matches("ens:"), GENERIC.trim_start_matches("ens:")); let (reservation_hash, mixed_hash) = (RESERVATION.trim_start_matches("ens:"), MIXED.trim_start_matches("ens:")); let (one, two, three, four) = (1_u64, 2_u64, 3_u64, 4_u64);
    raw_sql(&format!(
        "INSERT INTO token_lineages (token_lineage_id, chain_id, block_hash, block_number, canonicality_state)
         VALUES ('{LINEAGE}', '{CHAIN}', '{h100}', 100, 'canonical'), ('{VERSION_LINEAGE}', '{CHAIN}', '{h103}', 103, 'canonical');
         INSERT INTO resources (resource_id, token_lineage_id, chain_id, block_hash, block_number, canonicality_state)
         VALUES ('{RESOURCE}', '{LINEAGE}', '{CHAIN}', '{h100}', 100, 'canonical'), ('{VERSION_RESOURCE}', '{VERSION_LINEAGE}', '{CHAIN}', '{h103}', 103, 'canonical');
         INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash,
             labelhashes, normalizer_version, visibility_state, chain_id, block_hash, block_number, canonicality_state)
         VALUES
           ('{MAIN}', 'ens', 'alice.eth', ARRAY['alice','eth'], '\\x05616c6963650365746800', '{main_hash}', ARRAY['0x{one:064x}','0x{two:064x}'], 'ensip15', 'active', '{CHAIN}', '{h100}', 100, 'canonical'),
           ('{GENERIC}', 'ens', 'generic.eth', ARRAY['generic','eth'], '\\x0767656e657269630365746800', '{generic_hash}', ARRAY['0x{two:064x}','0x{one:064x}'], 'ensip15', 'active', '{CHAIN}', '{h100}', 100, 'canonical'),
           ('{RESERVATION}', 'ens', 'reserved.eth', ARRAY['reserved','eth'], '\\x0872657365727665640365746800', '{reservation_hash}', ARRAY['0x{three:064x}','0x{two:064x}'], 'ensip15', 'active', '{CHAIN}', '{h100}', 100, 'canonical'),
           ('{MIXED}', 'ens', 'mixed.eth', ARRAY['mixed','eth'], '\\x056d697865640365746800', '{mixed_hash}', ARRAY['0x{four:064x}','0x{two:064x}'], 'ensip15', 'active', '{CHAIN}', '{h100}', 100, 'canonical');
         INSERT INTO surface_bindings (surface_binding_id, logical_name_id, resource_id, binding_kind, authority_arm,
             active_from, active_to, chain_id, block_hash, block_number, canonicality_state)
         VALUES
           ('6347b94d-744e-5e3c-a8a9-38cefbcf0e25', '{MAIN}', '{RESOURCE}', 'declared_registry_path', 'ens_v2', to_timestamp(1700000100), to_timestamp(1800000000), '{CHAIN}', '{h100}', 100, 'canonical'),
           ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', '{GENERIC}', '{RESOURCE}', 'declared_registry_path', 'ens_v2', to_timestamp(1700000100), to_timestamp(1800000000), '{CHAIN}', '{h100}', 100, 'canonical'),
           ('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', '{MAIN}', '{RESOURCE}', 'declared_registry_path', 'ens_v2', to_timestamp(1800000001), to_timestamp(1900000000), '{CHAIN}', '{h102}', 102, 'canonical'),
           ('eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee', '{MAIN}', '{VERSION_RESOURCE}', 'declared_registry_path', 'ens_v2', to_timestamp(1900000000), NULL, '{CHAIN}', '{h103}', 103, 'canonical')",
    ))
    .execute(pool)
    .await?;

    let granted = |source: &str, token: &str, expiry: i64| json!({
        "source_event":source, "status":"registered", "authority_kind":"ens_v2_registry",
        "registrant":OWNER, "expiry":expiry, "token_id":token,
    });
    let permission = json!({
        "subject":SUBJECT,
        "scope":{"kind":"registry","chain_id":CHAIN,"registry_address":REGISTRY}, "effective_powers":["resource_control"],
        "grant_source":{"kind":"raw_log","source_event":"EACRolesChanged","changed_powers":["resource_control"],"registry_contract_instance_id":"00000000-0000-0000-0000-000000000001"},
        "revocation_source":null, "inheritance_path":[], "transfer_behavior":{}, "source_event":"EACRolesChanged",
        "upstream_resource":format!("0x{:064x}", 5001_u64), "role_bitmap":format!("0x{:064x}", 1_u64),
        "old_role_bitmap":format!("0x{:064x}", 0_u64), "root_resource":false, "registry_contract_instance_id":"00000000-0000-0000-0000-000000000001",
    });
    macro_rules! add { ($name:expr, $resource:expr, $block:expr, $log:expr, $kind:expr, $state:expr) => {
        event(pool, $name, $resource, $block, $log, $kind, $state).await?;
    }; }
    add!(MAIN, Some(RESOURCE), 100, Some(0), "RegistrationGranted", granted("LabelRegistered", TOKEN, 1_800_000_000));
    add!(MAIN, Some(RESOURCE), 100, Some(4), "PermissionChanged", permission);
    add!(RESERVATION, Some(RESOURCE), 100, Some(5), "RegistrationReserved", json!({"source_event":"LabelReserved","status":"reserved","expiry":1_800_000_000_i64}));
    add!(MIXED, Some(RESOURCE), 100, Some(6), "RegistrationReserved", json!({"source_event":"LabelReserved","status":"reserved","expiry":1_800_000_000_i64}));
    event(pool, MIXED, None, 100, Some(7), "AuthorityTransferred", json!({"source_event":"NewOwner","owner":OWNER})).await?;
    add!(MAIN, Some(RESOURCE), 101, None, "RegistrationReleased", expired(1_800_000_000));
    add!(MAIN, Some(RESOURCE), 101, Some(9), "RegistrationReleased", json!({"source_event":"LabelUnregistered","status":"released"}));
    add!(GENERIC, Some(RESOURCE), 101, Some(10), "RegistrationReleased", json!({"source_event":"LabelUnregistered","status":"released"})); add!(MAIN, Some(RESOURCE), 101, Some(11), "RegistrationRenewed", json!({"source_event":"ExpiryUpdated","status":"registered","expiry":1_800_000_000_i64,"token_id":TOKEN}));
    add!(RESERVATION, Some(RESOURCE), 101, None, "RegistrationReleased", expired(1_800_000_000));
    add!(MIXED, Some(RESOURCE), 101, None, "RegistrationReleased", expired(1_800_000_000));
    add!(MAIN, Some(RESOURCE), 102, Some(0), "RegistrationGranted", granted("ExpiryUpdated", TOKEN, 1_900_000_000));
    add!(MAIN, Some(RESOURCE), 102, Some(1), "ExpiryChanged", json!({"source_event":"ExpiryUpdated","expiry":1_900_000_000_i64,"token_id":TOKEN}));
    add!(RESERVATION, Some(RESOURCE), 102, Some(2), "RegistrationReserved", json!({"source_event":"ExpiryUpdated","status":"reserved","expiry":1_900_000_000_i64}));
    add!(MAIN, Some(RESOURCE), 103, None, "RegistrationReleased", expired(1_900_000_000)); add!(MAIN, Some(VERSION_RESOURCE), 103, Some(0), "RegistrationGranted", granted("LabelRegistered", VERSION_TOKEN, 2_000_000_000));
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

async fn fresh(prefix: &str, target: i64) -> Result<(TestDatabase, PgPool)> {
    let (database, pool) = database(prefix).await?;
    seed(&pool).await?;
    run(&pool, target, None).await?;
    Ok((database, pool))
}

#[tokio::test]
async fn expiry_permissions_and_names_converge_through_revival_and_version_bump() -> Result<()> {
    let (incremental_db, incremental) = database("v2_expiry_incremental").await?;
    seed(&incremental).await?;
    let live = run(&incremental, 100, None).await?;
    let live_state: (i64, i64, i64, Option<String>) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM name_current),
                (SELECT count(*) FROM permissions_current WHERE resource_id = $1::uuid),
                (SELECT count(*) FROM permissions_current_resource_summary WHERE resource_id = $1::uuid),
                (SELECT unsupported_reason FROM permissions_current_resource_summary
                 WHERE resource_id = $1::uuid)",
    )
    .bind(RESOURCE)
    .fetch_one(&incremental)
    .await?;
    assert_eq!(
        live_state,
        (
            4,
            1,
            1,
            Some("operator_approval_surfaces_not_ingested".into())
        )
    );

    let retired = run(&incremental, 101, Some(live)).await?;
    let (retired_db, retired_fresh) = fresh("v2_expiry_retired_fresh", 101).await?;
    assert_eq!(
        snapshot(&incremental).await?,
        snapshot(&retired_fresh).await?
    );
    let retired_state: (i64, i64, i64, i64, i64, Option<String>) = sqlx::query_as(
        "SELECT count(*) FILTER (WHERE logical_name_id = $1),
                count(*) FILTER (WHERE logical_name_id = $2),
                count(*) FILTER (WHERE logical_name_id = $3),
                count(*) FILTER (WHERE logical_name_id = $4),
                (SELECT count(*) FROM permissions_current WHERE resource_id = $5::uuid),
                (SELECT unsupported_reason FROM permissions_current_resource_summary
                 WHERE resource_id = $5::uuid)
         FROM name_current",
    )
    .bind(MAIN)
    .bind(GENERIC)
    .bind(RESERVATION)
    .bind(MIXED)
    .bind(RESOURCE)
    .fetch_one(&incremental)
    .await?;
    assert_eq!(
        retired_state,
        (
            0,
            1,
            0,
            1,
            0,
            Some("operator_approval_surfaces_not_ingested".into())
        )
    );
    let mixed: (Option<String>, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT provenance -> 'authority_selection' ->> 'authority_arm',
                declared_summary -> 'registration' ->> 'status',
                declared_summary -> 'control' ->> 'status'
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(MIXED)
    .fetch_one(&incremental)
    .await?;
    assert_eq!(mixed, (Some("ens_v1".into()), None, None));
    let retained: (i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM normalized_events WHERE logical_name_id = $1
                    AND event_kind = 'RegistrationReleased'
                    AND block_number = 101
                    AND after_state ->> 'source_event' = 'RegistryPathExpired'),
                (SELECT count(*) FROM resources WHERE resource_id = $2::uuid),
                (SELECT count(*) FROM token_lineages WHERE token_lineage_id = $3::uuid)",
    )
    .bind(MAIN)
    .bind(RESOURCE)
    .bind(LINEAGE)
    .fetch_one(&incremental)
    .await?;
    assert_eq!(retained, (1, 1, 1));

    let revived = run(&incremental, 102, Some(retired)).await?;
    let (revived_db, revived_fresh) = fresh("v2_expiry_revived_fresh", 102).await?;
    assert_eq!(
        snapshot(&incremental).await?,
        snapshot(&revived_fresh).await?
    );
    let revival: (String, String, i64) = sqlx::query_as(
        "SELECT resource_id::text, token_lineage_id::text,
                (SELECT count(*) FROM permissions_current
                 WHERE resource_id = name_current.resource_id)
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(MAIN)
    .fetch_one(&incremental)
    .await?;
    assert_eq!(revival, (RESOURCE.into(), LINEAGE.into(), 1));

    run(&incremental, 103, Some(revived)).await?;
    let (version_db, version_fresh) = fresh("v2_expiry_version_fresh", 103).await?;
    assert_eq!(
        snapshot(&incremental).await?,
        snapshot(&version_fresh).await?
    );
    let version: (String, i64, i64, i64, Option<String>) = sqlx::query_as(
        "SELECT (SELECT resource_id::text FROM name_current WHERE logical_name_id = $1),
                (SELECT count(*) FROM permissions_current WHERE resource_id = $2::uuid),
                (SELECT count(*) FROM permissions_current WHERE resource_id = $3::uuid),
                (SELECT count(*) FROM permissions_current_resource_summary
                 WHERE resource_id = $3::uuid),
                (SELECT unsupported_reason FROM permissions_current_resource_summary
                 WHERE resource_id = $3::uuid)",
    )
    .bind(MAIN)
    .bind(RESOURCE)
    .bind(VERSION_RESOURCE)
    .fetch_one(&incremental)
    .await?;
    assert_eq!(
        version,
        (
            VERSION_RESOURCE.into(),
            0,
            0,
            1,
            Some("operator_approval_surfaces_not_ingested".into()),
        )
    );

    version_db.cleanup().await?;
    revived_db.cleanup().await?;
    retired_db.cleanup().await?;
    incremental_db.cleanup().await?;
    Ok(())
}
