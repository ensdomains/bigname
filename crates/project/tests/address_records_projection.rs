//! Builder-level coverage for `address_records_current`, the reverse index over current
//! `addr:<coin_type>` resolver records ("names that resolve to this address").

use anyhow::{Context, Result};
use bigname_project::{BatchRequest, Engine, Marker, RunMode};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgPool, raw_sql};

const CHAIN: &str = "ethereum-mainnet";
const RESOURCE: &str = "68300000-0000-0000-0000-000000000100";
const BINDING: &str = "68300000-0000-0000-0000-000000000101";
const NODE: &str = "0x6830000000000000000000000000000000000000000000000000000000000000";
const LOGICAL_NAME: &str = "ens:0x6830000000000000000000000000000000000000000000000000000000000000";
const RESOLVER: &str = "0x6830000000000000000000000000000000000060";
const REGISTRY: &str = "0x6830000000000000000000000000000000000044";
const OWNER: &str = "0x68300000000000000000000000000000000000a1";
const ZERO20: &str = "0x0000000000000000000000000000000000000000";
const ADDRESS_A: &str = "0x1111111111111111111111111111111111111111";
const ADDRESS_B: &str = "0x2222222222222222222222222222222222222222";
const ADDRESS_A_MIXED_CASE: &str = "0x1111111111111111111111111111111111111111";

#[derive(Clone, Debug)]
struct RecordWrite {
    block: i64,
    log_index: i64,
    coin: &'static str,
    value: &'static str,
}

fn write(block: i64, log_index: i64, coin: &'static str, value: &'static str) -> RecordWrite {
    RecordWrite {
        block,
        log_index,
        coin,
        value,
    }
}

#[tokio::test]
async fn addr_records_follow_ownerless_serving_records_through_redo() -> Result<()> {
    let (database, pool) = migrated_pool("addr_records_ownerless").await?;
    // Simulate an already installed schema, then exercise the additive upgrade.
    raw_sql(
        "ALTER TABLE address_records_current ALTER COLUMN surface_binding_id SET NOT NULL,
        ALTER COLUMN resource_id SET NOT NULL, ALTER COLUMN binding_kind SET NOT NULL",
    )
    .execute(&pool)
    .await?;
    raw_sql(include_str!(
        "../../../migrations/20260915120000_address_records_optional_authority.sql"
    ))
    .execute(&pool)
    .await?;
    seed_name(&pool, false).await?;
    seed_writes(
        &pool,
        &[write(11, 1, "60", ADDRESS_A), write(12, 1, "60", ADDRESS_B)],
    )
    .await?;
    sqlx::query("UPDATE surface_bindings SET active_to = to_timestamp(1800000011)")
        .execute(&pool)
        .await?;
    insert_event(
        &pool,
        "ownerless",
        None,
        Some(RESOURCE),
        "AuthorityTransferred",
        "ens_v1_registry_l1",
        None,
        11,
        0,
        json!({"node": NODE, "owner": REGISTRY, "owner_getter": ZERO20,
               "owner_getter_reason": "registry_self", "authority_kind": null}),
        json!({"emitting_address": REGISTRY}),
    )
    .await?;
    run(&pool, 11, 0, None, RunMode::Normal).await?;
    let name: Value =
        sqlx::query_scalar("SELECT to_jsonb(n) FROM name_current n WHERE logical_name_id = $1")
            .bind(LOGICAL_NAME)
            .fetch_one(&pool)
            .await?;
    assert_eq!(name["resource_id"], Value::Null, "{name}");
    assert_eq!(name["serving_resource_id"], RESOURCE, "{name}");
    let inventory: Value = sqlx::query_scalar(
        "SELECT entries FROM record_inventory_current WHERE resource_id = $1::uuid",
    )
    .bind(RESOURCE)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        inventory[0]["value"], ADDRESS_A,
        "forward inventory remains readable"
    );
    assert_eq!(addresses(&pool, "60").await?, vec![ADDRESS_A]);
    let row = row_for_coin(&pool, "60").await?;
    for field in ["resource_id", "surface_binding_id", "binding_kind"] {
        assert_eq!(row[field], Value::Null, "must not invent authority: {row}");
    }
    assert_eq!(row["record_resource_id"], RESOURCE);
    run(&pool, 12, 12, Some(11), RunMode::Normal).await?;
    assert_eq!(addresses(&pool, "60").await?, vec![ADDRESS_B]);
    sqlx::query("UPDATE normalized_events SET canonicality_state = 'orphaned' WHERE block_number = 12 AND event_kind = 'RecordChanged'")
        .execute(&pool).await?;
    run(&pool, 12, 12, Some(12), RunMode::Redo).await?;
    assert_eq!(addresses(&pool, "60").await?, vec![ADDRESS_A]);
    let repaired = rows(&pool).await?;
    run(&pool, 12, 0, None, RunMode::Normal).await?;
    assert_eq!(rows(&pool).await?, repaired);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn addr_records_publish_one_row_per_address_coin_and_name() -> Result<()> {
    let (database, pool) = migrated_pool("addr_records_publish").await?;
    seed_name(&pool, false).await?;
    seed_writes(
        &pool,
        &[
            write(11, 1, "60", ADDRESS_A_MIXED_CASE),
            write(11, 2, "2147483658", ADDRESS_A),
            // A non-EVM-shaped payload never keys an EVM address row.
            write(11, 3, "0", "0x00142cb8d8a8f8"),
            write(11, 4, "61", ZERO20),
        ],
    )
    .await?;
    run(&pool, 11, 0, None, RunMode::Normal).await?;

    let rows = rows(&pool).await?;
    assert_eq!(
        rows,
        json!([
            {"address": ADDRESS_A, "coin_type": "2147483658", "record_key": "addr:2147483658"},
            {"address": ADDRESS_A, "coin_type": "60", "record_key": "addr:60"},
        ]),
        "{rows}"
    );
    let row: Value = sqlx::query_scalar(
        "SELECT to_jsonb(row) FROM address_records_current row WHERE coin_type = '60'",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(row["logical_name_id"], LOGICAL_NAME);
    assert_eq!(row["namespace"], "ens");
    assert_eq!(row["raw_name"], "resolves.fixture");
    assert_eq!(row["resource_id"], RESOURCE);
    assert_eq!(row["record_resource_id"], RESOURCE);
    assert_eq!(row["surface_binding_id"], BINDING);
    assert_eq!(row["binding_kind"], "declared_registry_path");
    assert_eq!(row["support_status"], "supported");
    assert_eq!(row["provenance"]["chain_id"], CHAIN);
    assert_eq!(row["provenance"]["resolver_address"], RESOLVER);
    assert_eq!(row["provenance"]["logical_name_id"], LOGICAL_NAME);
    assert!(row["provenance"].get("ensip19_default_address").is_none());
    assert_eq!(row["chain_positions"]["block_number"], 11);
    assert_eq!(row["chain_positions"]["block_hash"], block_hash(11));
    assert_eq!(row["chain_positions"]["target_block_number"], 11);
    assert_eq!(row["chain_positions"]["target_block_hash"], block_hash(11));
    assert_eq!(row["canonicality_summary"]["state"], "canonical_lineage");
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn addr_records_follow_incremental_replacement_clear_and_reset() -> Result<()> {
    let (database, pool) = migrated_pool("addr_records_incremental").await?;
    seed_name(&pool, false).await?;
    seed_writes(
        &pool,
        &[
            write(11, 1, "60", ADDRESS_A),
            write(12, 1, "60", ADDRESS_B),
            write(13, 1, "60", ZERO20),
            write(14, 1, "60", ADDRESS_A),
        ],
    )
    .await?;

    run(&pool, 11, 0, None, RunMode::Normal).await?;
    assert_eq!(addresses(&pool, "60").await?, vec![ADDRESS_A]);
    run(&pool, 12, 12, Some(11), RunMode::Normal).await?;
    assert_eq!(addresses(&pool, "60").await?, vec![ADDRESS_B]);
    run(&pool, 13, 13, Some(12), RunMode::Normal).await?;
    assert_eq!(addresses(&pool, "60").await?, Vec::<String>::new());
    run(&pool, 14, 14, Some(13), RunMode::Normal).await?;
    assert_eq!(addresses(&pool, "60").await?, vec![ADDRESS_A]);
    let incremental = rows(&pool).await?;
    database.cleanup().await?;

    let (database, pool) = migrated_pool("addr_records_from_zero").await?;
    seed_name(&pool, false).await?;
    seed_writes(
        &pool,
        &[
            write(11, 1, "60", ADDRESS_A),
            write(12, 1, "60", ADDRESS_B),
            write(13, 1, "60", ZERO20),
            write(14, 1, "60", ADDRESS_A),
        ],
    )
    .await?;
    run(&pool, 14, 0, None, RunMode::Normal).await?;
    assert_eq!(rows(&pool).await?, incremental, "from-zero drift");
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn addr_records_retire_when_the_selected_name_is_released() -> Result<()> {
    let (database, pool) = migrated_pool("addr_records_released").await?;
    seed_name(&pool, false).await?;
    sqlx::query(
        "UPDATE normalized_events
         SET event_kind = 'RegistrationGranted', source_family = 'ens_v1_registrar_l1',
             after_state = $1
         WHERE event_identity = 'owner:10:0'",
    )
    .bind(json!({
        "source_event": "NameRegistered", "registrant": OWNER,
        "authority_kind": "registrar", "status": "registered", "expiry": 1_800_000_012
    }))
    .execute(&pool)
    .await?;
    seed_writes(&pool, &[write(11, 1, "60", ADDRESS_A)]).await?;
    sqlx::query(
        "UPDATE surface_bindings SET active_to = to_timestamp(1800000012)
         WHERE surface_binding_id = $1::uuid",
    )
    .bind(BINDING)
    .execute(&pool)
    .await?;
    insert_event(
        &pool,
        "release",
        Some(LOGICAL_NAME),
        Some(RESOURCE),
        "RegistrationReleased",
        "ens_v1_registrar_l1",
        None,
        12,
        0,
        json!({"status": "released", "released_at": 1_800_000_012}),
        json!({"emitting_address": REGISTRY}),
    )
    .await?;

    run(&pool, 11, 0, None, RunMode::Normal).await?;
    assert_eq!(addresses(&pool, "60").await?, vec![ADDRESS_A]);
    run(&pool, 12, 12, Some(11), RunMode::Normal).await?;
    let released: (String, String, String) = sqlx::query_as(
        "SELECT surface_binding_id::text, resource_id::text,
                declared_summary #>> '{control,status}'
         FROM name_current WHERE logical_name_id = $1",
    )
    .bind(LOGICAL_NAME)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        released,
        (BINDING.into(), RESOURCE.into(), "unregistered".into())
    );
    assert!(addresses(&pool, "60").await?.is_empty());

    run(&pool, 12, 0, None, RunMode::Redo).await?;
    assert!(addresses(&pool, "60").await?.is_empty());
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn addr_records_redo_drops_orphaned_record_writes() -> Result<()> {
    let (database, pool) = migrated_pool("addr_records_redo").await?;
    seed_name(&pool, false).await?;
    seed_writes(
        &pool,
        &[write(11, 1, "60", ADDRESS_A), write(12, 1, "60", ADDRESS_B)],
    )
    .await?;
    run(&pool, 12, 0, None, RunMode::Normal).await?;
    assert_eq!(addresses(&pool, "60").await?, vec![ADDRESS_B]);

    // Redo without any change republishes the same rows.
    run(&pool, 12, 12, Some(12), RunMode::Redo).await?;
    assert_eq!(addresses(&pool, "60").await?, vec![ADDRESS_B]);

    // The block-12 write is orphaned: its event stops being canonical and the redo of that block
    // must retract the row it produced and restore the block-11 answer.
    sqlx::query(
        "UPDATE normalized_events SET canonicality_state = 'orphaned'
         WHERE block_number = 12 AND event_kind = 'RecordChanged'",
    )
    .execute(&pool)
    .await?;
    run(&pool, 12, 12, Some(12), RunMode::Redo).await?;
    assert_eq!(addresses(&pool, "60").await?, vec![ADDRESS_A]);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn addr_records_default_evm_address_rows_carry_shadowing_marker() -> Result<()> {
    let (database, pool) = migrated_pool("addr_records_default").await?;
    seed_name(&pool, true).await?;
    seed_writes(
        &pool,
        &[
            write(11, 1, "2147483648", ADDRESS_A),
            write(12, 1, "60", ADDRESS_B),
            write(13, 1, "2147483658", ZERO20),
        ],
    )
    .await?;

    run(&pool, 11, 0, None, RunMode::Normal).await?;
    let default_row = row_for_coin(&pool, "2147483648").await?;
    assert_eq!(default_row["address"], ADDRESS_A);
    assert_eq!(default_row["record_key"], "addr:2147483648");
    assert_eq!(default_row["provenance"]["ensip19_default_address"], true);
    assert_eq!(default_row["provenance"]["shadowed_coin_types"], json!([]));

    // An exact addr:60 write shadows the default for coin 60 only.
    run(&pool, 12, 12, Some(11), RunMode::Normal).await?;
    let default_row = row_for_coin(&pool, "2147483648").await?;
    assert_eq!(
        default_row["provenance"]["shadowed_coin_types"],
        json!(["60"])
    );
    assert_eq!(addresses(&pool, "60").await?, vec![ADDRESS_B]);

    // A zero-address clear of another EVM coin type still shadows the default for that coin.
    run(&pool, 13, 13, Some(12), RunMode::Normal).await?;
    let default_row = row_for_coin(&pool, "2147483648").await?;
    assert_eq!(
        default_row["provenance"]["shadowed_coin_types"],
        json!(["2147483658", "60"])
    );
    assert_eq!(addresses(&pool, "2147483658").await?, Vec::<String>::new());
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn addr_records_without_default_feature_carry_no_default_marker() -> Result<()> {
    let (database, pool) = migrated_pool("addr_records_no_feature").await?;
    seed_name(&pool, false).await?;
    seed_writes(&pool, &[write(11, 1, "2147483648", ADDRESS_A)]).await?;
    run(&pool, 11, 0, None, RunMode::Normal).await?;
    let default_row = row_for_coin(&pool, "2147483648").await?;
    assert!(
        default_row["provenance"]
            .get("ensip19_default_address")
            .is_none()
    );
    assert!(
        default_row["provenance"]
            .get("shadowed_coin_types")
            .is_none()
    );
    database.cleanup().await?;
    Ok(())
}

async fn rows(pool: &PgPool) -> Result<Value> {
    Ok(sqlx::query_scalar(
        "SELECT COALESCE(jsonb_agg(jsonb_build_object(
             'address', address, 'coin_type', coin_type, 'record_key', record_key
         ) ORDER BY address, coin_type), '[]'::jsonb)
         FROM address_records_current",
    )
    .fetch_one(pool)
    .await?)
}

async fn addresses(pool: &PgPool, coin_type: &str) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar(
        "SELECT address FROM address_records_current WHERE coin_type = $1 ORDER BY address",
    )
    .bind(coin_type)
    .fetch_all(pool)
    .await?)
}

async fn row_for_coin(pool: &PgPool, coin_type: &str) -> Result<Value> {
    sqlx::query_scalar("SELECT to_jsonb(row) FROM address_records_current row WHERE coin_type = $1")
        .bind(coin_type)
        .fetch_one(pool)
        .await
        .with_context(|| format!("expected one address_records_current row for coin {coin_type}"))
}

async fn run(
    pool: &PgPool,
    target_block: i64,
    affected_from_block: i64,
    resume_current: Option<i64>,
    mode: RunMode,
) -> Result<()> {
    let (affected_from_block, affected_to_block) = match mode {
        RunMode::Redo => (affected_from_block, affected_from_block),
        RunMode::Normal => (affected_from_block, target_block),
    };
    let outcome = Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block,
            affected_from_block,
            affected_to_block,
            resume_current: resume_current.map(|number| Marker {
                number,
                hash: block_hash(number),
            }),
            mode,
        })
        .await?;
    assert!(outcome.complete);
    Ok(())
}

fn block_hash(number: i64) -> String {
    format!("0x{number:064x}")
}

async fn seed_name(pool: &PgPool, default_address_feature: bool) -> Result<()> {
    for number in 10..=14 {
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, block_number, block_timestamp, canonicality_state
             ) VALUES ($1, $2, $3, to_timestamp($4), 'canonical')",
        )
        .bind(CHAIN)
        .bind(block_hash(number))
        .bind(number)
        .bind(1_800_000_000 + number)
        .execute(pool)
        .await?;
    }
    let read_features = if default_address_feature {
        json!(["ensip19_default_address"])
    } else {
        json!([])
    };
    let payload = json!({"deployment_epoch": "fixture", "contracts": [{
        "role": "resolver", "address": RESOLVER, "proxy_kind": "none", "start_block": 0,
        "read_features": read_features
    }]});
    let manifest_id: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions (
             manifest_version, namespace, source_family, chain_id, deployment_label,
             rollout_status, normalizer_version, file_path, manifest_payload
         ) VALUES (1, 'ens', 'ens_v1_resolver_l1', $1, 'fixture', 'active', 'fixture',
                   'fixture/address-records.toml', $2)
         RETURNING manifest_id",
    )
    .bind(CHAIN)
    .bind(&payload)
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, event_kind, source_family, manifest_version,
             source_manifest_id, chain_id, derivation_kind, canonicality_state, after_state
         ) VALUES ('manifest:address-records', 'ens', 'SourceManifestUpdated',
                   'ens_v1_resolver_l1', 1, $1, $2, 'manifest_sync', 'canonical', $3)",
    )
    .bind(manifest_id)
    .bind(CHAIN)
    .bind(json!({
        "rollout_status": "active", "normalizer_version": "fixture", "manifest_payload": payload
    }))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash,
             labelhashes, normalizer_version, visibility_state, chain_id, block_hash,
             block_number, canonicality_state
         ) VALUES ($1, 'ens', 'resolves.fixture', ARRAY['resolves', 'fixture'], '\\x00', $2,
                   ARRAY[$2, $2], 'fixture', 'active', $3, $4, 10, 'canonical')",
    )
    .bind(LOGICAL_NAME)
    .bind(NODE)
    .bind(CHAIN)
    .bind(block_hash(10))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1::uuid, $2, $3, 10, 'canonical')",
    )
    .bind(RESOURCE)
    .bind(CHAIN)
    .bind(block_hash(10))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind, authority_arm,
             active_from, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1::uuid, $2, $3::uuid, 'declared_registry_path', 'ens_v1',
                   to_timestamp(1800000010), $4, $5, 10, 'canonical')",
    )
    .bind(BINDING)
    .bind(LOGICAL_NAME)
    .bind(RESOURCE)
    .bind(CHAIN)
    .bind(block_hash(10))
    .execute(pool)
    .await?;
    insert_event(
        pool,
        "owner",
        Some(LOGICAL_NAME),
        Some(RESOURCE),
        "AuthorityTransferred",
        "ens_v1_registry_l1",
        None,
        10,
        0,
        json!({"owner": OWNER, "owner_getter": OWNER, "authority_kind": "registry_only"}),
        json!({"emitting_address": REGISTRY}),
    )
    .await?;
    insert_event(
        pool,
        "pointer",
        Some(LOGICAL_NAME),
        Some(RESOURCE),
        "ResolverChanged",
        "ens_v1_registry_l1",
        None,
        10,
        1,
        json!({"node": NODE, "resolver": RESOLVER}),
        json!({"emitting_address": REGISTRY}),
    )
    .await?;
    Ok(())
}

async fn seed_writes(pool: &PgPool, writes: &[RecordWrite]) -> Result<()> {
    let manifest_id: i64 = sqlx::query_scalar("SELECT manifest_id FROM manifest_versions")
        .fetch_one(pool)
        .await?;
    for write in writes {
        insert_event(
            pool,
            &format!("record-{}", write.coin),
            None,
            None,
            "RecordChanged",
            "ens_v1_resolver_l1",
            Some(manifest_id),
            write.block,
            write.log_index,
            json!({
                "node": NODE, "resolver": RESOLVER,
                "record_key": format!("addr:{}", write.coin), "record_family": "addr",
                "selector_key": write.coin, "source_event": "AddressChanged",
                "value": write.value
            }),
            json!({"emitting_address": RESOLVER}),
        )
        .await?;
        if write.coin == "60" {
            insert_event(
                pool,
                "record-60-compat",
                None,
                None,
                "RecordChanged",
                "ens_v1_resolver_l1",
                Some(manifest_id),
                write.block,
                write.log_index + 1,
                json!({
                    "node": NODE, "resolver": RESOLVER, "record_key": "addr:60",
                    "record_family": "addr", "selector_key": "60",
                    "source_event": "AddrChanged", "value": write.value
                }),
                json!({"emitting_address": RESOLVER}),
            )
            .await?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn insert_event(
    pool: &PgPool,
    identity: &str,
    logical_name_id: Option<&str>,
    resource_id: Option<&str>,
    event_kind: &str,
    source_family: &str,
    manifest_id: Option<i64>,
    block: i64,
    log_index: i64,
    after_state: Value,
    raw_fact_ref: Value,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, resource_id, event_kind, source_family,
             manifest_version, source_manifest_id, chain_id, block_number, block_hash,
             transaction_hash, transaction_index, log_index, derivation_kind,
             canonicality_state, after_state, raw_fact_ref
         ) VALUES ($1, 'ens', $2, $3::uuid, $4, $5, 1, $6, $7, $8, $9, $10, 0, $11,
                   'ens_v1_unwrapped_authority', 'canonical', $12, $13)",
    )
    .bind(format!("{identity}:{block}:{log_index}"))
    .bind(logical_name_id)
    .bind(resource_id)
    .bind(event_kind)
    .bind(source_family)
    .bind(manifest_id)
    .bind(CHAIN)
    .bind(block)
    .bind(block_hash(block))
    .bind(format!("0x{:064x}", block * 100))
    .bind(log_index)
    .bind(after_state)
    .bind(raw_fact_ref)
    .execute(pool)
    .await?;
    Ok(())
}

async fn migrated_pool(name: &str) -> Result<(TestDatabase, PgPool)> {
    let database = TestDatabase::create(TestDatabaseConfig::new(name)).await?;
    let pool = database.pool().clone();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    let mut transaction = pool.begin().await?;
    sqlx::query("CREATE SCHEMA bigname_phase")
        .execute(&mut *transaction)
        .await?;
    raw_sql(&format!(
        "ALTER DATABASE \"{}\" SET search_path TO bigname_phase, public",
        database_name.replace('"', "\"\"")
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
