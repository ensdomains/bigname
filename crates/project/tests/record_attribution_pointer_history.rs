//! Node-keyed record writes are attributed to a registration through its resolver pointer and
//! published in `record_inventory_current.provenance.attributed_event_ids`; registration-scoped
//! name history reads them back from there. A later resolver switch or clear changes which
//! records the name serves, but it must not erase the fact that the earlier write happened:
//! history attribution has to be retained independently of the current pointer selection.
use anyhow::{Context, Result};
use bigname_project::{BatchRequest, Engine, RunMode};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgPool, raw_sql};

const CHAIN: &str = "ethereum-mainnet";
const NAMESPACE: &str = "ens";
const NODE: &str = "0x2222222222222222222222222222222222222222222222222222222222222222";
const RESOURCE: &str = "66000000-0000-0000-0000-000000000200";
const BINDING: &str = "66000000-0000-0000-0000-000000000201";
const RESOLVER_A: &str = "0x00000000000000000000000000000000000000a1";
const RESOLVER_B: &str = "0x00000000000000000000000000000000000000b2";
const ZERO: &str = "0x0000000000000000000000000000000000000000";

#[derive(Clone)]
struct Block {
    hash: String,
    number: i64,
    timestamp: i64,
}

fn block(number: i64) -> Block {
    Block {
        hash: format!("0x{number:064x}"),
        number,
        timestamp: 1_700_000_000 + number,
    }
}

/// Positive control for the seed: with the pointer left on A the write is attributed.
#[tokio::test]
async fn node_keyed_write_is_attributed_while_the_pointer_is_unchanged() -> Result<()> {
    let (write, attributed) = project_write_then_pointer_change("unchanged", RESOLVER_A).await?;
    assert_eq!(attributed, vec![write]);
    Ok(())
}

#[tokio::test]
async fn node_keyed_write_stays_attributed_after_resolver_switch() -> Result<()> {
    let (write, attributed) = project_write_then_pointer_change("switch", RESOLVER_B).await?;
    assert!(
        attributed.contains(&write),
        "the write to the previous resolver must stay attributed after the pointer moves: \
         write={write} attributed={attributed:?}"
    );
    Ok(())
}

#[tokio::test]
async fn node_keyed_write_stays_attributed_after_resolver_clear() -> Result<()> {
    let (write, attributed) = project_write_then_pointer_change("clear", ZERO).await?;
    assert!(
        attributed.contains(&write),
        "the write to the cleared resolver must stay attributed after the pointer is cleared: \
         write={write} attributed={attributed:?}"
    );
    Ok(())
}

/// Seeds: block 1000 sets the resolver pointer to A and writes a text record on A (node-keyed,
/// no logical name or resource of its own); block 1001 moves the pointer to `later_resolver`.
/// Returns the write's normalized event id and the attributed ids Project published for the
/// registration after the whole range.
async fn project_write_then_pointer_change(
    case: &str,
    later_resolver: &str,
) -> Result<(i64, Vec<i64>)> {
    let (database, pool) = database(case).await?;
    let blocks = [block(1000), block(1001)];
    seed(&pool, &blocks).await?;
    let logical_name_id = format!("{NAMESPACE}:{NODE}");
    insert_event(
        &pool,
        "pointer-a",
        Some(&logical_name_id),
        Some(RESOURCE),
        "ResolverChanged",
        "ens_v1_registry_l1",
        &blocks[0],
        0,
        json!({"node": NODE, "resolver": RESOLVER_A}),
        json!({"emitting_address": "0x0000000000000000000000000000000000000660"}),
    )
    .await?;
    insert_event(
        &pool,
        "write-a",
        None,
        None,
        "RecordChanged",
        "ens_v1_resolver_l1",
        &blocks[0],
        1,
        json!({
            "source_event": "TextChanged",
            "resolver": RESOLVER_A,
            "node": NODE,
            "record_key": "text:url",
            "record_family": "text",
            "selector_key": "url",
            "value_retained": true,
            "value": "https://written-on-resolver-a.invalid",
        }),
        json!({"emitting_address": RESOLVER_A}),
    )
    .await?;
    insert_event(
        &pool,
        "pointer-later",
        Some(&logical_name_id),
        Some(RESOURCE),
        "ResolverChanged",
        "ens_v1_registry_l1",
        &blocks[1],
        0,
        json!({"node": NODE, "resolver": later_resolver}),
        json!({"emitting_address": "0x0000000000000000000000000000000000000660"}),
    )
    .await?;
    let write: i64 = sqlx::query_scalar(
        "SELECT normalized_event_id FROM normalized_events WHERE event_identity = $1",
    )
    .bind(format!("write-a:{}", blocks[0].number))
    .fetch_one(&pool)
    .await?;

    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block: blocks[1].number,
            affected_from_block: blocks[1].number,
            affected_to_block: blocks[1].number,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;

    let provenance: Option<Value> = sqlx::query_scalar(
        "SELECT provenance FROM record_inventory_current WHERE resource_id = $1::uuid",
    )
    .bind(RESOURCE)
    .fetch_optional(&pool)
    .await?;
    let attributed = provenance
        .as_ref()
        .and_then(|provenance| provenance.get("attributed_event_ids"))
        .and_then(Value::as_array)
        .map(|ids| ids.iter().filter_map(Value::as_i64).collect::<Vec<_>>())
        .unwrap_or_default();
    database.cleanup().await?;
    Ok((write, attributed))
}

async fn seed(pool: &PgPool, blocks: &[Block]) -> Result<()> {
    for block in blocks {
        sqlx::query("INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state) VALUES ($1,$2,$3,to_timestamp($4),'canonical')")
            .bind(CHAIN).bind(&block.hash).bind(block.number).bind(block.timestamp).execute(pool).await?;
    }
    let logical_name_id = format!("{NAMESPACE}:{NODE}");
    let first = blocks.first().context("first block")?;
    sqlx::query("INSERT INTO name_surfaces (logical_name_id,namespace,raw_name,raw_labels,dns_encoded_name,namehash,labelhashes,normalizer_version,visibility_state,chain_id,block_hash,block_number,canonicality_state) VALUES ($1,$2,$3,ARRAY[$3],decode('00','hex'),$4,ARRAY[$4],'fixture','active',$5,$6,$7,'canonical')")
        .bind(&logical_name_id).bind(NAMESPACE).bind("attribution-history.fixture").bind(NODE).bind(CHAIN).bind(&first.hash).bind(first.number).execute(pool).await?;
    sqlx::query("INSERT INTO resources (resource_id,chain_id,block_hash,block_number,canonicality_state) VALUES ($1::uuid,$2,$3,$4,'canonical')")
        .bind(RESOURCE).bind(CHAIN).bind(&first.hash).bind(first.number).execute(pool).await?;
    sqlx::query("INSERT INTO surface_bindings (surface_binding_id,logical_name_id,resource_id,binding_kind,authority_arm,active_from,chain_id,block_hash,block_number,canonicality_state) VALUES ($1::uuid,$2,$3::uuid,'declared_registry_path','ens_v1',to_timestamp($4),$5,$6,$7,'canonical')")
        .bind(BINDING).bind(&logical_name_id).bind(RESOURCE).bind(first.timestamp).bind(CHAIN).bind(&first.hash).bind(first.number).execute(pool).await?;
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
    block: &Block,
    log_index: i64,
    after_state: Value,
    raw_fact_ref: Value,
) -> Result<()> {
    sqlx::query("INSERT INTO normalized_events (event_identity,namespace,logical_name_id,resource_id,event_kind,source_family,manifest_version,chain_id,block_number,block_hash,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state,raw_fact_ref) VALUES ($1,$2,$3,$4::uuid,$5,$6,1,$7,$8,$9,$10,0,$11,'ens_v1_unwrapped_authority','canonical',$12,$13)")
        .bind(format!("{identity}:{}", block.number)).bind(NAMESPACE).bind(logical_name_id).bind(resource_id).bind(event_kind).bind(source_family).bind(CHAIN).bind(block.number).bind(&block.hash).bind(format!("0x{:064x}", block.number * 100 + log_index)).bind(log_index).bind(after_state).bind(raw_fact_ref).execute(pool).await?;
    Ok(())
}

async fn database(name: &str) -> Result<(TestDatabase, PgPool)> {
    let database = TestDatabase::create(TestDatabaseConfig::new(format!(
        "record_attribution_{name}"
    )))
    .await?;
    let pool = database.pool().clone();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    let mut transaction = pool.begin().await?;
    raw_sql(&format!("CREATE SCHEMA bigname_phase; ALTER DATABASE \"{}\" SET search_path TO bigname_phase, public; SET LOCAL search_path TO bigname_phase, public", database_name.replace('"', "\"\""))).execute(&mut *transaction).await?;
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
