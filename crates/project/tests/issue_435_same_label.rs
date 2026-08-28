use anyhow::Result;
use bigname_project::{BatchRequest, Engine, Marker, RunMode};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgPool, raw_sql};

const CHAIN: &str = "issue-435";
const PARENT_A: &str = "ens:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const CHILD_A: &str = "ens:0xaaa1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const PARENT_B: &str = "ens:0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const CHILD_B: &str = "ens:0xbbb1bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const LABEL: &str = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const V2_INSTANCE: &str = "00000000-0000-0000-0000-000000000435";
const V2_REGISTRY: &str = "0x0000000000000000000000000000000000000435";

fn hash(number: i64) -> String {
    format!("0x{number:064x}")
}

async fn database(name: &str) -> Result<(TestDatabase, PgPool)> {
    let database = TestDatabase::create(TestDatabaseConfig::new(name)).await?;
    let pool = database.pool().clone();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    let mut transaction = pool.begin().await?;
    raw_sql(&format!(
        "CREATE SCHEMA bigname_phase;
         ALTER DATABASE \"{}\" SET search_path TO bigname_phase, public;
         SET LOCAL search_path TO bigname_phase, public",
        database_name.replace('"', "\"\"")
    ))
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

async fn seed(pool: &PgPool) -> Result<()> {
    for number in [8_i64, 9] {
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, block_number, block_timestamp, canonicality_state
             ) VALUES ($1, $2, $3, to_timestamp($3), 'canonical')",
        )
        .bind(CHAIN)
        .bind(hash(number))
        .bind(number)
        .execute(pool)
        .await?;
    }
    for (id, raw_name, labels) in [
        (PARENT_A, "a.eth", vec!["a", "eth"]),
        (CHILD_A, "www.a.eth", vec!["www", "a", "eth"]),
        (PARENT_B, "b.eth", vec!["b", "eth"]),
        (CHILD_B, "www.b.eth", vec!["www", "b", "eth"]),
    ] {
        let namehash = id.strip_prefix("ens:").expect("fixture ID");
        let labelhashes = vec![LABEL; labels.len()];
        sqlx::query(
            "INSERT INTO name_surfaces (
                 logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
                 namehash, labelhashes, normalizer_version, visibility_state,
                 chain_id, block_hash, block_number, canonicality_state
             ) VALUES ($1, 'ens', $2, $3, decode('00', 'hex'), $4, $5,
                       'fixture', 'active', $6, $7, 8, 'canonical')",
        )
        .bind(id)
        .bind(raw_name)
        .bind(labels)
        .bind(namehash)
        .bind(labelhashes)
        .bind(CHAIN)
        .bind(hash(8))
        .execute(pool)
        .await?;
    }
    insert_edge(pool, "edge-a-8", 8, PARENT_A, CHILD_A).await?;
    insert_edge(pool, "edge-b-8", 8, PARENT_B, CHILD_B).await
}

async fn insert_edge(
    pool: &PgPool,
    identity: &str,
    block: i64,
    parent: &str,
    child: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, event_kind, source_family,
             manifest_version, chain_id, block_number, block_hash, derivation_kind,
             canonicality_state, after_state
         ) VALUES ($1, 'ens', $2, 'SubregistryChanged', 'ens_v1_registry_l1',
                   1, $3, $4, $5, 'ens_v1_unwrapped_authority', 'canonical', $6)",
    )
    .bind(identity)
    .bind(child)
    .bind(CHAIN)
    .bind(block)
    .bind(hash(block))
    .bind(json!({
        "node": parent.strip_prefix("ens:").expect("fixture parent"),
        "child_node": child.strip_prefix("ens:").expect("fixture child"),
        "labelhash": LABEL,
        "owner": "0x0000000000000000000000000000000000000435"
    }))
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_v2_parent(pool: &PgPool) -> Result<()> {
    raw_sql(&format!(
        "INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         VALUES ('{CHAIN}', '{}', 8, to_timestamp(8), 'canonical'),
                ('{CHAIN}', '{}', 9, to_timestamp(9), 'canonical');
         INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
         VALUES ('{V2_INSTANCE}', '{CHAIN}', 'contract');
         INSERT INTO contract_instance_addresses
             (contract_instance_id, chain_id, address, active_from_block_number)
         VALUES ('{V2_INSTANCE}', '{CHAIN}', '{V2_REGISTRY}', 0);
         INSERT INTO name_surfaces
             (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
              namehash, labelhashes, normalizer_version, visibility_state,
              chain_id, block_hash, block_number, canonicality_state)
         VALUES ('{PARENT_A}', 'ens', 'a.eth', ARRAY['a','eth'], decode('00','hex'),
                 '{}', ARRAY['0xa','0xeth'], 'fixture', 'active',
                 '{CHAIN}', '{}', 8, 'canonical');
         INSERT INTO normalized_events
             (event_identity, namespace, logical_name_id, event_kind, source_family,
              manifest_version, chain_id, block_number, block_hash, derivation_kind,
              canonicality_state, after_state)
         VALUES ('v2-parent-8', 'ens', '{PARENT_A}', 'SubregistryChanged',
                 'ens_v2_registry_l1', 1, '{CHAIN}', 8, '{}',
                 'ens_v1_unwrapped_authority', 'canonical',
                 jsonb_build_object('subregistry', '{V2_REGISTRY}'))",
        hash(8),
        hash(9),
        PARENT_A.strip_prefix("ens:").expect("fixture parent"),
        hash(8),
        hash(8),
    ))
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_v2_child(pool: &PgPool) -> Result<()> {
    raw_sql(&format!(
        "INSERT INTO name_surfaces
             (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
              namehash, labelhashes, normalizer_version, visibility_state,
              chain_id, block_hash, block_number, canonicality_state)
         VALUES ('{CHILD_A}', 'ens', 'www.a.eth', ARRAY['www','a','eth'], decode('00','hex'),
                 '{}', ARRAY['{LABEL}','0xa','0xeth'], 'fixture', 'active',
                 '{CHAIN}', '{}', 9, 'canonical');
         INSERT INTO normalized_events
             (event_identity, namespace, logical_name_id, event_kind, source_family,
              manifest_version, chain_id, block_number, block_hash, derivation_kind,
              canonicality_state, after_state)
         VALUES ('v2-child-9', 'ens', '{CHILD_A}', 'RegistrationGranted',
                 'ens_v2_registry_l1', 1, '{CHAIN}', 9, '{}',
                 'ens_v1_unwrapped_authority', 'canonical', jsonb_build_object(
                     'registry_contract_instance_id', '{V2_INSTANCE}',
                     'registrant', '0x0000000000000000000000000000000000000435'))",
        CHILD_A.strip_prefix("ens:").expect("fixture child"),
        hash(9),
        hash(9),
    ))
    .execute(pool)
    .await?;
    Ok(())
}

async fn run(pool: &PgPool, target: i64, previous: Option<i64>) -> Result<()> {
    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block: target,
            affected_from_block: previous.map_or(0, |number| number + 1),
            affected_to_block: target,
            resume_current: previous.map(|number| Marker {
                number,
                hash: hash(number),
            }),
            mode: RunMode::Normal,
        })
        .await?;
    Ok(())
}

#[rustfmt::skip]
async fn normalize(pool: &PgPool) -> Result<()> {
    for table in ["name_current", "children_current", "permissions_current", "record_inventory_current", "resolver_current", "address_names_current"] {
        sqlx::query(&format!("UPDATE {table} SET last_recomputed_at = to_timestamp(0), inserted_at = to_timestamp(0)")).execute(pool).await?;
    }
    sqlx::query("UPDATE permissions_current_resource_summary SET last_recomputed_at = to_timestamp(0)").execute(pool).await?;
    Ok(())
}

async fn snapshot(pool: &PgPool) -> Result<Vec<(String, Value)>> {
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
    let mut result = Vec::with_capacity(tables.len());
    for (table, order) in tables {
        let sql = format!(
            "SELECT COALESCE(jsonb_agg(
                 (to_jsonb(row) - 'chain_positions'
                  #- '{{canonicality_summary,target_block_number}}'
                  #- '{{canonicality_summary,target_block_hash}}'
                  #- '{{claim_provenance,target_block_number}}'
                  #- '{{claim_provenance,target_block_hash}}')
                 ORDER BY {order}), '[]'::jsonb)
             FROM {table} row"
        );
        result.push((
            table.to_owned(),
            sqlx::query_scalar(&sql).fetch_one(pool).await?,
        ));
    }
    Ok(result)
}

#[tokio::test]
async fn same_label_under_unrelated_parents_does_not_widen_scope_and_converges() -> Result<()> {
    let (incremental_database, incremental) = database("issue_435_same_label_incremental").await?;
    let (full_database, full) = database("issue_435_same_label_full").await?;
    for pool in [&incremental, &full] {
        seed(pool).await?;
    }

    run(&incremental, 8, None).await?;
    sqlx::query(
        "UPDATE children_current SET last_recomputed_at = to_timestamp(0), inserted_at = to_timestamp(0)
         WHERE parent_logical_name_id = $1 AND child_logical_name_id = $2",
    )
    .bind(PARENT_B)
    .bind(CHILD_B)
    .execute(&incremental)
    .await?;
    insert_edge(&incremental, "edge-a-9", 9, PARENT_A, CHILD_A).await?;
    insert_edge(&full, "edge-a-9", 9, PARENT_A, CHILD_A).await?;
    run(&incremental, 9, Some(8)).await?;
    run(&full, 9, None).await?;

    let unrelated_untouched: bool = sqlx::query_scalar(
        "SELECT last_recomputed_at = to_timestamp(0) AND inserted_at = to_timestamp(0)
         FROM children_current
         WHERE parent_logical_name_id = $1 AND child_logical_name_id = $2",
    )
    .bind(PARENT_B)
    .bind(CHILD_B)
    .fetch_one(&incremental)
    .await?;
    assert!(
        unrelated_untouched,
        "the unrelated same-label subtree was rebuilt"
    );

    normalize(&incremental).await?;
    normalize(&full).await?;
    assert_eq!(snapshot(&incremental).await?, snapshot(&full).await?);

    incremental_database.cleanup().await?;
    full_database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn first_v2_child_grant_reaches_its_parent_and_converges() -> Result<()> {
    let (incremental_database, incremental) = database("issue_435_v2_grant_incremental").await?;
    let (full_database, full) = database("issue_435_v2_grant_full").await?;
    for pool in [&incremental, &full] {
        seed_v2_parent(pool).await?;
    }

    run(&incremental, 8, None).await?;
    for pool in [&incremental, &full] {
        insert_v2_child(pool).await?;
    }
    run(&incremental, 9, Some(8)).await?;
    run(&full, 9, None).await?;

    let incremental_edges: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM children_current
         WHERE parent_logical_name_id = $1 AND child_logical_name_id = $2",
    )
    .bind(PARENT_A)
    .bind(CHILD_A)
    .fetch_one(&incremental)
    .await?;
    let full_edges: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM children_current
         WHERE parent_logical_name_id = $1 AND child_logical_name_id = $2",
    )
    .bind(PARENT_A)
    .bind(CHILD_A)
    .fetch_one(&full)
    .await?;
    assert_eq!(
        incremental_edges, full_edges,
        "a first ENSv2 child grant must stage the pre-existing parent topology event"
    );

    normalize(&incremental).await?;
    normalize(&full).await?;
    assert_eq!(snapshot(&incremental).await?, snapshot(&full).await?);

    incremental_database.cleanup().await?;
    full_database.cleanup().await?;
    Ok(())
}
