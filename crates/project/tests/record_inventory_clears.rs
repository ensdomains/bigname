use anyhow::{Context, Result};
use bigname_domain::resolver_read::{IndexedRecordStatus, evaluate_indexed_record};
use bigname_project::{BatchRequest, Engine, Marker, RunMode};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgPool, raw_sql};

const FIXTURE: &str =
    include_str!("../../adapters/tests/fixtures/interpreters/v1-record-clears.json");
const RESOURCE: &str = "66000000-0000-0000-0000-000000000100";
const BINDING: &str = "66000000-0000-0000-0000-000000000101";

#[derive(Clone)]
struct Block {
    hash: String,
    number: i64,
    timestamp: i64,
}

#[tokio::test]
async fn contenthash_clear_after_set_projects_not_found() -> Result<()> {
    assert_terminal("ens_v1_contenthash_clear_after_set").await
}

#[tokio::test]
async fn multicoin_address_clear_after_set_projects_not_found() -> Result<()> {
    assert_terminal("basenames_multicoin_address_clear_after_set").await
}

#[tokio::test]
async fn contenthash_set_clear_set_projects_latest_value() -> Result<()> {
    assert_terminal("ens_v1_contenthash_set_clear_set").await
}

#[tokio::test]
async fn multicoin_address_set_clear_set_projects_latest_value() -> Result<()> {
    assert_terminal("basenames_multicoin_address_set_clear_set").await
}

#[tokio::test]
async fn record_clear_sequences_are_replay_safe() -> Result<()> {
    for fixture in cases()? {
        let all_at_once = project_case(&fixture, Execution::AllAtOnce).await?;
        let incremental = project_case(&fixture, Execution::Incremental).await?;
        let repeated = project_case(&fixture, Execution::RepeatedPredecessor).await?;
        let redo = project_case(&fixture, Execution::Redo).await?;
        let case_id = fixture["case"]["id"].as_str().context("case id")?;
        assert_eq!(incremental, all_at_once, "{case_id}: incremental drift");
        assert_eq!(
            repeated, all_at_once,
            "{case_id}: repeated predecessor drift"
        );
        assert_eq!(redo, all_at_once, "{case_id}: redo drift");
    }
    Ok(())
}

async fn assert_terminal(case_id: &str) -> Result<()> {
    let fixture = cases()?
        .into_iter()
        .find(|fixture| fixture["case"]["id"] == case_id)
        .with_context(|| format!("missing record-clear fixture {case_id}"))?;
    let row = project_case(&fixture, Execution::AllAtOnce).await?;
    let expected = &fixture["expected_terminal_project"];
    let entry = row["entries"]
        .as_array()
        .and_then(|entries| entries.first())
        .context("projected record entry")?;
    assert_eq!(entry, &expected["entry"]);
    assert_eq!(
        row["last_change"]["chain_position"]["block_number"],
        expected["last_change"]["block_number"]
    );
    assert_eq!(
        row["last_change"]["chain_position"]["block_hash"],
        expected["last_change"]["block_hash"]
    );

    let family = entry["record_family"].as_str().context("record family")?;
    let selector = entry["selector_key"].as_str();
    let answer = evaluate_indexed_record(
        &row["entries"],
        &row["provenance"],
        &json!({"status":"projected"}),
        entry["record_key"].as_str().context("record key")?,
        family,
        selector,
    );
    assert_eq!(answer.status.as_str(), expected["answer"]["status"]);
    assert_eq!(
        answer.value,
        expected["answer"]["value"]
            .as_str()
            .map(|value| json!(value))
    );
    if answer.status == IndexedRecordStatus::NotFound {
        assert!(entry.get("value").is_none());
    } else {
        assert_ne!(
            &entry["value"],
            expected.get("first_value").unwrap_or(&Value::Null)
        );
    }
    Ok(())
}

fn cases() -> Result<Vec<Value>> {
    serde_json::from_str::<Value>(FIXTURE)?["cases"]
        .as_array()
        .cloned()
        .context("record-clear fixture cases")
}

#[derive(Clone, Copy)]
enum Execution {
    AllAtOnce,
    Incremental,
    RepeatedPredecessor,
    Redo,
}

async fn project_case(fixture: &Value, execution: Execution) -> Result<Value> {
    let case_id = fixture["case"]["id"].as_str().context("case id")?;
    let (database, pool) = database(case_id).await?;
    seed(&pool, fixture).await?;
    let blocks = blocks(fixture)?;
    match execution {
        Execution::AllAtOnce => {
            run(
                &pool,
                blocks.last().context("fixture block")?,
                None,
                RunMode::Normal,
            )
            .await?
        }
        Execution::Incremental | Execution::RepeatedPredecessor | Execution::Redo => {
            run(&pool, &blocks[0], None, RunMode::Normal).await?;
            for window in blocks.windows(2) {
                let mode = if matches!(execution, Execution::Redo) {
                    RunMode::Redo
                } else {
                    RunMode::Normal
                };
                run(&pool, &window[1], Some(&window[0]), mode).await?;
                if matches!(execution, Execution::RepeatedPredecessor) {
                    run(&pool, &window[1], Some(&window[0]), RunMode::Normal).await?;
                }
            }
        }
    }
    let raw_facts: i64 = sqlx::query_scalar("SELECT count(*) FROM raw_logs")
        .fetch_one(&pool)
        .await?;
    assert_eq!(raw_facts, 0, "Project must not mutate raw facts");
    let row = sqlx::query_scalar(
        "SELECT jsonb_build_object(
             'record_version_boundary', record_version_boundary,
             'selectors', selectors,
             'unsupported_families', unsupported_families,
             'last_change', last_change,
             'entries', entries,
             'provenance', provenance,
             'coverage', jsonb_build_object('support_status', support_status,
                                             'unsupported_reason', unsupported_reason),
             'chain_positions', chain_positions,
             'canonicality_summary', canonicality_summary,
             'manifest_version', manifest_version)
         FROM record_inventory_current WHERE resource_id = $1::uuid",
    )
    .bind(RESOURCE)
    .fetch_one(&pool)
    .await?;
    database.cleanup().await?;
    Ok(row)
}

async fn run(
    pool: &PgPool,
    target: &Block,
    predecessor: Option<&Block>,
    mode: RunMode,
) -> Result<()> {
    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: chain(pool).await?,
            target_block: target.number,
            affected_from_block: target.number,
            affected_to_block: target.number,
            resume_current: predecessor.map(|block| Marker {
                number: block.number,
                hash: block.hash.clone(),
            }),
            mode,
        })
        .await?;
    Ok(())
}

async fn chain(pool: &PgPool) -> Result<String> {
    Ok(
        sqlx::query_scalar("SELECT chain_id FROM chain_lineage LIMIT 1")
            .fetch_one(pool)
            .await?,
    )
}

async fn seed(pool: &PgPool, fixture: &Value) -> Result<()> {
    let manifest = fixture["case"]["manifests"]
        .as_array()
        .and_then(|items| items.first())
        .context("fixture manifest")?;
    let namespace = manifest["namespace"]
        .as_str()
        .context("manifest namespace")?;
    let source_family = manifest["source_family"]
        .as_str()
        .context("manifest family")?;
    let chain = manifest["chain"].as_str().context("manifest chain")?;
    let deployment_epoch = manifest["deployment_epoch"]
        .as_str()
        .context("manifest epoch")?;
    let role = manifest["role"].as_str().context("manifest role")?;
    let fixture_blocks = blocks(fixture)?;
    for block in &fixture_blocks {
        sqlx::query("INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state) VALUES ($1,$2,$3,to_timestamp($4),'canonical')")
            .bind(chain).bind(&block.hash).bind(block.number).bind(block.timestamp).execute(pool).await?;
    }
    let events = fixture["expected_normalized_events"]
        .as_array()
        .context("fixture events")?;
    let first = events.first().context("fixture event")?;
    let node = first["after_state"]["node"]
        .as_str()
        .context("fixture node")?;
    let resolver = first["after_state"]["resolver"]
        .as_str()
        .context("fixture resolver")?;
    let logical_name_id = format!("{namespace}:{node}");
    let payload = json!({"deployment_epoch":deployment_epoch,"contracts":[{"role":role,"address":resolver,"proxy_kind":"none","start_block":0}]});
    let case_id = fixture["case"]["id"].as_str().context("case id")?;
    let manifest_id: i64 = sqlx::query_scalar("INSERT INTO manifest_versions (manifest_version,namespace,source_family,chain_id,deployment_label,rollout_status,normalizer_version,file_path,manifest_payload) VALUES (1,$1,$2,$3,$4,'active','fixture',$5,$6) RETURNING manifest_id")
        .bind(namespace).bind(source_family).bind(chain).bind(deployment_epoch).bind(format!("fixture/{case_id}.toml")).bind(&payload).fetch_one(pool).await?;
    sqlx::query("INSERT INTO normalized_events (event_identity,namespace,event_kind,source_family,manifest_version,source_manifest_id,chain_id,derivation_kind,canonicality_state,after_state) VALUES ($1,$2,'SourceManifestUpdated',$3,1,$4,$5,'manifest_sync','canonical',$6)")
        .bind(format!("manifest:{case_id}")).bind(namespace).bind(source_family).bind(manifest_id).bind(chain).bind(json!({"rollout_status":"active","normalizer_version":"fixture","manifest_payload":payload})).execute(pool).await?;
    let first_block = fixture_blocks.first().context("fixture block")?;
    sqlx::query("INSERT INTO name_surfaces (logical_name_id,namespace,raw_name,raw_labels,dns_encoded_name,namehash,labelhashes,normalizer_version,visibility_state,chain_id,block_hash,block_number,canonicality_state) VALUES ($1,$2,$3,ARRAY[$3],decode('00','hex'),$4,ARRAY[$4],'fixture','active',$5,$6,$7,'canonical')")
        .bind(&logical_name_id).bind(namespace).bind(format!("{case_id}.fixture")).bind(node).bind(chain).bind(&first_block.hash).bind(first_block.number).execute(pool).await?;
    sqlx::query("INSERT INTO resources (resource_id,chain_id,block_hash,block_number,canonicality_state) VALUES ($1::uuid,$2,$3,$4,'canonical')")
        .bind(RESOURCE).bind(chain).bind(&first_block.hash).bind(first_block.number).execute(pool).await?;
    sqlx::query("INSERT INTO surface_bindings (surface_binding_id,logical_name_id,resource_id,binding_kind,authority_arm,active_from,chain_id,block_hash,block_number,canonicality_state) VALUES ($1::uuid,$2,$3::uuid,'declared_registry_path',$4,to_timestamp($5),$6,$7,$8,'canonical')")
        .bind(BINDING).bind(&logical_name_id).bind(RESOURCE).bind(if namespace == "basenames" { "basenames" } else { "ens_v1" }).bind(first_block.timestamp).bind(chain).bind(&first_block.hash).bind(first_block.number).execute(pool).await?;
    let pointer_family = if namespace == "basenames" {
        "basenames_base_registry"
    } else {
        "ens_v1_registry_l1"
    };
    insert_event(
        pool,
        "pointer",
        namespace,
        &logical_name_id,
        Some(RESOURCE),
        "ResolverChanged",
        pointer_family,
        None,
        chain,
        first_block,
        0,
        json!({"node":node,"resolver":resolver}),
        json!({"emitting_address":"0x0000000000000000000000000000000000000660"}),
    )
    .await?;
    for (index, event) in events.iter().enumerate() {
        let block_number = event["block_number"].as_i64().context("event block")?;
        let block = fixture_blocks
            .iter()
            .find(|block| block.number == block_number)
            .context("event lineage")?;
        insert_event(
            pool,
            &format!("record-{index}"),
            namespace,
            &logical_name_id,
            None,
            "RecordChanged",
            event["source_family"].as_str().context("event family")?,
            Some(manifest_id),
            chain,
            block,
            i64::try_from(index + 1)?,
            event["after_state"].clone(),
            json!({"emitting_address":resolver}),
        )
        .await?;
    }
    assert!(
        manifest["address"]
            .as_str()
            .is_some_and(|address| address.eq_ignore_ascii_case(resolver))
    );
    Ok(())
}

fn blocks(fixture: &Value) -> Result<Vec<Block>> {
    fixture["case"]["blocks"]
        .as_array()
        .context("fixture blocks")?
        .iter()
        .map(|block| {
            Ok(Block {
                hash: block["hash"].as_str().context("block hash")?.to_owned(),
                number: block["number"].as_i64().context("block number")?,
                timestamp: block["timestamp"].as_i64().context("block timestamp")?,
            })
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
async fn insert_event(
    pool: &PgPool,
    identity: &str,
    namespace: &str,
    logical_name_id: &str,
    resource_id: Option<&str>,
    event_kind: &str,
    source_family: &str,
    manifest_id: Option<i64>,
    chain: &str,
    block: &Block,
    log_index: i64,
    after_state: Value,
    raw_fact_ref: Value,
) -> Result<()> {
    sqlx::query("INSERT INTO normalized_events (event_identity,namespace,logical_name_id,resource_id,event_kind,source_family,manifest_version,source_manifest_id,chain_id,block_number,block_hash,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state,raw_fact_ref) VALUES ($1,$2,$3,$4::uuid,$5,$6,1,$7,$8,$9,$10,$11,0,$12,'ens_v1_unwrapped_authority','canonical',$13,$14)")
        .bind(format!("{identity}:{}", block.number)).bind(namespace).bind(logical_name_id).bind(resource_id).bind(event_kind).bind(source_family).bind(manifest_id).bind(chain).bind(block.number).bind(&block.hash).bind(format!("0x{:064x}", block.number * 100 + log_index)).bind(log_index).bind(after_state).bind(raw_fact_ref).execute(pool).await?;
    Ok(())
}

async fn database(name: &str) -> Result<(TestDatabase, PgPool)> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new(format!("record_clears_{name}"))).await?;
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
