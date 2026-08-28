use std::{env, fs, path::PathBuf, process::Command, time::Instant};

use anyhow::{Context, Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, Transaction, raw_sql};

const BASE_SHA: &str = "eabbd4c4485e7983251a33492c1d5952ea4fe4cd";
const PROJECT_SOURCE: &str = include_str!("../../src/scope/topology.rs");
const CORPUS: &str = include_str!("corpus.sql");
const EXPLAIN: &str = include_str!("explain.sql");
#[rustfmt::skip]
const BASELINE: &[&str] = &[
    include_str!("../../../../schema-v2/baseline/01_chain.sql"), include_str!("../../../../schema-v2/baseline/02_raw_facts.sql"), include_str!("../../../../schema-v2/baseline/03_identity.sql"), include_str!("../../../../schema-v2/baseline/04_manifests.sql"), include_str!("../../../../schema-v2/baseline/05_normalized_events.sql"), include_str!("../../../../schema-v2/baseline/06_projections.sql"), include_str!("../../../../schema-v2/baseline/07_labels.sql"), include_str!("../../../../schema-v2/baseline/08_heartbeats.sql"), include_str!("../../../../schema-v2/baseline/09_divergence.sql"), include_str!("../../../../schema-v2/baseline/10_phase_state.sql"),
];
#[rustfmt::skip]
const INDEXES: &[(&str, &str)] = &[
    ("after-node", "normalized_events_v1_subregistry_after_node_scope_idx"), ("after-child", "normalized_events_v1_subregistry_after_child_scope_idx"), ("before-node", "normalized_events_v1_subregistry_before_node_scope_idx"), ("before-child", "normalized_events_v1_subregistry_before_child_scope_idx"), ("v2-pointer", "normalized_events_v2_subregistry_pointer_scope_idx"),
];
#[rustfmt::skip]
const MIGRATIONS: &[&str] = &[
    include_str!("../../../../migrations/20260827130000_normalized_events_v1_after_node_scope_idx.sql"), include_str!("../../../../migrations/20260827130100_normalized_events_v1_after_child_scope_idx.sql"), include_str!("../../../../migrations/20260827130200_normalized_events_v1_before_node_scope_idx.sql"), include_str!("../../../../migrations/20260827130300_normalized_events_v1_before_child_scope_idx.sql"), include_str!("../../../../migrations/20260827130400_normalized_events_v2_subregistry_pointer_scope_idx.sql"),
];

#[rustfmt::skip]
fn scan_with_condition(value: &Value, relation: &str, index: Option<&str>, terms: &[&str]) -> bool { value.as_object().is_some_and(|object| (object.get("Relation Name").and_then(Value::as_str) == Some(relation) && index.is_none_or(|expected| object.get("Index Name").and_then(Value::as_str) == Some(expected)) && object.get("Index Cond").and_then(Value::as_str).is_some_and(|condition| terms.iter().all(|term| condition.contains(term)))) || object.values().any(|child| scan_with_condition(child, relation, index, terms))) || value.as_array().is_some_and(|array| array.iter().any(|child| scan_with_condition(child, relation, index, terms))) }
#[rustfmt::skip]
fn unexpected_v1_scan(value: &Value) -> bool { value.as_object().is_some_and(|object| (object.get("Relation Name").and_then(Value::as_str) == Some("normalized_events") && !INDEXES[..4].iter().any(|(_, index)| object.get("Index Name").and_then(Value::as_str) == Some(*index) && object.get("Index Cond").and_then(Value::as_str).is_some_and(|condition| ["chain_id", "scope.logical_name_id", "block_number"].iter().all(|term| condition.contains(term))))) || object.values().any(unexpected_v1_scan)) || value.as_array().is_some_and(|array| array.iter().any(unexpected_v1_scan)) }
#[rustfmt::skip]
fn unbounded_scope_scan(value: &Value) -> bool { value.as_object().is_some_and(|object| ((object.get("Relation Name").and_then(Value::as_str) == Some("normalized_events") && !((object.get("Index Name").and_then(Value::as_str) == Some("normalized_events_name_history_idx") && object.get("Index Cond").and_then(Value::as_str).is_some_and(|condition| condition.contains("logical_name_id"))) || (object.get("Index Name").and_then(Value::as_str) == Some("normalized_events_subregistry_registration_history_idx") && object.get("Index Cond").and_then(Value::as_str).is_some_and(|condition| condition.contains("registry_contract_instance_id"))) || (object.get("Node Type").and_then(Value::as_str) == Some("Bitmap Heap Scan") && serde_json::to_string(object).is_ok_and(|plan| plan.contains(INDEXES[4].1))))) || (object.get("Relation Name").and_then(Value::as_str) == Some("contract_instance_addresses") && !(object.get("Node Type").and_then(Value::as_str) == Some("Index Scan") && object.get("Index Cond").and_then(Value::as_str).is_some_and(|condition| (condition.contains("chain_id") && condition.contains("lower(")) || condition.contains("contract_instance_id"))))) || object.values().any(unbounded_scope_scan)) || value.as_array().is_some_and(|array| array.iter().any(unbounded_scope_scan)) }

fn extract(source: &str, prefix: &str, suffix: &str) -> Result<String> {
    let start = source.find(prefix).context("SQL prefix")? + prefix.len();
    let end = source[start..].find(suffix).context("SQL suffix")? + start;
    Ok(source[start..end].to_owned())
}

#[rustfmt::skip]
fn head_sql() -> Result<Vec<String>> {
    ["V1_AFTER_NODE_SQL", "V1_AFTER_CHILD_SQL", "V1_BEFORE_NODE_SQL", "V1_BEFORE_CHILD_SQL"]
        .map(|name| extract(PROJECT_SOURCE, &format!("pub(crate) const {name}: &str = r#\""), "\"#;")).into_iter().collect()
}

#[rustfmt::skip]
fn base_source() -> Result<String> {
    let output = Command::new("git").args(["show", &format!("{BASE_SHA}:crates/project/src/scope/topology.rs")]).output()?;
    ensure!(output.status.success(), "cannot read base topology source");
    Ok(String::from_utf8(output.stdout)?)
}

#[rustfmt::skip]
fn base_sql() -> Result<String> {
    let source = base_source()?;
    let function = source.split("pub(super) async fn include_event_edges").nth(1).context("base function")?;
    extract(function, "let v1 = sqlx::query(\n        \"", "\",\n    )")
}

#[rustfmt::skip]
fn v2_sql(source: &str, base: bool) -> Result<String> {
    let marker = if base { "async fn include_event_edges" } else { "async fn include_v2_event_edges" };
    let prefix = if base { "let v2 = sqlx::query(\n        \"" } else { "sqlx::query(\n        \"" };
    let function = source.split(marker).nth(1).context("v2 function")?;
    extract(function, prefix, "\",\n    )")
}

async fn seed_v2(pool: &PgPool) -> Result<()> {
    raw_sql("INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind) VALUES ('00000000-0000-0000-0000-000000000435','issue-435-measurement','contract'); INSERT INTO contract_instance_addresses (contract_instance_id,chain_id,address,active_from_block_number) VALUES ('00000000-0000-0000-0000-000000000435','issue-435-measurement','0x0000000000000000000000000000000000000435',0); INSERT INTO name_surfaces (logical_name_id,namespace,raw_name,raw_labels,dns_encoded_name,namehash,labelhashes,normalizer_version,visibility_state,chain_id,block_hash,block_number,canonicality_state) VALUES ('ens:0x0000000000000000000000000000000000000000000000000000000000000002','ens','p.eth',ARRAY['p','eth'],decode('00','hex'),'0x0000000000000000000000000000000000000000000000000000000000000002',ARRAY['0xp','0xeth'],'fixture','active','issue-435-measurement','0x435',435,'canonical'), ('ens:0x0000000000000000000000000000000000000000000000000000000000000001','ens','c.p.eth',ARRAY['c','p','eth'],decode('00','hex'),'0x0000000000000000000000000000000000000000000000000000000000000001',ARRAY['0xc','0xp','0xeth'],'fixture','active','issue-435-measurement','0x435',435,'canonical'); INSERT INTO normalized_events (event_identity,namespace,logical_name_id,event_kind,source_family,manifest_version,chain_id,block_number,block_hash,derivation_kind,canonicality_state,after_state) VALUES ('issue-435-v2-parent','ens','ens:0x0000000000000000000000000000000000000000000000000000000000000002','SubregistryChanged','ens_v2_registry_l1',1,'issue-435-measurement',435,'0x435','ens_v1_unwrapped_authority','canonical','{\"subregistry\":\"0x0000000000000000000000000000000000000435\"}'), ('issue-435-v2-child','ens','ens:0x0000000000000000000000000000000000000000000000000000000000000001','RegistrationGranted','ens_v2_registry_l1',1,'issue-435-measurement',435,'0x435','ens_v1_unwrapped_authority','canonical','{\"registry_contract_instance_id\":\"00000000-0000-0000-0000-000000000435\"}')").execute(pool).await?;
    Ok(())
}

#[rustfmt::skip]
async fn prepare() -> Result<(TestDatabase, PgPool)> {
    let database = TestDatabase::create(TestDatabaseConfig::new("issue_435_measurement")).await?;
    let pool = database.pool().clone();
    let name: String = sqlx::query_scalar("SELECT current_database()").fetch_one(&pool).await?;
    let mut tx = pool.begin().await?;
    raw_sql(&format!("CREATE SCHEMA bigname_phase; ALTER DATABASE \"{}\" SET search_path TO bigname_phase,public; SET LOCAL search_path TO bigname_phase,public", name.replace('"', "\"\""))).execute(&mut *tx).await?;
    for script in BASELINE { raw_sql(script).execute(&mut *tx).await?; }
    raw_sql(&INDEXES.iter().map(|(_, index)| format!("DROP INDEX {index};")).collect::<String>()).execute(&mut *tx).await?;
    tx.commit().await?;
    pool.set_connect_options(pool.connect_options().as_ref().clone().options([("search_path", "bigname_phase,public")]));
    let mut connections = Vec::new();
    for _ in 0..pool.options().get_max_connections() { connections.push(pool.acquire().await?); }
    for connection in &mut connections {
        sqlx::query("SET search_path TO bigname_phase, public").execute(&mut **connection).await?;
    }
    Ok((database, pool))
}

#[rustfmt::skip]
async fn load(pool: &PgPool, start: i64, rows: i64) -> Result<f64> {
    let started = Instant::now();
    let mut transaction = pool.begin().await?; sqlx::query("SET LOCAL session_replication_role = replica").execute(&mut *transaction).await?; raw_sql(&CORPUS.replace("__START__", &start.to_string()).replace("__V1_ROWS__", &rows.to_string()).replace("__FRONTIER__", "1000").replace("__DEPTH__", "8")).execute(&mut *transaction).await?; transaction.commit().await?;
    sqlx::query("VACUUM (ANALYZE) normalized_events").execute(pool).await?;
    Ok(started.elapsed().as_secs_f64())
}

#[rustfmt::skip]
async fn configure_graph(pool: &PgPool, frontier: i64, depth: i64) -> Result<()> {
    sqlx::query(
        "WITH numbered AS (
             SELECT normalized_event_id, split_part(event_identity, ':', 2)::bigint ordinal
             FROM normalized_events WHERE event_identity LIKE 'issue-435:%'
             ORDER BY normalized_event_id LIMIT 8000
         ), endpoints AS (
             SELECT *, CASE WHEN ordinal <= $1 * $2
                 THEN ((ordinal - 1) / $2) * ($2 + 1) + ((ordinal - 1) % $2) + 1
                 ELSE 1000000000000 + ordinal * 2 END parent_number
             FROM numbered
         )
         UPDATE normalized_events event SET
             after_state = jsonb_build_object('node', '0x' || lpad(to_hex(parent_number), 64, '0'), 'child_node', '0x' || lpad(to_hex(parent_number + 1), 64, '0')),
             before_state = CASE WHEN ordinal <= $1 * $2 OR ordinal % 2 = 0 THEN jsonb_build_object('node', '0x' || lpad(to_hex(parent_number), 64, '0'), 'child_node', '0x' || lpad(to_hex(parent_number + 1), 64, '0')) ELSE '{}'::jsonb END
         FROM endpoints WHERE event.normalized_event_id = endpoints.normalized_event_id",
    ).bind(frontier).bind(depth).execute(pool).await?;
    sqlx::query("ANALYZE normalized_events").execute(pool).await?; Ok(())
}

#[rustfmt::skip]
async fn base_tables(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    raw_sql("CREATE TEMP TABLE project_scope_names (logical_name_id text PRIMARY KEY) ON COMMIT DROP; CREATE TEMP TABLE project_scope_children (logical_name_id text PRIMARY KEY) ON COMMIT DROP").execute(&mut **tx).await?; Ok(())
}

#[rustfmt::skip]
async fn head_tables(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    raw_sql("CREATE TEMP TABLE project_scope_topology_pending (logical_name_id text PRIMARY KEY) ON COMMIT DROP; CREATE TEMP TABLE project_scope_topology_current (logical_name_id text PRIMARY KEY) ON COMMIT DROP; CREATE TEMP TABLE project_scope_topology_seen (logical_name_id text PRIMARY KEY) ON COMMIT DROP; CREATE TEMP TABLE project_scope_topology_candidates (logical_name_id text PRIMARY KEY) ON COMMIT DROP; CREATE TEMP TABLE project_scope_children (logical_name_id text PRIMARY KEY) ON COMMIT DROP").execute(&mut **tx).await?;
    sqlx::query("SET LOCAL jit = off").execute(&mut **tx).await?; Ok(())
}

#[rustfmt::skip]
async fn reset(tx: &mut Transaction<'_, Postgres>, head: bool, frontier: i64, depth: i64) -> Result<()> {
    let tables = if head {
        "project_scope_topology_pending, project_scope_topology_current, project_scope_topology_seen, project_scope_topology_candidates, project_scope_children"
    } else {
        "project_scope_names, project_scope_children"
    };
    sqlx::query(&format!("TRUNCATE {tables}")).execute(&mut **tx).await?;
    let table = if head { "project_scope_topology_pending" } else { "project_scope_names" };
    sqlx::query(&format!("INSERT INTO {table} SELECT 'ens:0x' || lpad(to_hex(component * ($2 + 1) + 1), 64, '0') FROM generate_series(0, $1 - 1) component"))
        .bind(frontier).bind(depth).execute(&mut **tx).await?;
    Ok(())
}

#[rustfmt::skip]
async fn base_once(tx: &mut Transaction<'_, Postgres>, sqls: &[String]) -> Result<(u64, Vec<Value>)> {
    let mut iterations = 0;
    let mut trace = Vec::new();
    loop {
        let mut rows = 0;
        for sql in sqls { rows += sqlx::query(sql).bind("issue-435-measurement").bind(435_i64).execute(&mut **tx).await?.rows_affected(); }
        iterations += 1; trace.push(json!({"iteration":iterations,"new_logical_ids":rows}));
        if rows == 0 { return Ok((iterations, trace)); }
    }
}

#[rustfmt::skip]
async fn head_once(tx: &mut Transaction<'_, Postgres>, sqls: &[String]) -> Result<(u64, Vec<Value>)> {
    let mut iterations = 0;
    let mut trace = Vec::new();
    loop {
        sqlx::query("TRUNCATE project_scope_topology_current, project_scope_topology_candidates").execute(&mut **tx).await?;
        let input = sqlx::query("WITH moved AS (DELETE FROM project_scope_topology_pending RETURNING logical_name_id) INSERT INTO project_scope_topology_current SELECT logical_name_id FROM moved").execute(&mut **tx).await?.rows_affected();
        if input == 0 { return Ok((iterations, trace)); }
        sqlx::query("INSERT INTO project_scope_topology_seen SELECT * FROM project_scope_topology_current ON CONFLICT DO NOTHING").execute(&mut **tx).await?;
        for sql in sqls { sqlx::query(sql).bind("issue-435-measurement").bind(435_i64).execute(&mut **tx).await?; }
        let found = sqlx::query("INSERT INTO project_scope_children SELECT * FROM project_scope_topology_candidates ON CONFLICT DO NOTHING").execute(&mut **tx).await?.rows_affected();
        let queued = sqlx::query("INSERT INTO project_scope_topology_pending SELECT candidate.logical_name_id FROM project_scope_topology_candidates candidate LEFT JOIN project_scope_topology_seen seen USING (logical_name_id) WHERE seen.logical_name_id IS NULL ON CONFLICT DO NOTHING").execute(&mut **tx).await?.rows_affected();
        iterations += 1; trace.push(json!({"iteration":iterations,"input_frontier_rows":input,"edges_matched":found,"new_logical_ids":queued}));
    }
}

fn stats(mut times: Vec<f64>) -> Value {
    times.sort_by(f64::total_cmp);
    let mean = times.iter().sum::<f64>() / times.len() as f64;
    let stddev =
        (times.iter().map(|time| (time - mean).powi(2)).sum::<f64>() / times.len() as f64).sqrt();
    json!({"minimum_ms":times[0],"median_ms":times[10],"p95_ms":times[18],"maximum_ms":times[19],"stddev_ms":stddev})
}

#[rustfmt::skip]
async fn await_quiet(pool: &PgPool) -> Result<()> { let mut quiet = 0; while quiet < 4 { let active: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND state <> 'idle' AND datname NOT LIKE 'issue_435_measurement_%'").fetch_one(pool).await?; quiet = if active == 0 { quiet + 1 } else { 0 }; if quiet < 4 { tokio::time::sleep(std::time::Duration::from_secs(5)).await; } } Ok(()) }

#[rustfmt::skip]
async fn measure(pool: &PgPool, sqls: &[String], head: bool, frontier: i64, depth: i64, samples: usize) -> Result<Value> {
    await_quiet(pool).await?;
    configure_graph(pool, frontier, depth).await?;
    let mut tx = pool.begin().await?;
    if !head && frontier > 1 { sqlx::query("SET LOCAL statement_timeout = '60s'").execute(&mut *tx).await?; }
    if head { head_tables(&mut tx).await? } else { base_tables(&mut tx).await? }
    reset(&mut tx, head, frontier, depth).await?;
    let fresh = Instant::now();
    let (iterations, trace) = if head { head_once(&mut tx, sqls).await? } else { base_once(&mut tx, sqls).await? };
    let fresh_ms = fresh.elapsed().as_secs_f64() * 1000.0;
    if samples > 0 { reset(&mut tx, head, frontier, depth).await?; if head { head_once(&mut tx, sqls).await?; } else { base_once(&mut tx, sqls).await?; } }
    let mut times = Vec::with_capacity(20);
    for _ in 0..samples {
        reset(&mut tx, head, frontier, depth).await?;
        let started = Instant::now();
        if head { head_once(&mut tx, sqls).await?; } else { base_once(&mut tx, sqls).await?; }
        times.push(started.elapsed().as_secs_f64() * 1000.0);
    }
    reset(&mut tx, head, frontier, depth).await?;
    if head {
        sqlx::query("WITH moved AS (DELETE FROM project_scope_topology_pending RETURNING logical_name_id) INSERT INTO project_scope_topology_current SELECT * FROM moved").execute(&mut *tx).await?;
    }
    let mut plan: Vec<Value> = Vec::new();
    for sql in sqls { plan.push(sqlx::query_scalar(&format!("{EXPLAIN} {sql}")).bind("issue-435-measurement").bind(435_i64).fetch_one(&mut *tx).await?); }
    tx.rollback().await?;
    let warm = if times.is_empty() { Value::Null } else { stats(times) };
    Ok(json!({"frontier":frontier,"depth":depth,"fresh_restored_ms":fresh_ms,"iterations":iterations,"iteration_trace":trace,"warm":warm,"plan":plan}))
}

#[rustfmt::skip]
async fn build_indexes(pool: &PgPool) -> Result<Vec<Value>> {
    await_quiet(pool).await?;
    let mut evidence = Vec::new();
    for (((label, index), migration), ordinal) in INDEXES.iter().zip(MIGRATIONS).zip(0..) {
        let mut tx = pool.begin().await?;
        let lock = Instant::now();
        sqlx::query("LOCK TABLE normalized_events IN SHARE MODE").execute(&mut *tx).await?;
        let lock_wait_ms = lock.elapsed().as_secs_f64() * 1000.0;
        let build = Instant::now();
        raw_sql(migration).execute(&mut *tx).await?;
        tx.commit().await?;
        let (size, valid): (i64, bool) = sqlx::query_as("SELECT pg_relation_size(indexrelid), indisvalid FROM pg_index WHERE indexrelid = to_regclass($1)").bind(*index).fetch_one(pool).await?;
        evidence.push(json!({"ordinal":ordinal,"index":label,"build_seconds":build.elapsed().as_secs_f64(),"lock_wait_ms":lock_wait_ms,"size_bytes":size,"valid":valid}));
    }
    Ok(evidence)
}

#[tokio::test]
#[ignore = "loads the production-scale issue #435 corpus"]
#[rustfmt::skip]
async fn issue_435_measurement() -> Result<()> {
    ensure!(env::var("ISSUE_435_SEED").as_deref() == Ok("435"), "ISSUE_435_SEED must be 435");
    let scale = env::var("ISSUE_435_SCALE_ROWS").map_or(Ok(5_000_000), |rows| rows.parse())?;
    let head_only = env::var_os("ISSUE_435_HEAD_ONLY").is_some();
    let (database, pool) = prepare().await?;
    let base_source = base_source()?;
    let base = vec![base_sql()?];
    let head = head_sql()?;
    let base_v2 = vec![v2_sql(&base_source, true)?];
    let head_v2 = vec![v2_sql(PROJECT_SOURCE, false)?];
    let load_5m_seconds = load(&pool, 0, scale).await?;
    seed_v2(&pool).await?;
    let mut base_cells = Vec::new();
    for &(frontier, depth) in if head_only { &[][..] } else { &[(1, 1), (100, 3), (1000, 8)] } {
        base_cells.push(match measure(&pool, &base, false, frontier, depth, 20).await { Ok(cell) => cell, Err(error) if error.to_string().contains("canceling statement due to statement timeout") => json!({"frontier":frontier,"depth":depth,"timed_out_ms":60_000,"result":"one base closure statement exceeded the timeout; no latency substituted"}), Err(error) => return Err(error) });
    }
    let base_v2_cell = if head_only { Value::Null } else { measure(&pool, &base_v2, false, 1, 1, 20).await? };
    let migrations = build_indexes(&pool).await?;
    let mut head_cells = Vec::new();
    for (frontier, depth) in [(1, 1), (100, 3), (1000, 8)] {
        head_cells.push(measure(&pool, &head, true, frontier, depth, 20).await?);
    }
    let head_v2_cell = measure(&pool, &head_v2, true, 1, 1, 20).await?;
    let plan = &head_v2_cell["plan"]; ensure!(scan_with_condition(plan, "normalized_events", Some("normalized_events_name_history_idx"), &["logical_name_id"]) && scan_with_condition(plan, "normalized_events", Some("normalized_events_subregistry_registration_history_idx"), &["registry_contract_instance_id"]) && scan_with_condition(plan, "contract_instance_addresses", Some("contract_instance_addresses_active_idx"), &["chain_id", "lower("]) && scan_with_condition(plan, "contract_instance_addresses", None, &["contract_instance_id"]) && serde_json::to_string(plan)?.contains(INDEXES[4].1) && !unbounded_scope_scan(plan), "v2 plan was not frontier-indexed");
    let exact_plan = serde_json::to_string(&head_cells[2]["plan"])?;
    for (_, index) in &INDEXES[..4] { ensure!(exact_plan.contains(index) && scan_with_condition(&head_cells[2]["plan"], "normalized_events", Some(index), &["chain_id", "scope.logical_name_id", "block_number"]), "plan did not use bounded {index}"); } ensure!(!unexpected_v1_scan(&head_cells[2]["plan"]), "v1 plan included an unexpected normalized-events scan");
    let mut visibility_mutation = head.clone(); visibility_mutation[0] = visibility_mutation[0].replacen("  AND consumer_visibility = 'activated'\n", "", 1);
    let mutation = measure(&pool, &visibility_mutation, true, 1, 1, 0).await?;
    ensure!(!serde_json::to_string(&mutation["plan"])?.contains(INDEXES[0].1), "mismatched predicate retained after-node index");
    let mut nonblank_mutation = head.clone(); nonblank_mutation[0] = nonblank_mutation[0].replacen("  AND btrim(after_state ->> 'node') <> ''\n", "  AND NULLIF(after_state ->> 'node', '') IS NOT NULL\n", 1);
    let nonblank = measure(&pool, &nonblank_mutation, true, 1, 1, 0).await?;
    let load_10m_seconds = load(&pool, scale, scale).await?;
    let head_10m = measure(&pool, &head, true, 1000, 8, 20).await?;
    let base_10m = if head_only { Value::Null } else { raw_sql(&INDEXES.iter().map(|(_, index)| format!("DROP INDEX {index};")).collect::<String>()).execute(&pool).await?; match measure(&pool, &base, false, 1000, 8, 20).await { Ok(cell) => cell, Err(error) if error.to_string().contains("canceling statement due to statement timeout") => json!({"frontier":1000,"depth":8,"timed_out_ms":60_000,"result":"one base closure statement exceeded the timeout; no latency substituted"}), Err(error) => return Err(error) } };
    let revision = String::from_utf8(Command::new("git").args(["rev-parse", "HEAD"]).output()?.stdout)?;
    let output = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/issue-435-evidence").join(revision.trim());
    fs::create_dir_all(&output)?;
    fs::write(output.join("topology-matrix.json"), serde_json::to_vec_pretty(&json!({"seed":435,"base_sha":BASE_SHA,"first_corpus_rows":scale,"second_corpus_rows":scale,"load_5m_seconds":load_5m_seconds,"load_10m_seconds":load_10m_seconds,"base_5m":base_cells,"head_5m":head_cells,"base_v2":base_v2_cell,"head_v2":head_v2_cell,"base_10m":base_10m,"head_10m":head_10m,"migrations":migrations,"predicate_mutation":mutation,"nonblank_mutation":nonblank}))?)?;
    database.cleanup().await?;
    Ok(())
}
