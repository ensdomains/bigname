//! Manual disposable performance fixture. Only retained inputs are bulk-written here; all
//! serving families are built by the production Project path. API RSS is measured separately
//! in a fresh, memory-limited API process, never in this fixture-seeding process.

use super::*;
use std::path::PathBuf;

const ADDRESS: &str = "0x000000000000000000000000000000000000a235";
const OTHER: &str = "0x000000000000000000000000000000000000b235";
const RESOLVER: &str = "0x000000000000000000000000000000000000c235";
const CHAIN: &str = "ethereum-mainnet";

#[tokio::test]
#[ignore = "manual production-Project fixture for isolated API performance measurement"]
async fn address_history_create_performance_fixture() -> Result<()> {
    let count: usize = std::env::var("BIGNAME_HISTORY_PERF_NAMES")?.parse()?;
    let directory = PathBuf::from(std::env::var("BIGNAME_HISTORY_PERF_DIR")?);
    std::fs::create_dir_all(&directory)?;
    let database = TestDatabase::new_migrated().await?;
    // Persist the disposable database name before seeding so failures can be cleaned up too.
    std::fs::write(
        directory.join(format!("database-{count}.txt")),
        &database.database_name,
    )?;
    database
        .seed_snapshot_selector_chain_positions(&json!({"ethereum": {
            "chain_id":CHAIN,"block_number":count,"block_hash":format!("perf-{count}"),
            "timestamp":"2026-04-17T00:00:00Z"
        }}))
        .await?;
    sqlx::query("INSERT INTO chain_lineage(chain_id,block_hash,block_number,block_timestamp,canonicality_state) SELECT $1, 'perf-'||n,n,to_timestamp(1776384000+n),'canonical'::canonicality_state FROM generate_series(1,$2) n ON CONFLICT DO NOTHING")
        .bind(CHAIN).bind(count as i64).execute(&database.pool).await?;
    let manifest = declare_family_fixture_resolver(
        &database.pool,
        "ens",
        CHAIN,
        "ens_v1_resolver_l1",
        RESOLVER,
    )
    .await?;
    for start in (1..=count).step_by(1_000) {
        let mut names = Vec::new();
        for n in start..=(start + 999).min(count) {
            let name = format!("perf-{n:06}.eth");
            let normalized = bigname_domain::normalization::normalize_name(&name)?;
            let (logical, node) = phase_logical_identity("ens", &name)?;
            let labels: Vec<_> = name.split('.').collect();
            let hashes: Vec<_> = labels
                .iter()
                .map(|label| format!("{:#x}", alloy_primitives::keccak256(label.as_bytes())))
                .collect();
            names.push(json!({"n":n,"name":name,"logical":logical,"node":node,"labels":labels,"hashes":hashes,
                "dns":hex::encode(normalized.dns_encoded_name),"resource":Uuid::from_u128(0x23500000+n as u128*4),
                "token":Uuid::from_u128(0x23500001+n as u128*4),"binding":Uuid::from_u128(0x23500002+n as u128*4)}));
        }
        let names = Value::Array(names);
        for statement in [
            "INSERT INTO token_lineages(token_lineage_id,chain_id,block_hash,block_number,canonicality_state) SELECT token,'ethereum-mainnet','perf-'||n,n,'canonical'::canonicality_state FROM jsonb_to_recordset($1) r(n bigint,token uuid)",
            "INSERT INTO resources(resource_id,token_lineage_id,chain_id,block_hash,block_number,canonicality_state) SELECT resource,token,'ethereum-mainnet','perf-'||n,n,'canonical'::canonicality_state FROM jsonb_to_recordset($1) r(n bigint,resource uuid,token uuid)",
            "INSERT INTO name_surfaces(logical_name_id,namespace,raw_name,raw_labels,dns_encoded_name,namehash,labelhashes,normalizer_version,visibility_state,chain_id,block_hash,block_number,canonicality_state) SELECT logical,'ens',name,labels,decode(dns,'hex'),node,hashes,'ens-normalize','active','ethereum-mainnet','perf-'||n,n,'canonical'::canonicality_state FROM jsonb_to_recordset($1) r(n bigint,logical text,name text,labels text[],dns text,node text,hashes text[])",
            "INSERT INTO surface_bindings(surface_binding_id,logical_name_id,resource_id,binding_kind,authority_arm,active_from,chain_id,block_hash,block_number,canonicality_state) SELECT binding,logical,resource,'declared_registry_path','ens_v1',lineage.block_timestamp,'ethereum-mainnet','perf-'||n,n,'canonical'::canonicality_state FROM jsonb_to_recordset($1) r(n bigint,binding uuid,logical text,resource uuid) JOIN chain_lineage lineage ON lineage.chain_id='ethereum-mainnet' AND lineage.block_hash='perf-'||n",
        ] {
            sqlx::query(statement)
                .bind(&names)
                .execute(&database.pool)
                .await?;
        }
        sqlx::query(r#"
            INSERT INTO normalized_events(event_identity,namespace,logical_name_id,resource_id,event_kind,source_family,manifest_version,source_manifest_id,chain_id,block_hash,block_number,transaction_hash,transaction_index,log_index,raw_fact_ref,derivation_kind,canonicality_state,after_state)
            SELECT 'perf:'||n||':'||kind,'ens',CASE WHEN ordinal<3 THEN logical END,CASE WHEN ordinal<3 THEN resource END,kind,family,1,CASE WHEN ordinal=3 THEN $4 END,'ethereum-mainnet','perf-'||n,n,'tx-'||n,0,ordinal,jsonb_build_object('kind','raw_log','emitting_address',$3::text),'ens_v1_unwrapped_authority','canonical'::canonicality_state,
                CASE ordinal WHEN 0 THEN jsonb_build_object('authority_kind','registrar','registrant',$2::text,'expiry',1900000000)
                    WHEN 1 THEN jsonb_build_object('source_event','Transfer','node',node,'owner',$5::text)
                    WHEN 2 THEN jsonb_build_object('node',node,'resolver',$3::text)
                    ELSE jsonb_build_object('node',node,'resolver',$3::text,'record_key','text:description','record_family','text','selector_key','description','value','fixture') END
            FROM jsonb_to_recordset($1) r(n bigint,logical text,resource uuid,node text)
            CROSS JOIN (VALUES (0,'RegistrationGranted','ens_v1_registrar_l1'),(1,'AuthorityTransferred','ens_v1_registry_l1'),(2,'ResolverChanged','ens_v1_registry_l1'),(3,'RecordChanged','ens_v1_resolver_l1')) events(ordinal,kind,family)
        "#).bind(&names).bind(ADDRESS).bind(RESOLVER).bind(manifest).bind(OTHER).execute(&database.pool).await?;
        println!(
            "seeded {} / {count} retained names",
            (start + 999).min(count)
        );
    }
    // Match the normalizer stamp from the same normalizer that created these names.
    sqlx::query("UPDATE name_surfaces SET normalizer_version=$1")
        .bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION)
        .execute(&database.pool)
        .await?;
    let token = bigname_project::families::input_token(&database.pool, CHAIN).await?;
    let outcome = bigname_project::families::apply(
        &database.pool,
        CHAIN,
        &bigname_project::Marker {
            number: count as i64,
            hash: format!("perf-{count}"),
        },
        bigname_project::families::FamilyMode::Rebuild,
        &token,
        &bigname_project::families::FamilyOptions::new(
            bigname_content_hash::INTERPRETER_CONTENT_HASH,
        )
        .with_max_blocks_per_run(count as u64 + 1)
        .with_rebuild_ranges(bigname_project::families::RebuildRanges::Through(
            count as i64,
        )),
    )
    .await?;
    anyhow::ensure!(
        outcome.marker.as_ref().map(|marker| marker.number) == Some(count as i64),
        "incomplete performance fixture publication: {outcome:?}"
    );
    sqlx::raw_sql("ANALYZE").execute(&database.pool).await?;
    let indexed: i64 = sqlx::query_scalar(
        "SELECT count(DISTINCT logical_name_id) FROM project_address_name_index WHERE address=$1",
    )
    .bind(ADDRESS)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(
        indexed, count as i64,
        "real Project must publish every fixture owner"
    );
    let (body, stats) = {
        let stats = std::sync::Arc::new(std::sync::Mutex::new(
            bigname_storage::AddressHistoryWorkingSet::default(),
        ));
        let body = bigname_storage::with_address_history_working_set(
            stats.clone(),
            v2_history_payload_for_database(
                &database,
                &format!("/v1/addresses/{ADDRESS}/history?relation=owner&page_size=1"),
            ),
        )
        .await?;
        let snapshot = stats.lock().unwrap().clone();
        (body, snapshot)
    };
    assert_eq!(body["data"].as_array().unwrap().len(), 1);
    assert_eq!(
        body["page"]["total_count"],
        if count * 4 > 10_000 {
            Value::Null
        } else {
            json!(count * 4)
        }
    );
    std::fs::write(
        directory.join(format!("fixture-{count}.json")),
        serde_json::to_vec_pretty(
            &json!({"database":database.database_name,"names":count,"events":count*4,"owner":ADDRESS,"source":"retained normalized/identity fixture inputs; production Project family builder","working_set":format!("{stats:?}"),"sample":body}),
        )?,
    )?;
    database.pool.close().await;
    database.lookup_pool.close().await;
    println!("performance fixture ready: {}", database.database_name);
    // Kept only for the explicitly requested external API measurement; its database name is
    // recorded above and the harness drops this task-owned database after measurement.
    Ok(())
}

/// Runs identically in the pre-catalogue and catalogue trees against separate clones of the
/// retained performance fixture. It measures the production reset/range publication path;
/// only the caller-owned disposable fixture URL is accepted, and no API backfill is involved.
#[tokio::test]
#[ignore = "manual isolated retained-fixture Project rebuild cost measurement"]
async fn address_history_rebuild_retained_performance_fixture() -> Result<()> {
    let url = std::env::var("BIGNAME_HISTORY_REBUILD_URL")?;
    let output = PathBuf::from(std::env::var("BIGNAME_HISTORY_REBUILD_RECEIPT")?);
    let options =
        PgConnectOptions::from_str(&url)?.options([("search_path", "bigname_phase".to_owned())]);
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await?;
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    anyhow::ensure!(
        database.starts_with("bigname_api_test_"),
        "requires disposable API fixture"
    );
    let (number, hash): (i64, String) = sqlx::query_as(
        "SELECT current_block_number,current_block_hash FROM project_family_marker WHERE chain_id=$1",
    )
    .bind(CHAIN)
    .fetch_one(&pool)
    .await?;
    let start_wal: String = sqlx::query_scalar("SELECT pg_current_wal_lsn()::text")
        .fetch_one(&pool)
        .await?;
    let token = bigname_project::families::input_token(&pool, CHAIN).await?;
    let initial = json!({"database":database,"content_hash":bigname_content_hash::INTERPRETER_CONTENT_HASH,
        "target_block":number,"start_wal_lsn":start_wal,"rows_written":{},"undo_rows_written":0,
        "initial_database_state":rebuild_progress(&pool, &start_wal).await?});
    durable_profile_receipt(&output.with_extension("start.json"), &initial)?;
    let progress_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(output.with_extension("progress.jsonl"))?;
    tracing_subscriber::fmt()
        .json()
        .with_env_filter("bigname_project::families=debug")
        .with_ansi(false)
        .with_writer(move || {
            DurableProfileWriter(progress_file.try_clone().expect("clone profile log"))
        })
        .try_init()
        .map_err(|error| anyhow::anyhow!("install profile tracing: {error}"))?;
    let (stop_progress, mut stopped) = tokio::sync::watch::channel(false);
    let monitor_pool = pool.clone();
    let monitor_wal = start_wal.clone();
    let monitor_output = output.with_extension("database-progress.jsonl");
    let monitor = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                    let value = rebuild_progress(&monitor_pool, &monitor_wal).await?;
                    let mut writer = std::fs::OpenOptions::new().create(true).append(true)
                        .open(&monitor_output)?;
                    serde_json::to_writer(&mut writer, &value)?;
                    std::io::Write::write_all(&mut writer, b"\n")?;
                    writer.sync_data()?;
                }
                _ = stopped.changed() => break,
            }
        }
        Ok::<_, anyhow::Error>(())
    });
    let started = std::time::Instant::now();
    let (outcome, error) = bigname_project::families::run(
        &pool,
        CHAIN,
        &bigname_project::Marker { number, hash },
        bigname_project::families::FamilyMode::Rebuild,
        &token,
        &bigname_project::families::FamilyOptions::new(
            bigname_content_hash::INTERPRETER_CONTENT_HASH,
        )
        .with_max_blocks_per_run(number as u64 + 1)
        .with_rebuild_ranges(bigname_project::families::RebuildRanges::Through(number)),
    )
    .await;
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.;
    let _ = stop_progress.send(true);
    let monitor_error = match monitor.await {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(error.to_string()),
        Err(error) => Some(error.to_string()),
    };
    let wal_bytes: String =
        sqlx::query_scalar("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(),$1::pg_lsn)::text")
            .bind(&start_wal)
            .fetch_one(&pool)
            .await?;
    sqlx::raw_sql("ANALYZE").execute(&pool).await?;
    let relations: Vec<Value> = sqlx::query_scalar(
        "SELECT jsonb_build_object('name',relname,'rows',reltuples::bigint,
          'heap_bytes',pg_relation_size(oid),'index_bytes',pg_indexes_size(oid),
          'total_bytes',pg_total_relation_size(oid))
         FROM pg_class WHERE relnamespace='bigname_phase'::regnamespace
          AND (relname LIKE 'project_history_%' OR relname='project_address_history_anchor'
            OR relname='project_family_undo' OR relname='normalized_events') AND relkind='r'
         ORDER BY relname",
    )
    .fetch_all(&pool)
    .await?;
    let indexes: Vec<Value> = sqlx::query_scalar(
        "SELECT jsonb_build_object('name',relname,'bytes',pg_relation_size(oid)) FROM pg_class
         WHERE relnamespace='bigname_phase'::regnamespace AND relname=ANY($1)",
    )
    .bind(vec![
        "normalized_events_name_history_idx",
        "normalized_events_resource_history_idx",
        "normalized_events_project_node_history_idx",
        "normalized_events_record_id_write_idx",
        "normalized_events_history_discovery_name_idx",
        "normalized_events_history_discovery_resource_idx",
    ])
    .fetch_all(&pool)
    .await?;
    let max_undo: Value = sqlx::query_scalar(
        "SELECT COALESCE(to_jsonb(generation),'{}'::jsonb) FROM (
         SELECT block_number,count(*) AS rows FROM project_family_undo WHERE chain_id=$1
         GROUP BY block_number ORDER BY count(*) DESC LIMIT 1) generation",
    )
    .bind(CHAIN)
    .fetch_one(&pool)
    .await?;
    let receipt = json!({"database":database,"content_hash":bigname_content_hash::INTERPRETER_CONTENT_HASH,
        "target_block":number,"elapsed_ms":elapsed_ms,"start_wal_lsn":start_wal,"wal_bytes":wal_bytes,
        "error":error.as_ref().map(ToString::to_string),"monitor_error":monitor_error,
        "marker_block":outcome.marker.as_ref().map(|marker|marker.number),
        "rows_written":outcome.rows,"undo_rows_written":outcome.undo_rows,"blocks":outcome.blocks,
        "ranges":outcome.ranges,"statistics_refreshes":outcome.statistics_refreshes,
        "max_retained_journal_generation":max_undo,"relations":relations,"source_indexes":indexes});
    durable_profile_receipt(&output, &receipt)?;
    println!("retained fixture rebuild: {receipt}");
    pool.close().await;
    anyhow::ensure!(error.is_none(), "rebuild failed: {error:?}");
    anyhow::ensure!(
        outcome.marker.as_ref().map(|marker| marker.number) == Some(number),
        "incomplete rebuild: {outcome:?}"
    );
    Ok(())
}

fn durable_profile_receipt(path: &std::path::Path, value: &Value) -> Result<()> {
    let mut output = std::fs::File::create(path)?;
    serde_json::to_writer_pretty(&mut output, value)?;
    output.sync_all()?;
    Ok(())
}

struct DurableProfileWriter(std::fs::File);
impl std::io::Write for DurableProfileWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        std::io::Write::write(&mut self.0, bytes)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.0.sync_data()
    }
}
impl Drop for DurableProfileWriter {
    fn drop(&mut self) {
        let _ = self.0.sync_data();
    }
}

async fn rebuild_progress(pool: &PgPool, start_wal: &str) -> Result<Value> {
    Ok(sqlx::query_scalar(
        "SELECT jsonb_build_object('at',clock_timestamp(),'wal_lsn',pg_current_wal_lsn()::text,
         'wal_bytes',pg_wal_lsn_diff(pg_current_wal_lsn(),$1::pg_lsn),
         'marker',(SELECT to_jsonb(marker) FROM project_family_marker marker WHERE chain_id=$2),
         'physical_row_counters',(SELECT jsonb_object_agg(relname,jsonb_build_object(
             'inserted',n_tup_ins,'updated',n_tup_upd,'deleted',n_tup_del))
             FROM pg_stat_user_tables WHERE schemaname='bigname_phase'))",
    )
    .bind(start_wal)
    .bind(CHAIN)
    .fetch_one(pool)
    .await?)
}
