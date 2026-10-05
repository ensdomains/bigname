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
