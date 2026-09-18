//! ENSv2 resolver support classification from the latest implementation observation: a
//! factory-announced implementation (`Upgraded` history with `source_event = ProxyDeployed`)
//! supports the proxy, and a discovered proxy with no observation is explicitly
//! `resolver_implementation_unknown`. Declaration precedence pairs a manifest declaration with
//! a same-namespace `resolver` edge; a creation self-edge is one.

use anyhow::Result;
use bigname_project::{BatchRequest, Engine, RunMode};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgPool, raw_sql};

const CHAIN: &str = "ethereum-sepolia";
const REGISTRY_INSTANCE: &str = "66000000-0000-0000-0000-000000000900";
const ANNOUNCED_INSTANCE: &str = "66000000-0000-0000-0000-000000000901";
const SILENT_INSTANCE: &str = "66000000-0000-0000-0000-000000000902";
const CREATED_INSTANCE: &str = "66000000-0000-0000-0000-000000000903";
const CREATED_RESOLVER: &str = "0x6666666666666666666666666666666666666666";
const ANNOUNCED_RESOLVER: &str = "0x1111111111111111111111111111111111111111";
const SILENT_RESOLVER: &str = "0x3333333333333333333333333333333333333333";
const REGISTRY: &str = "0x5555555555555555555555555555555555555555";
const FACTORY: &str = "0x4444444444444444444444444444444444444444";
const IMPLEMENTATION: &str = "0x2222222222222222222222222222222222222222";
const BLOCK: i64 = 10;

fn hash(n: i64) -> String {
    format!("0x{n:064x}")
}

#[tokio::test]
async fn announced_implementation_supports_the_proxy_and_silence_is_unknown() -> Result<()> {
    let (db, pool) = database("resolver_announcement_support").await?;
    seed(&pool).await?;
    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block: BLOCK,
            affected_from_block: BLOCK,
            affected_to_block: BLOCK,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;

    let rows: Vec<(String, String, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT lower(resolver_address), support_status, unsupported_reason,
                declared_summary #>> '{classification,role}'
         FROM resolver_current WHERE chain_id = $1 ORDER BY 1",
    )
    .bind(CHAIN)
    .fetch_all(&pool)
    .await?;
    assert_eq!(
        rows,
        [
            (
                ANNOUNCED_RESOLVER.to_owned(),
                "supported".to_owned(),
                None,
                Some("permissioned_resolver".to_owned()),
            ),
            (
                SILENT_RESOLVER.to_owned(),
                "unsupported".to_owned(),
                Some("resolver_implementation_unknown".to_owned()),
                None,
            ),
        ]
    );
    db.cleanup().await?;
    Ok(())
}

/// `ResolverCreated()` only says that events are read from this address. It names no
/// implementation, so a resolver known only by its creation self-edge is served as unsupported.
#[tokio::test]
async fn creation_self_edge_alone_is_served_as_implementation_unknown() -> Result<()> {
    let (db, pool) = database("resolver_creation_only_support").await?;
    seed(&pool).await?;
    seed_created_resolver(&pool).await?;
    run(&pool).await?;

    let row: Option<(String, Option<String>)> = sqlx::query_as(
        "SELECT support_status, unsupported_reason
         FROM resolver_current WHERE chain_id = $1 AND lower(resolver_address) = $2",
    )
    .bind(CHAIN)
    .bind(CREATED_RESOLVER)
    .fetch_optional(&pool)
    .await?;
    assert_eq!(
        row,
        Some((
            "unsupported".to_owned(),
            Some("resolver_implementation_unknown".to_owned()),
        ))
    );
    db.cleanup().await?;
    Ok(())
}

/// The resolver's own `ResolverCreated()` self-edge carries the resolver manifest's namespace,
/// so a same-namespace declaration is applied through it and provenance records that namespace.
#[tokio::test]
async fn creation_self_edge_admits_a_declared_resolver() -> Result<()> {
    let (db, pool) = database("resolver_creation_declaration").await?;
    seed_with_contracts(&pool, json!([declaration(CREATED_RESOLVER)])).await?;
    seed_created_resolver(&pool).await?;
    run(&pool).await?;
    assert_eq!(
        classification(&pool, CREATED_RESOLVER).await?,
        Some((
            "supported".to_owned(),
            Some("public_resolver_v2".to_owned()),
            Some("ens".to_owned()),
        ))
    );
    db.cleanup().await?;
    Ok(())
}

fn declaration(address: &str) -> Value {
    json!({"role":"public_resolver_v2","address":address,"proxy_kind":"none","start_block":BLOCK})
}

async fn run(pool: &PgPool) -> Result<()> {
    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block: BLOCK,
            affected_from_block: BLOCK,
            affected_to_block: BLOCK,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;
    Ok(())
}

/// `(support_status, classification role, admission namespace)` for one resolver row.
async fn classification(
    pool: &PgPool,
    address: &str,
) -> Result<Option<(String, Option<String>, Option<String>)>> {
    Ok(sqlx::query_as(
        "SELECT support_status, declared_summary #>> '{classification,role}',
                provenance ->> 'classification_admission_namespace'
         FROM resolver_current WHERE chain_id = $1 AND lower(resolver_address) = $2",
    )
    .bind(CHAIN)
    .bind(address)
    .fetch_optional(pool)
    .await?)
}

/// What Interpret writes for a `ResolverCreated()` log: the instance, its address, the
/// `resolver` self-edge with `admission_basis = resolver_created`, and `ContractDiscovered`.
/// There is no `Upgraded` for this address.
async fn seed_created_resolver(pool: &PgPool) -> Result<()> {
    let resolver_manifest: i64 = sqlx::query_scalar(
        "SELECT manifest_id FROM manifest_versions WHERE source_family = 'ens_v2_resolver_l1'",
    )
    .fetch_one(pool)
    .await?;
    sqlx::query("INSERT INTO contract_instances (contract_instance_id,chain_id,contract_kind) VALUES ($1::uuid,$2,'contract')")
        .bind(CREATED_INSTANCE).bind(CHAIN).execute(pool).await?;
    sqlx::query("INSERT INTO contract_instance_addresses (contract_instance_id,chain_id,address,active_from_block_number,active_from_block_hash,source_manifest_id) VALUES ($1::uuid,$2,$3,$4,$5,$6)")
        .bind(CREATED_INSTANCE).bind(CHAIN).bind(CREATED_RESOLVER).bind(BLOCK).bind(hash(BLOCK)).bind(resolver_manifest).execute(pool).await?;
    sqlx::query("INSERT INTO discovery_edges (chain_id,edge_kind,from_contract_instance_id,to_contract_instance_id,discovery_source,admission_basis,source_manifest_id,active_from_block_number,active_from_block_hash,canonicality_state) VALUES ($1,'resolver',$2::uuid,$2::uuid,'ResolverCreated','resolver_created',$3,$4,$5,'canonical')")
        .bind(CHAIN).bind(CREATED_INSTANCE).bind(resolver_manifest).bind(BLOCK).bind(hash(BLOCK)).execute(pool).await?;
    let after = json!({"source_event":"ResolverCreated","resolver":CREATED_RESOLVER});
    let raw_fact_ref = json!({"kind":"raw_log","chain_id":CHAIN,"block_hash":hash(BLOCK),"block_number":BLOCK,"transaction_hash":hash(3000),"transaction_index":2,"log_index":0,"emitting_address":CREATED_RESOLVER,"state_scope":format!("{CREATED_RESOLVER}:-:-:-:ResolverCreated"),"interpreter_state_key":format!("ens:ens_v2_resolver_l1:-:-:ResolverCreated:{CREATED_RESOLVER}:-:-:-:ResolverCreated")});
    sqlx::query("INSERT INTO normalized_events (event_identity,namespace,event_kind,source_family,manifest_version,chain_id,block_number,block_hash,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state,raw_fact_ref,before_state,source_manifest_id) VALUES ('created','ens','ContractDiscovered','ens_v2_resolver_l1',1,$1,$2,$3,$4,2,0,'ens_v2_resolver','canonical',$5,$6,'{}',$7)")
        .bind(CHAIN).bind(BLOCK).bind(hash(BLOCK)).bind(hash(3000)).bind(after).bind(raw_fact_ref).bind(resolver_manifest).execute(pool).await?;
    Ok(())
}

async fn seed(pool: &PgPool) -> Result<()> {
    seed_with_contracts(pool, json!([])).await
}

/// `contracts` is the resolver manifest's declaration list.
async fn seed_with_contracts(pool: &PgPool, contracts: Value) -> Result<()> {
    sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,$2,$3,to_timestamp($3::double precision),'canonical')")
        .bind(CHAIN).bind(hash(BLOCK)).bind(BLOCK).execute(pool).await?;
    let payload = json!({"deployment_epoch":"announcement_fixture","resolver_implementations":[{"role":"permissioned_resolver","address":IMPLEMENTATION}],"contracts":contracts,"capability_flags":{}});
    let resolver_manifest: i64 = sqlx::query_scalar("INSERT INTO manifest_versions (manifest_version,namespace,source_family,chain_id,deployment_label,rollout_status,normalizer_version,file_path,manifest_payload) VALUES (1,'ens','ens_v2_resolver_l1',$1,'announcement_fixture','active','fixture','fixture/resolver.toml',$2) RETURNING manifest_id").bind(CHAIN).bind(&payload).fetch_one(pool).await?;
    sqlx::query("INSERT INTO normalized_events (event_identity,namespace,event_kind,source_family,manifest_version,source_manifest_id,chain_id,derivation_kind,canonicality_state,after_state) VALUES ('manifest','ens','SourceManifestUpdated','ens_v2_resolver_l1',1,$1,$2,'manifest_sync','canonical',$3)").bind(resolver_manifest).bind(CHAIN).bind(json!({"rollout_status":"active","normalizer_version":"fixture","manifest_payload":payload})).execute(pool).await?;
    let registry_payload =
        json!({"deployment_epoch":"announcement_fixture","contracts":[],"capability_flags":{}});
    let registry_manifest: i64 = sqlx::query_scalar("INSERT INTO manifest_versions (manifest_version,namespace,source_family,chain_id,deployment_label,rollout_status,normalizer_version,file_path,manifest_payload) VALUES (1,'ens','ens_v2_registry_l1',$1,'announcement_fixture','active','fixture','fixture/registry.toml',$2) RETURNING manifest_id").bind(CHAIN).bind(&registry_payload).fetch_one(pool).await?;
    // Stage the registry manifest too, so its pointer edges carry the `ens` namespace that
    // declaration precedence compares against.
    sqlx::query("INSERT INTO normalized_events (event_identity,namespace,event_kind,source_family,manifest_version,source_manifest_id,chain_id,derivation_kind,canonicality_state,after_state) VALUES ('registry-manifest','ens','SourceManifestUpdated','ens_v2_registry_l1',1,$1,$2,'manifest_sync','canonical',$3)").bind(registry_manifest).bind(CHAIN).bind(json!({"rollout_status":"active","normalizer_version":"fixture","manifest_payload":registry_payload})).execute(pool).await?;
    for instance in [REGISTRY_INSTANCE, ANNOUNCED_INSTANCE, SILENT_INSTANCE] {
        sqlx::query("INSERT INTO contract_instances (contract_instance_id,chain_id,contract_kind) VALUES ($1::uuid,$2,'contract')").bind(instance).bind(CHAIN).execute(pool).await?;
    }
    for (instance, address) in [
        (ANNOUNCED_INSTANCE, ANNOUNCED_RESOLVER),
        (SILENT_INSTANCE, SILENT_RESOLVER),
    ] {
        sqlx::query("INSERT INTO contract_instance_addresses (contract_instance_id,chain_id,address,active_from_block_number,active_from_block_hash,source_manifest_id) VALUES ($1::uuid,$2,$3,$4,$5,$6)")
            .bind(instance).bind(CHAIN).bind(address).bind(BLOCK).bind(hash(BLOCK)).bind(registry_manifest).execute(pool).await?;
        sqlx::query("INSERT INTO discovery_edges (chain_id,edge_kind,from_contract_instance_id,to_contract_instance_id,discovery_source,admission_basis,source_manifest_id,active_from_block_number,active_from_block_hash,canonicality_state) VALUES ($1,'resolver',$2::uuid,$3::uuid,'ResolverUpdated','reachable_from_root',$4,$5,$6,'canonical')")
            .bind(CHAIN).bind(REGISTRY_INSTANCE).bind(instance).bind(registry_manifest).bind(BLOCK).bind(hash(BLOCK)).execute(pool).await?;
    }
    // The registry pointers that discovered both proxies put them in project scope.
    for (index, resolver) in [(0, ANNOUNCED_RESOLVER), (1, SILENT_RESOLVER)] {
        let after = json!({"source_event":"ResolverUpdated","resolver":resolver,"sender":FACTORY,"token_id":hash(index + 1)});
        let raw_fact_ref = json!({"kind":"raw_log","chain_id":CHAIN,"block_hash":hash(BLOCK),"block_number":BLOCK,"transaction_hash":hash(2000),"transaction_index":1,"log_index":index,"emitting_address":REGISTRY,"state_scope":format!("{REGISTRY}:-:{}:-:ResolverUpdated", hash(index + 1)),"interpreter_state_key":format!("ens:ens_v2_registry_l1:-:-:resolver:{REGISTRY}:-:{}:-:ResolverUpdated", hash(index + 1))});
        sqlx::query("INSERT INTO normalized_events (event_identity,namespace,event_kind,source_family,manifest_version,chain_id,block_number,block_hash,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state,raw_fact_ref,before_state,source_manifest_id) VALUES ($1,'ens','ResolverChanged','ens_v2_registry_l1',1,$2,$3,$4,$5,1,$6,'ens_v2_registry_resource_surface','canonical',$7,$8,'{}',$9)")
            .bind(format!("pointer-{index}")).bind(CHAIN).bind(BLOCK).bind(hash(BLOCK)).bind(hash(2000)).bind(index).bind(after).bind(raw_fact_ref).bind(registry_manifest).execute(pool).await?;
    }
    // The factory announcement, recorded by Interpret on the proxy's `Upgraded` stream.
    let after = json!({"source_event":"ProxyDeployed","proxy_address":ANNOUNCED_RESOLVER,"implementation":IMPLEMENTATION});
    let state_key =
        format!("ens:ens_v2_resolver_l1:-:-:Upgraded:{ANNOUNCED_RESOLVER}:-:-:-:Upgraded");
    let raw_fact_ref = json!({"kind":"raw_log","chain_id":CHAIN,"block_hash":hash(BLOCK),"block_number":BLOCK,"transaction_hash":hash(1000),"transaction_index":0,"log_index":0,"emitting_address":FACTORY,"state_scope":format!("{ANNOUNCED_RESOLVER}:-:-:-:Upgraded"),"interpreter_state_key":state_key});
    sqlx::query("INSERT INTO normalized_events (event_identity,namespace,event_kind,source_family,manifest_version,chain_id,block_number,block_hash,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state,raw_fact_ref,before_state,source_manifest_id) VALUES ('announced','ens','Upgraded','ens_v2_resolver_l1',1,$1,$2,$3,$4,0,0,'proxy_upgrade','canonical',$5,$6,'{}',$7)")
        .bind(CHAIN).bind(BLOCK).bind(hash(BLOCK)).bind(hash(1000)).bind(after).bind(raw_fact_ref).bind(resolver_manifest).execute(pool).await?;
    Ok(())
}

async fn database(name: &str) -> Result<(TestDatabase, PgPool)> {
    let database = TestDatabase::create(TestDatabaseConfig::new(name.to_string())).await?;
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
