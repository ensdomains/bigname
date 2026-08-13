use std::{path::Path, process::Stdio, str::FromStr, time::Duration};

use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use anyhow::{Context, Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig, database_url_from_env};
use serde::Serialize;
use serde_json::json;
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tokio::process::{Child, Command};
use url::Url;
use uuid::Uuid;

use crate::{
    api_load::{self, ApiReport},
    budgets::GateBudgets,
    database,
    indexing::{self, IndexingInput, IndexingReport},
};

const CHAIN: &str = "ethereum-mainnet";
const HEAD: i64 = 16;
const CONTRACT: &str = "0x0000000000000000000000000000000000000042";
const SENDER: &str = "0x0000000000000000000000000000000000000043";
const NORMALIZER: &str = "ensip15@ens-normalize-0.1.1";

sol! {
    event NameRegistered(
        string name,
        bytes32 indexed label,
        address indexed owner,
        uint256 expires
    );
}

#[derive(Debug, Serialize)]
pub struct SmokeReport {
    pub indexing: IndexingReport,
    pub api: ApiReport,
    pub green: bool,
}

pub async fn run(api_binary: &Path, budgets: &GateBudgets) -> Result<SmokeReport> {
    ensure!(
        api_binary.is_file(),
        "API binary {} does not exist",
        api_binary.display()
    );
    let scratch = TestDatabase::create(
        TestDatabaseConfig::new("benchmark_gate_smoke").pool_max_connections(20),
    )
    .await?;
    let scratch_url = scratch_database_url(scratch.database_name())?;

    let result = run_in_scratch(&scratch_url, scratch.pool(), api_binary, budgets).await;
    scratch
        .cleanup()
        .await
        .context("failed to clean benchmark smoke database")?;
    result
}

async fn run_in_scratch(
    scratch_url: &str,
    bootstrap_pool: &PgPool,
    api_binary: &Path,
    budgets: &GateBudgets,
) -> Result<SmokeReport> {
    initialize_schema_v2(bootstrap_pool).await?;
    let writer = smoke_writer_pool(scratch_url).await?;
    seed_fixture(&writer).await?;

    let indexing = indexing::run(
        &writer,
        &IndexingInput {
            chain_id: CHAIN.to_owned(),
            head_block: HEAD,
            walk_from_block: 1,
            walk_to_block: HEAD,
            hydration_rpc_urls: None,
        },
        budgets,
    )
    .await?;
    seed_serving_state(&writer).await?;

    let (api_addr, metrics_addr) = reserve_addresses().await?;
    let mut api = spawn_api(api_binary, scratch_url, &api_addr, &metrics_addr)?;
    wait_for_api(&api_addr, &mut api).await?;
    let reader = database::connect_read_only(scratch_url, 8).await?;
    let api_report = api_load::run(&reader, &format!("http://{api_addr}"), budgets).await;
    reader.close().await;
    stop_child(&mut api).await;
    writer.close().await;
    let api = api_report?;
    Ok(SmokeReport {
        green: indexing.green && api.green,
        indexing,
        api,
    })
}

async fn smoke_writer_pool(database_url: &str) -> Result<PgPool> {
    let options = PgConnectOptions::from_str(database_url)?
        .application_name("bigname-benchmark-gate-smoke")
        .options([("search_path", "bigname_phase")]);
    PgPoolOptions::new()
        .max_connections(12)
        .connect_with(options)
        .await
        .context("failed to connect to smoke database phase schema")
}

async fn initialize_schema_v2(pool: &PgPool) -> Result<()> {
    const BASELINE: &[&str] = &[
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
        include_str!("../../../schema-v2/baseline/11_manifest_authority_attestations.sql"),
    ];
    let mut transaction = pool.begin().await?;
    sqlx::query("CREATE SCHEMA bigname_phase")
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SET LOCAL search_path TO bigname_phase, public")
        .execute(&mut *transaction)
        .await?;
    for source in BASELINE {
        sqlx::raw_sql(source).execute(&mut *transaction).await?;
    }
    transaction.commit().await?;
    Ok(())
}

async fn seed_fixture(pool: &PgPool) -> Result<()> {
    sqlx::query(
        "INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number, block_timestamp, canonicality_state)
         SELECT $1, $1 || '-block-' || number,
                CASE WHEN number = 0 THEN NULL ELSE $1 || '-block-' || (number - 1) END,
                number, to_timestamp(1700000000 + number), 'canonical'::canonicality_state
         FROM generate_series(0, $2::bigint) AS number",
    )
    .bind(CHAIN)
    .bind(HEAD)
    .execute(pool)
    .await?;

    let instance_id = Uuid::new_v4();
    sqlx::query("INSERT INTO contract_instances VALUES ($1, $2, 'contract', '{}'::jsonb, now())")
        .bind(instance_id)
        .bind(CHAIN)
        .execute(pool)
        .await?;
    let payload = json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": "ens_v1_registrar_l1",
        "chain": CHAIN,
        "deployment_epoch": "benchmark-smoke",
        "rollout_status": "active",
        "normalizer_version": NORMALIZER,
        "capability_flags": {},
        "roots": [],
        "contracts": [{
            "role": "registrar",
            "address": CONTRACT,
            "proxy_kind": "none",
            "implementation": null,
            "start_block": 0
        }],
        "discovery_rules": [],
        "abi": {"events": [{
            "name": "NameRegistered",
            "fragment": "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
            "emitter_roles": ["registrar"],
            "normalized_events": ["RegistrationGranted"]
        }], "calls": []}
    });
    let manifest_id: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions (
             manifest_version, namespace, source_family, chain_id, deployment_label,
             rollout_status, normalizer_version, file_path, manifest_payload
         ) VALUES (1, 'ens', 'ens_v1_registrar_l1', $1, 'benchmark-smoke', 'active', $2, $3, $4)
         RETURNING manifest_id",
    )
    .bind(CHAIN)
    .bind(NORMALIZER)
    .bind("benchmarks/smoke-ens-v1-registrar.toml")
    .bind(payload)
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "INSERT INTO manifest_contract_instances (
             manifest_id, chain_id, declaration_kind, declaration_name,
             contract_instance_id, declared_address, role, proxy_kind, start_block_number
         ) VALUES ($1, $2, 'contract', 'registrar', $3, $4, 'registrar', 'none', 0)",
    )
    .bind(manifest_id)
    .bind(CHAIN)
    .bind(instance_id)
    .bind(CONTRACT)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO contract_instance_addresses (
             contract_instance_id, chain_id, address, active_from_block_number,
             source_manifest_id, provenance
         ) VALUES ($1, $2, $3, 0, $4, '{}'::jsonb)",
    )
    .bind(instance_id)
    .bind(CHAIN)
    .bind(CONTRACT)
    .bind(manifest_id)
    .execute(pool)
    .await?;

    for block in 1..=HEAD {
        insert_registration(pool, block).await?;
    }
    Ok(())
}

async fn insert_registration(pool: &PgPool, block: i64) -> Result<()> {
    let transaction_hash = format!("{CHAIN}-transaction-{block}");
    sqlx::query(
        "INSERT INTO raw_transactions (
             chain_id, block_hash, block_number, transaction_hash, transaction_index,
             from_address, to_address
         ) VALUES ($1, $2, $3, $4, 0, $5, $6)",
    )
    .bind(CHAIN)
    .bind(block_hash(block))
    .bind(block)
    .bind(&transaction_hash)
    .bind(SENDER)
    .bind(CONTRACT)
    .execute(pool)
    .await?;
    let label = format!("bench{block:04}");
    let mut owner_bytes = [0u8; 20];
    owner_bytes[12..].copy_from_slice(&(block as u64).to_be_bytes());
    let encoded = NameRegistered {
        name: label.clone(),
        label: B256::from(keccak256(label.as_bytes())),
        owner: Address::from(owner_bytes),
        expires: U256::from(2_000_000_000u64 + block as u64),
    }
    .encode_log_data();
    let topics = encoded
        .topics()
        .iter()
        .map(|topic| format!("{topic:#x}"))
        .collect::<Vec<_>>();
    sqlx::query(
        "INSERT INTO raw_logs (
             chain_id, block_hash, block_number, transaction_hash, transaction_index,
             log_index, emitting_address, topics, data
         ) VALUES ($1, $2, $3, $4, 0, 0, $5, $6, $7)",
    )
    .bind(CHAIN)
    .bind(block_hash(block))
    .bind(block)
    .bind(transaction_hash)
    .bind(CONTRACT)
    .bind(topics)
    .bind(encoded.data.to_vec())
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_serving_state(pool: &PgPool) -> Result<()> {
    // API fixture rows use the canonical UTC-seconds spelling required by snapshot tokens.
    // The phase timings above finish before this smoke-only fixture normalization runs.
    for table in ["name_current", "record_inventory_current"] {
        sqlx::query(&format!(
            "UPDATE {table}
             SET chain_positions = jsonb_set(
                 chain_positions,
                 '{{ethereum,timestamp}}',
                 to_jsonb(to_char(
                     (chain_positions #>> '{{ethereum,timestamp}}')::timestamptz AT TIME ZONE 'UTC',
                     'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'
                 ))
             )
             WHERE chain_positions #>> '{{ethereum,timestamp}}' IS NOT NULL"
        ))
        .execute(pool)
        .await?;
    }
    sqlx::query(
        "INSERT INTO chain_heads (chain_id, latest_block_hash, latest_block_number)
         VALUES ($1, $2, $3)",
    )
    .bind(CHAIN)
    .bind(block_hash(HEAD))
    .bind(HEAD)
    .execute(pool)
    .await?;
    for phase in ["ingest", "interpret", "project"] {
        let input_hash =
            (phase != "ingest").then_some(bigname_content_hash::INTERPRETER_CONTENT_HASH);
        sqlx::query(
            "INSERT INTO chain_phase_state (
                 chain_id, phase_name, phase_status, current_block_number, current_block_hash,
                 target_block_number, target_block_hash, input_content_hash, started_at, finished_at
             ) VALUES ($1, $2, 'completed', $3, $4, $3, $4, $5, now(), now())",
        )
        .bind(CHAIN)
        .bind(phase)
        .bind(HEAD)
        .bind(block_hash(HEAD))
        .bind(input_hash)
        .execute(pool)
        .await?;
    }
    Ok(())
}

fn scratch_database_url(database_name: &str) -> Result<String> {
    let mut url =
        Url::parse(&database_url_from_env()).context("failed to parse test database URL")?;
    url.set_path(&format!("/{database_name}"));
    Ok(url.into())
}

async fn reserve_addresses() -> Result<(String, String)> {
    let api = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let metrics = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let api_addr = api.local_addr()?.to_string();
    let metrics_addr = metrics.local_addr()?.to_string();
    drop(api);
    drop(metrics);
    Ok((api_addr, metrics_addr))
}

fn spawn_api(
    api_binary: &Path,
    database_url: &str,
    bind_addr: &str,
    metrics_addr: &str,
) -> Result<Child> {
    Command::new(api_binary)
        .arg("serve")
        .arg("--bind-addr")
        .arg(bind_addr)
        .arg("--metrics-bind-addr")
        .arg(metrics_addr)
        .arg("--database-url")
        .arg(database_url)
        .arg("--max-connections")
        .arg("20")
        .env("BIGNAME_API_MAX_IN_FLIGHT", "256")
        .env("BIGNAME_API_HEALTH_MAX_IN_FLIGHT", "16")
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .context("failed to start API for benchmark smoke run")
}

async fn wait_for_api(bind_addr: &str, child: &mut Child) -> Result<()> {
    let client = reqwest::Client::new();
    let health = format!("http://{bind_addr}/healthz");
    for _ in 0..100 {
        if let Some(status) = child.try_wait()? {
            anyhow::bail!("smoke API exited before it was ready: {status}");
        }
        if client
            .get(&health)
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    anyhow::bail!("smoke API did not become healthy at {health}")
}

async fn stop_child(child: &mut Child) {
    let _ = child.start_kill();
    let _ = child.wait().await;
}

fn block_hash(number: i64) -> String {
    format!("{CHAIN}-block-{number}")
}
