#[allow(dead_code)]
mod support;

use std::{sync::Arc, time::Duration};

use alloy_primitives::{Address, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use anyhow::{Context, Result};
use axum::{Json, Router, extract::State, routing::post};
use bigname_ingest::{BatchRequest, Engine, ErrorKind as IngestErrorKind, SourceDescriptor};
use phase_runner::{
    capacity::CapacityGuard,
    config::{CapacityConfig, ChainConfig, SeedBasis, SourceConfig, TimingConfig},
    ingest_phase::IngestPhase,
    interpret_phase::InterpretPhase,
    phase::{LoopbackPhase, PhaseName, PhaseSet},
    runner::PhaseRunner,
};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use support::ScratchDatabase;

const BLOCK_0: &str = "0x0000000000000000000000000000000000000000000000000000000000000001";
const BLOCK_1: &str = "0x0000000000000000000000000000000000000000000000000000000000000002";
const BLOCK_2: &str = "0x0000000000000000000000000000000000000000000000000000000000000007";
const TRANSACTION: &str = "0x0000000000000000000000000000000000000000000000000000000000000003";
const ANNOUNCEMENT_TRANSACTION: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000008";
const REGISTRATION_TRANSACTION: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000009";
const CONTRACT: &str = "0x0000000000000000000000000000000000000004";
const SENDER: &str = "0x0000000000000000000000000000000000000005";
const SIBLING_CONTRACT: &str = "0x0000000000000000000000000000000000000006";
const ANNOUNCED_REGISTRY: &str = "0x0000000000000000000000000000000000000045";
const TRANSFER_TOPIC: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const SIBLING_TOPIC: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const NORMALIZER: &str = "ensip15@ens-normalize-0.1.1";

sol! {
    event RegistryCreated();
    event LabelRegistered(
        uint256 indexed tokenId,
        bytes32 indexed labelHash,
        string label,
        address owner,
        uint64 expiry,
        address indexed sender
    );
}

#[tokio::test]
async fn production_ingest_writes_raw_facts_cursors_heads_and_handoff() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_production_ingest").await?;
    let chain_id = "rpc-ingest-test";
    seed_watch_set(scratch.pool(), chain_id).await?;
    let (endpoint, server) = spawn_rpc(true, false).await?;
    let configured_chain = ChainConfig::new(
        chain_id,
        vec![SourceConfig::new(
            chain_id,
            "rpc",
            "rpc",
            SeedBasis::NewSignatureRange,
            0,
            endpoint,
        )?],
        false,
    )?;
    let database = scratch.runner();
    let phases = PhaseSet::with_ingest(Arc::new(IngestPhase::new(database.pool().clone())))?;
    let runner = PhaseRunner::new(
        database,
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "production-ingest-test",
        TimingConfig {
            initial_backoff: Duration::from_millis(1),
            maximum_backoff: Duration::from_millis(4),
            live_poll_interval: Duration::from_millis(1),
        },
    )?;
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let mut task =
        tokio::spawn(async move { runner.run_chain(&configured_chain, task_cancellation).await });

    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let phase_state: Option<(String, Option<String>)> = sqlx::query_as(
                "
                SELECT phase_status, last_error
                FROM chain_phase_state
                WHERE chain_id = $1
                  AND phase_name = 'ingest'
                ",
            )
            .bind(chain_id)
            .fetch_optional(scratch.pool())
            .await?;
            match phase_state {
                Some((status, _)) if status == "completed" => {
                    return Ok::<_, anyhow::Error>(());
                }
                Some((status, reason)) if status == "failed" => {
                    anyhow::bail!(
                        "production ingest failed: {}",
                        reason.unwrap_or_else(|| "no failure reason".to_owned())
                    );
                }
                _ => {}
            }
            if task.is_finished() {
                let result = (&mut task)
                    .await
                    .context("production ingest task panicked")?;
                result.context("production ingest task exited before completion")?;
                anyhow::bail!("production ingest task exited before ingest completed");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("production ingest did not complete")??;
    cancellation.cancel();
    task.await??;
    server.abort();

    let lineage_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM chain_lineage WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(scratch.pool())
            .await?;
    let raw_log_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM raw_logs WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(scratch.pool())
            .await?;
    let raw_logs: Vec<(String, Vec<u8>)> = sqlx::query_as(
        "
        SELECT emitting_address, data
        FROM raw_logs
        WHERE chain_id = $1
        ORDER BY log_index
        ",
    )
    .bind(chain_id)
    .fetch_all(scratch.pool())
    .await?;
    let transaction: (Vec<u8>, String) = sqlx::query_as(
        "
        SELECT input, value::text
        FROM raw_transactions
        WHERE chain_id = $1
        ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    let cursor: (i64, Option<i64>, Option<i64>) = sqlx::query_as(
        "
        SELECT next_block_number, target_block_number, last_processed_block_number
        FROM ingest_cursors
        WHERE chain_id = $1
          AND source_key = 'rpc'
        ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    let state: (Option<i64>, Option<i64>) = sqlx::query_as(
        "
        SELECT current_block_number, live_handoff_block_number
        FROM chain_phase_state
        WHERE chain_id = $1
          AND phase_name = 'ingest'
        ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    let head: (
        i64,
        String,
        Option<i64>,
        Option<String>,
        Option<i64>,
        Option<String>,
    ) = sqlx::query_as(
        "
            SELECT latest_block_number,
                   latest_block_hash,
                   safe_block_number,
                   safe_block_hash,
                   finalized_block_number,
                   finalized_block_hash
            FROM chain_heads
            WHERE chain_id = $1
            ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    let finalized_lineage_count: i64 = sqlx::query_scalar(
        "
        SELECT count(*)
        FROM chain_lineage
        WHERE chain_id = $1
          AND canonicality_state = 'finalized'
        ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;

    assert_eq!(lineage_count, 2);
    assert_eq!(raw_log_count, 2);
    assert_eq!(
        raw_logs,
        vec![
            (CONTRACT.to_owned(), Vec::new()),
            (SIBLING_CONTRACT.to_owned(), vec![0x12, 0x34]),
        ]
    );
    assert_eq!(transaction, (vec![0xde, 0xad], "7".to_owned()));
    assert_eq!(cursor, (2, Some(1), Some(1)));
    assert_eq!(state, (Some(1), Some(1)));
    assert_eq!(
        head,
        (
            1,
            BLOCK_1.to_owned(),
            Some(1),
            Some(BLOCK_1.to_owned()),
            Some(1),
            Some(BLOCK_1.to_owned()),
        )
    );
    assert_eq!(finalized_lineage_count, 2);
    scratch.cleanup().await
}

#[tokio::test]
async fn cold_catch_up_fetches_events_after_registry_announcement() -> Result<()> {
    let scratch = ScratchDatabase::create("production_ingest_registry_announcement").await?;
    let chain_id = "rpc-registry-announcement-test";
    seed_announcement_watch_set(scratch.pool(), chain_id).await?;
    let (endpoint, server) = spawn_announcement_rpc().await?;
    let configured_chain = ChainConfig::new(
        chain_id,
        vec![SourceConfig::new(
            chain_id,
            "rpc",
            "rpc",
            SeedBasis::NewSignatureRange,
            0,
            endpoint,
        )?],
        true,
    )?;
    let database = scratch.runner();
    let phases = PhaseSet::new([
        Arc::new(IngestPhase::new(database.pool().clone())),
        Arc::new(InterpretPhase::new(database.pool().clone())),
        Arc::new(LoopbackPhase::new(PhaseName::Project)),
        Arc::new(LoopbackPhase::new(PhaseName::Verify)),
        Arc::new(LoopbackPhase::new(PhaseName::Live)),
    ])?;
    let runner = PhaseRunner::new(
        database,
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "registry-announcement-catch-up-test",
        TimingConfig {
            initial_backoff: Duration::from_millis(1),
            maximum_backoff: Duration::from_millis(4),
            live_poll_interval: Duration::from_millis(1),
        },
    )?;
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let task =
        tokio::spawn(async move { runner.run_chain(&configured_chain, task_cancellation).await });

    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let status: Option<String> = sqlx::query_scalar(
                "
                SELECT phase_status
                FROM chain_phase_state
                WHERE chain_id = $1
                  AND phase_name = 'interpret'
                ",
            )
            .bind(chain_id)
            .fetch_optional(scratch.pool())
            .await?;
            if status.as_deref() == Some("completed") {
                return Ok::<_, anyhow::Error>(());
            }
            if task.is_finished() {
                anyhow::bail!("phase runner exited before registry interpretation completed");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("registry announcement catch-up did not complete")??;
    cancellation.cancel();
    task.await??;
    server.abort();

    let raw_logs: Vec<(i64, String)> = sqlx::query_as(
        "
        SELECT block_number, emitting_address
        FROM raw_logs
        WHERE chain_id = $1
        ORDER BY block_number, log_index
        ",
    )
    .bind(chain_id)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        raw_logs,
        [
            (1, ANNOUNCED_REGISTRY.to_owned()),
            (2, ANNOUNCED_REGISTRY.to_owned()),
        ],
        "cold intake must retain the announced registry's later event"
    );
    let event_count: i64 = sqlx::query_scalar(
        "
        SELECT count(*)
        FROM normalized_events
        WHERE chain_id = $1
          AND event_kind = 'RegistrationGranted'
        ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(event_count, 1);
    let announcement: (String, Uuid, Uuid, i64) = sqlx::query_as(
        "
        SELECT edge_kind,
               from_contract_instance_id,
               to_contract_instance_id,
               active_from_block_number
        FROM discovery_edges
        WHERE chain_id = $1
          AND edge_kind = 'registry_announcement'
          AND deactivated_at IS NULL
        ",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(announcement.0, "registry_announcement");
    assert_eq!(announcement.1, announcement.2);
    assert_eq!(announcement.3, 1);

    let registered_topic = format!("{:#x}", LabelRegistered::SIGNATURE_HASH);
    let watch = bigname_ingest::load_watch_filter(scratch.pool(), chain_id, 0, 5).await?;
    assert!(!watch.includes(ANNOUNCED_REGISTRY, &registered_topic, 0));
    assert!(watch.includes(ANNOUNCED_REGISTRY, &registered_topic, 1));
    assert!(watch.includes(ANNOUNCED_REGISTRY, &registered_topic, 5));
    scratch.cleanup().await
}

#[tokio::test]
async fn ingest_rejects_a_provider_without_checkpoint_heads() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_ingest_missing_checkpoints").await?;
    let chain_id = "rpc-missing-checkpoints-test";
    seed_watch_set(scratch.pool(), chain_id).await?;
    let (endpoint, server) = spawn_rpc(false, false).await?;
    let outcome = Engine::new(scratch.pool().clone())
        .run_batch(BatchRequest {
            chain_id: chain_id.to_owned(),
            sources: vec![SourceDescriptor {
                key: "rpc".to_owned(),
                kind: "rpc".to_owned(),
                start_block: 0,
                endpoint,
            }],
            cursors: Vec::new(),
            redo_range: None,
            resume_current: None,
        })
        .await;
    server.abort();

    let error = outcome.expect_err("ingest must require safe and finalized checkpoints");
    assert_eq!(error.kind(), IngestErrorKind::DataIntegrity);
    assert!(error.to_string().contains("checkpoint"));
    scratch.cleanup().await
}

#[tokio::test]
async fn block_hash_pinned_log_mismatch_is_terminal_data_integrity() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_block_hash_log_mismatch").await?;
    let chain_id = "rpc-block-hash-log-mismatch-test";
    seed_watch_set(scratch.pool(), chain_id).await?;
    let (endpoint, server) = spawn_rpc(true, true).await?;
    let outcome = Engine::new(scratch.pool().clone())
        .run_batch(BatchRequest {
            chain_id: chain_id.to_owned(),
            sources: vec![SourceDescriptor {
                key: "rpc".to_owned(),
                kind: "rpc".to_owned(),
                start_block: 0,
                endpoint,
            }],
            cursors: Vec::new(),
            redo_range: None,
            resume_current: None,
        })
        .await;
    server.abort();

    let error = outcome.expect_err("blockHash-pinned log mismatch must fail ingest");
    assert_eq!(error.kind(), IngestErrorKind::DataIntegrity);
    assert!(error.to_string().contains("outside blockHash-pinned block"));
    scratch.cleanup().await
}

async fn seed_watch_set(pool: &sqlx::PgPool, chain_id: &str) -> Result<()> {
    let contract_id = Uuid::new_v4();
    sqlx::query(
        "
        INSERT INTO contract_instances (
            contract_instance_id, chain_id, contract_kind, provenance
        )
        VALUES ($1, $2, 'contract', '{}'::jsonb)
        ",
    )
    .bind(contract_id)
    .bind(chain_id)
    .execute(pool)
    .await?;
    let payload = json!({
        "manifest_version": 1,
        "namespace": "test",
        "source_family": "test_events",
        "chain": chain_id,
        "deployment_epoch": "test",
        "rollout_status": "active",
        "normalizer_version": "test",
        "capability_flags": {},
        "roots": [],
        "contracts": [],
        "discovery_rules": [],
        "abi": {
            "events": [{
                "name": "Transfer",
                "fragment": "event Transfer(address indexed from,address indexed to,uint256 value)",
                "emitter_roles": [],
                "normalized_events": []
            }]
        }
    });
    let manifest_id: i64 = sqlx::query_scalar(
        "
        INSERT INTO manifest_versions (
            manifest_version,
            namespace,
            source_family,
            chain_id,
            deployment_label,
            rollout_status,
            normalizer_version,
            file_path,
            manifest_payload
        )
        VALUES (1, 'test', 'test_events', $1, 'test', 'active', 'test', $2, $3::jsonb)
        RETURNING manifest_id
        ",
    )
    .bind(chain_id)
    .bind(format!("tests/{chain_id}.toml"))
    .bind(payload.to_string())
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "
        INSERT INTO manifest_contract_instances (
            manifest_id,
            chain_id,
            declaration_kind,
            declaration_name,
            contract_instance_id,
            declared_address,
            role,
            proxy_kind
        )
        VALUES ($1, $2, 'contract', 'test', $3, $4, 'test', 'none')
        ",
    )
    .bind(manifest_id)
    .bind(chain_id)
    .bind(contract_id)
    .bind(CONTRACT)
    .execute(pool)
    .await?;
    sqlx::query(
        "
        INSERT INTO contract_instance_addresses (
            contract_instance_id,
            chain_id,
            address,
            active_from_block_number,
            source_manifest_id,
            provenance
        )
        VALUES ($1, $2, $3, 0, $4, '{}'::jsonb)
        ",
    )
    .bind(contract_id)
    .bind(chain_id)
    .bind(CONTRACT)
    .bind(manifest_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_announcement_watch_set(pool: &sqlx::PgPool, chain_id: &str) -> Result<()> {
    let anchor_id = Uuid::new_v4();
    sqlx::query(
        "
        INSERT INTO contract_instances (
            contract_instance_id, chain_id, contract_kind, provenance
        )
        VALUES ($1, $2, 'contract', '{}'::jsonb)
        ",
    )
    .bind(anchor_id)
    .bind(chain_id)
    .execute(pool)
    .await?;
    let payload = json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": "ens_v2_registry_l1",
        "chain": chain_id,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": NORMALIZER,
        "capability_flags": {},
        "roots": [],
        "contracts": [{
            "role": "registry",
            "address": CONTRACT,
            "proxy_kind": "none",
            "implementation": null,
            "start_block": 0
        }],
        "discovery_rules": [{
            "edge_kind": "registry_announcement",
            "from_role": "registry",
            "admission": "reachable_from_root"
        }],
        "abi": { "events": [
            {
                "name": "RegistryCreated",
                "fragment": "event RegistryCreated()",
                "emitter_roles": [],
                "normalized_events": ["RegistryCreated"]
            },
            {
                "name": "LabelRegistered",
                "fragment": "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)",
                "emitter_roles": ["registry"],
                "normalized_events": ["RegistrationGranted"]
            }
        ], "calls": [] }
    });
    let manifest_id: i64 = sqlx::query_scalar(
        "
        INSERT INTO manifest_versions (
            manifest_version, namespace, source_family, chain_id,
            deployment_label, rollout_status, normalizer_version,
            file_path, manifest_payload
        )
        VALUES (1, 'ens', 'ens_v2_registry_l1', $1, 'fixture',
                'active', $2, $3, $4)
        RETURNING manifest_id
        ",
    )
    .bind(chain_id)
    .bind(NORMALIZER)
    .bind(format!("tests/{chain_id}.toml"))
    .bind(payload)
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "
        INSERT INTO manifest_contract_instances (
            manifest_id, chain_id, declaration_kind, declaration_name,
            contract_instance_id, declared_address, role, proxy_kind,
            start_block_number
        )
        VALUES ($1, $2, 'contract', 'registry', $3, $4,
                'registry', 'none', 0)
        ",
    )
    .bind(manifest_id)
    .bind(chain_id)
    .bind(anchor_id)
    .bind(CONTRACT)
    .execute(pool)
    .await?;
    sqlx::query(
        "
        INSERT INTO contract_instance_addresses (
            contract_instance_id, chain_id, address,
            active_from_block_number, source_manifest_id, provenance
        )
        VALUES ($1, $2, $3, 0, $4, '{}'::jsonb)
        ",
    )
    .bind(anchor_id)
    .bind(chain_id)
    .bind(CONTRACT)
    .bind(manifest_id)
    .execute(pool)
    .await?;
    sqlx::query(
        "
        INSERT INTO manifest_discovery_rules (
            manifest_id, edge_kind, from_role, admission
        )
        VALUES ($1, 'registry_announcement', 'registry', 'reachable_from_root')
        ",
    )
    .bind(manifest_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn spawn_announcement_rpc() -> Result<(String, tokio::task::JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        axum::serve(listener, Router::new().route("/", post(announcement_rpc)))
            .await
            .expect("announcement test RPC server");
    });
    Ok((format!("http://{address}/"), server))
}

async fn announcement_rpc(Json(request): Json<Value>) -> Json<Value> {
    if let Some(requests) = request.as_array() {
        return Json(Value::Array(
            requests
                .iter()
                .map(announcement_rpc_response)
                .collect::<Vec<_>>(),
        ));
    }
    Json(announcement_rpc_response(&request))
}

fn announcement_rpc_response(request: &Value) -> Value {
    let id = request.get("id").cloned().unwrap_or(json!(1));
    let method = request["method"].as_str().unwrap_or_default();
    let params = request["params"].as_array().cloned().unwrap_or_default();
    let result = match method {
        "eth_getBlockByNumber" => {
            let selection = params.first().and_then(Value::as_str).unwrap_or_default();
            match selection {
                "latest" | "safe" | "finalized" | "0x2" => Some(announcement_block(
                    2,
                    params.get(1) == Some(&Value::Bool(true)),
                )),
                "0x1" => Some(announcement_block(
                    1,
                    params.get(1) == Some(&Value::Bool(true)),
                )),
                "0x0" => Some(announcement_block(
                    0,
                    params.get(1) == Some(&Value::Bool(true)),
                )),
                _ => None,
            }
        }
        "eth_getBlockByHash" => {
            let full = params.get(1) == Some(&Value::Bool(true));
            match params.first().and_then(Value::as_str).unwrap_or_default() {
                BLOCK_0 => Some(announcement_block(0, full)),
                BLOCK_1 => Some(announcement_block(1, full)),
                BLOCK_2 => Some(announcement_block(2, full)),
                _ => None,
            }
        }
        "eth_getLogs" => {
            let filter = params.first().cloned().unwrap_or_default();
            match filter.get("blockHash").and_then(Value::as_str) {
                Some(BLOCK_0) => Some(json!([])),
                Some(BLOCK_1) => Some(json!([announcement_log()])),
                Some(BLOCK_2) => Some(json!([registration_log()])),
                Some(_) => None,
                None => Some(Value::Array(announcement_range_logs(&filter))),
            }
        }
        "eth_getBlockReceipts" => {
            match params.first().and_then(Value::as_str).unwrap_or_default() {
                BLOCK_0 => Some(json!([])),
                BLOCK_1 => Some(json!([announcement_receipt(1, ANNOUNCEMENT_TRANSACTION)])),
                BLOCK_2 => Some(json!([announcement_receipt(2, REGISTRATION_TRANSACTION)])),
                _ => None,
            }
        }
        _ => None,
    };
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn announcement_block(number: i64, full_transactions: bool) -> Value {
    let (hash, parent_hash, transaction_hash) = match number {
        0 => (
            BLOCK_0,
            "0x0000000000000000000000000000000000000000000000000000000000000000",
            None,
        ),
        1 => (BLOCK_1, BLOCK_0, Some(ANNOUNCEMENT_TRANSACTION)),
        _ => (BLOCK_2, BLOCK_1, Some(REGISTRATION_TRANSACTION)),
    };
    let transactions = transaction_hash.map_or_else(
        || json!([]),
        |transaction_hash| {
            if full_transactions {
                json!([{
                    "hash": transaction_hash,
                    "blockHash": hash,
                    "blockNumber": format!("0x{number:x}"),
                    "transactionIndex": "0x0",
                    "from": SENDER,
                    "to": ANNOUNCED_REGISTRY,
                    "input": "0x",
                    "value": "0x0"
                }])
            } else {
                json!([transaction_hash])
            }
        },
    );
    json!({
        "hash": hash,
        "parentHash": parent_hash,
        "number": format!("0x{number:x}"),
        "timestamp": format!("0x{:x}", number + 200),
        "logsBloom": "0x",
        "transactions": transactions
    })
}

fn announcement_range_logs(filter: &Value) -> Vec<Value> {
    let from = rpc_quantity(filter.get("fromBlock")).unwrap_or_default();
    let to = rpc_quantity(filter.get("toBlock")).unwrap_or(i64::MAX);
    let addresses = filter
        .get("address")
        .map(string_filter_values)
        .unwrap_or_default();
    let topics = filter
        .pointer("/topics/0")
        .map(string_filter_values)
        .unwrap_or_default();
    [announcement_log(), registration_log()]
        .into_iter()
        .filter(|log| {
            let number = rpc_quantity(log.get("blockNumber")).unwrap_or_default();
            let address = log
                .get("address")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let topic0 = log
                .pointer("/topics/0")
                .and_then(Value::as_str)
                .unwrap_or_default();
            (from..=to).contains(&number)
                && (addresses.is_empty()
                    || addresses
                        .iter()
                        .any(|expected| expected.eq_ignore_ascii_case(address)))
                && (topics.is_empty()
                    || topics
                        .iter()
                        .any(|expected| expected.eq_ignore_ascii_case(topic0)))
        })
        .collect()
}

fn string_filter_values(value: &Value) -> Vec<String> {
    value.as_array().map_or_else(
        || value.as_str().map(str::to_owned).into_iter().collect(),
        |values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        },
    )
}

fn rpc_quantity(value: Option<&Value>) -> Option<i64> {
    i64::from_str_radix(value?.as_str()?.trim_start_matches("0x"), 16).ok()
}

fn announcement_log() -> Value {
    encoded_rpc_log(
        RegistryCreated {}.encode_log_data(),
        1,
        BLOCK_1,
        ANNOUNCEMENT_TRANSACTION,
    )
}

fn registration_log() -> Value {
    encoded_rpc_log(
        LabelRegistered {
            tokenId: U256::from(1),
            labelHash: keccak256(b"alice"),
            label: "alice".to_owned(),
            owner: SENDER.parse::<Address>().expect("valid fixture owner"),
            expiry: 10_000,
            sender: SENDER.parse::<Address>().expect("valid fixture sender"),
        }
        .encode_log_data(),
        2,
        BLOCK_2,
        REGISTRATION_TRANSACTION,
    )
}

fn encoded_rpc_log(
    encoded: alloy_primitives::LogData,
    block_number: i64,
    block_hash: &str,
    transaction_hash: &str,
) -> Value {
    json!({
        "blockHash": block_hash,
        "blockNumber": format!("0x{block_number:x}"),
        "transactionHash": transaction_hash,
        "transactionIndex": "0x0",
        "logIndex": "0x0",
        "address": ANNOUNCED_REGISTRY,
        "topics": encoded
            .topics()
            .iter()
            .map(|topic| format!("{topic:#x}"))
            .collect::<Vec<_>>(),
        "data": format!("0x{}", alloy_primitives::hex::encode(encoded.data))
    })
}

fn announcement_receipt(block_number: i64, transaction_hash: &str) -> Value {
    let block_hash = if block_number == 1 { BLOCK_1 } else { BLOCK_2 };
    json!({
        "transactionHash": transaction_hash,
        "blockHash": block_hash,
        "blockNumber": format!("0x{block_number:x}"),
        "transactionIndex": "0x0",
        "status": "0x1",
        "cumulativeGasUsed": "0x5208",
        "gasUsed": "0x5208",
        "logsBloom": "0x"
    })
}

async fn spawn_rpc(
    checkpoint_support: bool,
    mismatched_block_hash_log: bool,
) -> Result<(String, tokio::task::JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/", post(rpc))
                .with_state((checkpoint_support, mismatched_block_hash_log)),
        )
        .await
        .expect("test RPC server");
    });
    Ok((format!("http://{address}/"), server))
}

async fn rpc(
    State((checkpoint_support, mismatched_block_hash_log)): State<(bool, bool)>,
    Json(request): Json<Value>,
) -> Json<Value> {
    if let Some(requests) = request.as_array() {
        return Json(Value::Array(
            requests
                .iter()
                .map(|request| rpc_response(request, checkpoint_support, mismatched_block_hash_log))
                .collect::<Vec<_>>(),
        ));
    }
    Json(rpc_response(
        &request,
        checkpoint_support,
        mismatched_block_hash_log,
    ))
}

fn rpc_response(
    request: &Value,
    checkpoint_support: bool,
    mismatched_block_hash_log: bool,
) -> Value {
    let id = request.get("id").cloned().unwrap_or(json!(1));
    let method = request["method"].as_str().unwrap_or_default();
    let params = request["params"].as_array().cloned().unwrap_or_default();
    let result = match method {
        "eth_getBlockByNumber" => {
            let selection = params.first().and_then(Value::as_str).unwrap_or_default();
            match selection {
                "latest" | "0x1" => Some(block(1, params.get(1) == Some(&Value::Bool(true)))),
                "0x0" => Some(block(0, params.get(1) == Some(&Value::Bool(true)))),
                "safe" | "finalized" if !checkpoint_support => {
                    return json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "error": {"code": -32602, "message": "unsupported block tag"}
                    });
                }
                "safe" | "finalized" => Some(block(1, false)),
                _ => None,
            }
        }
        "eth_getBlockByHash" => {
            let hash = params.first().and_then(Value::as_str).unwrap_or_default();
            match hash {
                BLOCK_0 => Some(block(0, params.get(1) == Some(&Value::Bool(true)))),
                BLOCK_1 => Some(block(1, params.get(1) == Some(&Value::Bool(true)))),
                _ => None,
            }
        }
        "eth_getLogs" => {
            let filter = params.first().cloned().unwrap_or_default();
            match filter.get("blockHash").and_then(Value::as_str) {
                Some(BLOCK_0) => Some(json!([])),
                Some(BLOCK_1) if mismatched_block_hash_log => {
                    Some(json!([block_hash_mismatched_log()]))
                }
                Some(BLOCK_1) => Some(json!([raw_log(), sibling_log()])),
                _ => Some(json!([raw_log()])),
            }
        }
        "eth_getBlockReceipts" => Some(json!([receipt()])),
        _ => None,
    };
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn block(number: i64, full_transactions: bool) -> Value {
    let (hash, parent_hash, transactions) = if number == 0 {
        (
            BLOCK_0,
            "0x0000000000000000000000000000000000000000000000000000000000000000",
            json!([]),
        )
    } else {
        (
            BLOCK_1,
            BLOCK_0,
            if full_transactions {
                json!([{
                    "hash": TRANSACTION,
                    "blockHash": BLOCK_1,
                    "blockNumber": "0x1",
                    "transactionIndex": "0x0",
                    "from": SENDER,
                    "to": CONTRACT,
                    "input": "0xdead",
                    "value": "0x7"
                }])
            } else {
                json!([TRANSACTION])
            },
        )
    };
    json!({
        "hash": hash,
        "parentHash": parent_hash,
        "number": format!("0x{number:x}"),
        "timestamp": format!("0x{:x}", number + 100),
        "logsBloom": "0x",
        "transactions": transactions
    })
}

fn raw_log() -> Value {
    json!({
        "blockHash": BLOCK_1,
        "blockNumber": "0x1",
        "transactionHash": TRANSACTION,
        "transactionIndex": "0x0",
        "logIndex": "0x0",
        "address": CONTRACT,
        "topics": [
            TRANSFER_TOPIC,
            format!("0x{}", "00".repeat(32)),
            format!("0x{}", "00".repeat(32))
        ],
        "data": "0x"
    })
}

fn block_hash_mismatched_log() -> Value {
    let mut log = raw_log();
    log["blockHash"] = json!(BLOCK_0);
    log
}

fn sibling_log() -> Value {
    json!({
        "blockHash": BLOCK_1,
        "blockNumber": "0x1",
        "transactionHash": TRANSACTION,
        "transactionIndex": "0x0",
        "logIndex": "0x1",
        "address": SIBLING_CONTRACT,
        "topics": [SIBLING_TOPIC],
        "data": "0x1234"
    })
}

fn receipt() -> Value {
    json!({
        "transactionHash": TRANSACTION,
        "blockHash": BLOCK_1,
        "blockNumber": "0x1",
        "transactionIndex": "0x0",
        "status": "0x1",
        "cumulativeGasUsed": "0x5208",
        "gasUsed": "0x5208",
        "logsBloom": "0x"
    })
}
