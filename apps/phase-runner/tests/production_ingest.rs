#[allow(dead_code)]
mod support;

use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result};
use axum::{Json, Router, extract::State, routing::post};
use bigname_ingest::{BatchRequest, Engine, ErrorKind as IngestErrorKind, SourceDescriptor};
use phase_runner::{
    capacity::CapacityGuard,
    config::{CapacityConfig, ChainConfig, SeedBasis, SourceConfig, TimingConfig},
    ingest_phase::IngestPhase,
    phase::PhaseSet,
    runner::PhaseRunner,
};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use support::ScratchDatabase;

const BLOCK_0: &str = "0x0000000000000000000000000000000000000000000000000000000000000001";
const BLOCK_1: &str = "0x0000000000000000000000000000000000000000000000000000000000000002";
const TRANSACTION: &str = "0x0000000000000000000000000000000000000000000000000000000000000003";
const CONTRACT: &str = "0x0000000000000000000000000000000000000004";
const SENDER: &str = "0x0000000000000000000000000000000000000005";
const SIBLING_CONTRACT: &str = "0x0000000000000000000000000000000000000006";
const TRANSFER_TOPIC: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const SIBLING_TOPIC: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[tokio::test]
async fn production_ingest_writes_raw_facts_cursors_heads_and_handoff() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_production_ingest").await?;
    let chain_id = "rpc-ingest-test";
    seed_watch_set(scratch.pool(), chain_id).await?;
    let (endpoint, server) = spawn_rpc(true).await?;
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
            if task.is_finished() {
                let result = (&mut task)
                    .await
                    .context("production ingest task panicked")?;
                result.context("production ingest task exited before completion")?;
                anyhow::bail!("production ingest task exited before ingest completed");
            }
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
async fn ingest_rejects_a_provider_without_checkpoint_heads() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_ingest_missing_checkpoints").await?;
    let chain_id = "rpc-missing-checkpoints-test";
    seed_watch_set(scratch.pool(), chain_id).await?;
    let (endpoint, server) = spawn_rpc(false).await?;
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

async fn spawn_rpc(checkpoint_support: bool) -> Result<(String, tokio::task::JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/", post(rpc))
                .with_state(checkpoint_support),
        )
        .await
        .expect("test RPC server");
    });
    Ok((format!("http://{address}/"), server))
}

async fn rpc(State(checkpoint_support): State<bool>, Json(request): Json<Value>) -> Json<Value> {
    if let Some(requests) = request.as_array() {
        return Json(Value::Array(
            requests
                .iter()
                .map(|request| rpc_response(request, checkpoint_support))
                .collect::<Vec<_>>(),
        ));
    }
    Json(rpc_response(&request, checkpoint_support))
}

fn rpc_response(request: &Value, checkpoint_support: bool) -> Value {
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
