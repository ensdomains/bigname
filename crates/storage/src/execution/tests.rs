use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use serde_json::json;
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};

use super::*;
use crate::default_database_url;

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

struct TestDatabase {
    admin_pool: PgPool,
    pool: PgPool,
    database_name: String,
}

impl TestDatabase {
    async fn new() -> Result<Self> {
        let database_url = std::env::var("BIGNAME_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| default_database_url().to_owned());
        let base_options = PgConnectOptions::from_str(&database_url)
            .context("failed to parse database URL for execution-trace tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bigname_storage_execution_trace_test_{}_{}_{}",
            std::process::id(),
            unique,
            sequence
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for execution-trace tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect execution-trace test pool")?;

        crate::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for execution-trace tests")?;

        Ok(Self {
            admin_pool,
            pool,
            database_name,
        })
    }

    fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn cleanup(self) -> Result<()> {
        self.pool.close().await;
        sqlx::query(&format!(
            r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#,
            self.database_name
        ))
        .execute(&self.admin_pool)
        .await
        .with_context(|| format!("failed to drop test database {}", self.database_name))?;
        self.admin_pool.close().await;
        Ok(())
    }
}

fn timestamp(seconds: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(seconds).expect("test timestamp must be valid")
}

fn execution_trace() -> ExecutionTrace {
    ExecutionTrace {
        execution_trace_id: Uuid::from_u128(0x0e7ec7ace00000000000000000000001),
        request_type: "verified_resolution".to_owned(),
        request_key: "ens:alice.eth:addr:60".to_owned(),
        namespace: "ens".to_owned(),
        chain_context: json!({
            "requested_positions": [
                {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 21_000_000,
                    "block_hash": "0xabc123"
                }
            ],
            "topology_version_boundary": {
                "ethereum-mainnet": 21_000_000
            }
        }),
        manifest_context: json!({
            "manifest_versions": [
                {
                    "source_manifest_id": 7,
                    "manifest_version": 3
                }
            ],
            "rollout_boundary": 3
        }),
        contracts_called: json!([
            {
                "chain_id": "ethereum-mainnet",
                "contract_address": "0x0000000000000000000000000000000000000abc",
                "selector": "0x9061b923"
            }
        ]),
        gateway_digests: json!([
            {
                "digest": "sha256:feedface",
                "content_type": "application/json"
            }
        ]),
        final_payload: Some(json!({
            "record_kind": "addr",
            "coin_type": 60,
            "value": "0x00000000000000000000000000000000000000aa"
        })),
        failure_payload: None,
        request_metadata: json!({
            "surface": "alice.eth",
            "normalizer_version": "nfkc-v1"
        }),
        finished_at: Some(timestamp(1_717_171_717)),
        steps: vec![
            ExecutionTraceStep {
                step_index: 0,
                step_kind: "load_declared_topology".to_owned(),
                input_digest: Some("sha256:topology-input".to_owned()),
                output_digest: Some("sha256:topology-output".to_owned()),
                latency_ms: Some(4),
                canonicality_dependency: json!({
                    "ethereum-mainnet": {
                        "block_hash": "0xabc123",
                        "block_number": 21_000_000,
                        "state": "canonical"
                    }
                }),
                step_payload: json!({
                    "entrypoint": "universal_resolver",
                    "resolver": "0x0000000000000000000000000000000000000abc"
                }),
            },
            ExecutionTraceStep {
                step_index: 1,
                step_kind: "call_universal_resolver".to_owned(),
                input_digest: Some("sha256:resolver-input".to_owned()),
                output_digest: Some("sha256:resolver-output".to_owned()),
                latency_ms: Some(28),
                canonicality_dependency: json!({
                    "ethereum-mainnet": {
                        "block_hash": "0xabc123",
                        "block_number": 21_000_000,
                        "state": "canonical"
                    }
                }),
                step_payload: json!({
                    "coin_type": 60,
                    "name": "alice.eth",
                    "resolved_address": "0x00000000000000000000000000000000000000aa"
                }),
            },
        ],
    }
}

async fn expect_trace_validation_error(
    database: &TestDatabase,
    trace: &ExecutionTrace,
    expected_message: &str,
) -> Result<()> {
    let error = upsert_execution_trace(database.pool(), trace)
        .await
        .expect_err("execution trace validation must fail");

    assert!(
        error.to_string().contains(expected_message),
        "unexpected error: {error:#}"
    );
    assert!(
        load_execution_trace(database.pool(), trace.execution_trace_id)
            .await?
            .is_none(),
        "invalid execution trace {} must not be written",
        trace.execution_trace_id
    );

    Ok(())
}

fn version_boundary(
    logical_name_id: &str,
    resource_id: Uuid,
    normalized_event_id: Option<i64>,
    event_kind: Option<&str>,
    block_number: i64,
    block_hash: &str,
    timestamp: &str,
) -> serde_json::Value {
    json!({
        "logical_name_id": logical_name_id,
        "resource_id": resource_id.to_string(),
        "normalized_event_id": normalized_event_id,
        "event_kind": event_kind,
        "chain_position": {
            "chain_id": "ethereum-mainnet",
            "block_number": block_number,
            "block_hash": block_hash,
            "timestamp": timestamp,
        }
    })
}

fn execution_trace_variant(
    execution_trace_id: Uuid,
    request_key: &str,
    finished_at: i64,
) -> ExecutionTrace {
    let mut trace = execution_trace();
    trace.execution_trace_id = execution_trace_id;
    trace.request_key = request_key.to_owned();
    trace.request_metadata = json!({
        "surface": request_key,
        "normalizer_version": "nfkc-v1"
    });
    trace.finished_at = Some(timestamp(finished_at));
    trace
}

fn execution_outcome_variant(
    trace: &ExecutionTrace,
    manifest_versions: serde_json::Value,
    topology_version_boundary: serde_json::Value,
    record_version_boundary: serde_json::Value,
) -> ExecutionOutcome {
    let mut outcome = execution_outcome(trace);
    outcome.cache_key.request_key = trace.request_key.clone();
    outcome.cache_key.manifest_versions = manifest_versions;
    outcome.cache_key.topology_version_boundary = topology_version_boundary;
    outcome.cache_key.record_version_boundary = record_version_boundary;
    outcome.finished_at = trace
        .finished_at
        .expect("execution trace variant fixture must finish");
    outcome
}

fn verified_primary_request_key(address: &str, coin_type: &str) -> String {
    format!("ens:{}:{coin_type}", address.to_ascii_lowercase())
}

async fn insert_trace_and_outcome(
    database: &TestDatabase,
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
) -> Result<()> {
    upsert_execution_trace(database.pool(), trace).await?;
    upsert_execution_outcome(database.pool(), outcome).await?;
    Ok(())
}

fn execution_outcome(trace: &ExecutionTrace) -> ExecutionOutcome {
    ExecutionOutcome {
        cache_key: ExecutionCacheKey {
            request_key: trace.request_key.clone(),
            requested_chain_positions: json!([
                {
                    "chain_id": "base-mainnet",
                    "block_number": 17_500_000,
                    "block_hash": "0xbase999"
                },
                {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 21_000_000,
                    "block_hash": "0xabc123"
                }
            ]),
            manifest_versions: json!([
                {
                    "source_family": "ens_v1_registry_l1",
                    "manifest_version": 3
                },
                {
                    "source_manifest_id": 7,
                    "manifest_version": 3
                }
            ]),
            topology_version_boundary: version_boundary(
                "ens:alice.eth",
                Uuid::from_u128(0x0e7ec7ace0000000000000000000aaa1),
                Some(1_200),
                Some("RecordsChanged"),
                21_000_000,
                "0xabc123",
                "2024-06-01T00:00:17Z",
            ),
            record_version_boundary: version_boundary(
                "ens:alice.eth",
                Uuid::from_u128(0x0e7ec7ace0000000000000000000aaa2),
                Some(1_200),
                Some("RecordsChanged"),
                21_000_000,
                "0xabc123",
                "2024-06-01T00:00:17Z",
            ),
        },
        execution_trace_id: trace.execution_trace_id,
        request_type: trace.request_type.clone(),
        namespace: trace.namespace.clone(),
        outcome_payload: Some(json!({
            "verified_queries": [
                {
                    "record_key": "addr:60",
                    "status": "success",
                    "value": {
                        "coin_type": "60",
                        "value": "0x00000000000000000000000000000000000000aa"
                    }
                }
            ]
        })),
        failure_payload: None,
        finished_at: trace
            .finished_at
            .expect("execution trace test fixture must finish"),
    }
}

async fn expect_outcome_validation_error(
    database: &TestDatabase,
    outcome: &ExecutionOutcome,
    expected_message: &str,
) -> Result<()> {
    let error = upsert_execution_outcome(database.pool(), outcome)
        .await
        .expect_err("execution outcome validation must fail");
    let rendered = format!("{error:#}");

    assert!(
        rendered.contains(expected_message),
        "unexpected error: {error:#}"
    );

    let persisted_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM execution_cache_outcomes")
        .fetch_one(database.pool())
        .await
        .context("failed to count execution_cache_outcomes rows after validation error")?;
    assert!(
        persisted_count == 0,
        "invalid execution outcomes must not be written"
    );

    Ok(())
}

#[tokio::test]
async fn upserts_and_loads_execution_trace_with_ordered_steps() -> Result<()> {
    let database = TestDatabase::new().await?;
    let trace = execution_trace();

    let inserted = upsert_execution_trace(database.pool(), &trace).await?;
    assert_eq!(inserted, trace);

    let upserted_again = upsert_execution_trace(database.pool(), &trace).await?;
    assert_eq!(upserted_again, trace);

    let loaded = load_execution_trace(database.pool(), trace.execution_trace_id)
        .await?
        .expect("execution trace must exist after upsert");
    assert_eq!(loaded, trace);
    assert_eq!(
        loaded
            .steps
            .iter()
            .map(|step| step.step_index)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert_eq!(loaded.steps[0].step_kind, "load_declared_topology");
    assert_eq!(loaded.steps[1].step_kind, "call_universal_resolver");

    database.cleanup().await
}

#[tokio::test]
async fn rejects_execution_trace_without_steps() -> Result<()> {
    let database = TestDatabase::new().await?;
    let mut trace = execution_trace();
    trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000002);
    trace.steps.clear();

    expect_trace_validation_error(&database, &trace, "must include at least one step").await?;

    database.cleanup().await
}

#[tokio::test]
async fn rejects_execution_trace_without_nonempty_contexts_or_terminal_state() -> Result<()> {
    let database = TestDatabase::new().await?;

    let mut empty_chain_context = execution_trace();
    empty_chain_context.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000003);
    empty_chain_context.chain_context = json!({});
    expect_trace_validation_error(
        &database,
        &empty_chain_context,
        "field chain_context must not be empty",
    )
    .await?;

    let mut empty_manifest_context = execution_trace();
    empty_manifest_context.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000004);
    empty_manifest_context.manifest_context = json!({});
    expect_trace_validation_error(
        &database,
        &empty_manifest_context,
        "field manifest_context must not be empty",
    )
    .await?;

    let mut missing_finished_at = execution_trace();
    missing_finished_at.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000005);
    missing_finished_at.finished_at = None;
    expect_trace_validation_error(&database, &missing_finished_at, "must set finished_at").await?;

    let mut missing_terminal_payload = execution_trace();
    missing_terminal_payload.execution_trace_id =
        Uuid::from_u128(0x0e7ec7ace00000000000000000000006);
    missing_terminal_payload.final_payload = None;
    missing_terminal_payload.failure_payload = None;
    expect_trace_validation_error(
        &database,
        &missing_terminal_payload,
        "must set final_payload or failure_payload",
    )
    .await?;

    database.cleanup().await
}

#[tokio::test]
async fn rejects_execution_trace_with_empty_step_canonicality_dependency() -> Result<()> {
    let database = TestDatabase::new().await?;
    let mut trace = execution_trace();
    trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000007);
    trace.steps[0].canonicality_dependency = json!({});

    expect_trace_validation_error(
        &database,
        &trace,
        "field canonicality_dependency must not be empty",
    )
    .await?;

    database.cleanup().await
}

#[tokio::test]
async fn partial_trace_cannot_get_stuck_before_complete_insert() -> Result<()> {
    let database = TestDatabase::new().await?;
    let complete_trace = execution_trace();
    let mut partial_trace = complete_trace.clone();
    partial_trace.final_payload = None;
    partial_trace.failure_payload = None;

    expect_trace_validation_error(
        &database,
        &partial_trace,
        "must set final_payload or failure_payload",
    )
    .await?;

    let inserted = upsert_execution_trace(database.pool(), &complete_trace).await?;
    assert_eq!(inserted, complete_trace);
    assert_eq!(
        load_execution_trace(database.pool(), complete_trace.execution_trace_id).await?,
        Some(complete_trace)
    );

    database.cleanup().await
}

#[tokio::test]
async fn upserts_and_loads_execution_outcome_by_cache_key() -> Result<()> {
    let database = TestDatabase::new().await?;
    let trace = execution_trace();
    upsert_execution_trace(database.pool(), &trace).await?;

    let outcome = execution_outcome(&trace);
    let inserted = upsert_execution_outcome(database.pool(), &outcome).await?;
    assert_eq!(inserted, outcome);

    let loaded = load_execution_outcome(database.pool(), &outcome.cache_key)
        .await?
        .expect("execution outcome must exist after upsert");
    assert_eq!(loaded, outcome);

    let mut replacement_trace = trace.clone();
    replacement_trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000008);
    replacement_trace.final_payload = Some(json!({
        "record_kind": "addr",
        "coin_type": 60,
        "value": "0x00000000000000000000000000000000000000bb"
    }));
    replacement_trace.finished_at = Some(timestamp(1_717_171_800));
    upsert_execution_trace(database.pool(), &replacement_trace).await?;

    let mut replacement_outcome = outcome.clone();
    replacement_outcome.execution_trace_id = replacement_trace.execution_trace_id;
    replacement_outcome.outcome_payload = Some(json!({
        "verified_queries": [
            {
                "record_key": "addr:60",
                "status": "success",
                "value": {
                    "coin_type": "60",
                    "value": "0x00000000000000000000000000000000000000bb"
                }
            }
        ]
    }));
    replacement_outcome.finished_at = replacement_trace
        .finished_at
        .expect("replacement trace must finish");

    let updated = upsert_execution_outcome(database.pool(), &replacement_outcome).await?;
    assert_eq!(updated, replacement_outcome);
    assert_eq!(
        load_execution_outcome(database.pool(), &replacement_outcome.cache_key).await?,
        Some(replacement_outcome)
    );

    database.cleanup().await
}

#[tokio::test]
async fn transaction_scoped_execution_upserts_stay_pending_until_commit() -> Result<()> {
    let database = TestDatabase::new().await?;
    let trace = execution_trace();
    let outcome = execution_outcome(&trace);

    let mut transaction = database.pool().begin().await?;
    let inserted_trace = upsert_execution_trace_in_transaction(&mut transaction, &trace).await?;
    let inserted_outcome =
        upsert_execution_outcome_in_transaction(&mut transaction, &outcome).await?;
    assert_eq!(inserted_trace, trace);
    assert_eq!(inserted_outcome, outcome);
    assert!(
        load_execution_trace(database.pool(), trace.execution_trace_id)
            .await?
            .is_none(),
        "transaction-scoped trace upsert must remain invisible before commit"
    );
    assert!(
        load_execution_outcome(database.pool(), &outcome.cache_key)
            .await?
            .is_none(),
        "transaction-scoped outcome upsert must remain invisible before commit"
    );

    transaction.commit().await?;

    assert_eq!(
        load_execution_trace(database.pool(), trace.execution_trace_id).await?,
        Some(trace)
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &outcome.cache_key).await?,
        Some(outcome)
    );

    database.cleanup().await
}

#[tokio::test]
async fn execution_outcome_cache_key_is_order_insensitive_for_positions_and_manifests() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let trace = execution_trace();
    upsert_execution_trace(database.pool(), &trace).await?;

    let outcome = execution_outcome(&trace);
    upsert_execution_outcome(database.pool(), &outcome).await?;

    let mut reordered_key = outcome.cache_key.clone();
    reordered_key.requested_chain_positions = json!([
        {
            "chain_id": "ethereum-mainnet",
            "block_number": 21_000_000,
            "block_hash": "0xabc123"
        },
        {
            "chain_id": "base-mainnet",
            "block_number": 17_500_000,
            "block_hash": "0xbase999"
        }
    ]);
    reordered_key.manifest_versions = json!([
        {
            "source_manifest_id": 7,
            "manifest_version": 3
        },
        {
            "source_family": "ens_v1_registry_l1",
            "manifest_version": 3
        }
    ]);

    assert_eq!(
        load_execution_outcome(database.pool(), &reordered_key).await?,
        Some(outcome)
    );

    database.cleanup().await
}

#[tokio::test]
async fn rejects_execution_outcome_with_duplicate_chain_position_or_manifest_identity() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let trace = execution_trace();
    upsert_execution_trace(database.pool(), &trace).await?;

    let mut duplicate_chain = execution_outcome(&trace);
    duplicate_chain.cache_key.requested_chain_positions = json!([
        {
            "chain_id": "ethereum-mainnet",
            "block_number": 21_000_000,
            "block_hash": "0xabc123"
        },
        {
            "chain_id": "ethereum-mainnet",
            "block_number": 21_000_001,
            "block_hash": "0xabc124"
        }
    ]);
    expect_outcome_validation_error(
        &database,
        &duplicate_chain,
        "requested_chain_positions must not repeat chain_id ethereum-mainnet",
    )
    .await?;

    let mut duplicate_manifest = execution_outcome(&trace);
    duplicate_manifest.cache_key.manifest_versions = json!([
        {
            "source_manifest_id": 7,
            "manifest_version": 3
        },
        {
            "source_manifest_id": 7,
            "manifest_version": 3
        }
    ]);
    expect_outcome_validation_error(
        &database,
        &duplicate_manifest,
        "manifest_versions must not repeat the same manifest identity",
    )
    .await?;

    database.cleanup().await
}

#[tokio::test]
async fn rejects_execution_outcome_when_same_cache_key_changes_route_identity() -> Result<()> {
    let database = TestDatabase::new().await?;
    let trace = execution_trace();
    upsert_execution_trace(database.pool(), &trace).await?;

    let outcome = execution_outcome(&trace);
    upsert_execution_outcome(database.pool(), &outcome).await?;

    let mut conflicting_trace = trace.clone();
    conflicting_trace.execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000009);
    conflicting_trace.request_type = "verified_primary_name".to_owned();
    conflicting_trace.final_payload = Some(json!({
        "verified_primary_name": {
            "status": "success",
            "name": "alice.eth"
        }
    }));
    upsert_execution_trace(database.pool(), &conflicting_trace).await?;

    let mut conflicting_outcome = execution_outcome(&conflicting_trace);
    conflicting_outcome.cache_key = outcome.cache_key.clone();
    conflicting_outcome.namespace = "basenames".to_owned();

    let error = upsert_execution_outcome(database.pool(), &conflicting_outcome)
        .await
        .expect_err("route identity drift on the same cache key must fail");
    assert!(
        error
            .to_string()
            .contains("execution outcome cache identity mismatch"),
        "unexpected error: {error:#}"
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &outcome.cache_key).await?,
        Some(outcome)
    );

    database.cleanup().await
}

#[tokio::test]
async fn invalidates_execution_outcomes_for_exact_manifest_version_only() -> Result<()> {
    let database = TestDatabase::new().await?;

    let stale_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000010),
        "ens:alice.eth:addr:60",
        1_717_171_900,
    );
    let stale_outcome = execution_outcome_variant(
        &stale_trace,
        json!([
            {
                "source_manifest_id": 7,
                "manifest_version": 3
            },
            {
                "source_family": "ens_execution",
                "manifest_version": 5
            }
        ]),
        version_boundary(
            "ens:alice.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000bbb1),
            Some(1_210),
            Some("ResolverChanged"),
            21_000_010,
            "0xaaa010",
            "2024-06-01T00:00:27Z",
        ),
        version_boundary(
            "ens:alice.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000ccc1),
            Some(1_220),
            Some("RecordsChanged"),
            21_000_011,
            "0xaaa011",
            "2024-06-01T00:00:28Z",
        ),
    );
    insert_trace_and_outcome(&database, &stale_trace, &stale_outcome).await?;

    let current_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000011),
        "ens:bob.eth:addr:60",
        1_717_171_901,
    );
    let current_outcome = execution_outcome_variant(
        &current_trace,
        json!([
            {
                "source_manifest_id": 7,
                "manifest_version": 4
            },
            {
                "source_family": "ens_execution",
                "manifest_version": 5
            }
        ]),
        version_boundary(
            "ens:bob.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000bbb2),
            Some(1_230),
            Some("ResolverChanged"),
            21_000_020,
            "0xbbb020",
            "2024-06-01T00:00:37Z",
        ),
        version_boundary(
            "ens:bob.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000ccc2),
            Some(1_240),
            Some("RecordsChanged"),
            21_000_021,
            "0xbbb021",
            "2024-06-01T00:00:38Z",
        ),
    );
    insert_trace_and_outcome(&database, &current_trace, &current_outcome).await?;

    let mut other_route_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000012),
        "ens:charlie.eth:addr:60",
        1_717_171_902,
    );
    other_route_trace.request_type = "verified_primary_name".to_owned();
    let other_route_outcome = execution_outcome_variant(
        &other_route_trace,
        json!([{
            "source_manifest_id": 7,
            "manifest_version": 3
        }]),
        version_boundary(
            "ens:charlie.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000bbb3),
            Some(1_250),
            Some("ResolverChanged"),
            21_000_030,
            "0xccc030",
            "2024-06-01T00:00:47Z",
        ),
        version_boundary(
            "ens:charlie.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000ccc3),
            Some(1_260),
            Some("RecordsChanged"),
            21_000_031,
            "0xccc031",
            "2024-06-01T00:00:48Z",
        ),
    );
    insert_trace_and_outcome(&database, &other_route_trace, &other_route_outcome).await?;

    let summary = invalidate_execution_outcomes_for_manifest_version(
        database.pool(),
        &ExecutionManifestInvalidation {
            request_type: "verified_resolution".to_owned(),
            namespace: "ens".to_owned(),
            source_manifest_id: Some(7),
            source_family: None,
            manifest_version: 3,
        },
    )
    .await?;
    assert_eq!(summary.deleted_outcome_count, 1);

    assert_eq!(
        load_execution_outcome(database.pool(), &stale_outcome.cache_key).await?,
        None
    );
    assert!(
        load_execution_trace(database.pool(), stale_trace.execution_trace_id)
            .await?
            .is_some(),
        "execution traces stay durable after cache invalidation"
    );
    assert!(
        load_execution_outcome(database.pool(), &current_outcome.cache_key)
            .await?
            .is_some(),
        "non-matching manifest version must remain cached"
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &other_route_outcome.cache_key).await?,
        Some(other_route_outcome)
    );

    database.cleanup().await
}

#[tokio::test]
async fn invalidates_execution_outcomes_for_exact_topology_boundary_only() -> Result<()> {
    let database = TestDatabase::new().await?;

    let target_boundary = version_boundary(
        "ens:alice.eth",
        Uuid::from_u128(0x0e7ec7ace0000000000000000000ddd1),
        Some(1_310),
        Some("ResolverChanged"),
        21_100_010,
        "0xddd010",
        "2024-06-02T00:00:27Z",
    );
    let record_boundary = version_boundary(
        "ens:alice.eth",
        Uuid::from_u128(0x0e7ec7ace0000000000000000000eee1),
        Some(1_320),
        Some("RecordsChanged"),
        21_100_011,
        "0xddd011",
        "2024-06-02T00:00:28Z",
    );

    let target_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000013),
        "ens:alice.eth:text",
        1_717_172_000,
    );
    let target_outcome = execution_outcome_variant(
        &target_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 5
        }]),
        target_boundary.clone(),
        record_boundary.clone(),
    );
    insert_trace_and_outcome(&database, &target_trace, &target_outcome).await?;

    let keep_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000014),
        "ens:bob.eth:text",
        1_717_172_001,
    );
    let keep_outcome = execution_outcome_variant(
        &keep_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 5
        }]),
        version_boundary(
            "ens:bob.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000ddd2),
            Some(1_330),
            Some("ResolverChanged"),
            21_100_020,
            "0xeee020",
            "2024-06-02T00:00:37Z",
        ),
        record_boundary.clone(),
    );
    insert_trace_and_outcome(&database, &keep_trace, &keep_outcome).await?;

    let mut other_namespace_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000015),
        "base:alice.base.eth:text",
        1_717_172_002,
    );
    other_namespace_trace.namespace = "basenames".to_owned();
    let other_namespace_outcome = execution_outcome_variant(
        &other_namespace_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 5
        }]),
        target_boundary.clone(),
        record_boundary.clone(),
    );
    insert_trace_and_outcome(&database, &other_namespace_trace, &other_namespace_outcome).await?;

    let summary = invalidate_execution_outcomes_for_topology_boundary(
        database.pool(),
        &ExecutionBoundaryInvalidation {
            request_type: "verified_resolution".to_owned(),
            namespace: "ens".to_owned(),
            boundary: target_boundary,
        },
    )
    .await?;
    assert_eq!(summary.deleted_outcome_count, 1);

    assert_eq!(
        load_execution_outcome(database.pool(), &target_outcome.cache_key).await?,
        None
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &keep_outcome.cache_key).await?,
        Some(keep_outcome)
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &other_namespace_outcome.cache_key).await?,
        Some(other_namespace_outcome)
    );

    database.cleanup().await
}

#[tokio::test]
async fn invalidates_execution_outcomes_for_exact_record_boundary_only() -> Result<()> {
    let database = TestDatabase::new().await?;

    let topology_boundary = version_boundary(
        "ens:alice.eth",
        Uuid::from_u128(0x0e7ec7ace0000000000000000000fff1),
        Some(1_410),
        Some("ResolverChanged"),
        21_200_010,
        "0xfff010",
        "2024-06-03T00:00:27Z",
    );
    let target_record_boundary = version_boundary(
        "ens:alice.eth",
        Uuid::from_u128(0x0e7ec7ace0000000000000000000aaa3),
        Some(1_420),
        Some("RecordsChanged"),
        21_200_011,
        "0xfff011",
        "2024-06-03T00:00:28Z",
    );

    let target_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000016),
        "ens:alice.eth:addr:60",
        1_717_172_100,
    );
    let target_outcome = execution_outcome_variant(
        &target_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 6
        }]),
        topology_boundary.clone(),
        target_record_boundary.clone(),
    );
    insert_trace_and_outcome(&database, &target_trace, &target_outcome).await?;

    let keep_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000017),
        "ens:alice.eth:text",
        1_717_172_101,
    );
    let keep_outcome = execution_outcome_variant(
        &keep_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 6
        }]),
        topology_boundary,
        version_boundary(
            "ens:alice.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000aaa4),
            Some(1_430),
            Some("RecordsChanged"),
            21_200_020,
            "0xaaa020",
            "2024-06-03T00:00:37Z",
        ),
    );
    insert_trace_and_outcome(&database, &keep_trace, &keep_outcome).await?;

    let mut other_route_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000018),
        "ens:alice.eth:contenthash",
        1_717_172_102,
    );
    other_route_trace.request_type = "verified_primary_name".to_owned();
    let other_route_outcome = execution_outcome_variant(
        &other_route_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 6
        }]),
        version_boundary(
            "ens:alice.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000fff2),
            Some(1_440),
            Some("ResolverChanged"),
            21_200_030,
            "0xaaa030",
            "2024-06-03T00:00:47Z",
        ),
        target_record_boundary.clone(),
    );
    insert_trace_and_outcome(&database, &other_route_trace, &other_route_outcome).await?;

    let summary = invalidate_execution_outcomes_for_record_boundary(
        database.pool(),
        &ExecutionBoundaryInvalidation {
            request_type: "verified_resolution".to_owned(),
            namespace: "ens".to_owned(),
            boundary: target_record_boundary,
        },
    )
    .await?;
    assert_eq!(summary.deleted_outcome_count, 1);

    assert_eq!(
        load_execution_outcome(database.pool(), &target_outcome.cache_key).await?,
        None
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &keep_outcome.cache_key).await?,
        Some(keep_outcome)
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &other_route_outcome.cache_key).await?,
        Some(other_route_outcome)
    );

    database.cleanup().await
}

#[tokio::test]
async fn invalidates_verified_primary_execution_outcomes_for_exact_manifest_and_request_key_only()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let target_request_key = verified_primary_request_key("0xAbCd", "60");

    let mut target_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000019),
        &target_request_key,
        1_717_172_200,
    );
    target_trace.request_type = "verified_primary_name".to_owned();
    let target_outcome = execution_outcome_variant(
        &target_trace,
        json!([{
            "source_manifest_id": 31,
            "manifest_version": 4
        }]),
        version_boundary(
            "ens:alice.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000abc1),
            Some(1_510),
            Some("ResolverChanged"),
            21_300_010,
            "0xabd010",
            "2024-06-04T00:00:27Z",
        ),
        version_boundary(
            "ens:alice.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000abc2),
            Some(1_520),
            Some("RecordsChanged"),
            21_300_011,
            "0xabd011",
            "2024-06-04T00:00:28Z",
        ),
    );
    insert_trace_and_outcome(&database, &target_trace, &target_outcome).await?;

    let mut other_tuple_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace0000000000000000000001a),
        &verified_primary_request_key("0xEf01", "60"),
        1_717_172_201,
    );
    other_tuple_trace.request_type = "verified_primary_name".to_owned();
    let other_tuple_outcome = execution_outcome_variant(
        &other_tuple_trace,
        json!([{
            "source_manifest_id": 31,
            "manifest_version": 4
        }]),
        version_boundary(
            "ens:bob.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000abc3),
            Some(1_530),
            Some("ResolverChanged"),
            21_300_020,
            "0xabd020",
            "2024-06-04T00:00:37Z",
        ),
        version_boundary(
            "ens:bob.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000abc4),
            Some(1_540),
            Some("RecordsChanged"),
            21_300_021,
            "0xabd021",
            "2024-06-04T00:00:38Z",
        ),
    );
    insert_trace_and_outcome(&database, &other_tuple_trace, &other_tuple_outcome).await?;

    let resolution_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace0000000000000000000001b),
        &target_request_key,
        1_717_172_202,
    );
    let resolution_outcome = execution_outcome_variant(
        &resolution_trace,
        json!([{
            "source_manifest_id": 31,
            "manifest_version": 4
        }]),
        version_boundary(
            "ens:charlie.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000abc5),
            Some(1_550),
            Some("ResolverChanged"),
            21_300_030,
            "0xabd030",
            "2024-06-04T00:00:47Z",
        ),
        version_boundary(
            "ens:charlie.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000abc6),
            Some(1_560),
            Some("RecordsChanged"),
            21_300_031,
            "0xabd031",
            "2024-06-04T00:00:48Z",
        ),
    );
    insert_trace_and_outcome(&database, &resolution_trace, &resolution_outcome).await?;

    let summary = invalidate_execution_outcomes_for_manifest_version_and_request_key(
        database.pool(),
        &ExecutionManifestInvalidation {
            request_type: "verified_primary_name".to_owned(),
            namespace: "ens".to_owned(),
            source_manifest_id: Some(31),
            source_family: None,
            manifest_version: 4,
        },
        &target_request_key,
    )
    .await?;
    assert_eq!(summary.deleted_outcome_count, 1);

    assert_eq!(
        load_execution_outcome(database.pool(), &target_outcome.cache_key).await?,
        None
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &other_tuple_outcome.cache_key).await?,
        Some(other_tuple_outcome)
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &resolution_outcome.cache_key).await?,
        Some(resolution_outcome)
    );

    database.cleanup().await
}

#[tokio::test]
async fn invalidates_verified_primary_execution_outcomes_for_exact_topology_and_request_key_only()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let target_request_key = verified_primary_request_key("0xAbCd", "60");
    let target_boundary = version_boundary(
        "ens:alice.eth",
        Uuid::from_u128(0x0e7ec7ace0000000000000000000abc7),
        Some(1_610),
        Some("ResolverChanged"),
        21_400_010,
        "0xabe010",
        "2024-06-05T00:00:27Z",
    );

    let mut target_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace0000000000000000000001c),
        &target_request_key,
        1_717_172_300,
    );
    target_trace.request_type = "verified_primary_name".to_owned();
    let target_outcome = execution_outcome_variant(
        &target_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 7
        }]),
        target_boundary.clone(),
        version_boundary(
            "ens:alice.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000abc8),
            Some(1_620),
            Some("RecordsChanged"),
            21_400_011,
            "0xabe011",
            "2024-06-05T00:00:28Z",
        ),
    );
    insert_trace_and_outcome(&database, &target_trace, &target_outcome).await?;

    let mut other_tuple_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace0000000000000000000001d),
        &verified_primary_request_key("0xEf01", "60"),
        1_717_172_301,
    );
    other_tuple_trace.request_type = "verified_primary_name".to_owned();
    let other_tuple_outcome = execution_outcome_variant(
        &other_tuple_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 7
        }]),
        target_boundary.clone(),
        version_boundary(
            "ens:bob.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000abc9),
            Some(1_630),
            Some("RecordsChanged"),
            21_400_021,
            "0xabe021",
            "2024-06-05T00:00:38Z",
        ),
    );
    insert_trace_and_outcome(&database, &other_tuple_trace, &other_tuple_outcome).await?;

    let resolution_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace0000000000000000000001e),
        &target_request_key,
        1_717_172_302,
    );
    let resolution_outcome = execution_outcome_variant(
        &resolution_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 7
        }]),
        target_boundary.clone(),
        version_boundary(
            "ens:charlie.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000abca),
            Some(1_640),
            Some("RecordsChanged"),
            21_400_031,
            "0xabe031",
            "2024-06-05T00:00:48Z",
        ),
    );
    insert_trace_and_outcome(&database, &resolution_trace, &resolution_outcome).await?;

    let summary = invalidate_execution_outcomes_for_topology_boundary_and_request_key(
        database.pool(),
        &ExecutionBoundaryInvalidation {
            request_type: "verified_primary_name".to_owned(),
            namespace: "ens".to_owned(),
            boundary: target_boundary,
        },
        &target_request_key,
    )
    .await?;
    assert_eq!(summary.deleted_outcome_count, 1);

    assert_eq!(
        load_execution_outcome(database.pool(), &target_outcome.cache_key).await?,
        None
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &other_tuple_outcome.cache_key).await?,
        Some(other_tuple_outcome)
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &resolution_outcome.cache_key).await?,
        Some(resolution_outcome)
    );

    database.cleanup().await
}

#[tokio::test]
async fn invalidates_verified_primary_execution_outcomes_for_exact_record_and_request_key_only()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let target_request_key = verified_primary_request_key("0xAbCd", "60");
    let target_record_boundary = version_boundary(
        "ens:alice.eth",
        Uuid::from_u128(0x0e7ec7ace0000000000000000000abcb),
        Some(1_710),
        Some("RecordsChanged"),
        21_500_010,
        "0xabf010",
        "2024-06-06T00:00:27Z",
    );

    let mut target_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace0000000000000000000001f),
        &target_request_key,
        1_717_172_400,
    );
    target_trace.request_type = "verified_primary_name".to_owned();
    let target_outcome = execution_outcome_variant(
        &target_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 8
        }]),
        version_boundary(
            "ens:alice.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000abcc),
            Some(1_720),
            Some("ResolverChanged"),
            21_500_011,
            "0xabf011",
            "2024-06-06T00:00:28Z",
        ),
        target_record_boundary.clone(),
    );
    insert_trace_and_outcome(&database, &target_trace, &target_outcome).await?;

    let mut other_tuple_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000020),
        &verified_primary_request_key("0xEf01", "60"),
        1_717_172_401,
    );
    other_tuple_trace.request_type = "verified_primary_name".to_owned();
    let other_tuple_outcome = execution_outcome_variant(
        &other_tuple_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 8
        }]),
        version_boundary(
            "ens:bob.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000abcd),
            Some(1_730),
            Some("ResolverChanged"),
            21_500_021,
            "0xabf021",
            "2024-06-06T00:00:38Z",
        ),
        target_record_boundary.clone(),
    );
    insert_trace_and_outcome(&database, &other_tuple_trace, &other_tuple_outcome).await?;

    let resolution_trace = execution_trace_variant(
        Uuid::from_u128(0x0e7ec7ace00000000000000000000021),
        &target_request_key,
        1_717_172_402,
    );
    let resolution_outcome = execution_outcome_variant(
        &resolution_trace,
        json!([{
            "source_family": "ens_execution",
            "manifest_version": 8
        }]),
        version_boundary(
            "ens:charlie.eth",
            Uuid::from_u128(0x0e7ec7ace0000000000000000000abce),
            Some(1_740),
            Some("ResolverChanged"),
            21_500_031,
            "0xabf031",
            "2024-06-06T00:00:48Z",
        ),
        target_record_boundary.clone(),
    );
    insert_trace_and_outcome(&database, &resolution_trace, &resolution_outcome).await?;

    let summary = invalidate_execution_outcomes_for_record_boundary_and_request_key(
        database.pool(),
        &ExecutionBoundaryInvalidation {
            request_type: "verified_primary_name".to_owned(),
            namespace: "ens".to_owned(),
            boundary: target_record_boundary,
        },
        &target_request_key,
    )
    .await?;
    assert_eq!(summary.deleted_outcome_count, 1);

    assert_eq!(
        load_execution_outcome(database.pool(), &target_outcome.cache_key).await?,
        None
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &other_tuple_outcome.cache_key).await?,
        Some(other_tuple_outcome)
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &resolution_outcome.cache_key).await?,
        Some(resolution_outcome)
    );

    database.cleanup().await
}
