use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::types::time::OffsetDateTime;
use sqlx::{Executor, PgPool, Postgres, Row, postgres::PgRow};
use uuid::Uuid;

/// Persisted execution trace with request, chain, manifest, and ordered-step context.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionTrace {
    pub execution_trace_id: Uuid,
    pub request_type: String,
    pub request_key: String,
    pub namespace: String,
    pub chain_context: Value,
    pub manifest_context: Value,
    pub contracts_called: Value,
    pub gateway_digests: Value,
    pub final_payload: Option<Value>,
    pub failure_payload: Option<Value>,
    pub request_metadata: Value,
    pub finished_at: Option<OffsetDateTime>,
    pub steps: Vec<ExecutionTraceStep>,
}

/// Persisted ordered execution step for one trace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionTraceStep {
    pub step_index: i64,
    pub step_kind: String,
    pub input_digest: Option<String>,
    pub output_digest: Option<String>,
    pub latency_ms: Option<i64>,
    pub canonicality_dependency: Value,
    pub step_payload: Value,
}

/// Load one stored execution trace and its ordered steps.
pub async fn load_execution_trace(
    pool: &PgPool,
    execution_trace_id: Uuid,
) -> Result<Option<ExecutionTrace>> {
    let Some(mut trace) = load_execution_trace_row_internal(pool, execution_trace_id).await? else {
        return Ok(None);
    };
    trace.steps = load_execution_steps_internal(pool, execution_trace_id).await?;
    Ok(Some(trace))
}

/// Insert one execution trace and its ordered steps transactionally, or reload the
/// same trace when the `execution_trace_id` already exists.
pub async fn upsert_execution_trace(
    pool: &PgPool,
    trace: &ExecutionTrace,
) -> Result<ExecutionTrace> {
    validate_execution_trace(trace)?;

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for execution trace upsert")?;

    if insert_execution_trace_row(&mut transaction, trace)
        .await?
        .is_some()
    {
        insert_execution_steps(&mut transaction, trace).await?;
    }

    let mut snapshot =
        load_execution_trace_row_internal(&mut *transaction, trace.execution_trace_id)
            .await?
            .with_context(|| {
                format!(
                    "failed to reload execution trace {} after upsert",
                    trace.execution_trace_id
                )
            })?;
    snapshot.steps =
        load_execution_steps_internal(&mut *transaction, trace.execution_trace_id).await?;
    ensure_execution_trace_identity_matches(&snapshot, trace)?;

    transaction
        .commit()
        .await
        .context("failed to commit execution trace upsert")?;

    Ok(snapshot)
}

async fn insert_execution_trace_row(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    trace: &ExecutionTrace,
) -> Result<Option<ExecutionTrace>> {
    let chain_context = serde_json::to_string(&trace.chain_context)
        .context("failed to serialize execution trace chain_context")?;
    let manifest_context = serde_json::to_string(&trace.manifest_context)
        .context("failed to serialize execution trace manifest_context")?;
    let contracts_called = serde_json::to_string(&trace.contracts_called)
        .context("failed to serialize execution trace contracts_called")?;
    let gateway_digests = serde_json::to_string(&trace.gateway_digests)
        .context("failed to serialize execution trace gateway_digests")?;
    let final_payload = trace
        .final_payload
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .context("failed to serialize execution trace final_payload")?;
    let failure_payload = trace
        .failure_payload
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .context("failed to serialize execution trace failure_payload")?;
    let request_metadata = serde_json::to_string(&trace.request_metadata)
        .context("failed to serialize execution trace request_metadata")?;

    let row = sqlx::query(
        r#"
        INSERT INTO execution_traces (
            execution_trace_id,
            request_type,
            request_key,
            namespace,
            chain_context,
            manifest_context,
            contracts_called,
            gateway_digests,
            final_payload,
            failure_payload,
            request_metadata,
            finished_at
        )
        VALUES (
            $1,
            $2,
            $3,
            $4,
            $5::jsonb,
            $6::jsonb,
            $7::jsonb,
            $8::jsonb,
            $9::jsonb,
            $10::jsonb,
            $11::jsonb,
            $12
        )
        ON CONFLICT (execution_trace_id) DO NOTHING
        RETURNING
            execution_trace_id,
            request_type,
            request_key,
            namespace,
            chain_context,
            manifest_context,
            contracts_called,
            gateway_digests,
            final_payload,
            failure_payload,
            request_metadata,
            finished_at
        "#,
    )
    .bind(trace.execution_trace_id)
    .bind(&trace.request_type)
    .bind(&trace.request_key)
    .bind(&trace.namespace)
    .bind(chain_context)
    .bind(manifest_context)
    .bind(contracts_called)
    .bind(gateway_digests)
    .bind(final_payload)
    .bind(failure_payload)
    .bind(request_metadata)
    .bind(trace.finished_at)
    .fetch_optional(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to insert execution trace {}",
            trace.execution_trace_id
        )
    })?;

    row.map(decode_execution_trace_row).transpose()
}

async fn insert_execution_steps(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    trace: &ExecutionTrace,
) -> Result<()> {
    for step in &trace.steps {
        let canonicality_dependency = serde_json::to_string(&step.canonicality_dependency)
            .context("failed to serialize execution step canonicality_dependency")?;
        let step_payload = serde_json::to_string(&step.step_payload)
            .context("failed to serialize execution step payload")?;

        sqlx::query(
            r#"
            INSERT INTO execution_steps (
                execution_trace_id,
                step_index,
                step_kind,
                input_digest,
                output_digest,
                latency_ms,
                canonicality_dependency,
                step_payload
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb)
            "#,
        )
        .bind(trace.execution_trace_id)
        .bind(step.step_index)
        .bind(&step.step_kind)
        .bind(&step.input_digest)
        .bind(&step.output_digest)
        .bind(step.latency_ms)
        .bind(canonicality_dependency)
        .bind(step_payload)
        .execute(&mut **executor)
        .await
        .with_context(|| {
            format!(
                "failed to insert execution step {} for trace {}",
                step.step_index, trace.execution_trace_id
            )
        })?;
    }

    Ok(())
}

async fn load_execution_trace_row_internal<'e, E>(
    executor: E,
    execution_trace_id: Uuid,
) -> Result<Option<ExecutionTrace>>
where
    E: Executor<'e, Database = Postgres>,
{
    let row = sqlx::query(
        r#"
        SELECT
            execution_trace_id,
            request_type,
            request_key,
            namespace,
            chain_context,
            manifest_context,
            contracts_called,
            gateway_digests,
            final_payload,
            failure_payload,
            request_metadata,
            finished_at
        FROM execution_traces
        WHERE execution_trace_id = $1
        "#,
    )
    .bind(execution_trace_id)
    .fetch_optional(executor)
    .await
    .with_context(|| format!("failed to load execution trace {execution_trace_id}"))?;

    row.map(decode_execution_trace_row).transpose()
}

async fn load_execution_steps_internal<'e, E>(
    executor: E,
    execution_trace_id: Uuid,
) -> Result<Vec<ExecutionTraceStep>>
where
    E: Executor<'e, Database = Postgres>,
{
    let rows = sqlx::query(
        r#"
        SELECT
            step_index,
            step_kind,
            input_digest,
            output_digest,
            latency_ms,
            canonicality_dependency,
            step_payload
        FROM execution_steps
        WHERE execution_trace_id = $1
        ORDER BY step_index
        "#,
    )
    .bind(execution_trace_id)
    .fetch_all(executor)
    .await
    .with_context(|| format!("failed to load execution steps for trace {execution_trace_id}"))?;

    rows.into_iter().map(decode_execution_step).collect()
}

fn validate_execution_trace(trace: &ExecutionTrace) -> Result<()> {
    if trace.request_type.is_empty() {
        bail!(
            "execution trace {} has empty request_type",
            trace.execution_trace_id
        );
    }
    if trace.request_key.is_empty() {
        bail!(
            "execution trace {} has empty request_key",
            trace.execution_trace_id
        );
    }
    if trace.namespace.is_empty() {
        bail!(
            "execution trace {} has empty namespace",
            trace.execution_trace_id
        );
    }
    ensure_nonempty_json_object(
        &trace.chain_context,
        "chain_context",
        trace.execution_trace_id,
    )?;
    ensure_nonempty_json_object(
        &trace.manifest_context,
        "manifest_context",
        trace.execution_trace_id,
    )?;
    ensure_json_array(
        &trace.contracts_called,
        "contracts_called",
        trace.execution_trace_id,
    )?;
    ensure_json_array(
        &trace.gateway_digests,
        "gateway_digests",
        trace.execution_trace_id,
    )?;
    ensure_json_object(
        &trace.request_metadata,
        "request_metadata",
        trace.execution_trace_id,
    )?;
    if trace.finished_at.is_none() {
        bail!(
            "execution trace {} must set finished_at",
            trace.execution_trace_id
        );
    }
    if trace.final_payload.is_none() && trace.failure_payload.is_none() {
        bail!(
            "execution trace {} must set final_payload or failure_payload",
            trace.execution_trace_id
        );
    }
    if trace.steps.is_empty() {
        bail!(
            "execution trace {} must include at least one step",
            trace.execution_trace_id
        );
    }

    for (expected_index, step) in trace.steps.iter().enumerate() {
        let expected_index = i64::try_from(expected_index)
            .context("execution trace step index does not fit in i64")?;
        if step.step_index != expected_index {
            bail!(
                "execution trace {} step order must be contiguous from 0; expected index {}, found {}",
                trace.execution_trace_id,
                expected_index,
                step.step_index
            );
        }
        validate_execution_step(trace.execution_trace_id, step)?;
    }

    Ok(())
}

fn validate_execution_step(execution_trace_id: Uuid, step: &ExecutionTraceStep) -> Result<()> {
    if step.step_kind.is_empty() {
        bail!(
            "execution trace {} step {} has empty step_kind",
            execution_trace_id,
            step.step_index
        );
    }
    if step.step_index < 0 {
        bail!(
            "execution trace {} step {} has negative step_index",
            execution_trace_id,
            step.step_index
        );
    }
    if let Some(latency_ms) = step.latency_ms
        && latency_ms < 0
    {
        bail!(
            "execution trace {} step {} has negative latency_ms {}",
            execution_trace_id,
            step.step_index,
            latency_ms
        );
    }
    ensure_nonempty_json_object(
        &step.canonicality_dependency,
        "canonicality_dependency",
        execution_trace_id,
    )?;
    ensure_json_object(&step.step_payload, "step_payload", execution_trace_id)?;

    Ok(())
}

fn ensure_json_object(value: &Value, field_name: &str, execution_trace_id: Uuid) -> Result<()> {
    if !value.is_object() {
        bail!(
            "execution trace {} field {} must be a JSON object",
            execution_trace_id,
            field_name
        );
    }

    Ok(())
}

fn ensure_nonempty_json_object(
    value: &Value,
    field_name: &str,
    execution_trace_id: Uuid,
) -> Result<()> {
    ensure_json_object(value, field_name, execution_trace_id)?;

    if value.as_object().is_some_and(|object| object.is_empty()) {
        bail!(
            "execution trace {} field {} must not be empty",
            execution_trace_id,
            field_name
        );
    }

    Ok(())
}

fn ensure_json_array(value: &Value, field_name: &str, execution_trace_id: Uuid) -> Result<()> {
    if !value.is_array() {
        bail!(
            "execution trace {} field {} must be a JSON array",
            execution_trace_id,
            field_name
        );
    }

    Ok(())
}

fn ensure_execution_trace_identity_matches(
    existing: &ExecutionTrace,
    incoming: &ExecutionTrace,
) -> Result<()> {
    if existing.request_type != incoming.request_type
        || existing.request_key != incoming.request_key
        || existing.namespace != incoming.namespace
        || existing.chain_context != incoming.chain_context
        || existing.manifest_context != incoming.manifest_context
        || existing.contracts_called != incoming.contracts_called
        || existing.gateway_digests != incoming.gateway_digests
        || existing.final_payload != incoming.final_payload
        || existing.failure_payload != incoming.failure_payload
        || existing.request_metadata != incoming.request_metadata
        || existing.finished_at != incoming.finished_at
        || existing.steps != incoming.steps
    {
        bail!(
            "execution trace identity mismatch for trace {}",
            existing.execution_trace_id
        );
    }

    Ok(())
}

fn decode_execution_trace_row(row: PgRow) -> Result<ExecutionTrace> {
    Ok(ExecutionTrace {
        execution_trace_id: row
            .try_get("execution_trace_id")
            .context("missing execution_trace_id")?,
        request_type: row
            .try_get("request_type")
            .context("missing request_type")?,
        request_key: row.try_get("request_key").context("missing request_key")?,
        namespace: row.try_get("namespace").context("missing namespace")?,
        chain_context: row
            .try_get("chain_context")
            .context("missing chain_context")?,
        manifest_context: row
            .try_get("manifest_context")
            .context("missing manifest_context")?,
        contracts_called: row
            .try_get("contracts_called")
            .context("missing contracts_called")?,
        gateway_digests: row
            .try_get("gateway_digests")
            .context("missing gateway_digests")?,
        final_payload: row
            .try_get("final_payload")
            .context("missing final_payload")?,
        failure_payload: row
            .try_get("failure_payload")
            .context("missing failure_payload")?,
        request_metadata: row
            .try_get("request_metadata")
            .context("missing request_metadata")?,
        finished_at: row.try_get("finished_at").context("missing finished_at")?,
        steps: Vec::new(),
    })
}

fn decode_execution_step(row: PgRow) -> Result<ExecutionTraceStep> {
    Ok(ExecutionTraceStep {
        step_index: row.try_get("step_index").context("missing step_index")?,
        step_kind: row.try_get("step_kind").context("missing step_kind")?,
        input_digest: row
            .try_get("input_digest")
            .context("missing input_digest")?,
        output_digest: row
            .try_get("output_digest")
            .context("missing output_digest")?,
        latency_ms: row.try_get("latency_ms").context("missing latency_ms")?,
        canonicality_dependency: row
            .try_get("canonicality_dependency")
            .context("missing canonicality_dependency")?,
        step_payload: row
            .try_get("step_payload")
            .context("missing step_payload")?,
    })
}

#[cfg(test)]
mod tests {
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
        empty_chain_context.execution_trace_id =
            Uuid::from_u128(0x0e7ec7ace00000000000000000000003);
        empty_chain_context.chain_context = json!({});
        expect_trace_validation_error(
            &database,
            &empty_chain_context,
            "field chain_context must not be empty",
        )
        .await?;

        let mut empty_manifest_context = execution_trace();
        empty_manifest_context.execution_trace_id =
            Uuid::from_u128(0x0e7ec7ace00000000000000000000004);
        empty_manifest_context.manifest_context = json!({});
        expect_trace_validation_error(
            &database,
            &empty_manifest_context,
            "field manifest_context must not be empty",
        )
        .await?;

        let mut missing_finished_at = execution_trace();
        missing_finished_at.execution_trace_id =
            Uuid::from_u128(0x0e7ec7ace00000000000000000000005);
        missing_finished_at.finished_at = None;
        expect_trace_validation_error(&database, &missing_finished_at, "must set finished_at")
            .await?;

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
}
