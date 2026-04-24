use anyhow::{Context, Result};
use sqlx::{Executor, Postgres, Row, postgres::PgRow};
use uuid::Uuid;

use super::types::{ExecutionTrace, ExecutionTraceStep};

pub(super) async fn insert_execution_trace_row(
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

pub(super) async fn insert_execution_steps(
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

pub(super) async fn load_execution_trace_row_internal<'e, E>(
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

pub(super) async fn load_execution_steps_internal<'e, E>(
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
