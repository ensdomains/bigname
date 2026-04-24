use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres};
use uuid::Uuid;

use super::{
    trace_rows::{
        insert_execution_steps, insert_execution_trace_row, load_execution_steps_internal,
        load_execution_trace_row_internal,
    },
    trace_validation::{ensure_execution_trace_identity_matches, validate_execution_trace},
    types::{ExecutionTrace, ExecutionTraceInspection},
};

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

/// Load one persisted execution trace for operational inspection.
///
/// This helper reads only `execution_traces` and `execution_steps`; it does not
/// mutate cache outcomes or start a fresh execution.
pub async fn load_execution_trace_inspection(
    pool: &PgPool,
    execution_trace_id: Uuid,
) -> Result<Option<ExecutionTraceInspection>> {
    Ok(load_execution_trace(pool, execution_trace_id)
        .await?
        .map(|trace| ExecutionTraceInspection { trace }))
}

/// Insert one execution trace and its ordered steps transactionally, or reload the
/// same trace when the `execution_trace_id` already exists.
pub async fn upsert_execution_trace(
    pool: &PgPool,
    trace: &ExecutionTrace,
) -> Result<ExecutionTrace> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for execution trace upsert")?;
    let snapshot = upsert_execution_trace_in_transaction(&mut transaction, trace).await?;

    transaction
        .commit()
        .await
        .context("failed to commit execution trace upsert")?;

    Ok(snapshot)
}

/// Insert one execution trace and its ordered steps inside an existing
/// transaction so callers can atomically persist related execution writes.
pub async fn upsert_execution_trace_in_transaction(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    trace: &ExecutionTrace,
) -> Result<ExecutionTrace> {
    validate_execution_trace(trace)?;

    if insert_execution_trace_row(&mut *transaction, trace)
        .await?
        .is_some()
    {
        insert_execution_steps(&mut *transaction, trace).await?;
    }

    let mut snapshot =
        load_execution_trace_row_internal(&mut **transaction, trace.execution_trace_id)
            .await?
            .with_context(|| {
                format!(
                    "failed to reload execution trace {} after upsert",
                    trace.execution_trace_id
                )
            })?;
    snapshot.steps =
        load_execution_steps_internal(&mut **transaction, trace.execution_trace_id).await?;
    ensure_execution_trace_identity_matches(&snapshot, trace)?;

    Ok(snapshot)
}
