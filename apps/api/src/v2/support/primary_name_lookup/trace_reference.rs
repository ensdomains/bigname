use super::*;

pub(super) async fn retain_trace_if_still_referenced(
    connection: &mut PgConnection,
    trace: Option<ExecutionTrace>,
    outcome: &ExecutionOutcome,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> ApiResult<Option<ExecutionTrace>> {
    if trace
        .as_ref()
        .is_some_and(|persisted_trace| !persisted_trace.steps.is_empty())
    {
        return Ok(trace);
    }

    // Retention deletes the outcome, trace, and steps atomically, but a
    // READ COMMITTED scan can observe the outcome immediately before that
    // commit. The trace loader also uses separate trace-row and step queries,
    // so the commit can appear as a trace with no steps. A vanished live
    // reference makes either shape a cache miss; a remaining reference makes
    // it durable-storage corruption.
    let outcome_still_references_trace = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM execution_cache_outcomes
            WHERE execution_trace_id = $1
              AND request_type = $2
              AND namespace = $3
              AND request_key = $4
        )
        "#,
    )
    .bind(outcome.execution_trace_id)
    .bind(&outcome.request_type)
    .bind(&outcome.namespace)
    .bind(&outcome.cache_key.request_key)
    .fetch_one(&mut *connection)
    .await
    .map_err(|load_error| {
        error!(
            service = "api",
            address = %address,
            namespace = %namespace,
            coin_type = %coin_type,
            execution_trace_id = %outcome.execution_trace_id,
            error = ?load_error,
            "failed to recheck persisted verified primary-name trace reference"
        );
        ApiError::internal_error(format!(
            "failed to recheck persisted verified primary-name outcome for address {address}"
        ))
    })?;
    if !outcome_still_references_trace {
        return Ok(None);
    }
    if trace.is_none() {
        error!(
            service = "api",
            address = %address,
            namespace = %namespace,
            coin_type = %coin_type,
            execution_trace_id = %outcome.execution_trace_id,
            "persisted verified primary-name trace missing"
        );
        return Err(ApiError::internal_error(format!(
            "persisted verified primary-name trace missing for address {address}"
        )));
    }
    error!(
        service = "api",
        address = %address,
        namespace = %namespace,
        coin_type = %coin_type,
        execution_trace_id = %outcome.execution_trace_id,
        "persisted verified primary-name trace steps missing"
    );
    Err(ApiError::internal_error(format!(
        "persisted verified primary-name trace steps missing for address {address}"
    )))
}
