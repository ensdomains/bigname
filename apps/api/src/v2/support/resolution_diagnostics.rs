use super::*;

pub(crate) fn build_resolution_execution_diagnostic_data(
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
) -> Result<JsonValue> {
    let verified_state =
        build_resolution_execution_explain_verified_state(row, records, trace, outcome)?;
    let mut data = verified_state
        .get("execution")
        .cloned()
        .context("persisted execution diagnostic must include execution summary")?;
    if !data.is_object() {
        bail!("persisted execution diagnostic execution summary must be an object");
    }
    let mut verified_queries = verified_state
        .get("verified_queries")
        .cloned()
        .context("persisted execution diagnostic must include verified_queries")?;
    normalize_execution_diagnostic_verified_query_statuses(&mut verified_queries);
    insert_value_field(&mut data, "verified_queries", verified_queries);
    Ok(data)
}

fn normalize_execution_diagnostic_verified_query_statuses(queries: &mut JsonValue) {
    for query in queries.as_array_mut().into_iter().flatten() {
        let Some(object) = query.as_object_mut() else {
            continue;
        };
        let Some(status) = object.get("status").and_then(JsonValue::as_str) else {
            continue;
        };
        let status = match status {
            "success" => "ok",
            "execution_failed" => "failed",
            "ok" | "not_found" | "invalid_name" | "mismatch" | "unsupported" | "stale"
            | "failed" => status,
            _ => "failed",
        }
        .to_owned();
        object.insert("status".to_owned(), JsonValue::String(status));
    }
}
