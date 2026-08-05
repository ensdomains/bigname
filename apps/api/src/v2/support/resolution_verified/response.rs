use super::{
    execution_summary::build_resolution_execution_summary,
    readback::reordered_persisted_verified_queries,
};

pub(super) fn build_resolution_execution_explain_verified_state(
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
) -> Result<JsonValue> {
    let mut verified_state = empty_object();
    insert_value_field(
        &mut verified_state,
        "execution",
        build_resolution_execution_summary(row, trace, outcome)?,
    );
    insert_value_field(
        &mut verified_state,
        "verified_queries",
        reordered_persisted_verified_queries(outcome, records)?,
    );
    Ok(verified_state)
}
