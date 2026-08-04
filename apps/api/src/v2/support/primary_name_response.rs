use super::*;

pub(crate) fn projected_primary_name_claim_is_not_normalized(
    lookup_state: &PrimaryNameLookupState,
) -> bool {
    matches!(
        lookup_state.tuple_state,
        PrimaryNameTupleState::TuplePresent(ref row)
            if row.claim_status == PrimaryNameClaimStatus::Success
                && !lookup_state.claim_name_is_normalized
    )
}

pub(crate) fn primary_name_claim_not_normalized_result() -> JsonValue {
    json!({
        "status": "invalid_name",
        "failure_reason": bigname_execution::VERIFIED_PRIMARY_NAME_CLAIM_NOT_NORMALIZED_REASON,
    })
}
