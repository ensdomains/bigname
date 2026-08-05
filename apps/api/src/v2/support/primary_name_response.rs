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
        "failure_reason": "claim_not_normalized",
    })
}

pub(crate) fn primary_name_verified_result(
    namespace: &str,
    lookup_state: &PrimaryNameLookupState,
) -> JsonValue {
    if projected_primary_name_claim_is_not_normalized(lookup_state) {
        return primary_name_claim_not_normalized_result();
    }

    match lookup_state.tuple_state {
        PrimaryNameTupleState::TupleMissing => match &lookup_state.on_demand_verified {
            OnDemandPrimaryNameVerificationState::ClaimNotNormalized => {
                primary_name_claim_not_normalized_result()
            }
            OnDemandPrimaryNameVerificationState::Verified(result) => result.clone(),
            OnDemandPrimaryNameVerificationState::NotAttempted
                if matches!(
                    lookup_state.on_demand_claim,
                    OnDemandPrimaryNameClaimState::InvalidName(_)
                ) =>
            {
                json!({
                    "status": "invalid_name",
                    "failure_reason": "claim_name_not_normalizable",
                })
            }
            OnDemandPrimaryNameVerificationState::NotAttempted
                if matches!(
                    lookup_state.on_demand_claim,
                    OnDemandPrimaryNameClaimState::Unavailable
                ) =>
            {
                json!({
                    "status": "execution_failed",
                    "failure_reason": "resolver_call_failed",
                })
            }
            OnDemandPrimaryNameVerificationState::NotAttempted => {
                json!({ "status": "not_found" })
            }
        },
        PrimaryNameTupleState::TuplePresent(_) if matches!(namespace, "ens" | "basenames") => {
            json!({ "status": "not_found" })
        }
        PrimaryNameTupleState::ProjectionUnavailable | PrimaryNameTupleState::TuplePresent(_) => {
            json!({
                "status": "unsupported",
                "unsupported_reason": "verified primary-name entrypoint is not yet supported",
            })
        }
    }
}
