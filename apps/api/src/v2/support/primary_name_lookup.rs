use super::*;

pub(crate) async fn load_primary_name_lookup_state(
    pool: &PgPool,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> ApiResult<PrimaryNameLookupState> {
    let coin_type = canonical_primary_name_coin_type(coin_type)?;
    match load_primary_name_current_snapshot(pool, address, namespace, &coin_type).await {
        Ok(Some(snapshot)) => Ok(PrimaryNameLookupState {
            tuple_state: PrimaryNameTupleState::TuplePresent(snapshot.row),
            normalized_claim_name: snapshot.normalized_claim_name,
            claim_name_is_normalized: snapshot.claim_name_is_normalized,
            on_demand_claim: OnDemandPrimaryNameClaimState::NotAttempted,
            on_demand_verified: OnDemandPrimaryNameVerificationState::NotAttempted,
        }),
        Ok(None) => Ok(PrimaryNameLookupState {
            tuple_state: PrimaryNameTupleState::TupleMissing,
            normalized_claim_name: None,
            claim_name_is_normalized: false,
            on_demand_claim: OnDemandPrimaryNameClaimState::NotAttempted,
            on_demand_verified: OnDemandPrimaryNameVerificationState::NotAttempted,
        }),
        Err(load_error) if primary_name_projection_unavailable(&load_error) => {
            Ok(PrimaryNameLookupState {
                tuple_state: PrimaryNameTupleState::ProjectionUnavailable,
                normalized_claim_name: None,
                claim_name_is_normalized: false,
                on_demand_claim: OnDemandPrimaryNameClaimState::NotAttempted,
                on_demand_verified: OnDemandPrimaryNameVerificationState::NotAttempted,
            })
        }
        Err(load_error) => {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                error = ?load_error,
                "failed to load primary-name tuple state"
            );
            Err(ApiError::internal_error(format!(
                "failed to load primary-name tuple for address {address}"
            )))
        }
    }
}

fn primary_name_projection_unavailable(load_error: &anyhow::Error) -> bool {
    load_error.chain().any(|cause| {
        cause
            .downcast_ref::<sqlx::Error>()
            .is_some_and(|sqlx_error| {
                matches!(
                    sqlx_error,
                    sqlx::Error::Database(error) if error.code().as_deref() == Some("42P01")
                )
            })
    })
}

pub(crate) fn canonical_primary_name_coin_type(coin_type: &str) -> ApiResult<String> {
    bigname_storage::canonical_addr_coin_type(coin_type).ok_or_else(|| ApiError {
        status: StatusCode::BAD_REQUEST,
        code: "invalid_input",
        message: "coin_type must fit in an unsigned 64-bit integer".to_owned(),
    })
}
