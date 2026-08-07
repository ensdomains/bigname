use super::*;

pub(crate) async fn load_primary_name_lookup_state(
    pool: &PgPool,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> ApiResult<PrimaryNameLookupState> {
    let coin_type = canonical_primary_name_coin_type(coin_type)?;
    let row =
        bigname_storage::load_primary_name_current_snapshot(pool, address, namespace, &coin_type)
            .await;

    match row {
        Ok(Some(snapshot)) => {
            let row = snapshot.row;
            // Storage derives the normalized spelling; a successful claim with no raw name is a
            // projection defect that would otherwise read as "no primary name".
            if row.claim_status == PrimaryNameClaimStatus::Success && row.raw_claim_name.is_none() {
                return Err(ApiError::internal_error(
                    "schema-v2 successful primary-name tuple omitted its claim",
                ));
            }
            Ok(PrimaryNameLookupState {
                tuple_state: PrimaryNameTupleState::TuplePresent(row),
                normalized_claim_name: snapshot.normalized_claim_name,
                claim_name_is_normalized: snapshot.claim_name_is_normalized,
                on_demand_claim: OnDemandPrimaryNameClaimState::NotAttempted,
                on_demand_verified: OnDemandPrimaryNameVerificationState::NotAttempted,
            })
        }
        Ok(None) => Ok(PrimaryNameLookupState {
            tuple_state: PrimaryNameTupleState::TupleMissing,
            normalized_claim_name: None,
            claim_name_is_normalized: false,
            on_demand_claim: OnDemandPrimaryNameClaimState::NotAttempted,
            on_demand_verified: OnDemandPrimaryNameVerificationState::NotAttempted,
        }),
        Err(load_error)
            if load_error
                .downcast_ref::<sqlx::Error>()
                .is_some_and(primary_name_projection_sqlx_unavailable) =>
        {
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
                "failed to load schema-v2 primary-name tuple state"
            );
            Err(ApiError::internal_error(format!(
                "failed to load primary-name tuple for address {address}"
            )))
        }
    }
}

fn primary_name_projection_sqlx_unavailable(error: &sqlx::Error) -> bool {
    matches!(
        error,
        sqlx::Error::Database(error) if error.code().as_deref() == Some("42P01")
    )
}

pub(crate) fn canonical_primary_name_coin_type(coin_type: &str) -> ApiResult<String> {
    bigname_storage::canonical_addr_coin_type(coin_type).ok_or_else(|| ApiError {
        status: StatusCode::BAD_REQUEST,
        code: "invalid_input",
        message: "coin_type must fit in an unsigned 64-bit integer".to_owned(),
    })
}
