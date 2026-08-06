use super::*;

pub(crate) async fn load_primary_name_lookup_state(
    pool: &PgPool,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> ApiResult<PrimaryNameLookupState> {
    type PhasePrimaryNameRow = (
        String,
        String,
        String,
        String,
        Option<String>,
        bool,
        JsonValue,
    );

    let coin_type = canonical_primary_name_coin_type(coin_type)?;
    let row = sqlx::query_as::<_, PhasePrimaryNameRow>(
        r#"
        SELECT
            address,
            namespace,
            coin_type,
            claim_status,
            raw_claim_name,
            claim_name_is_normalized,
            claim_provenance
        FROM primary_names_current
        WHERE address = $1
          AND namespace = $2
          AND coin_type = $3
        "#,
    )
    .bind(address.to_ascii_lowercase())
    .bind(namespace)
    .bind(&coin_type)
    .fetch_optional(pool)
    .await;

    match row {
        Ok(Some((
            address,
            namespace,
            coin_type,
            claim_status,
            raw_claim_name,
            claim_name_is_normalized,
            claim_provenance,
        ))) => {
            let claim_status = phase_primary_name_claim_status(&claim_status)?;
            let normalized_claim_name = if claim_status == PrimaryNameClaimStatus::Success {
                let raw_claim_name = raw_claim_name.as_deref().ok_or_else(|| {
                    ApiError::internal_error(
                        "schema-v2 successful primary-name tuple omitted its claim",
                    )
                })?;
                Some(
                    bigname_domain::normalization::normalize_name(raw_claim_name)
                        .map_err(|_| {
                            ApiError::internal_error(
                                "schema-v2 successful primary-name tuple has an invalid claim",
                            )
                        })?
                        .normalized_name,
                )
            } else {
                None
            };
            Ok(PrimaryNameLookupState {
                tuple_state: PrimaryNameTupleState::TuplePresent(
                    bigname_storage::PrimaryNameCurrentRow {
                        address,
                        namespace,
                        coin_type,
                        claim_status,
                        raw_claim_name,
                        claim_provenance,
                    },
                ),
                normalized_claim_name,
                claim_name_is_normalized,
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
        Err(load_error) if primary_name_projection_sqlx_unavailable(&load_error) => {
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

fn phase_primary_name_claim_status(value: &str) -> ApiResult<PrimaryNameClaimStatus> {
    match value {
        "success" => Ok(PrimaryNameClaimStatus::Success),
        "not_found" => Ok(PrimaryNameClaimStatus::NotFound),
        "unsupported" => Ok(PrimaryNameClaimStatus::Unsupported),
        "invalid_name" => Ok(PrimaryNameClaimStatus::InvalidName),
        _ => Err(ApiError::internal_error(
            "schema-v2 primary-name tuple has an unknown claim status",
        )),
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
