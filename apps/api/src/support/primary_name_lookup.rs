use super::*;

pub(super) async fn load_primary_name_lookup_state(
    pool: &PgPool,
    address: &str,
    namespace: &str,
    coin_type: &str,
    mode: ResolutionMode,
) -> ApiResult<PrimaryNameLookupState> {
    match load_primary_name_current_snapshot(pool, address, namespace, coin_type).await {
        Ok(Some(snapshot)) => Ok(PrimaryNameLookupState {
            tuple_state: PrimaryNameTupleState::TuplePresent(snapshot.row),
            normalized_claim_name: mode
                .includes_declared()
                .then_some(snapshot.normalized_claim_name)
                .flatten(),
            persisted_verified: if mode.includes_verified() {
                load_persisted_primary_name_verified_readback(pool, address, namespace, coin_type)
                    .await?
            } else {
                None
            },
        }),
        Ok(None) => Ok(PrimaryNameLookupState {
            tuple_state: PrimaryNameTupleState::TupleMissing,
            normalized_claim_name: None,
            persisted_verified: None,
        }),
        Err(load_error) if primary_name_projection_unavailable(&load_error) => {
            Ok(PrimaryNameLookupState {
                tuple_state: PrimaryNameTupleState::ProjectionUnavailable,
                normalized_claim_name: None,
                persisted_verified: None,
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

pub(super) fn primary_name_projection_unavailable(load_error: &anyhow::Error) -> bool {
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

pub(super) async fn load_persisted_primary_name_verified_readback(
    pool: &PgPool,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> ApiResult<Option<PersistedPrimaryNameVerifiedReadback>> {
    let request_key = primary_name_verified_request_key(namespace, address, coin_type);
    let row = sqlx::query(
        r#"
        SELECT
            request_key,
            requested_chain_positions,
            manifest_versions,
            topology_version_boundary,
            record_version_boundary,
            execution_trace_id,
            request_type,
            namespace,
            outcome_payload,
            failure_payload,
            finished_at
        FROM execution_cache_outcomes
        WHERE request_type = $1
          AND namespace = $2
          AND request_key = $3
        ORDER BY finished_at DESC, execution_trace_id DESC
        LIMIT 1
        "#,
    )
    .bind(VERIFIED_PRIMARY_NAME_REQUEST_TYPE)
    .bind(namespace)
    .bind(&request_key)
    .fetch_optional(pool)
    .await;

    let Some(row) = (match row {
        Ok(row) => row,
        Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("42P01") => {
            return Ok(None);
        }
        Err(load_error) => {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                error = ?load_error,
                "failed to load persisted verified primary-name outcome"
            );
            return Err(ApiError::internal_error(format!(
                "failed to load persisted verified primary-name outcome for address {address}"
            )));
        }
    }) else {
        return Ok(None);
    };

    let outcome = ExecutionOutcome {
        cache_key: ExecutionCacheKey {
            request_key: row.try_get("request_key").map_err(|load_error| {
                error!(
                    service = "api",
                    address = %address,
                    namespace = %namespace,
                    coin_type = %coin_type,
                    error = ?load_error,
                    "failed to decode persisted verified primary-name request_key"
                );
                ApiError::internal_error(format!(
                    "failed to decode persisted verified primary-name outcome for address {address}"
                ))
            })?,
            requested_chain_positions: row
                .try_get("requested_chain_positions")
                .map_err(|load_error| {
                    error!(
                        service = "api",
                        address = %address,
                        namespace = %namespace,
                        coin_type = %coin_type,
                        error = ?load_error,
                        "failed to decode persisted verified primary-name requested_chain_positions"
                    );
                    ApiError::internal_error(format!(
                        "failed to decode persisted verified primary-name outcome for address {address}"
                    ))
                })?,
            manifest_versions: row.try_get("manifest_versions").map_err(|load_error| {
                error!(
                    service = "api",
                    address = %address,
                    namespace = %namespace,
                    coin_type = %coin_type,
                    error = ?load_error,
                    "failed to decode persisted verified primary-name manifest_versions"
                );
                ApiError::internal_error(format!(
                    "failed to decode persisted verified primary-name outcome for address {address}"
                ))
            })?,
            topology_version_boundary: row
                .try_get("topology_version_boundary")
                .map_err(|load_error| {
                    error!(
                        service = "api",
                        address = %address,
                        namespace = %namespace,
                        coin_type = %coin_type,
                        error = ?load_error,
                        "failed to decode persisted verified primary-name topology_version_boundary"
                    );
                    ApiError::internal_error(format!(
                        "failed to decode persisted verified primary-name outcome for address {address}"
                    ))
                })?,
            record_version_boundary: row
                .try_get("record_version_boundary")
                .map_err(|load_error| {
                    error!(
                        service = "api",
                        address = %address,
                        namespace = %namespace,
                        coin_type = %coin_type,
                        error = ?load_error,
                        "failed to decode persisted verified primary-name record_version_boundary"
                    );
                    ApiError::internal_error(format!(
                        "failed to decode persisted verified primary-name outcome for address {address}"
                    ))
                })?,
        },
        execution_trace_id: row.try_get("execution_trace_id").map_err(|load_error| {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                error = ?load_error,
                "failed to decode persisted verified primary-name execution_trace_id"
            );
            ApiError::internal_error(format!(
                "failed to decode persisted verified primary-name outcome for address {address}"
            ))
        })?,
        request_type: row.try_get("request_type").map_err(|load_error| {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                error = ?load_error,
                "failed to decode persisted verified primary-name request_type"
            );
            ApiError::internal_error(format!(
                "failed to decode persisted verified primary-name outcome for address {address}"
            ))
        })?,
        namespace: row.try_get("namespace").map_err(|load_error| {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                error = ?load_error,
                "failed to decode persisted verified primary-name namespace"
            );
            ApiError::internal_error(format!(
                "failed to decode persisted verified primary-name outcome for address {address}"
            ))
        })?,
        outcome_payload: row.try_get("outcome_payload").map_err(|load_error| {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                error = ?load_error,
                "failed to decode persisted verified primary-name outcome_payload"
            );
            ApiError::internal_error(format!(
                "failed to decode persisted verified primary-name outcome for address {address}"
            ))
        })?,
        failure_payload: row.try_get("failure_payload").map_err(|load_error| {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                error = ?load_error,
                "failed to decode persisted verified primary-name failure_payload"
            );
            ApiError::internal_error(format!(
                "failed to decode persisted verified primary-name outcome for address {address}"
            ))
        })?,
        finished_at: row.try_get("finished_at").map_err(|load_error| {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                error = ?load_error,
                "failed to decode persisted verified primary-name finished_at"
            );
            ApiError::internal_error(format!(
                "failed to decode persisted verified primary-name outcome for address {address}"
            ))
        })?,
    };

    if outcome.request_type != VERIFIED_PRIMARY_NAME_REQUEST_TYPE
        || outcome.namespace != namespace
        || outcome.cache_key.request_key != request_key
    {
        error!(
            service = "api",
            address = %address,
            namespace = %namespace,
            coin_type = %coin_type,
            request_type = %outcome.request_type,
            cached_namespace = %outcome.namespace,
            cached_request_key = %outcome.cache_key.request_key,
            "persisted verified primary-name outcome identity mismatch"
        );
        return Err(ApiError::internal_error(format!(
            "persisted verified primary-name outcome identity mismatch for address {address}"
        )));
    }

    let trace = load_execution_trace(pool, outcome.execution_trace_id)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                execution_trace_id = %outcome.execution_trace_id,
                error = ?load_error,
                "failed to load persisted verified primary-name trace"
            );
            ApiError::internal_error(format!(
                "failed to load persisted verified primary-name trace for address {address}"
            ))
        })?
        .ok_or_else(|| {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                execution_trace_id = %outcome.execution_trace_id,
                "persisted verified primary-name trace missing"
            );
            ApiError::internal_error(format!(
                "persisted verified primary-name trace missing for address {address}"
            ))
        })?;

    let verified_primary_name =
        persisted_verified_primary_name_section(&trace, &outcome, address, namespace, coin_type)?;
    let provenance =
        primary_name_verified_readback_provenance(&trace, &outcome, address, namespace, coin_type)?;

    Ok(Some(PersistedPrimaryNameVerifiedReadback {
        verified_primary_name,
        provenance,
        finished_at: outcome.finished_at,
    }))
}

pub(super) fn primary_name_verified_request_key(
    namespace: &str,
    address: &str,
    coin_type: &str,
) -> String {
    format!("{namespace}:{address}:{coin_type}")
}
