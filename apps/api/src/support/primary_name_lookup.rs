use super::*;

const PERSISTED_PRIMARY_NAME_VERIFIED_READBACK_SCAN_LIMIT: i64 = 16;

#[cfg(test)]
#[path = "primary_name_lookup/test_hooks.rs"]
pub(crate) mod test_hooks;
#[path = "primary_name_lookup/trace_reference.rs"]
mod trace_reference;

pub(super) enum PrimaryNameVerifiedReadbackFence<'a> {
    ProjectedClaim,
    RouteLocalFallback(&'a SelectedSnapshot),
}

pub(super) async fn load_primary_name_lookup_state(
    pool: &PgPool,
    address: &str,
    namespace: &str,
    coin_type: &str,
    mode: ResolutionMode,
) -> ApiResult<PrimaryNameLookupState> {
    let coin_type = canonical_primary_name_coin_type(coin_type)?;
    match load_primary_name_current_snapshot(pool, address, namespace, &coin_type).await {
        Ok(Some(snapshot)) => {
            let claim_gates_verified_readback = snapshot.row.claim_status
                == PrimaryNameClaimStatus::Success
                && !snapshot.claim_name_is_normalized;
            let persisted_verified = if mode.includes_verified() && !claim_gates_verified_readback {
                load_persisted_primary_name_verified_readback(pool, address, namespace, &coin_type)
                    .await?
            } else {
                None
            };

            Ok(PrimaryNameLookupState {
                tuple_state: PrimaryNameTupleState::TuplePresent(snapshot.row),
                normalized_claim_name: mode
                    .includes_declared()
                    .then_some(snapshot.normalized_claim_name)
                    .flatten(),
                claim_name_is_normalized: snapshot.claim_name_is_normalized,
                on_demand_claim: OnDemandPrimaryNameClaimState::NotAttempted,
                on_demand_verified: OnDemandPrimaryNameVerificationState::NotAttempted,
                persisted_verified,
            })
        }
        Ok(None) => Ok(PrimaryNameLookupState {
            tuple_state: PrimaryNameTupleState::TupleMissing,
            normalized_claim_name: None,
            claim_name_is_normalized: false,
            on_demand_claim: OnDemandPrimaryNameClaimState::NotAttempted,
            on_demand_verified: OnDemandPrimaryNameVerificationState::NotAttempted,
            persisted_verified: None,
        }),
        Err(load_error) if primary_name_projection_unavailable(&load_error) => {
            Ok(PrimaryNameLookupState {
                tuple_state: PrimaryNameTupleState::ProjectionUnavailable,
                normalized_claim_name: None,
                claim_name_is_normalized: false,
                on_demand_claim: OnDemandPrimaryNameClaimState::NotAttempted,
                on_demand_verified: OnDemandPrimaryNameVerificationState::NotAttempted,
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

pub(super) fn canonical_primary_name_coin_type(coin_type: &str) -> ApiResult<String> {
    bigname_storage::canonical_addr_coin_type(coin_type).ok_or_else(|| ApiError {
        status: StatusCode::BAD_REQUEST,
        code: "invalid_input",
        message: "coin_type must fit in an unsigned 64-bit integer".to_owned(),
    })
}

pub(super) async fn load_persisted_primary_name_verified_readback_from_connection(
    connection: &mut PgConnection,
    address: &str,
    namespace: &str,
    coin_type: &str,
    fence: PrimaryNameVerifiedReadbackFence<'_>,
) -> ApiResult<Option<PersistedPrimaryNameVerifiedReadback>> {
    let request_key = primary_name_verified_request_key(namespace, address, coin_type);
    let rows = sqlx::query(
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
        LIMIT $4
        "#,
    )
    .bind(VERIFIED_PRIMARY_NAME_REQUEST_TYPE)
    .bind(namespace)
    .bind(&request_key)
    .bind(PERSISTED_PRIMARY_NAME_VERIFIED_READBACK_SCAN_LIMIT)
    .fetch_all(&mut *connection)
    .await;

    let rows = match rows {
        Ok(rows) => rows,
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
    };

    for row in rows {
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

        #[cfg(test)]
        test_hooks::run(connection).await?;

        let trace = load_execution_trace_from_connection(connection, outcome.execution_trace_id)
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
            })?;
        let Some(trace) = trace_reference::retain_trace_if_still_referenced(
            connection, trace, &outcome, address, namespace, coin_type,
        )
        .await?
        else {
            continue;
        };

        if !persisted_verified_primary_name_cache_identity_is_current(
            &trace, &outcome, address, namespace, coin_type,
        )? {
            continue;
        }

        let route_local_execution = bigname_execution::route_local_ens_primary_name_execution(
            &trace,
        )
        .map_err(|load_error| {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                execution_trace_id = %trace.execution_trace_id,
                error = ?load_error,
                "persisted route-local verified primary-name claim metadata malformed"
            );
            ApiError::internal_error(format!(
                "persisted verified primary-name trace metadata mismatch for address {address}"
            ))
        })?;
        let (route_local_claim, forward_call_attempted) = match (
            &fence,
            route_local_execution,
        ) {
            (PrimaryNameVerifiedReadbackFence::ProjectedClaim, None) => (None, false),
            (PrimaryNameVerifiedReadbackFence::ProjectedClaim, Some(_)) => continue,
            (PrimaryNameVerifiedReadbackFence::RouteLocalFallback(_), None) => continue,
            (
                PrimaryNameVerifiedReadbackFence::RouteLocalFallback(selected_snapshot),
                Some(route_local_execution),
            ) => {
                let requested_positions = bigname_storage::build_resolution_requested_chain_positions(
                    &selected_snapshot.chain_positions_value(),
                )
                .map_err(|load_error| {
                    error!(
                        service = "api",
                        address = %address,
                        namespace = %namespace,
                        coin_type = %coin_type,
                        error = ?load_error,
                        "failed to build route-local verified primary-name selected positions"
                    );
                    ApiError::internal_error(format!(
                        "failed to validate persisted verified primary-name snapshot for address {address}"
                    ))
                })?;
                if outcome.cache_key.requested_chain_positions != requested_positions {
                    continue;
                }
                (
                    Some(on_demand_claim_from_persisted_route_local_execution(
                        route_local_execution.claim,
                    )),
                    route_local_execution.forward_call_attempted,
                )
            }
        };

        let verified_primary_name = persisted_verified_primary_name_section(
            &trace, &outcome, address, namespace, coin_type,
        )?;
        let provenance = primary_name_verified_readback_provenance(
            &trace, &outcome, address, namespace, coin_type,
        )?;

        return Ok(Some(PersistedPrimaryNameVerifiedReadback {
            verified_primary_name,
            provenance,
            finished_at: outcome.finished_at,
            route_local_claim,
            forward_call_attempted,
        }));
    }

    Ok(None)
}

fn on_demand_claim_from_persisted_route_local_execution(
    claim: bigname_execution::RouteLocalEnsPrimaryNameClaim,
) -> OnDemandPrimaryNameClaimState {
    match claim {
        bigname_execution::RouteLocalEnsPrimaryNameClaim::Found {
            raw_name,
            normalized_name,
            resolver_address,
        } => OnDemandPrimaryNameClaimState::Found(OnDemandPrimaryNameClaim {
            raw_name,
            normalized_name,
            resolver_address,
        }),
        bigname_execution::RouteLocalEnsPrimaryNameClaim::NotFound => {
            OnDemandPrimaryNameClaimState::NotFound
        }
        bigname_execution::RouteLocalEnsPrimaryNameClaim::InvalidName {
            raw_name,
            resolver_address,
        } => OnDemandPrimaryNameClaimState::InvalidName(OnDemandPrimaryNameInvalidClaim {
            raw_name,
            resolver_address,
        }),
        bigname_execution::RouteLocalEnsPrimaryNameClaim::ExecutionFailed { .. } => {
            OnDemandPrimaryNameClaimState::Unavailable
        }
    }
}

pub(super) fn primary_name_verified_request_key(
    namespace: &str,
    address: &str,
    coin_type: &str,
) -> String {
    format!("{namespace}:{address}:{coin_type}")
}
