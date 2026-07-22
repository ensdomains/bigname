use std::time::Instant;

use super::*;

pub(super) async fn load_primary_name_route_read(
    state: &AppState,
    address: &str,
    namespace: &str,
    coin_type: &str,
    mode: ResolutionMode,
) -> ApiResult<PrimaryNameRouteRead> {
    let mut lookup_state =
        load_primary_name_lookup_state(&state.pool, address, namespace, coin_type, mode).await?;
    if !primary_name_route_fallback_is_eligible(namespace, coin_type, mode, &lookup_state) {
        return Ok(PrimaryNameRouteRead {
            lookup_state,
            selected_snapshot: None,
        });
    }

    let selected_snapshot = resolve_ens_primary_name_fallback_snapshot(&state.pool).await?;
    let persisted_verified = load_persisted_primary_name_route_fallback_readback(
        &state.pool,
        address,
        namespace,
        coin_type,
        &selected_snapshot,
    )
    .await?;
    if let Some(persisted_verified) = persisted_verified {
        lookup_state.on_demand_claim = persisted_verified
            .route_local_claim
            .clone()
            .ok_or_else(|| {
                ApiError::internal_error(format!(
                    "persisted route-local primary-name claim missing for address {address}"
                ))
            })?;
        if mode.includes_verified() {
            lookup_state.persisted_verified = Some(persisted_verified);
        }
        return Ok(PrimaryNameRouteRead {
            lookup_state,
            selected_snapshot: Some(selected_snapshot),
        });
    }

    // The route-local trace readback rechecks tuple absence under the same
    // projection-write fence used by persistence. Reload the tuple after a
    // miss so a row that won that race receives the normal projection and
    // normalization-gate behavior before any RPC call.
    let refreshed_lookup_state =
        load_primary_name_lookup_state(&state.pool, address, namespace, coin_type, mode).await?;
    if !primary_name_route_fallback_is_eligible(
        namespace,
        coin_type,
        mode,
        &refreshed_lookup_state,
    ) {
        return Ok(PrimaryNameRouteRead {
            lookup_state: refreshed_lookup_state,
            selected_snapshot: None,
        });
    }
    lookup_state = refreshed_lookup_state;
    let manifest_versions = if mode.includes_verified() {
        Some(load_ens_execution_manifest_versions(&state.pool).await?)
    } else {
        None
    };

    let position = selected_snapshot
        .chain_positions
        .get("ethereum")
        .ok_or_else(|| {
            ApiError::internal_error(
                "selected ENS primary-name snapshot is missing its Ethereum position",
            )
        })?;
    let reverse_started = Instant::now();
    let claim_read = load_on_demand_primary_name_claim(
        state,
        address,
        namespace,
        coin_type,
        &position.block_hash,
    )
    .await?;
    lookup_state.on_demand_claim = claim_read.claim;
    let mut execution_evidence = claim_read.evidence;
    let reverse_latency_ms = elapsed_millis(reverse_started);

    if mode.includes_verified() {
        let manifest_versions = manifest_versions.ok_or_else(|| {
            ApiError::internal_error(
                "verified ENS primary-name fallback is missing manifest versions",
            )
        })?;
        let mut forward_latency_ms = None;
        if let OnDemandPrimaryNameClaimState::Found(claim) = &lookup_state.on_demand_claim {
            let forward_call_attempted = claim.raw_name == claim.normalized_name;
            let forward_started = Instant::now();
            let verification_read = load_on_demand_primary_name_verification(
                state,
                address,
                namespace,
                coin_type,
                claim,
                &position.block_hash,
            )
            .await?;
            lookup_state.on_demand_verified = verification_read.verification;
            extend_primary_name_execution_evidence(
                &mut execution_evidence,
                verification_read.evidence,
            );
            if forward_call_attempted {
                forward_latency_ms = Some(elapsed_millis(forward_started));
            }
        }
        let route_local_trace_is_current = persist_route_local_primary_name_execution(
            state,
            address,
            namespace,
            coin_type,
            mode,
            &selected_snapshot,
            &mut lookup_state,
            manifest_versions,
            reverse_latency_ms,
            forward_latency_ms,
            &execution_evidence,
        )
        .await?;
        if !route_local_trace_is_current {
            return Ok(PrimaryNameRouteRead {
                lookup_state,
                selected_snapshot: None,
            });
        }
    }

    Ok(PrimaryNameRouteRead {
        lookup_state,
        selected_snapshot: Some(selected_snapshot),
    })
}

fn primary_name_route_fallback_is_eligible(
    namespace: &str,
    coin_type: &str,
    mode: ResolutionMode,
    lookup_state: &PrimaryNameLookupState,
) -> bool {
    (mode.includes_declared() || mode.includes_verified())
        && bigname_storage::primary_name_fallback::contains(namespace, coin_type)
        && matches!(lookup_state.tuple_state, PrimaryNameTupleState::TupleMissing)
}

async fn resolve_ens_primary_name_fallback_snapshot(pool: &PgPool) -> ApiResult<SelectedSnapshot> {
    let scope = SnapshotSelectionScope::new(
        vec![SnapshotPositionRequirement::new(
            "ethereum",
            bigname_storage::primary_name_fallback::CHAIN_ID,
        )],
        Some("ethereum".to_owned()),
    )
    .map_err(snapshot_selection_api_error)?;
    let selector = SnapshotSelectorInput::new(None, None, SnapshotConsistency::Head)
        .map_err(snapshot_selection_api_error)?;
    resolve_exact_name_snapshot_selection(pool, &scope, &selector)
        .await
        .map_err(snapshot_selection_api_error)
}

#[allow(clippy::too_many_arguments)]
async fn persist_route_local_primary_name_execution(
    state: &AppState,
    address: &str,
    namespace: &str,
    coin_type: &str,
    mode: ResolutionMode,
    selected_snapshot: &SelectedSnapshot,
    lookup_state: &mut PrimaryNameLookupState,
    manifest_versions: JsonValue,
    reverse_latency_ms: i64,
    forward_latency_ms: Option<i64>,
    execution_evidence: &bigname_execution::OnDemandEnsPrimaryNameExecutionEvidence,
) -> ApiResult<bool> {
    let position = selected_snapshot
        .chain_positions
        .get("ethereum")
        .ok_or_else(|| {
            ApiError::internal_error(
                "selected ENS primary-name snapshot is missing its Ethereum position",
            )
        })?;
    let route_local_claim = route_local_claim_for_persistence(&lookup_state.on_demand_claim)?;
    let verified_primary_name = primary_name_verified_result(namespace, lookup_state);
    let forward_call_attempted = forward_latency_ms.is_some();
    let request = bigname_execution::build_on_demand_ens_verified_primary_name_request(
        bigname_execution::BuildOnDemandEnsVerifiedPrimaryNameRequest {
            normalized_address: address,
            claim: &route_local_claim,
            verified_primary_name,
            block_number: position.block_number,
            block_hash: &position.block_hash,
            block_timestamp: &format_timestamp(position.timestamp),
            manifest_versions,
            forward_call_attempted,
            reverse_latency_ms,
            forward_latency_ms,
            execution_evidence,
        },
    )
    .map_err(|error| route_local_primary_name_persistence_error(address, error))?;

    if let Err(error) =
        bigname_execution::persist_ens_verified_primary_name(&state.pool, &request).await
    {
        let refreshed_lookup_state =
            load_primary_name_lookup_state(&state.pool, address, namespace, coin_type, mode)
                .await?;
        if !primary_name_route_fallback_is_eligible(
            namespace,
            coin_type,
            mode,
            &refreshed_lookup_state,
        ) {
            *lookup_state = refreshed_lookup_state;
            return Ok(false);
        }
        return Err(route_local_primary_name_persistence_error(address, error));
    }

    let persisted_verified = load_persisted_primary_name_route_fallback_readback(
        &state.pool,
        address,
        namespace,
        coin_type,
        selected_snapshot,
    )
    .await?;
    let Some(persisted_verified) = persisted_verified else {
        let refreshed_lookup_state =
            load_primary_name_lookup_state(&state.pool, address, namespace, coin_type, mode)
                .await?;
        if !primary_name_route_fallback_is_eligible(
            namespace,
            coin_type,
            mode,
            &refreshed_lookup_state,
        ) {
            *lookup_state = refreshed_lookup_state;
            return Ok(false);
        }
        return Err(ApiError::internal_error(format!(
            "persisted route-local verified primary-name outcome missing for address {address}"
        )));
    };
    lookup_state.on_demand_claim = persisted_verified
        .route_local_claim
        .clone()
        .ok_or_else(|| {
            ApiError::internal_error(format!(
                "persisted route-local primary-name claim missing for address {address}"
            ))
        })?;
    lookup_state.persisted_verified = Some(persisted_verified);
    Ok(true)
}

fn extend_primary_name_execution_evidence(
    target: &mut bigname_execution::OnDemandEnsPrimaryNameExecutionEvidence,
    source: bigname_execution::OnDemandEnsPrimaryNameExecutionEvidence,
) {
    target.contracts_called.extend(source.contracts_called);
    target.gateway_digests.extend(source.gateway_digests);
    target
        .ccip_step_payloads
        .extend(source.ccip_step_payloads);
}

fn route_local_claim_for_persistence(
    claim: &OnDemandPrimaryNameClaimState,
) -> ApiResult<bigname_execution::RouteLocalEnsPrimaryNameClaim> {
    match claim {
        OnDemandPrimaryNameClaimState::Found(claim) => {
            Ok(bigname_execution::RouteLocalEnsPrimaryNameClaim::Found {
                raw_name: claim.raw_name.clone(),
                normalized_name: claim.normalized_name.clone(),
                resolver_address: claim.resolver_address.clone(),
            })
        }
        OnDemandPrimaryNameClaimState::NotFound => {
            Ok(bigname_execution::RouteLocalEnsPrimaryNameClaim::NotFound)
        }
        OnDemandPrimaryNameClaimState::InvalidName(claim) => {
            Ok(bigname_execution::RouteLocalEnsPrimaryNameClaim::InvalidName {
                raw_name: claim.raw_name.clone(),
                resolver_address: claim.resolver_address.clone(),
            })
        }
        OnDemandPrimaryNameClaimState::Unavailable => Ok(
            bigname_execution::RouteLocalEnsPrimaryNameClaim::ExecutionFailed {
                failure_reason: "resolver_call_failed".to_owned(),
            },
        ),
        OnDemandPrimaryNameClaimState::NotAttempted => Err(ApiError::internal_error(
            "route-local primary-name claim was not attempted before persistence",
        )),
    }
}

async fn load_ens_execution_manifest_versions(pool: &PgPool) -> ApiResult<JsonValue> {
    let manifest = bigname_manifests::load_execution_owner_manifest_version(
        pool,
        bigname_storage::primary_name_fallback::NAMESPACE,
        bigname_execution::ENS_EXECUTION_SOURCE_FAMILY,
        bigname_storage::primary_name_fallback::CHAIN_ID,
        "ens_v1",
    )
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                namespace = bigname_storage::primary_name_fallback::NAMESPACE,
                error = ?load_error,
                "failed to load ENS execution manifest versions for primary-name persistence"
            );
            ApiError::internal_error(
                "failed to load ENS execution manifest versions for primary-name verification",
            )
        })?;
    let Some(manifest) = manifest else {
        return Err(ApiError {
            status: StatusCode::CONFLICT,
            code: "stale",
            message: "ENS primary-name verification manifest is unavailable".to_owned(),
        });
    };
    Ok(json!([{
        "source_family": manifest.source_family,
        "manifest_version": manifest.manifest_version,
    }]))
}

fn route_local_primary_name_persistence_error(
    address: &str,
    error: impl std::fmt::Display,
) -> ApiError {
    error!(
        service = "api",
        address = %address,
        error = %error,
        "failed to persist route-local verified primary-name execution"
    );
    ApiError {
        status: StatusCode::CONFLICT,
        code: "stale",
        message: "verified primary-name output could not be persisted for the selected snapshot"
            .to_owned(),
    }
}

fn elapsed_millis(started: Instant) -> i64 {
    i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX)
}
