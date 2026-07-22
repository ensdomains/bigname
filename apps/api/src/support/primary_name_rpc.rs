use super::*;

pub(super) struct OnDemandPrimaryNameClaimRead {
    pub(super) claim: OnDemandPrimaryNameClaimState,
    pub(super) evidence: bigname_execution::OnDemandEnsPrimaryNameExecutionEvidence,
}

pub(super) struct OnDemandPrimaryNameVerificationRead {
    pub(super) verification: OnDemandPrimaryNameVerificationState,
    pub(super) evidence: bigname_execution::OnDemandEnsPrimaryNameExecutionEvidence,
}

pub(super) async fn load_on_demand_primary_name_claim(
    state: &AppState,
    address: &str,
    namespace: &str,
    coin_type: &str,
    block_hash: &str,
) -> ApiResult<OnDemandPrimaryNameClaimRead> {
    let coin_type = canonical_primary_name_coin_type(coin_type)?;
    if namespace != bigname_storage::ENS_NAMESPACE || coin_type != "60" {
        return Ok(OnDemandPrimaryNameClaimRead {
            claim: OnDemandPrimaryNameClaimState::NotAttempted,
            evidence: Default::default(),
        });
    }

    let lookup = match bigname_execution::execute_ens_reverse_primary_name_lookup(
        bigname_execution::OnDemandEnsPrimaryNameRequest {
            normalized_address: address,
            chain_rpc_urls: &state.chain_rpc_urls,
            block_hash,
        },
    )
    .await
    {
        Ok(on_demand) => on_demand,
        Err(error) => {
            let evidence = error.evidence().clone();
            warn!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                error_kind = ?error.kind(),
                error = %error.message(),
                "on-demand primary-name reverse lookup failed"
            );
            if error.is_transport_failure() && !error.is_configured_timeout() {
                return Err(transient_primary_name_transport_error(address));
            }
            return Ok(OnDemandPrimaryNameClaimRead {
                claim: OnDemandPrimaryNameClaimState::Unavailable,
                evidence,
            });
        }
    };
    let evidence = lookup.evidence;
    let Some(on_demand) = lookup.primary_name else {
        return Ok(OnDemandPrimaryNameClaimRead {
            claim: OnDemandPrimaryNameClaimState::NotFound,
            evidence,
        });
    };

    let parsed = match normalize_inferred_route_name(&on_demand.name) {
        Ok(parsed) => parsed,
        Err(error) => {
            warn!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                raw_name = %on_demand.name,
                error = %error.message,
                "on-demand primary-name reverse lookup returned an unnormalizable name"
            );
            return Ok(OnDemandPrimaryNameClaimRead {
                claim: OnDemandPrimaryNameClaimState::InvalidName(
                    OnDemandPrimaryNameInvalidClaim {
                        raw_name: on_demand.name,
                        resolver_address: on_demand.resolver_address,
                    },
                ),
                evidence,
            });
        }
    };
    if parsed.namespace != namespace {
        return Ok(OnDemandPrimaryNameClaimRead {
            claim: OnDemandPrimaryNameClaimState::NotFound,
            evidence,
        });
    }

    Ok(OnDemandPrimaryNameClaimRead {
        claim: OnDemandPrimaryNameClaimState::Found(OnDemandPrimaryNameClaim {
            raw_name: on_demand.name,
            normalized_name: parsed.normalized_name,
            resolver_address: on_demand.resolver_address,
        }),
        evidence,
    })
}

pub(super) async fn load_on_demand_primary_name_verification(
    state: &AppState,
    address: &str,
    namespace: &str,
    coin_type: &str,
    claim: &OnDemandPrimaryNameClaim,
    block_hash: &str,
) -> ApiResult<OnDemandPrimaryNameVerificationRead> {
    let coin_type = canonical_primary_name_coin_type(coin_type)?;
    if namespace != bigname_storage::ENS_NAMESPACE || coin_type != "60" {
        return Ok(OnDemandPrimaryNameVerificationRead {
            verification: OnDemandPrimaryNameVerificationState::NotAttempted,
            evidence: Default::default(),
        });
    }
    if claim.raw_name != claim.normalized_name {
        return Ok(OnDemandPrimaryNameVerificationRead {
            verification: OnDemandPrimaryNameVerificationState::ClaimNotNormalized,
            evidence: Default::default(),
        });
    }

    let verification = match bigname_execution::verify_ens_primary_name_forward_address(
        bigname_execution::OnDemandEnsPrimaryNameVerificationRequest {
            normalized_address: address,
            normalized_name: &claim.normalized_name,
            chain_rpc_urls: &state.chain_rpc_urls,
            block_hash,
        },
    )
    .await
    {
        Ok(verification) => verification,
        Err(error) => {
            let evidence = error.evidence().clone();
            warn!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                normalized_name = %claim.normalized_name,
                error_kind = ?error.kind(),
                error = %error.message(),
                "on-demand primary-name forward verification failed"
            );
            if error.is_transport_failure() && !error.is_configured_timeout() {
                return Err(transient_primary_name_transport_error(address));
            }
            return Ok(OnDemandPrimaryNameVerificationRead {
                verification: OnDemandPrimaryNameVerificationState::Verified(json!({
                    "status": "execution_failed",
                    "failure_reason": "resolver_call_failed",
                })),
                evidence,
            });
        }
    };
    let evidence = verification.evidence;

    let name = on_demand_primary_name_ref(namespace, &claim.normalized_name)?;
    let section = match verification.resolved_address {
        Some(resolved_address) if resolved_address.eq_ignore_ascii_case(address) => json!({
            "status": "success",
            "name": name,
        }),
        Some(_) => json!({
            "status": "mismatch",
            "name": name,
            "failure_reason": "resolved_target_mismatch",
        }),
        None => json!({
            "status": "not_found",
        }),
    };
    Ok(OnDemandPrimaryNameVerificationRead {
        verification: OnDemandPrimaryNameVerificationState::Verified(section),
        evidence,
    })
}

fn on_demand_primary_name_ref(namespace: &str, normalized_name: &str) -> ApiResult<JsonValue> {
    let namehash = bigname_execution::ens_namehash_hex(normalized_name).map_err(|error| {
        error!(
            service = "api",
            namespace = %namespace,
            normalized_name = %normalized_name,
            error = ?error,
            "failed to build on-demand primary-name namehash"
        );
        ApiError::internal_error(format!(
            "failed to build on-demand primary-name identity for {namespace}/{normalized_name}"
        ))
    })?;

    Ok(json!({
        "logical_name_id": format!("{namespace}:{normalized_name}"),
        "namespace": namespace,
        "normalized_name": normalized_name,
        "canonical_display_name": normalized_name,
        "namehash": namehash,
    }))
}

fn transient_primary_name_transport_error(address: &str) -> ApiError {
    ApiError {
        status: StatusCode::CONFLICT,
        code: "stale",
        message: format!(
            "verified primary-name execution must be retried for address {address}"
        ),
    }
}
