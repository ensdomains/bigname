use super::*;

pub(crate) async fn load_v2_primary_name_route_read(
    state: &AppState,
    address: &str,
    namespace: &str,
    coin_type: &str,
    mode: ResolutionMode,
) -> ApiResult<PrimaryNameRouteRead> {
    if !mode.includes_verified()
        || namespace != bigname_storage::ENS_NAMESPACE
        || canonical_primary_name_coin_type(coin_type)? != "60"
    {
        let lookup_state =
            load_primary_name_lookup_state(&state.pool, address, namespace, coin_type).await?;
        return Ok(PrimaryNameRouteRead {
            lookup_state,
            selected_snapshot: None,
        });
    }

    let timer = crate::metrics::verified_execution_timer();
    let lookup = bigname_lookup::LookupEngine::new(
        state.lookup_pool.clone(),
        state.lookup_chain_rpc_urls.clone(),
    )
    .lookup_ens_primary_name(address)
    .await
    .map_err(|error| primary_name_lookup_error(address, error))?;
    let selected_snapshot = primary_name_lookup_snapshot(&lookup.position)?;
    let mut lookup_state = if mode == ResolutionMode::Both {
        load_mixed_primary_name_lookup_state_at_position(
            &state.pool,
            address,
            namespace,
            coin_type,
            &lookup.position,
        )
        .await?
    } else {
        load_primary_name_lookup_state(&state.pool, address, namespace, coin_type).await?
    };
    apply_primary_name_lookup(&mut lookup_state, namespace, lookup)?;
    let outcome = primary_name_verified_result(namespace, &lookup_state);
    timer.finish(crate::metrics::json_outcome(&outcome));

    Ok(PrimaryNameRouteRead {
        lookup_state,
        selected_snapshot: Some(selected_snapshot),
    })
}

async fn load_mixed_primary_name_lookup_state_at_position(
    pool: &PgPool,
    address: &str,
    namespace: &str,
    coin_type: &str,
    position: &bigname_lookup::LookupPosition,
) -> ApiResult<PrimaryNameLookupState> {
    require_primary_name_projection_position(pool, position).await?;
    let lookup_state = load_primary_name_lookup_state(pool, address, namespace, coin_type).await?;
    require_primary_name_projection_position(pool, position).await?;
    Ok(lookup_state)
}

async fn require_primary_name_projection_position(
    pool: &PgPool,
    position: &bigname_lookup::LookupPosition,
) -> ApiResult<()> {
    let checkpoint = load_chain_checkpoint(pool, bigname_lookup::ETHEREUM_MAINNET_CHAIN_ID)
        .await
        .map_err(|error| {
            error!(
                service = "api",
                chain_id = bigname_lookup::ETHEREUM_MAINNET_CHAIN_ID,
                error = ?error,
                "failed to fence indexed primary-name claim to lookup position"
            );
            ApiError::internal_error("failed to load primary-name data")
        })?;
    let matches_lookup = checkpoint.as_ref().is_some_and(|checkpoint| {
        checkpoint.canonical_block_number == Some(position.block_number)
            && checkpoint.canonical_block_hash.as_deref() == Some(position.block_hash.as_str())
    });
    if matches_lookup {
        return Ok(());
    }

    Err(ApiError {
        status: StatusCode::CONFLICT,
        code: "stale",
        message: "indexed and verified primary-name answers are not at one current position"
            .to_owned(),
    })
}

fn primary_name_lookup_snapshot(
    position: &bigname_lookup::LookupPosition,
) -> ApiResult<SelectedSnapshot> {
    if position.chain_id != bigname_lookup::ETHEREUM_MAINNET_CHAIN_ID {
        return Err(ApiError::internal_error(
            "ENS primary-name lookup returned a non-Ethereum position",
        ));
    }
    let timestamp = parse_rfc3339_utc_timestamp(&position.timestamp).map_err(|error| {
        error!(
            service = "api",
            timestamp = %position.timestamp,
            error = ?error,
            "schema-v2 primary-name lookup returned an invalid timestamp"
        );
        ApiError::internal_error("ENS primary-name lookup returned an invalid position")
    })?;
    Ok(SelectedSnapshot {
        chain_positions: ChainPositions::new(BTreeMap::from([(
            "ethereum".to_owned(),
            ChainPosition {
                slot: "ethereum".to_owned(),
                chain_id: position.chain_id.clone(),
                block_number: position.block_number,
                block_hash: position.block_hash.clone(),
                timestamp,
            },
        )])),
        consistency: SnapshotConsistency::Head,
    })
}

fn apply_primary_name_lookup(
    lookup_state: &mut PrimaryNameLookupState,
    namespace: &str,
    lookup: bigname_lookup::EnsPrimaryNameLookup,
) -> ApiResult<()> {
    use bigname_lookup::EnsPrimaryNameStatus;

    let found_claim = match (
        lookup.name.as_deref(),
        lookup.normalized_name.as_deref(),
        lookup.reverse_resolver_address.as_deref(),
    ) {
        (Some(raw_name), Some(normalized_name), Some(resolver_address)) => {
            Some(OnDemandPrimaryNameClaim {
                raw_name: raw_name.to_owned(),
                normalized_name: normalized_name.to_owned(),
                resolver_address: resolver_address.to_owned(),
            })
        }
        _ => None,
    };

    match lookup.status {
        EnsPrimaryNameStatus::Success | EnsPrimaryNameStatus::Mismatch => {
            let claim = found_claim.ok_or_else(|| {
                ApiError::internal_error(
                    "verified primary-name lookup omitted its normalized claim",
                )
            })?;
            let name = live_primary_name_ref(namespace, &claim.normalized_name)?;
            let status = if lookup.status == EnsPrimaryNameStatus::Success {
                "success"
            } else {
                "mismatch"
            };
            let mut verified = json!({ "status": status, "name": name });
            if let Some(reason) = lookup.failure_reason {
                verified["failure_reason"] = JsonValue::String(reason);
            }
            lookup_state.on_demand_claim = OnDemandPrimaryNameClaimState::Found(claim);
            lookup_state.on_demand_verified =
                OnDemandPrimaryNameVerificationState::Verified(verified);
        }
        EnsPrimaryNameStatus::NotFound => {
            if let Some(claim) = found_claim {
                lookup_state.on_demand_claim = OnDemandPrimaryNameClaimState::Found(claim);
                lookup_state.on_demand_verified =
                    OnDemandPrimaryNameVerificationState::Verified(json!({
                        "status": "not_found"
                    }));
            } else {
                lookup_state.on_demand_claim = OnDemandPrimaryNameClaimState::NotFound;
            }
        }
        EnsPrimaryNameStatus::InvalidName => {
            let raw_name = lookup.name.ok_or_else(|| {
                ApiError::internal_error("invalid primary-name lookup omitted its raw claim")
            })?;
            let resolver_address = lookup.reverse_resolver_address.ok_or_else(|| {
                ApiError::internal_error("invalid primary-name lookup omitted its resolver")
            })?;
            if let Some(normalized_name) = lookup.normalized_name {
                lookup_state.on_demand_claim =
                    OnDemandPrimaryNameClaimState::Found(OnDemandPrimaryNameClaim {
                        raw_name,
                        normalized_name,
                        resolver_address,
                    });
                lookup_state.on_demand_verified =
                    OnDemandPrimaryNameVerificationState::ClaimNotNormalized;
            } else {
                lookup_state.on_demand_claim =
                    OnDemandPrimaryNameClaimState::InvalidName(OnDemandPrimaryNameInvalidClaim {
                        raw_name,
                        resolver_address,
                    });
            }
        }
        EnsPrimaryNameStatus::ExecutionFailed => {
            lookup_state.on_demand_claim = found_claim
                .map(OnDemandPrimaryNameClaimState::Found)
                .unwrap_or(OnDemandPrimaryNameClaimState::Unavailable);
            lookup_state.on_demand_verified =
                OnDemandPrimaryNameVerificationState::Verified(json!({
                    "status": "execution_failed",
                    "failure_reason": lookup
                        .failure_reason
                        .unwrap_or_else(|| "resolver_call_failed".to_owned()),
                }));
        }
    }
    Ok(())
}

fn live_primary_name_ref(namespace: &str, normalized_name: &str) -> ApiResult<JsonValue> {
    let namehash = bigname_lookup::ens_namehash_hex(normalized_name).map_err(|error| {
        error!(
            service = "api",
            namespace = %namespace,
            normalized_name = %normalized_name,
            error = ?error,
            "failed to build live primary-name identity"
        );
        ApiError::internal_error(format!(
            "failed to build primary-name identity for {namespace}/{normalized_name}"
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

fn primary_name_lookup_error(address: &str, error: bigname_lookup::LookupError) -> ApiError {
    warn!(
        service = "api",
        address = %address,
        error_kind = ?error.kind(),
        error = %error.message(),
        "schema-v2 primary-name lookup failed"
    );
    match error.kind() {
        bigname_lookup::ErrorKind::Configuration
        | bigname_lookup::ErrorKind::Stale
        | bigname_lookup::ErrorKind::ConcurrentState => ApiError {
            status: StatusCode::CONFLICT,
            code: "stale",
            message: format!("verified primary-name lookup must be retried for address {address}"),
        },
        bigname_lookup::ErrorKind::Unsupported => ApiError {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            code: "unsupported",
            message: "verified primary-name lookup is not supported for this tuple".to_owned(),
        },
        bigname_lookup::ErrorKind::Transport
        | bigname_lookup::ErrorKind::Execution
        | bigname_lookup::ErrorKind::Database => ApiError::internal_error(format!(
            "failed to execute verified primary-name lookup for address {address}"
        )),
    }
}
