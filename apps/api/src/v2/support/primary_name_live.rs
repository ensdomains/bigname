use super::primary_name_claim_gate::unverifiable_claim_authority;
use super::*;

#[cfg(test)]
pub(crate) mod indexed_read_test_hooks {
    use std::sync::Arc;

    use bigname_test_support::{
        ScopedTestHookGuard, ScopedTestHookRegistry, current_test_database,
    };
    use tokio::sync::Barrier;

    use super::*;

    #[derive(Clone)]
    pub(crate) struct IndexedReadHook {
        reached: Arc<Barrier>,
        resume: Arc<Barrier>,
    }

    pub(crate) struct IndexedReadControl {
        reached: Arc<Barrier>,
        resume: Arc<Barrier>,
    }

    impl IndexedReadControl {
        pub(crate) async fn wait_until_reached(&self) {
            self.reached.wait().await;
        }

        pub(crate) async fn resume(&self) {
            self.resume.wait().await;
        }
    }

    static HOOKS: ScopedTestHookRegistry<String, IndexedReadHook> = ScopedTestHookRegistry::new();

    pub(crate) async fn install(
        pool: &PgPool,
    ) -> anyhow::Result<(
        ScopedTestHookGuard<String, IndexedReadHook>,
        IndexedReadControl,
    )> {
        let database = current_test_database(pool).await?;
        let reached = Arc::new(Barrier::new(2));
        let resume = Arc::new(Barrier::new(2));
        let guard = HOOKS.install(
            database,
            IndexedReadHook {
                reached: Arc::clone(&reached),
                resume: Arc::clone(&resume),
            },
        );
        Ok((guard, IndexedReadControl { reached, resume }))
    }

    pub(super) async fn run(pool: &PgPool) -> ApiResult<()> {
        let database = current_test_database(pool)
            .await
            .map_err(|_| ApiError::internal_error("failed to run primary-name read test hook"))?;
        if let Some(hook) = HOOKS.take(&database) {
            hook.reached.wait().await;
            hook.resume.wait().await;
        }
        Ok(())
    }
}

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
        let publication = current_primary_name_publication(&state.pool, namespace).await?;
        let lookup_state =
            load_primary_name_lookup_state(&state.pool, address, namespace, coin_type).await?;
        #[cfg(test)]
        indexed_read_test_hooks::run(&state.pool).await?;
        require_primary_name_publication_unchanged(
            &publication,
            current_primary_name_publication(&state.pool, namespace).await?,
        )?;
        return Ok(PrimaryNameRouteRead {
            lookup_state,
            selected_snapshot: publication.selected_snapshot,
        });
    }

    // Forward verification resolves the claimed name through the declared ENSv1 universal
    // resolver. When the projected claim names a name that resolver cannot speak for, the route
    // answers in band from projected state instead of dispatching a call whose answer would come
    // from a superseded authority.
    //
    // The publication is captured before the gate reads anything, so every row the decision rests
    // on sits inside the fence the comparison below closes. It stays unresolved until the gate
    // actually refuses: a request that goes on to the live lookup never depended on the
    // projection being publishable, and must not start failing when it is not.
    let gate_publication = current_primary_name_publication(&state.pool, namespace).await;
    if let Some(reason) =
        unverifiable_claim_authority(&state.pool, address, namespace, coin_type).await?
    {
        let publication = gate_publication?;
        let mut lookup_state =
            load_primary_name_lookup_state(&state.pool, address, namespace, coin_type).await?;
        lookup_state.on_demand_verified =
            OnDemandPrimaryNameVerificationState::AuthorityUnsupported(reason);
        #[cfg(test)]
        indexed_read_test_hooks::run(&state.pool).await?;
        require_primary_name_publication_unchanged(
            &publication,
            current_primary_name_publication(&state.pool, namespace).await?,
        )?;
        return Ok(PrimaryNameRouteRead {
            lookup_state,
            selected_snapshot: publication.selected_snapshot,
        });
    }

    let mixed_publication = if mode == ResolutionMode::Both {
        Some(current_primary_name_publication(&state.pool, namespace).await?)
    } else {
        None
    };
    let timer = crate::metrics::verified_execution_timer();
    let lookup =
        bigname_lookup::LookupEngine::new(state.pool.clone(), state.lookup_chain_rpc_urls.clone())
            .lookup_ens_primary_name(address)
            .await
            .map_err(|error| primary_name_lookup_error(address, error))?;
    let selected_snapshot = primary_name_lookup_snapshot(&lookup.position)?;
    if mixed_publication.as_ref().is_some_and(|publication| {
        publication.selected_snapshot.as_ref() != Some(&selected_snapshot)
    }) {
        return Err(primary_name_publication_changed());
    }
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
    if let Some(publication) = &mixed_publication {
        #[cfg(test)]
        indexed_read_test_hooks::run(&state.pool).await?;
        require_primary_name_publication_unchanged(
            publication,
            current_primary_name_publication(&state.pool, namespace).await?,
        )?;
    }
    apply_primary_name_lookup(&mut lookup_state, namespace, lookup)?;
    let outcome = primary_name_verified_result(namespace, &lookup_state);
    timer.finish(crate::metrics::json_outcome(&outcome));

    Ok(PrimaryNameRouteRead {
        lookup_state,
        selected_snapshot: Some(selected_snapshot),
    })
}

#[derive(Eq, PartialEq)]
struct PrimaryNamePublication {
    selected_snapshot: Option<SelectedSnapshot>,
    project_generation: Option<String>,
}

fn require_primary_name_publication_unchanged(
    before: &PrimaryNamePublication,
    after: PrimaryNamePublication,
) -> ApiResult<()> {
    if before == &after {
        Ok(())
    } else {
        Err(primary_name_publication_changed())
    }
}

fn primary_name_publication_changed() -> ApiError {
    ApiError {
        status: StatusCode::CONFLICT,
        code: "stale",
        message: "indexed primary-name data changed during the request".to_owned(),
    }
}

async fn current_primary_name_publication(
    phase_pool: &PgPool,
    namespace: &str,
) -> ApiResult<PrimaryNamePublication> {
    let scope = exact_name_snapshot_scope(
        phase_pool,
        namespace,
        ExactNameSnapshotSelector::default(),
        false,
    )
    .await?;
    let input = SnapshotSelectorInput::new(None, None, SnapshotConsistency::Head)
        .map_err(snapshot_selection_api_error)?;
    match resolve_exact_name_snapshot_selection(phase_pool, &scope, &input).await {
        Ok(selected_snapshot) => {
            let position = selected_snapshot
                .chain_positions
                .as_map()
                .values()
                .next()
                .filter(|_| selected_snapshot.chain_positions.as_map().len() == 1)
                .ok_or_else(|| {
                    ApiError::internal_error(
                        "primary-name snapshot scope did not select exactly one position",
                    )
                })?;
            let project_generation: Option<String> = sqlx::query_scalar(
                r#"
                SELECT project.xmin::text
                FROM chain_heads head
                JOIN chain_phase_state project
                  ON project.chain_id = head.chain_id
                 AND project.phase_name = 'project'
                 AND project.phase_status = 'completed'
                 AND project.current_block_number = head.latest_block_number
                 AND project.current_block_hash = head.latest_block_hash
                 AND project.input_content_hash = $4
                WHERE head.chain_id = $1
                  AND head.latest_block_number = $2
                  AND head.latest_block_hash = $3
                "#,
            )
            .bind(&position.chain_id)
            .bind(position.block_number)
            .bind(&position.block_hash)
            .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
            .fetch_optional(phase_pool)
            .await
            .map_err(|error| {
                error!(
                    service = "api",
                    chain_id = %position.chain_id,
                    error = ?error,
                    "failed to load primary-name project generation"
                );
                ApiError::internal_error("failed to load primary-name data")
            })?;
            let project_generation = project_generation.ok_or_else(|| ApiError {
                status: StatusCode::CONFLICT,
                code: "stale",
                message: "primary-name project publication changed during the request".to_owned(),
            })?;
            Ok(PrimaryNamePublication {
                selected_snapshot: Some(selected_snapshot),
                project_generation: Some(project_generation),
            })
        }
        Err(error)
            if error.kind() == SnapshotSelectionErrorKind::Conflict
                && scope.required_positions().len() == 1 =>
        {
            let chain_id = &scope.required_positions()[0].chain_id;
            if !snapshot_chain_has_head(phase_pool, chain_id)
                .await
                .map_err(snapshot_selection_api_error)?
            {
                return Ok(PrimaryNamePublication {
                    selected_snapshot: None,
                    project_generation: None,
                });
            }
            Err(snapshot_selection_api_error(error))
        }
        Err(error) => Err(snapshot_selection_api_error(error)),
    }
}

async fn load_mixed_primary_name_lookup_state_at_position(
    phase_pool: &PgPool,
    address: &str,
    namespace: &str,
    coin_type: &str,
    position: &bigname_lookup::LookupPosition,
) -> ApiResult<PrimaryNameLookupState> {
    require_primary_name_projection_position(phase_pool, position).await?;
    let lookup_state =
        load_primary_name_lookup_state(phase_pool, address, namespace, coin_type).await?;
    require_primary_name_projection_position(phase_pool, position).await?;
    Ok(lookup_state)
}

async fn require_primary_name_projection_position(
    pool: &PgPool,
    position: &bigname_lookup::LookupPosition,
) -> ApiResult<()> {
    let matches_lookup: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM chain_heads head
            JOIN chain_phase_state project
              ON project.chain_id = head.chain_id
             AND project.phase_name = 'project'
             AND project.phase_status = 'completed'
             AND project.current_block_number = head.latest_block_number
             AND project.current_block_hash = head.latest_block_hash
             AND project.input_content_hash = $4
            WHERE head.chain_id = $1
              AND head.latest_block_number = $2
              AND head.latest_block_hash = $3
        )
        "#,
    )
    .bind(bigname_lookup::ETHEREUM_MAINNET_CHAIN_ID)
    .bind(position.block_number)
    .bind(&position.block_hash)
    .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    .fetch_one(pool)
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
        "logical_name_id": bigname_storage::logical_name_id_for_name(namespace, normalized_name),
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
