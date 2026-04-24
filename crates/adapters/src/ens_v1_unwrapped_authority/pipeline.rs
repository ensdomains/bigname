use super::*;

pub async fn sync_ens_v1_unwrapped_authority(
    pool: &PgPool,
    chain: &str,
) -> Result<EnsV1UnwrappedAuthoritySyncSummary> {
    sync_ens_v1_unwrapped_authority_with_scope(pool, chain, false, &[]).await
}

impl EnsV1UnwrappedAuthoritySyncSummary {
    pub async fn sync_for_block_hashes(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
    ) -> Result<Self> {
        sync_ens_v1_unwrapped_authority_with_scope(pool, chain, true, block_hashes).await
    }
}

async fn sync_ens_v1_unwrapped_authority_with_scope(
    pool: &PgPool,
    chain: &str,
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
) -> Result<EnsV1UnwrappedAuthoritySyncSummary> {
    let active_emitters = load_active_emitters(pool, chain).await?;
    if active_emitters.is_empty() {
        return Ok(EnsV1UnwrappedAuthoritySyncSummary {
            scanned_log_count: 0,
            matched_log_count: 0,
            total_name_surface_count: 0,
            total_resource_count: 0,
            total_surface_binding_count: 0,
            total_normalized_event_count: 0,
            by_kind: BTreeMap::new(),
        });
    }

    let canonical_blocks = load_canonical_blocks(pool, chain).await?;
    if canonical_blocks.is_empty() {
        return Ok(EnsV1UnwrappedAuthoritySyncSummary {
            scanned_log_count: 0,
            matched_log_count: 0,
            total_name_surface_count: 0,
            total_resource_count: 0,
            total_surface_binding_count: 0,
            total_normalized_event_count: 0,
            by_kind: BTreeMap::new(),
        });
    }

    let block_index = CanonicalBlockIndex {
        blocks: canonical_blocks,
    };
    let reverse_claim_sources = load_reverse_claim_sources(pool, chain).await?;
    let resolver_profile_gate = ResolverProfileGate::load(pool).await?;
    let raw_logs = load_authority_raw_logs(
        pool,
        chain,
        &active_emitters,
        restrict_to_block_hashes,
        block_hashes,
    )
    .await?;
    let scanned_log_count = raw_logs.len();
    if raw_logs.is_empty() {
        return Ok(EnsV1UnwrappedAuthoritySyncSummary {
            scanned_log_count,
            matched_log_count: 0,
            total_name_surface_count: 0,
            total_resource_count: 0,
            total_surface_binding_count: 0,
            total_normalized_event_count: 0,
            by_kind: BTreeMap::new(),
        });
    }

    let mut histories = BTreeMap::<String, NameHistory>::new();
    let mut reverse_histories = BTreeMap::<String, ReverseClaimSourceHistory>::new();
    let mut namehash_to_labelhash = HashMap::<String, String>::new();
    let mut matched_log_count = 0usize;
    for raw_log in &raw_logs {
        if resolver_profile_gate.rejects_resolver_local_fact(raw_log) {
            continue;
        }
        let Some(observation) = build_authority_observation(raw_log)? else {
            continue;
        };
        matched_log_count += 1;

        let labelhash = if let Some(namehash) = observation_namehash(&observation) {
            if let Some(labelhash) = namehash_to_labelhash.get(namehash).cloned() {
                labelhash
            } else if let Some(claim_source) = reverse_claim_sources.get(namehash).cloned() {
                let history = reverse_histories
                    .entry(namehash.to_owned())
                    .or_insert_with(|| ReverseClaimSourceHistory {
                        claim_source,
                        current_resolver: None,
                        current_record_version: None,
                        events: Vec::new(),
                    });
                apply_reverse_claim_source_observation(history, observation)?;
                continue;
            } else {
                continue;
            }
        } else {
            observation_labelhash(&observation)
        };
        let history = histories
            .entry(labelhash.clone())
            .or_insert_with(|| NameHistory {
                name: None,
                labelhash: labelhash.clone(),
                first_name_ref: None,
                current_registration: None,
                current_wrapper_key: None,
                wrapper_authorities: BTreeMap::new(),
                current_registry_owner: None,
                current_resolver: None,
                current_record_version: None,
                open_binding: None,
                bindings: Vec::new(),
                events: Vec::new(),
                registry_resource_anchor: None,
                latest_registry_owner_ref: None,
                latest_registry_owner_before_registration: None,
            });

        apply_observation(history, observation, &block_index).await?;
        if let Some(name) = history.name.as_ref() {
            namehash_to_labelhash.insert(name.namehash.clone(), labelhash);
        }
    }

    let head_block = block_index
        .blocks
        .last()
        .cloned()
        .context("canonical block index must contain a head block")?;
    let head_ref = BoundaryRef {
        chain_id: head_block.chain_id.clone(),
        block_hash: head_block.block_hash.clone(),
        block_number: head_block.block_number,
        block_timestamp: head_block.block_timestamp,
        canonicality_state: head_block.canonicality_state,
        namespace: active_emitters
            .first()
            .map(|emitter| emitter.namespace.clone())
            .unwrap_or_else(|| "ens".to_owned()),
    };

    let mut token_lineages = Vec::<TokenLineage>::new();
    let mut resources = Vec::<Resource>::new();
    let mut surfaces = Vec::<NameSurface>::new();
    let mut bindings = Vec::<SurfaceBinding>::new();
    let mut events = Vec::<NormalizedEvent>::new();

    for history in histories.into_values() {
        let Some(name) = history.name.clone() else {
            continue;
        };

        let finalized = finalize_history(history, &head_ref)?;
        if let Some(surface) =
            build_name_surface(pool, &name, finalized.first_name_ref.as_ref()).await?
        {
            surfaces.push(surface);
        }

        if let Some(registry_anchor) = finalized.registry_resource_anchor.as_ref() {
            resources.push(
                build_resource(
                    pool,
                    deterministic_uuid(&format!(
                        "resource:registry-only:{}:{}",
                        chain, finalized.labelhash
                    )),
                    None,
                    &registry_anchor.chain_id,
                    registry_anchor,
                    json!({
                        "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
                        "authority_kind": "registry_only",
                        "authority_key": format!("registry-only:{}:{}", chain, finalized.labelhash),
                        "logical_name_id": name.logical_name_id,
                        "labelhash": finalized.labelhash,
                        "current_registry_owner": finalized.current_registry_owner,
                    }),
                )
                .await?,
            );
        }

        for lease in &finalized.registrar_leases {
            let token_lineage_id =
                deterministic_uuid(&format!("token-lineage:{}", lease.authority_key));
            token_lineages.push(
                build_token_lineage(
                    pool,
                    token_lineage_id,
                    &lease.start_ref.chain_id,
                    &lease.start_ref,
                    json!({
                        "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
                        "authority_kind": "registrar",
                        "authority_key": lease.authority_key,
                        "logical_name_id": name.logical_name_id,
                        "labelhash": finalized.labelhash,
                    }),
                )
                .await?,
            );
            resources.push(
                build_resource(
                    pool,
                    deterministic_uuid(&format!("resource:{}", lease.authority_key)),
                    Some(token_lineage_id),
                    &lease.start_ref.chain_id,
                    &lease.start_ref.as_boundary_ref(),
                    json!({
                        "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
                        "authority_kind": "registrar",
                        "authority_key": lease.authority_key,
                        "logical_name_id": name.logical_name_id,
                        "labelhash": finalized.labelhash,
                        "expiry": lease.expiry.unix_timestamp(),
                        "registrant": lease.registrant,
                        "released_at": lease.release_ref.as_ref().map(|value| value.block_timestamp.unix_timestamp()),
                    }),
                )
                .await?,
            );
        }

        for authority in &finalized.wrapper_authorities {
            let token_lineage_id =
                deterministic_uuid(&format!("token-lineage:{}", authority.authority_key));
            token_lineages.push(
                build_token_lineage(
                    pool,
                    token_lineage_id,
                    &authority.start_ref.chain_id,
                    &authority.start_ref,
                    json!({
                        "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
                        "authority_kind": "wrapper",
                        "authority_key": authority.authority_key,
                        "logical_name_id": name.logical_name_id,
                        "namehash": authority.node,
                    }),
                )
                .await?,
            );
            resources.push(
                build_resource(
                    pool,
                    deterministic_uuid(&format!("resource:{}", authority.authority_key)),
                    Some(token_lineage_id),
                    &authority.start_ref.chain_id,
                    &authority.start_ref.as_boundary_ref(),
                    json!({
                        "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
                        "authority_kind": "wrapper",
                        "authority_key": authority.authority_key,
                        "logical_name_id": name.logical_name_id,
                        "namehash": authority.node,
                        "owner": authority.owner,
                        "fuses": authority.fuses,
                        "expiry": authority.expiry.unix_timestamp(),
                        "unwrapped_at": authority.end_ref.as_ref().map(|value| value.block_timestamp.unix_timestamp()),
                    }),
                )
                .await?,
            );
        }

        for segment in finalized.bindings {
            bindings.push(
                build_surface_binding(pool, &name.logical_name_id, &segment, &head_ref.chain_id)
                    .await?,
            );
        }
        events.extend(finalized.events);
    }
    for history in reverse_histories.into_values() {
        events.extend(history.events);
    }

    let by_kind = count_events_by_kind(&events);
    upsert_token_lineages(pool, &token_lineages).await?;
    upsert_resources(pool, &resources).await?;
    upsert_name_surfaces(pool, &surfaces).await?;
    upsert_surface_bindings(pool, &bindings).await?;
    upsert_normalized_events(pool, &events).await?;

    Ok(EnsV1UnwrappedAuthoritySyncSummary {
        scanned_log_count,
        matched_log_count,
        total_name_surface_count: surfaces.len(),
        total_resource_count: resources.len(),
        total_surface_binding_count: bindings.len(),
        total_normalized_event_count: events.len(),
        by_kind,
    })
}
