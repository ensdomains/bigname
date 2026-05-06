use super::*;

mod apply;
mod identity;

use apply::*;
use identity::*;

pub async fn sync_ens_v1_unwrapped_authority(
    pool: &PgPool,
    chain: &str,
) -> Result<EnsV1UnwrappedAuthoritySyncSummary> {
    sync_ens_v1_unwrapped_authority_with_scope(pool, chain, false, &[], None).await
}

impl EnsV1UnwrappedAuthoritySyncSummary {
    fn empty(scanned_log_count: usize) -> Self {
        Self {
            scanned_log_count,
            matched_log_count: 0,
            total_name_surface_count: 0,
            total_resource_count: 0,
            total_surface_binding_count: 0,
            total_normalized_event_count: 0,
            total_normalized_event_inserted_count: 0,
            by_kind: BTreeMap::new(),
        }
    }

    pub async fn sync_for_block_hashes(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
    ) -> Result<Self> {
        sync_ens_v1_unwrapped_authority_with_scope(pool, chain, true, block_hashes, None).await
    }

    pub async fn sync_for_block_hashes_with_source_scope(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
        source_scope: &[(String, String, i64, i64)],
    ) -> Result<Self> {
        sync_ens_v1_unwrapped_authority_with_scope(
            pool,
            chain,
            true,
            block_hashes,
            Some(source_scope),
        )
        .await
    }
}

async fn sync_ens_v1_unwrapped_authority_with_scope(
    pool: &PgPool,
    chain: &str,
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
) -> Result<EnsV1UnwrappedAuthoritySyncSummary> {
    let source_scope = source_scope.map(normalized_authority_source_scope_targets);
    let total_started = Instant::now();
    if source_scope.as_ref().is_some_and(Vec::is_empty) {
        return Ok(EnsV1UnwrappedAuthoritySyncSummary::empty(0));
    }

    let active_emitters_started = Instant::now();
    let generic_resolver_event_sources =
        load_generic_resolver_event_sources(pool, chain, source_scope.as_deref()).await?;
    let active_emitters = load_active_emitters(pool, chain, source_scope.as_deref()).await?;
    let raw_log_active_emitters = active_emitters
        .iter()
        .filter(|emitter| {
            generic_resolver_event_sources.is_empty()
                || emitter.source_family != SOURCE_FAMILY_ENS_V1_RESOLVER_L1
        })
        .cloned()
        .collect::<Vec<_>>();
    let active_emitters_ms = active_emitters_started.elapsed().as_millis();
    if active_emitters.is_empty() && generic_resolver_event_sources.is_empty() {
        return Ok(EnsV1UnwrappedAuthoritySyncSummary::empty(0));
    }
    let event_topics = AuthorityEventTopics::load_for_authority_sources(
        pool,
        chain,
        &active_emitters,
        &generic_resolver_event_sources,
    )
    .await?;

    let mut histories = BTreeMap::<String, NameHistory>::new();
    let mut reverse_histories = BTreeMap::<String, ReverseClaimSourceHistory>::new();
    let mut known_names_by_namehash = HashMap::<String, NameMetadata>::new();
    let mut known_name_refs_by_namehash = HashMap::<String, ObservationRef>::new();
    let mut namehash_to_labelhash = HashMap::<String, String>::new();
    let mut pending_namehash_observations = HashMap::<String, Vec<AuthorityObservation>>::new();
    let mut same_tx_name_intro_positions = HashMap::<String, Vec<RawLogPosition>>::new();
    let mut migrated_registry_nodes = MigratedRegistryNodes::empty();
    let scanned_log_count;
    let block_index;
    let mut matched_log_count = 0usize;
    let mut raw_log_load_ms = 0;
    let canonical_blocks_ms;
    let reverse_claim_sources_ms;
    let resolver_profile_gate_ms;
    let mut same_tx_name_intro_ms = 0;
    let mut preload_name_metadata_ms = 0;
    let mut preload_restricted_histories_ms = 0;
    let mut migrated_registry_nodes_ms = 0;
    let apply_ms;

    if !restrict_to_block_hashes
        && source_scope.is_none()
        && generic_resolver_event_sources.is_empty()
    {
        let canonical_blocks_started = Instant::now();
        let canonical_blocks = load_canonical_blocks(pool, chain).await?;
        canonical_blocks_ms = canonical_blocks_started.elapsed().as_millis();
        if canonical_blocks.is_empty() {
            return Ok(EnsV1UnwrappedAuthoritySyncSummary::empty(0));
        }
        block_index = CanonicalBlockIndex {
            blocks: canonical_blocks,
        };
        let reverse_claim_sources_started = Instant::now();
        let reverse_claim_sources = load_reverse_claim_sources(pool, chain).await?;
        reverse_claim_sources_ms = reverse_claim_sources_started.elapsed().as_millis();
        let resolver_profile_gate_started = Instant::now();
        let resolver_profile_gate = ResolverProfileGate::load(pool).await?;
        resolver_profile_gate_ms = resolver_profile_gate_started.elapsed().as_millis();

        let stream_apply_started = Instant::now();
        scanned_log_count =
            stream_authority_raw_logs(pool, chain, &raw_log_active_emitters, |raw_log| {
                if apply_authority_raw_log(
                    &raw_log,
                    &mut histories,
                    &mut reverse_histories,
                    &mut known_names_by_namehash,
                    &mut known_name_refs_by_namehash,
                    &mut namehash_to_labelhash,
                    &mut pending_namehash_observations,
                    &same_tx_name_intro_positions,
                    &mut migrated_registry_nodes,
                    &reverse_claim_sources,
                    &resolver_profile_gate,
                    &block_index,
                    &event_topics,
                )? {
                    matched_log_count += 1;
                }
                Ok(())
            })
            .await?;
        apply_ms = stream_apply_started.elapsed().as_millis();
    } else {
        let raw_log_load_started = Instant::now();
        let raw_logs = load_authority_raw_logs(
            pool,
            chain,
            &raw_log_active_emitters,
            &generic_resolver_event_sources,
            &event_topics,
            restrict_to_block_hashes,
            block_hashes,
            source_scope.as_deref(),
        )
        .await?;
        raw_log_load_ms = raw_log_load_started.elapsed().as_millis();
        scanned_log_count = raw_logs.len();
        if raw_logs.is_empty() {
            return Ok(EnsV1UnwrappedAuthoritySyncSummary::empty(scanned_log_count));
        }

        let canonical_blocks_started = Instant::now();
        let canonical_blocks = load_canonical_blocks_for_restricted_authority_sync(
            pool,
            chain,
            &raw_logs,
            &event_topics,
        )
        .await?;
        canonical_blocks_ms = canonical_blocks_started.elapsed().as_millis();
        if canonical_blocks.is_empty() {
            return Ok(EnsV1UnwrappedAuthoritySyncSummary::empty(scanned_log_count));
        }
        block_index = CanonicalBlockIndex {
            blocks: canonical_blocks,
        };

        let resolver_profile_fact_nodes = resolver_profile_fact_nodes(&raw_logs, &event_topics)?;
        let reverse_claim_sources_started = Instant::now();
        let reverse_claim_sources = if !resolver_profile_fact_nodes.is_empty() {
            load_reverse_claim_sources_for_nodes(pool, chain, &resolver_profile_fact_nodes).await?
        } else {
            HashMap::new()
        };
        reverse_claim_sources_ms = reverse_claim_sources_started.elapsed().as_millis();
        let resolver_profile_gate_started = Instant::now();
        let resolver_profile_gate = if !resolver_profile_fact_nodes.is_empty() {
            ResolverProfileGate::load_for_raw_logs(pool, &raw_logs, &event_topics).await?
        } else {
            ResolverProfileGate::default()
        };
        resolver_profile_gate_ms = resolver_profile_gate_started.elapsed().as_millis();
        let same_tx_name_intro_started = Instant::now();
        same_tx_name_intro_positions = name_intro_positions_for_raw_logs(&raw_logs, &event_topics)?;
        same_tx_name_intro_ms = same_tx_name_intro_started.elapsed().as_millis();
        let preload_name_metadata_started = Instant::now();
        preload_name_metadata_for_raw_logs(
            pool,
            &raw_logs,
            &mut known_names_by_namehash,
            &event_topics,
        )
        .await?;
        preload_name_metadata_ms = preload_name_metadata_started.elapsed().as_millis();
        for name in known_names_by_namehash.values() {
            if let Some(labelhash) = name.labelhashes.first() {
                namehash_to_labelhash.insert(name.namehash.clone(), labelhash.clone());
            }
        }
        let preload_restricted_histories_started = Instant::now();
        preload_restricted_name_histories(
            pool,
            chain,
            &raw_logs,
            &mut histories,
            &mut known_names_by_namehash,
            &mut known_name_refs_by_namehash,
            &mut namehash_to_labelhash,
            &block_index,
            &event_topics,
        )
        .await?;
        preload_restricted_histories_ms =
            preload_restricted_histories_started.elapsed().as_millis();

        let preload_migrated_registry_nodes = raw_logs
            .iter()
            .any(|raw_log| raw_log.contract_role.as_deref() == Some(CONTRACT_ROLE_REGISTRY_OLD));
        if preload_migrated_registry_nodes {
            let migrated_registry_nodes_started = Instant::now();
            let first_selected_block = raw_logs
                .iter()
                .map(|raw_log| raw_log.block_number)
                .min()
                .context("non-empty raw log set must have a first block")?;
            migrated_registry_nodes = load_migrated_registry_nodes_before_block(
                pool,
                chain,
                &active_emitters,
                first_selected_block,
                &event_topics,
            )
            .await?;
            migrated_registry_nodes_ms = migrated_registry_nodes_started.elapsed().as_millis();
        }

        let apply_started = Instant::now();
        for raw_log in &raw_logs {
            if apply_authority_raw_log(
                raw_log,
                &mut histories,
                &mut reverse_histories,
                &mut known_names_by_namehash,
                &mut known_name_refs_by_namehash,
                &mut namehash_to_labelhash,
                &mut pending_namehash_observations,
                &same_tx_name_intro_positions,
                &mut migrated_registry_nodes,
                &reverse_claim_sources,
                &resolver_profile_gate,
                &block_index,
                &event_topics,
            )? {
                matched_log_count += 1;
            }
        }
        apply_ms = apply_started.elapsed().as_millis();
    }

    if scanned_log_count == 0 {
        return Ok(EnsV1UnwrappedAuthoritySyncSummary::empty(scanned_log_count));
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
            .or_else(|| {
                generic_resolver_event_sources
                    .first()
                    .map(|source| source.namespace.clone())
            })
            .unwrap_or_else(|| "ens".to_owned()),
    };

    let mut token_lineages = Vec::<TokenLineage>::new();
    let mut resources = Vec::<Resource>::new();
    let mut surfaces = Vec::<NameSurface>::new();
    let mut bindings = Vec::<SurfaceBinding>::new();
    let mut events = Vec::<NormalizedEvent>::new();
    let mut token_lineage_ids = HashSet::<Uuid>::new();
    let mut resource_ids = HashSet::<Uuid>::new();

    let materialization_started = Instant::now();
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
            push_resource_once(
                &mut resources,
                &mut resource_ids,
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
            push_token_lineage_once(
                &mut token_lineages,
                &mut token_lineage_ids,
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
            push_resource_once(
                &mut resources,
                &mut resource_ids,
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
            push_token_lineage_once(
                &mut token_lineages,
                &mut token_lineage_ids,
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
            push_resource_once(
                &mut resources,
                &mut resource_ids,
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
            ensure_binding_authority_identity_rows(
                pool,
                &mut token_lineages,
                &mut token_lineage_ids,
                &mut resources,
                &mut resource_ids,
                &name.logical_name_id,
                &segment,
            )
            .await?;
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
    let materialization_ms = materialization_started.elapsed().as_millis();

    let normalize_started = Instant::now();
    let by_kind = count_events_by_kind(&events);
    coalesce_name_surfaces_for_upsert(&mut surfaces);
    normalize_surface_bindings_for_upsert(&mut bindings)?;
    let normalize_ms = normalize_started.elapsed().as_millis();
    let closure_started = Instant::now();
    let closure_count = prepend_existing_open_binding_closures(pool, &mut bindings).await?;
    let closure_ms = closure_started.elapsed().as_millis();
    let token_lineages_started = Instant::now();
    upsert_token_lineages(pool, &token_lineages).await?;
    let token_lineages_upsert_ms = token_lineages_started.elapsed().as_millis();
    let resources_started = Instant::now();
    upsert_resources(pool, &resources).await?;
    let resources_upsert_ms = resources_started.elapsed().as_millis();
    let surfaces_started = Instant::now();
    upsert_name_surfaces(pool, &surfaces).await?;
    let surfaces_upsert_ms = surfaces_started.elapsed().as_millis();
    let binding_closures_started = Instant::now();
    if closure_count > 0 {
        upsert_surface_bindings(pool, &bindings[..closure_count]).await?;
    }
    let binding_closures_upsert_ms = binding_closures_started.elapsed().as_millis();
    let bindings_started = Instant::now();
    upsert_surface_bindings(pool, &bindings[closure_count..]).await?;
    let bindings_upsert_ms = bindings_started.elapsed().as_millis();
    let normalized_events_started = Instant::now();
    let normalized_event_upsert = upsert_normalized_events_with_summary(pool, &events).await?;
    let normalized_events_upsert_ms = normalized_events_started.elapsed().as_millis();

    tracing::info!(
        service = "adapters",
        adapter = DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
        chain,
        restrict_to_block_hashes,
        block_hash_count = block_hashes.len(),
        source_scope_target_count = source_scope.as_ref().map_or(0, Vec::len),
        active_emitter_count = active_emitters.len(),
        scanned_log_count,
        matched_log_count,
        history_count = surfaces.len(),
        resource_count = resources.len(),
        binding_count = bindings.len(),
        normalized_event_count = events.len(),
        normalized_event_inserted_count = normalized_event_upsert.inserted_count,
        active_emitters_ms,
        raw_log_load_ms,
        canonical_blocks_ms,
        reverse_claim_sources_ms,
        resolver_profile_gate_ms,
        same_tx_name_intro_ms,
        preload_name_metadata_ms,
        preload_restricted_histories_ms,
        migrated_registry_nodes_ms,
        apply_ms,
        materialization_ms,
        normalize_ms,
        closure_ms,
        token_lineages_upsert_ms,
        resources_upsert_ms,
        surfaces_upsert_ms,
        binding_closures_upsert_ms,
        bindings_upsert_ms,
        normalized_events_upsert_ms,
        total_ms = total_started.elapsed().as_millis(),
        "ENSv1 unwrapped-authority replay timing"
    );

    Ok(EnsV1UnwrappedAuthoritySyncSummary {
        scanned_log_count,
        matched_log_count,
        total_name_surface_count: surfaces.len(),
        total_resource_count: resources.len(),
        total_surface_binding_count: bindings.len(),
        total_normalized_event_count: events.len(),
        total_normalized_event_inserted_count: normalized_event_upsert.inserted_count,
        by_kind,
    })
}
