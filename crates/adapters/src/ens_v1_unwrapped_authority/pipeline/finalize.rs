use super::{
    count_events_by_kind,
    materialize::{AuthorityMaterialization, materialize_authority_histories},
    normalize_surface_bindings_for_upsert,
    summary::*,
    *,
};

pub(super) struct PreMaterializationTimings {
    pub(super) active_emitters_ms: u128,
    pub(super) raw_log_load_ms: u128,
    pub(super) canonical_blocks_ms: u128,
    pub(super) reverse_claim_sources_ms: u128,
    pub(super) resolver_profile_gate_ms: u128,
    pub(super) same_tx_name_intro_ms: u128,
    pub(super) preload_name_metadata_ms: u128,
    pub(super) preload_restricted_histories_ms: u128,
    pub(super) migrated_registry_nodes_ms: u128,
    pub(super) apply_ms: u128,
}

pub(super) struct FinalizeAuthoritySync<'a> {
    pub(super) pool: &'a PgPool,
    pub(super) chain: &'a str,
    pub(super) restrict_to_block_hashes: bool,
    pub(super) block_hash_count: usize,
    pub(super) source_scope_target_count: usize,
    pub(super) active_emitter_count: usize,
    pub(super) scanned_log_count: usize,
    pub(super) matched_log_count: usize,
    pub(super) block_index: &'a CanonicalBlockIndex,
    pub(super) active_emitters: &'a [ActiveEmitter],
    pub(super) generic_resolver_event_sources: &'a [GenericResolverEventSource],
    pub(super) histories: BTreeMap<String, NameHistory>,
    pub(super) reverse_histories: BTreeMap<String, ReverseClaimSourceHistory>,
    pub(super) pre_timings: PreMaterializationTimings,
    pub(super) total_started: Instant,
}

pub(super) async fn finalize_authority_sync(
    input: FinalizeAuthoritySync<'_>,
) -> Result<EnsV1UnwrappedAuthoritySyncSummary> {
    let head_block = input
        .block_index
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
        namespace: input
            .active_emitters
            .first()
            .map(|emitter| emitter.namespace.clone())
            .or_else(|| {
                input
                    .generic_resolver_event_sources
                    .first()
                    .map(|source| source.namespace.clone())
            })
            .unwrap_or_else(|| "ens".to_owned()),
    };

    let materialization_started = Instant::now();
    let AuthorityMaterialization {
        token_lineage_count,
        resource_count,
        surface_count,
        mut bindings,
        events,
        token_lineages_upsert_ms,
        resources_upsert_ms,
        surfaces_upsert_ms,
    } = materialize_authority_histories(
        input.pool,
        input.chain,
        &head_ref,
        input.histories,
        input.reverse_histories,
    )
    .await?;
    let materialization_ms = materialization_started.elapsed().as_millis();

    let normalize_started = Instant::now();
    let by_kind = count_events_by_kind(&events);
    normalize_surface_bindings_for_upsert(&mut bindings)?;
    let normalize_ms = normalize_started.elapsed().as_millis();

    let bindings_started = Instant::now();
    upsert_surface_bindings_without_snapshots(input.pool, &bindings).await?;
    let bindings_upsert_ms = bindings_started.elapsed().as_millis();
    let binding_count = bindings.len();

    let normalized_events_started = Instant::now();
    let normalized_event_count = events.len();
    let event_inserted_count =
        bigname_storage::upsert_normalized_events_with_summary(input.pool, &events)
            .await?
            .inserted_count;
    let normalized_events_upsert_ms = normalized_events_started.elapsed().as_millis();

    log_replay_timing(ReplayTimingLog::new(
        input.chain,
        (
            input.restrict_to_block_hashes,
            input.block_hash_count,
            input.source_scope_target_count,
            input.active_emitter_count,
        ),
        (input.scanned_log_count, input.matched_log_count),
        (
            surface_count,
            token_lineage_count,
            resource_count,
            binding_count,
        ),
        (normalized_event_count, event_inserted_count, 0, 0),
        ReplayTimings::new(
            (
                input.pre_timings.active_emitters_ms,
                input.pre_timings.raw_log_load_ms,
                input.pre_timings.canonical_blocks_ms,
                input.pre_timings.reverse_claim_sources_ms,
                input.pre_timings.resolver_profile_gate_ms,
            ),
            (
                input.pre_timings.same_tx_name_intro_ms,
                input.pre_timings.preload_name_metadata_ms,
                input.pre_timings.preload_restricted_histories_ms,
                input.pre_timings.migrated_registry_nodes_ms,
                input.pre_timings.apply_ms,
            ),
            (materialization_ms, normalize_ms, 0),
            (
                token_lineages_upsert_ms,
                resources_upsert_ms,
                surfaces_upsert_ms,
                0,
                0,
                0,
                bindings_upsert_ms,
                normalized_events_upsert_ms,
            ),
            input.total_started.elapsed().as_millis(),
        ),
    ));

    Ok(build_summary(
        input.scanned_log_count,
        input.matched_log_count,
        (surface_count, resource_count, binding_count),
        (0, 0),
        (normalized_event_count, event_inserted_count),
        by_kind,
    ))
}
