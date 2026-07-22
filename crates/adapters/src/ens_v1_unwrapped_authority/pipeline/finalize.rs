use super::{
    close_binding_overlaps, count_events_by_kind,
    materialize::{AuthorityMaterialization, materialize_authority_histories},
    merge_event_kind_counts, normalize_surface_bindings_for_upsert,
    prepend_existing_open_binding_closures,
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

pub(super) struct FinalizeAuthoritySync<'a, 'progress> {
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
    pub(super) flushed_events: UnwrappedAuthorityReplayFlushedEvents,
    pub(super) active_replay_checkpoint: &'a mut Option<UnwrappedAuthorityReplayCheckpoint>,
    pub(super) startup_progress: Option<&'progress mut dyn StartupAdapterProgress>,
    pub(super) pre_timings: PreMaterializationTimings,
    pub(super) total_started: Instant,
}

pub(super) async fn finalize_authority_sync(
    mut input: FinalizeAuthoritySync<'_, '_>,
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
        mut events,
        token_lineages_upsert_ms,
        resources_upsert_ms,
        surfaces_upsert_ms,
    } = materialize_authority_histories(
        input.pool,
        input.chain,
        &head_ref,
        input.histories,
        input.reverse_histories,
        &mut input.startup_progress,
    )
    .await?;
    record_startup_adapter_progress(input.pool, &mut input.startup_progress).await?;
    let materialization_ms = materialization_started.elapsed().as_millis();

    let normalize_started = Instant::now();
    let mut by_kind = input.flushed_events.by_kind.clone();
    merge_event_kind_counts(&mut by_kind, count_events_by_kind(&events));
    normalize_surface_bindings_for_upsert(&mut bindings)?;
    record_startup_adapter_progress(input.pool, &mut input.startup_progress).await?;
    let normalize_ms = normalize_started.elapsed().as_millis();
    let closure_started = Instant::now();
    let closure_count = prepend_existing_open_binding_closures_with_progress(
        input.pool,
        &mut bindings,
        &mut input.startup_progress,
    )
    .await?;
    let closure_ms = closure_started.elapsed().as_millis();
    let binding_closures_started = Instant::now();
    if closure_count > 0 {
        upsert_binding_batches(
            input.pool,
            &bindings[..closure_count],
            &mut input.startup_progress,
        )
        .await?;
    }
    let binding_closures_upsert_ms = binding_closures_started.elapsed().as_millis();
    let (binding_overlap_repair_count, binding_overlap_repair_ms) = close_binding_overlaps(
        input.pool,
        &bindings[closure_count..],
        &mut input.startup_progress,
    )
    .await?;
    let bindings_started = Instant::now();
    upsert_binding_batches(
        input.pool,
        &bindings[closure_count..],
        &mut input.startup_progress,
    )
    .await?;
    let bindings_upsert_ms = bindings_started.elapsed().as_millis();
    let binding_count = bindings.len();
    drop(bindings);
    if let Some(checkpoint) = input
        .active_replay_checkpoint
        .as_mut()
        .filter(|checkpoint| checkpoint.is_startup())
    {
        checkpoint
            .publish_startup_events(
                input.pool,
                &mut input.flushed_events,
                &mut input.startup_progress,
            )
            .await?;
    }
    let normalized_events_started = Instant::now();
    let normalized_event_count = events.len();
    let event_inserted_count =
        upsert_event_batches(input.pool, &mut events, &mut input.startup_progress).await?;
    let normalized_events_upsert_ms = normalized_events_started.elapsed().as_millis();
    drop(events);

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
        (
            normalized_event_count,
            event_inserted_count,
            input.flushed_events.total_count,
            input.flushed_events.inserted_count,
        ),
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
            (materialization_ms, normalize_ms, closure_ms),
            (
                token_lineages_upsert_ms,
                resources_upsert_ms,
                surfaces_upsert_ms,
                binding_closures_upsert_ms,
                binding_overlap_repair_count,
                binding_overlap_repair_ms,
                bindings_upsert_ms,
                normalized_events_upsert_ms,
            ),
            input.total_started.elapsed().as_millis(),
        ),
    ));

    let summary = build_summary(
        input.scanned_log_count,
        input.matched_log_count,
        (surface_count, resource_count, binding_count),
        (
            input.flushed_events.total_count,
            input.flushed_events.inserted_count,
        ),
        (normalized_event_count, event_inserted_count),
        by_kind,
    );
    if let Some(checkpoint) = input.active_replay_checkpoint.as_mut() {
        checkpoint.mark_completed(input.pool, &summary).await?;
        record_startup_adapter_progress(input.pool, &mut input.startup_progress).await?;
    }
    Ok(summary)
}

const FINALIZATION_BINDING_NAME_BATCH_SIZE: usize = 1_000;
const FINALIZATION_EVENT_BATCH_SIZE: usize = 5_000;

async fn prepend_existing_open_binding_closures_with_progress(
    pool: &PgPool,
    bindings: &mut Vec<SurfaceBinding>,
    startup_progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<usize> {
    if startup_progress.is_none() {
        return prepend_existing_open_binding_closures(pool, bindings).await;
    }

    let mut source = std::mem::take(bindings).into_iter().peekable();
    let mut closures = Vec::new();
    let mut incoming = Vec::new();
    while source.peek().is_some() {
        let mut batch = Vec::new();
        for _ in 0..FINALIZATION_BINDING_NAME_BATCH_SIZE {
            let Some(logical_name_id) =
                source.peek().map(|binding| binding.logical_name_id.clone())
            else {
                break;
            };
            while source
                .peek()
                .is_some_and(|binding| binding.logical_name_id == logical_name_id)
            {
                batch.push(source.next().expect("peeked binding must remain available"));
            }
        }
        let closure_count = prepend_existing_open_binding_closures(pool, &mut batch).await?;
        incoming.extend(batch.drain(closure_count..));
        closures.extend(batch);
        record_startup_adapter_progress(pool, startup_progress).await?;
    }
    let closure_count = closures.len();
    closures.append(&mut incoming);
    *bindings = closures;
    Ok(closure_count)
}

async fn upsert_binding_batches(
    pool: &PgPool,
    bindings: &[SurfaceBinding],
    startup_progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    if startup_progress.is_none() {
        return upsert_surface_bindings_without_snapshots(pool, bindings).await;
    }
    for chunk in bindings.chunks(FINALIZATION_BINDING_NAME_BATCH_SIZE) {
        upsert_surface_bindings_without_snapshots(pool, chunk).await?;
        record_startup_adapter_progress(pool, startup_progress).await?;
    }
    Ok(())
}

async fn upsert_event_batches(
    pool: &PgPool,
    events: &mut [NormalizedEvent],
    startup_progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<usize> {
    if startup_progress.is_none() {
        return event_persistence::upsert_events_preserving_manifest_provenance(pool, events).await;
    }
    let mut inserted_count = 0usize;
    for chunk in events.chunks_mut(FINALIZATION_EVENT_BATCH_SIZE) {
        inserted_count +=
            event_persistence::upsert_events_preserving_manifest_provenance(pool, chunk).await?;
        record_startup_adapter_progress(pool, startup_progress).await?;
    }
    Ok(inserted_count)
}
