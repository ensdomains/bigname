use super::resolver_profile_reconciliation::ResolverProfileReplayContext;
use super::*;
use crate::checkpoint_context::{
    AdapterCheckpointContext, StartupAdapterProgress, record_startup_adapter_progress,
};
use anyhow::ensure;

mod apply;
mod entrypoints;
mod finalize;
mod flush;
mod identity;
mod materialize;
mod profile_stream;
mod summary;
use apply::*;
pub use entrypoints::*;
use finalize::{FinalizeAuthoritySync, PreMaterializationTimings, finalize_authority_sync};
use flush::*;
use identity::*;
use profile_stream::{ResolverProfileStreamInput, sync_resolver_profile_stream};
use summary::empty_summary;

const FULL_REPLAY_RAW_LOG_STREAM_MAX_BLOCK_SCAN_SPAN: i64 = 262_144;
const FULL_REPLAY_RAW_LOG_STREAM_DEFAULT_MAX_LOGS_PER_PAGE: usize = 100_000;

pub(super) async fn sync_ens_v1_unwrapped_authority_with_scope(
    pool: &PgPool,
    chain: &str,
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
    transaction_hashes: Option<&[String]>,
    source_scope: Option<&[(String, String, i64, i64)]>,
    replay_checkpoint: Option<&AdapterCheckpointContext>,
    replay_max_raw_logs_per_page: Option<usize>,
    resolver_profile_replay: Option<&mut ResolverProfileReplayContext>,
    mut startup_progress: Option<&mut dyn StartupAdapterProgress>,
) -> Result<EnsV1UnwrappedAuthoritySyncSummary> {
    let source_scope = source_scope.map(normalized_authority_source_scope_targets);
    let total_started = Instant::now();
    if source_scope.as_ref().is_some_and(Vec::is_empty) {
        return Ok(empty_summary(0));
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
        let summary = empty_summary(0);
        if let Some(context) = replay_checkpoint.filter(|context| context.is_startup()) {
            let mut checkpoint =
                UnwrappedAuthorityReplayCheckpoint::load_or_start(pool, chain, context).await?;
            if let Some(completed_summary) = checkpoint.completed_summary()? {
                return Ok(completed_summary);
            }
            checkpoint.mark_completed(pool, &summary).await?;
        }
        return Ok(summary);
    }
    let event_topics = AuthorityEventTopics::load_for_authority_sources(
        pool,
        chain,
        &active_emitters,
        &generic_resolver_event_sources,
    )
    .await?;
    if let Some(replay) = resolver_profile_replay {
        ensure!(
            !restrict_to_block_hashes
                && transaction_hashes.is_none()
                && source_scope.is_none()
                && replay_checkpoint.is_none(),
            "resolver-profile replay cannot be combined with another restricted or checkpointed authority scope"
        );
        return sync_resolver_profile_stream(
            ResolverProfileStreamInput {
                pool,
                chain,
                raw_log_active_emitters: &raw_log_active_emitters,
                generic_resolver_event_sources: &generic_resolver_event_sources,
                event_topics: &event_topics,
                replay,
            },
            &mut startup_progress,
        )
        .await;
    }
    let max_raw_logs_per_page = replay_max_raw_logs_per_page
        .unwrap_or(FULL_REPLAY_RAW_LOG_STREAM_DEFAULT_MAX_LOGS_PER_PAGE);
    if max_raw_logs_per_page == 0 {
        bail!("ENSv1 unwrapped-authority replay max logs per page must be positive");
    }
    let mut histories = BTreeMap::<String, NameHistory>::new();
    let mut reverse_histories = BTreeMap::<String, ReverseClaimSourceHistory>::new();
    let mut known_names_by_namehash = HashMap::<String, NameMetadata>::new();
    let mut known_name_refs_by_namehash = HashMap::<String, ObservationRef>::new();
    let mut namehash_to_labelhash = HashMap::<String, String>::new();
    let mut pending_namehash_observations = HashMap::<String, Vec<AuthorityObservation>>::new();
    let mut migrated_registry_nodes = MigratedRegistryNodes::empty();
    let mut active_replay_checkpoint = None::<UnwrappedAuthorityReplayCheckpoint>;
    let mut flushed_events = UnwrappedAuthorityReplayFlushedEvents::default();
    let scanned_log_count;
    let block_index;
    let mut matched_log_count = 0usize;
    let mut raw_log_load_ms = 0;
    let canonical_blocks_ms;
    let reverse_claim_sources_ms;
    let mut same_tx_name_intro_ms = 0;
    let mut preload_name_metadata_ms = 0;
    let mut preload_restricted_histories_ms = 0;
    let mut migrated_registry_nodes_ms = 0;
    let mut resolver_profile_gate_ms = 0;
    let apply_ms;
    if !restrict_to_block_hashes && source_scope.is_none() {
        if let Some(context) = replay_checkpoint {
            let checkpoint =
                UnwrappedAuthorityReplayCheckpoint::load_or_start(pool, chain, context).await?;
            if let Some(summary) = checkpoint.completed_summary()? {
                return Ok(summary);
            }
            active_replay_checkpoint = Some(checkpoint);
        }
        let canonical_blocks_started = Instant::now();
        let canonical_blocks = load_canonical_blocks(
            pool,
            chain,
            active_replay_checkpoint
                .as_ref()
                .map(UnwrappedAuthorityReplayCheckpoint::target_block_number),
        )
        .await?;
        canonical_blocks_ms = canonical_blocks_started.elapsed().as_millis();
        if canonical_blocks.is_empty() {
            let summary = empty_summary(0);
            if let Some(checkpoint) = active_replay_checkpoint
                .as_mut()
                .filter(|checkpoint| checkpoint.is_startup())
            {
                checkpoint.mark_completed(pool, &summary).await?;
            }
            return Ok(summary);
        }
        block_index = CanonicalBlockIndex {
            blocks: canonical_blocks,
        };
        let first_block = block_index
            .blocks
            .first()
            .cloned()
            .context("canonical block index must contain a first block")?;
        let head_block = block_index
            .blocks
            .last()
            .cloned()
            .context("canonical block index must contain a head block")?;
        let reverse_claim_sources_started = Instant::now();
        let reverse_claim_sources = load_reverse_claim_sources(pool, chain).await?;
        reverse_claim_sources_ms = reverse_claim_sources_started.elapsed().as_millis();
        if let Some(checkpoint) = active_replay_checkpoint.as_ref() {
            let include_replay_auxiliary_state = checkpoint.needs_replay_auxiliary_state();
            if let Some(state) = checkpoint
                .load_state(pool, include_replay_auxiliary_state)
                .await?
            {
                histories = state.histories;
                reverse_histories = state.reverse_histories;
                known_names_by_namehash = state.known_names_by_namehash;
                known_name_refs_by_namehash = state.known_name_refs_by_namehash;
                namehash_to_labelhash = state.namehash_to_labelhash;
                pending_namehash_observations = state.pending_namehash_observations;
                migrated_registry_nodes = state.migrated_registry_nodes;
                matched_log_count = checkpoint.matched_log_count();
                flushed_events = checkpoint.flushed_events().clone();
            }
        }

        let stream_apply_started = Instant::now();
        let stream_source_router = AuthorityRawLogStreamSourceRouter::new(
            &raw_log_active_emitters,
            &generic_resolver_event_sources,
            &event_topics,
            None,
        )?;
        let mut stream_conn = None;
        let mut total_scanned_log_count = active_replay_checkpoint.as_ref().map_or(
            0usize,
            UnwrappedAuthorityReplayCheckpoint::scanned_log_count,
        );
        matched_log_count = active_replay_checkpoint.as_ref().map_or(
            matched_log_count,
            UnwrappedAuthorityReplayCheckpoint::matched_log_count,
        );
        let mut page_from_block = active_replay_checkpoint
            .as_ref()
            .and_then(UnwrappedAuthorityReplayCheckpoint::last_block_number)
            .map(|block_number| {
                block_number
                    .checked_add(1)
                    .context("authority replay checkpoint block boundary overflowed")
            })
            .transpose()?
            .unwrap_or(first_block.block_number)
            .max(first_block.block_number);
        let mut stream_page_count = 0usize;
        let mut checkpoint_delta = UnwrappedAuthorityReplayCheckpointDelta::default();
        while page_from_block <= head_block.block_number {
            if stream_conn.is_none() {
                let conn = pool
                    .acquire()
                    .await
                    .context("failed to acquire authority raw-log stream connection")?;
                stream_conn = Some(conn);
            }
            let conn = stream_conn
                .as_mut()
                .expect("authority raw-log stream connection was prepared");
            let raw_log_scan_to_block = page_from_block
                .checked_add(FULL_REPLAY_RAW_LOG_STREAM_MAX_BLOCK_SCAN_SPAN - 1)
                .unwrap_or(head_block.block_number)
                .min(head_block.block_number);
            let page_to_block = select_authority_raw_log_stream_to_block(
                &mut *conn,
                chain,
                &stream_source_router,
                &event_topics,
                page_from_block,
                raw_log_scan_to_block,
                max_raw_logs_per_page,
                None,
            )
            .await?;
            let mut page_raw_logs = Vec::new();
            total_scanned_log_count += stream_authority_raw_logs(
                &mut *conn,
                chain,
                &stream_source_router,
                &event_topics,
                page_from_block,
                page_to_block,
                None,
                |raw_log| {
                    page_raw_logs.push(raw_log);
                    Ok(())
                },
            )
            .await?;
            // The checkpointed invocation already dedicates one pooled
            // connection to the raw-log mutation fence. Release the page
            // stream connection before auxiliary reads and checkpoint writes
            // so a two-connection pool can still make forward progress.
            drop(stream_conn.take());
            let page_intro_positions =
                name_intro_positions_for_raw_logs(&page_raw_logs, &event_topics)?;
            let resolver_profile_gate_started = Instant::now();
            let page_resolver_profile_gate =
                ResolverProfileGate::load_for_raw_logs(pool, &page_raw_logs, &event_topics).await?;
            resolver_profile_gate_ms += resolver_profile_gate_started.elapsed().as_millis();
            for raw_log in &page_raw_logs {
                if apply_authority_raw_log(
                    raw_log,
                    &mut histories,
                    &mut reverse_histories,
                    &mut known_names_by_namehash,
                    &mut known_name_refs_by_namehash,
                    &mut namehash_to_labelhash,
                    &mut pending_namehash_observations,
                    &page_intro_positions,
                    &mut migrated_registry_nodes,
                    &reverse_claim_sources,
                    &page_resolver_profile_gate,
                    &block_index,
                    &event_topics,
                    active_replay_checkpoint
                        .as_ref()
                        .map(|_| &mut checkpoint_delta),
                )? {
                    matched_log_count += 1;
                }
            }
            stream_page_count += 1;
            if let Some(checkpoint) = active_replay_checkpoint.as_ref() {
                let flushed_event_count = if checkpoint.is_startup() {
                    stage_startup_checkpoint_events(
                        pool,
                        checkpoint,
                        &mut histories,
                        &mut reverse_histories,
                        &mut checkpoint_delta,
                        &mut flushed_events,
                    )
                    .await?
                } else {
                    flush_staged_replay_events(
                        pool,
                        &mut histories,
                        &mut reverse_histories,
                        &mut checkpoint_delta,
                        &mut flushed_events,
                    )
                    .await?
                };
                let checkpoint = active_replay_checkpoint
                    .as_mut()
                    .context("authority replay checkpoint disappeared before saving")?;
                checkpoint
                    .save_progress(
                        pool,
                        page_to_block,
                        total_scanned_log_count,
                        matched_log_count,
                        UnwrappedAuthorityReplayCheckpointStateRef {
                            histories: &histories,
                            reverse_histories: &reverse_histories,
                            known_names_by_namehash: &known_names_by_namehash,
                            known_name_refs_by_namehash: &known_name_refs_by_namehash,
                            namehash_to_labelhash: &namehash_to_labelhash,
                            pending_namehash_observations: &pending_namehash_observations,
                            migrated_registry_nodes: &migrated_registry_nodes,
                        },
                        &checkpoint_delta,
                        &flushed_events,
                    )
                    .await?;
                record_startup_adapter_progress(pool, &mut startup_progress).await?;
                tracing::info!(
                    service = "adapters",
                    adapter = DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
                    chain,
                    max_raw_logs_per_page,
                    checkpoint_block_number = page_to_block,
                    scanned_log_count = total_scanned_log_count,
                    matched_log_count,
                    dirty_history_count = checkpoint_delta.history_keys.len(),
                    dirty_reverse_history_count = checkpoint_delta.reverse_history_keys.len(),
                    dirty_aux_item_count = checkpoint_delta.known_name_keys.len()
                        + checkpoint_delta.known_name_ref_keys.len()
                        + checkpoint_delta.namehash_labelhash_keys.len()
                        + checkpoint_delta.pending_observation_keys.len()
                        + checkpoint_delta.migrated_nodes.len(),
                    flushed_event_count,
                    flushed_normalized_event_count = flushed_events.total_count,
                    flushed_normalized_event_inserted_count = flushed_events.inserted_count,
                    "ENSv1 unwrapped-authority replay checkpoint saved"
                );
                checkpoint_delta.clear();
            }
            tracing::info!(
                service = "adapters",
                adapter = DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
                chain,
                page_from_block,
                page_to_block,
                raw_log_scan_to_block,
                stream_page_count,
                max_raw_logs_per_page,
                scanned_log_count = total_scanned_log_count,
                matched_log_count,
                elapsed_ms = stream_apply_started.elapsed().as_millis(),
                "ENSv1 unwrapped-authority replay stream progress"
            );
            page_from_block = page_to_block
                .checked_add(1)
                .context("authority raw-log stream page boundary overflowed")?;
        }
        drop(stream_conn);
        if let Some(checkpoint) = active_replay_checkpoint.as_mut() {
            checkpoint
                .mark_stream_complete(pool, total_scanned_log_count, matched_log_count)
                .await?;
            record_startup_adapter_progress(pool, &mut startup_progress).await?;
        }
        scanned_log_count = total_scanned_log_count;
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
            transaction_hashes,
            source_scope.as_deref(),
        )
        .await?;
        raw_log_load_ms = raw_log_load_started.elapsed().as_millis();
        scanned_log_count = raw_logs.len();
        if raw_logs.is_empty() {
            return Ok(empty_summary(scanned_log_count));
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
            return Ok(empty_summary(scanned_log_count));
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
        resolver_profile_gate_ms += resolver_profile_gate_started.elapsed().as_millis();
        let same_tx_name_intro_started = Instant::now();
        let same_tx_name_intro_positions =
            name_intro_positions_for_raw_logs(&raw_logs, &event_topics)?;
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
                None,
            )? {
                matched_log_count += 1;
            }
        }
        apply_ms = apply_started.elapsed().as_millis();
    }

    if scanned_log_count == 0 {
        let summary = empty_summary(scanned_log_count);
        if let Some(checkpoint) = active_replay_checkpoint
            .as_mut()
            .filter(|checkpoint| checkpoint.is_startup())
        {
            checkpoint.mark_completed(pool, &summary).await?;
        }
        return Ok(summary);
    }

    if let Some(checkpoint) = active_replay_checkpoint.as_ref() {
        checkpoint.ensure_raw_log_input_current(pool).await?;
    }

    finalize_authority_sync(FinalizeAuthoritySync {
        pool,
        chain,
        restrict_to_block_hashes,
        block_hash_count: block_hashes.len(),
        source_scope_target_count: source_scope.as_ref().map_or(0, Vec::len),
        active_emitter_count: active_emitters.len(),
        scanned_log_count,
        matched_log_count,
        block_index: &block_index,
        active_emitters: &active_emitters,
        generic_resolver_event_sources: &generic_resolver_event_sources,
        histories,
        reverse_histories,
        flushed_events,
        active_replay_checkpoint: &mut active_replay_checkpoint,
        startup_progress,
        pre_timings: PreMaterializationTimings {
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
        },
        total_started,
    })
    .await
}
