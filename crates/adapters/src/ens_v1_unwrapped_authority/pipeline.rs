use super::*;

mod apply;
mod entrypoints;
mod finalize;
mod identity;
mod materialize;
mod summary;
use apply::*;
pub use entrypoints::*;
use finalize::{FinalizeAuthoritySync, PreMaterializationTimings, finalize_authority_sync};
use identity::ensure_binding_authority_identity_rows;
use summary::empty_summary;

const FULL_REPLAY_RAW_LOG_STREAM_MAX_BLOCK_SCAN_SPAN: i64 = 262_144;
const FULL_REPLAY_RAW_LOG_STREAM_DEFAULT_MAX_LOGS_PER_PAGE: usize = 100_000;

pub(super) async fn sync_ens_v1_unwrapped_authority_with_scope(
    pool: &PgPool,
    chain: &str,
    target_block_number: Option<i64>,
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
    transaction_hashes: Option<&[String]>,
    source_scope: Option<&[(String, String, i64, i64)]>,
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
    let mut raw_log_active_emitters = Vec::new();
    for emitter in &active_emitters {
        if generic_resolver_event_sources.is_empty()
            || emitter.source_family != SOURCE_FAMILY_ENS_V1_RESOLVER_L1
        {
            raw_log_active_emitters.push(emitter.clone());
        }
    }
    let active_emitters_ms = active_emitters_started.elapsed().as_millis();
    if active_emitters.is_empty() && generic_resolver_event_sources.is_empty() {
        return Ok(empty_summary(0));
    }
    let event_topics = AuthorityEventTopics::load_for_authority_sources(
        pool,
        chain,
        &active_emitters,
        &generic_resolver_event_sources,
    )
    .await?;
    let max_raw_logs_per_page = FULL_REPLAY_RAW_LOG_STREAM_DEFAULT_MAX_LOGS_PER_PAGE;
    let mut histories = BTreeMap::<String, NameHistory>::new();
    let mut reverse_histories = BTreeMap::<String, ReverseClaimSourceHistory>::new();
    let mut known_names_by_namehash = HashMap::<String, NameMetadata>::new();
    let mut known_name_refs_by_namehash = HashMap::<String, ObservationRef>::new();
    let mut namehash_to_labelhash = HashMap::<String, String>::new();
    let mut pending_namehash_observations = HashMap::<String, Vec<AuthorityObservation>>::new();
    let mut migrated_registry_nodes = MigratedRegistryNodes::empty();
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
        let canonical_blocks_started = Instant::now();
        let canonical_blocks = load_canonical_blocks(pool, chain, target_block_number).await?;
        canonical_blocks_ms = canonical_blocks_started.elapsed().as_millis();
        if canonical_blocks.is_empty() {
            return Ok(empty_summary(0));
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

        let stream_apply_started = Instant::now();
        let stream_source_router = AuthorityRawLogStreamSourceRouter::new(
            &raw_log_active_emitters,
            &generic_resolver_event_sources,
            &event_topics,
            None,
        )?;
        let mut stream_conn = None;
        let mut total_scanned_log_count = 0usize;
        let mut page_from_block = first_block.block_number;
        let mut stream_page_count = 0usize;
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
                |raw_log| {
                    page_raw_logs.push(raw_log);
                    Ok(())
                },
            )
            .await?;
            // Release the page stream connection before auxiliary reads so a
            // small connection pool can still make forward progress.
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
                )? {
                    matched_log_count += 1;
                }
            }
            stream_page_count += 1;
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
            )? {
                matched_log_count += 1;
            }
        }
        apply_ms = apply_started.elapsed().as_millis();
    }

    if scanned_log_count == 0 {
        return Ok(empty_summary(scanned_log_count));
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
