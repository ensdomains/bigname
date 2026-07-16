use super::*;
use anyhow::ensure;

mod state;
use state::{MAX_LIVE_STATE_ITEM_COUNT, ProfilePageState};

const PROFILE_REPLAY_MAX_BLOCK_SCAN_SPAN: i64 = 262_144;
const PROFILE_REPLAY_HARD_MAX_PAGE_LOG_COUNT: usize = 250_000;

pub(super) struct ResolverProfileStreamInput<'a> {
    pub(super) pool: &'a PgPool,
    pub(super) chain: &'a str,
    pub(super) raw_log_active_emitters: &'a [ActiveEmitter],
    pub(super) generic_resolver_event_sources: &'a [GenericResolverEventSource],
    pub(super) event_topics: &'a AuthorityEventTopics,
    pub(super) replay: &'a mut ResolverProfileReplayContext,
}

pub(super) async fn sync_resolver_profile_stream(
    input: ResolverProfileStreamInput<'_>,
) -> Result<EnsV1UnwrappedAuthoritySyncSummary> {
    let router = AuthorityRawLogStreamSourceRouter::new(
        input.raw_log_active_emitters,
        input.generic_resolver_event_sources,
        input.event_topics,
        None,
    )?;
    let mut from_block = input.replay.first_block_number;
    let mut scanned_log_count = 0usize;
    let mut matched_log_count = 0usize;
    let mut event_count = 0usize;
    let mut by_kind = BTreeMap::new();

    while from_block <= input.replay.last_block_number {
        let scan_to_block = from_block
            .checked_add(PROFILE_REPLAY_MAX_BLOCK_SCAN_SPAN - 1)
            .unwrap_or(input.replay.last_block_number)
            .min(input.replay.last_block_number);
        let mut connection = input
            .pool
            .acquire()
            .await
            .context("failed to acquire resolver-profile raw-log stream connection")?;
        let to_block = select_authority_raw_log_stream_to_block(
            &mut connection,
            input.chain,
            &router,
            input.event_topics,
            from_block,
            scan_to_block,
            input.replay.max_raw_logs_per_page,
            Some(input.replay.run_id),
        )
        .await?;
        let mut raw_logs = Vec::new();
        let page_scanned = stream_authority_raw_logs(
            &mut connection,
            input.chain,
            &router,
            input.event_topics,
            from_block,
            to_block,
            Some(input.replay.run_id),
            |raw_log| {
                raw_logs.push(raw_log);
                Ok(())
            },
        )
        .await?;
        drop(connection);
        ensure!(
            raw_logs.len() <= PROFILE_REPLAY_HARD_MAX_PAGE_LOG_COUNT,
            "resolver-profile page contains {} routed logs, exceeding hard bound {}",
            raw_logs.len(),
            PROFILE_REPLAY_HARD_MAX_PAGE_LOG_COUNT
        );
        scanned_log_count += page_scanned;

        let mut affected_keys = authority_state_keys_for_raw_logs(&raw_logs, input.event_topics)?;
        let mut state =
            ProfilePageState::load(input.pool, input.replay.run_id, &affected_keys).await?;
        if raw_logs
            .iter()
            .any(|raw_log| raw_log.contract_role.as_deref() == Some(CONTRACT_ROLE_REGISTRY_OLD))
        {
            bail!(
                "bounded resolver-profile replay cannot safely preload old-registry migration state"
            );
        }

        if !raw_logs.is_empty() {
            preload_page_histories(
                input.pool,
                input.chain,
                &raw_logs,
                &affected_keys,
                input.event_topics,
                &mut state,
            )
            .await?;
            let fact_nodes = resolver_profile_fact_nodes(&raw_logs, input.event_topics)?;
            let reverse_claim_sources =
                load_reverse_claim_sources_for_nodes(input.pool, input.chain, &fact_nodes).await?;
            preload_reverse_histories(
                input.pool,
                input.chain,
                raw_logs[0].block_number,
                &reverse_claim_sources,
                &mut state.reverse_histories,
            )
            .await?;
            let head = load_canonical_block_at_number(input.pool, input.chain, to_block).await?;
            let block_index = CanonicalBlockIndex {
                blocks: load_canonical_blocks_for_authority_logs_through_head(
                    input.pool,
                    input.chain,
                    &raw_logs,
                    &head,
                    input.event_topics,
                )
                .await?,
            };
            let intro_positions = name_intro_positions_for_raw_logs(&raw_logs, input.event_topics)?;
            let profile_gate =
                ResolverProfileGate::load_for_raw_logs(input.pool, &raw_logs, input.event_topics)
                    .await?;
            for raw_log in &raw_logs {
                if profile_gate.resolver_local_fact_profile_status(raw_log, input.event_topics)?
                    == Some(ResolverFactProfileStatus::Pending)
                {
                    bail!(
                        "resolver-profile reconciliation cannot publish pending profile evidence for {} on {} at block {}; wait for a complete code-hash classification",
                        raw_log.emitting_address,
                        raw_log.chain_id,
                        raw_log.block_number,
                    );
                }
            }
            let mut delta = UnwrappedAuthorityReplayCheckpointDelta::default();
            for raw_log in &raw_logs {
                if apply_authority_raw_log(
                    raw_log,
                    &mut state.histories,
                    &mut state.reverse_histories,
                    &mut state.known_names_by_namehash,
                    &mut state.known_name_refs_by_namehash,
                    &mut state.namehash_to_labelhash,
                    &mut state.pending_namehash_observations,
                    &intro_positions,
                    &mut state.migrated_registry_nodes,
                    &reverse_claim_sources,
                    &profile_gate,
                    &block_index,
                    input.event_topics,
                    Some(&mut delta),
                )? {
                    matched_log_count += 1;
                }
            }
            affected_keys.extend(delta.history_keys);
            affected_keys.extend(delta.reverse_history_keys);
            affected_keys.extend(delta.known_name_keys);
            affected_keys.extend(delta.known_name_ref_keys);
            affected_keys.extend(delta.namehash_labelhash_keys);
            affected_keys.extend(delta.pending_observation_keys);
            affected_keys.extend(delta.migrated_nodes);
        }

        ensure!(
            state.live_item_count() <= MAX_LIVE_STATE_ITEM_COUNT,
            "resolver-profile page live state exceeds hard item bound"
        );
        let events = state.drain_resolver_events();
        merge_event_kind_counts(&mut by_kind, count_events_by_kind(&events));
        event_count += events.len();
        let payload_bytes = state
            .persist(input.pool, input.replay.run_id, &affected_keys, &events)
            .await?;
        input
            .replay
            .record_page(raw_logs.len(), state.live_item_count(), payload_bytes);
        from_block = to_block
            .checked_add(1)
            .context("resolver-profile page boundary overflowed")?;
    }

    Ok(EnsV1UnwrappedAuthoritySyncSummary {
        scanned_log_count,
        matched_log_count,
        total_name_surface_count: 0,
        total_resource_count: 0,
        total_surface_binding_count: 0,
        total_normalized_event_count: event_count,
        total_normalized_event_inserted_count: 0,
        by_kind,
    })
}

async fn preload_page_histories(
    pool: &PgPool,
    chain: &str,
    raw_logs: &[AuthorityRawLogRow],
    affected_keys: &BTreeSet<String>,
    event_topics: &AuthorityEventTopics,
    state: &mut ProfilePageState,
) -> Result<()> {
    let mut known_names = state.known_names_by_namehash.clone();
    preload_name_metadata_for_raw_logs(pool, raw_logs, &mut known_names, event_topics).await?;
    let mut labelhashes = state.namehash_to_labelhash.clone();
    for name in known_names.values() {
        if let Some(labelhash) = name.labelhashes.first() {
            labelhashes
                .entry(name.namehash.clone())
                .or_insert_with(|| labelhash.clone());
        }
    }
    let head = load_canonical_block_at_number(
        pool,
        chain,
        raw_logs
            .last()
            .context("page raw logs disappeared")?
            .block_number,
    )
    .await?;
    let block_index = CanonicalBlockIndex {
        blocks: load_canonical_blocks_for_authority_logs_through_head(
            pool,
            chain,
            raw_logs,
            &head,
            event_topics,
        )
        .await?,
    };
    let mut histories = BTreeMap::new();
    let mut known_refs = state.known_name_refs_by_namehash.clone();
    preload_restricted_name_histories(
        pool,
        chain,
        raw_logs,
        &mut histories,
        &mut known_names,
        &mut known_refs,
        &mut labelhashes,
        &block_index,
        event_topics,
    )
    .await?;
    for key in affected_keys {
        if let Some(history) = histories.remove(key) {
            state.histories.entry(key.clone()).or_insert(history);
        }
        if let Some(value) = known_names.remove(key) {
            state
                .known_names_by_namehash
                .entry(key.clone())
                .or_insert(value);
        }
        if let Some(value) = known_refs.remove(key) {
            state
                .known_name_refs_by_namehash
                .entry(key.clone())
                .or_insert(value);
        }
        if let Some(value) = labelhashes.remove(key) {
            state
                .namehash_to_labelhash
                .entry(key.clone())
                .or_insert(value);
        }
    }
    Ok(())
}

async fn preload_reverse_histories(
    pool: &PgPool,
    chain: &str,
    before_block: i64,
    sources: &HashMap<String, ReverseClaimSource>,
    histories: &mut BTreeMap<String, ReverseClaimSourceHistory>,
) -> Result<()> {
    let missing = sources
        .keys()
        .filter(|node| !histories.contains_key(*node))
        .cloned()
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Ok(());
    }
    let rows = sqlx::query_as::<_, (String, Option<String>, Option<i64>)>(
        r#"
        SELECT DISTINCT ON (LOWER(after_state->>'namehash'))
            LOWER(after_state->>'namehash'),
            NULLIF(LOWER(after_state->>'resolver'), ''),
            NULLIF(after_state->>'record_version', '')::BIGINT
        FROM normalized_events
        WHERE chain_id = $1
          AND derivation_kind = $2
          AND event_kind IN ($3, $4)
          AND block_number < $5
          AND LOWER(after_state->>'namehash') = ANY($6::TEXT[])
          AND canonicality_state IN ('canonical', 'safe', 'finalized')
        ORDER BY LOWER(after_state->>'namehash'), block_number DESC, log_index DESC NULLS LAST
        "#,
    )
    .bind(chain)
    .bind(DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY)
    .bind(EVENT_KIND_RESOLVER_CHANGED)
    .bind(EVENT_KIND_RECORD_VERSION_CHANGED)
    .bind(before_block)
    .bind(&missing)
    .fetch_all(pool)
    .await
    .context("failed to preload reverse-source resolver state")?;
    let previous = rows
        .into_iter()
        .map(|(node, resolver, version)| (node, (resolver, version)))
        .collect::<HashMap<_, _>>();
    for node in missing {
        let source = sources
            .get(&node)
            .cloned()
            .context("reverse claim source disappeared")?;
        let (current_resolver, current_record_version) =
            previous.get(&node).cloned().unwrap_or_default();
        histories.insert(
            node,
            ReverseClaimSourceHistory {
                claim_source: source,
                current_resolver,
                current_record_version,
                events: Vec::new(),
            },
        );
    }
    Ok(())
}
