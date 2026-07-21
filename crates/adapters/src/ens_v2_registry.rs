use std::collections::{BTreeMap, HashSet};

use anyhow::{Context, Result};
use bigname_manifests::DiscoveryObservation;
use bigname_storage::{
    NameSurface, NormalizedEvent, Resource, SurfaceBinding, TokenLineage, upsert_name_surfaces,
    upsert_normalized_events_with_summary, upsert_resources, upsert_token_lineages,
};
use sqlx::PgPool;
use sqlx::types::Uuid;

mod constants;
mod decode;
mod discovery;
mod emitters;
mod events;
mod identity;
mod live;
mod load;
mod names;
mod normalized;
mod recovery;
mod types;
mod util;

use crate::adapter_manifest::load_required_active_manifest_event_topic0s_by_signature;
use crate::normalized_event_support::count_events_by_kind;
use constants::*;
use decode::build_registry_observations;
use discovery::{latest_discovery_observations, reconcile_discovery_observation_history_for_chain};
use emitters::{load_active_emitters, normalized_source_scope_targets};
use events::{
    RegistryObservationContext, apply_registry_observation, hydrate_subregistry_event_target_ids,
};
use identity::{
    build_name_surface, build_resource, build_resource_events, build_surface_binding,
    build_token_lineage, coalesce_name_surfaces_for_upsert, normalize_surface_bindings_for_upsert,
    upsert_surface_bindings_close_before_open,
};
use live::{
    FullSourceRawLogHistoryGuard, RegistryReplayState, acquire_registry_sync_fence,
    clear_live_registry_replay_checkpoints_for_chain, has_authoritative_ens_v2_closure_through,
    invalidate_live_registry_replay_state, release_registry_sync_fence,
};
use load::{RawLogCanonicalityFilter, load_registry_raw_logs};
use names::initial_registry_suffixes;
use types::*;
use util::normalize_address;

pub use live::{
    ensure_ens_v2_retained_history_proof_through, record_ens_v2_live_selected_raw_log_coverage,
    sync_ens_v2_registry_resource_surface_live_poll,
};
pub use recovery::{EnsV2MissingCoverage, ens_v2_missing_coverage, is_ens_v2_missing_coverage};

#[cfg(test)]
use crate::evm_abi::keccak_signature_hex;
#[cfg(test)]
use bigname_manifests::WatchedContractSource;
#[cfg(test)]
use bigname_storage::{CanonicalityState, upsert_surface_bindings};
#[cfg(test)]
use emitters::{preferred_emitters_by_scope, source_rank};
#[cfg(test)]
use serde_json::{Value, json};
#[cfg(test)]
use sqlx::types::time::OffsetDateTime;
#[cfg(test)]
use util::{deterministic_uuid, event_position_timestamp, hex_string, keccak256_bytes};

pub struct EnsV2RegistryResourceSurfaceSyncSummary {
    pub scanned_log_count: usize,
    pub matched_log_count: usize,
    pub total_name_surface_count: usize,
    pub total_resource_count: usize,
    pub total_surface_binding_count: usize,
    pub total_normalized_event_count: usize,
    pub total_normalized_event_inserted_count: usize,
    pub active_discovery_observation_count: usize,
    pub active_edge_count: usize,
    pub admitted_edge_count: usize,
    pub inserted_edge_count: usize,
    pub deactivated_edge_count: usize,
    pub discovery_admission_epoch_bump_count: usize,
    pub by_kind: BTreeMap<String, usize>,
}

impl EnsV2RegistryResourceSurfaceSyncSummary {
    pub fn empty(scanned_log_count: usize) -> Self {
        Self {
            scanned_log_count,
            matched_log_count: 0,
            total_name_surface_count: 0,
            total_resource_count: 0,
            total_surface_binding_count: 0,
            total_normalized_event_count: 0,
            total_normalized_event_inserted_count: 0,
            active_discovery_observation_count: 0,
            active_edge_count: 0,
            admitted_edge_count: 0,
            inserted_edge_count: 0,
            deactivated_edge_count: 0,
            discovery_admission_epoch_bump_count: 0,
            by_kind: BTreeMap::new(),
        }
    }

    pub async fn sync_for_block_hashes(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
    ) -> Result<Self> {
        sync_ens_v2_registry_resource_surface_with_scope(
            pool,
            chain,
            true,
            block_hashes,
            None,
            RawLogCanonicalityFilter::IncludeObserved,
            None,
        )
        .await
    }

    pub async fn sync_for_block_hashes_canonical_only(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
    ) -> Result<Self> {
        sync_ens_v2_registry_resource_surface_with_scope(
            pool,
            chain,
            true,
            block_hashes,
            None,
            RawLogCanonicalityFilter::CanonicalOnly,
            None,
        )
        .await
    }

    pub async fn sync_for_block_hashes_with_source_scope(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
        source_scope: &[(String, String, i64, i64)],
    ) -> Result<Self> {
        sync_ens_v2_registry_resource_surface_with_scope(
            pool,
            chain,
            true,
            block_hashes,
            Some(source_scope),
            RawLogCanonicalityFilter::IncludeObserved,
            None,
        )
        .await
    }

    pub async fn sync_for_block_hashes_with_source_scope_canonical_only(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
        source_scope: &[(String, String, i64, i64)],
    ) -> Result<Self> {
        sync_ens_v2_registry_resource_surface_with_scope(
            pool,
            chain,
            true,
            block_hashes,
            Some(source_scope),
            RawLogCanonicalityFilter::CanonicalOnly,
            None,
        )
        .await
    }
}

pub async fn sync_ens_v2_registry_resource_surface(
    pool: &PgPool,
    chain: &str,
) -> Result<EnsV2RegistryResourceSurfaceSyncSummary> {
    sync_ens_v2_registry_resource_surface_with_scope(
        pool,
        chain,
        false,
        &[],
        None,
        RawLogCanonicalityFilter::IncludeObserved,
        None,
    )
    .await
}

pub async fn sync_ens_v2_registry_resource_surface_through_block(
    pool: &PgPool,
    chain: &str,
    target_block_number: i64,
) -> Result<EnsV2RegistryResourceSurfaceSyncSummary> {
    sync_ens_v2_registry_resource_surface_with_scope(
        pool,
        chain,
        false,
        &[],
        None,
        RawLogCanonicalityFilter::CanonicalOnly,
        Some(target_block_number),
    )
    .await
}

async fn sync_ens_v2_registry_resource_surface_with_scope(
    pool: &PgPool,
    chain: &str,
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
    canonicality_filter: RawLogCanonicalityFilter,
    max_block_number: Option<i64>,
) -> Result<EnsV2RegistryResourceSurfaceSyncSummary> {
    let mut registry_sync_fence = Some(acquire_registry_sync_fence(pool, chain).await?);
    // Non-live entrypoints may rewrite persisted state behind the process-local live cache.
    invalidate_live_registry_replay_state(pool, chain);
    clear_live_registry_replay_checkpoints_for_chain(pool, chain).await?;
    let full_source_guard = if !restrict_to_block_hashes {
        let full_source_target = if let Some(target) = max_block_number {
            target
        } else {
            sqlx::query_scalar::<_, Option<i64>>(
                r#"
                SELECT MAX(block_number)::BIGINT
                FROM chain_lineage
                WHERE chain_id = $1
                  AND canonicality_state <> 'orphaned'::canonicality_state
                "#,
            )
            .bind(chain)
            .fetch_one(pool)
            .await
            .with_context(|| format!("failed to load ENSv2 full-source target for {chain}"))?
            .unwrap_or(0)
        };
        if !has_authoritative_ens_v2_closure_through(pool, chain, full_source_target).await? {
            let fence = registry_sync_fence
                .take()
                .context("ENSv2 registry sync fence is absent before empty-source release")?;
            release_registry_sync_fence(fence, chain).await?;
            return Ok(EnsV2RegistryResourceSurfaceSyncSummary::empty(0));
        }
        let fence = registry_sync_fence
            .take()
            .context("ENSv2 registry sync fence is absent before full-source upgrade")?;
        let guard = FullSourceRawLogHistoryGuard::acquire(fence, chain).await?;
        let proof = guard.ensure_proof_through(pool, full_source_target).await?;
        let pre_sync_requirements = guard
            .load_requirements_through(pool, proof, full_source_target)
            .await?;
        Some((guard, proof, full_source_target, pre_sync_requirements))
    } else {
        None
    };
    let expected_discovery_admission_epoch = full_source_guard
        .as_ref()
        .map(|(_, proof, _, _)| proof.discovery_admission_epoch);
    let sync_result = sync_ens_v2_registry_resource_surface_with_scope_and_state(
        pool,
        chain,
        restrict_to_block_hashes,
        block_hashes,
        source_scope,
        canonicality_filter,
        max_block_number,
        None,
        !restrict_to_block_hashes,
        !restrict_to_block_hashes,
        expected_discovery_admission_epoch,
    )
    .await;
    let result = match (sync_result, full_source_guard) {
        (
            Ok((summary, replay_state)),
            Some((guard, proof, full_source_target, pre_sync_requirements)),
        ) => {
            guard
                .finish(
                    pool,
                    proof,
                    full_source_target,
                    summary.discovery_admission_epoch_bump_count,
                    &pre_sync_requirements,
                    None,
                )
                .await?;
            Ok((summary, replay_state))
        }
        (Ok(result), None) => Ok(result),
        (Err(error), Some((guard, _, _, _))) => {
            let _ = guard.abort().await;
            Err(error)
        }
        (Err(error), None) => Err(error),
    };
    let release = if let Some(registry_sync_fence) = registry_sync_fence {
        release_registry_sync_fence(registry_sync_fence, chain).await
    } else {
        Ok(())
    };
    prioritize_operation_error(result.map(|(summary, _)| summary), release)
}

fn prioritize_operation_error<T>(operation: Result<T>, release: Result<()>) -> Result<T> {
    match (operation, release) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

#[allow(clippy::too_many_arguments)]
async fn sync_ens_v2_registry_resource_surface_with_scope_and_state(
    pool: &PgPool,
    chain: &str,
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
    canonicality_filter: RawLogCanonicalityFilter,
    max_block_number: Option<i64>,
    replay_state: Option<RegistryReplayState>,
    include_historical_emitters: bool,
    reconcile_full_sources: bool,
    expected_discovery_admission_epoch: Option<i64>,
) -> Result<(EnsV2RegistryResourceSurfaceSyncSummary, RegistryReplayState)> {
    let is_resumed_replay = replay_state.is_some();
    let mut replay_state = replay_state.unwrap_or_default();
    let source_scope = source_scope.map(normalized_source_scope_targets);
    if source_scope.as_ref().is_some_and(Vec::is_empty) {
        return Ok((
            EnsV2RegistryResourceSurfaceSyncSummary::empty(0),
            replay_state,
        ));
    }
    let scoped_emitter_identities = source_scope.as_ref().map(|source_scope| {
        source_scope
            .iter()
            .map(|target| (target.source_family.clone(), target.address.clone()))
            .collect::<HashSet<_>>()
    });

    let active_emitters = load_active_emitters(
        pool,
        chain,
        scoped_emitter_identities.as_ref(),
        include_historical_emitters,
    )
    .await?;
    if active_emitters.is_empty() {
        return Ok((
            EnsV2RegistryResourceSurfaceSyncSummary::empty(0),
            replay_state,
        ));
    }
    let manifest_ids = active_emitters
        .iter()
        .map(|emitter| emitter.source_manifest_id)
        .collect::<Vec<_>>();
    let event_topics = load_required_active_manifest_event_topic0s_by_signature(
        pool,
        &manifest_ids,
        &ABI_EVENT_SIGNATURES,
        "ENSv2 registry",
    )
    .await?;

    let raw_logs = load_registry_raw_logs(
        pool,
        chain,
        &active_emitters,
        restrict_to_block_hashes,
        block_hashes,
        source_scope.as_deref(),
        canonicality_filter,
        max_block_number,
    )
    .await?;
    let scanned_log_count = raw_logs.len();
    let mut matched_log_count = 0usize;
    initialize_registry_suffixes(&mut replay_state, &active_emitters, is_resumed_replay);
    replay_state.registry_contract_by_address = active_emitters
        .iter()
        .map(|emitter| (emitter.address.clone(), emitter.contract_instance_id))
        .collect();
    let RegistryReplayState {
        mut registry_suffix_by_address,
        mut registry_contract_by_address,
        mut states_by_registry_token,
        mut state_keys_by_registry_namehash,
        mut token_aliases,
        mut current_token_alias_by_canonical_key,
    } = replay_state;
    let mut linked_resource_states = BTreeMap::<Uuid, RegistryNameState>::new();
    let mut closed_bindings = BTreeMap::<Uuid, SurfaceBinding>::new();
    let mut observations = Vec::<DiscoveryObservation>::new();
    let mut graph_events = Vec::<NormalizedEvent>::new();

    for raw_log in &raw_logs {
        let observations_for_log = build_registry_observations(raw_log, &event_topics)?;
        if observations_for_log.is_empty() {
            continue;
        }
        matched_log_count += 1;
        let mut context = RegistryObservationContext {
            registry_suffix_by_address: &mut registry_suffix_by_address,
            registry_contract_by_address: &mut registry_contract_by_address,
            states_by_registry_token: &mut states_by_registry_token,
            state_keys_by_registry_namehash: &mut state_keys_by_registry_namehash,
            linked_resource_states: &mut linked_resource_states,
            closed_bindings: &mut closed_bindings,
            token_aliases: &mut token_aliases,
            current_token_alias_by_canonical_key: &mut current_token_alias_by_canonical_key,
            observations: &mut observations,
            graph_events: &mut graph_events,
        };
        for observation in observations_for_log {
            apply_registry_observation(observation, &mut context)?;
        }
    }

    let latest_observations = latest_discovery_observations(observations.clone())?;
    let reconciliation = reconcile_discovery_observation_history_for_chain(
        pool,
        chain,
        &observations,
        reconcile_full_sources,
        max_block_number,
        expected_discovery_admission_epoch,
    )
    .await
    .with_context(|| format!("failed to reconcile ENSv2 discovery observations for {chain}"))?;
    hydrate_subregistry_event_target_ids(pool, &mut graph_events).await?;

    let mut token_lineages = Vec::<TokenLineage>::new();
    let mut resources = Vec::<Resource>::new();
    let mut surfaces = Vec::<NameSurface>::new();
    let mut bindings = Vec::<SurfaceBinding>::new();
    let mut events = graph_events;

    for state in linked_resource_states.values() {
        let Some(link) = state.resource.as_ref() else {
            continue;
        };
        token_lineages.push(build_token_lineage(pool, state, link).await?);
        resources.push(build_resource(pool, state, link).await?);
        surfaces.push(build_name_surface(pool, &state.name, &state.first_ref).await?);
        if let Some(closed_binding) = closed_bindings.get(&link.surface_binding_id) {
            bindings.push(closed_binding.clone());
        } else {
            bindings.push(build_surface_binding(pool, state, link).await?);
        }
        events.extend(build_resource_events(state, link));
    }
    let materialized_binding_ids = bindings
        .iter()
        .map(|binding| binding.surface_binding_id)
        .collect::<HashSet<_>>();
    bindings.extend(
        closed_bindings
            .into_iter()
            .filter(|(binding_id, _)| !materialized_binding_ids.contains(binding_id))
            .map(|(_, binding)| binding),
    );

    let by_kind = count_events_by_kind(&events);
    coalesce_name_surfaces_for_upsert(&mut surfaces)?;
    normalize_surface_bindings_for_upsert(pool, &mut bindings).await?;
    upsert_token_lineages(pool, &token_lineages).await?;
    upsert_resources(pool, &resources).await?;
    upsert_name_surfaces(pool, &surfaces).await?;
    upsert_surface_bindings_close_before_open(pool, &bindings).await?;
    let normalized_event_upsert = upsert_normalized_events_with_summary(pool, &events).await?;

    let summary = EnsV2RegistryResourceSurfaceSyncSummary {
        scanned_log_count,
        matched_log_count,
        total_name_surface_count: surfaces.len(),
        total_resource_count: resources.len(),
        total_surface_binding_count: bindings.len(),
        total_normalized_event_count: events.len(),
        total_normalized_event_inserted_count: normalized_event_upsert.inserted_count,
        active_discovery_observation_count: latest_observations
            .iter()
            .filter(|observation| normalize_address(&observation.to_address) != ZERO_ADDRESS)
            .count(),
        active_edge_count: reconciliation.active_edge_count,
        admitted_edge_count: reconciliation.admitted_edge_count,
        inserted_edge_count: reconciliation.inserted_edge_count,
        deactivated_edge_count: reconciliation.deactivated_edge_count,
        discovery_admission_epoch_bump_count: reconciliation.admission_epoch_bump_count,
        by_kind,
    };
    Ok((
        summary,
        RegistryReplayState {
            registry_suffix_by_address,
            registry_contract_by_address,
            states_by_registry_token,
            state_keys_by_registry_namehash,
            token_aliases,
            current_token_alias_by_canonical_key,
        },
    ))
}

fn initialize_registry_suffixes(
    replay_state: &mut RegistryReplayState,
    active_emitters: &[ActiveEmitter],
    is_resumed_replay: bool,
) {
    if !is_resumed_replay {
        replay_state.registry_suffix_by_address = initial_registry_suffixes(active_emitters);
    }
}

#[cfg(test)]
mod tests;
