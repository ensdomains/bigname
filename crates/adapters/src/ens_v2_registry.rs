use std::collections::{BTreeMap, HashSet};

use anyhow::{Context, Result};
use bigname_manifests::DiscoveryObservation;
use bigname_storage::{
    NameSurface, NormalizedEvent, Resource, SurfaceBinding, TokenLineage, upsert_name_surfaces,
    upsert_normalized_events_with_summary, upsert_resources, upsert_token_lineages,
};
use sqlx::PgPool;
use sqlx::types::Uuid;

mod announcements;
mod constants;
mod decode;
mod discovery;
mod emitters;
mod entrypoints;
mod events;
mod identity;
mod live;
mod load;
mod names;
mod normalized;
mod restricted;
mod types;
mod util;

use crate::{
    adapter_manifest::load_required_active_manifest_event_topic0s_by_signature,
    normalized_event_support::count_events_by_kind,
};
use announcements::load_registry_announcement_observations;
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
use live::{RegistryReplayState, invalidate_live_registry_replay_state};
use load::{RawLogCanonicalityFilter, load_registry_raw_logs};
use names::initial_registry_suffixes;
use restricted::{reconstruct_prior_registry_state, requires_prior_registry_state};
use types::*;
use util::normalize_address;

pub use entrypoints::{
    sync_ens_v2_registry_resource_surface, sync_ens_v2_registry_resource_surface_through_block,
};
pub use live::sync_ens_v2_registry_resource_surface_live_poll;

#[cfg(test)]
use crate::evm_abi::keccak_signature_hex;
#[cfg(test)]
use bigname_manifests::WatchedContractSource;
#[cfg(test)]
use bigname_storage::{CanonicalityState, upsert_surface_bindings};
#[cfg(test)]
use emitters::{preferred_emitters_by_scope, source_rank};
#[cfg(test)]
use load::load_registry_raw_log_prefix;
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

#[allow(clippy::too_many_arguments)]
async fn sync_ens_v2_registry_resource_surface_with_scope(
    pool: &PgPool,
    chain: &str,
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
    canonicality_filter: RawLogCanonicalityFilter,
    max_block_number: Option<i64>,
) -> Result<EnsV2RegistryResourceSurfaceSyncSummary> {
    // Non-live entrypoints may rewrite persisted state behind the process-local live cache.
    invalidate_live_registry_replay_state(pool, chain);
    sync_ens_v2_registry_resource_surface_with_scope_and_state(
        pool,
        chain,
        restrict_to_block_hashes,
        block_hashes,
        source_scope,
        canonicality_filter,
        max_block_number,
        None,
        true,
        true,
        false,
        None,
    )
    .await
    .map(|(summary, _)| summary)
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
    reconstruct_restricted_prior_state: bool,
    reconcile_full_sources: bool,
    expected_discovery_admission_epoch: Option<i64>,
) -> Result<(EnsV2RegistryResourceSurfaceSyncSummary, RegistryReplayState)> {
    let is_resumed_replay = replay_state.is_some();
    let mut replay_state = replay_state.unwrap_or_default();
    let reconcile_orphaned_starts = source_scope.is_none();
    let source_scope = source_scope.map(normalized_source_scope_targets);
    if source_scope.as_ref().is_some_and(Vec::is_empty) {
        return Ok((
            EnsV2RegistryResourceSurfaceSyncSummary::empty(0),
            replay_state,
        ));
    }
    let scoped_emitter_identities = source_scope.as_ref().and_then(|source_scope| {
        (!source_scope
            .iter()
            .any(emitters::is_generic_registry_scope_target))
        .then(|| {
            source_scope
                .iter()
                .map(|target| (target.source_family.clone(), target.address.clone()))
                .collect::<HashSet<_>>()
        })
    });

    let announcement_observations = load_registry_announcement_observations(
        pool,
        chain,
        restrict_to_block_hashes,
        block_hashes,
        source_scope.as_deref(),
        canonicality_filter,
        max_block_number,
    )
    .await?;
    let latest_announcement_observations =
        latest_discovery_observations(announcement_observations.clone())?;
    let announcement_reconciliation = reconcile_discovery_observation_history_for_chain(
        pool,
        chain,
        &announcement_observations,
        false,
        max_block_number,
        expected_discovery_admission_epoch,
    )
    .await
    .with_context(|| format!("failed to reconcile ENSv2 registry announcements for {chain}"))?;
    let expected_discovery_admission_epoch = expected_discovery_admission_epoch
        .map(|epoch| {
            i64::try_from(announcement_reconciliation.admission_epoch_bump_count)
                .context("ENSv2 registry-announcement epoch bump count exceeds i64")
                .and_then(|bumps| {
                    epoch
                        .checked_add(bumps)
                        .context("ENSv2 discovery admission epoch overflow")
                })
        })
        .transpose()?;

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
    let observations_by_log = raw_logs
        .iter()
        .map(|raw_log| build_registry_observations(raw_log, &event_topics))
        .collect::<Result<Vec<_>>>()?;
    let matched_log_count = observations_by_log
        .iter()
        .filter(|observations| !observations.is_empty())
        .count();
    initialize_registry_suffixes(&mut replay_state, &active_emitters, is_resumed_replay);
    replay_state.registry_contract_by_address = active_emitters
        .iter()
        .map(|emitter| (emitter.address.clone(), emitter.contract_instance_id))
        .collect();
    let reconstruct_each_selected_log = !is_resumed_replay
        && restrict_to_block_hashes
        && reconstruct_restricted_prior_state
        && requires_prior_registry_state(&observations_by_log);
    let mut linked_resource_states = BTreeMap::<Uuid, RegistryNameState>::new();
    let mut closed_bindings = BTreeMap::<Uuid, SurfaceBinding>::new();
    let mut observations = Vec::<DiscoveryObservation>::new();
    let mut graph_events = Vec::<NormalizedEvent>::new();

    for (raw_log, observations_for_log) in raw_logs.iter().zip(observations_by_log.into_iter()) {
        if reconstruct_each_selected_log && !observations_for_log.is_empty() {
            replay_state = reconstruct_prior_registry_state(pool, chain, raw_log).await?;
        }
        if !observations_for_log.is_empty() {
            let mut context = RegistryObservationContext {
                registry_suffix_by_address: &mut replay_state.registry_suffix_by_address,
                registry_contract_by_address: &mut replay_state.registry_contract_by_address,
                states_by_registry_token: &mut replay_state.states_by_registry_token,
                state_keys_by_registry_namehash: &mut replay_state.state_keys_by_registry_namehash,
                linked_resource_states: &mut linked_resource_states,
                closed_bindings: &mut closed_bindings,
                token_aliases: &mut replay_state.token_aliases,
                current_token_alias_by_canonical_key: &mut replay_state
                    .current_token_alias_by_canonical_key,
                observations: &mut observations,
                graph_events: &mut graph_events,
            };
            for observation in observations_for_log {
                apply_registry_observation(observation, &mut context)?;
            }
        }
    }

    if reconcile_orphaned_starts {
        let mut orphaned_starts =
            discovery::load_orphaned_discovery_start_tombstones(pool, chain).await?;
        orphaned_starts.append(&mut observations);
        observations = orphaned_starts;
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
    let normalized_event_inserted_count = upsert_normalized_events_with_summary(pool, &events)
        .await?
        .inserted_count;

    let summary = EnsV2RegistryResourceSurfaceSyncSummary {
        scanned_log_count,
        matched_log_count,
        total_name_surface_count: surfaces.len(),
        total_resource_count: resources.len(),
        total_surface_binding_count: bindings.len(),
        total_normalized_event_count: events.len(),
        total_normalized_event_inserted_count: normalized_event_inserted_count,
        active_discovery_observation_count: latest_observations
            .iter()
            .chain(latest_announcement_observations.iter())
            .filter(|observation| normalize_address(&observation.to_address) != ZERO_ADDRESS)
            .count(),
        active_edge_count: reconciliation.active_edge_count
            + announcement_reconciliation.active_edge_count,
        admitted_edge_count: reconciliation.admitted_edge_count
            + announcement_reconciliation.admitted_edge_count,
        inserted_edge_count: reconciliation.inserted_edge_count
            + announcement_reconciliation.inserted_edge_count,
        deactivated_edge_count: reconciliation.deactivated_edge_count
            + announcement_reconciliation.deactivated_edge_count,
        discovery_admission_epoch_bump_count: reconciliation.admission_epoch_bump_count
            + announcement_reconciliation.admission_epoch_bump_count,
        by_kind,
    };
    Ok((summary, replay_state))
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
