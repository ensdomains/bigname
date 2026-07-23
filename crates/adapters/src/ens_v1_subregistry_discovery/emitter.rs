use std::collections::BTreeMap;

use anyhow::Result;
use bigname_storage::{
    NormalizedEventReplayAuthoritySummary,
    upsert_normalized_events_with_stateless_replay_authority,
    upsert_normalized_events_with_summary,
};
use sqlx::PgPool;

use crate::checkpoint_context::{StartupAdapterProgress, record_startup_adapter_progress};

use super::{
    assignment::ObservedRegistryAssignment,
    checkpoint::{EVENT_PAGE_LIMIT, SubregistryReplayCheckpoint},
    event::build_registry_changed_event,
    hex_topic::ZERO_ADDRESS,
    loader::{ActiveRegistryEdge, load_registry_edges_by_observation_point},
};

const NORMALIZED_EVENT_UPSERT_CHUNK_SIZE: usize = 5_000;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct RegistryChangedEventEmitSummary {
    pub synced_count: usize,
    pub inserted_count: usize,
    pub stateless_replay_authority: NormalizedEventReplayAuthoritySummary,
}

pub(super) async fn emit_registry_changed_events(
    pool: &PgPool,
    latest_assignments: &BTreeMap<String, ObservedRegistryAssignment>,
    discovery_sources: &[String],
    stateless_replay_authority: bool,
) -> Result<RegistryChangedEventEmitSummary> {
    let mut startup_progress = None;
    let discovery_sources = discovery_sources
        .iter()
        .map(String::as_str)
        .collect::<std::collections::HashSet<_>>();
    let mut events = Vec::with_capacity(NORMALIZED_EVENT_UPSERT_CHUNK_SIZE);
    let mut assignments = Vec::with_capacity(NORMALIZED_EVENT_UPSERT_CHUNK_SIZE);
    let mut summary = RegistryChangedEventEmitSummary::default();
    for assignment in latest_assignments.values() {
        if !discovery_sources.contains(assignment.discovery_source.as_str()) {
            continue;
        }
        assignments.push(assignment);
        if assignments.len() >= NORMALIZED_EVENT_UPSERT_CHUNK_SIZE {
            emit_registry_changed_event_chunk(
                pool,
                &assignments,
                &mut events,
                &mut summary,
                &mut startup_progress,
                stateless_replay_authority,
            )
            .await?;
            assignments.clear();
        }
    }
    emit_registry_changed_event_chunk(
        pool,
        &assignments,
        &mut events,
        &mut summary,
        &mut startup_progress,
        stateless_replay_authority,
    )
    .await?;
    flush_registry_changed_events(
        pool,
        &mut events,
        &mut summary,
        &mut startup_progress,
        stateless_replay_authority,
    )
    .await?;
    Ok(summary)
}

/// Emit registry-changed events for a finalizing checkpointed replay by
/// paging the staged latest-per-key assignments per discovery source, so the
/// emit phase never materializes a source's assignments in memory (#168).
pub(super) async fn emit_registry_changed_events_from_checkpoint(
    pool: &PgPool,
    checkpoint: &SubregistryReplayCheckpoint,
    discovery_sources: &[String],
    startup_progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<RegistryChangedEventEmitSummary> {
    let mut events = Vec::with_capacity(usize::try_from(EVENT_PAGE_LIMIT)?);
    let mut summary = RegistryChangedEventEmitSummary::default();
    for discovery_source in discovery_sources {
        let mut after_key = None::<String>;
        loop {
            let page = checkpoint
                .load_assignment_page(
                    pool,
                    discovery_source,
                    after_key.as_deref(),
                    EVENT_PAGE_LIMIT,
                )
                .await?;
            let Some((last_key, _)) = page.last() else {
                break;
            };
            after_key = Some(last_key.clone());
            let assignments = page
                .iter()
                .map(|(_, assignment)| assignment)
                .collect::<Vec<_>>();
            emit_registry_changed_event_chunk(
                pool,
                &assignments,
                &mut events,
                &mut summary,
                startup_progress,
                false,
            )
            .await?;
            record_startup_adapter_progress(pool, startup_progress).await?;
        }
    }
    flush_registry_changed_events(pool, &mut events, &mut summary, startup_progress, false).await?;
    Ok(summary)
}

pub(super) async fn emit_registry_changed_event_chunk(
    pool: &PgPool,
    assignments: &[&ObservedRegistryAssignment],
    events: &mut Vec<bigname_storage::NormalizedEvent>,
    summary: &mut RegistryChangedEventEmitSummary,
    startup_progress: &mut Option<&mut dyn StartupAdapterProgress>,
    stateless_replay_authority: bool,
) -> Result<()> {
    if assignments.is_empty() {
        return Ok(());
    }

    let requested_edges = assignments
        .iter()
        .filter(|assignment| assignment.to_address != ZERO_ADDRESS)
        .map(|assignment| registry_edge_lookup_key(assignment))
        .collect::<Vec<_>>();
    let edges_by_observation_point =
        load_registry_edges_by_observation_point(pool, &requested_edges).await?;

    for assignment in assignments {
        let edge = edge_for_assignment(&edges_by_observation_point, assignment);
        if let Some(event) = build_registry_changed_event(assignment, edge)? {
            events.push(event);
        }
        if events.len() >= NORMALIZED_EVENT_UPSERT_CHUNK_SIZE {
            flush_registry_changed_events(
                pool,
                events,
                summary,
                startup_progress,
                stateless_replay_authority,
            )
            .await?;
        }
    }

    Ok(())
}

pub(super) async fn flush_registry_changed_events(
    pool: &PgPool,
    events: &mut Vec<bigname_storage::NormalizedEvent>,
    summary: &mut RegistryChangedEventEmitSummary,
    startup_progress: &mut Option<&mut dyn StartupAdapterProgress>,
    stateless_replay_authority: bool,
) -> Result<()> {
    if events.is_empty() {
        return Ok(());
    }

    summary.synced_count += events.len();
    if stateless_replay_authority {
        let authority =
            upsert_normalized_events_with_stateless_replay_authority(pool, events).await?;
        summary.inserted_count += authority.identities_inserted;
        summary.stateless_replay_authority.add(&authority);
    } else {
        summary.inserted_count += upsert_normalized_events_with_summary(pool, events)
            .await?
            .inserted_count;
    }
    events.clear();
    record_startup_adapter_progress(pool, startup_progress).await?;
    Ok(())
}

fn edge_for_assignment<'a>(
    edges_by_observation_point: &'a std::collections::HashMap<
        (String, String, i64, String),
        ActiveRegistryEdge,
    >,
    assignment: &ObservedRegistryAssignment,
) -> Option<&'a ActiveRegistryEdge> {
    edges_by_observation_point.get(&registry_edge_lookup_key(assignment))
}

fn registry_edge_lookup_key(
    assignment: &ObservedRegistryAssignment,
) -> (String, String, i64, String) {
    (
        assignment.discovery_source.clone(),
        assignment.observation_key.clone(),
        assignment.raw_log.block_number,
        assignment.raw_log.block_hash.clone(),
    )
}
