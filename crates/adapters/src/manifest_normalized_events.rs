use std::collections::BTreeMap;

use anyhow::Result;
use bigname_manifests::{
    ManifestRuntimeProgress, load_manifest_drift_inputs, load_manifest_drift_inputs_with_progress,
};
use bigname_storage::upsert_normalized_events_with_summary;
use sqlx::PgPool;

mod builders;
mod constants;
mod drift_alerts;
mod loading;
mod types;
mod utils;

use crate::normalized_event_support::count_events_by_kind;
use builders::build_normalized_events;
use loading::load_active_capabilities;
pub use types::{ManifestNormalizedEventKindSyncSummary, ManifestNormalizedEventSyncSummary};
use utils::active_proxy_contracts_by_manifest;

const MANIFEST_EVENT_UPSERT_PROGRESS_ROWS: usize = 1_000;

/// Sync manifest-derived normalized events from stored active manifest state.
pub async fn sync_manifest_normalized_events(
    pool: &PgPool,
) -> Result<ManifestNormalizedEventSyncSummary> {
    sync_manifest_normalized_events_inner(pool, None).await
}

pub async fn sync_manifest_normalized_events_with_progress(
    pool: &PgPool,
    progress: &mut dyn ManifestRuntimeProgress,
) -> Result<ManifestNormalizedEventSyncSummary> {
    sync_manifest_normalized_events_inner(pool, Some(progress)).await
}

async fn sync_manifest_normalized_events_inner(
    pool: &PgPool,
    mut progress: Option<&mut dyn ManifestRuntimeProgress>,
) -> Result<ManifestNormalizedEventSyncSummary> {
    let drift_inputs = match progress.as_deref_mut() {
        Some(progress) => load_manifest_drift_inputs_with_progress(pool, progress).await?,
        None => load_manifest_drift_inputs(pool).await?,
    };
    if drift_inputs.active_manifests.is_empty() {
        return Ok(ManifestNormalizedEventSyncSummary {
            total_synced_count: 0,
            total_inserted_count: 0,
            by_kind: BTreeMap::new(),
        });
    }

    let capabilities = load_active_capabilities(pool).await?;
    if let Some(progress) = progress.as_deref_mut() {
        progress.record(pool).await?;
    }
    let contracts = active_proxy_contracts_by_manifest(&drift_inputs);
    let events = build_normalized_events(
        pool,
        &drift_inputs,
        &capabilities,
        &contracts,
        &mut progress,
    )
    .await?;

    if events.is_empty() {
        return Ok(ManifestNormalizedEventSyncSummary {
            total_synced_count: 0,
            total_inserted_count: 0,
            by_kind: BTreeMap::new(),
        });
    }

    let synced_by_kind = count_events_by_kind(&events);
    let mut by_kind = synced_by_kind
        .iter()
        .map(|(kind, synced_count)| {
            (
                kind.clone(),
                ManifestNormalizedEventKindSyncSummary {
                    synced_count: *synced_count,
                    inserted_count: 0,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut total_inserted_count = 0;
    let mut chunk_start = 0usize;
    while chunk_start < events.len() {
        let kind = events[chunk_start].event_kind.clone();
        let mut chunk_end = chunk_start + 1;
        while chunk_end < events.len()
            && chunk_end - chunk_start < MANIFEST_EVENT_UPSERT_PROGRESS_ROWS
            && events[chunk_end].event_kind == kind
        {
            chunk_end += 1;
        }
        let inserted_count =
            upsert_normalized_events_with_summary(pool, &events[chunk_start..chunk_end])
                .await?
                .inserted_count;
        total_inserted_count += inserted_count;
        by_kind
            .get_mut(&kind)
            .expect("synced event kind must have a summary")
            .inserted_count += inserted_count;
        if let Some(progress) = progress.as_deref_mut() {
            progress.record(pool).await?;
        }
        chunk_start = chunk_end;
    }

    Ok(ManifestNormalizedEventSyncSummary {
        total_synced_count: synced_by_kind.values().sum(),
        total_inserted_count,
        by_kind,
    })
}

#[cfg(test)]
mod tests;
