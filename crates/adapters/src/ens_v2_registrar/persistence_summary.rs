use std::collections::{BTreeMap, HashSet};

use anyhow::{Context, Result};
use bigname_storage::NormalizedEvent;
use sqlx::PgPool;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsV2RegistrarSyncSummary {
    pub scanned_log_count: usize,
    pub matched_log_count: usize,
    pub total_synced_count: usize,
    pub total_inserted_count: usize,
    pub by_kind: BTreeMap<String, EnsV2RegistrarKindSyncSummary>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsV2RegistrarKindSyncSummary {
    pub synced_count: usize,
    pub inserted_count: usize,
}

pub(super) async fn load_existing_event_identities(
    pool: &PgPool,
    events: &[NormalizedEvent],
) -> Result<HashSet<String>> {
    let event_identities = events
        .iter()
        .map(|event| event.event_identity.clone())
        .collect::<Vec<_>>();
    if event_identities.is_empty() {
        return Ok(HashSet::new());
    }

    let rows = sqlx::query_scalar::<_, String>(
        "SELECT event_identity FROM normalized_events WHERE event_identity = ANY($1::TEXT[])",
    )
    .bind(event_identities)
    .fetch_all(pool)
    .await
    .context("failed to load existing ENSv2 registrar event identities")?;

    Ok(rows.into_iter().collect())
}

pub(super) fn count_events_by_kind(events: &[NormalizedEvent]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for event in events {
        *counts.entry(event.event_kind.clone()).or_insert(0) += 1;
    }
    counts
}

pub(super) fn count_inserted_events_by_kind(
    events: &[NormalizedEvent],
    existing_event_identities: &HashSet<String>,
) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for event in events
        .iter()
        .filter(|event| !existing_event_identities.contains(&event.event_identity))
    {
        *counts.entry(event.event_kind.clone()).or_insert(0) += 1;
    }
    counts
}

pub(super) fn empty_summary(scanned_log_count: usize) -> EnsV2RegistrarSyncSummary {
    EnsV2RegistrarSyncSummary {
        scanned_log_count,
        matched_log_count: 0,
        total_synced_count: 0,
        total_inserted_count: 0,
        by_kind: BTreeMap::new(),
    }
}
