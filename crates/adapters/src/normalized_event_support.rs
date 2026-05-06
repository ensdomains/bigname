use std::collections::{BTreeMap, HashSet};

use anyhow::{Context, Result, bail};
use bigname_storage::{NormalizedEvent, upsert_normalized_events};
use sqlx::PgPool;

pub(crate) struct NormalizedEventSyncCounts {
    pub(crate) synced_by_kind: BTreeMap<String, usize>,
    pub(crate) inserted_by_kind: BTreeMap<String, usize>,
    pub(crate) total_synced_count: usize,
    pub(crate) total_inserted_count: usize,
}

impl NormalizedEventSyncCounts {
    pub(crate) fn into_parts_by_kind<T>(
        self,
        build_kind_summary: impl Fn(usize, usize) -> T,
    ) -> (usize, usize, BTreeMap<String, T>) {
        let Self {
            synced_by_kind,
            inserted_by_kind,
            total_synced_count,
            total_inserted_count,
        } = self;
        let by_kind = synced_by_kind
            .into_iter()
            .map(|(event_kind, synced_count)| {
                let inserted_count = inserted_by_kind.get(&event_kind).copied().unwrap_or(0);
                (event_kind, build_kind_summary(synced_count, inserted_count))
            })
            .collect();

        (total_synced_count, total_inserted_count, by_kind)
    }
}

pub(crate) async fn load_existing_event_identities(
    pool: &PgPool,
    events: &[NormalizedEvent],
    adapter_label: &str,
) -> Result<HashSet<String>> {
    if events.is_empty() {
        return Ok(HashSet::new());
    }

    let event_identities = events
        .iter()
        .map(|event| event.event_identity.clone())
        .collect::<Vec<_>>();
    let rows = sqlx::query_scalar::<_, String>(
        r#"
        SELECT event_identity
        FROM normalized_events
        WHERE event_identity = ANY($1::TEXT[])
        "#,
    )
    .bind(event_identities)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load existing {adapter_label} event identities"))?;

    Ok(rows.into_iter().collect())
}

pub(crate) async fn upsert_normalized_events_with_counts(
    pool: &PgPool,
    events: &[NormalizedEvent],
    adapter_label: &str,
) -> Result<NormalizedEventSyncCounts> {
    let counts = count_normalized_event_sync(pool, events, adapter_label).await?;
    upsert_normalized_events(pool, events).await?;
    Ok(counts)
}

pub(crate) async fn upsert_normalized_events_in_chunks_with_counts(
    pool: &PgPool,
    events: &[NormalizedEvent],
    adapter_label: &str,
    chunk_size: usize,
) -> Result<NormalizedEventSyncCounts> {
    if chunk_size == 0 {
        bail!("normalized event upsert chunk size must be positive");
    }

    let counts = count_normalized_event_sync(pool, events, adapter_label).await?;
    for chunk in events.chunks(chunk_size) {
        upsert_normalized_events(pool, chunk).await?;
    }
    Ok(counts)
}

async fn count_normalized_event_sync(
    pool: &PgPool,
    events: &[NormalizedEvent],
    adapter_label: &str,
) -> Result<NormalizedEventSyncCounts> {
    let existing_event_identities =
        load_existing_event_identities(pool, events, adapter_label).await?;
    let inserted_by_kind = count_inserted_events_by_kind(events, &existing_event_identities);
    let synced_by_kind = count_events_by_kind(events);

    Ok(NormalizedEventSyncCounts {
        synced_by_kind,
        total_synced_count: events.len(),
        total_inserted_count: inserted_by_kind.values().sum(),
        inserted_by_kind,
    })
}

pub(crate) fn count_events_by_kind(events: &[NormalizedEvent]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for event in events {
        *counts.entry(event.event_kind.clone()).or_insert(0) += 1;
    }
    counts
}

pub(crate) fn count_inserted_events_by_kind(
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
