use std::collections::{BTreeMap, HashSet};

use anyhow::{Context, Result};
use bigname_storage::NormalizedEvent;
use sqlx::PgPool;

pub(super) async fn load_existing_event_identities(
    pool: &PgPool,
    events: &[NormalizedEvent],
) -> Result<HashSet<String>> {
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
    .context("failed to load existing block-derived normalized-event identities")?;

    Ok(rows.into_iter().collect())
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

pub(super) fn count_events_by_kind(events: &[NormalizedEvent]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for event in events {
        *counts.entry(event.event_kind.clone()).or_insert(0) += 1;
    }
    counts
}
