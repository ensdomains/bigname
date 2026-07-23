use std::{collections::HashMap, time::Instant};

use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres, Transaction};
use tracing::info;

use crate::label_preimages::upsert_label_preimages_from_normalized_events;

use super::{types::NormalizedEvent, validation::validate_normalized_event};

#[path = "upsert/batch.rs"]
mod batch;
#[path = "upsert/identity.rs"]
mod identity;
#[path = "upsert/metrics.rs"]
mod metrics;
#[path = "upsert/repair.rs"]
mod repair;
#[path = "upsert/replay_authority.rs"]
mod replay_authority;
#[path = "upsert/sanitize.rs"]
mod sanitize;

use batch::{insert_normalized_events_do_nothing, upsert_normalized_event_batch};
pub(super) use identity::{
    normalized_event_identity_differences, normalized_event_identity_summary,
};
use identity::{normalized_event_snapshots_after_upsert, validate_existing_normalized_events};
use metrics::{count_normalized_events_by_event_kind, count_normalized_events_by_source_family};
use repair::{
    repair_after_state_conflicts, repair_resource_id_conflicts,
    supersede_basenames_registry_boundary_derivation_change_events,
};
pub use replay_authority::{
    NormalizedEventReplayAuthoritySummary, upsert_normalized_events_with_stateless_replay_authority,
};
use sanitize::jsonb_safe_normalized_event;
pub use sanitize::serialize_jsonb_value;

pub(super) const NORMALIZED_EVENT_FAST_INSERT_BATCH_SIZE: usize = 10_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NormalizedEventUpsertSummary {
    pub snapshots: Vec<NormalizedEvent>,
    pub inserted_count: usize,
}

/// Insert missing normalized events or refresh canonicality for existing rows.
pub async fn upsert_normalized_events(
    pool: &PgPool,
    events: &[NormalizedEvent],
) -> Result<Vec<NormalizedEvent>> {
    Ok(upsert_normalized_events_with_summary(pool, events)
        .await?
        .snapshots)
}

/// Insert missing normalized events or refresh canonicality for existing rows.
pub async fn upsert_normalized_events_with_summary(
    pool: &PgPool,
    events: &[NormalizedEvent],
) -> Result<NormalizedEventUpsertSummary> {
    if events.is_empty() {
        return Ok(NormalizedEventUpsertSummary {
            snapshots: Vec::new(),
            inserted_count: 0,
        });
    }

    let total_started = Instant::now();
    let begin_started = Instant::now();
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for normalized-event upsert")?;
    let transaction_begin_ms = begin_started.elapsed().as_millis();

    let validate_started = Instant::now();
    let mut jsonb_safe_events = Vec::with_capacity(events.len());
    for event in events {
        validate_normalized_event(event)?;
        jsonb_safe_events.push(jsonb_safe_normalized_event(event));
    }
    let validate_and_sanitize_ms = validate_started.elapsed().as_millis();

    let mut snapshots = Vec::with_capacity(events.len());
    let mut inserted_count = 0usize;
    let mut chunk_count = 0usize;
    let mut fast_insert_ms = 0u128;
    let mut conflict_partition_ms = 0u128;
    let mut conflict_load_ms = 0u128;
    let mut conflict_snapshot_ms = 0u128;
    let mut conflict_update_ms = 0u128;
    let mut after_state_repair_ms = 0u128;
    let mut after_state_repaired_count = 0usize;
    let mut resource_id_repair_ms = 0u128;
    let mut resource_id_repaired_count = 0usize;
    let mut boundary_supersession_ms = 0u128;
    let mut boundary_superseded_count = 0usize;
    let mut label_preimage_ms = 0u128;
    let mut label_preimage_count = 0usize;
    let mut output_snapshot_ms = 0u128;
    for chunk in jsonb_safe_events.chunks(NORMALIZED_EVENT_FAST_INSERT_BATCH_SIZE) {
        chunk_count += 1;
        let conflict_load_started = Instant::now();
        let mut existing_events =
            validate_existing_normalized_events(&mut transaction, chunk).await?;
        conflict_load_ms += conflict_load_started.elapsed().as_millis();

        let conflict_partition_started = Instant::now();
        let mut missing_events = Vec::new();
        let mut conflicted_events = Vec::new();
        for event in chunk {
            if existing_events.contains_key(&event.event_identity) {
                conflicted_events.push(event.clone());
            } else {
                missing_events.push(event.clone());
            }
        }
        conflict_partition_ms += conflict_partition_started.elapsed().as_millis();

        let fast_insert_started = Instant::now();
        let inserted_identities =
            insert_normalized_events_do_nothing(&mut transaction, &missing_events).await?;
        fast_insert_ms += fast_insert_started.elapsed().as_millis();
        inserted_count += inserted_identities.len();

        let raced_conflicted_events = missing_events
            .iter()
            .filter(|event| !inserted_identities.contains(&event.event_identity))
            .cloned()
            .collect::<Vec<_>>();
        if !raced_conflicted_events.is_empty() {
            let conflict_load_started = Instant::now();
            let raced_existing =
                validate_existing_normalized_events(&mut transaction, &raced_conflicted_events)
                    .await?;
            conflict_load_ms += conflict_load_started.elapsed().as_millis();
            existing_events.extend(raced_existing);
            conflicted_events.extend(raced_conflicted_events);
        }

        let after_state_repair_started = Instant::now();
        after_state_repaired_count +=
            repair_after_state_conflicts(&mut transaction, &conflicted_events, &existing_events)
                .await?;
        after_state_repair_ms += after_state_repair_started.elapsed().as_millis();

        let resource_id_repair_started = Instant::now();
        resource_id_repaired_count +=
            repair_resource_id_conflicts(&mut transaction, &conflicted_events, &existing_events)
                .await?;
        resource_id_repair_ms += resource_id_repair_started.elapsed().as_millis();

        let events_requiring_canonicality_refresh = conflicted_events
            .iter()
            .filter(|event| {
                existing_events
                    .get(&event.event_identity)
                    .map(|existing| {
                        existing
                            .canonicality_state
                            .merge_observation(event.canonicality_state)
                            != existing.canonicality_state
                    })
                    .unwrap_or(true)
            })
            .cloned()
            .collect::<Vec<_>>();

        let conflict_snapshot_started = Instant::now();
        let mut conflicting_snapshots =
            normalized_event_snapshots_after_upsert(&conflicted_events, &existing_events);
        let mut conflicting_snapshots_by_identity = conflicting_snapshots
            .drain(..)
            .map(|event| (event.event_identity.clone(), event))
            .collect::<HashMap<_, _>>();
        conflict_snapshot_ms += conflict_snapshot_started.elapsed().as_millis();

        let conflict_update_started = Instant::now();
        for event in &events_requiring_canonicality_refresh {
            upsert_normalized_event_batch(&mut transaction, std::slice::from_ref(event)).await?;
        }
        conflict_update_ms += conflict_update_started.elapsed().as_millis();

        let boundary_supersession_started = Instant::now();
        boundary_superseded_count +=
            supersede_basenames_registry_boundary_derivation_change_events(&mut transaction, chunk)
                .await?;
        boundary_supersession_ms += boundary_supersession_started.elapsed().as_millis();

        let label_preimage_started = Instant::now();
        let changed_labelhashes =
            upsert_label_preimages_from_normalized_events(&mut transaction, chunk).await?;
        label_preimage_count += changed_labelhashes.len();
        label_preimage_ms += label_preimage_started.elapsed().as_millis();

        let output_snapshot_started = Instant::now();
        for event in chunk {
            if inserted_identities.contains(&event.event_identity) {
                snapshots.push(event.clone());
            } else {
                snapshots.push(
                    conflicting_snapshots_by_identity
                        .remove(&event.event_identity)
                        .unwrap_or_else(|| event.clone()),
                );
            }
        }
        output_snapshot_ms += output_snapshot_started.elapsed().as_millis();
    }

    let commit_started = Instant::now();
    transaction
        .commit()
        .await
        .context("failed to commit normalized-event upsert")?;
    let commit_ms = commit_started.elapsed().as_millis();

    info!(
        service = "storage",
        operation = "upsert_normalized_events",
        normalized_event_count = events.len(),
        inserted_count,
        conflict_count = events.len().saturating_sub(inserted_count),
        chunk_count,
        event_kind_counts = ?count_normalized_events_by_event_kind(events),
        source_family_counts = ?count_normalized_events_by_source_family(events),
        transaction_begin_ms,
        validate_and_sanitize_ms,
        fast_insert_ms,
        conflict_partition_ms,
        conflict_load_ms,
        conflict_snapshot_ms,
        conflict_update_ms,
        after_state_repair_ms,
        after_state_repaired_count,
        resource_id_repair_ms,
        resource_id_repaired_count,
        boundary_supersession_ms,
        boundary_superseded_count,
        label_preimage_ms,
        label_preimage_count,
        output_snapshot_ms,
        commit_ms,
        elapsed_ms = total_started.elapsed().as_millis(),
        "normalized-event upsert timing completed"
    );

    Ok(NormalizedEventUpsertSummary {
        snapshots,
        inserted_count,
    })
}

/// Insert missing normalized events or refresh canonicality for existing rows without returning
/// event snapshots. Use this for replay paths that already own their in-memory event state and need
/// bounded memory while committing large batches.
pub async fn upsert_normalized_events_count_only(
    pool: &PgPool,
    events: &[NormalizedEvent],
) -> Result<usize> {
    if events.is_empty() {
        return Ok(0);
    }

    let total_started = Instant::now();
    let mut inserted_count = 0usize;
    let mut chunk_count = 0usize;
    let mut transaction_begin_ms = 0u128;
    let mut validate_and_sanitize_ms = 0u128;
    let mut fast_insert_ms = 0u128;
    let mut conflict_partition_ms = 0u128;
    let mut conflict_load_ms = 0u128;
    let mut conflict_update_ms = 0u128;
    let mut after_state_repair_ms = 0u128;
    let mut after_state_repaired_count = 0usize;
    let mut resource_id_repair_ms = 0u128;
    let mut resource_id_repaired_count = 0usize;
    let mut boundary_supersession_ms = 0u128;
    let mut boundary_superseded_count = 0usize;
    let mut label_preimage_ms = 0u128;
    let mut label_preimage_count = 0usize;
    let mut commit_ms = 0u128;

    for raw_chunk in events.chunks(NORMALIZED_EVENT_FAST_INSERT_BATCH_SIZE) {
        chunk_count += 1;
        let begin_started = Instant::now();
        let mut transaction = pool
            .begin()
            .await
            .context("failed to open transaction for normalized-event count-only upsert chunk")?;
        transaction_begin_ms += begin_started.elapsed().as_millis();

        let validate_started = Instant::now();
        let mut chunk = Vec::with_capacity(raw_chunk.len());
        for event in raw_chunk {
            validate_normalized_event(event)?;
            chunk.push(jsonb_safe_normalized_event(event));
        }
        validate_and_sanitize_ms += validate_started.elapsed().as_millis();

        let conflict_load_started = Instant::now();
        let mut existing_events =
            validate_existing_normalized_events(&mut transaction, &chunk).await?;
        conflict_load_ms += conflict_load_started.elapsed().as_millis();

        let conflict_partition_started = Instant::now();
        let mut missing_events = Vec::new();
        let mut conflicted_events = Vec::new();
        for event in &chunk {
            if existing_events.contains_key(&event.event_identity) {
                conflicted_events.push(event.clone());
            } else {
                missing_events.push(event.clone());
            }
        }
        conflict_partition_ms += conflict_partition_started.elapsed().as_millis();

        let fast_insert_started = Instant::now();
        let inserted_identities =
            insert_normalized_events_do_nothing(&mut transaction, &missing_events).await?;
        fast_insert_ms += fast_insert_started.elapsed().as_millis();
        inserted_count += inserted_identities.len();

        let raced_conflicted_events = missing_events
            .iter()
            .filter(|event| !inserted_identities.contains(&event.event_identity))
            .cloned()
            .collect::<Vec<_>>();
        if !raced_conflicted_events.is_empty() {
            let conflict_load_started = Instant::now();
            let raced_existing =
                validate_existing_normalized_events(&mut transaction, &raced_conflicted_events)
                    .await?;
            conflict_load_ms += conflict_load_started.elapsed().as_millis();
            existing_events.extend(raced_existing);
            conflicted_events.extend(raced_conflicted_events);
        }

        let after_state_repair_started = Instant::now();
        after_state_repaired_count +=
            repair_after_state_conflicts(&mut transaction, &conflicted_events, &existing_events)
                .await?;
        after_state_repair_ms += after_state_repair_started.elapsed().as_millis();

        let resource_id_repair_started = Instant::now();
        resource_id_repaired_count +=
            repair_resource_id_conflicts(&mut transaction, &conflicted_events, &existing_events)
                .await?;
        resource_id_repair_ms += resource_id_repair_started.elapsed().as_millis();

        let events_requiring_canonicality_refresh = conflicted_events
            .iter()
            .filter(|event| {
                existing_events
                    .get(&event.event_identity)
                    .map(|existing| {
                        existing
                            .canonicality_state
                            .merge_observation(event.canonicality_state)
                            != existing.canonicality_state
                    })
                    .unwrap_or(true)
            })
            .cloned()
            .collect::<Vec<_>>();

        let conflict_update_started = Instant::now();
        for event in &events_requiring_canonicality_refresh {
            upsert_normalized_event_batch(&mut transaction, std::slice::from_ref(event)).await?;
        }
        conflict_update_ms += conflict_update_started.elapsed().as_millis();

        let boundary_supersession_started = Instant::now();
        boundary_superseded_count +=
            supersede_basenames_registry_boundary_derivation_change_events(
                &mut transaction,
                &chunk,
            )
            .await?;
        boundary_supersession_ms += boundary_supersession_started.elapsed().as_millis();

        let label_preimage_started = Instant::now();
        let changed_labelhashes =
            upsert_label_preimages_from_normalized_events(&mut transaction, &chunk).await?;
        label_preimage_count += changed_labelhashes.len();
        label_preimage_ms += label_preimage_started.elapsed().as_millis();

        let commit_started = Instant::now();
        transaction
            .commit()
            .await
            .context("failed to commit normalized-event count-only upsert chunk")?;
        commit_ms += commit_started.elapsed().as_millis();
    }

    info!(
        service = "storage",
        operation = "upsert_normalized_events_count_only",
        normalized_event_count = events.len(),
        inserted_count,
        conflict_count = events.len().saturating_sub(inserted_count),
        chunk_count,
        event_kind_counts = ?count_normalized_events_by_event_kind(events),
        source_family_counts = ?count_normalized_events_by_source_family(events),
        transaction_begin_ms,
        validate_and_sanitize_ms,
        fast_insert_ms,
        conflict_partition_ms,
        conflict_load_ms,
        conflict_update_ms,
        after_state_repair_ms,
        after_state_repaired_count,
        resource_id_repair_ms,
        resource_id_repaired_count,
        boundary_supersession_ms,
        boundary_superseded_count,
        label_preimage_ms,
        label_preimage_count,
        commit_ms,
        elapsed_ms = total_started.elapsed().as_millis(),
        "normalized-event count-only upsert timing completed"
    );

    Ok(inserted_count)
}

/// Insert or refresh normalized events inside a caller-owned transaction.
///
/// This is used by absence-aware re-derivations that must publish their
/// replacement rows and orphan rows missing from the replacement set in one
/// atomic commit.
pub async fn upsert_normalized_events_count_only_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
) -> Result<usize> {
    let mut inserted_count = 0usize;
    for raw_chunk in events.chunks(NORMALIZED_EVENT_FAST_INSERT_BATCH_SIZE) {
        let mut chunk = Vec::with_capacity(raw_chunk.len());
        for event in raw_chunk {
            validate_normalized_event(event)?;
            chunk.push(jsonb_safe_normalized_event(event));
        }

        let mut existing_events = validate_existing_normalized_events(transaction, &chunk).await?;
        let mut missing_events = Vec::new();
        let mut conflicted_events = Vec::new();
        for event in &chunk {
            if existing_events.contains_key(&event.event_identity) {
                conflicted_events.push(event.clone());
            } else {
                missing_events.push(event.clone());
            }
        }

        let inserted_identities =
            insert_normalized_events_do_nothing(transaction, &missing_events).await?;
        inserted_count += inserted_identities.len();
        let raced_conflicted_events = missing_events
            .iter()
            .filter(|event| !inserted_identities.contains(&event.event_identity))
            .cloned()
            .collect::<Vec<_>>();
        if !raced_conflicted_events.is_empty() {
            let raced_existing =
                validate_existing_normalized_events(transaction, &raced_conflicted_events).await?;
            existing_events.extend(raced_existing);
            conflicted_events.extend(raced_conflicted_events);
        }

        repair_after_state_conflicts(transaction, &conflicted_events, &existing_events).await?;
        repair_resource_id_conflicts(transaction, &conflicted_events, &existing_events).await?;
        let events_requiring_canonicality_refresh = conflicted_events
            .iter()
            .filter(|event| {
                existing_events
                    .get(&event.event_identity)
                    .map(|existing| {
                        existing
                            .canonicality_state
                            .merge_observation(event.canonicality_state)
                            != existing.canonicality_state
                    })
                    .unwrap_or(true)
            })
            .cloned()
            .collect::<Vec<_>>();
        for event in &events_requiring_canonicality_refresh {
            upsert_normalized_event_batch(transaction, std::slice::from_ref(event)).await?;
        }
        supersede_basenames_registry_boundary_derivation_change_events(transaction, &chunk).await?;
        upsert_label_preimages_from_normalized_events(transaction, &chunk).await?;
    }
    Ok(inserted_count)
}
