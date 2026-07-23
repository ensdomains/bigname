use std::{
    collections::{BTreeMap, HashMap, HashSet},
    time::Instant,
};

use anyhow::{Context, Result, bail, ensure};
use sqlx::{PgPool, Postgres, Transaction};
use tracing::info;

use crate::label_preimages::upsert_label_preimages_from_normalized_events;
use crate::normalized_events::{types::NormalizedEvent, validation::validate_normalized_event};

use super::{
    NORMALIZED_EVENT_FAST_INSERT_BATCH_SIZE,
    batch::{
        insert_normalized_events_do_nothing, load_normalized_events_by_identities,
        upsert_normalized_event_batch,
    },
    identity::normalized_event_identity_differences,
    sanitize::{jsonb_safe_normalized_event, serialize_jsonb_value},
};

#[path = "replay_authority/projection_identity.rs"]
mod projection_identity;

use projection_identity::ensure_stateless_replay_projection_identity_matches;

/// Counts the rows examined by an explicitly authoritative stateless replay.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NormalizedEventReplayAuthoritySummary {
    pub identities_examined: usize,
    pub identities_inserted: usize,
    pub identities_unchanged: usize,
    pub identities_superseded: usize,
    pub identities_skipped_non_canonical_source: usize,
    pub inserted_by_event_kind: BTreeMap<String, usize>,
}

impl NormalizedEventReplayAuthoritySummary {
    pub fn add(&mut self, other: &Self) {
        self.identities_examined += other.identities_examined;
        self.identities_inserted += other.identities_inserted;
        self.identities_unchanged += other.identities_unchanged;
        self.identities_superseded += other.identities_superseded;
        self.identities_skipped_non_canonical_source +=
            other.identities_skipped_non_canonical_source;
        for (event_kind, count) in &other.inserted_by_event_kind {
            *self
                .inserted_by_event_kind
                .entry(event_kind.clone())
                .or_default() += count;
        }
    }
}

#[derive(Debug)]
struct ReplayIdentityLog {
    event_identity: String,
    derivation_kind: String,
    outcome: &'static str,
    differing_fields: Vec<&'static str>,
}

/// Persist selected stateless replay output with authority to replace stale content.
///
/// This is intentionally separate from ordinary adapter upsert. Ordinary writes still
/// fail closed when a stable event identity resolves to different content. The caller
/// must already have restricted derivation to producers whose central replay
/// dependency model is `stateless_raw_fact` before using this function.
pub async fn upsert_normalized_events_with_stateless_replay_authority(
    pool: &PgPool,
    events: &[NormalizedEvent],
) -> Result<NormalizedEventReplayAuthoritySummary> {
    if events.is_empty() {
        return Ok(NormalizedEventReplayAuthoritySummary::default());
    }

    let total_started = Instant::now();
    let mut seen_identities = HashSet::with_capacity(events.len());
    let mut safe_events = Vec::with_capacity(events.len());
    for event in events {
        validate_normalized_event(event)?;
        if !seen_identities.insert(event.event_identity.clone()) {
            bail!(
                "stateless normalized-event replay authority received duplicate identity {}",
                event.event_identity
            );
        }
        safe_events.push(jsonb_safe_normalized_event(event));
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open stateless normalized-event replay authority transaction")?;
    let mut summary = NormalizedEventReplayAuthoritySummary {
        identities_examined: safe_events.len(),
        ..NormalizedEventReplayAuthoritySummary::default()
    };
    let mut row_logs = Vec::with_capacity(safe_events.len());

    for chunk in safe_events.chunks(NORMALIZED_EVENT_FAST_INSERT_BATCH_SIZE) {
        persist_authoritative_chunk(&mut transaction, chunk, &mut summary, &mut row_logs).await?;
        let authoritative_events = chunk
            .iter()
            .filter(|event| source_canonicality_supports_authoritative_replay(event))
            .cloned()
            .collect::<Vec<_>>();
        upsert_label_preimages_from_normalized_events(&mut transaction, &authoritative_events)
            .await?;
    }

    ensure!(
        summary.identities_examined
            == summary.identities_inserted
                + summary.identities_unchanged
                + summary.identities_superseded
                + summary.identities_skipped_non_canonical_source,
        "stateless normalized-event replay authority outcome counts do not cover every identity"
    );
    transaction
        .commit()
        .await
        .context("failed to commit stateless normalized-event replay authority transaction")?;

    for row in row_logs {
        info!(
            service = "storage",
            operation = "stateless_normalized_event_replay_authority",
            event_identity = row.event_identity,
            derivation_kind = row.derivation_kind,
            identity_outcome = row.outcome,
            differing_fields = ?row.differing_fields,
            "stateless-only normalized-event replay identity examined"
        );
    }
    info!(
        service = "storage",
        operation = "stateless_normalized_event_replay_authority",
        identities_examined = summary.identities_examined,
        identities_inserted = summary.identities_inserted,
        identities_unchanged = summary.identities_unchanged,
        identities_superseded = summary.identities_superseded,
        identities_skipped_non_canonical_source = summary.identities_skipped_non_canonical_source,
        elapsed_ms = total_started.elapsed().as_millis(),
        "stateless-only normalized-event replay authority completed"
    );

    Ok(summary)
}

async fn persist_authoritative_chunk(
    transaction: &mut Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
    summary: &mut NormalizedEventReplayAuthoritySummary,
    row_logs: &mut Vec<ReplayIdentityLog>,
) -> Result<()> {
    let authoritative_events = events
        .iter()
        .filter(|event| source_canonicality_supports_authoritative_replay(event))
        .cloned()
        .collect::<Vec<_>>();
    for event in events
        .iter()
        .filter(|event| !source_canonicality_supports_authoritative_replay(event))
    {
        summary.identities_skipped_non_canonical_source += 1;
        row_logs.push(replay_identity_log(
            event,
            "skipped_non_canonical_source",
            Vec::new(),
        ));
    }
    if authoritative_events.is_empty() {
        return Ok(());
    }

    let mut existing_by_identity =
        load_existing_for_update(transaction, &authoritative_events).await?;
    let missing_events = authoritative_events
        .iter()
        .filter(|event| !existing_by_identity.contains_key(&event.event_identity))
        .cloned()
        .collect::<Vec<_>>();
    let inserted_identities =
        insert_normalized_events_do_nothing(transaction, &missing_events).await?;

    let raced_events = missing_events
        .iter()
        .filter(|event| !inserted_identities.contains(&event.event_identity))
        .cloned()
        .collect::<Vec<_>>();
    if !raced_events.is_empty() {
        existing_by_identity.extend(load_existing_for_update(transaction, &raced_events).await?);
    }

    for incoming in &authoritative_events {
        if inserted_identities.contains(&incoming.event_identity) {
            summary.identities_inserted += 1;
            *summary
                .inserted_by_event_kind
                .entry(incoming.event_kind.clone())
                .or_default() += 1;
            row_logs.push(replay_identity_log(incoming, "inserted", Vec::new()));
            continue;
        }

        let existing = existing_by_identity.get(&incoming.event_identity).with_context(|| {
            format!(
                "stateless normalized-event replay authority could not lock existing identity {}",
                incoming.event_identity
            )
        })?;
        let differing_fields = normalized_event_identity_differences(existing, incoming);
        let merged_canonicality = existing
            .canonicality_state
            .merge_observation(incoming.canonicality_state);
        let mut replayed = incoming.clone();
        replayed.canonicality_state = merged_canonicality;

        if differing_fields.is_empty() {
            if merged_canonicality != existing.canonicality_state {
                upsert_normalized_event_batch(transaction, std::slice::from_ref(&replayed)).await?;
            }
            summary.identities_unchanged += 1;
            row_logs.push(replay_identity_log(incoming, "unchanged", Vec::new()));
            continue;
        }

        ensure_stateless_replay_projection_identity_matches(existing, &replayed)?;

        supersede_normalized_event(transaction, &replayed).await?;
        summary.identities_superseded += 1;
        row_logs.push(replay_identity_log(
            incoming,
            "superseded",
            differing_fields,
        ));
    }

    Ok(())
}

async fn load_existing_for_update(
    transaction: &mut Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
) -> Result<HashMap<String, NormalizedEvent>> {
    let identities = events
        .iter()
        .map(|event| event.event_identity.clone())
        .collect::<Vec<_>>();
    if identities.is_empty() {
        return Ok(HashMap::new());
    }

    sqlx::query_scalar::<_, String>(
        "SELECT event_identity FROM normalized_events WHERE event_identity = ANY($1::TEXT[]) FOR UPDATE",
    )
    .bind(&identities)
    .fetch_all(&mut **transaction)
    .await
    .context("failed to lock normalized events for stateless replay authority")?;
    Ok(
        load_normalized_events_by_identities(transaction, &identities)
            .await?
            .into_iter()
            .map(|event| (event.event_identity.clone(), event))
            .collect(),
    )
}

async fn supersede_normalized_event(
    transaction: &mut Transaction<'_, Postgres>,
    event: &NormalizedEvent,
) -> Result<()> {
    let raw_fact_ref = serialize_jsonb_value(
        &event.raw_fact_ref,
        "failed to serialize stateless replay raw_fact_ref",
    )?;
    let before_state = serialize_jsonb_value(
        &event.before_state,
        "failed to serialize stateless replay before_state",
    )?;
    let after_state = serialize_jsonb_value(
        &event.after_state,
        "failed to serialize stateless replay after_state",
    )?;
    let rows_affected = sqlx::query(
        r#"
        UPDATE normalized_events
        SET
            namespace = $2,
            logical_name_id = $3,
            resource_id = $4,
            event_kind = $5,
            source_family = $6,
            manifest_version = $7,
            source_manifest_id = $8,
            chain_id = $9,
            block_number = $10,
            block_hash = $11,
            transaction_hash = $12,
            log_index = $13,
            raw_fact_ref = $14::JSONB,
            derivation_kind = $15,
            canonicality_state = $16::canonicality_state,
            before_state = $17::JSONB,
            after_state = $18::JSONB,
            observed_at = now()
        WHERE event_identity = $1
        "#,
    )
    .bind(&event.event_identity)
    .bind(&event.namespace)
    .bind(&event.logical_name_id)
    .bind(event.resource_id)
    .bind(&event.event_kind)
    .bind(&event.source_family)
    .bind(event.manifest_version)
    .bind(event.source_manifest_id)
    .bind(&event.chain_id)
    .bind(event.block_number)
    .bind(&event.block_hash)
    .bind(&event.transaction_hash)
    .bind(event.log_index)
    .bind(raw_fact_ref)
    .bind(&event.derivation_kind)
    .bind(event.canonicality_state.as_str())
    .bind(before_state)
    .bind(after_state)
    .execute(&mut **transaction)
    .await
    .with_context(|| {
        format!(
            "failed to supersede normalized event {} with stateless replay authority",
            event.event_identity
        )
    })?
    .rows_affected();
    ensure!(
        rows_affected == 1,
        "stateless normalized-event replay supersession updated {rows_affected} rows for identity {}",
        event.event_identity
    );

    Ok(())
}

fn source_canonicality_supports_authoritative_replay(event: &NormalizedEvent) -> bool {
    matches!(
        event.canonicality_state,
        crate::CanonicalityState::Canonical
            | crate::CanonicalityState::Safe
            | crate::CanonicalityState::Finalized
    )
}

fn replay_identity_log(
    event: &NormalizedEvent,
    outcome: &'static str,
    differing_fields: Vec<&'static str>,
) -> ReplayIdentityLog {
    ReplayIdentityLog {
        event_identity: event.event_identity.clone(),
        derivation_kind: event.derivation_kind.clone(),
        outcome,
        differing_fields,
    }
}
