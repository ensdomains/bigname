use anyhow::{Context, Result};
use sqlx::PgPool;

use super::NormalizedEvent;
use crate::label_preimages::upsert_label_preimages_from_normalized_events;

const FIXTURE_INSERT_BATCH_SIZE: usize = 10_000;

/// Seed normalized events for tests of surviving read and projection behavior.
///
/// This helper is absent from production builds. It deliberately does not reproduce the deleted
/// runtime's repair, supersession, reconciliation, or replay-authority behavior.
pub async fn insert_normalized_event_fixtures(
    pool: &PgPool,
    events: &[NormalizedEvent],
) -> Result<Vec<NormalizedEvent>> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open normalized-event fixture transaction")?;
    let mut snapshots = Vec::with_capacity(events.len());

    for events in events.chunks(FIXTURE_INSERT_BATCH_SIZE) {
        let events = events.iter().map(jsonb_safe_event).collect::<Vec<_>>();
        let event_identities = events
            .iter()
            .map(|event| event.event_identity.clone())
            .collect::<Vec<_>>();
        let namespaces = events
            .iter()
            .map(|event| event.namespace.clone())
            .collect::<Vec<_>>();
        let logical_name_ids = events
            .iter()
            .map(|event| event.logical_name_id.clone())
            .collect::<Vec<_>>();
        let resource_ids = events
            .iter()
            .map(|event| event.resource_id)
            .collect::<Vec<_>>();
        let event_kinds = events
            .iter()
            .map(|event| event.event_kind.clone())
            .collect::<Vec<_>>();
        let source_families = events
            .iter()
            .map(|event| event.source_family.clone())
            .collect::<Vec<_>>();
        let manifest_versions = events
            .iter()
            .map(|event| event.manifest_version)
            .collect::<Vec<_>>();
        let source_manifest_ids = events
            .iter()
            .map(|event| event.source_manifest_id)
            .collect::<Vec<_>>();
        let chain_ids = events
            .iter()
            .map(|event| event.chain_id.clone())
            .collect::<Vec<_>>();
        let block_numbers = events
            .iter()
            .map(|event| event.block_number)
            .collect::<Vec<_>>();
        let block_hashes = events
            .iter()
            .map(|event| event.block_hash.clone())
            .collect::<Vec<_>>();
        let transaction_hashes = events
            .iter()
            .map(|event| event.transaction_hash.clone())
            .collect::<Vec<_>>();
        let log_indexes = events
            .iter()
            .map(|event| event.log_index)
            .collect::<Vec<_>>();
        let raw_fact_refs = serialize_json_values(
            events.iter().map(|event| &event.raw_fact_ref),
            "failed to serialize normalized-event fixture raw_fact_ref",
        )?;
        let derivation_kinds = events
            .iter()
            .map(|event| event.derivation_kind.clone())
            .collect::<Vec<_>>();
        let canonicality_states = events
            .iter()
            .map(|event| event.canonicality_state.as_str().to_owned())
            .collect::<Vec<_>>();
        let before_states = serialize_json_values(
            events.iter().map(|event| &event.before_state),
            "failed to serialize normalized-event fixture before_state",
        )?;
        let after_states = serialize_json_values(
            events.iter().map(|event| &event.after_state),
            "failed to serialize normalized-event fixture after_state",
        )?;

        sqlx::query(
            r#"
            INSERT INTO normalized_events (
                event_identity, namespace, logical_name_id, resource_id, event_kind,
                source_family, manifest_version, source_manifest_id, chain_id, block_number,
                block_hash, transaction_hash, log_index, raw_fact_ref, derivation_kind,
                canonicality_state, before_state, after_state
            )
            SELECT
                event_identity, namespace, logical_name_id, resource_id, event_kind,
                source_family, manifest_version, source_manifest_id, chain_id, block_number,
                block_hash, transaction_hash, log_index, raw_fact_ref::JSONB, derivation_kind,
                canonicality_state::canonicality_state, before_state::JSONB, after_state::JSONB
            FROM unnest(
                $1::TEXT[], $2::TEXT[], $3::TEXT[], $4::UUID[], $5::TEXT[], $6::TEXT[],
                $7::BIGINT[], $8::BIGINT[], $9::TEXT[], $10::BIGINT[], $11::TEXT[],
                $12::TEXT[], $13::BIGINT[], $14::TEXT[], $15::TEXT[], $16::TEXT[],
                $17::TEXT[], $18::TEXT[]
            ) AS input(
                event_identity, namespace, logical_name_id, resource_id, event_kind,
                source_family, manifest_version, source_manifest_id, chain_id, block_number,
                block_hash, transaction_hash, log_index, raw_fact_ref, derivation_kind,
                canonicality_state, before_state, after_state
            )
            ON CONFLICT (event_identity) DO UPDATE SET
                namespace = EXCLUDED.namespace,
                logical_name_id = EXCLUDED.logical_name_id,
                resource_id = EXCLUDED.resource_id,
                event_kind = EXCLUDED.event_kind,
                source_family = EXCLUDED.source_family,
                manifest_version = EXCLUDED.manifest_version,
                source_manifest_id = EXCLUDED.source_manifest_id,
                chain_id = EXCLUDED.chain_id,
                block_number = EXCLUDED.block_number,
                block_hash = EXCLUDED.block_hash,
                transaction_hash = EXCLUDED.transaction_hash,
                log_index = EXCLUDED.log_index,
                raw_fact_ref = EXCLUDED.raw_fact_ref,
                derivation_kind = EXCLUDED.derivation_kind,
                canonicality_state = EXCLUDED.canonicality_state,
                before_state = EXCLUDED.before_state,
                after_state = EXCLUDED.after_state,
                observed_at = now()
            "#,
        )
        .bind(&event_identities)
        .bind(&namespaces)
        .bind(&logical_name_ids)
        .bind(&resource_ids)
        .bind(&event_kinds)
        .bind(&source_families)
        .bind(&manifest_versions)
        .bind(&source_manifest_ids)
        .bind(&chain_ids)
        .bind(&block_numbers)
        .bind(&block_hashes)
        .bind(&transaction_hashes)
        .bind(&log_indexes)
        .bind(&raw_fact_refs)
        .bind(&derivation_kinds)
        .bind(&canonicality_states)
        .bind(&before_states)
        .bind(&after_states)
        .execute(&mut *transaction)
        .await
        .context("failed to insert normalized-event fixtures")?;

        upsert_label_preimages_from_normalized_events(&mut transaction, &events).await?;
        snapshots.extend(events);
    }

    transaction
        .commit()
        .await
        .context("failed to commit normalized-event fixtures")?;
    Ok(snapshots)
}

fn serialize_json_values<'a>(
    values: impl Iterator<Item = &'a serde_json::Value>,
    context: &'static str,
) -> Result<Vec<String>> {
    values
        .map(|value| serde_json::to_string(value).context(context))
        .collect()
}

fn jsonb_safe_event(event: &NormalizedEvent) -> NormalizedEvent {
    let mut event = event.clone();
    event.event_identity = postgres_text_safe(&event.event_identity);
    event.namespace = postgres_text_safe(&event.namespace);
    event.logical_name_id = event.logical_name_id.as_deref().map(postgres_text_safe);
    event.event_kind = postgres_text_safe(&event.event_kind);
    event.source_family = postgres_text_safe(&event.source_family);
    event.chain_id = event.chain_id.as_deref().map(postgres_text_safe);
    event.block_hash = event.block_hash.as_deref().map(postgres_text_safe);
    event.transaction_hash = event.transaction_hash.as_deref().map(postgres_text_safe);
    event.derivation_kind = postgres_text_safe(&event.derivation_kind);
    event.raw_fact_ref = jsonb_safe_value(&event.raw_fact_ref);
    event.before_state = jsonb_safe_value(&event.before_state);
    event.after_state = jsonb_safe_value(&event.after_state);
    event
}

fn postgres_text_safe(text: &str) -> String {
    text.replace('\0', "\\u0000")
}

fn jsonb_safe_value(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::String(text) => serde_json::Value::String(postgres_text_safe(text)),
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(jsonb_safe_value).collect())
        }
        serde_json::Value::Object(fields) => serde_json::Value::Object(
            fields
                .iter()
                .map(|(key, value)| (postgres_text_safe(key), jsonb_safe_value(value)))
                .collect(),
        ),
        _ => value.clone(),
    }
}
