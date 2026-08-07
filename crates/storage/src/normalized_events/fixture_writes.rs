use anyhow::{Context, Result};
use sqlx::PgPool;

use super::NormalizedEvent;

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
        let mut events = events.iter().map(jsonb_safe_event).collect::<Vec<_>>();
        for event in &mut events {
            let (Some(chain_id), Some(block_number), Some(block_hash)) = (
                event.chain_id.as_deref(),
                event.block_number,
                event.block_hash.as_deref(),
            ) else {
                continue;
            };
            if !matches!(
                event.canonicality_state.as_str(),
                "canonical" | "safe" | "finalized"
            ) {
                continue;
            }
            let readable_hash: Option<String> = sqlx::query_scalar(
                "SELECT block_hash FROM bigname_phase.chain_lineage
                 WHERE chain_id = $1 AND block_number = $2
                   AND canonicality_state IN ('canonical', 'safe', 'finalized')
                 LIMIT 1",
            )
            .bind(chain_id)
            .bind(block_number)
            .fetch_optional(&mut *transaction)
            .await
            .context("failed to inspect normalized-event fixture lineage")?;
            if let Some(readable_hash) = readable_hash
                && readable_hash != block_hash
            {
                event.block_hash = Some(readable_hash);
            }
        }
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
            INSERT INTO bigname_phase.chain_lineage (
                chain_id,
                block_hash,
                block_number,
                block_timestamp,
                canonicality_state
            )
            SELECT DISTINCT
                input.chain_id,
                input.block_hash,
                input.block_number,
                COALESCE(
                    existing_lineage.block_timestamp,
                    TIMESTAMPTZ '2000-01-01 00:00:00+00'
                        + input.block_number * INTERVAL '1 second'
                ),
                input.canonicality_state::bigname_phase.canonicality_state
            FROM unnest(
                $1::TEXT[], $2::BIGINT[], $3::TEXT[], $4::TEXT[]
            ) AS input(chain_id, block_number, block_hash, canonicality_state)
            LEFT JOIN bigname_phase.chain_lineage existing_lineage
              ON existing_lineage.chain_id = input.chain_id
             AND existing_lineage.block_hash = input.block_hash
            WHERE input.chain_id IS NOT NULL
              AND input.block_number IS NOT NULL
              AND input.block_hash IS NOT NULL
              AND input.canonicality_state IN ('canonical', 'safe', 'finalized')
            ON CONFLICT (chain_id, block_hash) DO NOTHING
            "#,
        )
        .bind(&chain_ids)
        .bind(&block_numbers)
        .bind(&block_hashes)
        .bind(&canonicality_states)
        .execute(&mut *transaction)
        .await
        .context("failed to insert normalized-event fixture lineage")?;

        sqlx::query(
            r#"
            INSERT INTO bigname_phase.normalized_events (
                event_identity, namespace, logical_name_id, resource_id, event_kind,
                source_family, manifest_version, source_manifest_id, chain_id, block_number,
                block_hash, transaction_hash, transaction_index, log_index, raw_fact_ref, derivation_kind,
                canonicality_state, before_state, after_state
            )
            SELECT
                event_identity, namespace, logical_name_id, resource_id, event_kind,
                source_family, manifest_version, source_manifest_id, chain_id, block_number,
                block_hash, transaction_hash,
                CASE WHEN log_index IS NULL THEN NULL ELSE 0 END,
                log_index, raw_fact_ref::JSONB, derivation_kind,
                canonicality_state::bigname_phase.canonicality_state, before_state::JSONB, after_state::JSONB
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
                transaction_index = EXCLUDED.transaction_index,
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
    event.logical_name_id = event
        .logical_name_id
        .as_deref()
        .map(normalize_fixture_logical_name_id);
    event.event_kind = postgres_text_safe(&event.event_kind);
    event.source_family = postgres_text_safe(&event.source_family);
    event.chain_id = event.chain_id.as_deref().map(postgres_text_safe);
    event.block_hash = event.block_hash.as_deref().map(postgres_text_safe);
    event.transaction_hash = event.transaction_hash.as_deref().map(postgres_text_safe);
    event.derivation_kind = normalized_fixture_derivation_kind(&event.derivation_kind).to_owned();
    event.raw_fact_ref = jsonb_safe_value(&event.raw_fact_ref);
    event.before_state = jsonb_safe_value(&event.before_state);
    event.after_state = jsonb_safe_value(&event.after_state);
    event
}

fn normalized_fixture_derivation_kind(value: &str) -> &str {
    match value {
        "ens_v1_reverse_claim"
        | "ens_v1_unwrapped_authority"
        | "ens_v2_permissions"
        | "ens_v2_registrar"
        | "ens_v2_registry_resource_surface"
        | "ens_v2_resolver"
        | "manifest_sync"
        | "proxy_upgrade"
        | "raw_log_preimage_observation" => value,
        _ => "ens_v1_unwrapped_authority",
    }
}

fn normalize_fixture_logical_name_id(logical_name_id: &str) -> String {
    let Some((namespace, name_or_hash)) = logical_name_id.split_once(':') else {
        return postgres_text_safe(logical_name_id);
    };
    if name_or_hash.starts_with("0x") && name_or_hash.len() == 66 {
        return postgres_text_safe(logical_name_id);
    }
    crate::logical_name_id_for_name(&postgres_text_safe(namespace), name_or_hash)
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
