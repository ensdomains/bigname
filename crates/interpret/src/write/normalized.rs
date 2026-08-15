use bigname_adapters::schema_v2::NormalizedEvent;
use sqlx::{Postgres, QueryBuilder, Transaction};
use std::collections::HashSet;

use crate::{InterpretError, Result};

use super::batching::{batch_row_context, conflict_free_batches_with_singletons};

pub(super) async fn events(
    transaction: &mut Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
) -> Result<()> {
    let event_identities = events
        .iter()
        .map(|event| event.event_identity.clone())
        .collect::<Vec<_>>();
    let singleton_identities = if event_identities.is_empty() {
        HashSet::new()
    } else {
        // PostgreSQL checks foreign keys after a VALUES statement has allocated every
        // identity value. Keep rows with currently missing references as singletons so
        // a failed statement consumes IDs only through the first failing row, matching
        // the former row-at-a-time writer. UNNEST keeps this production-volume
        // preflight at a fixed bind count.
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
        let block_hashes = events
            .iter()
            .map(|event| event.block_hash.clone())
            .collect::<Vec<_>>();
        let block_numbers = events
            .iter()
            .map(|event| event.block_number)
            .collect::<Vec<_>>();
        sqlx::query_scalar::<_, String>(
            "SELECT submitted.event_identity
             FROM unnest(
                 $1::text[], $2::text[], $3::text[], $4::uuid[], $5::text[],
                 $6::bigint[], $7::bigint[], $8::text[], $9::text[], $10::bigint[]
             ) AS submitted(
                 event_identity, namespace, logical_name_id, resource_id,
                 source_family, manifest_version, source_manifest_id, chain_id,
                 block_hash, block_number
             )
             WHERE EXISTS (
                 SELECT 1 FROM normalized_events stored
                 WHERE stored.event_identity = submitted.event_identity
             )
             OR (
                 submitted.logical_name_id IS NOT NULL
                 AND NOT EXISTS (
                     SELECT 1 FROM name_surfaces names
                     WHERE names.chain_id = submitted.chain_id
                       AND names.logical_name_id = submitted.logical_name_id
                 )
             )
             OR (
                 submitted.resource_id IS NOT NULL
                 AND NOT EXISTS (
                     SELECT 1 FROM resources stored_resources
                     WHERE stored_resources.chain_id = submitted.chain_id
                       AND stored_resources.resource_id = submitted.resource_id
                 )
             )
             OR (
                 submitted.source_manifest_id IS NOT NULL
                 AND NOT EXISTS (
                     SELECT 1 FROM manifest_versions manifests
                     WHERE manifests.manifest_id = submitted.source_manifest_id
                       AND manifests.namespace = submitted.namespace
                       AND manifests.source_family = submitted.source_family
                       AND manifests.manifest_version = submitted.manifest_version
                       AND manifests.chain_id = submitted.chain_id
                 )
             )
             OR (
                 submitted.block_hash IS NOT NULL
                 AND submitted.block_number IS NOT NULL
                 AND NOT EXISTS (
                     SELECT 1 FROM chain_lineage blocks
                     WHERE blocks.chain_id = submitted.chain_id
                       AND blocks.block_hash = submitted.block_hash
                       AND blocks.block_number = submitted.block_number
                 )
             )",
        )
        .bind(&event_identities)
        .bind(&namespaces)
        .bind(&logical_name_ids)
        .bind(&resource_ids)
        .bind(&source_families)
        .bind(&manifest_versions)
        .bind(&source_manifest_ids)
        .bind(&chain_ids)
        .bind(&block_hashes)
        .bind(&block_numbers)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|error| {
            let context = batch_row_context(0, &event_identities);
            InterpretError::database(
                format!("failed to preflight normalized-event batches; {context}"),
                error,
            )
        })?
        .into_iter()
        .collect()
    };
    for (start, batch) in conflict_free_batches_with_singletons(
        events,
        |event| event.event_identity.clone(),
        &singleton_identities,
    ) {
        let mut query = QueryBuilder::<Postgres>::new(
            "
            INSERT INTO normalized_events (
                event_identity,
                namespace,
                logical_name_id,
                resource_id,
                event_kind,
                source_family,
                manifest_version,
                source_manifest_id,
                chain_id,
                block_number,
                block_hash,
                transaction_hash,
                transaction_index,
                log_index,
                raw_fact_ref,
                derivation_kind,
                canonicality_state,
                before_state,
                after_state,
                migration_correlation_ids,
                consumer_visibility
            ) ",
        );
        query.push_values(batch, |mut row, event| {
            row.push_bind(&event.event_identity)
                .push_bind(&event.namespace)
                .push_bind(&event.logical_name_id)
                .push_bind(event.resource_id)
                .push_bind(&event.event_kind)
                .push_bind(&event.source_family)
                .push_bind(event.manifest_version)
                .push_bind(event.source_manifest_id)
                .push_bind(&event.chain_id)
                .push_bind(event.block_number)
                .push_bind(&event.block_hash)
                .push_bind(&event.transaction_hash)
                .push_bind(event.transaction_index)
                .push_bind(event.log_index)
                .push_bind(&event.raw_fact_ref)
                .push_bind(&event.derivation_kind)
                .push_bind(&event.canonicality_state)
                .push_unseparated("::canonicality_state")
                .push_bind(&event.before_state)
                .push_bind(&event.after_state)
                .push_bind(&event.migration_correlation_ids)
                .push_bind(&event.consumer_visibility);
        });
        query.push(
            "
            ON CONFLICT (event_identity) DO UPDATE
            SET canonicality_state = EXCLUDED.canonicality_state,
                observed_at = now()
            WHERE ROW(
                normalized_events.namespace,
                normalized_events.logical_name_id,
                normalized_events.resource_id,
                normalized_events.event_kind,
                normalized_events.source_family,
                normalized_events.manifest_version,
                normalized_events.source_manifest_id,
                normalized_events.chain_id,
                normalized_events.block_number,
                normalized_events.block_hash,
                normalized_events.transaction_hash,
                normalized_events.transaction_index,
                normalized_events.log_index,
                normalized_events.raw_fact_ref,
                normalized_events.derivation_kind,
                normalized_events.before_state,
                normalized_events.after_state
                , normalized_events.migration_correlation_ids
                , normalized_events.consumer_visibility
            ) IS NOT DISTINCT FROM ROW(
                EXCLUDED.namespace,
                EXCLUDED.logical_name_id,
                EXCLUDED.resource_id,
                EXCLUDED.event_kind,
                EXCLUDED.source_family,
                EXCLUDED.manifest_version,
                EXCLUDED.source_manifest_id,
                EXCLUDED.chain_id,
                EXCLUDED.block_number,
                EXCLUDED.block_hash,
                EXCLUDED.transaction_hash,
                EXCLUDED.transaction_index,
                EXCLUDED.log_index,
                EXCLUDED.raw_fact_ref,
                EXCLUDED.derivation_kind,
                EXCLUDED.before_state,
                EXCLUDED.after_state
                , EXCLUDED.migration_correlation_ids
                , EXCLUDED.consumer_visibility
            )
            RETURNING event_identity
            ",
        );
        let written = query
            .build_query_scalar::<String>()
            .fetch_all(&mut **transaction)
            .await
            .map_err(|error| {
                let context =
                    batch_row_context(start, batch.iter().map(|event| &event.event_identity));
                InterpretError::database(
                    format!("failed to write normalized-event batch; {context}"),
                    error,
                )
            })?
            .into_iter()
            .collect::<HashSet<_>>();
        let conflicting = batch
            .iter()
            .enumerate()
            .filter(|(_, event)| !written.contains(&event.event_identity))
            .map(|(offset, event)| format!("{}={}", start + offset, event.event_identity))
            .collect::<Vec<_>>();
        if !conflicting.is_empty() {
            return Err(InterpretError::data_integrity(format!(
                "normalized event identities are already bound to different event data; conflicting batch rows [{}]",
                conflicting.join(", ")
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
