use sqlx::{Postgres, Transaction};

use crate::{Marker, ProjectError, Result};

pub(super) async fn build(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    sqlx::query(
        r#"
        WITH latest_pointers AS (
            SELECT DISTINCT ON (event.resource_id)
                   event.resource_id,
                   event.logical_name_id,
                   lower(event.after_state ->> 'resolver') AS resolver_address,
                   event.manifest_version AS pointer_manifest_version,
                   event.normalized_event_id AS pointer_event_id,
                   event.block_number AS pointer_block_number,
                   event.block_hash AS pointer_block_hash
            FROM project_events event
            WHERE event.event_kind = 'ResolverChanged'
              AND event.resource_id IS NOT NULL
              AND event.logical_name_id IS NOT NULL
            ORDER BY event.resource_id,
                     event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
        ),
        pointers AS (
            SELECT * FROM latest_pointers
            WHERE resolver_address IS NOT NULL
              AND resolver_address NOT IN (
                  '0x0000000000000000000000000000000000000000', ''
              )
        )
        INSERT INTO project_stage_record_inventory_current (
            resource_id, record_version_boundary_key, record_version_boundary,
            selectors, unsupported_families, last_change, entries,
            support_status, unsupported_reason, provenance, chain_positions,
            canonicality_summary, manifest_version
        )
        SELECT pointer.resource_id,
               concat_ws(':',
                   pointer.logical_name_id,
                   pointer.resource_id::text,
                   COALESCE(version.normalized_event_id, 0)::text,
                   boundary.block_number::text,
                   boundary.block_hash
               ),
               jsonb_build_object(
                   'logical_name_id', pointer.logical_name_id,
                   'resource_id', pointer.resource_id,
                   'normalized_event_id', version.normalized_event_id,
                   'event_kind', CASE
                       WHEN version.normalized_event_id IS NOT NULL
                           THEN 'RecordVersionChanged'
                       ELSE NULL
                   END,
                   'chain_position', jsonb_strip_nulls(jsonb_build_object(
                       'chain_id', $1,
                       'block_number', boundary.block_number,
                       'block_hash', boundary.block_hash,
                       'timestamp', boundary.block_timestamp
                   ))
               ),
               COALESCE(records.selectors, '[]'::jsonb),
               COALESCE(records.unsupported_families, '[]'::jsonb) || CASE
                   WHEN resolver.support_status = 'supported' THEN '[]'::jsonb
                   ELSE jsonb_build_array(jsonb_build_object(
                       'record_family', 'resolver_classification',
                       'unsupported_reason', COALESCE(
                           resolver.unsupported_reason,
                           'resolver_classification_missing'
                       )
                   ))
               END,
               COALESCE(records.last_change, jsonb_build_object(
                   'normalized_event_id', COALESCE(
                       version.normalized_event_id,
                       pointer.pointer_event_id
                   ),
                   'event_kind', CASE
                       WHEN version.normalized_event_id IS NOT NULL
                           THEN 'RecordVersionChanged'
                       ELSE 'ResolverChanged'
                   END,
                   'chain_position', jsonb_strip_nulls(jsonb_build_object(
                       'chain_id', $1,
                       'block_number', boundary.block_number,
                       'block_hash', boundary.block_hash,
                       'timestamp', boundary.block_timestamp
                   ))
               )),
               COALESCE(records.entries, '[]'::jsonb),
               CASE WHEN resolver.support_status = 'supported'
                   THEN 'supported' ELSE 'unsupported' END,
               CASE
                   WHEN resolver.resolver_address IS NULL
                       THEN 'resolver_classification_missing'
                   WHEN resolver.support_status <> 'supported'
                       THEN resolver.unsupported_reason
                   ELSE NULL
               END,
               jsonb_build_object(
                   'chain_id', $1,
                   'logical_name_id', pointer.logical_name_id,
                   'resolver_address', pointer.resolver_address,
                   'resolver_pointer_event_id', pointer.pointer_event_id,
                   'record_event_ids', COALESCE(records.event_ids, '[]'::jsonb),
                   'coverage', jsonb_build_object(
                       'status', 'projected',
                       'exhaustiveness', 'not_asserted'
                   )
               ),
               jsonb_strip_nulls(jsonb_build_object(
                   'block_number', latest_position.block_number,
                   'block_hash', latest_position.block_hash,
                   'target_block_number', $2,
                   'target_block_hash', $3
               )),
               jsonb_build_object(
                   'state', 'canonical_lineage',
                   'target_block_number', $2,
                   'target_block_hash', $3
               ),
               GREATEST(
                   pointer.pointer_manifest_version,
                   COALESCE(version.manifest_version, 1),
                   COALESCE(records.manifest_version, 1),
                   COALESCE(resolver.manifest_version, 1)
               )
        FROM pointers pointer
        LEFT JOIN project_stage_resolver_current resolver
          ON resolver.chain_id = $1
         AND lower(resolver.resolver_address) = pointer.resolver_address
        LEFT JOIN LATERAL (
            SELECT event.after_state ->> 'record_version' AS record_version,
                   event.normalized_event_id,
                   event.block_number,
                   event.block_hash,
                   event.transaction_index,
                   event.log_index,
                   event.manifest_version
            FROM project_events event
            WHERE event.logical_name_id = pointer.logical_name_id
              AND event.event_kind = 'RecordVersionChanged'
              AND lower(COALESCE(
                    NULLIF(event.after_state ->> 'resolver', ''),
                    NULLIF(event.raw_fact_ref ->> 'emitting_address', '')
                  )) = pointer.resolver_address
            ORDER BY event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) version ON TRUE
        LEFT JOIN LATERAL (
            SELECT COALESCE(version.block_number, pointer.pointer_block_number) AS block_number,
                   COALESCE(version.block_hash, pointer.pointer_block_hash) AS block_hash,
                   lineage.block_timestamp
            FROM chain_lineage lineage
            WHERE lineage.chain_id = $1
              AND lineage.block_number = COALESCE(
                  version.block_number, pointer.pointer_block_number
              )
              AND lineage.block_hash = COALESCE(
                  version.block_hash, pointer.pointer_block_hash
              )
            LIMIT 1
        ) boundary ON TRUE
        LEFT JOIN LATERAL (
            WITH current_records AS (
                SELECT DISTINCT ON (event.after_state ->> 'record_key') event.*
                FROM project_events event
                WHERE event.logical_name_id = pointer.logical_name_id
                  AND event.event_kind = 'RecordChanged'
                  AND lower(COALESCE(
                        NULLIF(event.after_state ->> 'resolver', ''),
                        NULLIF(event.raw_fact_ref ->> 'emitting_address', '')
                      )) = pointer.resolver_address
                  AND (
                      version.normalized_event_id IS NULL
                      OR ROW(
                          event.block_number,
                          COALESCE(event.transaction_index, -1),
                          COALESCE(event.log_index, -1),
                          event.normalized_event_id
                      ) > ROW(
                          version.block_number,
                          COALESCE(version.transaction_index, -1),
                          COALESCE(version.log_index, -1),
                          version.normalized_event_id
                      )
                  )
                ORDER BY event.after_state ->> 'record_key',
                         event.block_number DESC NULLS LAST,
                         event.transaction_index DESC NULLS LAST,
                         event.log_index DESC NULLS LAST,
                         event.normalized_event_id DESC
            )
            SELECT jsonb_agg(jsonb_build_object(
                       'record_key', event.after_state ->> 'record_key',
                       'record_family', event.after_state ->> 'record_family',
                       'selector_key', event.after_state -> 'selector_key',
                       'cacheable', true
                   ) ORDER BY event.after_state ->> 'record_key')
                   FILTER (WHERE
                       (
                           event.after_state ->> 'record_family' = 'text'
                           AND (
                               event.after_state ->> 'record_key' = 'text'
                               OR event.after_state ->> 'record_key' = concat(
                                   'text:', event.after_state ->> 'selector_key'
                               )
                           )
                       ) OR (
                           event.after_state ->> 'record_family' = 'addr'
                           AND event.after_state ->> 'selector_key' IS NOT NULL
                           AND event.after_state ->> 'record_key' = concat(
                               'addr:', event.after_state ->> 'selector_key'
                           )
                       ) OR (
                           event.after_state ->> 'record_family' = 'contenthash'
                           AND event.after_state ->> 'record_key' = 'contenthash'
                       )
                   ) AS selectors,
                   jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
                       'record_key', event.after_state ->> 'record_key',
                       'record_family', event.after_state ->> 'record_family',
                       'selector_key', event.after_state -> 'selector_key',
                       'status', CASE
                           WHEN event.after_state ? 'value' THEN CASE
                               WHEN event.after_state ->> 'record_family' = 'contenthash'
                                AND event.after_state ->> 'value' IN ('', '0x')
                                   THEN 'not_found'
                               ELSE 'success'
                           END
                           WHEN event.after_state ? 'contenthash_hex' THEN CASE
                               WHEN event.after_state ->> 'contenthash_hex' IN ('', '0x')
                                   THEN 'not_found'
                               ELSE 'success'
                           END
                           WHEN event.after_state ? 'address_bytes_hex' THEN CASE
                               WHEN event.after_state ->> 'address_bytes_hex' IN ('', '0x')
                                   THEN 'not_found'
                               ELSE 'success'
                           END
                           ELSE 'unsupported'
                       END,
                       'value', CASE
                           WHEN event.after_state ? 'value'
                            AND NOT (
                                event.after_state ->> 'record_family' = 'contenthash'
                                AND event.after_state ->> 'value' IN ('', '0x')
                            ) THEN event.after_state -> 'value'
                           WHEN event.after_state ? 'contenthash_hex'
                            AND event.after_state ->> 'contenthash_hex' NOT IN ('', '0x')
                               THEN jsonb_build_object(
                                   'encoding', 'hex',
                                   'bytes', event.after_state ->> 'contenthash_hex'
                               )
                           WHEN event.after_state ? 'address_bytes_hex'
                            AND event.after_state ->> 'address_bytes_hex' NOT IN ('', '0x')
                               THEN event.after_state -> 'address_bytes_hex'
                       END,
                       'unsupported_reason', CASE
                           WHEN NOT (event.after_state ? 'value')
                            AND NOT (event.after_state ? 'contenthash_hex')
                            AND NOT (event.after_state ? 'address_bytes_hex')
                               THEN 'value_not_retained_in_normalized_events'
                       END
                   )) ORDER BY event.after_state ->> 'record_key')
                   FILTER (WHERE
                       event.after_state ->> 'record_family' IN (
                           'text', 'addr', 'contenthash'
                       )
                   ) AS entries,
                   COALESCE((
                       SELECT jsonb_agg(jsonb_build_object(
                                  'record_family', family,
                                  'unsupported_reason',
                                      'record_family_not_supported_in_phase6_projection'
                              ) ORDER BY family)
                       FROM (
                           SELECT DISTINCT event.after_state ->> 'record_family' AS family
                           FROM current_records event
                           WHERE event.after_state ->> 'record_family' NOT IN (
                               'text', 'addr', 'contenthash'
                           )
                       ) unsupported
                   ), '[]'::jsonb) AS unsupported_families,
                   jsonb_agg(to_jsonb(event.normalized_event_id)
                             ORDER BY event.normalized_event_id) AS event_ids,
                   max(event.block_number) AS block_number,
                   (array_agg(event.block_hash
                              ORDER BY event.block_number DESC,
                                       event.transaction_index DESC NULLS LAST,
                                       event.log_index DESC NULLS LAST,
                                       event.normalized_event_id DESC))[1] AS block_hash,
                   max(event.manifest_version) AS manifest_version,
                   (array_agg(jsonb_build_object(
                       'normalized_event_id', event.normalized_event_id,
                       'event_kind', event.event_kind,
                       'chain_position', jsonb_strip_nulls(jsonb_build_object(
                           'chain_id', event.chain_id,
                           'block_number', event.block_number,
                           'block_hash', event.block_hash,
                           'timestamp', lineage.block_timestamp
                       ))
                   ) ORDER BY event.block_number DESC NULLS LAST,
                              event.transaction_index DESC NULLS LAST,
                              event.log_index DESC NULLS LAST,
                              event.normalized_event_id DESC))[1] AS last_change
            FROM current_records event
            LEFT JOIN chain_lineage lineage
              ON lineage.chain_id = event.chain_id
             AND lineage.block_number = event.block_number
             AND lineage.block_hash = event.block_hash
        ) records ON TRUE
        LEFT JOIN LATERAL (
            SELECT candidate.block_number, candidate.block_hash
            FROM (
                VALUES
                    (pointer.pointer_block_number, pointer.pointer_block_hash),
                    (version.block_number, version.block_hash),
                    (records.block_number, records.block_hash)
            ) candidate(block_number, block_hash)
            WHERE candidate.block_number IS NOT NULL
              AND candidate.block_hash IS NOT NULL
            ORDER BY candidate.block_number DESC, candidate.block_hash DESC
            LIMIT 1
        ) latest_position ON TRUE
        ORDER BY pointer.resource_id
        "#,
    )
    .bind(chain_id)
    .bind(target.number)
    .bind(&target.hash)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to build record_inventory_current", error))?;
    Ok(())
}
