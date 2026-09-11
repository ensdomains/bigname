use sqlx::{Postgres, Transaction};

use crate::{Marker, ProjectError, Result};

/// Reverse index over current `addr:<coin_type>` records: one row per (address the record
/// resolves to, coin type, current bound name). It re-reads the staged record inventory rather
/// than the record events, so a row exists exactly when the forward read of that record on the
/// name's record-serving resource answers `success` with a 20-byte non-zero EVM address.
///
/// The ENSIP-19 default EVM address (`addr:2147483648`) answers any EVM coin type without its own
/// exact entry, so its row carries `provenance.ensip19_default_address` and the coin types whose
/// exact entry shadows it; the read side applies that fallback rule per requested coin type
/// (`bigname_domain::resolver_read::evaluate_indexed_record`).
pub(super) async fn build(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    sqlx::query(
        r#"
        WITH record_entries AS (
            SELECT inventory.resource_id,
                   inventory.record_version_boundary_key,
                   inventory.provenance,
                   inventory.chain_positions,
                   inventory.last_change,
                   inventory.manifest_version,
                   entry ->> 'record_key' AS record_key,
                   (entry ->> 'selector_key')::numeric::text AS coin_type,
                   entry ->> 'status' AS status,
                   lower(CASE
                       WHEN jsonb_typeof(entry -> 'value') = 'string' THEN entry ->> 'value'
                       ELSE COALESCE(entry #>> '{value,value}', entry #>> '{value,bytes}')
                   END) AS address
            FROM project_stage_record_inventory_current inventory
            CROSS JOIN LATERAL jsonb_array_elements(inventory.entries) entry
            WHERE inventory.support_status = 'supported'
              AND inventory.provenance ->> 'record_serving' IS DISTINCT FROM 'false'
              AND entry ->> 'record_family' = 'addr'
              AND entry ->> 'selector_key' ~ '^[0-9]{1,30}$'
        ),
        -- Exact entries that stop the default EVM address from answering their coin type: any
        -- retained answer, or the coin-60 zero-address clear the inventory marks as absent.
        shadows AS (
            SELECT entry.resource_id,
                   jsonb_agg(DISTINCT to_jsonb(entry.coin_type)
                             ORDER BY to_jsonb(entry.coin_type)) AS coin_types
            FROM record_entries entry
            WHERE entry.record_key <> 'addr:2147483648'
              AND (
                  entry.status <> 'not_found'
                  OR (
                      entry.record_key = 'addr:60'
                      AND COALESCE(
                          entry.provenance -> 'exact_nonempty_not_found_record_keys'
                              ? 'addr:60',
                          false
                      )
                  )
              )
            GROUP BY entry.resource_id
        ),
        names AS (
            SELECT name.logical_name_id, name.namespace, name.raw_name, name.namehash,
                   name.surface_binding_id, name.resource_id, name.binding_kind,
                   name.manifest_version,
                   COALESCE(name.serving_resource_id, name.resource_id) AS record_resource_id
            FROM project_stage_name_current name
            WHERE name.surface_binding_id IS NOT NULL
              AND name.resource_id IS NOT NULL
              AND name.binding_kind IS NOT NULL
        )
        INSERT INTO project_stage_address_records_current (
            address, coin_type, logical_name_id, namespace, raw_name, namehash,
            surface_binding_id, resource_id, record_resource_id, binding_kind, record_key,
            support_status, unsupported_reason, provenance, chain_positions,
            canonicality_summary, manifest_version
        )
        SELECT DISTINCT ON (record.address, record.coin_type, name.logical_name_id)
               record.address,
               record.coin_type,
               name.logical_name_id,
               name.namespace,
               name.raw_name,
               name.namehash,
               name.surface_binding_id,
               name.resource_id,
               record.resource_id,
               name.binding_kind,
               record.record_key,
               'supported',
               NULL,
               jsonb_strip_nulls(jsonb_build_object(
                   'chain_id', $1,
                   'logical_name_id', name.logical_name_id,
                   'resolver_address', record.provenance ->> 'resolver_address',
                   'record_version_boundary_key', record.record_version_boundary_key,
                   'normalized_event_id', record.last_change -> 'normalized_event_id',
                   'coverage', jsonb_build_object(
                       'status', 'projected',
                       'exhaustiveness', 'not_asserted'
                   )
               )) || CASE
                   WHEN record.record_key = 'addr:2147483648'
                    AND COALESCE(record.provenance -> 'read_rules', '[]'::jsonb) @>
                        '[{"kind": "ensip19_default_address",
                           "source_record_key": "addr:2147483648"}]'::jsonb
                       THEN jsonb_build_object(
                           'ensip19_default_address', true,
                           'shadowed_coin_types', COALESCE(shadow.coin_types, '[]'::jsonb)
                       )
                   ELSE '{}'::jsonb
               END,
               jsonb_strip_nulls(jsonb_build_object(
                   'block_number', record.chain_positions -> 'block_number',
                   'block_hash', record.chain_positions -> 'block_hash',
                   'target_block_number', $2,
                   'target_block_hash', $3
               )),
               jsonb_build_object(
                   'state', 'canonical_lineage',
                   'target_block_number', $2,
                   'target_block_hash', $3
               ),
               GREATEST(name.manifest_version, record.manifest_version)
        FROM record_entries record
        JOIN names name ON name.record_resource_id = record.resource_id
        LEFT JOIN shadows shadow ON shadow.resource_id = record.resource_id
        WHERE record.status = 'success'
          AND record.address ~ '^0x[0-9a-f]{40}$'
          AND record.address <> '0x0000000000000000000000000000000000000000'
        -- A resource publishes one inventory row per build; the boundary key is a deterministic
        -- tie-breaker so a duplicate can never make the row choice depend on scan order.
        ORDER BY record.address, record.coin_type, name.logical_name_id,
                 record.record_version_boundary_key DESC
        "#,
    )
    .bind(chain_id)
    .bind(target.number)
    .bind(&target.hash)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to build address_records_current", error))?;
    Ok(())
}
