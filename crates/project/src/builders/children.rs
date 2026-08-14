use sqlx::{Postgres, Transaction};

use crate::{Marker, ProjectError, Result};

pub(super) async fn build(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    sqlx::query(
        r#"
        WITH ranked_v1 AS (
            SELECT event.*,
                   row_number() OVER (
                       PARTITION BY event.namespace,
                                    event.after_state ->> 'child_node'
                       ORDER BY event.block_number DESC NULLS LAST,
                                event.transaction_index DESC NULLS LAST,
                                event.log_index DESC NULLS LAST,
                                event.normalized_event_id DESC
                   ) AS current_rank
            FROM project_events event
            WHERE event.event_kind = 'SubregistryChanged'
              AND event.source_family IN (
                  'ens_v1_registry_l1', 'basenames_base_registry'
              )
              AND event.after_state ->> 'node' IS NOT NULL
              AND event.after_state ->> 'child_node' IS NOT NULL
              AND event.after_state ->> 'labelhash' IS NOT NULL
        ),
        -- A proof-checked label whose text fails normalization keeps its raw bytes in
        -- label_preimages but must not serve that text as a name: serving falls to the
        -- documented placeholder only when both name columns are null, so raw_name is
        -- suppressed with decoded_name here. Label bytes that do not decode keep raw_name
        -- for the documented escaped form.
        v1_rows AS (
            SELECT parent.logical_name_id AS parent_logical_name_id,
                   event.namespace || ':' ||
                       lower(event.after_state ->> 'child_node') AS child_logical_name_id,
                   event.namespace,
                   CASE WHEN preimage.raw_label IS NULL THEN NULL
                       WHEN preimage.decoded_label IS NOT NULL
                            AND NOT preimage.normalized_under_version THEN NULL
                       WHEN parent.raw_name = '' THEN preimage.raw_label
                       ELSE preimage.raw_label || decode('2e', 'hex') ||
                            convert_to(parent.raw_name, 'UTF8')
                   END AS raw_name,
                   CASE WHEN preimage.decoded_label IS NULL THEN NULL
                       WHEN NOT preimage.normalized_under_version THEN NULL
                       WHEN parent.raw_name = '' THEN preimage.decoded_label
                       ELSE preimage.decoded_label || '.' || parent.raw_name
                   END AS decoded_name,
                   preimage.raw_label,
                   CASE WHEN preimage.normalized_under_version THEN preimage.decoded_label
                   END AS decoded_label,
                   lower(event.after_state ->> 'child_node') AS namehash,
                   lower(event.after_state ->> 'labelhash') AS labelhash,
                   lower(event.after_state ->> 'owner') AS owner,
                   NULL::text AS registrant,
                   event.normalized_event_id,
                   event.source_manifest_id,
                   event.source_family,
                   event.manifest_version,
                   event.block_number,
                   event.block_hash,
                   event.raw_fact_ref,
                   event.canonicality_state::text AS canonicality_state,
                   1 AS source_priority
            FROM ranked_v1 event
            JOIN project_surfaces parent
              ON lower(parent.namehash) = lower(event.after_state ->> 'node')
             AND parent.namespace = event.namespace
             AND parent.chain_id = event.chain_id
             AND parent.visibility_state = 'active'
            LEFT JOIN label_preimages preimage
              ON lower(preimage.labelhash) =
                 lower(event.after_state ->> 'labelhash')
            WHERE event.current_rank = 1
              AND lower(COALESCE(event.after_state ->> 'owner', '')) NOT IN (
                  '', '0x0000000000000000000000000000000000000000'
              )
        ),
        ranked_v2_subregistries AS (
            SELECT event.*,
                   row_number() OVER (
                       PARTITION BY event.logical_name_id
                       ORDER BY event.block_number DESC NULLS LAST,
                                event.transaction_index DESC NULLS LAST,
                                event.log_index DESC NULLS LAST,
                                event.normalized_event_id DESC
                   ) AS current_rank
            FROM project_events event
            WHERE event.event_kind = 'SubregistryChanged'
              AND event.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
              AND event.logical_name_id IS NOT NULL
        ),
        current_v2_subregistries AS (
            SELECT event.*,
                   lower(event.after_state ->> 'subregistry') AS subregistry_address
            FROM ranked_v2_subregistries event
            WHERE event.current_rank = 1
              AND lower(COALESCE(event.after_state ->> 'subregistry', '')) NOT IN (
                  '', '0x0000000000000000000000000000000000000000'
              )
        ),
        ranked_v2_registrations AS (
            SELECT event.*,
                   row_number() OVER (
                       PARTITION BY event.logical_name_id,
                                    event.after_state ->> 'registry_contract_instance_id'
                       ORDER BY event.block_number DESC NULLS LAST,
                                event.transaction_index DESC NULLS LAST,
                                event.log_index DESC NULLS LAST,
                                event.normalized_event_id DESC
                   ) AS current_rank
            FROM project_events event
            WHERE event.event_kind IN (
                  'RegistrationGranted', 'RegistrationRenewed', 'RegistrationReleased'
              )
              AND event.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
              AND event.logical_name_id IS NOT NULL
        ),
        v2_rows AS (
            SELECT parent.logical_name_id AS parent_logical_name_id,
                   child.logical_name_id AS child_logical_name_id,
                   child.namespace,
                   CASE WHEN preimage.raw_label IS NULL THEN NULL
                       WHEN preimage.decoded_label IS NOT NULL
                            AND NOT preimage.normalized_under_version THEN NULL
                       ELSE preimage.raw_label || decode('2e', 'hex') ||
                            convert_to(parent.raw_name, 'UTF8')
                   END AS raw_name,
                   CASE WHEN preimage.decoded_label IS NULL THEN NULL
                       WHEN NOT preimage.normalized_under_version THEN NULL
                       ELSE preimage.decoded_label || '.' || parent.raw_name
                   END AS decoded_name,
                   preimage.raw_label,
                   CASE WHEN preimage.normalized_under_version THEN preimage.decoded_label
                   END AS decoded_label,
                   child.namehash,
                   lower(child.labelhashes[1]) AS labelhash,
                   NULL::text AS owner,
                   lower(registration.after_state ->> 'registrant') AS registrant,
                   registration.normalized_event_id,
                   registration.source_manifest_id,
                   registration.source_family,
                   GREATEST(
                       registration.manifest_version,
                       subregistry.manifest_version
                   ) AS manifest_version,
                   GREATEST(
                       registration.block_number,
                       subregistry.block_number
                   ) AS block_number,
                   CASE
                       WHEN registration.block_number >= subregistry.block_number
                           THEN registration.block_hash
                       ELSE subregistry.block_hash
                   END AS block_hash,
                   jsonb_build_object(
                       'subregistry', subregistry.raw_fact_ref,
                       'registration', registration.raw_fact_ref
                   ) AS raw_fact_ref,
                   registration.canonicality_state::text AS canonicality_state,
                   2 AS source_priority
            FROM current_v2_subregistries subregistry
            JOIN project_surfaces parent
              ON parent.logical_name_id = subregistry.logical_name_id
             AND parent.visibility_state = 'active'
            JOIN contract_instance_addresses address
              ON address.chain_id = subregistry.chain_id
             AND lower(address.address) = subregistry.subregistry_address
             AND (address.active_from_block_number IS NULL
                  OR address.active_from_block_number <= $2)
             AND (address.active_to_block_number IS NULL
                  OR address.active_to_block_number > $2)
             AND address.deactivated_at IS NULL
            JOIN ranked_v2_registrations registration
              ON registration.current_rank = 1
             AND registration.event_kind <> 'RegistrationReleased'
             AND registration.after_state ->> 'registry_contract_instance_id' =
                 address.contract_instance_id::text
            JOIN project_surfaces child
              ON child.logical_name_id = registration.logical_name_id
             AND child.namespace = parent.namespace
             AND child.chain_id = parent.chain_id
             AND child.visibility_state = 'active'
             AND cardinality(child.labelhashes) = cardinality(parent.labelhashes) + 1
             AND child.labelhashes[2:cardinality(child.labelhashes)] = parent.labelhashes
            LEFT JOIN label_preimages preimage
              ON lower(preimage.labelhash) = lower(child.labelhashes[1])
            WHERE parent.raw_name <> ''
        ),
        ranked_rows AS (
            SELECT row.*,
                   row_number() OVER (
                       PARTITION BY row.parent_logical_name_id,
                                    row.child_logical_name_id
                       ORDER BY row.block_number DESC NULLS LAST,
                                row.normalized_event_id DESC,
                                row.source_priority DESC
                   ) AS pair_rank
            FROM (
                SELECT * FROM v1_rows
                UNION ALL
                SELECT * FROM v2_rows
            ) row
        )
        INSERT INTO project_stage_children_current (
            parent_logical_name_id, child_logical_name_id, surface_class,
            namespace, raw_name, decoded_name, raw_label, decoded_label,
            namehash, labelhash, owner, registrant, provenance, chain_positions,
            canonicality_summary, manifest_version
        )
        SELECT child.parent_logical_name_id,
               child.child_logical_name_id,
               'declared',
               child.namespace,
               child.raw_name,
               child.decoded_name,
               child.raw_label,
               child.decoded_label,
               child.namehash,
               child.labelhash,
               child.owner,
               child.registrant,
               jsonb_build_object(
                   'normalized_event_ids', jsonb_build_array(
                       child.normalized_event_id
                   ),
                   'raw_fact_refs', jsonb_build_array(child.raw_fact_ref),
                   'manifest_versions', jsonb_build_array(jsonb_build_object(
                       'source_manifest_id', child.source_manifest_id,
                       'source_family', child.source_family,
                       'manifest_version', child.manifest_version
                   )),
                   'derivation_kind', 'children_current_rebuild',
                   'chain_id', $1,
                   'coverage', jsonb_build_object(
                       'status', 'projected',
                       'exhaustiveness', 'not_asserted'
                   )
               ),
               jsonb_build_object(
                   'block_number', child.block_number,
                   'block_hash', child.block_hash,
                   'target_block_number', $2,
                   'target_block_hash', $3
               ),
               jsonb_build_object(
                   'state', child.canonicality_state,
                   'target_block_number', $2,
                   'target_block_hash', $3
               ),
               child.manifest_version
        FROM ranked_rows child
        WHERE child.pair_rank = 1
        ORDER BY child.parent_logical_name_id, child.child_logical_name_id
        "#,
    )
    .bind(chain_id)
    .bind(target.number)
    .bind(&target.hash)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to build children_current", error))?;
    Ok(())
}
