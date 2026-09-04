use sqlx::{Postgres, Transaction};

use crate::{Marker, ProjectError, Result};

/// Builds the parent-child relations each authority arm currently states, then publishes the one
/// the child's own authority selects.
///
/// Parent ENSv1→ENSv2 migration reachability filters the ENSv1 arm before the child's authority
/// selects an arm. A released ENSv2 child publishes nothing and never falls back to ENSv1, and a
/// pair whose arms cannot be told apart is omitted as unsupported rather than ranked.
pub(super) async fn build(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    candidates(transaction, chain_id, target).await?;
    publish(transaction, chain_id, target).await
}

async fn candidates(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    validate_parent_migration_paths(transaction).await?;
    sqlx::query(
        r#"
        CREATE TEMP TABLE project_child_candidates ON COMMIT DROP AS
        WITH target_time AS (
            SELECT extract(epoch FROM block_timestamp) AS epoch_seconds,
                   block_timestamp + interval '1 second' AS binding_cutoff
            FROM chain_lineage
            WHERE chain_id = $1 AND block_number = $2 AND block_hash = $3
        ), ranked_v2_subregistries AS (
            SELECT event.*,
                   row_number() OVER (
                       PARTITION BY event.logical_name_id
                       ORDER BY event.block_number DESC NULLS LAST,
                                event.transaction_index DESC NULLS LAST,
                                event.log_index DESC NULLS LAST,
                                event.event_identity DESC
                   ) AS current_rank
            FROM project_events event
            WHERE event.event_kind = 'SubregistryChanged'
              AND event.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
              AND event.logical_name_id IS NOT NULL
        ), current_v2_subregistries AS (
            SELECT event.*,
                   lower(event.after_state ->> 'subregistry') AS subregistry_address
            FROM ranked_v2_subregistries event
            WHERE event.current_rank = 1
              AND lower(COALESCE(event.after_state ->> 'subregistry', '')) NOT IN (
                  '', '0x0000000000000000000000000000000000000000'
              )
        ), parent_boundaries AS (
            SELECT DISTINCT ON (event.logical_name_id)
                   event.logical_name_id,
                   event.after_state ->> 'migration_path' AS migration_path,
                   jsonb_build_object(
                       'event_identity', event.event_identity,
                       'raw_fact_ref', event.raw_fact_ref,
                       'manifest', jsonb_build_object(
                           'source_manifest_id', event.source_manifest_id,
                           'source_family', event.source_family,
                           'manifest_version', event.manifest_version
                       )
                   ) AS evidence
            FROM project_events event
            WHERE event.source_family = 'ens_v2_migration_l1'
              AND event.event_kind = 'MigrationApplied'
              AND event.logical_name_id IS NOT NULL
            ORDER BY event.logical_name_id,
                     event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.event_identity DESC
        ), parent_migrations AS (
            SELECT boundary.*,
                   address.contract_instance_id::text
                       AS migration_registry_contract_instance_id,
                   jsonb_build_object(
                       'normalized_event_id', subregistry.normalized_event_id,
                       'event_identity', subregistry.event_identity,
                       'raw_fact_ref', subregistry.raw_fact_ref,
                       'manifest', jsonb_build_object(
                           'source_manifest_id', subregistry.source_manifest_id,
                           'source_family', subregistry.source_family,
                           'manifest_version', subregistry.manifest_version
                       )
                   ) AS migration_registry_evidence
            FROM parent_boundaries boundary
            LEFT JOIN current_v2_subregistries subregistry
              ON subregistry.logical_name_id = boundary.logical_name_id
            LEFT JOIN contract_instance_addresses address
              ON address.chain_id = subregistry.chain_id
             AND lower(address.address) = subregistry.subregistry_address
             AND (address.active_from_block_number IS NULL
                  OR address.active_from_block_number <= $2)
             AND (address.active_to_block_number IS NULL
                  OR address.active_to_block_number > $2)
             AND address.deactivated_at IS NULL
        ), latest_wrapper_modifiers AS (
            SELECT DISTINCT ON (event.logical_name_id)
                   event.logical_name_id, event.resource_id,
                   CASE event.after_state ->> 'wrapper_state'
                       WHEN 'wrapped' THEN 'wrapped'
                       WHEN 'emancipated' THEN 'emancipated'
                       WHEN 'locked' THEN 'locked'
                   END AS wrapper_state,
                   CASE WHEN jsonb_typeof(event.after_state -> 'fuses') = 'number'
                         AND (event.after_state ->> 'fuses')::numeric BETWEEN 0 AND 4294967295
                       THEN (event.after_state ->> 'fuses')::bigint END AS fuses,
                   jsonb_build_object(
                       'normalized_event_id', event.normalized_event_id,
                       'event_identity', event.event_identity,
                       'raw_fact_ref', event.raw_fact_ref,
                       'manifest', jsonb_build_object(
                           'source_manifest_id', event.source_manifest_id,
                           'source_family', event.source_family,
                           'manifest_version', event.manifest_version
                       )
                   ) AS evidence
            FROM project_events event
            WHERE event.source_family = 'ens_v1_wrapper_l1'
              AND event.event_kind = 'PermissionScopeChanged'
              AND event.logical_name_id IS NOT NULL AND event.resource_id IS NOT NULL
            ORDER BY event.logical_name_id,
                     event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.event_identity DESC
        ), latest_wrapper_expiries AS (
            SELECT DISTINCT ON (event.logical_name_id, event.resource_id)
                   event.logical_name_id, event.resource_id,
                   CASE WHEN jsonb_typeof(event.after_state -> 'expiry') = 'number'
                         AND (event.after_state ->> 'expiry')::numeric BETWEEN
                             0 AND 18446744073709551615
                       THEN (event.after_state ->> 'expiry')::numeric END AS expiry_seconds,
                   jsonb_build_object(
                       'normalized_event_id', event.normalized_event_id,
                       'event_identity', event.event_identity,
                       'raw_fact_ref', event.raw_fact_ref,
                       'manifest', jsonb_build_object(
                           'source_manifest_id', event.source_manifest_id,
                           'source_family', event.source_family,
                           'manifest_version', event.manifest_version
                       )
                   ) AS evidence
            FROM project_events event
            WHERE event.event_kind = 'ExpiryChanged'
              AND event.logical_name_id IS NOT NULL AND event.resource_id IS NOT NULL
              AND (event.source_family = 'ens_v1_wrapper_l1' OR (
                   event.source_family = 'ens_v1_registrar_l1'
                   AND event.after_state ->> 'source_event' = 'NameRenewed'
                   AND event.after_state ->> 'authority_kind' = 'wrapper'))
            ORDER BY event.logical_name_id, event.resource_id,
                     event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.event_identity DESC
        ), effective_wrapper_state AS (
            SELECT modifier.logical_name_id,
                   CASE WHEN modifier.wrapper_state IS NULL OR modifier.fuses IS NULL
                         OR expiry.expiry_seconds IS NULL OR target_time.epoch_seconds IS NULL
                         OR expiry.expiry_seconds < target_time.epoch_seconds THEN 0
                       ELSE modifier.fuses END AS fuses,
                   modifier.evidence AS modifier_evidence,
                   expiry.evidence AS expiry_evidence
            FROM latest_wrapper_modifiers modifier
            CROSS JOIN target_time
            LEFT JOIN latest_wrapper_expiries expiry
              ON expiry.logical_name_id = modifier.logical_name_id
             AND expiry.resource_id = modifier.resource_id
        ), v2_registration_history AS (
            SELECT DISTINCT event.logical_name_id,
                   event.after_state ->> 'registry_contract_instance_id'
                       AS registry_contract_instance_id
            FROM project_events event
            WHERE event.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
              AND event.event_kind IN (
                  'RegistrationReserved', 'RegistrationGranted', 'RegistrationRenewed'
              )
              AND event.logical_name_id IS NOT NULL
              AND event.after_state ->> 'registry_contract_instance_id' IS NOT NULL
        ), ranked_v1 AS (
            SELECT event.*,
                   row_number() OVER (
                       PARTITION BY event.namespace,
                                    event.after_state ->> 'child_node'
                       ORDER BY event.block_number DESC NULLS LAST,
                                event.transaction_index DESC NULLS LAST,
                                event.log_index DESC NULLS LAST,
                                -- Stable identity resolves only an exact-position duplicate;
                                -- generated IDs never participate.
                                event.event_identity DESC
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
                   lower(COALESCE(
                       ownership.owner_getter,
                       event.after_state ->> 'owner_getter', event.after_state ->> 'owner'
                   )) AS owner,
                   NULL::text AS registrant,
                   event.normalized_event_id,
                   event.event_identity,
                   event.source_manifest_id,
                   event.source_family,
                   event.manifest_version,
                   event.block_number,
                   event.transaction_index AS evidence_transaction_index,
                   event.log_index AS evidence_log_index,
                   event.block_hash,
                   event.raw_fact_ref,
                   event.canonicality_state::text AS canonicality_state,
                   CASE WHEN event.source_family = 'basenames_base_registry'
                       THEN 'basenames' ELSE 'ens_v1'
                   END AS authority_arm,
                   jsonb_path_query_array(jsonb_build_array(
                       migration.evidence, migration.migration_registry_evidence,
                       wrapper.modifier_evidence, wrapper.expiry_evidence
                   ), '$[*] ? (@ != null)') AS reachability_evidence,
                   CASE WHEN migration.migration_path IN ('locked_wrapped', 'locked_child')
                       THEN jsonb_build_object(
                           'derivation_kind', 'locked_parent_migratable_child',
                           'migration_registry_contract_instance_id',
                           migration.migration_registry_contract_instance_id
                       )
                   END AS parent_reachability
            FROM ranked_v1 event
            JOIN project_surfaces parent
              ON lower(parent.namehash) = lower(event.after_state ->> 'node')
             AND parent.namespace = event.namespace
             AND parent.chain_id = event.chain_id
             AND parent.visibility_state = 'active'
            LEFT JOIN label_preimages preimage
              ON lower(preimage.labelhash) =
                 lower(event.after_state ->> 'labelhash')
            LEFT JOIN project_latest_registry_owner ownership
              ON ownership.logical_name_id = event.namespace || ':' ||
                 lower(event.after_state ->> 'child_node')
            LEFT JOIN parent_migrations migration
              ON migration.logical_name_id = parent.logical_name_id
            LEFT JOIN effective_wrapper_state wrapper
              ON wrapper.logical_name_id = event.namespace || ':' ||
                 lower(event.after_state ->> 'child_node')
            WHERE event.current_rank = 1
              AND (
                  lower(COALESCE(
                      ownership.owner_getter, event.after_state ->> 'owner_getter',
                      event.after_state ->> 'owner', ''
                  )) NOT IN (
                      '', '0x0000000000000000000000000000000000000000'
                  )
                  OR EXISTS (
                      SELECT 1 FROM project_name_serving serving
                      WHERE serving.logical_name_id =
                            event.namespace || ':' ||
                            lower(event.after_state ->> 'child_node')
                  )
              )
              -- Unlocked ENSv1→ENSv2 migration has no child subregistry, and the Graveyard
              -- clears the unreachable ENSv1 path. (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L29-L31 @ ens_v2@a971bd64)
              -- (upstream: .refs/ens_v2/contracts/src/migration/Graveyard.sol:L170-L201 @ ens_v2@a971bd64)
              -- The locked registry retains exactly the contract's migratable children
              -- (docs/glossary.md#migratable-child).
              -- (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L293-L307 @ ens_v2@a971bd64)
              -- The fuse mask and its bit values come from the pinned ENS contracts.
              -- (upstream: .refs/ens_v2/contracts/src/migration/libraries/LibMigration.sol:L84-L89 @ ens_v2@a971bd64)
              -- (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L18-L19 @ ens_v1@91c966f)
              AND (event.source_family <> 'ens_v1_registry_l1'
                   OR migration.logical_name_id IS NULL
                   OR (migration.migration_path IN ('locked_wrapped', 'locked_child')
                       AND migration.migration_registry_contract_instance_id IS NOT NULL
                       AND (wrapper.fuses & 196608) = 65536
                       AND lower(COALESCE(
                           ownership.owner_getter, event.after_state ->> 'owner_getter',
                           event.after_state ->> 'owner', ''
                       )) NOT IN ('', '0x0000000000000000000000000000000000000000')
                       AND NOT EXISTS (
                           SELECT 1 FROM v2_registration_history history
                           WHERE history.logical_name_id = event.namespace || ':' ||
                                 lower(event.after_state ->> 'child_node')
                             AND history.registry_contract_instance_id =
                                 migration.migration_registry_contract_instance_id
                       )))
        ), ranked_v2_registrations AS (
            SELECT event.*,
                   row_number() OVER (
                       PARTITION BY event.logical_name_id,
                                    event.after_state ->> 'registry_contract_instance_id'
                       ORDER BY event.block_number DESC NULLS LAST,
                                event.transaction_index DESC NULLS LAST,
                                event.log_index DESC NULLS LAST,
                                -- Stable identity resolves only an exact-position duplicate;
                                -- generated IDs never participate.
                                event.event_identity DESC
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
                   registration.event_identity,
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
                   registration.transaction_index AS evidence_transaction_index,
                   registration.log_index AS evidence_log_index,
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
                   'ens_v2' AS authority_arm,
                   '[]'::jsonb AS reachability_evidence,
                   NULL::jsonb AS parent_reachability
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
        )
        SELECT * FROM v1_rows
        UNION ALL
        SELECT * FROM v2_rows
        "#,
    )
    .bind(chain_id)
    .bind(target.number)
    .bind(&target.hash)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to stage child candidates", error))?;

    sqlx::query(
        "CREATE INDEX ON project_child_candidates (
             parent_logical_name_id, child_logical_name_id
         )",
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to index child candidates", error))?;
    Ok(())
}

async fn validate_parent_migration_paths(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<()> {
    let invalid: Option<(String, Option<String>)> = sqlx::query_as(
        "SELECT logical_name_id, migration_path
         FROM (
             SELECT DISTINCT ON (logical_name_id) logical_name_id,
                    after_state ->> 'migration_path' AS migration_path
             FROM project_events
             WHERE source_family = 'ens_v2_migration_l1'
               AND event_kind = 'MigrationApplied' AND logical_name_id IS NOT NULL
             ORDER BY logical_name_id, block_number DESC NULLS LAST,
                      transaction_index DESC NULLS LAST, log_index DESC NULLS LAST,
                      event_identity DESC
         ) latest
         WHERE migration_path IS NULL OR migration_path NOT IN (
             'unwrapped', 'unlocked_wrapped', 'locked_wrapped',
             'locked_child', 'emancipated_child'
         )
         LIMIT 1",
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to validate parent migration paths", error))?;
    if let Some((logical_name_id, migration_path)) = invalid {
        return Err(ProjectError::data_integrity(format!(
            "unsupported ENSv1→ENSv2 migration path {:?} for {logical_name_id}",
            migration_path
        )));
    }
    Ok(())
}

async fn publish(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    sqlx::query(
        r#"
        WITH selected AS (
            SELECT candidate.*,
                   row_number() OVER (
                       PARTITION BY candidate.parent_logical_name_id,
                                    candidate.child_logical_name_id
                       -- Recency picks the current relation inside one selected arm; it never
                       -- picks the arm.
                       ORDER BY candidate.block_number DESC NULLS LAST,
                                candidate.evidence_transaction_index DESC NULLS LAST,
                                candidate.evidence_log_index DESC NULLS LAST,
                                -- Stable identity is the final exact-position tie-break.
                                candidate.event_identity DESC
                   ) AS pair_rank
            FROM project_child_candidates candidate
            LEFT JOIN project_name_authority authority
              ON authority.logical_name_id = candidate.child_logical_name_id
            WHERE authority.selected_authority_arm = candidate.authority_arm
               -- A child whose own authority is undetermined keeps an unambiguous single-arm
               -- relation, which is what an ordinary unmigrated subname has; a pair whose arms
               -- disagree is omitted rather than ranked.
               OR (
                   authority.selected_authority_arm IS NULL
                   AND NOT EXISTS (
                       SELECT 1 FROM project_child_candidates other
                       WHERE other.parent_logical_name_id =
                             candidate.parent_logical_name_id
                         AND other.child_logical_name_id =
                             candidate.child_logical_name_id
                         AND other.authority_arm <> candidate.authority_arm
                   )
               )
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
                   'normalized_event_ids', jsonb_build_array(child.normalized_event_id)
                       || jsonb_path_query_array(
                           child.reachability_evidence, '$[*].normalized_event_id'),
                   'raw_fact_refs', jsonb_build_array(child.raw_fact_ref)
                       || jsonb_path_query_array(
                           child.reachability_evidence, '$[*].raw_fact_ref'),
                   'manifest_versions', jsonb_build_array(jsonb_build_object(
                       'source_manifest_id', child.source_manifest_id,
                       'source_family', child.source_family,
                       'manifest_version', child.manifest_version
                   )) || jsonb_path_query_array(
                       child.reachability_evidence, '$[*].manifest'),
                   'derivation_kind', 'children_current_rebuild',
                   'chain_id', $1,
                   'coverage', jsonb_build_object(
                       'status', 'projected',
                       'exhaustiveness', 'not_asserted'
                   )
               ) || CASE WHEN child.parent_reachability IS NULL THEN '{}'::jsonb
                   ELSE jsonb_build_object(
                       'event_identities', jsonb_build_array(child.event_identity)
                           || jsonb_path_query_array(
                               child.reachability_evidence, '$[*].event_identity'),
                       'parent_reachability', child.parent_reachability
                   ) END,
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
               GREATEST(child.manifest_version, COALESCE(
                   (SELECT max((evidence -> 'manifest' ->> 'manifest_version')::bigint)
                    FROM jsonb_array_elements(child.reachability_evidence) evidence),
                   child.manifest_version
               ))
        FROM selected child
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
